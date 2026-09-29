"""Parsing des PDF de tracking France Galop — code repris tel quel du notebook
galoptrack_scraper (cellule 5), validé sur des PDF réels.

Architecture : pour chaque partant (lu en page 1), on retrouve sa page
individuelle, source unique des tronçons (labels réels, cumuls, durées,
vitesses, foulées) ; la page 1 ne sert qu'aux infos générales de course.
"""

import io
import json
import logging
import re

import pdfplumber

log = logging.getLogger(__name__)


def _find_horse_page(pages_text, nom_up, mots_nom):
    best_page, best_score = None, 0
    for page_text in pages_text[1:]:
        score = sum(1 for m in mots_nom if m in page_text.upper())
        if score > best_score and 'Vitesse moyenne' in page_text and 'Position' in page_text:
            best_score = score
            best_page  = page_text
    return best_page if best_score > 0 else None


_RE_TITRE_BLOC = re.compile(r'Tronçons de (\d+)m')


def longueur_troncons(page_text):
    """200 en plat ; 1000 en obstacles (premier tableau de la page)."""
    if 'Tronçons de 200m' in page_text:
        return 200
    m = _RE_TITRE_BLOC.search(page_text)
    return int(m.group(1)) if m else 200


def _extraire_troncons_page_individuelle(page_text):
    """
    Source UNIQUE de vérité pour les tronçons : tout est lu depuis la page
    individuelle du cheval (labels réels, cumuls, durées officielles, vitesses,
    foulées). Retourne une liste de dicts, un par tronçon de 200m, dans l'ordre
    chronologique (du départ vers l'arrivée).
    """
    lines_full = page_text.split('\n')
    idx_titre = next((i for i, l in enumerate(lines_full) if 'Tronçons de 200m' in l), None)
    ligne_arrivee = None
    if idx_titre is None:
        # Obstacles : tronçons de 1000m, bornes d'arrivée sur la ligne du titre
        # ('     Tronçons de 1000m  4000m   3000m ...  ARR'), bornes de départ
        # sur la ligne au-dessus ('DEP  4000m  3000m ...').
        idx_titre = next((i for i, l in enumerate(lines_full) if _RE_TITRE_BLOC.search(l)), None)
        if idx_titre is not None:
            ligne_arrivee = _RE_TITRE_BLOC.split(lines_full[idx_titre], maxsplit=1)[-1]
    if idx_titre is None or idx_titre == 0 or idx_titre >= len(lines_full) - 1:
        return []

    ligne_depart  = lines_full[idx_titre - 1]
    if ligne_arrivee is None:
        ligne_arrivee = lines_full[idx_titre + 1]
    tokens_depart  = re.findall(r'(DEP|\d+m)', ligne_depart)
    tokens_arrivee = re.findall(r'(\d+m|ARR)', ligne_arrivee)
    if len(tokens_depart) != len(tokens_arrivee) or not tokens_depart:
        return []
    labels = [f'{d}-{a}' for d, a in zip(tokens_depart, tokens_arrivee)]
    n = len(labels)

    # Recherche des lignes de données UNIQUEMENT après le titre, pour ne jamais
    # confondre avec les champs d'en-tête (ex: 'Temps de parcours 02:43.85' seul,
    # qui apparaît une première fois avant le titre, vs. la ligne du tableau).
    texte_apres_titre = '\n'.join(lines_full[idx_titre:])

    m_cumuls = re.search(r'Temps de parcours\s+([\d:.\s]+?)(?:\n|$)', texte_apres_titre)
    cumuls_sec = []
    if m_cumuls:
        cumuls = re.findall(r'(\d{2}):(\d{2})\.(\d{2})', m_cumuls.group(1))
        cumuls_sec = [round(int(mm) * 60 + int(ss) + int(cc) / 100, 2) for mm, ss, cc in cumuls]

    m_durees = re.search(r'Temps du tronçon\s+([\d:.\s]+?)(?:\n|$)', texte_apres_titre)
    durees_sec = []
    if m_durees:
        durees = re.findall(r'(\d{2}):(\d{2})\.(\d{2})', m_durees.group(1))
        durees_sec = [round(int(mm) * 60 + int(ss) + int(cc) / 100, 2) for mm, ss, cc in durees]

    m_vitesses = re.search(r'Vitesse moyenne\s+([\d,\.\s]+?)(?:\n|$)', texte_apres_titre)
    vitesses = []
    if m_vitesses:
        vitesses = [float(v.replace(',', '.')) for v in re.findall(r'(\d{1,3}(?:[,.]\d)?)', m_vitesses.group(1))]

    m_foulees = re.search(r'Nombre de foulées\s+([\d\s]+?)(?:\n|$)', texte_apres_titre)
    foulees = []
    if m_foulees:
        foulees = [int(v) for v in re.findall(r'\d+', m_foulees.group(1))]

    troncons = []
    for i in range(n):
        t = {'index': i + 1, 'label': labels[i]}
        if i < len(cumuls_sec):
            t['cumul_sec'] = cumuls_sec[i]
        if i < len(durees_sec):
            sec = durees_sec[i]
            t['temps_sec'] = sec
            t['temps'] = f'{int(sec // 60):02d}:{sec % 60:05.2f}'
        if i < len(vitesses):
            t['vitesse_kmh'] = vitesses[i]
        if i < len(foulees):
            t['foulees'] = foulees[i]
        troncons.append(t)

    return troncons


def _extraire_positions_page_individuelle(pdf_pages, idx_page, bloc_m=200):
    """
    Les positions en course sont affichées sous forme de graphique (nombres
    positionnés par coordonnées, pas du texte tabulaire), donc on les lit
    via les coordonnées des mots sur la page PDF correspondante.
    """
    if idx_page is None or idx_page >= len(pdf_pages):
        return []
    page = pdf_pages[idx_page]
    words = page.extract_words()
    pos_y = None
    for w in words:
        if w['text'] == 'Position':
            pos_y = w['top']
            break
    if not pos_y:
        return []
    y_max = pos_y + 100
    if bloc_m != 200:
        # Obstacles : un 2e tableau (par obstacle) suit juste en dessous, on
        # s'arrête à son titre 'Tronçons' pour ne pas mélanger les deux.
        below = [w['top'] for w in words if w['text'] == 'Tronçons' and w['top'] > pos_y]
        if below:
            y_max = min(y_max, min(below))
    nums = []
    for w in words:
        if pos_y - 150 < w['top'] < y_max:
            if re.match(r'^\d{1,2}$', w['text']) and int(w['text']) <= 20:
                nums.append((float(w['x0']), int(w['text'])))
    nums.sort(key=lambda t: t[0])
    return [n for _, n in nums]


def _parse_horse_header(page_text, cheval):
    """Champs d'en-tête de la page individuelle : redk, vitesse max, temps officiel, etc."""
    m = re.search(r'redk\s*:?\s*(\d+)[\u2019\'](\d{2})[\u201d\"](\d{2})', page_text)
    if m:
        mn, sec, cs = int(m.group(1)), int(m.group(2)), int(m.group(3))
        cheval['redk']     = f"{mn}'{sec:02d}\"{cs:02d}"
        cheval['redk_sec'] = round(mn * 60 + sec + cs / 100, 2)

    # Temps de parcours = temps officiel, affiché en haut de la page individuelle
    m = re.search(r"Temps de parcours\s+(\d{2}):(\d{2})\.(\d{2})", page_text)
    if m:
        mn, sec, cs = int(m.group(1)), int(m.group(2)), int(m.group(3))
        cheval['temps_officiel_sec'] = round(mn * 60 + sec + cs / 100, 2)
        cheval['temps_officiel']     = f"{mn}'{sec:02d}\"{cs:02d}"

    m = re.search(r"Rang d'arriv[ée]e\s+(\d+)", page_text) or \
        re.search(r"Temps de parcours\s+\d{2}:\d{2}\.\d{2}\s+\(rang\s+(\d+)", page_text)  # obstacles
    if m:
        cheval['position_arrivee'] = int(m.group(1))

    m = re.search(r'Vitesse maximale\s+([\d,\.]+)', page_text)
    if m:
        cheval['vitesse_max_kmh'] = float(m.group(1).replace(',', '.'))

    m = re.search(r'Vitesse moyenne\s+([\d,\.]+)\s*$', page_text, re.M)
    if m:
        cheval['vitesse_moyenne_kmh'] = float(m.group(1).replace(',', '.'))

    m = re.search(r'Tronçon le plus rapide\s+([\d:,.]+)\s+\(tronçon ([^)]+)\)', page_text)
    if m:
        cheval['troncon_plus_rapide']       = m.group(1)
        cheval['troncon_plus_rapide_label'] = m.group(2).strip()


# Pattern de ligne partant validé contre plusieurs PDFs réels (SAINT-CLOUD
# 12/06/2026 C1 et 19/09/2025 C7) :
#   '   2   KALKARA            1     72,79 00:10.72 00:16.42 5 00:28.78 5 ...'
_RE_LIGNE_CHEVAL = re.compile(
    r"^\s*(\d{1,2})\s+"
    r"([A-ZÀÂÉÈÊËÎÏÔÙÛÜŸŒÆ][A-ZÀÂÉÈÊËÎÏÔÙÛÜŸŒÆ' \-]*?)\s+"
    r"(\d{1,2})\s+"
    r"([\d,]+)\s+"
    r"(.*)$"
)

def _extraire_noms_partants(page1_text):
    """
    Extrait les noms de chevaux directement depuis la page 1 du PDF (liste des
    partants) — plus simple et plus fiable qu'un appel API séparé, puisque le
    PDF est de toute façon déjà téléchargé et contient cette info en clair.
    Exclut naturellement les non-partants (pas de temps cumulé sur leur ligne).
    """
    noms = []
    for line in page1_text.split('\n'):
        m = _RE_LIGNE_CHEVAL.match(line)
        if not m:
            continue
        nom   = m.group(2).strip()
        reste = m.group(5)
        if not re.search(r'\d{2}:\d{2}\.\d{2}', reste):
            continue
        if len(nom) < 2:
            continue
        if nom not in noms:
            noms.append(nom)
    return noms


def parse_pdf_complet(pdf_bytes, date_galop, code_hippo, num_reunion, num_course):
    """
    Parse un PDF entier et retourne (chevaux, troncons).
    Les partants sont identifiés directement depuis la page 1 du PDF.
    chevaux  : liste de dicts (1 par cheval)
    troncons : liste de dicts (1 par tronçon x cheval)
    """
    try:
        with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
            pages_text = [page.extract_text(layout=True) or '' for page in pdf.pages]
            pdf_pages  = pdf.pages

            # ── Infos générales de la course (page 1) ──
            # Priorité au titre de course (ex: 'C7 - PRIX ... - 2400m'), qui donne
            # la vraie distance totale sans ambiguïté.
            course_info = {}
            lines_p1 = pages_text[0].split('\n') if pages_text else []
            for line in lines_p1[:10]:
                m = re.search(r'-\s*(\d{3,5})\s*m\s*$', line.strip())
                if m:
                    course_info['distance_m'] = int(m.group(1))
                    break

            partants = _extraire_noms_partants(pages_text[0]) if pages_text else []

            # Redk du 1er (vainqueur) : valeur commune à toute la course, affichée
            # en en-tête de chaque page (page 1 et toutes les pages individuelles).
            # Contrairement au redk individuel (cheval['redk'], absent pour certains
            # chevaux), cette valeur est toujours présente — on l'extrait une fois
            # depuis la page 1 et on l'ajoute à chaque ligne cheval.
            redk_premier_sec = None
            redk_premier_str = ''
            if pages_text:
                m_redk1 = re.search(
                    r"Redk du 1er\s*:\s*(\d+)['\u2019](\d{2})[\u201d\u0022](\d{2})", pages_text[0])
                if m_redk1:
                    mn, sec, cs = int(m_redk1.group(1)), int(m_redk1.group(2)), int(m_redk1.group(3))
                    redk_premier_sec = round(mn * 60 + sec + cs / 100, 2)
                    redk_premier_str = f"{mn}'{sec:02d}\"{cs:02d}"

            chevaux_out  = []
            troncons_out = []

            for nom_pmu in partants:
                nom_up   = nom_pmu.upper().strip()
                mots_nom = [m for m in nom_up.split() if len(m) > 2] or [nom_up]

                # Retrouve la page individuelle de ce cheval, parmi pages_text[1:]
                idx_page_trouvee, cheval_page = None, None
                best_score = 0
                for i, page_text in enumerate(pages_text[1:], start=1):
                    score = sum(1 for m in mots_nom if m in page_text.upper())
                    if score > best_score and 'Vitesse moyenne' in page_text and 'Position' in page_text:
                        best_score = score
                        cheval_page = page_text
                        idx_page_trouvee = i

                if not cheval_page:
                    # Cheval non trouvé dans le PDF (ex: NON PARTANT, ou pas tracké) — on l'ignore
                    continue

                cheval = {
                    'nom':         nom_pmu,
                    'course_info': course_info,
                    'troncons':    [],
                }
                _parse_horse_header(cheval_page, cheval)

                troncons = _extraire_troncons_page_individuelle(cheval_page)
                cheval['troncons'] = troncons
                bloc_m = longueur_troncons(cheval_page)

                # Temps 600m / 200m final / départ 400m : définis sur des
                # tronçons de 200m (plat) ; sans objet en obstacles (1000m).
                if troncons and bloc_m == 200:
                    # Temps des 600 DERNIERS mètres = somme des 3 derniers tronçons de
                    # 200m (ex: '600m-400m' + '400m-200m' + '200m-ARR'), PAS juste le
                    # dernier tronçon — corrigé suite à un signalement réel : le dernier
                    # tronçon seul ne couvre que les 200 derniers mètres, pas 600.
                    derniers_3 = troncons[-3:]
                    if len(derniers_3) == 3 and all(t.get('temps_sec') is not None for t in derniers_3):
                        sec600 = round(sum(t['temps_sec'] for t in derniers_3), 2)
                        cheval['temps_600m_sec'] = sec600
                        cheval['temps_600m'] = f'{int(sec600 // 60):02d}:{sec600 % 60:05.2f}'

                    # Temps des 200 DERNIERS mètres = le dernier tronçon seul (nouvelle
                    # colonne séparée, demandée en plus de temps_600m).
                    if troncons[-1].get('temps_sec') is not None:
                        sec200 = troncons[-1]['temps_sec']
                        cheval['temps_200m_final_sec'] = sec200
                        cheval['temps_200m_final'] = f'{int(sec200 // 60):02d}:{sec200 % 60:05.2f}'

                # Positions en course (lecture par coordonnées, graphique)
                positions = _extraire_positions_page_individuelle(pdf_pages, idx_page_trouvee, bloc_m)
                if positions:
                    n = len(troncons)
                    positions_alignees = positions[-n:] if len(positions) >= n else positions
                    for i, t in enumerate(troncons):
                        if i < len(positions_alignees):
                            t['position'] = positions_alignees[i]
                    cheval['positions_troncons'] = positions_alignees

                if troncons:
                    cheval['vitesses_troncons'] = [t['vitesse_kmh'] for t in troncons if 'vitesse_kmh' in t]

                # DEP → 400m équivalent (premier tronçon de la liste)
                if troncons and bloc_m == 200:
                    t0 = troncons[0]
                    # BUG CORRIGÉ : le nombre dans le label (ex: 'DEP-2200m') est une
                    # distance-repère depuis l'arrivée, PAS la longueur du segment lui-
                    # même. Chaque tronçon mesure ~200m de long par construction (c'est
                    # le découpage standard du tracking), peu importe son label. Utiliser
                    # la distance du label donnait des vitesses aberrantes (>500 km/h)
                    # sur les courses longues, où le premier label affiche une grande
                    # distance-repère (ex: 2200m) pour un segment qui ne fait en réalité
                    # que ~200m.
                    dist_troncon = 200
                    temps_400_eq = None
                    if t0.get('temps_sec'):
                        temps_400_eq = round(t0['temps_sec'] * (400 / dist_troncon), 2)
                    cheval['dep_troncon'] = {
                        'label':        t0['label'],
                        'temps_sec':    t0.get('temps_sec'),
                        'dist_m':       dist_troncon,
                        'temps_400_eq': temps_400_eq,
                    }

                cheval['redk_premier_sec'] = redk_premier_sec
                cheval['redk_premier']     = redk_premier_str

                chevaux_out.append(_cheval_to_row(cheval, date_galop, code_hippo, num_reunion, num_course))
                for t in troncons:
                    troncons_out.append(_troncon_to_row(t, nom_pmu, date_galop, code_hippo, num_reunion, num_course))

        return chevaux_out, troncons_out

    except Exception as e:
        log.warning(f'parse_pdf_complet error {date_galop}/{code_hippo}/C{num_course:02d}: {e}')
        return [], []


# ── Conversion en lignes CSV ──────────────────────────────────

# Colonnes du CSV principal : UNIQUEMENT des données brutes extraites du PDF
# (+ terrain, qui vient du programme PMU). Aucun indicateur recalculé ici —
# ces calculs (profil_leader, score_finish, etc.) restent la responsabilité de l'app.
COLS_MAIN = [
    'date', 'code_hippo', 'num_reunion', 'num_course', 'nom_cheval',
    'position_arrivee',
    'temps_officiel', 'temps_officiel_sec',
    'temps_600m', 'temps_600m_sec',           # somme des 3 derniers tronçons de 200m
    'temps_200m_final', 'temps_200m_final_sec',  # dernier tronçon seul
    'redk', 'redk_sec',                       # redk individuel du cheval (absent pour certains)
    'redk_premier', 'redk_premier_sec',       # redk du 1er de la course (toujours présent)
    'vitesse_max_kmh', 'vitesse_moyenne_kmh',
    'distance_m',
    'terrain', 'penetrometre', 'type_piste',
    'troncon_plus_rapide', 'troncon_plus_rapide_label',
    'dep_label', 'dep_temps_sec', 'dep_dist_m', 'dep_temps_400_eq',
    'positions_troncons',   # JSON string — donnée brute lue (positions par tronçon)
    'vitesses_troncons',    # JSON string — donnée brute lue (vitesses par tronçon)
    'nb_troncons',
]

COLS_TRONC = [
    'date', 'code_hippo', 'num_reunion', 'num_course', 'nom_cheval',
    'troncon_index', 'troncon_label',
    'temps', 'temps_sec', 'cumul_sec',
    'vitesse_kmh', 'position', 'foulees',
]

def _cheval_to_row(cheval, date_galop, code_hippo, num_reunion, num_course):
    dep = cheval.get('dep_troncon', {})
    row = {
        'date':                  date_galop,
        'code_hippo':            code_hippo,
        'num_reunion':           num_reunion,
        'num_course':            num_course,
        'nom_cheval':            cheval.get('nom', ''),
        'position_arrivee':      cheval.get('position_arrivee', ''),
        'temps_officiel':        cheval.get('temps_officiel', ''),
        'temps_officiel_sec':    cheval.get('temps_officiel_sec', ''),
        'temps_600m':            cheval.get('temps_600m', ''),
        'temps_600m_sec':        cheval.get('temps_600m_sec', ''),
        'temps_200m_final':      cheval.get('temps_200m_final', ''),
        'temps_200m_final_sec':  cheval.get('temps_200m_final_sec', ''),
        'redk':                  cheval.get('redk', ''),
        'redk_sec':              cheval.get('redk_sec', ''),
        'redk_premier':          cheval.get('redk_premier', ''),
        'redk_premier_sec':      cheval.get('redk_premier_sec', ''),
        'vitesse_max_kmh':       cheval.get('vitesse_max_kmh', ''),
        'vitesse_moyenne_kmh':   cheval.get('vitesse_moyenne_kmh', ''),
        'distance_m':            cheval.get('course_info', {}).get('distance_m', ''),
        'terrain':               '',  # rempli par traiter_course() à partir du programme PMU
        'penetrometre':          '',  # idem
        'type_piste':            '',  # idem
        'troncon_plus_rapide':       cheval.get('troncon_plus_rapide', ''),
        'troncon_plus_rapide_label': cheval.get('troncon_plus_rapide_label', ''),
        'dep_label':         dep.get('label', ''),
        'dep_temps_sec':     dep.get('temps_sec', ''),
        'dep_dist_m':        dep.get('dist_m', ''),
        'dep_temps_400_eq':  dep.get('temps_400_eq', ''),
        'positions_troncons':json.dumps(cheval.get('positions_troncons', [])),
        'vitesses_troncons': json.dumps(cheval.get('vitesses_troncons', [])),
        'nb_troncons':       len(cheval.get('troncons', [])),
    }
    return row

# num_reunion transmis via parse_pdf_complet
def _troncon_to_row(t, nom_cheval, date_galop, code_hippo, num_reunion, num_course):
    return {
        'date':          date_galop,
        'code_hippo':    code_hippo,
        'num_reunion':   num_reunion,
        'num_course':    num_course,
        'nom_cheval':    nom_cheval,
        'troncon_index': t.get('index', ''),
        'troncon_label': t.get('label', ''),
        'temps':         t.get('temps', ''),
        'temps_sec':     t.get('temps_sec', ''),
        'cumul_sec':     t.get('cumul_sec', ''),
        'vitesse_kmh':   t.get('vitesse_kmh', ''),
        'position':      t.get('position', ''),
        'foulees':       t.get('foulees', ''),
    }
