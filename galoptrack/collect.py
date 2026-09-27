"""Collecte d'une ou plusieurs journées : tracking France Galop, participants
PMU (ex-enrichissement + cotes) et rapports définitifs.

Remplace les notebooks galoptrack_scraper, galoptrack_enrichissement,
galoptrack_cotes et galoptrack_rapports. Toutes les tables sont écrites en
« upsert par course » : relancer une journée ne crée jamais de doublon."""

import json
import logging
import time
from concurrent.futures import ThreadPoolExecutor

import requests

from . import config, pmu, tables
from .hippos import pdf_codes
from .tracking_pdf import parse_pdf_complet

log = logging.getLogger(__name__)

_pdf_session = requests.Session()
_pdf_session.headers.update(config.HEADERS)


# ── Tracking (PDF France Galop) ──────────────────────────────────────────────

def fetch_pdf(date_galop, code_hippo, num_course):
    """PDF de tracking, ou None (404 = pas publié / course non trackée)."""
    for code in pdf_codes(code_hippo):
        url = f"{config.TRACKING_BASE}/{date_galop}{code}{int(num_course):02d}_last_times_fr.pdf"
        try:
            r = _pdf_session.get(url, timeout=config.TIMEOUT_PDF)
        except requests.RequestException as e:
            log.warning("PDF %s : %s", url, e)
            continue
        if r.status_code == 200 and r.headers.get('content-type', '').startswith('application/pdf'):
            return r.content
    return None


def collect_tracking_course(d, course):
    date_galop = config.yyyymmdd(d)
    pdf = fetch_pdf(date_galop, course['code_hippo'], course['num_course'])
    if not pdf:
        return [], [], ('pdf_absent' if course.get('trackee') else None)
    chevaux, troncons = parse_pdf_complet(
        pdf, date_galop, course['code_hippo'], course['num_reunion'], int(course['num_course']))
    if not chevaux:
        return [], [], 'parse_vide'
    for c in chevaux:
        c['terrain'] = course.get('terrain', '')
        c['penetrometre'] = course.get('penetrometre_valeur', '')
        c['type_piste'] = course.get('type_piste_brut', '')
    return chevaux, troncons, None


# ── Participants (ex-enrichissement + cotes) ─────────────────────────────────

def participant_row(d, course, p):
    gains = p.get('gainsParticipant', {}) or {}
    poids_brut = p.get('handicapPoids')
    poids_kg = round(poids_brut / 10, 1) if poids_brut else ''
    return {
        'date': config.yyyymmdd(d),
        'code_hippo': course['code_hippo'],
        'num_reunion': course['num_reunion'],
        'num_course': course['num_course'],
        'nom_cheval': p.get('nom', '').strip().upper(),
        'id_cheval': p.get('idCheval', ''),
        'jockey': p.get('driver', ''),
        'entraineur': p.get('entraineur', ''),
        'proprietaire': p.get('proprietaire', ''),
        'eleveur': p.get('eleveur', ''),
        'age': p.get('age', ''),
        'sexe': p.get('sexe', ''),
        'numero_partant': p.get('numPmu', ''),
        'corde': p.get('placeCorde', ''),
        'poids_kg': poids_kg,
        'musique': p.get('musique', ''),
        'oeilleres': p.get('oeilleres', ''),
        'nom_pere': p.get('nomPere', ''),
        'nom_mere': p.get('nomMere', ''),
        'nb_courses_carriere': p.get('nombreCourses', ''),
        'nb_victoires_carriere': p.get('nombreVictoires', ''),
        'nb_places_carriere': p.get('nombrePlaces', ''),
        'gains_carriere': gains.get('gainsCarriere', ''),
        'handicap_valeur': p.get('handicapValeur', ''),
        'categorie_particularite': course.get('categorie_particularite', ''),
        'allocation_totale': course.get('allocation_totale', ''),
        'allocation_1er': course.get('allocation_1er', ''),
        'condition_age': course.get('condition_age', ''),
        'condition_sexe': course.get('condition_sexe', ''),
        'distance_course': course.get('distance_course', ''),
        'type_piste': course.get('type_piste', ''),
        'duree_course_ms': course.get('duree_course_ms', ''),
        'penetrometre': course.get('terrain', ''),
        'ordre_arrivee': p.get('ordreArrivee', '') if p.get('ordreArrivee') is not None else '',
        'ecart_precedent': (p.get('distanceChevalPrecedent') or {}).get('libelleCourt', ''),
        'commentaire_course': (p.get('commentaireApresCourse') or {}).get('texte', ''),
        'cote_directe': pmu.cote_directe(p),
    }


def collect_participants_course(d, course):
    participants = pmu.fetch_participants(d, course['num_reunion'], course['num_course'])
    rows = [participant_row(d, course, p) for p in participants if p.get('nom')]
    return rows, (None if rows else 'participants_vide')


# ── Rapports définitifs ──────────────────────────────────────────────────────

TYPES_PARI_CHEVAL = {'SIMPLE_GAGNANT', 'SIMPLE_PLACE'}


def collect_rapports_course(d, course, participants_rows):
    data = pmu.fetch_rapports_definitifs(d, course['num_reunion'], course['num_course'])
    if not data:
        return [], [], 'rapports_indisponibles'
    date_i = int(config.yyyymmdd(d))
    base = {'date': date_i, 'code_hippo': course['code_hippo'],
            'num_reunion': course['num_reunion'], 'num_course': course['num_course']}
    num_to_nom = {}
    for r in participants_rows:
        try:
            num_to_nom[int(r['numero_partant'])] = r['nom_cheval']
        except (TypeError, ValueError):
            pass

    par_numero, combines = {}, []
    for pari in data:
        type_pari = pari.get('typePari')
        for r in pari.get('rapports', []):
            combinaison = r.get('combinaison')
            if type_pari in TYPES_PARI_CHEVAL and combinaison is not None:
                try:
                    num = int(str(combinaison).split('-')[0])
                except ValueError:
                    continue
                entry = par_numero.setdefault(num, {})
                if type_pari == 'SIMPLE_GAGNANT':
                    entry['rapport_gagnant'] = r.get('dividendePourUnEuro')
                    entry['nb_gagnants_gagnant'] = r.get('nombreGagnants')
                else:
                    entry['rapport_place'] = r.get('dividendePourUnEuro')
                    entry['nb_gagnants_place'] = r.get('nombreGagnants')
            else:
                combines.append({
                    **base,
                    'type_pari': type_pari, 'libelle': r.get('libelle'),
                    'combinaison': combinaison,
                    'dividende_pour_un_euro': r.get('dividendePourUnEuro'),
                    'dividende_pour_mise_base': r.get('dividendePourUneMiseDeBase'),
                    'mise_base': pari.get('miseBase'),
                    'nombre_gagnants': r.get('nombreGagnants'),
                })
    cheval = [{**base, 'nom_cheval': num_to_nom.get(num), 'num_pmu': num, **vals}
              for num, vals in par_numero.items()]
    return cheval, combines, None


# ── Orchestration ────────────────────────────────────────────────────────────

def _collect_course(d, course, what):
    out = {'tracking': [], 'troncons': [], 'chevaux': [], 'rapports': [], 'rapports_combines': [],
           'errors': []}
    tag = f"{config.yyyymmdd(d)} {course['code_hippo']} R{course['num_reunion']}C{course['num_course']}"
    if 'participants' in what or 'rapports' in what:
        rows, err = collect_participants_course(d, course)
        if 'participants' in what:
            out['chevaux'] = rows
        if err:
            out['errors'].append(f"{tag} {err}")
        if 'rapports' in what:
            ch, comb, err = collect_rapports_course(d, course, rows)
            out['rapports'], out['rapports_combines'] = ch, comb
            if err:
                out['errors'].append(f"{tag} {err}")
    if 'tracking' in what:
        tr, tc, err = collect_tracking_course(d, course)
        out['tracking'], out['troncons'] = tr, tc
        if err:
            out['errors'].append(f"{tag} {err}")
    time.sleep(config.PAUSE_ENTRE_REQ)
    return out


def collect_days(storage, days, what=('tracking', 'participants', 'rapports')):
    """Collecte toutes les courses de plat françaises des jours donnés, puis
    écrit chaque table une seule fois. Retourne un résumé."""
    what = set(what)
    acc = {k: [] for k in tables.SCHEMAS}
    errors = []
    summary_days = {}
    for d in days:
        courses = pmu.list_courses_plat(d)
        with ThreadPoolExecutor(max_workers=config.WORKERS) as ex:
            results = list(ex.map(lambda c: _collect_course(d, c, what), courses))
        day = {'courses': len(courses)}
        for k in acc:
            rows = [r for res in results for r in res[k]]
            acc[k].extend(rows)
            day[k] = len(rows)
        errors.extend(e for res in results for e in res['errors'])
        summary_days[config.yyyymmdd(d)] = day
        log.info("%s : %s", config.yyyymmdd(d), day)

    written = {}
    for name, rows in acc.items():
        _, n = tables.upsert_races(storage, name, rows)
        written[name] = n
    summary = {'days': summary_days, 'written': written, 'errors': errors,
               'run_at': config.now_paris().isoformat(timespec='seconds')}
    if summary_days:
        first, last = min(summary_days), max(summary_days)
        storage.write_bytes(f"logs/collect_{first}_{last}.json",
                            json.dumps(summary, ensure_ascii=False, indent=1).encode('utf-8'),
                            content_type='application/json')
    return summary
