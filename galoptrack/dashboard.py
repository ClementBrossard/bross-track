"""Génération du dashboard HTML autonome (ex-notebook
galoptrack_dashboard_generator) : historique + courses du jour scorées par le
modèle + backtest enrichi des paris simulés.

Les cellules du notebook sont reprises fonction par fonction :
  6b -> add_today_races      6c -> score_today     6d -> enrich_backtest
  7  -> encode               8  -> render"""

import base64
import gzip
import json
import logging
from collections import Counter, defaultdict
from datetime import date
from pathlib import Path

import numpy as np
import pandas as pd

from . import config, model_store, pmu, tables
from .features import FEATURES, build_features
from .hippos import reunion_code_today
from .races import build_races

log = logging.getLogger(__name__)

TEMPLATE_PATH = Path(__file__).parent / "templates" / "dashboard.html"


# ── Cellule 6b : courses du jour via l'API PMU ───────────────────────────────

def parse_participant(p):
    """Participant PMU -> dict cheval du dashboard (structure API confirmée
    le 22/06/2026 : poids en dixièmes de kg, gains en centimes...)."""
    def sn(v):
        if v is None:
            return None
        s = str(v).strip()
        return s if s else None

    def si(v):
        try:
            return int(v)
        except (TypeError, ValueError):
            return None

    def sf(v, nd=1):
        try:
            v = round(float(v), nd)
            return int(v) if v == int(v) else v
        except (TypeError, ValueError):
            return None

    poids_kg = None
    for pfield in ('poidsConditionMonte', 'handicapPoids', 'poids'):
        v = p.get(pfield)
        if v is not None and str(v).strip() != '':
            try:
                vf = float(v)
                poids_kg = round(vf / 10, 1) if vf > 200 else round(vf, 1)
                break
            except (TypeError, ValueError):
                pass

    gains_obj = p.get('gainsParticipant') or {}
    gains = None
    if isinstance(gains_obj, dict) and gains_obj.get('gainsCarriere') is not None:
        try:
            gains = int(float(gains_obj['gainsCarriere']))
        except (TypeError, ValueError):
            pass

    cote_direct = cote_ref = None
    try:
        cote_direct = float((p.get('dernierRapportDirect') or {}).get('rapport') or 0) or None
    except (TypeError, ValueError):
        pass
    try:
        cote_ref = float((p.get('dernierRapportReference') or {}).get('rapport') or 0) or None
    except (TypeError, ValueError):
        pass

    return {
        'nom': (p.get('nom') or '').strip().upper(),
        'pa': None,
        'jk': sn(p.get('driver') or p.get('jockey')),
        'en': sn(p.get('entraineur') or p.get('trainer')),
        'ag': si(p.get('age')),
        'sx': sn(p.get('sexe')),
        'npart': si(p.get('numPmu')),
        'cd': si(p.get('placeCorde')),
        'pd': poids_kg,
        'mu': sn(p.get('musique')),
        'oe': sn(p.get('oeilleres')),
        'nc': si(p.get('nombreCourses')),
        'nv': si(p.get('nombreVictoires')),
        'np': si(p.get('nombrePlaces')),
        'gc': gains,
        'hv': sf(p.get('handicapValeur'), 1),
        'cote': cote_direct,
        'cote_ref': cote_ref,
        'to': None, 't6': None, 't2f': None, 't2p': None,
        'vm': None, 'vy': None, 'd4': None, 'v4p': None,
        'rel': None, 'dp': None, 'rg': None, 'pmn': None, 'pmx': None,
        'tr': [],
    }


def add_today_races(races_list, day: date):
    """Ajoute les courses de plat françaises du jour (flag 'today'). Une course
    déjà présente dans l'historique (résultat connu) n'est jamais ajoutée une
    seconde fois : ce serait une fuite du résultat vers sa propre prédiction."""
    prog = pmu.fetch_programme(day)
    today_int = int(config.yyyymmdd(day))
    if not prog:
        log.info("Courses du jour : pas de programme PMU")
        return 0
    existing = {(r['date'], r['hippo'], r['rnum'], r['num']) for r in races_list if r['date'] == today_int}
    added = skipped = 0
    reunions = prog.get('programme', {}).get('reunions', [])
    for reunion in [r for r in reunions if (r.get('pays', {}) or {}).get('code', '').upper() == 'FRA']:
        num_r = reunion['numOfficiel']
        code_h = reunion_code_today(reunion)
        for course in reunion.get('courses', []):
            disc = (course.get('specialite') or course.get('discipline') or '').upper()
            if 'PLAT' not in disc:
                continue
            num_c = course.get('numOrdre', 0)
            if not num_c:
                continue
            if (today_int, code_h, num_r, num_c) in existing:
                skipped += 1
                continue
            partants = pmu.fetch_participants(day, num_r, num_c)
            horses = [parse_participant(p) for p in partants if p.get('nom')]
            if not horses:
                continue
            penetro = course.get('penetrometre') or {}
            cond = course.get('conditions') or {}
            cond_age = cond.get('age') if isinstance(cond, dict) else None
            dist = course.get('distance') or None
            alloc = course.get('montantPrix') or None
            terrain = penetro.get('intitule') or None
            penetro_val = penetro.get('valeurMesure') or None
            piste = course.get('typePiste') or None
            cat = course.get('categorieParticularite') or None
            races_list.append({
                'id': f'TODAY_{code_h}_R{num_r}_{num_c}',
                'date': today_int, 'hippo': code_h, 'rnum': num_r, 'num': num_c,
                'dist': int(dist) if dist else None,
                'terrain': str(terrain).strip() if terrain else None,
                'penetro': str(penetro_val).strip() if penetro_val else None,
                'piste': str(piste).strip() if piste else None,
                'cat': str(cat).strip() if cat else None,
                'alloc': float(alloc) if alloc else None,
                'cond_age': str(cond_age).strip() if cond_age else None,
                'today': True,
                'horses': horses,
            })
            added += 1
    log.info("Courses du jour ajoutées : %d (%d déjà courues ignorées)", added, skipped)
    return added


# ── Cellule 6c : scoring des courses du jour ─────────────────────────────────

LABEL_FR = {
    'last_race_opp_elo_ahead_avg': 'Niveau moyen des chevaux devant lors de la dernière course',
    'last_race_opp_elo_ahead_max': 'Niveau du meilleur adversaire devant lors de la dernière course',
    'last_race_opp_elo_beaten_avg': 'Niveau moyen des chevaux battus lors de la dernière course',
    'last_race_opp_elo_beaten_max': 'Niveau du meilleur cheval battu lors de la dernière course',
    'mu_last1': 'Dernière position (musique)', 'h2h_net_field': 'Bilan face-à-face (champ du jour)',
    'jk_prior_winrate': '% victoires jockey', 'elo': 'Rating Elo', 'gain_moy': 'Gain moyen/course',
    'jk_prior_n': 'Expérience jockey', 'en_prior_n': 'Expérience entraîneur',
    'jk_prior_placerate': '% places jockey', 'alloc_ratio': 'Ratio montée en classe',
    'finish_kick_roll5': 'Vitesse finish (200m-arrivée)', 'alloc_delta': "Delta d'allocation",
    'days_since_last': 'Jours depuis dernière course', 'hippo': 'Hippodrome',
    'mu_avg5': 'Forme moy. 5 dernières courses', 'mu_wins5': 'Nb victoires / 5 dernières',
    'en_prior_winrate': '% victoires entraîneur', 'en_prior_placerate': '% places entraîneur',
    'accel_roll5': 'Accélération', 'gain_roll5': 'Gain de places (fin de course)',
    'h2h_wins_field': 'Victoires directes', 'h2h_losses_field': 'Défaites directes',
    'h2h_opp_met': 'Adversaires déjà croisés', 'hv': 'Valeur handicap', 'pd': 'Poids porté',
    'cat': 'Catégorie', 'alloc': 'Allocation du jour', 'cd': 'Corde de départ',
    'win_rate_career': '% victoires carrière', 'place_rate_career': '% places carrière',
    'terrain': 'Terrain', 'piste': 'Piste', 'dist': 'Distance', 'ag': 'Âge', 'sx': 'Sexe', 'oe': 'Œillères',
}

FEATURE_GROUPS = [
    ('Forme récente', ['mu_last1', 'mu_avg5', 'mu_wins5', 'win_rate_career', 'place_rate_career', 'gain_moy']),
    ('Rating & historique de rencontres', ['elo', 'h2h_wins_field', 'h2h_losses_field', 'h2h_net_field',
                                           'h2h_opp_met', 'last_race_opp_elo_ahead_avg',
                                           'last_race_opp_elo_ahead_max', 'last_race_opp_elo_beaten_avg',
                                           'last_race_opp_elo_beaten_max']),
    ('Tronçons (vitesses, tracking)', ['accel_roll5', 'gain_roll5', 'finish_kick_roll5']),
    ('Jockey & entraîneur', ['jk_prior_n', 'jk_prior_winrate', 'jk_prior_placerate',
                             'en_prior_n', 'en_prior_winrate', 'en_prior_placerate']),
    ('Classe & fraîcheur', ['alloc', 'alloc_delta', 'alloc_ratio', 'cat', 'days_since_last', 'hv']),
    ('Profil du cheval', ['ag', 'sx', 'oe']),
    ('Contexte de course', ['dist', 'terrain', 'piste', 'hippo']),
]


def model_meta(version):
    return {
        'version': version,
        'n_features': len(FEATURES),
        'feature_groups': [
            {'group': g, 'features': [{'key': f, 'label': LABEL_FR.get(f, f)} for f in feats if f in FEATURES]}
            for g, feats in FEATURE_GROUPS
        ],
        'excluded': [
            "La cote n'est jamais utilisée comme feature d'entraînement : elle n'existe pas dans l'historique (disponible uniquement pour la course du jour), et l'objectif du modèle est justement de la comparer à sa propre estimation (colonne \"Écart\"), pas de la reproduire.",
            "Corde de départ et poids porté ont été testés puis retirés (v7) : ils dégradaient légèrement le taux de réussite (23.3% → 24.4% sans eux sur la validation), probablement parce que le poids est déjà dérivé de la valeur handicap (redondant) et que la corde est un facteur globalement faible en plat français.",
        ],
        'methodology': [
            "Modèle : LightGBM en mode ranking (lambdarank), entraîné course par course (chaque course est un groupe, label 3=gagnant / 1=placé top 3 / 0=autre).",
            "Toutes les features sont calculées strictement AVANT la course concernée (aucune fuite de données) : stats jockey/entraîneur cumulées dans le temps, Elo mis à jour chronologiquement, moyennes glissantes des tronçons sur les 5 dernières courses trackées de la même catégorie de surface, etc.",
            "Validation : split temporel (entraînement sur les courses les plus anciennes, validation sur les plus récentes) pour éviter toute fuite du futur vers le passé.",
            "Calibration : régression isotonique sur le score brut du modèle pour obtenir une vraie probabilité de victoire, normalisée pour sommer à 100% par course.",
            "Interprétabilité : valeurs SHAP calculées pour chaque cheval de chaque course du jour, affichées dans l'onglet Modèle.",
            "Le modèle est réentraîné automatiquement chaque mois (pipeline GalopTrack) et appliqué ici uniquement aux courses du jour — jamais aux courses passées.",
        ],
    }


def score_today(races_list, labels_array, booster, calib):
    """Probabilité calibrée + détail SHAP pour chaque cheval des courses du
    jour. Retourne le nombre de chevaux scorés."""
    today_ids = {r['id'] for r in races_list if r.get('today')}
    if not today_ids:
        return 0
    fdf = build_features(races_list, labels_array)
    X = fdf[fdf['race_id'].isin(today_ids)].reset_index(drop=True)
    X['pred'] = booster.predict(X[FEATURES])
    X['calib'] = calib.predict(X['pred'])
    X['calib_norm'] = X.groupby('race_id')['calib'].transform(lambda s: s / s.sum())
    X['market_prob'] = 1 / pd.to_numeric(X['cote'], errors='coerce')
    X['market_prob'] = X.groupby('race_id')['market_prob'].transform(
        lambda s: s / s.sum() if s.notna().any() else s)

    # Valeurs SHAP (TreeSHAP natif LightGBM, identique à shap.TreeExplainer)
    contrib = booster.predict(X[FEATURES], pred_contrib=True)
    shap_vals, shap_base = contrib[:, :-1], contrib[:, -1]

    index = {r['id']: r for r in races_list}
    n = 0
    for i in range(len(X)):
        row = X.iloc[i]
        horse = next((h for h in index[row['race_id']]['horses'] if h['nom'] == row['nom']), None)
        if horse is None:
            continue
        vals = shap_vals[i]
        order = np.argsort(-np.abs(vals))[:8]
        horse['model_prob'] = None if pd.isna(row['calib_norm']) else round(float(row['calib_norm']) * 100, 2)
        horse['market_prob'] = None if pd.isna(row['market_prob']) else round(float(row['market_prob']) * 100, 2)
        horse['shap_base'] = round(float(shap_base[i]), 4)
        horse['shap_top'] = [{'f': LABEL_FR.get(FEATURES[j], FEATURES[j]), 'v': round(float(vals[j]), 4)}
                             for j in order]
        n += 1
    return n


# ── Cellule 6d : backtest enrichi des rapports PMU (paris simulés) ───────────

def _to_int_or_none(v):
    if v is None or (not isinstance(v, str) and pd.isna(v)):
        return None
    try:
        return int(v)
    except (ValueError, TypeError):
        try:
            return int(float(v))
        except (ValueError, TypeError):
            return None


def _to_float_or_none(v):
    if v is None or (not isinstance(v, str) and pd.isna(v)):
        return None
    try:
        return float(v)
    except (ValueError, TypeError):
        return None


def _horse_name(h):
    return h['nom'] if isinstance(h, dict) else h


def enrich_backtest(backtest, ch, rap, comb):
    """Rattache à chaque course du backtest ses numéros PMU, cotes et
    rapports définitifs (couplé placé, Quinté+ désordre). Les dividendes PMU
    sont stockés en centimes (/100)."""
    if backtest is None or comb is None or comb.empty:
        return backtest

    comb_cp = comb[comb['type_pari'] == 'COUPLE_PLACE']
    comb_q = comb[comb['type_pari'] == 'QUINTE_PLUS']

    quinte_races, quinte_races_reunion = set(), set()
    for _, rr in comb_q.iterrows():
        if pd.isna(rr['date']) or pd.isna(rr['num_course']):
            continue
        d, h, c = int(rr['date']), str(rr['code_hippo']), int(rr['num_course'])
        r = _to_int_or_none(rr.get('num_reunion'))
        quinte_races.add((d, h, c))
        if r is not None:
            quinte_races_reunion.add((d, h, r, c))

    comb_qd = comb_q[comb_q['libelle'].astype(str).str.contains('sordre', case=False, na=False)]
    desordre_by_race, desordre_by_race_reunion = defaultdict(list), defaultdict(list)
    for _, rr in comb_qd.iterrows():
        if pd.isna(rr['date']) or pd.isna(rr['num_course']):
            continue
        div = _to_float_or_none(rr.get('dividende_pour_un_euro'))
        if div is None:
            continue
        entry = {'combi': str(rr.get('combinaison')), 'div': round(div / 100.0, 2)}
        d, h, c = int(rr['date']), str(rr['code_hippo']), int(rr['num_course'])
        r = _to_int_or_none(rr.get('num_reunion'))
        desordre_by_race[(d, h, c)].append(entry)
        if r is not None:
            desordre_by_race_reunion[(d, h, r, c)].append(entry)

    # Index (date, hippo, nom) -> [(num_course, num_reunion, num_pmu, cote, source)]
    horse_index = defaultdict(list)
    if ch is not None and len(ch):
        has_cote = 'cote_directe' in ch.columns
        for rr in ch.itertuples(index=False):
            rr = rr._asdict()
            k = (int(rr['date']), str(rr['code_hippo']), str(rr['nom_cheval']).strip().upper())
            horse_index[k].append((int(rr['num_course']), _to_int_or_none(rr.get('num_reunion')),
                                   _to_int_or_none(rr.get('numero_partant')),
                                   _to_float_or_none(rr.get('cote_directe')) if has_cote else None,
                                   'chevaux'))
    if rap is not None and len(rap):
        for rr in rap.itertuples(index=False):
            rr = rr._asdict()
            if pd.isna(rr.get('date')) or pd.isna(rr.get('num_course')):
                continue
            k = (int(rr['date']), str(rr['code_hippo']), str(rr['nom_cheval']).strip().upper())
            horse_index[k].append((int(rr['num_course']), _to_int_or_none(rr.get('num_reunion')),
                                   _to_int_or_none(rr.get('num_pmu')), None, 'rapports'))

    couples_by_race, couples_by_race_reunion = defaultdict(dict), defaultdict(dict)
    mise_base_by_race = {}
    reunions_vues = defaultdict(set)
    for _, rr in comb_cp.iterrows():
        if pd.isna(rr['date']) or pd.isna(rr['num_course']):
            continue
        d, h, c = int(rr['date']), str(rr['code_hippo']), int(rr['num_course'])
        r = _to_int_or_none(rr.get('num_reunion'))
        reunions_vues[(d, h, c)].add(r)
        try:
            a, b = str(rr['combinaison']).split('-')
            pk = '-'.join(sorted([a.strip(), b.strip()], key=lambda x: int(x)))
        except Exception:
            continue
        if pd.isna(rr['dividende_pour_un_euro']):
            continue
        div_eur = float(rr['dividende_pour_un_euro']) / 100.0
        couples_by_race[(d, h, c)][pk] = div_eur
        if r is not None:
            couples_by_race_reunion[(d, h, r, c)][pk] = div_eur
        if pd.notna(rr.get('mise_base')):
            mise_base_by_race[(d, h, c)] = float(rr['mise_base']) / 100.0

    stats = Counter()
    warnings_list = []
    for race in backtest['races']:
        date_i, hippo = int(race['date']), str(race['hippo'])
        noms = list(dict.fromkeys(
            [str(_horse_name(h)).strip().upper() for h in race.get('pred_top10', [])] +
            [str(_horse_name(h)).strip().upper() for h in race.get('real_top5', [])]
        ))
        votes_course, votes_reunion = Counter(), Counter()
        for nom in noms:
            for (nc, nr, _n, _c, _s) in horse_index.get((date_i, hippo, nom), []):
                votes_course[nc] += 1
                if nr is not None:
                    votes_reunion[nr] += 1
        if not votes_course:
            stats['notfound'] += 1
            continue
        top_nc = votes_course.most_common(1)[0][0]
        top_nr = votes_reunion.most_common(1)[0][0] if votes_reunion else None
        if len(votes_course) > 1:
            stats['ambig'] += 1
            warnings_list.append(
                f"{date_i} / {hippo} : plusieurs num_course candidats parmi les chevaux "
                f"du top5 ({dict(votes_course)}) -- retenu C{top_nc} (majorite), a verifier.")
        if len(reunions_vues.get((date_i, hippo, top_nc), set())) > 1:
            warnings_list.append(
                f"{date_i} / {hippo} / C{top_nc} : plusieurs reunions trouvees pour ce "
                f"numero de course ce jour-la -- jointure potentiellement ambigue.")

        def _attach(lst):
            out = []
            for h in lst:
                nom = _horse_name(h)
                cands = [c for c in horse_index.get((date_i, hippo, str(nom).strip().upper()), [])
                         if c[0] == top_nc]
                match_npart = next((c for c in cands if c[2] is not None), None)
                match_cote = next((c for c in cands if c[3] is not None), None)
                if match_npart or match_cote:
                    obj = {'nom': nom}
                    if match_npart:
                        obj['npart'] = match_npart[2]
                    if match_cote:
                        obj['cote'] = match_cote[3]
                    out.append(obj)
                else:
                    out.append(nom)
            return out

        for nom in noms:
            cands = [c for c in horse_index.get((date_i, hippo, nom), []) if c[0] == top_nc]
            if any(c[3] is not None for c in cands):
                stats['cote_trouvee'] += 1
            if not any(c[2] is not None for c in cands):
                stats['npart_absent'] += 1

        race['pred_top10'] = _attach(race.get('pred_top10', []))
        race['real_top5'] = _attach(race.get('real_top5', []))
        race['num_course_identifie'] = top_nc
        if top_nr is not None:
            race['num_reunion_identifie'] = top_nr
        key_reunion = (date_i, hippo, top_nr, top_nc) if top_nr is not None else None

        if key_reunion is not None and quinte_races_reunion:
            race['is_quinte'] = key_reunion in quinte_races_reunion
        else:
            race['is_quinte'] = (date_i, hippo, top_nc) in quinte_races
        if race['is_quinte']:
            stats['quinte'] += 1
            dmap = (desordre_by_race_reunion.get(key_reunion) if key_reunion else None) \
                or desordre_by_race.get((date_i, hippo, top_nc))
            if dmap:
                race['quinte_desordre'] = dmap

        cmap = (couples_by_race_reunion.get(key_reunion) if key_reunion else None) \
            or couples_by_race.get((date_i, hippo, top_nc))
        if cmap:
            race['couples_place'] = cmap
            race['couple_mise_base'] = mise_base_by_race.get((date_i, hippo, top_nc), 1.0)
            stats['ok'] += 1
        else:
            stats['no_rapport'] += 1

    log.info("Enrichissement paris : %s", dict(stats))
    backtest['enrich_warnings'] = warnings_list
    return backtest


# ── Cellules 7 et 8 : encodage + HTML ────────────────────────────────────────

def render(output_data):
    raw = json.dumps(output_data, ensure_ascii=False, separators=(',', ':'))
    b64 = base64.b64encode(gzip.compress(raw.encode('utf-8'), compresslevel=9)).decode('ascii')
    return TEMPLATE_PATH.read_text(encoding='utf-8').replace('__DATA_B64__', b64)


def build(storage, day=None, with_today=True):
    """Construit le dashboard complet. Retourne (html, stats)."""
    day = day or config.today_paris()
    tr = tables.read_table(storage, 'tracking')
    tc = tables.read_table(storage, 'troncons')
    ch_raw = tables.read_table(storage, 'chevaux')
    races_list, labels_array, ch = build_races(tr, tc, ch_raw)
    stats = {'day': config.yyyymmdd(day), 'races_history': len(races_list)}

    stats['races_today'] = add_today_races(races_list, day) if with_today else 0

    version = model_store.current_version(storage)
    backtest, meta = None, None
    stats['model_version'] = version
    if version:
        booster, calib, backtest, _ = model_store.load_version(storage, version)
        stats['horses_scored'] = score_today(races_list, labels_array, booster, calib)
        meta = model_meta(version)
    else:
        log.warning("Aucun modèle en production (model/current.json absent) : onglet Modèle vide")

    if backtest is not None:
        rap = tables.read_table(storage, 'rapports')
        comb = tables.read_table(storage, 'rapports_combines')
        backtest = enrich_backtest(backtest, ch, rap, comb)

    output = {'labels': labels_array, 'races': races_list}
    if meta:
        output['model_meta'] = meta
    if backtest is not None:
        output['backtest'] = backtest
    html = render(output)
    stats['html_mb'] = round(len(html) / 1e6, 1)
    return html, stats


def publish(storage, html, stats):
    # Un seul fichier (~30 Mo) : en garder un par jour dépasserait vite les
    # 10 Go gratuits de R2.
    storage.write_bytes("dashboards/latest.html", html.encode('utf-8'), content_type='text/html; charset=utf-8')
    stats = {**stats, 'generated_at': config.now_paris().isoformat(timespec='seconds')}
    storage.write_bytes("dashboards/latest.json", json.dumps(stats).encode('utf-8'),
                        content_type='application/json')
