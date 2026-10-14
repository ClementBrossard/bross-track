import copy
import gzip
import base64
import io
import json
import pickle
import re

import numpy as np
import pandas as pd
import pytest

from galoptrack import dashboard, migrate, model_store, pmu, tables, train
from galoptrack.features import BASE_FEATURES, FEATURES, FIELD_FEATURES, build_features, opposition_features
from galoptrack.races import build_races, is_plat
from galoptrack.storage import LocalStorage

from .reference_notebook import notebook_features
from .synth import make_raw


@pytest.fixture(scope='module')
def raw():
    return make_raw()


@pytest.fixture(scope='module')
def built(raw):
    return build_races(raw['tracking'], raw['troncons'], raw['chevaux'])


def _store_tables(storage, raw, with_cotes=True):
    for name, df in raw.items():
        df = df.copy()
        if name == 'chevaux' and with_cotes:
            df['cote_directe'] = (df['numero_partant'] * 1.7 + 1).round(1)
        tables.write_table(storage, name, df)


# ── Parité des features avec le notebook d'entraînement ─────────────────────

def test_features_parity_with_notebook(built):
    races_list, labels_array, _ = built
    ref = notebook_features(copy.deepcopy(races_list), labels_array)
    new = build_features(copy.deepcopy(races_list), labels_array)
    assert len(ref) == len(new)
    key = ['race_id', 'nom']
    ref = ref.sort_values(key).reset_index(drop=True)
    new = new.sort_values(key).reset_index(drop=True)
    assert (ref[key].values == new[key].values).all()
    checked = 0
    # Les 37 variables du notebook (les variables « peloton » de la v9 n'y existent pas)
    for f in BASE_FEATURES:
        a, b = ref[f], new[f]
        if isinstance(a.dtype, pd.CategoricalDtype) or isinstance(b.dtype, pd.CategoricalDtype) or a.dtype == object:
            def norm(s):
                s = s.astype(object)
                return np.where(pd.isna(s), '<manquant>', s.astype(str))
            assert (norm(a) == norm(b)).all(), f
        else:
            np.testing.assert_allclose(a.astype(float).values, b.astype(float).values, rtol=1e-9,
                                       equal_nan=True, err_msg=f)
        checked += 1
    assert checked == len(BASE_FEATURES)
    assert all(new[f].notna().any() for f in FIELD_FEATURES)
    # Les features tronçons doivent être réellement calculées sur ces données
    assert new['accel_roll5'].notna().sum() > 100
    assert new['elo'].nunique() > 50


def test_today_race_has_no_leak(built):
    """Une course 'today' ne modifie pas les features de l'historique."""
    races_list, labels_array, _ = built
    base = build_features(copy.deepcopy(races_list), labels_array)
    rl = copy.deepcopy(races_list)
    last = max(r['date'] for r in rl)
    src = [r for r in rl if r['date'] == last][0]
    today = copy.deepcopy(src)
    today.update({'id': 'TODAY_X_R9_1', 'today': True})
    for h in today['horses']:
        h['pa'] = None
    rl.append(today)
    f2 = build_features(rl, labels_array)
    hist = f2[~f2['is_today']].sort_values(['race_id', 'nom']).reset_index(drop=True)
    base = base.sort_values(['race_id', 'nom']).reset_index(drop=True)
    np.testing.assert_allclose(hist['elo'].values, base['elo'].values)
    t = f2[f2['is_today']]
    assert len(t) == len(today['horses'])
    # Le jockey stats du jour incluent la course réelle de la même date (passée)
    assert t['h2h_opp_met'].sum() >= 0


# ── Calibration ─────────────────────────────────────────────────────────────

def test_calibrator_matches_isotonic():
    from sklearn.isotonic import IsotonicRegression
    rng = np.random.default_rng(1)
    x = rng.normal(size=2000)
    y = (rng.random(2000) < 1 / (1 + np.exp(-2 * x))).astype(int)
    iso = IsotonicRegression(out_of_bounds='clip', y_min=0.0001, y_max=0.9999).fit(x, y)
    cal = model_store.Calibrator.from_isotonic(iso)
    cal2 = model_store.Calibrator.from_json(json.loads(json.dumps(cal.to_json())))
    xt = np.concatenate([rng.normal(size=500) * 2, [-10, 10]])
    np.testing.assert_allclose(cal2.predict(xt), iso.predict(xt), atol=1e-12)


# ── Upsert par course ───────────────────────────────────────────────────────

def test_upsert_replaces_race_without_duplicates(tmp_path):
    st = LocalStorage(tmp_path)
    row = {c: '' for c in tables.COLS_CHEVAUX}
    row.update({'date': '20260920', 'code_hippo': 'CHA', 'num_reunion': 1, 'num_course': 1})
    pre = [dict(row, nom_cheval='A', ordre_arrivee=''), dict(row, nom_cheval='B', ordre_arrivee='')]
    other = [dict(row, num_course=2, nom_cheval='C', ordre_arrivee=3)]
    tables.upsert_races(st, 'chevaux', pre + other)
    post = [dict(row, nom_cheval='A', ordre_arrivee=2), dict(row, nom_cheval='B', ordre_arrivee=1)]
    df, n = tables.upsert_races(st, 'chevaux', post)
    df = tables.read_table(st, 'chevaux')
    assert n == 2 and len(df) == 3
    assert sorted(df.loc[df['num_course'] == 1, 'ordre_arrivee'].tolist()) == [1, 2]
    assert df.loc[df['num_course'] == 2, 'nom_cheval'].tolist() == ['C']
    tables.upsert_races(st, 'chevaux', post)
    assert len(tables.read_table(st, 'chevaux')) == 3


# ── Entraînement + dashboard de bout en bout ────────────────────────────────

def test_train_and_dashboard_end_to_end(tmp_path, raw, monkeypatch):
    st = LocalStorage(tmp_path)
    _store_tables(st, raw)
    monkeypatch.setattr(train.config, 'MIN_HIT_RATE_TOP1', 0.0)
    monkeypatch.setattr(train.config, 'MAX_DROP_VS_TEMOIN', 1.0)
    # Modèle à 41 variables (37 + peloton) pour tester le témoin et le scoring mixte
    monkeypatch.setattr(train, 'FEATURES', BASE_FEATURES + FIELD_FEATURES)
    meta = train.run(st)
    assert meta['promoted'] and model_store.current_version(st) == 'v1'
    # Témoin v8 entraîné sur la même période, mesures « top 3 »
    assert meta['temoin_v8']['n_valid_races'] == meta['metrics']['n_valid_races']
    assert 0 <= meta['metrics']['top3_in_top3'] <= 3 and meta['metrics']['label_mode'] == 'top3'
    m = meta['metrics']
    assert m['n_valid_races'] > 20
    # Les chevaux ont une vraie « ability » cachée : le modèle doit battre le hasard
    assert m['hit_rate_top1'] > m['baseline_random']

    # Course du jour simulée (sans appel PMU)
    races_list, labels_array, ch = build_races(*(tables.read_table(st, n) for n in ('tracking', 'troncons', 'chevaux')))
    last = max(r['date'] for r in races_list)
    today = copy.deepcopy([r for r in races_list if r['date'] == last][0])
    from datetime import datetime, timedelta
    next_day = int((datetime.strptime(str(last), '%Y%m%d') + timedelta(days=1)).strftime('%Y%m%d'))
    today.update({'id': 'TODAY_CHA_R1_1', 'today': True, 'date': next_day})
    for i, h in enumerate(today['horses']):
        h.update({'pa': None, 'tr': [], 'cote': 2.0 + i})
    races_list.append(today)
    # Même course en obstacles : jamais scorée (modèle de plat)
    obst = copy.deepcopy(today)
    obst.update({'id': 'TODAY_AUT_R2_1', 'hippo': 'AUT', 'rnum': 2, 'disc': 'HAIES'})
    races_list.append(obst)
    booster, calib, backtest, _ = model_store.load_version(st, 'v1')
    n = dashboard.score_today(races_list, labels_array, booster, calib)
    assert n == len(today['horses'])
    assert not any('model_prob' in h for h in obst['horses'])
    # « Peloton déjà dépassé » renseigné, avec les meilleurs chevaux battus
    assert any(h['opp']['pct'] is not None and h['opp']['best'] for h in today['horses'])
    # Un modèle à 37 variables (v8) se score toujours avec ses propres variables
    fdf_all = build_features([r for r in races_list if r.get('disc', 'PLAT') == 'PLAT'], labels_array)
    m37, cal37, _, _ = train.train_model(fdf_all, features=BASE_FEATURES)
    for h in today['horses']:
        h.pop('model_prob', None)
    assert dashboard.score_today(races_list, labels_array, m37, cal37) == len(today['horses'])
    assert m37.feature_name() == BASE_FEATURES
    probs = [h['model_prob'] for h in today['horses']]
    assert abs(sum(probs) - 100) < 0.5
    assert all(len(h['shap_top']) == 8 for h in today['horses'])

    bt = dashboard.enrich_backtest(backtest, ch, tables.read_table(st, 'rapports'),
                                   tables.read_table(st, 'rapports_combines'))
    assert any('couples_place' in r for r in bt['races'])
    assert any(isinstance(h, dict) and h.get('cote') for r in bt['races'] for h in r['pred_top10'])

    html, stats = dashboard.build(st, with_today=False)
    assert '__DATA_B64__' not in html
    b64 = re.search(r'const DATA_B64 = "([^"]+)"', html).group(1)
    data = json.loads(gzip.decompress(base64.b64decode(b64)))
    assert data['model_meta']['version'] == 'v1'
    assert data['backtest']['summary']['n_courses'] > 0
    assert len(data['races']) == stats['races_history']


# ── Migration ───────────────────────────────────────────────────────────────

def test_migration(tmp_path, raw, monkeypatch):
    st = LocalStorage(tmp_path)
    ch = raw['chevaux']
    # doublons (bug enrichissement), ligne corrompue, première copie sans arrivée
    dup = ch.head(500).copy()
    first = ch.head(50).copy()
    first['ordre_arrivee'] = np.nan
    bad = ch.head(1).copy()
    bad['date'] = 'KALKARA'
    ch_raw = pd.concat([first, ch, dup, bad], ignore_index=True)
    # cotes_data avec l'ancien code Lyon La Soie et un code hippo différent
    cot = ch[['date', 'code_hippo', 'num_course', 'nom_cheval']].copy()
    cot['code_hippo'] = 'XXX'
    cot['cote_directe'] = 3.5
    # rapports avec code PMU brut + réunion recalculée
    rap = raw['rapports'].copy()
    rap['code_hippo'] = rap['code_hippo'].map({'CHA': 'CHN', 'DEA': 'DEA', 'LPA': 'PAR', 'SAI': 'STC'})
    rap['num_reunion'] = rap['num_reunion'] + 10
    comb = raw['rapports_combines'].copy()
    comb['code_hippo'] = comb['code_hippo'].map({'CHA': 'CHN', 'DEA': 'DEA', 'LPA': 'PAR', 'SAI': 'STC'})
    comb['num_reunion'] = comb['num_reunion'] + 10

    def put(name, df):
        st.write_bytes(f"raw/{name}.csv.gz", gzip.compress(df.to_csv(index=False).encode()))
    put('tracking_data', raw['tracking'])
    put('troncons_data', raw['troncons'])
    put('chevaux_data', ch_raw)
    put('cotes_data', cot)
    put('rapports_data', rap)
    put('rapports_combines_data', comb)

    # modèle « Colab » v7
    fdf = build_features(*build_races(raw['tracking'], raw['troncons'], raw['chevaux'])[:2])
    model, calib, backtest, _ = train.train_model(fdf)
    from sklearn.isotonic import IsotonicRegression
    iso = IsotonicRegression(out_of_bounds='clip').fit([0, 1, 2], [0.1, 0.2, 0.5])
    st.write_bytes('raw/model/race_model_v7.txt', model.model_to_string().encode())
    st.write_bytes('raw/model/iso_calib_v7.pkl', pickle.dumps(iso))
    st.write_bytes('raw/model/backtest_v7.json', json.dumps(backtest).encode())

    report, md = migrate.run(st)
    assert report['chevaux']['invalid_rows_removed'] == 1
    assert report['chevaux']['duplicates_removed'] == 550
    assert report['chevaux']['rows_gaining_result_vs_keep_first'] == 50
    clean = tables.read_table(st, 'chevaux')
    assert len(clean) == len(ch)
    assert clean['ordre_arrivee'].notna().all()
    assert (clean['cote_directe'] == 3.5).all()
    r2 = tables.read_table(st, 'rapports')
    assert set(r2['code_hippo']) <= {'CHA', 'DEA', 'LPA', 'SAI'}
    assert (r2['num_reunion'] < 10).all()
    c2 = tables.read_table(st, 'rapports_combines')
    assert set(c2['code_hippo']) <= {'CHA', 'DEA', 'LPA', 'SAI'}
    assert model_store.current_version(st) == 'v7'
    _, cal, bt, _ = model_store.load_version(st, 'v7')
    np.testing.assert_allclose(cal.predict([0.5, 5]), iso.predict([0.5, 5]))
    assert 'Rapport de migration' in md

    # Après migration, le dashboard se construit sur les tables migrées
    html, stats = dashboard.build(st, with_today=False)
    assert stats['model_version'] == 'v7'


def test_merge_cotes_tolerates_other_column_names(raw):
    ch = raw['chevaux'].head(200).copy()
    cot = ch[['date', 'code_hippo', 'num_course', 'nom_cheval']].copy()
    cot['cote'] = '4,5'
    rep = {}
    out = migrate.merge_cotes(ch, cot, rep)
    assert rep['cotes_data']['cote_column_used'] == 'cote'
    assert (out['cote_directe'] == 4.5).all()
    # fichier inexploitable : pas d'exception, migration non bloquée
    rep = {}
    out = migrate.merge_cotes(ch, cot.drop(columns=['cote']), rep)
    assert out['cote_directe'].isna().all()
    assert rep['cotes_data']['status'].startswith('ignoré')


def test_health_detects_gaps_and_renders(tmp_path, raw):
    from datetime import datetime, timedelta
    from galoptrack import health
    st = LocalStorage(tmp_path)
    tr = raw['tracking'].copy()
    last = int(tr['date'].max())
    # Simule le bug du Drive : les 5 derniers jours de tracking perdus, tronçons gardés
    cut = int((datetime.strptime(str(last), '%Y%m%d') - timedelta(days=5)).strftime('%Y%m%d'))
    raw2 = dict(raw, tracking=tr[tr['date'] <= cut])
    _store_tables(st, raw2)
    today = datetime.strptime(str(last), '%Y%m%d').date() + timedelta(days=1)
    h = health.compute(st, today=today, dashboard_stats={'races_today': 0})
    assert h['coherence']['troncons_sans_tracking_30j'] > 0
    assert h['status'] in ('warn', 'error')
    msgs = ' '.join(a['msg'] for a in h['alerts'])
    assert 'pas de résumé tracking' in msgs and 'aucun PDF de tracking' in msgs
    assert any(a['msg'].startswith('« tracking » en retard') for a in h['alerts'])
    assert any('Aucun modèle' in a['msg'] for a in h['alerts'])
    assert len(h['recent_days']) == health.RECENT_DAYS
    health.publish(st, h)
    page = st.read_bytes('dashboards/sante.html').decode()
    assert 'Santé des données' in page and 'sans résumé tracking' in page
    badged = health.inject_badge('<html><body>x</body></html>', h)
    assert 'href="/sante"' in badged and 'alerte' in badged


def test_finishing_position_is_integer_text(raw):
    from galoptrack.races import norm_pa
    assert norm_pa(1.0) == '1' and norm_pa('3.0') == '3' and norm_pa(12) == '12'
    assert norm_pa(float('nan')) is None and norm_pa('') is None and norm_pa('DAI') == 'DAI'
    tr = raw['tracking'].copy()
    tr['position_arrivee'] = tr['position_arrivee'].astype(float)   # « 1.0 » comme en production
    races_list, _, _ = build_races(tr, raw['troncons'], raw['chevaux'])
    tracked = [r for r in races_list if r.get('tracked')]
    assert tracked and all(any(h['pa'] == '1' for h in r['horses']) for r in tracked)


def test_daily_backup_slot_skips_when_already_done(tmp_path, monkeypatch, capsys):
    import json as _json
    from galoptrack import __main__ as cli, collect as _collect, config as _config, health
    from datetime import date
    st = LocalStorage(tmp_path)
    monkeypatch.setattr(_config, 'today_paris', lambda: date(2026, 9, 28))
    monkeypatch.setattr(cli.config, 'today_paris', lambda: date(2026, 9, 28))
    calls = []
    monkeypatch.setattr(_collect, 'collect_days', lambda *a, **k: calls.append(1) or
                        {'days': {}, 'written': {}, 'errors': []})
    monkeypatch.setattr(cli.dashboard, 'build', lambda *a, **k: ('<html><body></body></html>', {'races_today': 0}))
    monkeypatch.setattr(cli, '_publish_with_health', lambda *a, **k: None)

    class A:  # arguments de la ligne de commande
        if_needed = True
    st.write_bytes(cli.DAILY_MARKER, _json.dumps({'day': 20260927}).encode())
    cli.cmd_daily(st, A)                      # marqueur d'hier -> la collecte tourne
    assert len(calls) == 1
    assert _json.loads(st.read_bytes(cli.DAILY_MARKER))['day'] == 20260928
    cli.cmd_daily(st, A)                      # créneau de secours -> rien à faire
    assert len(calls) == 1
    A.if_needed = False
    cli.cmd_daily(st, A)                      # lancement manuel -> toujours exécuté
    assert len(calls) == 2


def test_health_alerts_when_daily_did_not_run(tmp_path, raw):
    import json as _json
    from datetime import date
    from galoptrack import health
    st = LocalStorage(tmp_path)
    _store_tables(st, raw)
    st.write_bytes('logs/daily_last.json', _json.dumps({'day': 20260926}).encode())
    h = health.compute(st, today=date(2026, 9, 28))
    assert any(a['level'] == 'error' and "n'a pas tourné" in a['msg'] for a in h['alerts'])
    assert 'gt-health-badge' in health.badge_html(h)


# ── Obstacles ───────────────────────────────────────────────────────────────

def test_discipline_galop():
    assert pmu.discipline_galop({'specialite': 'PLAT', 'discipline': 'PLAT'}) == 'PLAT'
    assert pmu.discipline_galop({'specialite': 'OBSTACLE', 'discipline': 'HAIE'}) == 'HAIES'
    assert pmu.discipline_galop({'discipline': 'STEEPLECHASE'}) == 'STEEPLE'
    assert pmu.discipline_galop({'discipline': 'CROSS_COUNTRY'}) == 'CROSS'
    assert pmu.discipline_galop({'specialite': 'OBSTACLE'}) == 'OBSTACLE'
    assert pmu.discipline_galop({'specialite': 'TROT_ATTELE', 'discipline': 'ATTELE'}) is None
    assert pmu.wanted('HAIES', None) and pmu.wanted('HAIES', {'obstacle'})
    assert not pmu.wanted('HAIES', {'plat'}) and not pmu.wanted(None, None)


def test_list_courses_galop_filters(monkeypatch):
    def course(n, spec, disc):
        return {'numOrdre': n, 'specialite': spec, 'discipline': disc, 'statut': 'FIN_COURSE'}
    prog = {'programme': {'reunions': [
        {'numOfficiel': 1, 'pays': {'code': 'FRA'}, 'hippodrome': {'libelleLong': 'AUTEUIL'},
         'courses': [course(1, 'OBSTACLE', 'HAIE'), course(2, 'OBSTACLE', 'STEEPLECHASE'), course(3, 'PLAT', 'PLAT')]},
        {'numOfficiel': 2, 'pays': {'code': 'FRA'}, 'hippodrome': {'libelleLong': 'CHANTILLY'},
         'courses': [course(1, 'PLAT', 'PLAT')]},
        {'numOfficiel': 3, 'pays': {'code': 'FRA'}, 'hippodrome': {'libelleLong': 'VINCENNES'},
         'courses': [course(1, 'TROT_ATTELE', 'ATTELE')]},
    ]}}
    monkeypatch.setattr(pmu, 'fetch_programme', lambda d: prog)
    from datetime import date
    allc = pmu.list_courses_galop(date(2026, 9, 26))
    assert [(c['code_hippo'], c['num_course'], c['discipline']) for c in allc] == [
        ('AUT', 1, 'HAIES'), ('AUT', 2, 'STEEPLE'), ('AUT', 3, 'PLAT'), ('CHA', 1, 'PLAT')]
    assert [c['code_hippo'] for c in pmu.list_courses_plat(date(2026, 9, 26))] == ['AUT', 'CHA']
    assert len(pmu.list_courses_galop(date(2026, 9, 26), {'obstacle'})) == 2


def test_obstacles_stay_out_of_model_features(raw):
    """Des courses d'obstacles ajoutées aux tables ne changent pas les
    features du plat (entraînement et scoring filtrent le plat)."""
    ch = raw['chevaux'].copy()
    tr = raw['tracking'].copy()
    last = pd.to_numeric(ch['date']).max()
    obst = ch[pd.to_numeric(ch['date']) == last].copy()
    obst['code_hippo'] = 'AUT'
    obst['num_reunion'] = 9
    obst['discipline'] = 'HAIES'
    obst_tr = tr[pd.to_numeric(tr['date']) == last].copy()
    obst_tr['code_hippo'] = 'AUT'
    obst_tr['num_reunion'] = 9
    obst_tr['discipline'] = 'HAIES'
    ch2 = pd.concat([ch, obst], ignore_index=True)
    tr2 = pd.concat([tr, obst_tr], ignore_index=True)

    races, labels, _ = build_races(tr2, raw['troncons'], ch2)
    discs = {r['disc'] for r in races}
    assert discs == {'PLAT', 'HAIES'}
    assert all(r['disc'] == 'HAIES' for r in races if r['hippo'] == 'AUT')

    ref_races, ref_labels, _ = build_races(raw['tracking'], raw['troncons'], raw['chevaux'])
    a = build_features([r for r in races if is_plat(r)], labels)
    b = build_features(ref_races, ref_labels)
    cols = [c for c in FEATURES if c not in ('terrain', 'piste', 'hippo', 'cat', 'sx', 'oe')]
    pd.testing.assert_frame_equal(a[cols].reset_index(drop=True), b[cols].reset_index(drop=True))


# Page individuelle d'un PDF d'obstacles réel (Auteuil 26/09/2026 C1), texte pdfplumber
PAGE_OBSTACLE = """\
                                                  Statistiques Tracking
                                                       AUTEUIL
                          C1 - PRIX DUC D'ALBUQUERQUE - CHALLENGE DE L'OBSTACLE EQUINAXY - 4400m
                                                   Redk du 1er: 1'18"46
   Cheval        HOKUSAI VALLIS       Distance parcourue 4463,11m             Vitesse moyenne 46,5
   Jockey        J. MAJORCRYK         Tronçon le plus rapide                  Réduction km  1'18"46
   Temps de parcours 05:45.23 (rang 1, redk : 1'18"46) Vitesse maximale 54,32 (tronçon DEP - 4000m) Nombre de foulées 704
                                                     Données de tracking
                       DEP                4000m               3000m               2000m              1000m
    Tronçons de 1000m  4000m              3000m               2000m               1000m               ARR
    Temps de parcours 00:32.50            01:53.57           03:13.26            04:30.60            05:45.23
     Temps du tronçon 00:32.50            01:21.06           01:19.69            01:17.34            01:14.62
     Vitesse moyenne   48,7                43,9                46                 46,6                49
    Nombre de foulées   66                 167                 159                158                 154
       Position         2                   2                  2                   2                   1
       Tronçons
    Temps de parcours 00:17.27 00:33.58 00:47.92 00:51.95 01:19.03 01:37.88 01:50.37 02:24.43
     Temps du tronçon 00:17.27 00:16.30 00:14.33 00:04.03 00:27.07 00:18.84 00:12.49 00:34.05
     Vitesse moyenne  49  43,7  35,7 43,1 48,9 46,7 42,3 44,1
"""


def test_obstacle_pdf_page_parsing():
    from galoptrack import tracking_pdf as tp
    assert tp.longueur_troncons(PAGE_OBSTACLE) == 1000
    tr = tp._extraire_troncons_page_individuelle(PAGE_OBSTACLE)
    assert [t['label'] for t in tr] == ['DEP-4000m', '4000m-3000m', '3000m-2000m', '2000m-1000m', '1000m-ARR']
    assert [t['vitesse_kmh'] for t in tr] == [48.7, 43.9, 46.0, 46.6, 49.0]
    assert [t['temps_sec'] for t in tr] == [32.5, 81.06, 79.69, 77.34, 74.62]
    assert tr[-1]['cumul_sec'] == 345.23 and tr[1]['foulees'] == 167
    cheval = {}
    tp._parse_horse_header(PAGE_OBSTACLE, cheval)
    assert cheval['position_arrivee'] == 1
    assert cheval['temps_officiel_sec'] == 345.23
    assert cheval['vitesse_moyenne_kmh'] == 46.5 and cheval['vitesse_max_kmh'] == 54.32


def test_backup_and_restore_tables(tmp_path, raw):
    from galoptrack import __main__ as cli
    st = LocalStorage(tmp_path)
    _store_tables(st, raw)
    before = st.read_bytes(tables.table_key('chevaux'))
    name = cli.backup(st, 'test')
    assert cli.list_backups(st) == [name]
    tables.write_table(st, 'chevaux', raw['chevaux'].head(3))
    assert cli.restore(st, name) >= len(tables.SCHEMAS)
    assert st.read_bytes(tables.table_key('chevaux')) == before
    with pytest.raises(SystemExit):
        cli.restore(st, 'inexistante')


# ── Poids ───────────────────────────────────────────────────────────────────

def test_weight_info_for_today_races():
    hist = {'id': 'H', 'date': 20261001, 'hippo': 'CHA', 'horses': [{'nom': 'A', 'pd': 58}, {'nom': 'B', 'pd': 55}]}
    obst = {'id': 'O', 'date': 20261005, 'hippo': 'AUT', 'disc': 'HAIES', 'horses': [{'nom': 'A', 'pd': 67}]}
    today = {'id': 'T', 'date': 20261010, 'hippo': 'CHA', 'today': True, 'horses': [
        {'nom': 'A', 'pd': 54, 'pdh': 56},   # décharge de 2 kg
        {'nom': 'B', 'pd': 57, 'pdh': 57},
        {'nom': 'C', 'pd': None, 'pdh': None},
    ]}
    assert dashboard.add_weight_info([hist, obst, today]) == 2
    a, b, c = today['horses']
    # Comparaison au plat (pas à la course d'obstacles), poids avant décharge
    assert a['w'] == {'kg': 56, 'last': 58.0, 'delta': -2.0, 'field': -0.5, 'decharge': 2}
    assert b['w'] == {'kg': 57, 'last': 55.0, 'delta': 2.0, 'field': 0.5}
    assert 'w' not in c


def test_weight_features_use_only_the_past(built):
    from galoptrack import experiment
    races_list, labels_array, _ = built
    fdf = experiment.add_weight_features(build_features(copy.deepcopy(races_list), labels_array))
    for f in experiment.WEIGHT_FEATURES:
        assert f in fdf and fdf[f].notna().any()
    # Écart avec la course précédente = différence de poids entre deux sorties consécutives
    g = fdf[fdf['nom'] == fdf['nom'].iloc[-1]]
    w = pd.to_numeric(g['pd'], errors='coerce').to_numpy()
    np.testing.assert_allclose(g['pd_delta_last'].to_numpy()[1:], w[1:] - w[:-1])
    # La course précédente d'un cheval n'est jamais celle du jour même
    first = fdf.groupby('nom').head(1)
    assert first['pd_delta_last'].isna().all() and first['last_race_beaten_pd_diff'].isna().all()


def test_opposition_features_use_only_the_past(built):
    from galoptrack import experiment
    races_list, labels_array, _ = built
    fdf = opposition_features(build_features(copy.deepcopy(races_list), labels_array))
    extra = ['field_avg', 'field_max', 'field_top3', 'beaten_vs_field_90', 'beaten_vs_top3_90',
             'field_below_bmax_90', 'field_below_bavg_90']
    for f in experiment.opposition_set(90) + experiment.opposition_set(180) + extra:
        assert f in fdf and fdf[f].notna().any(), f
    # Première course d'un cheval : aucun historique d'adversaires
    first = fdf.sort_values(['date', 'is_today']).groupby('nom').head(1)
    assert first['opp_avg_90'].isna().all() and first['beaten_max_180'].isna().all()
    # Peloton du jour : moyenne des Elo des rivaux (le cheval lui-même exclu)
    r = fdf[fdf['race_id'] == fdf['race_id'].iloc[0]]
    i = r.index[0]
    assert np.isclose(fdf.at[i, 'field_avg'], r['elo'].drop(i).mean())
    # 6 mois couvrent au moins autant de courses que 3 mois
    assert fdf['opp_avg_180'].notna().sum() >= fdf['opp_avg_90'].notna().sum()
    # Parts du peloton : entre 0 et 1, et « sous le meilleur battu » >= « sous la moyenne des battus »
    both = fdf[['field_below_bmax_90', 'field_below_bavg_90']].dropna()
    assert both.min().min() >= 0 and both.max().max() <= 1
    assert (both['field_below_bmax_90'] >= both['field_below_bavg_90']).all()


def test_promote_existing_version(tmp_path, capsys):
    from galoptrack import __main__ as cli
    st = LocalStorage(tmp_path)
    for v in ('v8', 'v9'):
        st.write_bytes(f'model/{v}/meta.json', b'{}')
    model_store.set_current(st, 'v8')
    cli.cmd_promote(st, type('A', (), {'version': 'v9'})())
    assert model_store.current_version(st) == 'v9'
    with pytest.raises(SystemExit):
        cli.cmd_promote(st, type('A', (), {'version': 'v12'})())


# ── Note de classe ──────────────────────────────────────────────────────────

def _class_frame():
    """A gagne une fois (allocation 20 000), B gagne trois fois ; ils se
    retrouvent le 2025-03-01 avec C, débutant."""
    rows = []
    def race(rid, date, alloc, res):
        for nom, pa in res:
            rows.append({'race_id': rid, 'date': date, 'is_today': False, 'nom': nom,
                         'alloc': alloc, 'pa_num': pa})
    race('r1', 20250101, 20000, [('A', 1), ('X', 2)])
    race('r2', 20250105, 20000, [('B', 1), ('Y', 2)])
    race('r3', 20250110, 20000, [('B', 1), ('Y', 2)])
    race('r4', 20250115, 20000, [('B', 1), ('X', 2)])
    race('r5', 20250301, 30000, [('A', np.nan), ('B', np.nan), ('C', np.nan)])
    return pd.DataFrame(rows)


def test_class_note_accounts_for_experience():
    from galoptrack.classe import class_features
    raw = class_features(_class_frame(), prior_weight=0.0)
    cor = class_features(_class_frame(), prior_weight=1.0)
    get = lambda d, nom, col='classe': d[(d['race_id'] == 'r5') & (d['nom'] == nom)][col].iloc[0]
    # Sans correction, 1 victoire = 3 victoires ; avec correction, l'expérience compte
    assert np.isclose(get(raw, 'A'), get(raw, 'B'))
    assert get(cor, 'B') > get(cor, 'A') > get(cor, 'C')
    assert get(cor, 'A', 'classe_n12m') == 1 and get(cor, 'B', 'classe_n12m') == 3
    # Débutant sans correction : pas de note ; rang 1 = meilleure note
    assert np.isnan(get(raw, 'C'))
    assert get(cor, 'B', 'classe_rank') == 1
    # La course du jour (sans arrivée) ne sert jamais à sa propre note
    first = cor[cor['race_id'] == 'r1']
    assert (first['classe_n12m'] == 0).all()
    # Valeur d'une performance : 2e d'une course à 20 000 = 8 000
    from galoptrack.classe import performance_value
    assert performance_value([20000], [2])[0] == 8000


# ── Confrontations ──────────────────────────────────────────────────────────

def test_parse_ecart():
    from galoptrack.confront import parse_ecart
    assert parse_ecart('Nez') == 0.05 and parse_ecart('Cte Tête') == 0.1 and parse_ecart('Tête') == 0.2
    assert parse_ecart('1 L') == 1 and parse_ecart('3 L 1/2') == 3.5 and parse_ecart('1/2 L') == 0.5
    assert parse_ecart('Loin') == 30 and np.isnan(parse_ecart(None)) and np.isnan(parse_ecart('?'))


def test_confrontations_use_only_the_past():
    from galoptrack.confront import confrontation_features
    rows = []
    def race(rid, date, alloc, res):
        for nom, pa, ec in res:
            rows.append({'race_id': rid, 'date': date, 'is_today': False, 'nom': nom,
                         'alloc': alloc, 'pa_num': pa, 'ec': ec})
    # CADOR gagne gros ; A finit 2e à une courte tête de CADOR ; FAIBLE bat B
    race('r1', 20250101, 100000, [('CADOR', 1, None), ('Z', 2, '2 L')])
    race('r2', 20250201, 20000, [('CADOR', 1, None), ('A', 2, 'Cte Tête'), ('Y', 3, '5 L')])
    race('r3', 20250210, 4000, [('FAIBLE', 1, None), ('B', 2, '1 L')])
    race('r4', 20250215, 20000, [('A', 1, None), ('B', 2, '3 L')])
    race('r5', 20250301, 20000, [('A', np.nan, None), ('B', np.nan, None), ('FAIBLE', np.nan, None)])
    fdf = confrontation_features(pd.DataFrame(rows))
    get = lambda nom, col: fdf[(fdf['race_id'] == 'r5') & (fdf['nom'] == nom)][col].iloc[0]
    # A a fini à une courte tête de CADOR : niveau « battu de peu » renseigné
    assert not np.isnan(get('A', 'cf_close_vs_best'))
    # B a été battu par FAIBLE, rival du jour moins bien noté que lui
    assert get('B', 'cf_lost_to_weaker') == 1
    # A a déjà battu B ; rencontres directes vides pour qui n'a jamais rencontré le mieux noté
    assert get('A', 'cf_lost_to_weaker') == 0 or np.isnan(get('A', 'cf_lost_to_weaker'))
    # Rien n'est calculé à partir de la course du jour elle-même
    first = fdf[fdf['race_id'] == 'r1']
    assert first['cf_niveau_vs_best'].isna().all() and first['cf_beat_top'].isna().all()


def test_train_model_top3_labels(built):
    races_list, labels_array, _ = built
    fdf = build_features(copy.deepcopy(races_list), labels_array)
    model, _, _, metrics = train.train_model(fdf, features=BASE_FEATURES, label_mode='top3')
    assert metrics['n_valid_races'] > 0 and model.num_trees() > 0


def test_tracking_features_use_only_the_past(built):
    from galoptrack.troncons import TRACK_FEATURES, tracking_features
    races_list, labels_array, _ = built
    fdf = tracking_features(build_features(copy.deepcopy(races_list), labels_array))
    for f in TRACK_FEATURES:
        assert f in fdf, f
    assert fdf['tk_fin600_mean'].notna().sum() > 100 and fdf['tk_style'].notna().sum() > 100
    assert fdf['tk_fin600_mean'].dropna().between(0, 1).all()
    first = fdf.sort_values(['date', 'is_today']).groupby('nom').head(1)
    assert (first['tk_n'] == 0).all() and first['tk_fin600_mean'].isna().all()
