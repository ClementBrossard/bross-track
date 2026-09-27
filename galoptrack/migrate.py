"""Migration unique de l'historique Google Drive vers le nouveau stockage.

Entrée  : les fichiers du Drive déposés tels quels sous `raw/` (voir
          notebooks/migration_drive_vers_r2.ipynb) :
            raw/tracking_data.csv.gz, raw/troncons_data.csv.gz,
            raw/chevaux_data.csv.gz, raw/cotes_data.csv.gz,
            raw/rapports_data.csv.gz, raw/rapports_combines_data.csv.gz,
            raw/model/race_model_v7.txt, raw/model/iso_calib_v7.pkl,
            raw/model/backtest_v7.json
Sortie  : data/*.csv.gz nettoyées, model/v7/ (modèle actuel, en
          production), logs/migration_report.{json,md}.

Nettoyage appliqué (les fichiers du Drive, eux, ne sont jamais modifiés) :
  - lignes aux clés invalides (date, hippodrome, n° de course) isolées ;
  - doublons cheval x course retirés (bug de l'enrichissement) ;
  - codes hippodromes harmonisés ;
  - cotes_data fusionné dans chevaux (colonne cote_directe) ;
  - rapports rattachés aux mêmes code hippodrome / n° de réunion que
    chevaux, via les noms des chevaux (le notebook rapports utilisait le
    code PMU brut et un n° de réunion recalculé)."""

import io
import json
import logging
import pickle
from collections import Counter, defaultdict

import pandas as pd

from . import config, model_store, tables
from .hippos import CODES_FRANCE, LEGACY_CODE_FIXES

log = logging.getLogger(__name__)

KEY = ['date', 'code_hippo', 'num_course', 'nom_cheval']
RAW_FILES = ['tracking_data', 'troncons_data', 'chevaux_data', 'cotes_data', 'rapports_data',
             'rapports_combines_data']


def read_raw(storage, name):
    for suffix, comp in (('.csv.gz', 'gzip'), ('.csv', None)):
        key = f"raw/{name}{suffix}"
        if storage.exists(key):
            return pd.read_csv(io.BytesIO(storage.read_bytes(key)), compression=comp, low_memory=False)
    return None


def _valid_keys(df, check_hippo=True):
    date = pd.to_numeric(df['date'], errors='coerce')
    nc = pd.to_numeric(df['num_course'], errors='coerce')
    ok = date.between(20000101, 21001231) & nc.between(1, 30)
    if check_hippo:
        ok &= df['code_hippo'].astype(str).str.match(r'^[A-Z][A-Z\-]*$', na=False)
    return ok


def _describe(df):
    if df is None or df.empty:
        return {'rows': 0}
    d = pd.to_numeric(df['date'], errors='coerce')
    out = {'rows': int(len(df)), 'date_min': int(d.min()), 'date_max': int(d.max())}
    if {'code_hippo', 'num_course'}.issubset(df.columns):
        out['races'] = int(df[['date', 'code_hippo', 'num_course']].drop_duplicates().shape[0])
        out['hippos'] = sorted(df['code_hippo'].dropna().astype(str).unique().tolist())
    return out


def clean_basic(df, dedup_cols, name, report, prefer_col=None):
    rep = {'raw': _describe(df)}
    df = df.copy()
    if 'code_hippo' in df.columns:
        df['code_hippo'] = df['code_hippo'].astype(str).str.strip().replace(LEGACY_CODE_FIXES)
    ok = _valid_keys(df)
    rep['invalid_rows_removed'] = int((~ok).sum())
    df = df[ok].copy()
    df['date'] = pd.to_numeric(df['date']).astype('int64')
    df['num_course'] = pd.to_numeric(df['num_course']).astype('int64')
    if 'nom_cheval' in df.columns:
        df['nom_cheval'] = df['nom_cheval'].astype(str).str.strip()
    n = len(df)
    if prefer_col is not None and prefer_col in df.columns:
        # Parmi les doublons, garder en priorité la ligne qui a l'arrivée
        has = df[prefer_col].notna() & (df[prefer_col].astype(str).str.strip() != '')
        first_has = df.assign(_h=has).drop_duplicates(dedup_cols, keep='first')['_h']
        df = df.assign(_h=~has).sort_values('_h', kind='stable').drop(columns='_h')
        df = df.drop_duplicates(dedup_cols, keep='first').sort_index()
        after_has = (df[prefer_col].notna() & (df[prefer_col].astype(str).str.strip() != ''))
        rep['rows_gaining_result_vs_keep_first'] = int(after_has.sum() - first_has.sum())
    else:
        df = df.drop_duplicates(dedup_cols, keep='first')
    rep['duplicates_removed'] = int(n - len(df))
    rep['clean'] = _describe(df)
    report[name] = rep
    return df


COTE_CANDIDATES = ['cote_directe', 'cote', 'cote_direct', 'rapport_direct', 'dernier_rapport_direct',
                   'rapport', 'cote_simple_gagnant']


def _find_cote_column(cot):
    for c in COTE_CANDIDATES:
        if c in cot.columns:
            return c
    # Repli : première colonne dont le nom contient « cote » ou « rapport »
    for c in cot.columns:
        if 'cote' in c.lower() or 'rapport' in c.lower():
            return c
    return None


def merge_cotes(ch, cot, report):
    """cotes_data -> chevaux.cote_directe, jointure (date, n° course, nom) :
    le code hippodrome de cotes_data n'est pas fiable (table différente).
    Jamais bloquant : si le fichier est inexploitable, la migration continue
    sans cotes (elles seront re-collectées par le rattrapage)."""
    rep = {'raw': _describe(cot) if cot is not None and {'date'} <= set(cot.columns) else {},
           'columns': list(cot.columns) if cot is not None else None}
    report['cotes_data'] = rep
    ch = ch.copy()
    ch['cote_directe'] = pd.NA
    rep['matched'] = 0
    if cot is None or cot.empty:
        rep['status'] = 'absent ou vide'
        return ch
    col = _find_cote_column(cot)
    missing = [c for c in ('date', 'num_course', 'nom_cheval') if c not in cot.columns]
    if col is None or missing:
        rep['status'] = (f"ignoré : colonne de cote introuvable" if col is None
                         else f"ignoré : colonnes manquantes {missing}")
        log.warning("cotes_data %s (colonnes : %s)", rep['status'], list(cot.columns))
        return ch
    rep['cote_column_used'] = col
    cot = cot[_valid_keys(cot, check_hippo=False)].copy()
    cot['date'] = pd.to_numeric(cot['date']).astype('int64')
    cot['num_course'] = pd.to_numeric(cot['num_course']).astype('int64')
    cot['nom_cheval'] = cot['nom_cheval'].astype(str).str.strip().str.upper()
    cot['_cote'] = pd.to_numeric(cot[col].astype(str).str.replace(',', '.', regex=False), errors='coerce')
    cot = cot.dropna(subset=['_cote'])
    if cot.empty:
        rep['status'] = f"ignoré : colonne '{col}' sans valeur numérique"
        return ch
    k = ['date', 'num_course', 'nom_cheval']
    # Une même clé avec deux cotes différentes = ambigu -> ignorée
    nvals = cot.groupby(k)['_cote'].nunique()
    ambig = nvals[nvals > 1].index
    cot = cot.drop_duplicates(k).set_index(k)
    cot = cot[~cot.index.isin(ambig)]
    rep['ambiguous_keys_ignored'] = int(len(ambig))

    nom_up = ch['nom_cheval'].astype(str).str.strip().str.upper()
    idx = pd.MultiIndex.from_arrays([ch['date'], ch['num_course'], nom_up])
    ch['cote_directe'] = cot['_cote'].reindex(idx).values
    in_range = ch['date'].between(cot.index.get_level_values(0).min(), cot.index.get_level_values(0).max())
    rep['chevaux_rows_in_cotes_period'] = int(in_range.sum())
    rep['matched'] = int(ch['cote_directe'].notna().sum())
    rep['match_rate_in_period'] = round(float(ch.loc[in_range, 'cote_directe'].notna().mean()), 4) \
        if in_range.any() else None
    rep['status'] = 'ok'
    return ch


def remap_rapports(rap, comb, ch, report):
    """Aligne code_hippo / num_reunion des rapports sur ceux de chevaux, en
    retrouvant la course par les noms des chevaux (date + n° de course +
    recouvrement des noms)."""
    rep = {'rapports_raw': _describe(rap), 'combines_raw': _describe(comb)}
    cand = defaultdict(list)  # (date, num_course) -> [(code, reunion, set(noms))]
    for (d, h, r, c), g in ch.groupby(['date', 'code_hippo', 'num_reunion', 'num_course'], dropna=False):
        reunion = None if pd.isna(r) else int(r)
        cand[(int(d), int(c))].append((str(h), reunion, set(g['nom_cheval'].astype(str).str.strip().str.upper())))

    def raw_key(df):
        # -1 plutôt que NaN : des NaN dans une clé de dictionnaire ne se
        # retrouvent jamais (nan != nan)
        def num(col):
            return pd.to_numeric(df[col], errors='coerce').fillna(-1).astype('int64')
        return list(zip(num('date'), df['code_hippo'].astype(str), num('num_reunion'), num('num_course')))

    mapping, stats = {}, Counter()
    if rap is not None and not rap.empty:
        rap = rap.copy()
        rap['_k'] = raw_key(rap)
        for k, g in rap.groupby('_k', sort=False):
            d, h, r, c = k
            if d < 0 or c < 0:
                continue
            noms = set(g['nom_cheval'].dropna().astype(str).str.strip().str.upper())
            best, best_ov = None, 0
            for code, reunion, names in cand.get((int(d), int(c)), []):
                ov = len(noms & names)
                if ov > best_ov:
                    best, best_ov = (code, reunion), ov
            if best:
                mapping[k] = best
                stats['matched_by_names'] += 1
                if best[0] != h:
                    stats['code_hippo_changed'] += 1
                if best[1] is not None and r >= 0 and best[1] != r:
                    stats['num_reunion_changed'] += 1
            else:
                stats['unmatched'] += 1

    def apply(df):
        if df is None or df.empty:
            return df
        df = df.copy()
        keys = raw_key(df)
        codes, reunions = [], []
        for k, h0, r0 in zip(keys, df['code_hippo'], df['num_reunion']):
            m = mapping.get(k)
            codes.append(m[0] if m else LEGACY_CODE_FIXES.get(str(h0), str(h0)))
            reunions.append(m[1] if (m and m[1] is not None) else r0)
        df['code_hippo'] = codes
        df['num_reunion'] = reunions
        df = df[_valid_keys(df, check_hippo=False)].copy()
        df['date'] = pd.to_numeric(df['date']).astype('int64')
        df['num_course'] = pd.to_numeric(df['num_course']).astype('int64')
        return df.drop(columns=['_k'], errors='ignore')

    rap2, comb2 = apply(rap), apply(comb)
    if rap2 is not None and not rap2.empty:
        n = len(rap2)
        rap2 = rap2.drop_duplicates(['date', 'code_hippo', 'num_reunion', 'num_course', 'num_pmu'])
        stats['rapports_duplicates_removed'] = n - len(rap2)
        stats['rapports_races_not_in_france_codes'] = int(
            (~rap2['code_hippo'].isin(CODES_FRANCE)).sum())
    if comb2 is not None and not comb2.empty:
        n = len(comb2)
        comb2 = comb2.drop_duplicates(['date', 'code_hippo', 'num_reunion', 'num_course', 'type_pari',
                                       'libelle', 'combinaison'])
        stats['combines_duplicates_removed'] = n - len(comb2)
    rep.update(dict(stats))
    rep['rapports_clean'] = _describe(rap2)
    rep['combines_clean'] = _describe(comb2)
    report['rapports'] = rep
    return rap2, comb2


def migrate_model(storage, report, version='v7'):
    base = 'raw/model'
    model_key = f"{base}/race_model_{version}.txt"
    if not storage.exists(model_key):
        report['model'] = {'status': f'absent ({model_key})'}
        return
    with_calib = storage.exists(f"{base}/iso_calib_{version}.pkl")
    if not with_calib:
        report['model'] = {'status': 'calibration absente'}
        return
    iso = pickle.loads(storage.read_bytes(f"{base}/iso_calib_{version}.pkl"))
    calib = model_store.Calibrator.from_isotonic(iso)
    bt_key = f"{base}/backtest_{version}.json"
    backtest = json.loads(storage.read_bytes(bt_key)) if storage.exists(bt_key) else None
    meta = {'version': version, 'migrated_from_colab': True,
            'migrated_at': config.now_paris().isoformat(timespec='seconds'),
            'metrics': (backtest or {}).get('summary', {})}
    model_store.save_version(storage, version, storage.read_bytes(model_key).decode('utf-8'),
                             calib, backtest, meta)
    model_store.set_current(storage, version)
    report['model'] = {'status': 'ok', 'version': version, 'backtest': bool(backtest)}


def to_markdown(report):
    lines = ['# Rapport de migration GalopTrack', '', f"Généré le {report['generated_at']}", '']
    for name in ('tracking', 'troncons', 'chevaux'):
        r = report.get(name, {})
        raw, clean = r.get('raw', {}), r.get('clean', {})
        lines += [f"## {name}", '',
                  f"- Lignes : {raw.get('rows', 0):,} → {clean.get('rows', 0):,}",
                  f"- Lignes aux clés invalides retirées : {r.get('invalid_rows_removed', 0):,}",
                  f"- Doublons retirés : {r.get('duplicates_removed', 0):,}",
                  f"- Période : {clean.get('date_min')} → {clean.get('date_max')}",
                  f"- Courses : {raw.get('races', 0):,} → {clean.get('races', 0):,}", '']
        if 'rows_gaining_result_vs_keep_first' in r:
            lines.insert(-1, f"- Lignes récupérant l'arrivée grâce au dédoublonnage : "
                             f"{r['rows_gaining_result_vs_keep_first']:,}")
    c = report.get('cotes_data', {})
    lines += ['## cotes_data → chevaux.cote_directe', '',
              f"- Statut : {c.get('status')} (colonne utilisée : {c.get('cote_column_used')})",
              f"- Colonnes du fichier : {c.get('columns')}",
              f"- Lignes cotes : {c.get('raw', {}).get('rows', 0):,}, période "
              f"{c.get('raw', {}).get('date_min')} → {c.get('raw', {}).get('date_max')}",
              f"- Chevaux avec cote : {c.get('matched', 0):,} "
              f"(taux sur la période couverte : {c.get('match_rate_in_period')})", '']
    r = report.get('rapports', {})
    lines += ['## rapports', '',
              f"- Courses rattachées par les noms : {r.get('matched_by_names', 0):,} "
              f"(non rattachées : {r.get('unmatched', 0):,})",
              f"- Code hippodrome corrigé : {r.get('code_hippo_changed', 0):,} course(s)",
              f"- N° de réunion corrigé : {r.get('num_reunion_changed', 0):,} course(s)",
              f"- Lignes rapports : {r.get('rapports_raw', {}).get('rows', 0):,} → "
              f"{r.get('rapports_clean', {}).get('rows', 0):,}",
              f"- Lignes combinés : {r.get('combines_raw', {}).get('rows', 0):,} → "
              f"{r.get('combines_clean', {}).get('rows', 0):,}", '']
    lines += ['## Modèle', '', f"- {report.get('model')}", '']
    return '\n'.join(lines)


def run(storage):
    report = {'generated_at': config.now_paris().isoformat(timespec='seconds')}
    raw = {n: read_raw(storage, n) for n in RAW_FILES}
    report['raw_columns'] = {n: (list(df.columns) if df is not None else None) for n, df in raw.items()}
    missing = [n for n in ('tracking_data', 'troncons_data', 'chevaux_data') if raw[n] is None]
    if missing:
        raise RuntimeError(f"Fichiers bruts manquants sous raw/ : {missing}")

    tr = clean_basic(raw['tracking_data'], KEY, 'tracking', report)
    tc = clean_basic(raw['troncons_data'], KEY + ['troncon_index'], 'troncons', report)
    ch = clean_basic(raw['chevaux_data'], KEY, 'chevaux', report, prefer_col='ordre_arrivee')
    ch = merge_cotes(ch, raw['cotes_data'], report)
    rap, comb = remap_rapports(raw['rapports_data'], raw['rapports_combines_data'], ch, report)

    for name, df in (('tracking', tr), ('troncons', tc), ('chevaux', ch),
                     ('rapports', rap), ('rapports_combines', comb)):
        if df is None:
            continue
        cols = tables.SCHEMAS[name]
        for c in cols:
            if c not in df.columns:
                df[c] = pd.NA
        tables.write_table(storage, name, tables.normalize(df[cols]))

    migrate_model(storage, report)

    storage.write_bytes('logs/migration_report.json',
                        json.dumps(report, ensure_ascii=False, indent=1, default=str).encode('utf-8'),
                        content_type='application/json')
    md = to_markdown(report)
    storage.write_bytes('logs/migration_report.md', md.encode('utf-8'), content_type='text/markdown')
    return report, md
