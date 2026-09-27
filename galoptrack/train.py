"""Entraînement du modèle (ex-notebook galoptrack_train_model, cellules 7 à 9) :
LightGBM lambdarank par course, backtest détaillé sur la validation,
calibration isotonique, puis enregistrement d'une nouvelle version."""

import logging
import re

import numpy as np
import pandas as pd

from . import config, model_store, tables
from .features import CAT_COLS, FEATURES, build_features
from .races import build_races

log = logging.getLogger(__name__)

PARAMS = {'objective': 'lambdarank', 'metric': 'ndcg', 'ndcg_eval_at': [1, 3],
          'learning_rate': 0.05, 'num_leaves': 31, 'min_data_in_leaf': 50, 'verbosity': -1}


def _groups(d):
    return d.groupby('race_id', sort=False).size().values


def load_feature_frame(storage):
    tr = tables.read_table(storage, 'tracking')
    tc = tables.read_table(storage, 'troncons')
    ch = tables.read_table(storage, 'chevaux')
    races_list, labels_array, _ = build_races(tr, tc, ch)
    log.info("Courses construites : %d (trackées %d)", len(races_list),
             sum(1 for r in races_list if r.get('tracked')))
    return build_features(races_list, labels_array)


def train_model(fdf, num_boost_round=500, early_stopping=30):
    import lightgbm as lgb
    from sklearn.isotonic import IsotonicRegression

    fdf = fdf.copy()
    fdf['label'] = 0
    fdf.loc[fdf['pa_num'] <= 3, 'label'] = 1
    fdf.loc[fdf['pa_num'] == 1, 'label'] = 3

    model_ready = fdf.dropna(subset=['pa_num']).sort_values(['date', 'race_id']).reset_index(drop=True)

    # Split temporel : 80 % des dates les plus anciennes pour l'entraînement
    dates_sorted = sorted(model_ready['date'].unique())
    cutoff = dates_sorted[int(len(dates_sorted) * 0.8)]
    train = model_ready[model_ready['date'] < cutoff]
    valid = model_ready[model_ready['date'] >= cutoff].copy()
    log.info("Coupure temporelle : %s | train %d lignes | valid %d lignes", cutoff, len(train), len(valid))

    lgb_train = lgb.Dataset(train[FEATURES], label=train['label'], group=_groups(train),
                            categorical_feature=CAT_COLS, free_raw_data=False)
    lgb_valid = lgb.Dataset(valid[FEATURES], label=valid['label'], group=_groups(valid),
                            categorical_feature=CAT_COLS, reference=lgb_train, free_raw_data=False)
    model = lgb.train(PARAMS, lgb_train, num_boost_round=num_boost_round, valid_sets=[lgb_valid],
                      callbacks=[lgb.early_stopping(early_stopping), lgb.log_evaluation(50)])

    valid['pred'] = model.predict(valid[FEATURES])
    hits_model, hits_hv, hits_mu = [], [], []
    for _, g in valid.groupby('race_id'):
        if g['win'].sum() == 0:
            continue
        winner = g.loc[g['win'] == 1, 'nom'].iloc[0]
        hits_model.append(int(g.loc[g['pred'].idxmax(), 'nom'] == winner))
        hv = g['hv'].fillna(-1)
        hits_hv.append(int(g.loc[hv.idxmax(), 'nom'] == winner))
        mu = g['mu_last1'].fillna(99)
        hits_mu.append(int(g.loc[mu.idxmin(), 'nom'] == winner))

    metrics = {
        'cutoff': int(cutoff),
        'n_train_rows': int(len(train)),
        'n_valid_rows': int(len(valid)),
        'best_iteration': int(model.best_iteration or 0),
        'ndcg@1': float(model.best_score['valid_0']['ndcg@1']),
        'ndcg@3': float(model.best_score['valid_0']['ndcg@3']),
        'n_valid_races': len(hits_model),
        'hit_rate_top1': float(np.mean(hits_model)) if hits_model else None,
        'baseline_hv': float(np.mean(hits_hv)) if hits_hv else None,
        'baseline_last_perf': float(np.mean(hits_mu)) if hits_mu else None,
        'baseline_random': float((1 / valid.groupby('race_id').size()).mean()) if len(valid) else None,
    }

    backtest = build_backtest(valid, hits_model)

    iso = IsotonicRegression(out_of_bounds='clip', y_min=0.0001, y_max=0.9999)
    iso.fit(valid['pred'], valid['win'])
    return model, model_store.Calibrator.from_isotonic(iso), backtest, metrics


_rc_re = re.compile(r'_R(\d+)_(\d+)$')


def _horse_entry(row):
    npart = row['npart'] if 'npart' in row and pd.notna(row['npart']) else None
    return {'nom': row['nom'], 'npart': int(npart) if npart is not None else None}


def build_backtest(valid, hits_model):
    """Cellule 8 : top 10 prédit vs top 5 réel, course par course."""
    out = []
    for rid, g in valid.groupby('race_id'):
        if len(g) < 5 or g['pa_num'].isna().all():
            continue
        top10_pred = g.nlargest(10, 'pred')
        top5_real = g.nsmallest(5, 'pa_num')
        true_top3 = set(g.nsmallest(3, 'pa_num')['nom'])
        nb_in_top3 = sum(1 for n in top10_pred['nom'].iloc[:5] if n in true_top3)
        m = _rc_re.search(rid)
        out.append({
            'race_id': rid,
            'date': int(g['date'].iloc[0]),
            'hippo': str(g['hippo'].iloc[0]),
            'rnum': int(m.group(1)) if m else None,
            'num': int(m.group(2)) if m else None,
            'dist': int(g['dist'].iloc[0]) if pd.notna(g['dist'].iloc[0]) else None,
            'cat': str(g['cat'].iloc[0]) if pd.notna(g['cat'].iloc[0]) else None,
            'n_partants': int(len(g)),
            'pred_top10': [_horse_entry(r) for _, r in top10_pred.iterrows()],
            'real_top5': [_horse_entry(r) for _, r in top5_real.iterrows()],
            'nb_pred_top5_in_real_top3': int(nb_in_top3),
        })
    if not out:
        return {'summary': {'n_courses': 0}, 'races': []}
    nb = [b['nb_pred_top5_in_real_top3'] for b in out]
    dist = pd.Series(nb).value_counts().sort_index()
    summary = {
        'n_courses': len(out),
        'date_min': min(b['date'] for b in out),
        'date_max': max(b['date'] for b in out),
        'moyenne_top5_dans_top3': round(float(np.mean(nb)), 3),
        'pct_au_moins_1': round(float(np.mean([n >= 1 for n in nb])), 3),
        'pct_au_moins_2': round(float(np.mean([n >= 2 for n in nb])), 3),
        'pct_au_moins_3': round(float(np.mean([n >= 3 for n in nb])), 3),
        'distribution': {str(k): int(v) for k, v in dist.items()},
        'hit_rate_top1': round(float(np.mean(hits_model)), 3) if hits_model else None,
    }
    return {'summary': summary, 'races': out}


def run(storage, promote=True):
    """Entraîne une nouvelle version et la met en production si elle passe
    le garde-fou. Retourne le résumé."""
    fdf = load_feature_frame(storage)
    model, calib, backtest, metrics = train_model(fdf)
    version = model_store.next_version(storage)
    previous = model_store.current_version(storage)
    meta = {'version': version, 'trained_at': config.now_paris().isoformat(timespec='seconds'),
            'previous': previous, 'metrics': metrics, 'features': FEATURES}
    model_store.save_version(storage, version, model.model_to_string(), calib, backtest, meta)

    hit = metrics['hit_rate_top1'] or 0
    promoted = bool(promote and hit >= config.MIN_HIT_RATE_TOP1)
    if promoted:
        model_store.set_current(storage, version)
    meta['promoted'] = promoted
    model_store.write_meta(storage, version, meta)
    log.info("Modèle %s : hit-rate top1 %.1f%% -> %s", version, hit * 100,
             "EN PRODUCTION" if promoted else f"non promu (seuil {config.MIN_HIT_RATE_TOP1:.0%})")
    return meta
