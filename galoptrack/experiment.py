"""Expérience « poids » : n'écrit aucun modèle, ne touche pas à la production.

1. Mesure, sur la période de validation, le taux de victoire selon le niveau
   des chevaux battus lors de la dernière course (last_race_opp_elo_beaten_avg).
2. Mesure l'effet brut du poids (écart avec la dernière course, avec le champ).
3. Entraîne deux modèles sur exactement les mêmes données et la même coupure
   temporelle : les FEATURES de production, puis FEATURES + variables de poids
   relatif, et compare leurs résultats course par course."""

import logging
import os

import numpy as np
import pandas as pd

from . import train
from .features import FEATURES

log = logging.getLogger(__name__)

WEIGHT_FEATURES = ['pd_vs_field', 'pd_delta_last', 'last_race_beaten_pd_diff', 'last_race_ahead_pd_diff']


def add_weight_features(fdf):
    """Variables de poids relatif, calculées strictement avant chaque course.
    fdf doit être trié chronologiquement (sortie de build_features).
      pd_vs_field              poids - poids moyen des partants de la course
      pd_delta_last            poids - poids porté lors de la course précédente
      last_race_beaten_pd_diff course précédente : poids porté - poids moyen des chevaux battus
                               (> 0 : il rendait du poids à ceux qu'il a battus)
      last_race_ahead_pd_diff  course précédente : poids porté - poids moyen des chevaux devant"""
    fdf = fdf.copy()
    w = pd.to_numeric(fdf['pd'], errors='coerce')
    fdf['_pd'] = w
    fdf['pd_vs_field'] = w - fdf.groupby('race_id')['_pd'].transform('mean')
    fdf['pd_delta_last'] = w - fdf.groupby('nom', sort=False)['_pd'].shift(1)

    pa = fdf['pa_num'].to_numpy(dtype=float)
    wv = w.to_numpy(dtype=float)
    beaten = np.full(len(fdf), np.nan)
    ahead = np.full(len(fdf), np.nan)
    for idx in fdf.groupby('race_id').indices.values():
        p, ww = pa[idx], wv[idx]
        ok = ~np.isnan(ww)
        for k, i in enumerate(idx):
            if np.isnan(p[k]) or np.isnan(ww[k]):
                continue
            m = (p > p[k]) & ok
            if m.any():
                beaten[i] = ww[k] - ww[m].mean()
            m = (p < p[k]) & ok
            if m.any():
                ahead[i] = ww[k] - ww[m].mean()
    fdf['_b'], fdf['_a'] = beaten, ahead
    g = fdf.groupby('nom', sort=False)
    fdf['last_race_beaten_pd_diff'] = g['_b'].shift(1)
    fdf['last_race_ahead_pd_diff'] = g['_a'].shift(1)
    return fdf.drop(columns=['_pd', '_b', '_a'])


def _split(fdf):
    ready = fdf.dropna(subset=['pa_num']).sort_values(['date', 'race_id']).reset_index(drop=True)
    dates = sorted(ready['date'].unique())
    cutoff = dates[int(len(dates) * 0.8)]
    return ready, cutoff


def _hits(valid, pred):
    v = valid.assign(pred=pred)
    out = {}
    for rid, g in v.groupby('race_id'):
        if g['win'].sum() == 0:
            continue
        top = g.loc[g['pred'].idxmax()]
        top3 = set(g.nlargest(3, 'pred')['nom'])
        out[rid] = (int(top['win'] == 1), int(top['pa_num'] <= 3),
                    int(g.loc[g['win'] == 1, 'nom'].iloc[0] in top3))
    return out


def _bucket_table(df, col, bins, labels, title):
    d = df[[col, 'win', 'pa_num']].copy()
    d['tranche'] = pd.cut(d[col], bins=bins, labels=labels)
    d['tranche'] = d['tranche'].cat.add_categories(['(inconnu)']).fillna('(inconnu)')
    t = d.groupby('tranche', observed=False).agg(chevaux=('win', 'size'), victoire=('win', 'mean'),
                                                 top3=('pa_num', lambda s: (s <= 3).mean()))
    lines = [f"### {title}", "", "| Tranche | Chevaux | % victoire | % top 3 |", "|---|---:|---:|---:|"]
    for k, r in t.iterrows():
        if r['chevaux']:
            lines.append(f"| {k} | {int(r['chevaux'])} | {r['victoire']:.1%} | {r['top3']:.1%} |")
    return lines


def run(storage):
    fdf = add_weight_features(train.load_feature_frame(storage))
    ready, cutoff = _split(fdf)
    valid = ready[ready['date'] >= cutoff]
    md = [f"# Expérience poids — coupure {cutoff}", "",
          f"Validation : {valid['race_id'].nunique()} courses, {len(valid)} chevaux (plat).", ""]

    # 1. Niveau des chevaux battus (quintiles calculés sur la validation)
    col = 'last_race_opp_elo_beaten_avg'
    qs = valid[col].quantile([0, .2, .4, .6, .8, 1]).to_numpy()
    qs[0], qs[-1] = -np.inf, np.inf
    md += _bucket_table(valid, col, np.unique(qs),
                        ['très bas', 'bas', 'moyen', 'haut', 'très haut'][:len(np.unique(qs)) - 1],
                        "1. Niveau des chevaux battus lors de la dernière course (quintiles)")
    md += ["", "« (inconnu) » : pas de course précédente connue, ou aucun cheval battu (dernier).", ""]

    # 2. Effet brut du poids (toute la période, plat)
    md += _bucket_table(ready, 'pd_delta_last', [-np.inf, -3, -1, 0.5, 2.5, np.inf],
                        ['≤ -3 kg', '-2,5 à -1 kg', 'même poids', '+1 à +2 kg', '≥ +3 kg'],
                        "2a. Écart de poids avec la course précédente")
    md += [""]
    md += _bucket_table(ready, 'pd_vs_field', [-np.inf, -3, -1, 1, 3, np.inf],
                        ['≤ -3 kg', '-3 à -1 kg', '± 1 kg', '+1 à +3 kg', '≥ +3 kg'],
                        "2b. Poids par rapport à la moyenne du champ")
    md += [""]

    # 3. Modèle actuel vs modèle + poids, mêmes données, même coupure
    res = {}
    for name, feats in (('Production (37 variables)', FEATURES),
                        ('Production + poids relatif', FEATURES + WEIGHT_FEATURES)):
        model, _, _, metrics = train.train_model(fdf, features=feats)
        res[name] = (metrics, _hits(valid, model.predict(valid[feats])),
                     dict(zip(feats, model.feature_importance('gain'))))
    (n0, (m0, h0, _)), (n1, (m1, h1, imp1)) = res.items()
    md += ["### 3. Modèle actuel vs modèle avec le poids relatif", "",
           "| | Gagnant trouvé (top 1) | Top 1 dans les 3 premiers | Gagnant dans le top 3 du modèle | NDCG@1 |",
           "|---|---:|---:|---:|---:|"]
    for n, (m, h, _) in res.items():
        a = np.array(list(h.values()))
        md.append(f"| {n} | {a[:, 0].mean():.1%} | {a[:, 1].mean():.1%} | {a[:, 2].mean():.1%} | {m['ndcg@1']:.3f} |")
    common = h0.keys() & h1.keys()
    only1 = sum(1 for r in common if h1[r][0] and not h0[r][0])
    only0 = sum(1 for r in common if h0[r][0] and not h1[r][0])
    md += ["", f"Courses où seul le modèle avec poids trouve le gagnant : **{only1}** ; "
               f"seul le modèle actuel : **{only0}** (sur {len(common)} courses).",
           "Un écart de moins de ~1 point de pourcentage est dans le bruit statistique.", "",
           "Importance (gain) des variables de poids dans le modèle candidat :", ""]
    total = sum(imp1.values()) or 1
    ranked = sorted(imp1, key=imp1.get, reverse=True)
    for f in WEIGHT_FEATURES:
        md.append(f"- `{f}` : {imp1[f] / total:.1%} du total (rang {ranked.index(f) + 1}/{len(ranked)})")

    out = '\n'.join(md)
    summary = os.environ.get('GITHUB_STEP_SUMMARY')
    if summary:
        with open(summary, 'a', encoding='utf-8') as f:
            f.write(out + '\n')
    return out
