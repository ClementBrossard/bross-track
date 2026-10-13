"""Expériences sur le modèle : n'écrivent aucun modèle, ne touchent pas à la
production. Chacune compare, sur exactement les mêmes données et la même
coupure temporelle, le modèle de production et des variantes enrichies.

Expérience « poids » :

1. Mesure, sur la période de validation, le taux de victoire selon le niveau
   des chevaux battus lors de la dernière course (last_race_opp_elo_beaten_avg).
2. Mesure l'effet brut du poids (écart avec la dernière course, avec le champ).
3. Entraîne deux modèles sur exactement les mêmes données et la même coupure
   temporelle : les FEATURES de production, puis FEATURES + variables de poids
   relatif, et compare leurs résultats course par course."""

import logging
import os
from collections import deque

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


def _compare(fdf, valid, variants):
    """variants : {nom: liste de variables}. Retourne les lignes markdown."""
    res = {}
    for name, feats in variants.items():
        model, _, _, metrics = train.train_model(fdf, features=feats)
        res[name] = (metrics, _hits(valid, model.predict(valid[feats])),
                     dict(zip(feats, model.feature_importance('gain'))), feats)
    md = ["| | Gagnant trouvé (top 1) | Top 1 dans les 3 premiers | Gagnant dans le top 3 du modèle | NDCG@1 |",
          "|---|---:|---:|---:|---:|"]
    for n, (m, h, _, _) in res.items():
        a = np.array(list(h.values()))
        md.append(f"| {n} | {a[:, 0].mean():.1%} | {a[:, 1].mean():.1%} | {a[:, 2].mean():.1%} | {m['ndcg@1']:.3f} |")
    names = list(res)
    h0 = res[names[0]][1]
    md.append("")
    for n in names[1:]:
        h1, imp, feats = res[n][1], res[n][2], res[n][3]
        common = h0.keys() & h1.keys()
        only1 = sum(1 for r in common if h1[r][0] and not h0[r][0])
        only0 = sum(1 for r in common if h0[r][0] and not h1[r][0])
        total = sum(imp.values()) or 1
        ranked = sorted(imp, key=imp.get, reverse=True)
        extra = [f for f in feats if f not in FEATURES]
        md += [f"**{n}** : gagnant trouvé seulement par cette variante dans **{only1}** courses, "
               f"seulement par la production dans **{only0}** (sur {len(common)}).", ""]
        md += [f"- `{f}` : {imp[f] / total:.1%} de l'importance (rang {ranked.index(f) + 1}/{len(ranked)})"
               for f in extra]
        md.append("")
    md.append("Un écart de moins de ~1 point de pourcentage est dans le bruit statistique.")
    return md


def _publish(md):
    out = '\n'.join(md)
    summary = os.environ.get('GITHUB_STEP_SUMMARY')
    if summary:
        with open(summary, 'a', encoding='utf-8') as f:
            f.write(out + '\n')
    return out


# ── Expérience « adversaires » ───────────────────────────────────────────────
# Idée : un cheval qui a déjà affronté (ou battu) de très bons chevaux ces
# derniers mois et qui retrouve aujourd'hui un peloton plus faible. Le niveau
# d'un cheval = son Elo juste avant la course concernée (aucune fuite).

def opposition_features(fdf, windows=(90, 180)):
    """Ajoute, pour chaque fenêtre de N jours (courses strictement antérieures) :
      opp_avg_{N}        niveau moyen des pelotons affrontés
      opp_max_{N}        meilleur adversaire affronté
      beaten_max_{N}     meilleur cheval battu
      beaten_avg_{N}     niveau moyen des chevaux battus
      drop_avg_{N}       opp_avg_{N} - niveau moyen du peloton du jour (> 0 : il descend de niveau)
      beaten_vs_top_{N}  beaten_max_{N} - meilleur rival du jour (> 0 : il a déjà battu mieux)
    et le niveau du peloton du jour : field_avg, field_max (rivaux seulement)."""
    fdf = fdf.copy()
    elo = fdf['elo'].to_numpy(dtype=float)
    pa = fdf['pa_num'].to_numpy(dtype=float)
    n = len(fdf)
    field_avg, field_max = np.full(n, np.nan), np.full(n, np.nan)
    r_beaten_max, r_beaten_avg = np.full(n, np.nan), np.full(n, np.nan)
    for idx in fdf.groupby('race_id').indices.values():
        e, p = elo[idx], pa[idx]
        if len(idx) < 2:
            continue
        tot = e.sum()
        for k, i in enumerate(idx):
            others = np.delete(e, k)
            field_avg[i] = (tot - e[k]) / (len(idx) - 1)
            field_max[i] = others.max()
            if not np.isnan(p[k]):
                b = e[(p > p[k])]
                if len(b):
                    r_beaten_max[i], r_beaten_avg[i] = b.max(), b.mean()
    fdf['field_avg'], fdf['field_max'] = field_avg, field_max

    dates = pd.to_datetime(fdf['date'].astype(int).astype(str), format='%Y%m%d').to_numpy()
    out = {f'{c}_{w}': np.full(n, np.nan) for w in windows
           for c in ('opp_avg', 'opp_max', 'beaten_max', 'beaten_avg')}
    order = np.lexsort((fdf['is_today'].to_numpy(), dates, fdf['nom'].to_numpy()))
    noms = fdf['nom'].to_numpy()
    start = 0
    while start < n:
        end = start
        while end < n and noms[order[end]] == noms[order[start]]:
            end += 1
        rows = order[start:end]
        for w in windows:
            win = deque()
            for i in rows:
                lim = dates[i] - np.timedelta64(w, 'D')
                while win and dates[win[0]] < lim:
                    win.popleft()
                past = [j for j in win if dates[j] < dates[i]]
                if past:
                    fa = field_avg[past]
                    fm = field_max[past]
                    bm = r_beaten_max[past]
                    ba = r_beaten_avg[past]
                    if not np.isnan(fa).all():
                        out[f'opp_avg_{w}'][i] = np.nanmean(fa)
                        out[f'opp_max_{w}'][i] = np.nanmax(fm)
                    if not np.isnan(bm).all():
                        out[f'beaten_max_{w}'][i] = np.nanmax(bm)
                        out[f'beaten_avg_{w}'][i] = np.nanmean(ba)
                win.append(i)
        start = end
    for k, v in out.items():
        fdf[k] = v
    for w in windows:
        fdf[f'drop_avg_{w}'] = fdf[f'opp_avg_{w}'] - fdf['field_avg']
        fdf[f'beaten_vs_top_{w}'] = fdf[f'beaten_max_{w}'] - fdf['field_max']
    return fdf


def opposition_set(w):
    return [f'opp_avg_{w}', f'opp_max_{w}', f'beaten_max_{w}', f'beaten_avg_{w}',
            f'drop_avg_{w}', f'beaten_vs_top_{w}']


def run_opposition(storage):
    fdf = opposition_features(train.load_feature_frame(storage))
    ready, cutoff = _split(fdf)
    valid = ready[ready['date'] >= cutoff]
    md = [f"# Expérience adversaires — coupure {cutoff}", "",
          f"Validation : {valid['race_id'].nunique()} courses, {len(valid)} chevaux (plat).", "",
          "Niveau d'un cheval = son rating Elo juste avant la course. « Peloton du jour » = ses rivaux.", ""]

    def quint(col, title, labels=('très bas', 'bas', 'moyen', 'haut', 'très haut')):
        qs = np.unique(valid[col].quantile([0, .2, .4, .6, .8, 1]).to_numpy())
        qs[0], qs[-1] = -np.inf, np.inf
        return _bucket_table(valid, col, qs, list(labels)[:len(qs) - 1], title) + [""]

    md += quint('drop_avg_90', "1. Descente de niveau sur 3 mois : peloton moyen affronté - peloton du jour",
                ('monte fort', 'monte', 'stable', 'descend', 'descend fort'))
    md += quint('beaten_vs_top_90', "2. Meilleur cheval battu sur 3 mois - meilleur rival du jour",
                ('très en dessous', 'en dessous', 'proche', 'au-dessus', 'très au-dessus'))
    md += quint('beaten_max_90', "3. Meilleur cheval battu sur 3 mois (niveau absolu)")
    md += quint('last_race_opp_elo_beaten_avg', "4. Pour comparaison : chevaux battus, dernière course seulement")
    md += ["« (inconnu) » : pas de course dans la fenêtre, ou aucun cheval battu.", "",
           "### 5. Modèle actuel vs modèle + adversaires", ""]
    md += _compare(fdf, valid, {
        'Production (37 variables)': FEATURES,
        '+ adversaires 3 mois': FEATURES + opposition_set(90),
        '+ adversaires 6 mois': FEATURES + opposition_set(180),
        '+ adversaires 3 et 6 mois': FEATURES + opposition_set(90) + opposition_set(180),
    })
    return _publish(md)


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
