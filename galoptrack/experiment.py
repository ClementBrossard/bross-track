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

import numpy as np
import pandas as pd

from . import train
from .features import BASE_FEATURES, FEATURES

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
        extra = [f for f in feats if f not in res[names[0]][3]]
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
# Variables calculées par features.opposition_features (passées en production
# pour la fenêtre de 3 mois, voir FIELD_FEATURES).

def opposition_set(w):
    return [f'opp_avg_{w}', f'opp_max_{w}', f'beaten_max_{w}', f'beaten_avg_{w}',
            f'drop_avg_{w}', f'beaten_vs_top_{w}']


FOLDS = ((0.6, 0.1), (0.7, 0.1), (0.8, 0.1), (0.9, None))


def _compare_folds(fdf, variants, folds=FOLDS):
    """Même comparaison que _compare, répétée sur plusieurs périodes de test
    successives (le modèle n'apprend que sur ce qui précède chaque période)."""
    ready = fdf.dropna(subset=['pa_num']).sort_values(['date', 'race_id']).reset_index(drop=True)
    dates = sorted(ready['date'].unique())
    res = {name: [] for name in variants}
    periods = []
    for tf, vf in folds:
        lo = dates[int(len(dates) * tf)]
        valid = ready[ready['date'] >= lo]
        if vf is not None and int(len(dates) * (tf + vf)) < len(dates):
            valid = valid[valid['date'] < dates[int(len(dates) * (tf + vf))]]
        periods.append(f"{str(valid['date'].min())[4:6]}/{str(valid['date'].min())[2:4]}"
                       f"→{str(valid['date'].max())[4:6]}/{str(valid['date'].max())[2:4]}")
        for name, feats in variants.items():
            model, _, _, _ = train.train_model(fdf, features=feats, train_frac=tf, valid_frac=vf)
            res[name].append(_hits(valid, model.predict(valid[feats])))
    names = list(variants)
    md = ["| | " + " | ".join(periods) + " | Moyenne | Périodes où elle bat la production |",
          "|---|" + "---:|" * (len(periods) + 2)]
    base = [np.mean([v[0] for v in h.values()]) for h in res[names[0]]]
    for name in names:
        rates = [np.mean([v[0] for v in h.values()]) for h in res[name]]
        wins = '—' if name == names[0] else f"{sum(r > b for r, b in zip(rates, base))}/{len(rates)}"
        md.append(f"| {name} | " + " | ".join(f"{r:.1%}" for r in rates) + f" | **{np.mean(rates):.1%}** | {wins} |")
    md += ["", "Sur toutes les périodes réunies :", ""]
    for name in names[1:]:
        only1 = only0 = total = 0
        for h0, h1 in zip(res[names[0]], res[name]):
            for r in h0.keys() & h1.keys():
                total += 1
                only1 += h1[r][0] and not h0[r][0]
                only0 += h0[r][0] and not h1[r][0]
        md.append(f"- **{name}** : gagnant trouvé seulement par la variante dans {only1} courses, "
                  f"seulement par la production dans {only0} (sur {total}).")
    md += ["", "Pourcentages = part des courses où le cheval classé 1er par le modèle a gagné."]
    return md


def run_opposition(storage):
    fdf = train.load_feature_frame(storage)  # contient déjà la fenêtre de 3 mois
    ready, cutoff = _split(fdf)
    valid = ready[ready['date'] >= cutoff]
    md = [f"# Expérience adversaires (peloton entier) — 3 mois", "",
          f"Tableaux sur la validation récente : {valid['race_id'].nunique()} courses de plat depuis {cutoff}.", "",
          "Niveau d'un cheval = son rating Elo juste avant la course. « Peloton du jour » = ses rivaux.", ""]

    def quint(col, title, labels=('très bas', 'bas', 'moyen', 'haut', 'très haut')):
        qs = np.unique(valid[col].quantile([0, .2, .4, .6, .8, 1]).to_numpy())
        qs[0], qs[-1] = -np.inf, np.inf
        return _bucket_table(valid, col, qs, list(labels)[:len(qs) - 1], title) + [""]

    below = ['0-20 %', '20-40 %', '40-60 %', '60-80 %', '80-100 %']
    edges = [-0.01, 0.2, 0.4, 0.6, 0.8, 1.0]
    md += _bucket_table(valid, 'field_below_bavg_90', edges, below,
                        "1. Part du peloton du jour moins forte que la moyenne des chevaux qu'il a battus (3 mois)") + [""]
    md += _bucket_table(valid, 'field_below_bmax_90', edges, below,
                        "2. Part du peloton du jour moins forte que le meilleur cheval qu'il a battu (3 mois)") + [""]
    md += quint('beaten_vs_field_90', "3. Moyenne des chevaux battus (3 mois) - moyenne du peloton du jour",
                ('bien en dessous', 'en dessous', 'proche', 'au-dessus', 'bien au-dessus'))
    md += quint('beaten_vs_top3_90', "4. Moyenne des chevaux battus (3 mois) - moyenne des 3 meilleurs rivaux",
                ('bien en dessous', 'en dessous', 'proche', 'au-dessus', 'bien au-dessus'))
    md += ["« (inconnu) » : pas de course dans les 3 mois, ou aucun cheval battu.", "",
           "### 5. Modèle actuel vs variantes, sur 4 périodes de test successives", ""]
    lean = ['beaten_vs_top_90', 'drop_avg_90']
    whole = ['beaten_vs_field_90', 'beaten_vs_top3_90', 'field_below_bmax_90', 'field_below_bavg_90']
    md += _compare_folds(fdf, {
        'Production (37 variables)': BASE_FEATURES,
        'A. Meilleur battu + descente (2)': BASE_FEATURES + lean,
        'B. Peloton entier (4)': BASE_FEATURES + whole,
        'A + B (6)': BASE_FEATURES + lean + whole,
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
    for name, feats in (('Production', FEATURES),
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
