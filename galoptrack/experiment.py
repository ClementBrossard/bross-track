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
import re

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
        # (n°1 gagne, n°1 placé, gagnant dans le top 3 du modèle,
        #  nombre de chevaux du top 3 du modèle finissant dans les 3 premiers)
        out[rid] = (int(top['win'] == 1), int(top['pa_num'] <= 3),
                    int(g.loc[g['win'] == 1, 'nom'].iloc[0] in top3),
                    int(g[g['nom'].isin(top3)]['pa_num'].le(3).sum()))
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


# ── Étape 1 du modèle Compétition : note de classe ──────────────────────────

def _pick_rate(valid, col, higher=True):
    """Part des courses gagnées par le cheval classé n°1 selon `col`."""
    hits = []
    for _, g in valid.groupby('race_id'):
        if g['win'].sum() == 0 or g[col].isna().all():
            continue
        i = g[col].idxmax() if higher else g[col].idxmin()
        hits.append(int(g.at[i, 'win'] == 1))
    return (np.mean(hits) if hits else np.nan), len(hits)


def run_classe(storage):
    from .classe import class_features
    fdf = train.load_feature_frame(storage)
    fdf = class_features(fdf, prior_weight=0.0, suffix='_m0')   # sans correction d'expérience
    fdf = class_features(fdf, prior_weight=1.0)                 # retenue
    fdf = class_features(fdf, prior_weight=3.0, suffix='_m3')   # correction forte
    ready, cutoff = _split(fdf)
    valid = ready[ready['date'] >= cutoff].copy()
    md = [f"# Étape 1 — note de classe « turfiste »", "",
          f"Validation : {valid['race_id'].nunique()} courses de plat depuis {cutoff}.", "",
          "Performance = allocation de la course x part de la place (100/40/30/20/10 %). "
          "Note = moyenne des 3 meilleures performances sur 12 mois, tirée vers la moyenne "
          "générale quand le cheval a peu couru (1 performance moyenne fictive ajoutée).", ""]

    qs = np.unique(valid['classe_vs_field'].quantile([0, .2, .4, .6, .8, 1]).to_numpy())
    qs[0], qs[-1] = -np.inf, np.inf
    md += _bucket_table(valid, 'classe_vs_field', qs,
                        ['bien en dessous', 'en dessous', 'dans la moyenne', 'au-dessus', 'bien au-dessus'][:len(qs) - 1],
                        "1. Note de classe du cheval comparée à la moyenne du peloton du jour") + [""]

    md += ["### 2. Le cheval classé n°1 selon… gagne la course", "",
           "| Classement selon | % de courses gagnées par le n°1 |", "|---|---:|"]
    valid['_rank_mix'] = (valid.groupby('race_id')['elo'].rank(ascending=False)
                          + valid.groupby('race_id')['classe'].rank(ascending=False))
    for label, col, hi in (("Rating Elo (variable de la v8)", 'elo', True),
                           ("Note de classe, sans correction d'expérience", 'classe_m0', True),
                           ("Note de classe, correction légère (retenue)", 'classe', True),
                           ("Note de classe, correction forte", 'classe_m3', True),
                           ("Elo + note de classe (rang moyen)", '_rank_mix', False),
                           ("Valeur handicap", 'hv', True),
                           ("Musique : moyenne des 5 dernières places", 'mu_avg5', False)):
        r, nb = _pick_rate(valid, col, hi)
        md.append(f"| {label} | {r:.1%} |")
    rnd = (1 / valid.groupby('race_id').size()).mean()
    md += [f"| Hasard | {rnd:.1%} |", "",
           f"Pour comparaison, la v8 (37 variables) trouve le gagnant dans ~25 % des courses.", ""]

    # 3. Expérience : le n°1 de la note gagne-t-il autant avec peu de courses ?
    md += ["### 3. Effet de l'expérience : le cheval n°1 de la note, selon son nombre de courses (12 mois)", "",
           "| Courses sur 12 mois | Sans correction : n°1 | % victoire | Avec correction : n°1 | % victoire |",
           "|---|---:|---:|---:|---:|"]
    for lo, hi_, lab in ((0, 0, 'aucune'), (1, 2, '1 à 2'), (3, 5, '3 à 5'), (6, 999, '6 et plus')):
        row = [lab]
        for col, ncol in (('classe_m0', 'classe_n12m_m0'), ('classe', 'classe_n12m')):
            top = valid[valid.groupby('race_id')[col].rank(ascending=False, method='first') == 1]
            top = top[(top[ncol] >= lo) & (top[ncol] <= hi_)]
            row += [str(len(top)), f"{top['win'].mean():.1%}" if len(top) else '—']
        md.append("| " + " | ".join(row) + " |")
    md += ["", "Sans correction, un cheval peu expérimenté avec une belle note est surévalué si son % de "
               "victoire est nettement plus bas que celui des chevaux expérimentés.", ""]

    qs = np.unique(valid['classe_vs_race'].quantile([0, .2, .4, .6, .8, 1]).to_numpy())
    qs[0], qs[-1] = -np.inf, np.inf
    md += _bucket_table(valid, 'classe_vs_race', qs,
                        ['monte fort', 'monte', 'à son niveau', 'descend', 'descend fort'][:len(qs) - 1],
                        "4. Note de classe comparée au niveau (allocation) de la course du jour") + [""]

    # 5. Classe vs Elo : mesurent-elles la même chose ?
    corr = valid[['classe', 'elo']].corr(method='spearman').iloc[0, 1]
    dis = []
    for _, g in valid.groupby('race_id'):
        if g['win'].sum() == 0:
            continue
        ie, ic = g['elo'].idxmax(), g['classe'].idxmax()
        if ie != ic:
            dis.append((g.at[ie, 'win'], g.at[ic, 'win']))
    d = np.array(dis) if dis else np.zeros((0, 2))
    md += ["### 5. Note de classe et Elo", "",
           f"Corrélation (rangs) entre les deux : {corr:.2f} (1 = identiques, 0 = sans rapport).", "",
           f"Courses où le n°1 Elo et le n°1 de la note sont différents : {len(d)}. "
           f"Le n°1 Elo gagne {d[:, 0].mean():.1%} d'entre elles, le n°1 de la note {d[:, 1].mean():.1%}." if len(d) else "", ""]
    return _publish(md)


# ── Étape 2 du modèle Compétition : confrontations ──────────────────────────

CLASSE_FEATURES = ['classe_m0', 'classe_n12m_m0', 'classe_vs_field_m0', 'classe_vs_best_m0', 'classe_vs_race_m0']
# Modèle « Compétition » partiel : ni Elo (ni variables dérivées de l'Elo), ni
# jockey / entraîneur (étape 4), ni tronçons (étape 3).
COMPET_BASE = ['ag', 'sx', 'oe', 'dist', 'alloc', 'cat', 'piste', 'terrain', 'hippo',
               'win_rate_career', 'place_rate_career', 'gain_moy', 'mu_avg5', 'mu_last1', 'mu_wins5',
               'h2h_wins_field', 'h2h_losses_field', 'h2h_net_field', 'h2h_opp_met',
               'days_since_last', 'alloc_delta', 'alloc_ratio']


def _yes_no_table(df, col, title, labels=('oui', 'non', 'jamais rencontré')):
    lines = [f"### {title}", "", "| | Chevaux | % victoire | % top 3 |", "|---|---:|---:|---:|"]
    for lab, mask in ((labels[0], df[col] == 1), (labels[1], df[col] == 0), (labels[2], df[col].isna())):
        d = df[mask]
        if len(d):
            lines.append(f"| {lab} | {len(d)} | {d['win'].mean():.1%} | {(d['pa_num'] <= 3).mean():.1%} |")
    return lines + [""]


def run_confront(storage):
    from .classe import class_features
    from .confront import CONFRONT_FEATURES, confrontation_features
    fdf = train.load_feature_frame(storage)
    fdf = class_features(fdf, prior_weight=0.0, suffix='_m0')
    fdf = confrontation_features(fdf)
    ready, cutoff = _split(fdf)
    valid = ready[ready['date'] >= cutoff].copy()
    md = ["# Étape 2 — confrontations", "",
          f"Validation : {valid['race_id'].nunique()} courses de plat depuis {cutoff}.", "",
          "Force d'un adversaire = sa note de classe au moment de la rencontre. Fenêtre : 12 mois.", ""]

    def quint(col, title, labels):
        qs = np.unique(valid[col].quantile([0, .2, .4, .6, .8, 1]).to_numpy())
        qs[0], qs[-1] = -np.inf, np.inf
        return _bucket_table(valid, col, qs, list(labels)[:len(qs) - 1], title) + [""]

    lv = ('très en dessous', 'en dessous', 'proche', 'au-dessus', 'très au-dessus')
    md += quint('cf_niveau_vs_best', "1. Niveau démontré (meilleur battu ou battu de peu) vs meilleur rival du jour", lv)
    md += quint('cf_close_vs_best', "2. « Battu de moins d'une longueur par un cador » : ce cador vs meilleur rival du jour", lv)
    md += _bucket_table(valid, 'cf_weak_ahead_rate', [-0.01, 0, 0.5, 1, 2, np.inf],
                        ['aucun', '≤ 0,5 par course', '0,5 à 1', '1 à 2', 'plus de 2'],
                        "3. « Des daubes devant lui » : chevaux plus faibles que le peloton du jour qui l'ont battu (par course, 12 mois)") + [""]
    md += _yes_no_table(valid, 'cf_beat_top', "4. A déjà fini devant le rival le mieux noté du jour")
    md += _yes_no_table(valid, 'cf_lost_to_weaker', "5. A déjà été battu par un rival du jour moins bien noté que lui",
                        labels=('oui', 'non', 'aucune rencontre'))

    md += ["### 6. Le cheval classé n°1 selon… gagne la course", "",
           "| Classement selon | % |", "|---|---:|"]
    valid['_mix'] = (valid.groupby('race_id')['classe_m0'].rank(ascending=False)
                     + valid.groupby('race_id')['cf_niveau_vs_best'].rank(ascending=False))
    for label, col, hi in (("Rating Elo", 'elo', True), ("Note de classe", 'classe_m0', True),
                           ("Niveau démontré", 'cf_niveau_vs_best', True),
                           ("Note de classe + niveau démontré", '_mix', False)):
        md.append(f"| {label} | {_pick_rate(valid, col, hi)[0]:.1%} |")
    md += ["", "### 7. Modèles, sur 4 périodes de test successives", ""]
    md += _compare_folds(fdf, {
        'Production (37 variables)': BASE_FEATURES,
        'Production + classe + confrontations': BASE_FEATURES + CLASSE_FEATURES + CONFRONT_FEATURES,
        'Compétition partiel (sans Elo, jockey, tronçons)': COMPET_BASE + CLASSE_FEATURES + CONFRONT_FEATURES,
        'Compétition partiel sans classe ni confrontations': COMPET_BASE,
    })
    return _publish(md)


# ── Objectif « les 3 premiers » ─────────────────────────────────────────────

def _compare_folds_top3(fdf, variants, folds=FOLDS):
    """variants : {nom: (variables, label_mode)}. Mesure principale : sur les 3
    chevaux classés en tête par le modèle, combien finissent dans les 3 premiers."""
    ready = fdf.dropna(subset=['pa_num']).sort_values(['date', 'race_id']).reset_index(drop=True)
    dates = sorted(ready['date'].unique())
    res = {name: [] for name in variants}
    periods = []
    for tf, vf in folds:
        lo = dates[int(len(dates) * tf)]
        valid = ready[ready['date'] >= lo]
        if vf is not None and int(len(dates) * (tf + vf)) < len(dates):
            valid = valid[valid['date'] < dates[int(len(dates) * (tf + vf))]]
        valid = valid[valid.groupby('race_id')['nom'].transform('size') >= 4]
        periods.append(f"{str(valid['date'].min())[4:6]}/{str(valid['date'].min())[2:4]}"
                       f"→{str(valid['date'].max())[4:6]}/{str(valid['date'].max())[2:4]}")
        for name, (feats, mode) in variants.items():
            model, _, _, _ = train.train_model(fdf, features=feats, train_frac=tf, valid_frac=vf, label_mode=mode)
            res[name].append(_hits(valid, model.predict(valid[feats])))
    names = list(variants)
    p3 = lambda h: np.mean([v[3] for v in h.values()])
    md = ["Sur les 3 chevaux classés en tête par le modèle, nombre moyen qui finissent dans les 3 premiers "
          "(courses d'au moins 4 partants) :", "",
          "| | " + " | ".join(periods) + " | Moyenne | Bat la référence |",
          "|---|" + "---:|" * (len(periods) + 2)]
    base = [p3(h) for h in res[names[0]]]
    for name in names:
        r = [p3(h) for h in res[name]]
        wins = '—' if name == names[0] else f"{sum(a > b for a, b in zip(r, base))}/{len(r)}"
        md.append(f"| {name} | " + " | ".join(f"{x:.2f}" for x in r) + f" | **{np.mean(r):.3f}** | {wins} |")
    md += ["", "Détail, toutes périodes réunies :", "",
           "| | 0 sur 3 | 1 sur 3 | 2 sur 3 | **3 sur 3 (trio)** | ≥ 2 sur 3 | Gagnant dans le top 3 | N°1 placé |",
           "|---|---:|---:|---:|---:|---:|---:|---:|"]
    for name in names:
        allv = [v for h in res[name] for v in h.values()]
        c = np.array([v[3] for v in allv])
        md.append(f"| {name} | " + " | ".join(f"{(c == k).mean():.1%}" for k in range(3))
                  + f" | **{(c == 3).mean():.1%}** | {(c >= 2).mean():.1%} | "
                  f"{np.mean([v[2] for v in allv]):.1%} | {np.mean([v[1] for v in allv]):.1%} |")
    n_races = sum(len(h) for h in res[names[0]])
    md += ["", f"{n_races} courses de test au total. Un écart de moins de ~0,02 sur la moyenne est dans le bruit."]
    return md


def run_top3(storage):
    from .classe import class_features
    from .confront import CONFRONT_FEATURES, confrontation_features
    fdf = train.load_feature_frame(storage)
    fdf = class_features(fdf, prior_weight=0.0, suffix='_m0')
    fdf = confrontation_features(fdf)
    ready, cutoff = _split(fdf)
    valid = ready[ready['date'] >= cutoff]
    md = ["# Objectif : les 3 premiers", "",
          "Deux façons d'entraîner le modèle : « gagnant » (production : gagnant 3 points, placé 1 point) "
          "et « top 3 » (dans les 3 premiers = 1 point, sans favoriser le gagnant).", ""]
    # Tes règles jugées au % de top 3 (tableaux de l'étape 2, période récente)
    md += _bucket_table(valid, 'cf_weak_ahead_rate', [-0.01, 0, 0.5, 1, 2, np.inf],
                        ['aucun', '≤ 0,5 par course', '0,5 à 1', '1 à 2', 'plus de 2'],
                        "Rappel : « des daubes devant lui » (12 mois)") + [""]
    rules = BASE_FEATURES + CLASSE_FEATURES + CONFRONT_FEATURES
    md += ["### Modèles, sur 4 périodes de test successives", ""]
    md += _compare_folds_top3(fdf, {
        'v8 (référence, appris « gagnant »)': (BASE_FEATURES, 'win'),
        'v8 appris « top 3 »': (BASE_FEATURES, 'top3'),
        'v8 + classe + confrontations, appris « top 3 »': (rules, 'top3'),
        'v8 + classe + confrontations, appris « gagnant »': (rules, 'win'),
    })
    return _publish(md)


# ── Étape 3 du modèle Compétition : tronçons ────────────────────────────────

def _scenario_table(valid):
    """Style du cheval x nombre de meneurs parmi ses rivaux -> % top 3."""
    d = valid.dropna(subset=['tk_style', 'tk_field_leaders'])
    styles = (('meneur (part en tête)', d['tk_style'] < 0.25),
              ('placé (milieu)', (d['tk_style'] >= 0.25) & (d['tk_style'] < 0.6)),
              ('finisseur (part de l\'arrière)', d['tk_style'] >= 0.6))
    packs = (('0 autre meneur', d['tk_field_leaders'] == 0), ('1', d['tk_field_leaders'] == 1),
             ('2', d['tk_field_leaders'] == 2), ('3 et plus', d['tk_field_leaders'] >= 3))
    lines = ["### 3. Scénario de course : style du cheval x nombre de meneurs parmi ses rivaux (% top 3)", "",
             "| Style / meneurs en face | " + " | ".join(p for p, _ in packs) + " |",
             "|---|" + "---:|" * len(packs)]
    for sname, sm in styles:
        cells = []
        for _, pm in packs:
            x = d[sm & pm]
            cells.append(f"{(x['pa_num'] <= 3).mean():.0%} ({len(x)})" if len(x) >= 30 else '—')
        lines.append(f"| {sname} | " + " | ".join(cells) + " |")
    return lines + ["", "Entre parenthèses : nombre de chevaux.", ""]


def run_troncons(storage):
    from .classe import class_features
    from .confront import CONFRONT_FEATURES, confrontation_features
    from .troncons import TRACK_FEATURES, tracking_features
    fdf = train.load_feature_frame(storage)
    fdf = tracking_features(fdf)
    fdf = class_features(fdf, prior_weight=0.0, suffix='_m0')
    fdf = confrontation_features(fdf)
    ready, cutoff = _split(fdf)
    valid = ready[ready['date'] >= cutoff].copy()
    cov = valid['tk_n'].gt(0).mean()
    md = ["# Étape 3 — tronçons", "",
          f"Validation : {valid['race_id'].nunique()} courses de plat depuis {cutoff}. "
          f"{cov:.0%} des partants ont au moins une course trackée dans les 12 mois.", "",
          "Rangs dans la course (0 = le meilleur du peloton, 1 = le dernier), moyenne des 3 dernières courses trackées.", ""]
    lv = ('les plus rapides', 'rapides', 'moyens', 'lents', 'les plus lents')

    def quint(col, title, labels):
        qs = np.unique(valid[col].quantile([0, .2, .4, .6, .8, 1]).to_numpy())
        qs[0], qs[-1] = -np.inf, np.inf
        return _bucket_table(valid, col, qs, list(labels)[:len(qs) - 1], title) + [""]

    md += quint('tk_fin600_mean', "1. Vitesse sur les 600 derniers mètres, comparée au peloton de chaque course", lv)
    md += quint('tk_fin600_vs_field', "2. Sa vitesse de finish comparée à celle de ses rivaux du jour",
                ('bien plus rapide', 'plus rapide', 'pareil', 'plus lent', 'bien plus lent'))
    md += _scenario_table(valid)
    md += ["### 4. Modèles « top 3 », sur 4 périodes de test successives", ""]
    rules = CLASSE_FEATURES + CONFRONT_FEATURES
    md += _compare_folds_top3(fdf, {
        'v8 apprise « top 3 » (nouvelle référence)': (BASE_FEATURES, 'top3'),
        '+ tronçons': (BASE_FEATURES + TRACK_FEATURES, 'top3'),
        '+ tronçons + classe + confrontations': (BASE_FEATURES + TRACK_FEATURES + rules, 'top3'),
    })
    return _publish(md)


# ── Seuils de probabilité du modèle en production ──────────────────────────

PROB_BINS = [0, 5, 10, 15, 20, 25, 30, 40, 50, 101]


def _seuil_tables(d, title):
    lines = [f"### {title}", "",
             f"{d['race_id'].nunique()} courses, {len(d)} chevaux.", "",
             "| Proba du modèle (dashboard) | Chevaux | % 1er | % dans les 3 premiers | Partants (moyenne) |",
             "|---|---:|---:|---:|---:|"]
    cut = pd.cut(d['prob'], PROB_BINS, right=False)
    for b, g in d.groupby(cut, observed=True):
        lab = f"{int(b.left)} à {int(b.right)} %" if b.right <= 100 else f"{int(b.left)} % et plus"
        lines.append(f"| {lab} | {len(g)} | {g['win'].mean():.0%} | **{g['top3'].mean():.0%}** | {g['n'].mean():.1f} |")
    lines += ["", "Cumulé : « au moins X % » ->", "",
              "| Proba ≥ | Chevaux | Par course (moy.) | % dans les 3 premiers |", "|---|---:|---:|---:|"]
    nr = d['race_id'].nunique()
    for t in (10, 15, 20, 25, 30, 35, 40, 50):
        g = d[d['prob'] >= t]
        if len(g):
            lines.append(f"| {t} % | {len(g)} | {len(g) / nr:.2f} | **{g['top3'].mean():.0%}** |")
    lines += ["", "Selon la taille du peloton, pour les chevaux à 30 % ou plus :", "",
              "| Partants | Chevaux | % dans les 3 premiers |", "|---|---:|---:|"]
    g = d[d['prob'] >= 30]
    for lab, m in (("jusqu'à 8", g['n'] <= 8), ("9 à 12", (g['n'] >= 9) & (g['n'] <= 12)), ("13 et plus", g['n'] >= 13)):
        x = g[m]
        if len(x):
            lines.append(f"| {lab} | {len(x)} | {x['top3'].mean():.0%} |")
    rank1 = d[d.groupby('race_id')['prob'].rank(ascending=False, method='first') == 1]
    lines += ["", f"Le n°1 du modèle (quelle que soit sa proba) finit dans les 3 premiers dans "
                  f"**{rank1['top3'].mean():.0%}** des courses.", ""]
    return lines


def run_seuils(storage):
    from . import model_store
    version = model_store.current_version(storage)
    booster, calib, _, meta = model_store.load_version(storage, version)
    feats = booster.feature_name()
    fdf = train.load_feature_frame(storage)
    d = fdf.dropna(subset=['pa_num']).copy()
    cutoff = int(meta.get('metrics', {}).get('cutoff') or 0)
    trained = int(str(meta.get('trained_at', ''))[:10].replace('-', '') or 0)
    d = d[d['date'] >= cutoff].copy()
    d['pred'] = booster.predict(d[feats])
    d['cal'] = calib.predict(d['pred'])
    d['prob'] = d['cal'] / d.groupby('race_id')['cal'].transform('sum') * 100
    d['n'] = d.groupby('race_id')['nom'].transform('size')
    d['top3'] = d['pa_num'] <= 3
    md = [f"# Modèle {version} : à partir de quelle probabilité un cheval est-il quasi sûr d'être placé ?", "",
          "Proba = celle affichée dans le dashboard (chance de GAGNER, calibrée, somme = 100 % par course). "
          "« Placé » = dans les 3 premiers.", ""]
    md += _seuil_tables(d, f"Toutes les courses depuis la coupure de {version} ({cutoff})")
    if trained:
        after = d[d['date'] > trained]
        if after['race_id'].nunique() >= 30:
            md += _seuil_tables(after, f"Seulement les courses après son entraînement ({trained}), jamais vues du tout")
    return _publish(md)


# ── Rentabilité du simple placé (proba du modèle x cote) ────────────────────

_RC = re.compile(r'_R(\d+)_(\d+)$')


def _scored_with_odds(storage, min_runners=11):
    """Courses passées notées par le modèle en production, avec la cote
    (cote directe PMU, avant le départ) et le rapport placé réel."""
    from . import model_store, tables
    version = model_store.current_version(storage)
    booster, calib, _, meta = model_store.load_version(storage, version)
    feats = booster.feature_name()
    d = train.load_feature_frame(storage).dropna(subset=['pa_num']).copy()
    cutoff = int(meta.get('metrics', {}).get('cutoff') or 0)
    d = d[d['date'] >= cutoff].copy()
    d['pred'] = booster.predict(d[feats])
    d['cal'] = calib.predict(d['pred'])
    d['prob'] = d['cal'] / d.groupby('race_id')['cal'].transform('sum') * 100
    d['n'] = d.groupby('race_id')['nom'].transform('size')
    d = d[d['n'] >= min_runners].copy()
    num = d['race_id'].str.extract(_RC)
    d['num_course'] = pd.to_numeric(num[1], errors='coerce')
    d['nom_u'] = d['nom'].astype(str).str.strip().str.upper()

    ch = tables.read_table(storage, 'chevaux')
    ch = ch.assign(date=pd.to_numeric(ch['date'], errors='coerce'),
                   num_course=pd.to_numeric(ch['num_course'], errors='coerce'),
                   nom_u=ch['nom_cheval'].astype(str).str.strip().str.upper(),
                   cote=pd.to_numeric(ch['cote_directe'], errors='coerce'),
                   npart_ch=pd.to_numeric(ch['numero_partant'], errors='coerce'))
    ch = ch.drop_duplicates(['date', 'code_hippo', 'num_course', 'nom_u'])
    d = d.drop(columns=['cote'], errors='ignore')   # cote du jour seulement, vide dans l'historique
    d = d.merge(ch[['date', 'code_hippo', 'num_course', 'nom_u', 'cote', 'npart_ch']],
                left_on=['date', 'hippo', 'num_course', 'nom_u'],
                right_on=['date', 'code_hippo', 'num_course', 'nom_u'], how='left')
    d['npart'] = pd.to_numeric(d['npart'], errors='coerce').fillna(d['npart_ch'])

    rap = tables.read_table(storage, 'rapports')
    rap = rap.assign(date=pd.to_numeric(rap['date'], errors='coerce'),
                     num_course=pd.to_numeric(rap['num_course'], errors='coerce'),
                     num_pmu=pd.to_numeric(rap['num_pmu'], errors='coerce'),
                     rp=pd.to_numeric(rap['rapport_place'], errors='coerce') / 100)
    has_rap = set(zip(rap.loc[rap['rp'].notna(), 'date'], rap.loc[rap['rp'].notna(), 'code_hippo'],
                      rap.loc[rap['rp'].notna(), 'num_course']))
    rap = rap.dropna(subset=['rp']).drop_duplicates(['date', 'code_hippo', 'num_course', 'num_pmu'])
    d = d.merge(rap[['date', 'code_hippo', 'num_course', 'num_pmu', 'rp']].rename(columns={'code_hippo': 'hippo_r'}),
                left_on=['date', 'hippo', 'num_course', 'npart'],
                right_on=['date', 'hippo_r', 'num_course', 'num_pmu'], how='left')
    d['race_has_rap'] = [k in has_rap for k in zip(d['date'], d['hippo'], d['num_course'])]
    d['payout'] = d['rp'].fillna(0.0)
    return d, version, cutoff


def _strategy(d, mask):
    x = d[mask]
    if not len(x):
        return None
    paid = x['payout'] > 0
    return {'n': len(x), 'races': x['race_id'].nunique(), 'pct': paid.mean(),
            'avg': x.loc[paid, 'payout'].mean() if paid.any() else 0.0,
            'roi': x['payout'].sum() / len(x) - 1}


def _grid(d, min_bets):
    rows = []
    for pmin in (0, 5, 8, 10, 12, 15, 20, 25, 30):
        for cmin in (1, 3, 5, 8, 10, 15, 20):
            r = _strategy(d, (d['prob'] >= pmin) & (d['cote'] >= cmin))
            if r and r['n'] >= min_bets:
                rows.append({'regle': f"proba ≥ {pmin} % et cote ≥ {cmin}", 'pmin': pmin, 'cmin': cmin,
                             'ev': None, **r})
    for ev in (1.0, 1.2, 1.5, 2.0, 2.5, 3.0):
        r = _strategy(d, d['prob'] / 100 * d['cote'] >= ev)
        if r and r['n'] >= min_bets:
            rows.append({'regle': f"proba x cote ≥ {ev}", 'pmin': None, 'cmin': None, 'ev': ev, **r})
    return rows


def _rule_mask(d, row):
    if row['ev'] is not None:
        return d['prob'] / 100 * d['cote'] >= row['ev']
    return (d['prob'] >= row['pmin']) & (d['cote'] >= row['cmin'])


def _fmt(r):
    return (f"{r['n']} | {r['races']} | {r['pct']:.0%} | {r['avg']:.2f} € | "
            f"**{r['roi'] * 100:+.1f} €**")


def run_rentabilite(storage):
    d, version, cutoff = _scored_with_odds(storage)
    d = d[d['race_has_rap'] & d['cote'].notna()].copy()
    dates = sorted(d['date'].unique())
    mid = dates[len(dates) // 2] if dates else 0
    A, B = d[d['date'] < mid], d[d['date'] >= mid]
    md = [f"# Simple placé : quand jouer ? (modèle {version}, courses de 11 partants et plus)", "",
          f"{d['race_id'].nunique()} courses depuis {cutoff}, avec cote et rapports connus. "
          f"Mise : 1 € par cheval joué. Résultat = gain ou perte pour **100 € misés** "
          f"(rapport placé réel PMU, mise perdue si non placé).", "",
          f"Les seuils sont **choisis** sur la période 1 (avant {mid}) puis **vérifiés** sur la période 2 "
          f"(à partir de {mid}), que le choix n'a pas vue.", "",
          "| Joue… | Paris | Courses | % payés | Rapport moyen si payé | Pour 100 € |", "|---|---:|---:|---:|---:|---:|"]
    for lab, m in (("tous les chevaux", d['prob'] >= 0),
                   ("le n°1 du modèle", d.groupby('race_id')['prob'].rank(ascending=False, method='first') == 1),
                   ("le favori du marché", d.groupby('race_id')['cote'].rank(method='first') == 1)):
        r = _strategy(d, m)
        md.append(f"| {lab} (toute la période) | {_fmt(r)} |")
    md += ["", "### Les 10 meilleures règles sur la période 1 (au moins 100 paris), et ce qu'elles donnent ensuite", "",
           "| Règle | Période 1 : paris | P1 : pour 100 € | **Période 2 : paris** | **P2 : % payés** | **P2 : pour 100 €** |",
           "|---|---:|---:|---:|---:|---:|"]
    gA = sorted(_grid(A, 100), key=lambda r: -r['roi'])[:10]
    for r in gA:
        rb = _strategy(B, _rule_mask(B, r))
        b = f"{rb['n']} | {rb['pct']:.0%} | **{rb['roi'] * 100:+.1f} €**" if rb else "0 | — | —"
        md.append(f"| {r['regle']} | {r['n']} | {r['roi'] * 100:+.1f} € | {b} |")
    md += ["", "### Carte complète, toute la période : résultat pour 100 € selon proba minimum (lignes) et cote minimum (colonnes)", "",
           "| Proba ≥ / cote ≥ | 1 | 3 | 5 | 8 | 10 | 15 | 20 |", "|---|---:|---:|---:|---:|---:|---:|---:|"]
    for pmin in (0, 5, 8, 10, 12, 15, 20, 25, 30):
        cells = []
        for cmin in (1, 3, 5, 8, 10, 15, 20):
            r = _strategy(d, (d['prob'] >= pmin) & (d['cote'] >= cmin))
            cells.append(f"{r['roi'] * 100:+.0f} ({r['n']})" if r and r['n'] >= 30 else '—')
        md.append(f"| {pmin} % | " + " | ".join(cells) + " |")
    md += ["", "Entre parenthèses : nombre de paris. Moins de 30 paris : non affiché.", "",
           "Rappels : la cote utilisée est la dernière cote PMU avant le départ (au moment du rafraîchissement "
           "de midi, elle peut encore bouger) ; le prélèvement du PMU sur le placé est d'environ 15 à 20 %, "
           "donc « jouer tout » perd mécaniquement.", ""]
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
