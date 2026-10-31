"""Modèle « Profil » : un modèle À PART, construit uniquement à partir du
tracking (et des conditions de la course du jour). Il ne touche pas au modèle
en production : il apprend comment court chaque cheval, puis classe les
partants d'une course.

1. Chaque course trackée passée, chaque cheval (`run_metrics`) :
   fig        chiffre de vitesse : vitesse moyenne du cheval moins la vitesse
              « normale » de ce parcours (hippodrome x distance x surface,
              calculée uniquement sur les mois précédents) et moins l'écart du
              jour (terrain plus ou moins rapide que d'habitude, mesuré sur
              toutes les courses de la réunion). En km/h : 0 = cheval moyen
              sur ce parcours, + = plus rapide. Comparable d'une course à
              l'autre, contrairement aux rangs dans la course.
   fig_adj    le même, en rendant au cheval le temps perdu sur une gêne
   gene_sec   temps perdu sur une gêne : tronçon nettement plus lent que le
              peloton, sans gain de place, suivi d'une reprise (un cheval qui
              cède en fin de course ne compte pas : ni premier tronçon, ni
              400 derniers mètres)
   style      position aux 2 premiers tronçons, en part du peloton (0 = en tête)
   fin_rel    vitesse sur les 600 derniers mètres moins celle du peloton (km/h)
   pace       rythme de la course : vitesse du peloton avant les 600 derniers
              mètres moins sur les 600 derniers, comparé à l'habitude sur cette
              distance (+ = course partie vite, usante pour les meneurs)

2. Profil du cheval avant la course du jour (`profile_features`), sur ses
   courses trackées des 12 derniers mois (strictement antérieures) : niveau
   (dernier chiffre, meilleur des 3 derniers, moyenne pondérée par la
   fraîcheur), tendance, niveau sur la distance et la surface du jour, écart
   avec sa distance de prédilection, style, finish, préférence de rythme,
   gênes récentes.

3. Scénario de la course du jour (`field_features`) : niveau comparé aux
   rivaux, nombre de meneurs parmi eux, style et finish comparés.

Le classement est appris par LightGBM (même recette que la production, avec
ces seules variables), évalué par `run_profil` contre le modèle en production
et contre les cotes, sur 4 périodes de test successives."""

import logging
from collections import defaultdict

import numpy as np
import pandas as pd

from .features import _classify_surface

log = logging.getLogger(__name__)

MIN_RUNNERS = 4          # coureurs trackés valides pour exploiter une course
PAR_MIN_N = 8            # courses passées nécessaires pour une vitesse de référence
GENE_KMH = 2.0           # ralentissement par rapport au peloton signalant une gêne
GENE_REPRISE_KMH = 1.5   # reprise au tronçon suivant
HALF_LIFE_DAYS = 180     # poids d'une course : divisé par 2 tous les 6 mois
WINDOW_DAYS = 365

RUN_COLS = ['pf_fig', 'pf_fig_adj', 'pf_gene_sec', 'pf_style', 'pf_fin_rel', 'pf_pace']

PROFILE_FEATURES = ['pf_n', 'pf_days_last', 'pf_fig_last', 'pf_fig_best3', 'pf_fig_wmean', 'pf_fig_trend',
                    'pf_fig_dist', 'pf_fig_surf', 'pf_dist_gap', 'pf_style_mean', 'pf_style_sd',
                    'pf_fin_mean', 'pf_fin_best', 'pf_pace_pref', 'pf_gene_last', 'pf_gene_n5']
FIELD_FEATURES = ['fv_best_vs_field', 'fv_wmean_vs_field', 'fv_rank', 'fv_cov', 'fv_n',
                  'sc_leaders', 'sc_style_vs_field', 'sc_fin_vs_field']
CONTEXT_FEATURES = ['dist', 'piste', 'terrain', 'hippo']
PROFIL_FEATURES = PROFILE_FEATURES + FIELD_FEATURES + CONTEXT_FEATURES


# ── 1. Chiffres de chaque course trackée ────────────────────────────────────

def _valid_segments(tr):
    """(vitesses, positions) des tronçons si le tracking est exploitable."""
    if not isinstance(tr, (list, tuple)) or len(tr) < 4:
        return None
    try:
        sp = np.array([s[2] for s in tr], dtype=float)
        cum = np.array([s[1] for s in tr], dtype=float)
        pos = np.array([np.nan if s[3] is None else s[3] for s in tr], dtype=float)
    except (TypeError, IndexError, ValueError):
        return None
    if np.isnan(sp).any() or (sp < 20).any() or (sp > 80).any():
        return None
    if np.isnan(cum).any() or (np.diff(cum) <= 0).any():
        return None
    return sp, pos


def _expanding_par(levels, values, months, min_n=PAR_MIN_N):
    """Référence de chaque course : médiane des valeurs des courses des mois
    STRICTEMENT antérieurs ayant la même clé, au niveau le plus précis qui a
    au moins `min_n` courses (levels : listes de clés, du plus précis au plus
    large). NaN s'il n'y a pas assez d'historique."""
    n = len(values)
    par = np.full(n, np.nan)
    hist = [defaultdict(list) for _ in levels]
    by_month = defaultdict(list)
    for i, m in enumerate(months):
        by_month[m].append(i)
    for m in sorted(by_month):
        idx = by_month[m]
        cache = {}
        for i in idx:
            for lv, keys in enumerate(levels):
                k = keys[i]
                if (lv, k) not in cache:
                    h = hist[lv].get(k)
                    cache[(lv, k)] = float(np.median(h)) if h is not None and len(h) >= min_n else None
                if cache[(lv, k)] is not None:
                    par[i] = cache[(lv, k)]
                    break
        for i in idx:
            if not np.isnan(values[i]):
                for lv, keys in enumerate(levels):
                    hist[lv][keys[i]].append(values[i])
    return par


def run_metrics(fdf):
    """Ajoute RUN_COLS : chiffres de chaque cheval dans chaque course trackée
    passée (NaN sinon). fdf : sortie de build_features (plat)."""
    fdf = fdf.copy()
    n = len(fdf)
    out = {c: np.full(n, np.nan) for c in RUN_COLS}
    vy = pd.to_numeric(fdf['vy'], errors='coerce').to_numpy(dtype=float)
    dist = pd.to_numeric(fdf['dist'], errors='coerce').to_numpy(dtype=float)
    trs = fdf['tr'].to_numpy() if 'tr' in fdf else np.full(n, None)
    surf = np.array([_classify_surface(t, p) for t, p in
                     zip(fdf['terrain'].astype(object), fdf['piste'].astype(object))], dtype=object)

    race_rows, race_info = [], []
    for rid, idx in fdf.groupby('race_id', sort=False).indices.items():
        segs = {}
        for i in idx:
            v = _valid_segments(trs[i])
            if v is not None:
                segs[i] = v
        if len(segs) < MIN_RUNNERS:
            continue
        m = min(len(s[0]) for s in segs.values())      # tronçons alignés depuis l'arrivée
        rows = list(segs)
        S = np.array([segs[i][0][-m:] for i in rows])
        P = np.array([segs[i][1][-m:] for i in rows])
        med = np.median(S, axis=0)
        n_field = len(idx)
        late = S[:, -3:].mean(axis=1)
        out['pf_fin_rel'][rows] = late - np.median(late)
        early_pos = np.nanmean(P[:, :2], axis=1)
        out['pf_style'][rows] = np.clip((early_pos - 1) / max(n_field - 1, 1), 0, 1)
        pace = np.nan
        if m >= 5:
            pace = float(np.median(S[:, :-3].mean(axis=1)) - np.median(late))
        d = dist[rows[0]]
        for r, i in enumerate(rows):
            lost = 0.0
            for k in range(1, m - 2):
                dev = S[r, k] - med[k]
                if (dev <= -GENE_KMH and (S[r, k + 1] - med[k + 1]) - dev >= GENE_REPRISE_KMH
                        and not (P[r, k] < P[r, k - 1])):
                    lost += 200 / (S[r, k] / 3.6) - 200 / (med[k] / 3.6)
            out['pf_gene_sec'][i] = lost
        v_r = vy[rows]
        v_r = np.where(np.isnan(v_r), S.mean(axis=1), v_r)
        race_rows.append((rows, v_r))
        rid_date = int(fdf['date'].iat[rows[0]])
        hippo = str(fdf['hippo'].iat[rows[0]])
        race_info.append({'date': rid_date, 'month': rid_date // 100, 'hippo': hippo, 'surf': surf[rows[0]],
                          'd100': int(round(d / 100.0)) * 100 if not np.isnan(d) else -1,
                          'd400': int(round(d / 400.0)) * 400 if not np.isnan(d) else -1,
                          'dist': d, 'speed': float(np.median(v_r)), 'pace': pace})

    if not race_info:
        for c, v in out.items():
            fdf[c] = v
        return fdf
    ri = pd.DataFrame(race_info)
    months = ri['month'].to_numpy()
    sp_par = _expanding_par([list(zip(ri['hippo'], ri['d100'], ri['surf'])), list(zip(ri['d100'], ri['surf'])),
                             list(zip(ri['d400'], ri['surf'])), list(ri['d400'])],
                            ri['speed'].to_numpy(dtype=float), months)
    pace_par = _expanding_par([list(zip(ri['d100'], ri['surf'])), list(ri['d400'])],
                              ri['pace'].to_numpy(dtype=float), months)
    # Écart du jour : la réunion entière est-elle plus rapide que d'habitude ?
    # (moyenne réduite vers 0 : une seule course ne suffit pas à juger le terrain)
    ri['dev'] = ri['speed'] - sp_par
    g = ri.groupby(['date', 'hippo'])['dev']
    variant = (g.transform('sum') / (g.transform('count') + 2)).to_numpy()
    for k, (rows, v_r) in enumerate(race_rows):
        if np.isnan(sp_par[k]):
            continue
        base = sp_par[k] + (0.0 if np.isnan(variant[k]) else variant[k])
        out['pf_fig'][rows] = v_r - base
        d = race_info[k]['dist']
        lost = out['pf_gene_sec'][rows]
        if not np.isnan(d) and d > 0:
            t = d / (v_r / 3.6)
            v_adj = d / np.maximum(t - lost, t * 0.9) * 3.6
        else:
            v_adj = v_r
        out['pf_fig_adj'][rows] = v_adj - base
        if not np.isnan(race_info[k]['pace']) and not np.isnan(pace_par[k]):
            out['pf_pace'][rows] = race_info[k]['pace'] - pace_par[k]
    for c, v in out.items():
        fdf[c] = v
    return fdf


# ── 2. Profil du cheval avant la course ─────────────────────────────────────

def profile_features(fdf, window_days=WINDOW_DAYS, half_life=HALF_LIFE_DAYS):
    """PROFILE_FEATURES, calculées sur les courses trackées strictement
    antérieures (fdf doit contenir RUN_COLS)."""
    fdf = fdf.copy()
    n = len(fdf)
    out = {c: np.full(n, np.nan) for c in PROFILE_FEATURES}
    out['pf_n'] = np.zeros(n)
    days = (pd.to_datetime(fdf['date'].astype(int).astype(str), format='%Y%m%d')
            .to_numpy().astype('datetime64[D]').astype(np.int64))
    noms = fdf['nom'].to_numpy()
    dist = pd.to_numeric(fdf['dist'], errors='coerce').to_numpy(dtype=float)
    surf = np.array([_classify_surface(t, p) for t, p in
                     zip(fdf['terrain'].astype(object), fdf['piste'].astype(object))], dtype=object)
    fig = fdf['pf_fig'].to_numpy(dtype=float)
    figa = fdf['pf_fig_adj'].to_numpy(dtype=float)
    gene = fdf['pf_gene_sec'].to_numpy(dtype=float)
    style = fdf['pf_style'].to_numpy(dtype=float)
    fin = fdf['pf_fin_rel'].to_numpy(dtype=float)
    pace = fdf['pf_pace'].to_numpy(dtype=float)
    has = ~np.isnan(figa)
    order = np.lexsort((fdf['is_today'].to_numpy(), days, noms))

    start = 0
    while start < n:
        end = start
        while end < n and noms[order[end]] == noms[order[start]]:
            end += 1
        past_runs = []     # indices des courses chiffrées du cheval, chronologiques
        for i in order[start:end]:
            p = [j for j in past_runs if days[i] - window_days <= days[j] < days[i]]
            if p:
                p = np.array(p)
                age = days[i] - days[p]
                w = 0.5 ** (age / half_life)
                fa = figa[p]
                out['pf_n'][i] = len(p)
                out['pf_days_last'][i] = age[-1]
                out['pf_fig_last'][i] = fa[-1]
                out['pf_fig_best3'][i] = fa[-3:].max()
                l6, w6 = fa[-6:], w[-6:]
                out['pf_fig_wmean'][i] = np.sum(l6 * w6) / np.sum(w6)
                if len(fa) >= 2:
                    out['pf_fig_trend'][i] = fa[-1] - fa[-5:-1].mean()
                if not np.isnan(dist[i]):
                    near = np.abs(dist[p] - dist[i]) <= 0.15 * dist[i]
                    if near.any():
                        out['pf_fig_dist'][i] = np.sum(fa[near] * w[near]) / np.sum(w[near])
                    best = p[-6:][np.argmax(l6)]
                    if not np.isnan(dist[best]):
                        out['pf_dist_gap'][i] = dist[i] - dist[best]
                same = surf[p] == surf[i]
                if same.any():
                    out['pf_fig_surf'][i] = np.sum(fa[same] * w[same]) / np.sum(w[same])
                st = style[p[-5:]]
                st = st[~np.isnan(st)]
                if len(st):
                    out['pf_style_mean'][i] = st.mean()
                    if len(st) >= 2:
                        out['pf_style_sd'][i] = st.std()
                fr = fin[p[-5:]]
                fr = fr[~np.isnan(fr)]
                if len(fr):
                    out['pf_fin_mean'][i] = fr.mean()
                    out['pf_fin_best'][i] = fr.max()
                # Préférence de rythme : niveau dans les courses parties vite
                # moins dans les courses lentes (réduit si peu de courses)
                pc, fp = pace[p[-8:]], fa[-8:]
                fast, slow = fp[pc > 0], fp[pc <= 0]
                if len(fast) and len(slow):
                    k = min(len(fast), len(slow))
                    out['pf_pace_pref'][i] = (fast.mean() - slow.mean()) * k / (k + 1)
                out['pf_gene_last'][i] = gene[p[-1]]
                out['pf_gene_n5'][i] = float((gene[p[-5:]] > 0.1).sum())
            if has[i]:
                past_runs.append(i)
        start = end
    for c, v in out.items():
        fdf[c] = v
    return fdf


# ── 3. Scénario de la course du jour ────────────────────────────────────────

def field_features(fdf):
    fdf = fdf.copy()
    n = len(fdf)
    out = {c: np.full(n, np.nan) for c in FIELD_FEATURES}
    best = fdf['pf_fig_best3'].to_numpy(dtype=float)
    wm = fdf['pf_fig_wmean'].to_numpy(dtype=float)
    st = fdf['pf_style_mean'].to_numpy(dtype=float)
    fi = fdf['pf_fin_mean'].to_numpy(dtype=float)
    for idx in fdf.groupby('race_id', sort=False).indices.values():
        k = len(idx)
        out['fv_n'][idx] = k
        known = ~np.isnan(wm[idx])
        out['fv_cov'][idx] = known.mean()
        if known.sum() >= 2:
            vals = wm[idx][known]
            r = (-vals).argsort(kind='stable').argsort(kind='stable')
            out['fv_rank'][idx[known]] = r / (len(vals) - 1)
        for pos, i in enumerate(idx):
            o = np.delete(idx, pos)
            if not np.isnan(best[i]) and not np.isnan(best[o]).all():
                out['fv_best_vs_field'][i] = best[i] - np.nanmax(best[o])
            if not np.isnan(wm[i]) and not np.isnan(wm[o]).all():
                out['fv_wmean_vs_field'][i] = wm[i] - np.nanmean(wm[o])
            so = st[o]
            if not np.isnan(so).all():
                out['sc_leaders'][i] = float((so < 0.25).sum())
                if not np.isnan(st[i]):
                    out['sc_style_vs_field'][i] = st[i] - np.nanmean(so)
            fo = fi[o]
            if not np.isnan(fi[i]) and not np.isnan(fo).all():
                out['sc_fin_vs_field'][i] = fi[i] - np.nanmean(fo)
    for c, v in out.items():
        fdf[c] = v
    return fdf


def profil_features(fdf):
    """Toutes les variables du modèle Profil (fdf : sortie de build_features)."""
    return field_features(profile_features(run_metrics(fdf)))


# ── Évaluation (lecture seule) ──────────────────────────────────────────────

def _oos(fdf, variants, folds):
    """Prédictions hors échantillon de chaque variante, période par période
    (le modèle n'apprend que sur ce qui précède). Retourne (lignes de test
    avec une colonne de score par variante et le n° de période, importances
    du dernier modèle de chaque variante)."""
    from . import train
    ready = fdf.dropna(subset=['pa_num']).sort_values(['date', 'race_id']).reset_index(drop=True)
    dates = sorted(ready['date'].unique())
    parts, imp = [], {}
    for f, (tf, vf) in enumerate(folds):
        lo = dates[int(len(dates) * tf)]
        valid = ready[ready['date'] >= lo]
        if vf is not None and int(len(dates) * (tf + vf)) < len(dates):
            valid = valid[valid['date'] < dates[int(len(dates) * (tf + vf))]]
        valid = valid.assign(fold=f)
        for name, feats in variants.items():
            model, _, _, _ = train.train_model(fdf, features=feats, train_frac=tf, valid_frac=vf, label_mode='win')
            valid[name] = model.predict(valid[feats])
            imp[name] = dict(zip(feats, model.feature_importance('gain')))
        parts.append(valid)
    return pd.concat(parts, ignore_index=True), imp


def _race_scores(d, col):
    """Par course : (n°1 gagne, n°1 placé, nb du top 3 dans les 3 premiers)."""
    out = []
    for _, g in d.groupby('race_id', sort=False):
        if g['win'].sum() == 0 or len(g) < 4:
            continue
        top = g.loc[g[col].idxmax()]
        t3 = g.nlargest(3, col)
        out.append((int(top['win'] == 1), int(top['pa_num'] <= 3), int(t3['pa_num'].le(3).sum())))
    return np.array(out) if out else np.zeros((0, 3))


def _picks(d, col):
    """Le n°1 de chaque course pour la variante `col`, avec son rang de cote."""
    i = d.groupby('race_id', sort=False)[col].idxmax()
    return d.loc[i]


MARKET_BANDS = ((1, 1, 'favori'), (2, 3, '2e-3e favori'), (4, 6, '4e-6e'), (7, 99, '7e et au-delà'))


def run_profil(storage):
    from .experiment import FOLDS, _attach_odds, _attach_payouts, _bucket_table, _publish
    from .features import FEATURES
    from .train import load_feature_frame

    fdf = profil_features(load_feature_frame(storage))
    fdf = _attach_odds(storage, fdf)
    ready = fdf.dropna(subset=['pa_num'])
    tracked_runs = int(fdf['pf_fig'].notna().sum())
    md = ["# Modèle Profil (tracking seul) — modèle à part", "",
          "Construit uniquement sur les tronçons : chiffre de vitesse corrigé du parcours et du terrain du jour, "
          "temps perdu sur les gênes, style, finish, rythme de course, profil de chaque cheval et scénario "
          "de la course. Rien n'est écrit : ni modèle enregistré, ni production modifiée.", "",
          f"- Courses-chevaux chiffrées (tracking exploitable et référence connue) : {tracked_runs}",
          f"- Partants avec au moins une course chiffrée dans les 12 mois : {ready['pf_n'].gt(0).mean():.0%}",
          f"- Courses où au moins 70 % du peloton a un profil : "
          f"{ready.groupby('race_id')['fv_cov'].first().ge(0.7).mean():.0%}", ""]

    cut = sorted(ready['date'].unique())[int(ready['date'].nunique() * 0.8)]
    recent = ready[ready['date'] >= cut]
    md += [f"## Signaux bruts (courses depuis {cut})", ""]
    md += _bucket_table(recent, 'fv_rank', [-0.01, 0.0, 0.25, 0.5, 0.75, 1.0],
                        ['meilleur chiffre du peloton', 'haut', 'milieu haut', 'milieu bas', 'bas'],
                        "Rang du chiffre de vitesse moyen dans le peloton") + [""]
    md += _bucket_table(recent, 'fv_best_vs_field', [-np.inf, -3, -1.5, -0.5, 0, 1, np.inf],
                        ['> 3 km/h derrière le meilleur rival', '1,5 à 3 derrière', '0,5 à 1,5 derrière',
                         'au contact (< 0,5)', 'devant de 0 à 1', 'devant de plus de 1 km/h'],
                        "Meilleur chiffre (3 dernières courses) comparé au meilleur rival") + [""]
    md += _bucket_table(recent, 'pf_gene_last', [-0.01, 0.0, 0.3, 0.8, np.inf],
                        ['aucune gêne', 'petite gêne (< 0,3 s)', 'gêne nette (0,3 à 0,8 s)', 'grosse gêne (> 0,8 s)'],
                        "Gêne lors de sa dernière course trackée") + [""]

    variants = {'v9 (production)': FEATURES, 'Profil seul': PROFIL_FEATURES,
                'v9 + Profil (pour info)': FEATURES + [f for f in PROFIL_FEATURES if f not in FEATURES]}
    d, imp = _oos(fdf, variants, FOLDS)
    d = _attach_payouts(storage, d)
    names = list(variants)
    folds = sorted(d['fold'].unique())

    def period(f):
        x = d[d['fold'] == f]['date']
        return f"{str(x.min())[4:6]}/{str(x.min())[2:4]}→{str(x.max())[4:6]}/{str(x.max())[2:4]}"

    md += ["## Modèles, sur 4 périodes de test successives", "",
           "Sur les 3 chevaux classés en tête, nombre moyen qui finissent dans les 3 premiers "
           "(courses d'au moins 4 partants) :", "",
           "| | " + " | ".join(period(f) for f in folds) + " | Moyenne | N°1 gagnant | N°1 placé |",
           "|---|" + "---:|" * (len(folds) + 3)]
    for nm in names:
        per = [_race_scores(d[d['fold'] == f], nm) for f in folds]
        allr = np.vstack(per)
        md.append(f"| {nm} | " + " | ".join(f"{p[:, 2].mean():.2f}" for p in per)
                  + f" | **{allr[:, 2].mean():.3f}** | {allr[:, 0].mean():.1%} | {allr[:, 1].mean():.1%} |")
    covered = d[d['fv_cov'] >= 0.7]
    md += ["", f"Même mesure sur les seules courses où au moins 70 % du peloton a un profil "
           f"({covered['race_id'].nunique()} courses) :", "",
           "| | Top 3 dans les 3 premiers | N°1 gagnant | N°1 placé |", "|---|---:|---:|---:|"]
    for nm in names:
        r = _race_scores(covered, nm)
        if len(r):
            md.append(f"| {nm} | **{r[:, 2].mean():.3f}** | {r[:, 0].mean():.1%} | {r[:, 1].mean():.1%} |")
    md += ["", f"{d['race_id'].nunique()} courses de test. Un écart de moins de ~0,02 sur la moyenne est dans le bruit."]

    # Ce que le modèle voit et que le marché ne voit pas
    bet = d[d['race_has_rap'] & d['cote'].notna()].copy()
    bet['mk_rank'] = bet.groupby('race_id')['cote'].rank(method='min')
    md += ["", "## Le n°1 du modèle selon son rang dans les cotes", "",
           "Mise de 1 € sur le n°1 de chaque course, vrais rapports PMU (gain net pour 100 € misés). "
           "Les lignes « 4e-6e » et « 7e et au-delà » montrent si le modèle repère des chevaux que le "
           "marché sous-estime.", "",
           "| Modèle | N°1 du modèle | Paris | % gagnant | Simple gagnant | % placé | Simple placé |",
           "|---|---|---:|---:|---:|---:|---:|"]
    for nm in names[:2]:
        pk = _picks(bet, nm)
        for lo, hi, lab in MARKET_BANDS + ((1, 99, '**tous**'),):
            x = pk[pk['mk_rank'].between(lo, hi)]
            if len(x):
                md.append(f"| {nm} | {lab} | {len(x)} | {(x['pay_g'] > 0).mean():.0%} | "
                          f"{(x['pay_g'].mean() - 1) * 100:+.1f} € | {(x['pay_p'] > 0).mean():.0%} | "
                          f"{(x['pay_p'].mean() - 1) * 100:+.1f} € |")
    p9, pp = _picks(bet, names[0]).set_index('race_id'), _picks(bet, names[1]).set_index('race_id')
    common = p9.index.intersection(pp.index)
    diff = [r for r in common if p9.at[r, 'nom'] != pp.at[r, 'nom']]
    if diff:
        a, b = p9.loc[diff], pp.loc[diff]
        md += ["", f"Courses où les deux modèles ne choisissent pas le même n°1 : {len(diff)} sur {len(common)}.", "",
               "| N°1 de | % gagnant | Simple gagnant | % placé | Simple placé |", "|---|---:|---:|---:|---:|"]
        for lab, x in ((names[0], a), (names[1], b)):
            md.append(f"| {lab} | {(x['pay_g'] > 0).mean():.0%} | {(x['pay_g'].mean() - 1) * 100:+.1f} € | "
                      f"{(x['pay_p'] > 0).mean():.0%} | {(x['pay_p'].mean() - 1) * 100:+.1f} € |")

    top = sorted(imp[names[1]].items(), key=lambda kv: -kv[1])
    tot = sum(v for _, v in top) or 1
    md += ["", "## Ce qui compte pour le modèle Profil (dernière période)", "",
           "| Variable | Part de l'importance |", "|---|---:|"]
    md += [f"| {k} | {v / tot:.1%} |" for k, v in top[:15]]
    return _publish(md)
