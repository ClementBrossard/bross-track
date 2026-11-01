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
   positions  (en part du peloton, 0 = en tête) :
     entry      position à l'entrée des 600 derniers mètres
     gain600    places gagnées sur les 600 derniers mètres (+ = remonte)
     lost_mid   places perdues en cours de course avant les 600 derniers
                mètres (repoussé, enfermé qui recule)
     bloque     enfermé dans la ligne droite : ralenti sans gagner de place
                alors qu'il n'est pas en tête, puis accélération nette dès
                qu'il trouve l'ouverture ; ou meilleur finish du tiers le plus
                rapide mais arrivée dans la seconde moitié (vitesse non
                convertie)
   fig_pa     chiffre corrigé du rythme : une course partie lentement donne
              des temps lents à tout le peloton sans que les chevaux soient
              moins bons (coefficient appris sur les mois précédents)
   close      chiffre de finish : vitesse sur les 600 derniers mètres comparée
              à la vitesse habituelle de ce parcours sur ces 600 mètres (et à
              l'écart du jour), indépendant du rythme du début de course
   pace       rythme de la course : vitesse du peloton avant les 600 derniers
              mètres moins sur les 600 derniers, comparé à l'habitude sur cette
              distance (+ = course partie vite, usante pour les meneurs)

2. Profil du cheval avant la course du jour (`profile_features`), sur ses
   courses trackées des 12 derniers mois (strictement antérieures) : niveau
   (dernier chiffre, meilleur des 3 derniers, moyenne pondérée par la
   fraîcheur), tendance, niveau sur la distance et la surface du jour, écart
   avec sa distance de prédilection, style, finish, préférence de rythme,
   gênes récentes.

   Niveau de course : allocation des courses passées comparée à celle du
   jour (montée ou descente de catégorie), chiffre obtenu dans des courses
   au moins de ce niveau (un cheval qui domine en bas n'a peut-être jamais
   couru aussi vite en haut), et pente « chiffre selon le niveau » propre au
   cheval. Positions : place habituelle à l'entrée de la ligne droite,
   capacité à remonter quand il est dans la seconde moitié du peloton
   (s'extraire), capacité à tenir quand il est devant, places perdues en
   course, fréquence des courses où il a été enfermé. Poids : écart avec le poids de sa dernière course et de sa
   meilleure course.

3. Scénario de la course du jour (`field_features`) : niveau comparé aux
   rivaux, niveau du peloton (chiffres et valeurs handicap), nombre de
   meneurs parmi eux, style et finish comparés, poids comparé au peloton,
   corde (en part du peloton) et biais de corde du parcours : chiffre moyen
   des chevaux partis à la corde / au milieu / à l'extérieur sur ce parcours,
   mesuré sur les mois précédents. Le modèle peut ainsi croiser corde et
   style (un meneur à l'extérieur, un finisseur à la corde qui risque d'être
   enfermé) et corde et gênes.

4. Les adversaires de ses courses passées (`opposition_features`) : chaque
   course passée est décrite par ses concurrents, tels qu'on les connaissait
   AVANT cette course — Elo, chiffres de vitesse, nombre de meneurs
   habituels. On en tire : le niveau des pelotons affrontés, l'Elo des
   chevaux qu'il a battus et de ceux qui l'ont battu (battu par des cracks ou
   par des carnes ?), sa performance par rapport au niveau attendu du
   peloton, son chiffre face à un peloton au moins aussi fort que celui du
   jour, et son chiffre selon qu'il y avait beaucoup de meneurs ou non.
   Aujourd'hui : son Elo comparé aux rivaux.

L'écart du jour (terrain rapide ou lent) est mesuré après avoir retiré
l'effet du niveau des courses : une réunion de Groupe court plus vite parce
que les chevaux sont meilleurs, pas parce que le terrain est rapide.

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

RUN_COLS = ['pf_fig', 'pf_fig_adj', 'pf_gene_sec', 'pf_style', 'pf_fin_rel', 'pf_pace',
            'pf_entry', 'pf_gain600', 'pf_lost_mid', 'pf_bloque', 'pf_fig_pa', 'pf_close']

PROFILE_FEATURES = ['pf_n', 'pf_days_last', 'pf_fig_last', 'pf_fig_best3', 'pf_fig_wmean', 'pf_fig_trend',
                    'pf_fig_dist', 'pf_fig_surf', 'pf_dist_gap', 'pf_style_mean', 'pf_style_sd',
                    'pf_fin_mean', 'pf_fin_best', 'pf_pace_pref', 'pf_gene_last', 'pf_gene_n5', 'pf_gene_rate',
                    # niveau de course
                    'pf_class_gap', 'pf_alloc_max_gap', 'pf_fig_at_level', 'pf_n_at_level', 'pf_class_slope',
                    # poids
                    'pf_pd_delta_last', 'pf_pd_delta_best',
                    # positions
                    'pf_entry_mean', 'pf_gain600_mean', 'pf_escape', 'pf_hold_front', 'pf_lost_mid_mean',
                    'pf_bloque_last', 'pf_bloque_rate',
                    # chiffres fiabilisés
                    'pf_figpa_wmean', 'pf_figpa_best3', 'pf_close_wmean', 'pf_close_best3']
FIELD_FEATURES = ['fv_best_vs_field', 'fv_wmean_vs_field', 'fv_rank', 'fv_cov', 'fv_n',
                  'fl_fig_mean', 'fl_hv_mean',
                  'sc_leaders', 'sc_style_vs_field', 'sc_fin_vs_field', 'sc_entry_vs_field',
                  'pd_vs_field', 'cd_pct', 'cd_bias', 'fv_figpa_vs_field', 'fv_close_vs_field']
OPP_FEATURES = ['elo', 'elo_vs_field', 'elo_rank',
                'op_field_elo_wmean', 'op_field_elo_vs_today', 'op_beaten_elo_best', 'op_beaten_elo_vs_field',
                'op_ahead_elo_mean', 'op_ahead_elo_vs_field', 'op_perf_vs_exp', 'op_fig_strong_field',
                'op_n_strong_field', 'op_fig_vs_leaders']
# Sans l'hippodrome : le parcours est déjà neutralisé dans les chiffres, et la
# variable faisait apprendre du bruit (expérience : 1,322 sans contre 1,296 avec).
JOCKEY_FEATURES = ['jt_n', 'jt_gene_rate', 'jt_bloque_rate', 'jt_escape', 'jt_lost_mid', 'jt_plusvalue',
                   'jt_tactic', 'jt_plusvalue_vs_field', 'jt_escape_vs_field']
CONTEXT_FEATURES = ['dist', 'piste', 'terrain', 'alloc', 'cat']
POSITION_FEATURES = ['pf_entry_mean', 'pf_gain600_mean', 'pf_escape', 'pf_hold_front', 'pf_lost_mid_mean',
                     'pf_bloque_last', 'pf_bloque_rate', 'sc_entry_vs_field']
ALLOC_BINS = (12000, 18000, 25000, 35000, 50000, 80000, 150000, 400000)
LEVEL_RATIO = 1.25       # « au moins ce niveau » : allocation >= celle du jour / 1,25
PROFIL_FEATURES = PROFILE_FEATURES + FIELD_FEATURES + OPP_FEATURES + JOCKEY_FEATURES + CONTEXT_FEATURES
NEW_FIG_FEATURES = ['pf_figpa_wmean', 'pf_figpa_best3', 'pf_close_wmean', 'pf_close_best3',
                    'fv_figpa_vs_field', 'fv_close_vs_field']


def _alloc_bucket(a):
    return -1 if a is None or np.isnan(a) else int(np.searchsorted(ALLOC_BINS, a))


def _num(fdf, c):
    return pd.to_numeric(fdf[c], errors='coerce').to_numpy(dtype=float) if c in fdf else np.full(len(fdf), np.nan)


def _surfaces(fdf):
    return np.array([_classify_surface(t, p) for t, p in
                     zip(fdf['terrain'].astype(object), fdf['piste'].astype(object))], dtype=object)


def _d100(d):
    return np.where(np.isnan(d), -1, np.round(np.nan_to_num(d) / 100.0) * 100).astype(int)


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


def _expanding_slope(x, y, months, min_n=50):
    """Pente de y selon x, estimée sur les mois STRICTEMENT antérieurs."""
    out = np.full(len(x), np.nan)
    sx = sy = sxx = sxy = 0.0
    nn = 0
    by_month = defaultdict(list)
    for i, m in enumerate(months):
        by_month[m].append(i)
    for m in sorted(by_month):
        idx = by_month[m]
        if nn >= min_n:
            var = sxx / nn - (sx / nn) ** 2
            if var > 1e-9:
                out[idx] = (sxy / nn - sx / nn * sy / nn) / var
        for i in idx:
            if not np.isnan(x[i]) and not np.isnan(y[i]):
                sx += x[i]; sy += y[i]; sxx += x[i] * x[i]; sxy += x[i] * y[i]; nn += 1
    return out


def run_metrics(fdf):
    """Ajoute RUN_COLS : chiffres de chaque cheval dans chaque course trackée
    passée (NaN sinon). fdf : sortie de build_features (plat)."""
    fdf = fdf.copy()
    n = len(fdf)
    out = {c: np.full(n, np.nan) for c in RUN_COLS}
    vy = pd.to_numeric(fdf['vy'], errors='coerce').to_numpy(dtype=float)
    dist = pd.to_numeric(fdf['dist'], errors='coerce').to_numpy(dtype=float)
    alloc = _num(fdf, 'alloc')
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
        scale = max(n_field - 1, 1)
        DEV = S - med
        fin200_rank = (-S[:, -1]).argsort(kind='stable').argsort(kind='stable') / max(len(rows) - 1, 1)
        for r, i in enumerate(rows):
            pr = P[r]
            # Gêne : ralenti nettement par rapport au peloton ET des places
            # perdues sur ce tronçon, puis reprise de vitesse (un cheval qui
            # cède ne reprend pas)
            lost = 0.0
            for k in range(1, m - 2):
                if (DEV[r, k] <= -GENE_KMH and DEV[r, k + 1] - DEV[r, k] >= GENE_REPRISE_KMH
                        and pr[k] > pr[k - 1]):
                    lost += 200 / (S[r, k] / 3.6) - 200 / (med[k] / 3.6)
            out['pf_gene_sec'][i] = lost
            if m < 5 or np.isnan(pr[-4]) or np.isnan(pr[-1]):
                continue
            out['pf_entry'][i] = (pr[-4] - 1) / scale
            out['pf_gain600'][i] = (pr[-4] - pr[-1]) / scale
            mid = np.diff(pr[1:-3]) if m >= 6 else np.array([])
            mid = mid[~np.isnan(mid)]
            out['pf_lost_mid'][i] = float(mid[mid > 0].sum()) / scale if len(mid) else 0.0
            bloque = False
            for k in (m - 3, m - 2):       # 600-400 et 400-200
                if (DEV[r, k] <= -0.5 and not pr[k] < pr[k - 1] and (pr[k - 1] - 1) / scale > 0.2
                        and DEV[r, k + 1] - DEV[r, k] >= 2.0):
                    bloque = True
            if fin200_rank[r] <= 1 / 3 and (pr[-1] - 1) / scale > 0.5:
                bloque = True
            out['pf_bloque'][i] = float(bloque)
        v_r = vy[rows]
        v_r = np.where(np.isnan(v_r), S.mean(axis=1), v_r)
        race_rows.append((rows, v_r, late))
        rid_date = int(fdf['date'].iat[rows[0]])
        hippo = str(fdf['hippo'].iat[rows[0]])
        race_info.append({'date': rid_date, 'month': rid_date // 100, 'hippo': hippo, 'surf': surf[rows[0]],
                          'd100': int(round(d / 100.0)) * 100 if not np.isnan(d) else -1,
                          'd400': int(round(d / 400.0)) * 400 if not np.isnan(d) else -1,
                          'dist': d, 'speed': float(np.median(v_r)), 'pace': pace,
                          'close': float(np.median(late)),
                          'abk': _alloc_bucket(alloc[rows[0]])})

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
    # On retire d'abord l'effet du niveau (allocation) : à parcours égal, une
    # course mieux dotée va plus vite parce que les chevaux sont meilleurs.
    # Moyenne réduite vers 0 : une seule course ne suffit pas à juger le terrain.
    resid = ri['speed'].to_numpy(dtype=float) - sp_par
    class_off = np.nan_to_num(_expanding_par([list(ri['abk'])], resid, months))
    ri['dev'] = resid - class_off
    g = ri.groupby(['date', 'hippo'])['dev']
    variant = (g.transform('sum') / (g.transform('count') + 2)).to_numpy()
    # Même chose pour les 600 derniers mètres
    levels = [list(zip(ri['hippo'], ri['d100'], ri['surf'])), list(zip(ri['d100'], ri['surf'])),
              list(zip(ri['d400'], ri['surf'])), list(ri['d400'])]
    cl_par = _expanding_par(levels, ri['close'].to_numpy(dtype=float), months)
    cl_res = ri['close'].to_numpy(dtype=float) - cl_par
    ri['cdev'] = cl_res - np.nan_to_num(_expanding_par([list(ri['abk'])], cl_res, months))
    gc = ri.groupby(['date', 'hippo'])['cdev']
    cl_var = (gc.transform('sum') / (gc.transform('count') + 2)).to_numpy()
    # Effet du rythme sur les temps : pente (mois précédents) de l'écart de
    # vitesse de la course (après niveau et jour) selon le rythme
    pace_norm = ri['pace'].to_numpy(dtype=float) - pace_par
    beta = _expanding_slope(pace_norm, ri['dev'].to_numpy(dtype=float) - np.nan_to_num(variant), months)
    for k, (rows, v_r, late) in enumerate(race_rows):
        if not np.isnan(cl_par[k]):
            out['pf_close'][rows] = late - cl_par[k] - (0.0 if np.isnan(cl_var[k]) else cl_var[k])
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
        pn = 0.0 if np.isnan(pace_norm[k]) else pace_norm[k]
        out['pf_fig_pa'][rows] = out['pf_fig_adj'][rows] - (0.0 if np.isnan(beta[k]) else beta[k]) * pn
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
    entry, gain6 = fdf['pf_entry'].to_numpy(dtype=float), fdf['pf_gain600'].to_numpy(dtype=float)
    lmid, blq = fdf['pf_lost_mid'].to_numpy(dtype=float), fdf['pf_bloque'].to_numpy(dtype=float)
    figpa, clos = fdf['pf_fig_pa'].to_numpy(dtype=float), fdf['pf_close'].to_numpy(dtype=float)
    poids = _num(fdf, 'pd')
    la = np.log(np.where(_num(fdf, 'alloc') > 0, _num(fdf, 'alloc'), np.nan))
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
                out['pf_gene_rate'][i] = float((gene[p] > 0.1).mean())
                for arr, wm_c, b3_c in ((figpa, 'pf_figpa_wmean', 'pf_figpa_best3'),
                                        (clos, 'pf_close_wmean', 'pf_close_best3')):
                    a6, ww = arr[p[-6:]], w[-6:]
                    ok6 = ~np.isnan(a6)
                    if ok6.any():
                        out[wm_c][i] = np.sum(a6[ok6] * ww[ok6]) / np.sum(ww[ok6])
                        a3 = arr[p[-3:]]
                        out[b3_c][i] = np.nanmax(a3) if not np.isnan(a3).all() else np.nan
                # Positions
                en, g6 = entry[p], gain6[p]
                okp = ~np.isnan(en) & ~np.isnan(g6)
                if okp.any():
                    en, g6, bq = en[okp], g6[okp], blq[p][okp]
                    out['pf_entry_mean'][i] = en[-5:].mean()
                    out['pf_gain600_mean'][i] = g6[-5:].mean()
                    out['pf_lost_mid_mean'][i] = np.nanmean(lmid[p][okp][-5:])
                    out['pf_bloque_last'][i] = bq[-1]
                    out['pf_bloque_rate'][i] = bq.mean()
                    back, front = en > 0.5, en < 1 / 3
                    if back.any():       # s'extraire : remonte-t-il quand il est derrière ?
                        out['pf_escape'][i] = g6[back].mean() * back.sum() / (back.sum() + 1)
                    if front.any():      # tenir : se fait-il passer quand il est devant ?
                        out['pf_hold_front'][i] = g6[front].mean() * front.sum() / (front.sum() + 1)
                # Poids : par rapport à sa dernière course et à sa meilleure
                out['pf_pd_delta_last'][i] = poids[i] - poids[p[-1]]
                out['pf_pd_delta_best'][i] = poids[i] - poids[p[-6:][np.argmax(l6)]]
                # Niveau de course
                lp = la[p]
                okl = ~np.isnan(lp)
                if okl.any() and not np.isnan(la[i]):
                    out['pf_class_gap'][i] = la[i] - np.sum(lp[okl] * w[okl]) / np.sum(w[okl])
                    out['pf_alloc_max_gap'][i] = np.max(lp[okl]) - la[i]
                    lvl = okl & (np.nan_to_num(lp, nan=-1) >= la[i] - np.log(LEVEL_RATIO))
                    out['pf_n_at_level'][i] = float(lvl.sum())
                    if lvl.any():
                        out['pf_fig_at_level'][i] = np.sum(fa[lvl] * w[lvl]) / np.sum(w[lvl])
                    x, y = lp[-8:], fa[-8:]
                    ok8 = ~np.isnan(x)
                    x, y = x[ok8], y[ok8]
                    if len(x) >= 3 and x.std() > 0.1:
                        slope = np.cov(x, y, bias=True)[0, 1] / x.var()
                        out['pf_class_slope'][i] = slope * len(x) / (len(x) + 3)
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
    ent = fdf['pf_entry_mean'].to_numpy(dtype=float)
    fpa = fdf['pf_figpa_wmean'].to_numpy(dtype=float)
    fcl = fdf['pf_close_wmean'].to_numpy(dtype=float)
    poids, corde, hv = _num(fdf, 'pd'), _num(fdf, 'cd'), _num(fdf, 'hv')
    for idx in fdf.groupby('race_id', sort=False).indices.values():
        k = len(idx)
        out['fv_n'][idx] = k
        if not np.isnan(wm[idx]).all():
            out['fl_fig_mean'][idx] = np.nanmean(wm[idx])
        if not np.isnan(hv[idx]).all():
            out['fl_hv_mean'][idx] = np.nanmean(hv[idx])
        if not np.isnan(poids[idx]).all():
            out['pd_vs_field'][idx] = poids[idx] - np.nanmean(poids[idx])
        out['cd_pct'][idx] = np.clip((corde[idx] - 1) / max(k - 1, 1), 0, 1)
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
            eo = ent[o]
            if not np.isnan(ent[i]) and not np.isnan(eo).all():
                out['sc_entry_vs_field'][i] = ent[i] - np.nanmean(eo)
            for arr, col in ((fpa, 'fv_figpa_vs_field'), (fcl, 'fv_close_vs_field')):
                ao = arr[o]
                if not np.isnan(arr[i]) and not np.isnan(ao).all():
                    out[col][i] = arr[i] - np.nanmean(ao)
    # Biais de corde du parcours : chiffre médian des chevaux partis dans le
    # même tiers (corde / milieu / extérieur) sur ce parcours, mois précédents,
    # moins le chiffre médian de tous les partants du parcours.
    months = (fdf['date'].astype(int) // 100).to_numpy()
    hip = fdf['hippo'].astype(str).to_numpy()
    d100 = _d100(_num(fdf, 'dist'))
    surf = _surfaces(fdf)
    cdp = out['cd_pct']
    terc = np.where(np.isnan(cdp), -1, np.minimum((np.nan_to_num(cdp) * 3).astype(int), 2))
    figa = fdf['pf_fig_adj'].to_numpy(dtype=float)
    figa_t = np.where(terc >= 0, figa, np.nan)
    by_terc = _expanding_par([list(zip(hip, d100, surf, terc)), list(zip(hip, d100, terc))], figa_t, months,
                             min_n=40)
    by_all = _expanding_par([list(zip(hip, d100, surf)), list(zip(hip, d100))], figa_t, months, min_n=40)
    out['cd_bias'] = np.where(terc >= 0, by_terc - by_all, np.nan)
    for c, v in out.items():
        fdf[c] = v
    return fdf


# ── 4. Les adversaires des courses passées ──────────────────────────────────

def opposition_features(fdf, window_days=WINDOW_DAYS, half_life=HALF_LIFE_DAYS, n_last=5):
    """OPP_FEATURES. Chaque course est décrite par ses partants tels qu'on
    les connaissait avant elle (Elo d'avant course, profil d'avant course :
    fl_fig_mean, styles), puis agrégée pour le cheval sur ses courses
    chiffrées strictement antérieures. fdf : sortie de field_features."""
    fdf = fdf.copy()
    n = len(fdf)
    elo = _num(fdf, 'elo')
    pa = fdf['pa_num'].to_numpy(dtype=float)
    figa = fdf['pf_fig_adj'].to_numpy(dtype=float)
    flfig = fdf['fl_fig_mean'].to_numpy(dtype=float)
    st = fdf['pf_style_mean'].to_numpy(dtype=float)
    out = {c: np.full(n, np.nan) for c in OPP_FEATURES}
    out['elo'] = elo
    # Description de chaque course (ce qu'on savait avant le départ) et
    # résultat du cheval face à ces adversaires
    r_elo, r_lead = np.full(n, np.nan), np.full(n, np.nan)
    beaten_max, ahead_mean = np.full(n, np.nan), np.full(n, np.nan)
    for idx in fdf.groupby('race_id', sort=False).indices.values():
        e = elo[idx]
        if not np.isnan(e).all():
            r_elo[idx] = np.nanmean(e)
        r_lead[idx] = float((st[idx] < 0.25).sum())
        if len(idx) >= 2:
            for k, i in enumerate(idx):
                o = np.delete(idx, k)
                eo = elo[o]
                if not np.isnan(e[k]) and not np.isnan(eo).all():
                    out['elo_vs_field'][i] = e[k] - np.nanmean(eo)
            ok = ~np.isnan(e)
            if ok.sum() >= 2:
                r = (-e[ok]).argsort(kind='stable').argsort(kind='stable')
                out['elo_rank'][idx[ok]] = r / (ok.sum() - 1)
        p = pa[idx]
        for k, i in enumerate(idx):
            if np.isnan(p[k]):
                continue
            b = e[(p > p[k]) & ~np.isnan(e)]
            a = e[(p < p[k]) & ~np.isnan(e)]
            if len(b):
                beaten_max[i] = b.max()
            if len(a):
                ahead_mean[i] = a.mean()
    perf = figa - flfig       # son chiffre comparé au niveau attendu du peloton

    days = (pd.to_datetime(fdf['date'].astype(int).astype(str), format='%Y%m%d')
            .to_numpy().astype('datetime64[D]').astype(np.int64))
    noms = fdf['nom'].to_numpy()
    has = ~np.isnan(figa)
    order = np.lexsort((fdf['is_today'].to_numpy(), days, noms))
    start = 0
    while start < n:
        end = start
        while end < n and noms[order[end]] == noms[order[start]]:
            end += 1
        past_runs = []
        for i in order[start:end]:
            p = [j for j in past_runs if days[i] - window_days <= days[j] < days[i]]
            if p:
                p = np.array(p)
                w = 0.5 ** ((days[i] - days[p]) / half_life)
                re = r_elo[p]
                ok = ~np.isnan(re)
                if ok.any():
                    out['op_field_elo_wmean'][i] = np.sum(re[ok] * w[ok]) / np.sum(w[ok])
                    if not np.isnan(r_elo[i]):
                        out['op_field_elo_vs_today'][i] = out['op_field_elo_wmean'][i] - r_elo[i]
                last = p[-n_last:]
                bm = beaten_max[last]
                if not np.isnan(bm).all():
                    out['op_beaten_elo_best'][i] = np.nanmax(bm)
                    if not np.isnan(r_elo[i]):
                        out['op_beaten_elo_vs_field'][i] = np.nanmax(bm) - r_elo[i]
                am = ahead_mean[last]
                if not np.isnan(am).all():
                    out['op_ahead_elo_mean'][i] = np.nanmean(am)
                    if not np.isnan(r_elo[i]):
                        out['op_ahead_elo_vs_field'][i] = np.nanmean(am) - r_elo[i]
                pv = perf[last]
                if not np.isnan(pv).all():
                    out['op_perf_vs_exp'][i] = np.nanmean(pv)
                if not np.isnan(flfig[i]):
                    strong = ~np.isnan(flfig[p]) & (np.nan_to_num(flfig[p], nan=-99) >= flfig[i] - 0.5)
                    out['op_n_strong_field'][i] = float(strong.sum())
                    if strong.any():
                        out['op_fig_strong_field'][i] = np.sum(figa[p][strong] * w[strong]) / np.sum(w[strong])
                rl, fp = r_lead[p[-8:]], figa[p[-8:]]
                many, few = fp[rl >= 2], fp[rl <= 1]
                if len(many) and len(few):
                    k = min(len(many), len(few))
                    out['op_fig_vs_leaders'][i] = (many.mean() - few.mean()) * k / (k + 1)
            if has[i]:
                past_runs.append(i)
        start = end
    for c, v in out.items():
        fdf[c] = v
    return fdf


# ── 5. Le jockey, vu par le tracking (toutes ses montes) ────────────────────

JOCKEY_SHRINK = 20      # montes « fictives » au niveau moyen (0) pour les petits effectifs


def jockey_features(fdf, window_days=WINDOW_DAYS):
    """JOCKEY_FEATURES : ce que le jockey fait de ses chevaux, mesuré sur
    toutes ses montes trackées des 12 derniers mois (strictement antérieures),
    quel que soit le cheval :
      jt_gene_rate     part de montes avec une gêne
      jt_bloque_rate   part de montes enfermé dans la ligne droite
      jt_escape        places gagnées dans la ligne droite quand il y entre
                       dans la seconde moitié du peloton (sortir du paquet)
      jt_lost_mid      places perdues en course avant la ligne droite
      jt_plusvalue     chiffre du cheval ce jour-là moins son niveau habituel
                       d'avant course (+ = le jockey en tire plus que d'habitude)
      jt_tactic        position à l'entrée de la ligne droite moins la position
                       habituelle du cheval (< 0 = mieux placé que d'habitude)
    Les moyennes plus-value / tactique sont réduites vers 0 quand le jockey
    a peu de montes. fdf : sortie d'opposition_features."""
    fdf = fdf.copy()
    n = len(fdf)
    out = {c: np.full(n, np.nan) for c in JOCKEY_FEATURES}
    out['jt_n'] = np.zeros(n)
    figa = fdf['pf_fig_adj'].to_numpy(dtype=float)
    entry = fdf['pf_entry'].to_numpy(dtype=float)
    ride = ~np.isnan(figa)
    metrics = {
        'jt_gene_rate': np.where(ride, (fdf['pf_gene_sec'].to_numpy(dtype=float) > 0.1).astype(float), np.nan),
        'jt_bloque_rate': fdf['pf_bloque'].to_numpy(dtype=float),
        'jt_escape': np.where(entry > 0.5, fdf['pf_gain600'].to_numpy(dtype=float), np.nan),
        'jt_lost_mid': fdf['pf_lost_mid'].to_numpy(dtype=float),
        'jt_plusvalue': figa - fdf['pf_fig_wmean'].to_numpy(dtype=float),
        'jt_tactic': entry - fdf['pf_entry_mean'].to_numpy(dtype=float),
    }
    shrunk = {'jt_plusvalue', 'jt_tactic'}
    days = (pd.to_datetime(fdf['date'].astype(int).astype(str), format='%Y%m%d')
            .to_numpy().astype('datetime64[D]').astype(np.int64))
    jk = fdf['jk'].astype(object).where(fdf['jk'].notna(), None).to_numpy()
    for name, idx in pd.Series(np.arange(n)).groupby(jk, sort=False).indices.items():
        if name is None:
            continue
        idx = np.asarray(idx)
        r = idx[ride[idx]]
        if not len(r):
            continue
        r = r[np.argsort(days[r], kind='stable')]
        rd = days[r]
        lo = np.searchsorted(rd, days[idx] - window_days, 'left')
        hi = np.searchsorted(rd, days[idx], 'left')       # strictement avant le jour
        out['jt_n'][idx] = hi - lo
        for col, arr in metrics.items():
            v = arr[r]
            ok = ~np.isnan(v)
            cs = np.concatenate([[0.0], np.cumsum(np.where(ok, v, 0.0))])
            cn = np.concatenate([[0], np.cumsum(ok)])
            s_, c_ = cs[hi] - cs[lo], cn[hi] - cn[lo]
            if col in shrunk:
                val = s_ / (c_ + JOCKEY_SHRINK)
                val[c_ == 0] = np.nan
            else:
                with np.errstate(invalid='ignore', divide='ignore'):
                    val = np.where(c_ > 0, s_ / np.maximum(c_, 1), np.nan)
            out[col][idx] = val
    pv, es = out['jt_plusvalue'], out['jt_escape']
    for g in fdf.groupby('race_id', sort=False).indices.values():
        if len(g) < 2:
            continue
        for k, i in enumerate(g):
            o = np.delete(g, k)
            for arr, col in ((pv, 'jt_plusvalue_vs_field'), (es, 'jt_escape_vs_field')):
                if not np.isnan(arr[i]) and not np.isnan(arr[o]).all():
                    out[col][i] = arr[i] - np.nanmean(arr[o])
    for c, v in out.items():
        fdf[c] = v
    return fdf


def profil_features(fdf):
    """Toutes les variables du modèle Profil (fdf : sortie de build_features)."""
    return jockey_features(opposition_features(field_features(profile_features(run_metrics(fdf)))))


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


def _stability_table(fdf):
    """Qualité des chiffres : un bon chiffre de vitesse se répète d'une course
    à l'autre (corrélation entre deux courses trackées successives du même
    cheval, à moins de 90 jours) et annonce la suivante."""
    d = fdf[fdf['pf_fig'].notna()].sort_values(['nom', 'date'])
    nxt = d.groupby('nom').shift(-1)
    gap = (pd.to_datetime(nxt['date'].astype('Int64').astype(str), format='%Y%m%d', errors='coerce')
           - pd.to_datetime(d['date'].astype(int).astype(str), format='%Y%m%d')).dt.days
    ok = gap.le(90)
    md = ["## Qualité des chiffres de vitesse", "",
          "Corrélation entre deux courses trackées successives d'un même cheval (à moins de 90 jours) : "
          "plus elle est haute, plus le chiffre mesure le cheval plutôt que le hasard de la course. "
          "Dernière colonne : % de top 3 la course suivante pour le meilleur quart des chiffres.", "",
          "| Chiffre | Paires | Corrélation | Top 3 suivant (meilleur quart) |", "|---|---:|---:|---:|"]
    for col, lab in (('pf_fig', 'brut'), ('pf_fig_adj', 'gênes rendues'), ('pf_fig_pa', 'gênes + rythme'),
                     ('pf_close', 'finish (600 derniers m)')):
        a, b = d[col][ok], nxt[col][ok]
        m = a.notna() & b.notna()
        if m.sum() < 30:
            continue
        q = a[m] >= a[m].quantile(0.75)
        t3 = (nxt.loc[m[m].index[q.to_numpy()], 'pa_num'] <= 3).mean()
        md.append(f"| {lab} | {int(m.sum())} | {np.corrcoef(a[m], b[m])[0, 1]:.3f} | {t3:.1%} |")
    return md


def _draw_style_table(d):
    """Corde x style habituel -> % top 3 et part de chevaux gênés."""
    d = d.dropna(subset=['cd_pct', 'pf_style_mean'])
    nxt_gene = d['pf_gene_sec'] > 0.1 if 'pf_gene_sec' in d else None
    cordes = (('corde (1er tiers)', d['cd_pct'] < 1 / 3), ('milieu', (d['cd_pct'] >= 1 / 3) & (d['cd_pct'] < 2 / 3)),
              ('extérieur (dernier tiers)', d['cd_pct'] >= 2 / 3))
    styles = (('meneur', d['pf_style_mean'] < 0.25), ('placé', (d['pf_style_mean'] >= 0.25) & (d['pf_style_mean'] < 0.6)),
              ('finisseur', d['pf_style_mean'] >= 0.6))
    md = ["### Corde x style habituel : % top 3 (et % gênés dans la course)", "",
          "| Corde | " + " | ".join(s for s, _ in styles) + " |", "|---|" + "---:|" * len(styles)]
    for cl, cm in cordes:
        cells = []
        for _, sm in styles:
            x = d[cm & sm]
            g = f" ({nxt_gene[cm & sm].mean():.0%} gênés)" if nxt_gene is not None and len(x) else ""
            cells.append(f"{(x['pa_num'] <= 3).mean():.1%}{g} — {len(x)}" if len(x) else "—")
        md.append(f"| {cl} | " + " | ".join(cells) + " |")
    return md


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

    md += _stability_table(fdf) + [""]
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
    md += _bucket_table(recent, 'pf_bloque_last', [-0.01, 0.5, 1.01], ['non', 'oui'],
                        "Enfermé lors de sa dernière course trackée (ralenti sans ouverture puis accélération, "
                        "ou finish rapide non converti)") + [""]
    md += _bucket_table(recent, 'pf_escape', [-np.inf, -0.05, 0.05, 0.15, np.inf],
                        ['recule encore', 'reste où il est', 'remonte un peu', 'remonte nettement'],
                        "Capacité à s'extraire : places gagnées dans la ligne droite quand il y entre "
                        "dans la seconde moitié du peloton") + [""]
    md += _bucket_table(recent, 'pf_hold_front', [-np.inf, -0.15, -0.05, np.inf],
                        ['se fait nettement passer', 'se fait un peu passer', 'tient sa place'],
                        "Quand il entre dans la ligne droite dans le premier tiers") + [""]
    md += _bucket_table(recent, 'op_ahead_elo_vs_field', [-np.inf, -25, -8, 8, 25, np.inf],
                        ['nettement plus faibles que le peloton du jour', 'un peu plus faibles', 'du même niveau',
                         'un peu plus forts', 'nettement plus forts'],
                        "Elo des chevaux qui l'ont battu (5 dernières courses), comparé au peloton du jour") + [""]
    md += _bucket_table(recent, 'op_perf_vs_exp', [-np.inf, -1.5, -0.5, 0.5, 1.5, np.inf],
                        ['nettement sous le niveau du peloton', 'un peu sous', 'au niveau', 'un peu au-dessus',
                         'nettement au-dessus'],
                        "Son chiffre comparé au niveau attendu des pelotons affrontés (5 dernières courses)") + [""]
    md += _bucket_table(recent, 'jt_escape_vs_field', [-np.inf, -0.08, -0.02, 0.02, 0.08, np.inf],
                        ['sort nettement moins bien du paquet', 'un peu moins bien', 'comme les autres',
                         'un peu mieux', 'nettement mieux'],
                        "Jockey : capacité à sortir du paquet (tracking, toutes ses montes), comparée aux "
                        "autres jockeys de la course") + [""]
    md += _bucket_table(recent, 'jt_plusvalue_vs_field', [-np.inf, -0.3, -0.1, 0.1, 0.3, np.inf],
                        ['en tire nettement moins', 'un peu moins', 'comme les autres', 'un peu plus',
                         'nettement plus'],
                        "Jockey : ce qu'il tire de ses chevaux par rapport à leur niveau habituel, comparé "
                        "aux autres jockeys de la course") + [""]
    md += _bucket_table(recent, 'pf_class_gap', [-np.inf, -0.4, -0.1, 0.1, 0.4, np.inf],
                        ['descend nettement (allocation < 2/3)', 'descend un peu', 'même niveau', 'monte un peu',
                         'monte nettement (allocation > 1,5x)'],
                        "Niveau du jour comparé à ses courses trackées (allocation)") + [""]
    lvl = recent.assign(_lvl=np.where(recent['pf_n'].eq(0), np.nan,
                                      np.where(recent['pf_n_at_level'].gt(0),
                                               recent['pf_fig_at_level'] - recent['fl_fig_mean'], -99)))
    md += _bucket_table(lvl, '_lvl', [-100, -98, -1.5, 0, 1.5, np.inf],
                        ['jamais couru à ce niveau', 'à ce niveau : chiffre nettement sous le peloton',
                         'à ce niveau : un peu sous le peloton', 'à ce niveau : un peu au-dessus',
                         'à ce niveau : nettement au-dessus'],
                        "Chiffre obtenu dans des courses au moins de ce niveau, comparé au peloton du jour") + [""]
    md += _bucket_table(recent, 'pd_vs_field', [-np.inf, -2, -0.5, 0.5, 2, np.inf],
                        ['> 2 kg de moins que la moyenne', '0,5 à 2 kg de moins', 'dans la moyenne',
                         '0,5 à 2 kg de plus', '> 2 kg de plus'], "Poids comparé au peloton") + [""]
    md += _draw_style_table(recent) + [""]

    # Élagage : les positions n'ont rien apporté au modèle (1,285 contre 1,283) ;
    # « sans Elo » mesure ce que le Profil vaut sans l'Elo de la production.
    variants = {'v9 (production)': FEATURES, 'Profil seul': PROFIL_FEATURES,
                'v9 + Profil (pour info)': FEATURES + [f for f in PROFIL_FEATURES if f not in FEATURES],
                'Profil sans jockey': [f for f in PROFIL_FEATURES if f not in JOCKEY_FEATURES],
                'Profil sans chiffres fiabilisés': [f for f in PROFIL_FEATURES if f not in NEW_FIG_FEATURES]}
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
