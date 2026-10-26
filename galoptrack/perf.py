"""Performances démontrées « à la handicapeur » : valeur handicap des chevaux
battus (ou qui l'ont battu), corrigée du poids porté par chacun ce jour-là.
En France la valeur handicap est en kilos : battre un cheval de valeur V en lui
rendant k kg vaut une performance d'au moins V + k.

Pour chaque course passée d'un cheval A (arrivée connue) :
  perf     max sur les chevaux B battus par A de  hv(B) + poids(A) - poids(B)
  plafond  min sur les chevaux C qui ont battu A de hv(C) + poids(A) - poids(C)
Pour la course du jour, sur ses `n_last` dernières courses des `window_days`
jours précédents (strictement antérieures) :
  pr_best, pr_mean, pr_ceiling, pr_n
  pr_vs_hv           pr_best - sa valeur handicap du jour (> 0 : mieux que sa valeur)
  pr_vs_field_best   pr_best - meilleur pr_best des rivaux du jour
  pr_vs_field_mean   pr_best - moyenne des pr_best des rivaux
"""

from collections import deque

import numpy as np
import pandas as pd

PERF_FEATURES = ['pr_best', 'pr_mean', 'pr_ceiling', 'pr_n', 'pr_vs_hv', 'pr_vs_field_best', 'pr_vs_field_mean']  # = features.PERF_FEATURES


def performance_ratings(fdf, n_last=5, window_days=365):
    fdf = fdf.copy()
    n = len(fdf)
    hv = pd.to_numeric(fdf['hv'], errors='coerce').to_numpy(dtype=float)
    w = pd.to_numeric(fdf['pd'], errors='coerce').to_numpy(dtype=float)
    pa = fdf['pa_num'].to_numpy(dtype=float)
    races = list(fdf.groupby('race_id').indices.values())

    perf, ceil = np.full(n, np.nan), np.full(n, np.nan)
    for idx in races:
        p, h, ww = pa[idx], hv[idx], w[idx]
        if np.isnan(p).all():
            continue
        ok = ~np.isnan(h) & ~np.isnan(ww)
        for k, i in enumerate(idx):
            if np.isnan(p[k]) or np.isnan(ww[k]):
                continue
            beaten = (p > p[k]) & ok
            if beaten.any():
                perf[i] = (h[beaten] + ww[k] - ww[beaten]).max()
            ahead = (p < p[k]) & ok
            if ahead.any():
                ceil[i] = (h[ahead] + ww[k] - ww[ahead]).min()

    best, mean, cmin, cnt = (np.full(n, np.nan) for _ in range(4))
    dates = pd.to_datetime(fdf['date'].astype(int).astype(str), format='%Y%m%d').to_numpy()
    noms = fdf['nom'].to_numpy()
    order = np.lexsort((fdf['is_today'].to_numpy(), dates, noms))
    lim = np.timedelta64(window_days, 'D')
    start = 0
    while start < n:
        end = start
        while end < n and noms[order[end]] == noms[order[start]]:
            end += 1
        win = deque()
        for i in order[start:end]:
            while win and dates[win[0]] < dates[i] - lim:
                win.popleft()
            past = [j for j in win if dates[j] < dates[i]][-n_last:]
            cnt[i] = len(past)
            if past:
                a, c = perf[past], ceil[past]
                if not np.isnan(a).all():
                    best[i], mean[i] = np.nanmax(a), np.nanmean(a)
                if not np.isnan(c).all():
                    cmin[i] = np.nanmin(c)
            if not np.isnan(pa[i]):
                win.append(i)
        start = end

    fdf['pr_best'], fdf['pr_mean'], fdf['pr_ceiling'], fdf['pr_n'] = best, mean, cmin, cnt
    fdf['pr_vs_hv'] = best - hv
    vs_best, vs_mean = np.full(n, np.nan), np.full(n, np.nan)
    for idx in races:
        if len(idx) < 2:
            continue
        b = best[idx]
        for k, i in enumerate(idx):
            o = np.delete(b, k)
            if np.isnan(b[k]) or np.isnan(o).all():
                continue
            vs_best[i] = b[k] - np.nanmax(o)
            vs_mean[i] = b[k] - np.nanmean(o)
    fdf['pr_vs_field_best'], fdf['pr_vs_field_mean'] = vs_best, vs_mean
    return fdf
