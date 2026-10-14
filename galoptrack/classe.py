"""Note de classe « turfiste » (étape 1 du modèle Compétition).

Valeur d'une performance = allocation totale de la course x part de la place
(barème du partage des allocations en plat : 1er 100 %, 2e 40 %, 3e 30 %,
4e 20 %, 5e 10 %, au-delà 0).

Note de classe d'un cheval à une date = moyenne de ses `top` meilleures
performances sur les `window_days` jours précédents (courses strictement
antérieures), « tirée vers la moyenne » quand il a peu couru : on ajoute
`prior_weight` performances fictives égales à la performance moyenne de tous
les partants jusque-là. Exprimée en log10 (1 point = un facteur 10 d'argent).
"""

from collections import deque

import numpy as np
import pandas as pd

PLACE_SHARE = {1: 1.0, 2: 0.4, 3: 0.3, 4: 0.2, 5: 0.1}


def performance_value(alloc, pa):
    share = pd.Series(pa).map(lambda p: PLACE_SHARE.get(int(p), 0.0) if pd.notna(p) else np.nan)
    return pd.to_numeric(pd.Series(alloc), errors='coerce').to_numpy() * share.to_numpy()


def class_features(fdf, window_days=365, top=3, prior_weight=1.0, suffix=''):
    """Ajoute à fdf (trié chronologiquement, sortie de build_features) :
      classe{s}            note de classe (log10 €)
      classe_n12m{s}       nombre de courses connues sur la fenêtre
      classe_vs_field{s}   note - moyenne des notes des rivaux du jour
      classe_vs_best{s}    note - meilleure note parmi les rivaux
      classe_rank{s}       rang de la note dans la course (1 = meilleure)
      classe_vs_race{s}    note - log10(allocation du jour) (> 0 : course facile pour lui)"""
    fdf = fdf.copy()
    s = suffix
    n = len(fdf)
    perf = performance_value(fdf['alloc'].to_numpy(), fdf['pa_num'].to_numpy())
    dates = pd.to_datetime(fdf['date'].astype(int).astype(str), format='%Y%m%d').to_numpy()

    # Performance moyenne de tous les partants strictement avant chaque date
    ok = ~np.isnan(perf)
    by_day = pd.DataFrame({'d': dates[ok], 'v': perf[ok]}).groupby('d')['v'].agg(['sum', 'count'])
    cs, cc = by_day['sum'].cumsum().shift(1).fillna(0), by_day['count'].cumsum().shift(1).fillna(0)
    prior_day = (cs / cc.replace(0, np.nan)).fillna(by_day['sum'].sum() / max(by_day['count'].sum(), 1))
    days_known = by_day.index.to_numpy()
    pos = np.searchsorted(days_known, dates, side='left')
    prior = np.where(pos < len(days_known),
                     prior_day.to_numpy()[np.minimum(pos, len(days_known) - 1)],
                     by_day['sum'].sum() / max(by_day['count'].sum(), 1))

    note_eur = np.full(n, np.nan)
    n12 = np.zeros(n)
    order = np.lexsort((fdf['is_today'].to_numpy(), dates, fdf['nom'].to_numpy()))
    noms = fdf['nom'].to_numpy()
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
            past = [perf[j] for j in win if dates[j] < dates[i] and not np.isnan(perf[j])]
            best = sorted(past, reverse=True)[:top]
            n12[i] = len(past)
            note_eur[i] = (sum(best) + prior_weight * prior[i]) / (len(best) + prior_weight) \
                if (best or prior_weight) else np.nan
            win.append(i)
        start = end

    note = np.log10(1 + note_eur)
    fdf[f'classe{s}'] = note
    fdf[f'classe_n12m{s}'] = n12
    vs_field, vs_best, rank = np.full(n, np.nan), np.full(n, np.nan), np.full(n, np.nan)
    for idx in fdf.groupby('race_id').indices.values():
        v = note[idx]
        if len(idx) < 2:
            continue
        order_desc = (-v).argsort(kind='stable')
        ranks = np.empty(len(idx))
        ranks[order_desc] = np.arange(1, len(idx) + 1)
        tot = np.nansum(v)
        for k, i in enumerate(idx):
            others = np.delete(v, k)
            vs_field[i] = v[k] - (tot - v[k]) / (len(idx) - 1)
            vs_best[i] = v[k] - np.nanmax(others)
            rank[i] = ranks[k]
    fdf[f'classe_vs_field{s}'] = vs_field
    fdf[f'classe_vs_best{s}'] = vs_best
    fdf[f'classe_rank{s}'] = rank
    fdf[f'classe_vs_race{s}'] = note - np.log10(1 + pd.to_numeric(fdf['alloc'], errors='coerce').to_numpy())
    return fdf
