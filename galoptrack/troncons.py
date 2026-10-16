"""Tronçons (étape 3 du modèle Compétition) : chiffres de vitesse comparés au
reste de la course (ce qui neutralise terrain et piste) et style de course.

Pour chaque course trackée passée, et chaque cheval :
  fin600   rang du temps des 600 derniers mètres dans la course, en part du
           peloton (0 = le plus rapide, 1 = le plus lent)
  fin200   idem pour les 200 derniers mètres
  vmax     rang de la vitesse maximale (0 = la plus élevée)
  style    position au premier tronçon, en part du peloton (0 = en tête)
Pour le cheval du jour, sur ses `n_last` dernières courses trackées des
`window_days` jours précédents (strictement antérieures) :
  tk_fin600_mean / tk_fin600_best / tk_fin200_mean / tk_vmax_mean / tk_style / tk_n
Scénario de la course du jour (rivaux) :
  tk_field_leaders     nombre de rivaux qui partent habituellement en tête (style < 0,25)
  tk_style_vs_field    son style - style moyen des rivaux (< 0 : il part plus vite qu'eux)
  tk_fin600_vs_field   sa vitesse de finish - celle des rivaux (< 0 : il finit plus vite)
"""

from collections import deque

import numpy as np
import pandas as pd

TRACK_FEATURES = ['tk_fin600_mean', 'tk_fin600_best', 'tk_fin200_mean', 'tk_vmax_mean', 'tk_style', 'tk_n',
                  'tk_field_leaders', 'tk_style_vs_field', 'tk_fin600_vs_field']


def _rank_pct(v, higher_better=False):
    out = np.full(len(v), np.nan)
    ok = ~np.isnan(v)
    k = ok.sum()
    if k < 2:
        return out
    vals = -v[ok] if higher_better else v[ok]
    r = vals.argsort(kind='stable').argsort(kind='stable')
    out[ok] = r / (k - 1)
    return out


def _first_pos(tr):
    if isinstance(tr, (list, tuple)) and tr and isinstance(tr[0], (list, tuple)) and len(tr[0]) > 3:
        p = tr[0][3]
        return float(p) if p is not None else np.nan
    return np.nan


def tracking_features(fdf, n_last=3, window_days=365):
    fdf = fdf.copy()
    n = len(fdf)
    num = lambda c: pd.to_numeric(fdf[c], errors='coerce').to_numpy(dtype=float) if c in fdf else np.full(n, np.nan)
    t6, t2f, vm = num('t6'), num('t2f'), num('vm')
    early = np.array([_first_pos(t) for t in fdf['tr']], dtype=float) if 'tr' in fdf else np.full(n, np.nan)
    races = list(fdf.groupby('race_id').indices.values())

    fin600, fin200, vmax_r, style = (np.full(n, np.nan) for _ in range(4))
    for idx in races:
        fin600[idx] = _rank_pct(t6[idx])
        fin200[idx] = _rank_pct(t2f[idx])
        vmax_r[idx] = _rank_pct(vm[idx], higher_better=True)
        e = early[idx]
        k = (~np.isnan(e)).sum()
        if k >= 2:
            style[idx] = (e - 1) / (len(idx) - 1)

    has = ~(np.isnan(fin600) & np.isnan(fin200) & np.isnan(style))
    out = {c: np.full(n, np.nan) for c in ('tk_fin600_mean', 'tk_fin600_best', 'tk_fin200_mean',
                                            'tk_vmax_mean', 'tk_style')}
    out['tk_n'] = np.zeros(n)
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
            if past:
                out['tk_n'][i] = len(past)
                for col, arr, f in (('tk_fin600_mean', fin600, np.nanmean), ('tk_fin600_best', fin600, np.nanmin),
                                    ('tk_fin200_mean', fin200, np.nanmean), ('tk_vmax_mean', vmax_r, np.nanmean),
                                    ('tk_style', style, np.nanmean)):
                    a = arr[past]
                    if not np.isnan(a).all():
                        out[col][i] = f(a)
            if has[i]:
                win.append(i)
        start = end
    for c, v in out.items():
        fdf[c] = v

    leaders, sty_vs, fin_vs = np.full(n, np.nan), np.full(n, np.nan), np.full(n, np.nan)
    st, f6 = out['tk_style'], out['tk_fin600_mean']
    for idx in races:
        if len(idx) < 2:
            continue
        for k, i in enumerate(idx):
            o = np.delete(idx, k)
            so = st[o]
            if not np.isnan(so).all():
                leaders[i] = float((so < 0.25).sum())
                if not np.isnan(st[i]):
                    sty_vs[i] = st[i] - np.nanmean(so)
            fo = f6[o]
            if not np.isnan(f6[i]) and not np.isnan(fo).all():
                fin_vs[i] = f6[i] - np.nanmean(fo)
    fdf['tk_field_leaders'], fdf['tk_style_vs_field'], fdf['tk_fin600_vs_field'] = leaders, sty_vs, fin_vs
    return fdf
