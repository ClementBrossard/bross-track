"""Blocs d'information du modèle challenger (ce que regardent les parieurs
pros), à comparer au marché seul. Toutes les variables sont calculées
strictement avant chaque course (aucune fuite du résultat).

Intention   : changement de jockey (et de son niveau), œillères mises ou
              changées, changement de distance / catégorie / valeur handicap,
              nombre de courses sur 90 jours.
Entourage   : forme de l'entraîneur et du jockey sur les 30 derniers jours,
              réussite du duo jockey-entraîneur.
Conditions  : biais de corde (hippodrome x distance x corde), préférences du
              cheval pour la surface, la distance et l'hippodrome.

Les « réussites » sont mesurées en écart au hasard : dans les 3 premiers
(1/0) moins 3 / nombre de partants, ce qui neutralise la taille du champ."""

import numpy as np
import pandas as pd

from .features import SORT_CHRONO

INTENT_FEATURES = ['it_jk_change', 'it_jk_wr_delta', 'it_oe_on', 'it_oe_first', 'it_oe_change',
                   'it_dist_delta', 'it_cat_change', 'it_hv_delta', 'it_runs_90']
ENTOURAGE_FEATURES = ['en_30_n', 'en_30_winrate', 'en_30_resid', 'en_form_vs_career',
                      'jk_30_n', 'jk_30_winrate', 'jk_30_resid',
                      'duo_n', 'duo_resid']
CONDITION_FEATURES = ['cd_rel', 'cb_n', 'cb_resid', 'hz_surf_n', 'hz_surf_resid',
                      'hz_dist_n', 'hz_dist_resid', 'hz_hippo_n', 'hz_hippo_resid', 'hz_all_resid']


def _with_results(fdf):
    """Colonnes communes : partants, résultat connu, écart au hasard."""
    fdf = fdf.sort_values(SORT_CHRONO).reset_index(drop=True)
    fdf['_nrun'] = fdf.groupby('race_id')['nom'].transform('size')
    known = fdf['pa_num'].notna()
    fdf['_resid'] = np.where(known, (fdf['pa_num'] <= 3).astype(float) - 3 / fdf['_nrun'], np.nan)
    fdf['_win'] = np.where(known, (fdf['pa_num'] == 1).astype(float), np.nan)
    fdf['_ord'] = pd.to_datetime(fdf['date'].astype(int).astype(str), format='%Y%m%d').map(pd.Timestamp.toordinal)
    return fdf


def _prior(fdf, keys, col):
    """Nombre et moyenne de `col` sur les lignes précédentes du même groupe
    (ordre chronologique, ligne courante exclue, lignes sans résultat ignorées)."""
    v = fdf[col]
    s, c = v.fillna(0.0), v.notna().astype(float)
    g = pd.DataFrame({'s': s, 'c': c, **{f'k{i}': fdf[k].astype(str) for i, k in enumerate(keys)}})
    gk = [f'k{i}' for i in range(len(keys))]
    cs = g.groupby(gk)['s'].cumsum() - s
    cc = g.groupby(gk)['c'].cumsum() - c
    return cc, (cs / cc).where(cc > 0)


def _window(fdf, key, days, cols):
    """Pour chaque ligne : somme de `cols` et nombre de courses du même `key`
    dans les `days` jours précédents (jour même exclu)."""
    d = fdf[[key, '_ord'] + cols].copy()
    d = d[d[cols[0]].notna() & d[key].notna()]
    daily = d.groupby([key, '_ord'])[cols].agg(['sum', 'count'])
    daily.columns = [f'{a}_{b}' for a, b in daily.columns]
    out = {f'{c}_{s}': np.full(len(fdf), np.nan) for c in cols for s in ('sum', 'count')}
    rows = fdf.groupby(fdf[key].astype(str)).indices
    for k, sub in daily.groupby(level=0):
        idx = rows.get(str(k))
        if idx is None:
            continue
        ords = sub.index.get_level_values(1).to_numpy()
        q = fdf['_ord'].to_numpy()[idx]
        hi = np.searchsorted(ords, q, 'left')
        lo = np.searchsorted(ords, q - days, 'left')
        for name in out:
            cum = np.concatenate([[0.0], np.cumsum(sub[name].to_numpy(dtype=float))])
            out[name][idx] = cum[hi] - cum[lo]
    return out


def intention_features(fdf):
    fdf = _with_results(fdf)
    d = fdf.sort_values(['nom', 'date'])
    g = d.groupby('nom')
    prev = lambda c: g[c].shift(1)
    jk, pjk = d['jk'].astype(object), prev('jk').astype(object)
    d['it_jk_change'] = np.where(pjk.isna(), np.nan, (jk != pjk).astype(float))
    d['it_jk_wr_delta'] = d['jk_prior_winrate'] - prev('jk_prior_winrate')
    oe = d['oe'].astype(object).fillna('').astype(str).str.upper()
    on = (oe.str.contains('OEIL') & ~oe.str.startswith('SANS')).astype(float)
    pon = on.groupby(d['nom']).shift(1)
    d['it_oe_on'] = on
    d['it_oe_first'] = np.where(pon.isna(), np.nan, ((on == 1) & (pon == 0)).astype(float))
    poe = oe.groupby(d['nom']).shift(1)
    d['it_oe_change'] = np.where(poe.isna(), np.nan, (oe != poe).astype(float))
    d['it_dist_delta'] = pd.to_numeric(d['dist'], errors='coerce') - pd.to_numeric(prev('dist'), errors='coerce')
    cat, pcat = d['cat'].astype(object), prev('cat').astype(object)
    d['it_cat_change'] = np.where(pcat.isna(), np.nan, (cat != pcat).astype(float))
    d['it_hv_delta'] = pd.to_numeric(d['hv'], errors='coerce') - pd.to_numeric(prev('hv'), errors='coerce')
    runs = np.full(len(d), np.nan)
    for _, idx in d.groupby('nom').indices.items():
        o = d['_ord'].to_numpy()[idx]
        runs[idx] = np.arange(len(idx)) - np.searchsorted(o, o - 90, 'left')
    d['it_runs_90'] = runs
    return d.sort_values(SORT_CHRONO).reset_index(drop=True)


def entourage_features(fdf):
    fdf = _with_results(fdf)
    for key, pre in (('en', 'en'), ('jk', 'jk')):
        w = _window(fdf, key, 30, ['_win', '_resid'])
        n = w['_win_count']
        fdf[f'{pre}_30_n'] = n
        fdf[f'{pre}_30_winrate'] = np.where(n > 0, w['_win_sum'] / np.where(n > 0, n, 1), np.nan)
        fdf[f'{pre}_30_resid'] = np.where(n > 0, w['_resid_sum'] / np.where(n > 0, n, 1), np.nan)
    fdf['en_form_vs_career'] = fdf['en_30_winrate'] - fdf['en_prior_winrate']
    fdf['duo_n'], fdf['duo_resid'] = _prior(fdf, ['jk', 'en'], '_resid')
    return fdf


def conditions_features(fdf):
    from .features import _classify_surface
    fdf = _with_results(fdf)
    cd = pd.to_numeric(fdf['cd'], errors='coerce')
    fdf['cd_rel'] = cd / fdf['_nrun']
    fdf['_dbin'] = (pd.to_numeric(fdf['dist'], errors='coerce') / 200).round()
    fdf['_cd'] = cd
    fdf['cb_n'], fdf['cb_resid'] = _prior(fdf, ['hippo', '_dbin', '_cd'], '_resid')
    fdf['cb_resid'] = fdf['cb_resid'].where(cd.notna())
    if 'surface_grp' not in fdf:
        fdf['surface_grp'] = [_classify_surface(t, p) for t, p in zip(fdf['terrain'], fdf['piste'])]
    fdf['hz_surf_n'], fdf['hz_surf_resid'] = _prior(fdf, ['nom', 'surface_grp'], '_resid')
    fdf['hz_dist_n'], fdf['hz_dist_resid'] = _prior(fdf, ['nom', '_dbin'], '_resid')
    fdf['hz_hippo_n'], fdf['hz_hippo_resid'] = _prior(fdf, ['nom', 'hippo'], '_resid')
    _, fdf['hz_all_resid'] = _prior(fdf, ['nom'], '_resid')
    return fdf.drop(columns=['_dbin', '_cd'])


def all_blocks(fdf):
    out = conditions_features(entourage_features(intention_features(fdf)))
    return out.drop(columns=['_nrun', '_resid', '_win', '_ord'])
