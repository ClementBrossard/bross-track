"""Référence : cellule 6 du notebook galoptrack_train_model_V4, copiée à
l'identique (seul l'emballage en fonction change). Sert au test de parité
entre le nouveau module galoptrack.features et le notebook."""

import bisect
import re
from collections import defaultdict
from datetime import datetime as _dt3

import numpy as np
import pandas as pd


def notebook_features(races_list, labels_array):
    # ======================================================================
    # CELLULE 6 — Feature engineering complet (identique a la cellule 6c du
    # dashboard, mais applique ici a TOUT l'historique pour l'entrainement)
    # ======================================================================
    _rows = []
    for _r in races_list:
        for _h in _r['horses']:
            _rows.append({
                'race_id': _r['id'], 'date': _r['date'], 'dist': _r['dist'], 'terrain': _r.get('terrain'),
                'piste': _r.get('piste'), 'cat': _r.get('cat'), 'alloc': _r.get('alloc'), 'hippo': _r.get('hippo'),
                'nom': _h['nom'], 'npart': _h.get('npart'), 'jk': _h.get('jk'), 'en': _h.get('en'), 'ag': _h.get('ag'), 'sx': _h.get('sx'),
                'cd': _h.get('cd'), 'pd': _h.get('pd'), 'oe': _h.get('oe'), 'nc': _h.get('nc'), 'nv': _h.get('nv'),
                'np': _h.get('np'), 'gc': _h.get('gc'), 'hv': _h.get('hv'), 'mu': _h.get('mu'),
                'pa': _h.get('pa'), 'tr': _h.get('tr'),
            })
    fdf = pd.DataFrame(_rows)
    fdf['pa_num'] = pd.to_numeric(fdf['pa'], errors='coerce')
    fdf = fdf.sort_values(['date', 'race_id']).reset_index(drop=True)

    fdf['win_rate_career']   = fdf['nv'] / fdf['nc'].replace(0, np.nan)
    fdf['place_rate_career'] = fdf['np'] / fdf['nc'].replace(0, np.nan)
    fdf['gain_moy']          = fdf['gc'] / fdf['nc'].replace(0, np.nan)

    def _parse_mu(mu):
        if not isinstance(mu, str) or not mu:
            return pd.Series({'mu_avg5': np.nan, 'mu_last1': np.nan, 'mu_wins5': np.nan})
        mu2 = re.sub(r'\([^)]*\)', '', mu)
        toks = re.findall(r'\d{1,2}', mu2)
        nums = [10 if int(t) == 0 else min(int(t), 20) for t in toks[:5]]
        if not nums:
            return pd.Series({'mu_avg5': np.nan, 'mu_last1': np.nan, 'mu_wins5': np.nan})
        return pd.Series({'mu_avg5': np.mean(nums), 'mu_last1': nums[0], 'mu_wins5': sum(1 for n in nums if n == 1)})
    fdf = pd.concat([fdf, fdf['mu'].apply(_parse_mu)], axis=1)

    fdf['win']   = (fdf['pa_num'] == 1).astype(int)
    fdf['place'] = (fdf['pa_num'] <= 3).astype(int)
    for _col, _pre in [('jk', 'jk'), ('en', 'en')]:
        _g = fdf.groupby(_col)
        _cum_n = _g.cumcount()
        _cum_w = _g['win'].cumsum() - fdf['win']
        _cum_p = _g['place'].cumsum() - fdf['place']
        fdf[f'{_pre}_prior_n']         = _cum_n
        fdf[f'{_pre}_prior_winrate']   = np.where(_cum_n > 0, _cum_w / _cum_n, np.nan)
        fdf[f'{_pre}_prior_placerate'] = np.where(_cum_n > 0, _cum_p / _cum_n, np.nan)

    _idx600 = labels_array.index('600m-400m')
    _idx400 = labels_array.index('400m-200m')
    _idx200 = labels_array.index('200m-ARR')
    _FINAL  = {_idx600, _idx400, _idx200}

    def _tr_metrics(tr):
        _nan3 = pd.Series({'accel': np.nan, 'gain': np.nan, 'finish_kick': np.nan})
        if not tr or len(tr) < 5: return _nan3
        if any(s[0] is None for s in tr): return _nan3
        if any(s[2] is None or s[2] < 20 or s[2] > 80 for s in tr): return _nan3
        times = [s[1] for s in tr]
        if times != sorted(times): return _nan3
        seg = {s[5]: s for s in tr}
        if not all(l in seg for l in (_idx600, _idx400, _idx200)): return _nan3
        f3 = [seg[_idx600], seg[_idx400], seg[_idx200]]
        fspeed = sum(s[2] for s in f3) / 3
        cruise = [s for s in tr[1:] if s[5] not in _FINAL]
        if not cruise: return _nan3
        cspeed = sum(s[2] for s in cruise) / len(cruise)
        return pd.Series({'accel': fspeed - cspeed, 'gain': seg[_idx600][3] - seg[_idx200][3], 'finish_kick': seg[_idx200][2]})

    fdf = pd.concat([fdf, fdf['tr'].apply(_tr_metrics)], axis=1)
    fdf = fdf.sort_values(['nom', 'date']).reset_index(drop=True)

    # Classification de surface : le comportement d'un cheval en tronçons varie
    # fortement entre un terrain souple/collant, un Bon, et le PSF. On segmente
    # donc la moyenne glissante des tronçons PAR TYPE DE SURFACE plutot que de
    # tout melanger.
    def _classify_surface(terrain, piste):
        t = str(terrain).strip() if pd.notna(terrain) else ''
        if t.upper().startswith('PSF') or piste == 'PSF':
            return 'PSF'
        if t in ('Bon', 'Bon léger', 'Bon souple'):
            return 'HERBE_BON'
        if t in ('Souple', 'Très souple', 'Collant', 'Lourd', 'Très lourd'):
            return 'HERBE_SOUPLE_COLLANT'
        return 'AUTRE'

    fdf['surface_grp'] = [_classify_surface(t, p) for t, p in zip(fdf['terrain'], fdf['piste'])]

    # Moyenne glissante des 5 dernieres courses TRACKEES DE LA MEME SURFACE
    # uniquement. Une course non trackee, ou trackee mais sur une autre surface,
    # ne fait ni remettre a zero ni perimer la derniere valeur connue pour cette
    # surface : elle reste "en attente" de la prochaine course trackee sur cette
    # meme surface (voir notebook dashboard, cellule 6c, pour le detail).
    _tracked_sub = fdf[fdf['accel'].notna()][['nom', 'surface_grp', 'date', 'accel', 'gain', 'finish_kick']].copy()
    _tracked_sub = _tracked_sub.sort_values(['nom', 'surface_grp', 'date']).reset_index(drop=True)
    for _col in ['accel', 'gain', 'finish_kick']:
        _tracked_sub[f'{_col}_roll5'] = (
            _tracked_sub.groupby(['nom', 'surface_grp'])[_col]
                        .transform(lambda s: s.rolling(5, min_periods=1).mean())
        )

    fdf = fdf.sort_values(['date', 'race_id']).reset_index(drop=True)
    _tracked_sub = _tracked_sub.sort_values(['date']).reset_index(drop=True)
    fdf = pd.merge_asof(
        fdf, _tracked_sub[['nom', 'surface_grp', 'date', 'accel_roll5', 'gain_roll5', 'finish_kick_roll5']],
        on='date', by=['nom', 'surface_grp'], direction='backward', allow_exact_matches=False
    )

    race_field, race_date = defaultdict(list), {}
    for _row in fdf.itertuples():
        race_field[_row.race_id].append(_row.nom)
        race_date[_row.race_id] = _row.date
    race_valid = defaultdict(list)
    for _row in fdf.dropna(subset=['pa_num']).itertuples():
        race_valid[_row.race_id].append((_row.nom, _row.pa_num))

    pair_wins = defaultdict(list)
    for _rid, _entries in race_valid.items():
        _date = race_date[_rid]; _n = len(_entries)
        for _i in range(_n):
            for _j in range(_n):
                if _i == _j: continue
                if _entries[_i][1] < _entries[_j][1]:
                    pair_wins[(_entries[_i][0], _entries[_j][0])].append(_date)
    for _k in pair_wins: pair_wins[_k].sort()

    _h2h_w, _h2h_l, _h2h_n, _h2h_o = {}, {}, {}, {}
    for _rid, _names in race_field.items():
        _date = race_date[_rid]
        for _nom in _names:
            _w = _l = _o = 0
            for _opp in _names:
                if _opp == _nom: continue
                _dw = pair_wins.get((_nom, _opp)); _dl = pair_wins.get((_opp, _nom))
                _nw = bisect.bisect_left(_dw, _date) if _dw else 0
                _nl = bisect.bisect_left(_dl, _date) if _dl else 0
                _w += _nw; _l += _nl
                if _nw > 0 or _nl > 0: _o += 1
            _h2h_w[(_rid, _nom)] = _w; _h2h_l[(_rid, _nom)] = _l
            _h2h_n[(_rid, _nom)] = _w - _l; _h2h_o[(_rid, _nom)] = _o

    fdf['h2h_wins_field']   = [_h2h_w[(r, n)] for r, n in zip(fdf['race_id'], fdf['nom'])]
    fdf['h2h_losses_field'] = [_h2h_l[(r, n)] for r, n in zip(fdf['race_id'], fdf['nom'])]
    fdf['h2h_net_field']    = [_h2h_n[(r, n)] for r, n in zip(fdf['race_id'], fdf['nom'])]
    fdf['h2h_opp_met']      = [_h2h_o[(r, n)] for r, n in zip(fdf['race_id'], fdf['nom'])]

    elo = defaultdict(lambda: 1500.0)
    elo_before = {}
    _K = 24
    _race_order = fdf[fdf['pa_num'].notna()][['race_id', 'date']].drop_duplicates().sort_values(['date', 'race_id'])
    for _rid in _race_order['race_id']:
        _entries = race_valid[_rid]; _n = len(_entries)
        if _n < 2: continue
        _pre = {nom: elo[nom] for nom, _ in _entries}
        for nom, _ in _entries: elo_before[(_rid, nom)] = _pre[nom]
        _delta = defaultdict(float)
        for _i in range(_n):
            _nomi, _pai = _entries[_i]
            for _j in range(_n):
                if _i == _j: continue
                _nomj, _paj = _entries[_j]
                _Ri, _Rj = _pre[_nomi], _pre[_nomj]
                _E = 1 / (1 + 10 ** ((_Rj - _Ri) / 400))
                _S = 1.0 if _pai < _paj else (0.0 if _pai > _paj else 0.5)
                _delta[_nomi] += (_S - _E)
        for nom, _ in _entries: elo[nom] += _K * _delta[nom] / (_n - 1)
    fdf['elo'] = [elo_before.get((r, n), elo.get(n, 1500.0)) for r, n in zip(fdf['race_id'], fdf['nom'])]

    # Qualite des adversaires lors de SA DERNIERE course -- ceux qui l'ont
    # DEVANCE (pertinent s'il n'a pas gagne) ET ceux qu'il a BATTUS (pertinent
    # surtout s'il a gagne : battre un champ plein de "cadors" est plus fort que
    # battre un champ faible, cette info ne doit pas etre perdue juste parce
    # qu'il a gagne). Calcule avec l'Elo des adversaires AU MOMENT de cette
    # course (pas de fuite), puis decale d'une course par cheval.
    _ahead_avg, _ahead_max = {}, {}
    _beaten_avg, _beaten_max = {}, {}
    for _rid, _entries in race_valid.items():
        for _nom, _pa in _entries:
            _ahead = [elo_before.get((_rid, _o)) for _o, _po in _entries if _po < _pa and _o != _nom]
            _ahead = [e for e in _ahead if e is not None]
            if _ahead:
                _ahead_avg[(_rid, _nom)] = float(np.mean(_ahead))
                _ahead_max[(_rid, _nom)] = float(np.max(_ahead))
            _beaten = [elo_before.get((_rid, _o)) for _o, _po in _entries if _po > _pa and _o != _nom]
            _beaten = [e for e in _beaten if e is not None]
            if _beaten:
                _beaten_avg[(_rid, _nom)] = float(np.mean(_beaten))
                _beaten_max[(_rid, _nom)] = float(np.max(_beaten))

    fdf['_ahead_avg']  = [_ahead_avg.get((r, n), np.nan) for r, n in zip(fdf['race_id'], fdf['nom'])]
    fdf['_ahead_max']  = [_ahead_max.get((r, n), np.nan) for r, n in zip(fdf['race_id'], fdf['nom'])]
    fdf['_beaten_avg'] = [_beaten_avg.get((r, n), np.nan) for r, n in zip(fdf['race_id'], fdf['nom'])]
    fdf['_beaten_max'] = [_beaten_max.get((r, n), np.nan) for r, n in zip(fdf['race_id'], fdf['nom'])]
    fdf = fdf.sort_values(['nom', 'date']).reset_index(drop=True)
    fdf['last_race_opp_elo_ahead_avg']  = fdf.groupby('nom')['_ahead_avg'].shift(1)
    fdf['last_race_opp_elo_ahead_max']  = fdf.groupby('nom')['_ahead_max'].shift(1)
    fdf['last_race_opp_elo_beaten_avg'] = fdf.groupby('nom')['_beaten_avg'].shift(1)
    fdf['last_race_opp_elo_beaten_max'] = fdf.groupby('nom')['_beaten_max'].shift(1)
    fdf = fdf.drop(columns=['_ahead_avg', '_ahead_max', '_beaten_avg', '_beaten_max'])
    fdf = fdf.sort_values(['date', 'race_id']).reset_index(drop=True)

    fdf = fdf.sort_values(['nom', 'date']).reset_index(drop=True)
    fdf['date_dt'] = fdf['date'].apply(lambda x: _dt3(int(x)//10000, (int(x)//100) % 100, int(x) % 100))
    fdf['prev_date'] = fdf.groupby('nom')['date_dt'].shift(1)
    fdf['days_since_last'] = (fdf['date_dt'] - fdf['prev_date']).dt.days
    fdf['alloc_avg_prev3'] = fdf.groupby('nom')['alloc'].apply(lambda s: s.shift(1).rolling(3, min_periods=1).mean()).reset_index(level=0, drop=True)
    fdf['alloc_delta'] = fdf['alloc'] - fdf['alloc_avg_prev3']
    fdf['alloc_ratio'] = fdf['alloc'] / fdf['alloc_avg_prev3']
    fdf = fdf.sort_values(['date', 'race_id']).reset_index(drop=True)


    return fdf
