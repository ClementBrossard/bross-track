"""Feature engineering du modèle — UNE seule implémentation, partagée par
l'entraînement et le scoring des courses du jour.

Dans les notebooks, ce code existait en deux copies (cellule 6 de
galoptrack_train_model, cellule 6c du générateur de dashboard) ; la moindre
divergence entre les deux aurait faussé les prédictions sans alerte. Le code
ci-dessous est la version du dashboard (qui gère les courses du jour via
`is_today`) ; sur l'historique seul elle donne exactement les mêmes valeurs
que la version d'entraînement."""

import bisect
import re
from collections import defaultdict, deque
from datetime import datetime as _dt3

import numpy as np
import pandas as pd

FEATURES = ['hv', 'ag', 'sx', 'oe', 'dist', 'alloc', 'cat', 'piste', 'terrain', 'hippo',
            'win_rate_career', 'place_rate_career', 'gain_moy', 'mu_avg5', 'mu_last1', 'mu_wins5',
            'jk_prior_n', 'jk_prior_winrate', 'jk_prior_placerate',
            'en_prior_n', 'en_prior_winrate', 'en_prior_placerate',
            'accel_roll5', 'gain_roll5', 'finish_kick_roll5',
            'h2h_wins_field', 'h2h_losses_field', 'h2h_net_field', 'h2h_opp_met',
            'elo', 'days_since_last', 'alloc_delta', 'alloc_ratio',
            'last_race_opp_elo_ahead_avg', 'last_race_opp_elo_ahead_max',
            'last_race_opp_elo_beaten_avg', 'last_race_opp_elo_beaten_max']
BASE_FEATURES = list(FEATURES)
# Niveau des chevaux battus sur 3 mois comparé au peloton du jour : calculées
# (affichées dans l'onglet Modèle) mais PAS utilisées par le modèle. Testées
# sur 4 périodes (expérience « adversaires ») : signal brut net (14,6 % de
# victoires contre 6,2 %), mais aucun gain du modèle au-delà du bruit (24,8 %
# contre 24,7 %), l'Elo et les face-à-face portant déjà l'information. Pour
# les ajouter un jour : FEATURES = BASE_FEATURES + FIELD_FEATURES (l'entraînement
# compare alors automatiquement au témoin à 37 variables).
FIELD_FEATURES = ['beaten_vs_field_90', 'beaten_vs_top3_90', 'field_below_bmax_90', 'field_below_bavg_90']
# v10 : + chevaux battus (et qui l'ont battu) mesurés par leur valeur handicap,
# corrigée du poids porté par chacun (perf.py). Testé sur 4 périodes : 4/4
# meilleures que la recette v8 en « top 3 » (1,433 contre 1,413 sur 3).
PERF_FEATURES = ['pr_best', 'pr_mean', 'pr_ceiling', 'pr_n', 'pr_vs_hv', 'pr_vs_field_best', 'pr_vs_field_mean']
FEATURES = BASE_FEATURES + PERF_FEATURES
CAT_COLS = ['sx', 'oe', 'cat', 'piste', 'terrain', 'hippo']

SORT_CHRONO = ['date', 'is_today', 'race_id']


def _parse_mu(mu):
    if not isinstance(mu, str) or not mu:
        return pd.Series({'mu_avg5': np.nan, 'mu_last1': np.nan, 'mu_wins5': np.nan})
    mu2 = re.sub(r'\([^)]*\)', '', mu)
    toks = re.findall(r'\d{1,2}', mu2)
    nums = [10 if int(t) == 0 else min(int(t), 20) for t in toks[:5]]
    if not nums:
        return pd.Series({'mu_avg5': np.nan, 'mu_last1': np.nan, 'mu_wins5': np.nan})
    return pd.Series({'mu_avg5': np.mean(nums), 'mu_last1': nums[0], 'mu_wins5': sum(1 for n in nums if n == 1)})


def _classify_surface(terrain, piste):
    t = str(terrain).strip() if pd.notna(terrain) else ''
    if t.upper().startswith('PSF') or piste == 'PSF':
        return 'PSF'
    if t in ('Bon', 'Bon léger', 'Bon souple'):
        return 'HERBE_BON'
    if t in ('Souple', 'Très souple', 'Collant', 'Lourd', 'Très lourd'):
        return 'HERBE_SOUPLE_COLLANT'
    return 'AUTRE'


def flatten(races_list):
    rows = []
    for r in races_list:
        for h in r['horses']:
            rows.append({
                'race_id': r['id'], 'date': r['date'], 'dist': r['dist'], 'terrain': r.get('terrain'),
                'piste': r.get('piste'), 'cat': r.get('cat'), 'alloc': r.get('alloc'), 'hippo': r.get('hippo'),
                'is_today': bool(r.get('today')),
                'nom': h['nom'], 'npart': h.get('npart'), 'jk': h.get('jk'), 'en': h.get('en'),
                'ag': h.get('ag'), 'sx': h.get('sx'),
                'cd': h.get('cd'), 'pd': h.get('pd'), 'oe': h.get('oe'), 'nc': h.get('nc'), 'nv': h.get('nv'),
                'np': h.get('np'), 'gc': h.get('gc'), 'hv': h.get('hv'), 'cote': h.get('cote'),
                'mu': h.get('mu'), 'pa': h.get('pa'), 'tr': h.get('tr'), 'ec': h.get('ec'),
                't6': h.get('t6'), 't2f': h.get('t2f'), 'vm': h.get('vm'), 'vy': h.get('vy'),
            })
    return pd.DataFrame(rows)


def build_features(races_list, labels_array):
    """DataFrame cheval x course avec toutes les FEATURES, calculées
    strictement avant chaque course (aucune fuite)."""
    fdf = flatten(races_list)
    fdf['pa_num'] = pd.to_numeric(fdf['pa'], errors='coerce')
    # Les courses 'today' passent toujours après l'historique de la même date
    fdf = fdf.sort_values(SORT_CHRONO).reset_index(drop=True)

    # Forme (carrière + musique)
    fdf['win_rate_career'] = fdf['nv'] / fdf['nc'].replace(0, np.nan)
    fdf['place_rate_career'] = fdf['np'] / fdf['nc'].replace(0, np.nan)
    fdf['gain_moy'] = fdf['gc'] / fdf['nc'].replace(0, np.nan)
    fdf = pd.concat([fdf, fdf['mu'].apply(_parse_mu)], axis=1)

    # Stats jockey / entraîneur cumulées strictement avant chaque course
    fdf['win'] = (fdf['pa_num'] == 1).astype(int)
    fdf['place'] = (fdf['pa_num'] <= 3).astype(int)
    for col, pre in [('jk', 'jk'), ('en', 'en')]:
        g = fdf.groupby(col)
        cum_n = g.cumcount()
        cum_w = g['win'].cumsum() - fdf['win']
        cum_p = g['place'].cumsum() - fdf['place']
        fdf[f'{pre}_prior_n'] = cum_n
        fdf[f'{pre}_prior_winrate'] = np.where(cum_n > 0, cum_w / cum_n, np.nan)
        fdf[f'{pre}_prior_placerate'] = np.where(cum_n > 0, cum_p / cum_n, np.nan)

    # Tronçons : accélération / gain de places / finish, moyenne glissante
    # des 5 dernières courses trackées de la même surface
    try:
        idx600 = labels_array.index('600m-400m')
        idx400 = labels_array.index('400m-200m')
        idx200 = labels_array.index('200m-ARR')
    except ValueError:
        idx600 = idx400 = idx200 = None
    final = {idx600, idx400, idx200}

    def _tr_metrics(tr):
        nan3 = pd.Series({'accel': np.nan, 'gain': np.nan, 'finish_kick': np.nan})
        if idx600 is None:
            return nan3
        if not tr or len(tr) < 5:
            return nan3
        if any(s[0] is None for s in tr):
            return nan3
        if any(s[2] is None or s[2] < 20 or s[2] > 80 for s in tr):
            return nan3
        times = [s[1] for s in tr]
        if times != sorted(times):
            return nan3
        seg = {s[5]: s for s in tr}
        if not all(lbl in seg for lbl in (idx600, idx400, idx200)):
            return nan3
        f3 = [seg[idx600], seg[idx400], seg[idx200]]
        fspeed = sum(s[2] for s in f3) / 3
        cruise = [s for s in tr[1:] if s[5] not in final]
        if not cruise:
            return nan3
        cspeed = sum(s[2] for s in cruise) / len(cruise)
        return pd.Series({'accel': fspeed - cspeed,
                          'gain': seg[idx600][3] - seg[idx200][3],
                          'finish_kick': seg[idx200][2]})

    fdf = pd.concat([fdf, fdf['tr'].apply(_tr_metrics)], axis=1)
    fdf = fdf.sort_values(['nom', 'date']).reset_index(drop=True)
    fdf['surface_grp'] = [_classify_surface(t, p) for t, p in zip(fdf['terrain'], fdf['piste'])]

    tracked_sub = fdf[fdf['accel'].notna()][['nom', 'surface_grp', 'date', 'accel', 'gain', 'finish_kick']].copy()
    tracked_sub = tracked_sub.sort_values(['nom', 'surface_grp', 'date']).reset_index(drop=True)
    for col in ['accel', 'gain', 'finish_kick']:
        tracked_sub[f'{col}_roll5'] = (
            tracked_sub.groupby(['nom', 'surface_grp'])[col]
                       .transform(lambda s: s.rolling(5, min_periods=1).mean())
        )
    fdf = fdf.sort_values(SORT_CHRONO).reset_index(drop=True)
    tracked_sub = tracked_sub.sort_values(['date']).reset_index(drop=True)
    fdf = pd.merge_asof(
        fdf, tracked_sub[['nom', 'surface_grp', 'date', 'accel_roll5', 'gain_roll5', 'finish_kick_roll5']],
        on='date', by=['nom', 'surface_grp'], direction='backward', allow_exact_matches=False
    )

    # Confrontations directes vs le champ exact de chaque course
    race_field, race_date = defaultdict(list), {}
    for row in fdf.itertuples():
        race_field[row.race_id].append(row.nom)
        race_date[row.race_id] = row.date
    race_valid = defaultdict(list)
    for row in fdf.dropna(subset=['pa_num']).itertuples():
        race_valid[row.race_id].append((row.nom, row.pa_num))

    pair_wins = defaultdict(list)
    for rid, entries in race_valid.items():
        d = race_date[rid]
        n = len(entries)
        for i in range(n):
            for j in range(n):
                if i != j and entries[i][1] < entries[j][1]:
                    pair_wins[(entries[i][0], entries[j][0])].append(d)
    for k in pair_wins:
        pair_wins[k].sort()

    h2h_w, h2h_l, h2h_n, h2h_o = {}, {}, {}, {}
    for rid, names in race_field.items():
        d = race_date[rid]
        for nom in names:
            w = l = o = 0
            for opp in names:
                if opp == nom:
                    continue
                dw = pair_wins.get((nom, opp))
                dl = pair_wins.get((opp, nom))
                nw = bisect.bisect_left(dw, d) if dw else 0
                nl = bisect.bisect_left(dl, d) if dl else 0
                w += nw
                l += nl
                if nw > 0 or nl > 0:
                    o += 1
            h2h_w[(rid, nom)] = w
            h2h_l[(rid, nom)] = l
            h2h_n[(rid, nom)] = w - l
            h2h_o[(rid, nom)] = o

    keys = list(zip(fdf['race_id'], fdf['nom']))
    fdf['h2h_wins_field'] = [h2h_w[k] for k in keys]
    fdf['h2h_losses_field'] = [h2h_l[k] for k in keys]
    fdf['h2h_net_field'] = [h2h_n[k] for k in keys]
    fdf['h2h_opp_met'] = [h2h_o[k] for k in keys]

    # Elo chronologique pré-course, mise à jour multi-joueurs
    elo = defaultdict(lambda: 1500.0)
    elo_before = {}
    K = 24
    race_order = (fdf[fdf['pa_num'].notna()][['race_id', 'date', 'is_today']]
                  .drop_duplicates().sort_values(SORT_CHRONO))
    for rid in race_order['race_id']:
        entries = race_valid[rid]
        n = len(entries)
        if n < 2:
            continue
        pre = {nom: elo[nom] for nom, _ in entries}
        for nom, _ in entries:
            elo_before[(rid, nom)] = pre[nom]
        delta = defaultdict(float)
        for i in range(n):
            nomi, pai = entries[i]
            for j in range(n):
                if i == j:
                    continue
                nomj, paj = entries[j]
                E = 1 / (1 + 10 ** ((pre[nomj] - pre[nomi]) / 400))
                S = 1.0 if pai < paj else (0.0 if pai > paj else 0.5)
                delta[nomi] += (S - E)
        for nom, _ in entries:
            elo[nom] += K * delta[nom] / (n - 1)
    fdf['elo'] = [elo_before.get((r, n), elo.get(n, 1500.0)) for r, n in zip(fdf['race_id'], fdf['nom'])]

    # Qualité des adversaires lors de la dernière course (devant / battus)
    ahead_avg, ahead_max, beaten_avg, beaten_max = {}, {}, {}, {}
    for rid, entries in race_valid.items():
        for nom, pa in entries:
            ahead = [elo_before.get((rid, o)) for o, po in entries if po < pa and o != nom]
            ahead = [e for e in ahead if e is not None]
            if ahead:
                ahead_avg[(rid, nom)] = float(np.mean(ahead))
                ahead_max[(rid, nom)] = float(np.max(ahead))
            beaten = [elo_before.get((rid, o)) for o, po in entries if po > pa and o != nom]
            beaten = [e for e in beaten if e is not None]
            if beaten:
                beaten_avg[(rid, nom)] = float(np.mean(beaten))
                beaten_max[(rid, nom)] = float(np.max(beaten))

    keys = list(zip(fdf['race_id'], fdf['nom']))
    fdf['_ahead_avg'] = [ahead_avg.get(k, np.nan) for k in keys]
    fdf['_ahead_max'] = [ahead_max.get(k, np.nan) for k in keys]
    fdf['_beaten_avg'] = [beaten_avg.get(k, np.nan) for k in keys]
    fdf['_beaten_max'] = [beaten_max.get(k, np.nan) for k in keys]
    fdf = fdf.sort_values(['nom', 'date']).reset_index(drop=True)
    fdf['last_race_opp_elo_ahead_avg'] = fdf.groupby('nom')['_ahead_avg'].shift(1)
    fdf['last_race_opp_elo_ahead_max'] = fdf.groupby('nom')['_ahead_max'].shift(1)
    fdf['last_race_opp_elo_beaten_avg'] = fdf.groupby('nom')['_beaten_avg'].shift(1)
    fdf['last_race_opp_elo_beaten_max'] = fdf.groupby('nom')['_beaten_max'].shift(1)
    fdf = fdf.drop(columns=['_ahead_avg', '_ahead_max', '_beaten_avg', '_beaten_max'])
    fdf = fdf.sort_values(SORT_CHRONO).reset_index(drop=True)

    # Fraîcheur + trajectoire de classe
    fdf = fdf.sort_values(['nom', 'date']).reset_index(drop=True)
    fdf['date_dt'] = fdf['date'].apply(lambda x: _dt3(int(x) // 10000, (int(x) // 100) % 100, int(x) % 100))
    fdf['prev_date'] = fdf.groupby('nom')['date_dt'].shift(1)
    fdf['days_since_last'] = (fdf['date_dt'] - fdf['prev_date']).dt.days
    fdf['alloc_avg_prev3'] = (
        fdf.groupby('nom')['alloc']
           .apply(lambda s: s.shift(1).rolling(3, min_periods=1).mean())
           .reset_index(level=0, drop=True)
    )
    fdf['alloc_delta'] = fdf['alloc'] - fdf['alloc_avg_prev3']
    fdf['alloc_ratio'] = fdf['alloc'] / fdf['alloc_avg_prev3']
    fdf = fdf.sort_values(SORT_CHRONO).reset_index(drop=True)

    for c in CAT_COLS:
        fdf[c] = fdf[c].astype('category')

    # Niveau des chevaux battus sur 3 mois vs peloton du jour (affichage), puis
    # performances démontrées corrigées du poids (variables du modèle v10)
    from .perf import performance_ratings
    return performance_ratings(opposition_features(fdf, windows=(90,)))


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
    field_avg, field_max, field_top3 = np.full(n, np.nan), np.full(n, np.nan), np.full(n, np.nan)
    r_beaten_max, r_beaten_avg = np.full(n, np.nan), np.full(n, np.nan)
    r_beaten_sum, r_beaten_cnt = np.zeros(n), np.zeros(n)
    for idx in fdf.groupby('race_id').indices.values():
        e, p = elo[idx], pa[idx]
        if len(idx) < 2:
            continue
        tot = e.sum()
        for k, i in enumerate(idx):
            others = np.delete(e, k)
            field_avg[i] = (tot - e[k]) / (len(idx) - 1)
            field_max[i] = others.max()
            field_top3[i] = np.sort(others)[-3:].mean()
            if not np.isnan(p[k]):
                b = e[(p > p[k])]
                if len(b):
                    r_beaten_max[i], r_beaten_avg[i] = b.max(), b.mean()
                    r_beaten_sum[i], r_beaten_cnt[i] = b.sum(), len(b)
    fdf['field_avg'], fdf['field_max'], fdf['field_top3'] = field_avg, field_max, field_top3

    dates = pd.to_datetime(fdf['date'].astype(int).astype(str), format='%Y%m%d').to_numpy()
    out = {f'{c}_{w}': np.full(n, np.nan) for w in windows
           for c in ('opp_avg', 'opp_max', 'beaten_max', 'beaten_avg', 'beaten_pool')}
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
                        # tous les chevaux battus de la période, à poids égal
                        out[f'beaten_pool_{w}'][i] = r_beaten_sum[past].sum() / r_beaten_cnt[past].sum()
                win.append(i)
        start = end
    for k, v in out.items():
        fdf[k] = v
    for w in windows:
        fdf[f'drop_avg_{w}'] = fdf[f'opp_avg_{w}'] - fdf['field_avg']
        fdf[f'beaten_vs_top_{w}'] = fdf[f'beaten_max_{w}'] - fdf['field_max']
        fdf[f'beaten_vs_field_{w}'] = fdf[f'beaten_pool_{w}'] - fdf['field_avg']
        fdf[f'beaten_vs_top3_{w}'] = fdf[f'beaten_pool_{w}'] - fdf['field_top3']
        # Part du peloton du jour moins bien notée que les chevaux qu'il a déjà
        # battus (meilleur battu / moyenne des battus)
        bmax, bpool = fdf[f'beaten_max_{w}'].to_numpy(), fdf[f'beaten_pool_{w}'].to_numpy()
        f_max, f_avg = np.full(n, np.nan), np.full(n, np.nan)
        for idx in fdf.groupby('race_id').indices.values():
            if len(idx) < 2:
                continue
            e = elo[idx]
            for k, i in enumerate(idx):
                others = np.delete(e, k)
                if not np.isnan(bmax[i]):
                    f_max[i] = (others < bmax[i]).mean()
                if not np.isnan(bpool[i]):
                    f_avg[i] = (others < bpool[i]).mean()
        fdf[f'field_below_bmax_{w}'], fdf[f'field_below_bavg_{w}'] = f_max, f_avg
    return fdf

