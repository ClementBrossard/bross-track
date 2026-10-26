"""Confrontations (étape 2 du modèle Compétition).

La force d'un adversaire = sa note de classe (classe.py, sans correction
d'expérience) au moment où il a été rencontré. Toutes les variables utilisent
uniquement les courses strictement antérieures.

Sur une fenêtre de `window_days` jours :
  cf_beaten_vs_best   meilleur cheval battu - meilleur rival du jour
  cf_close_vs_best    meilleur cheval devant lequel il a fini à moins d'une
                      longueur - meilleur rival du jour (« battu de peu par un cador »)
  cf_niveau_vs_best   max des deux (niveau démontré) - meilleur rival du jour
  cf_niveau_vs_top3   niveau démontré - moyenne des 3 meilleurs rivaux du jour
  cf_weak_ahead_n     nombre de chevaux plus faibles que le peloton du jour
                      (note < moyenne des rivaux) qui ont fini devant lui
  cf_weak_ahead_rate  le même, par course disputée
Rencontres directes avec les partants du jour (tout l'historique) :
  cf_beat_top         a déjà fini devant le rival le mieux noté du jour (1/0, vide si jamais rencontré)
  cf_lost_to_weaker   un rival du jour moins bien noté que lui a déjà fini devant lui (1/0, vide si aucune rencontre)
"""

import re
from collections import defaultdict, deque

import numpy as np
import pandas as pd

from .classe import class_features

_FRAC = {'1/4': 0.25, '1/2': 0.5, '3/4': 0.75}


def parse_ecart(s):
    """Écart avec le cheval précédent (libellé PMU) -> longueurs."""
    if s is None or (not isinstance(s, str) and pd.isna(s)):
        return np.nan
    t = str(s).strip().lower().replace('ê', 'e').replace('é', 'e')
    if not t:
        return np.nan
    if 'nez' in t:
        return 0.05
    if 'courte' in t or 'cte' in t:
        return 0.1
    if 'tete' in t:
        return 0.2
    if 'encol' in t:
        return 0.3
    if 'loin' in t or 'dist' in t:
        return 30.0
    total, found = 0.0, False
    for frac, v in _FRAC.items():
        if frac in t:
            total += v
            found = True
            t = t.replace(frac, ' ')
    m = re.search(r'\d+(?:[.,]\d+)?', t)
    if m:
        total += float(m.group().replace(',', '.'))
        found = True
    return total if found else np.nan


def confrontation_features(fdf, window_days=365):
    if 'classe_m0' not in fdf:
        fdf = class_features(fdf, prior_weight=0.0, suffix='_m0')
    fdf = fdf.copy()
    n = len(fdf)
    note = fdf['classe_m0'].to_numpy(dtype=float)
    pa = fdf['pa_num'].to_numpy(dtype=float)
    ec = np.array([parse_ecart(v) for v in fdf.get('ec', pd.Series([None] * n))], dtype=float)
    noms = fdf['nom'].to_numpy()
    dates = pd.to_datetime(fdf['date'].astype(int).astype(str), format='%Y%m%d').to_numpy()
    races = list(fdf.groupby('race_id').indices.values())

    # Par course passée : meilleur battu, meilleur « devant de peu », notes des chevaux devant
    beaten_best = np.full(n, np.nan)
    close_best = np.full(n, np.nan)
    ahead_notes = [None] * n
    # Peloton du jour : notes des rivaux
    f_max, f_mean, f_top3 = np.full(n, np.nan), np.full(n, np.nan), np.full(n, np.nan)
    meetings = defaultdict(list)   # (cheval, rival) -> [(date, cheval devant ?)]
    for idx in races:
        v = note[idx]
        p = pa[idx]
        for k, i in enumerate(idx):
            others = np.delete(v, k)
            if len(others) and not np.isnan(others).all():
                f_max[i] = np.nanmax(others)
                f_mean[i] = np.nanmean(others)
                f_top3[i] = np.sort(others[~np.isnan(others)])[-3:].mean()
        if np.isnan(p).all():
            continue
        # longueurs derrière le gagnant
        order = np.argsort(np.where(np.isnan(p), 1e9, p), kind='stable')
        behind = np.full(len(idx), np.nan)
        acc = 0.0
        for rank_pos, k in enumerate(order):
            if np.isnan(p[k]):
                break
            if rank_pos > 0:
                acc = acc + ec[idx[k]] if not np.isnan(acc) else np.nan
            behind[k] = acc
        for k, i in enumerate(idx):
            if np.isnan(p[k]):
                continue
            beaten = v[(p > p[k]) & ~np.isnan(v)]
            if len(beaten):
                beaten_best[i] = beaten.max()
            ahead_mask = (p < p[k])
            ahead_notes[i] = v[ahead_mask & ~np.isnan(v)]
            close = ahead_mask & ~np.isnan(v) & ((behind[k] - behind) <= 1.0)
            if close.any():
                close_best[i] = v[close].max()
            for k2, j in enumerate(idx):
                if k2 != k and not np.isnan(p[k2]):
                    meetings[(noms[i], noms[j])].append((dates[i], p[k] < p[k2]))

    # Fenêtre glissante par cheval
    niv_bat, niv_close = np.full(n, np.nan), np.full(n, np.nan)
    weak_n, weak_rate = np.full(n, np.nan), np.full(n, np.nan)
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
            past = [j for j in win if dates[j] < dates[i] and ahead_notes[j] is not None]
            if past:
                b = beaten_best[past]
                c = close_best[past]
                if not np.isnan(b).all():
                    niv_bat[i] = np.nanmax(b)
                if not np.isnan(c).all():
                    niv_close[i] = np.nanmax(c)
                if not np.isnan(f_mean[i]):
                    w = sum(int((ahead_notes[j] < f_mean[i]).sum()) for j in past)
                    weak_n[i], weak_rate[i] = w, w / len(past)
            win.append(i)
        start = end

    niveau = np.fmax(niv_bat, niv_close)
    fdf['cf_beaten_vs_best'] = niv_bat - f_max
    fdf['cf_close_vs_best'] = niv_close - f_max
    fdf['cf_niveau_vs_best'] = niveau - f_max
    fdf['cf_niveau_vs_top3'] = niveau - f_top3
    fdf['cf_weak_ahead_n'] = weak_n
    fdf['cf_weak_ahead_rate'] = weak_rate

    # Rencontres directes avec les partants du jour
    beat_top, lost_weaker = np.full(n, np.nan), np.full(n, np.nan)
    for idx in races:
        v = note[idx]
        if len(idx) < 2:
            continue
        for k, i in enumerate(idx):
            d = dates[i]
            others = [(k2, j) for k2, j in enumerate(idx) if k2 != k]
            noted = [(v[k2], j) for k2, j in others if not np.isnan(v[k2])]
            if noted:
                top_j = max(noted, key=lambda t: t[0])[1]
                m = [ahead for dd, ahead in meetings.get((noms[i], noms[top_j]), []) if dd < d]
                if m:
                    beat_top[i] = float(any(m))
            if not np.isnan(v[k]):
                met_weaker, lost = False, False
                for k2, j in others:
                    if np.isnan(v[k2]) or v[k2] >= v[k]:
                        continue
                    m = [ahead for dd, ahead in meetings.get((noms[i], noms[j]), []) if dd < d]
                    if m:
                        met_weaker = True
                        lost = lost or not all(m)
                if met_weaker:
                    lost_weaker[i] = float(lost)
    fdf['cf_beat_top'] = beat_top
    fdf['cf_lost_to_weaker'] = lost_weaker
    return fdf


CONFRONT_FEATURES = ['cf_beaten_vs_best', 'cf_close_vs_best', 'cf_niveau_vs_best', 'cf_niveau_vs_top3',
                     'cf_weak_ahead_n', 'cf_weak_ahead_rate', 'cf_beat_top', 'cf_lost_to_weaker']
