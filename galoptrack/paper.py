"""Suivi « pour de faux » des stratégies de paris candidates (aucun argent).

Chaque jour, pour les courses de plat du jour :
- le dashboard garde les pronostics du modèle (pronos/AAAAMMJJ.json) ;
- le relevé toutes les 5 min garde les cotes (cotes/live/AAAAMMJJ.json.gz).
Le lendemain, après la collecte des rapports, `evaluate` applique les règles
avec la cote ~30 min avant le départ et la cote ~2 min avant (dernier relevé),
puis compte le gain réel en simple gagnant (1 € par pari) : paper/AAAAMMJJ.json.

Règles (issues de l'expérience « strategie » du 31/10/2026, non prouvées) :
  R1 « valeur dernière minute » : probabilité combinée marché + modèle +
     mouvement x cote 30 min >= 1,8 et cote 30 min <= 10 ;
  R2 « outsider joué validé par le modèle » : cote 30 min entre 12 et 30,
     cote 2 min <= 74 % de la cote 30 min, cheval dans le top 3 du modèle.
Probabilité combinée : poids appris sur 2025-2026 (marché 1,21, modèle 0,03,
mouvement 1,31), ramenée à 100 % sur la course."""

import json
import math

from . import live

REGLES = {
    'R1': "Valeur dernière minute (marché + modèle + mouvement × cote ≥ 1,8, cote ≤ 10)",
    'R2': "Outsider (cote 12-30) joué (−26 % ou plus) et dans le top 3 du modèle",
}
W_MKT, W_MOD, W_MV = 1.21, 0.03, 1.31
MIN_30, MIN_2 = 30 * 60_000, 2 * 60_000
MAX_AGE = 15 * 60_000   # un relevé plus vieux que ça avant l'instant visé : inutilisable


def pronos_key(day):
    return f"pronos/{day}.json"


def paper_key(day):
    return f"paper/{day}.json"


def save_pronos(storage, races_list, day):
    """Pronostics du modèle des courses de plat du jour (appelé par le dashboard)."""
    out = {}
    for r in races_list:
        if not r.get('today'):
            continue
        hs = [h for h in r['horses'] if h.get('model_prob') is not None and h.get('npart') is not None]
        if not hs:
            continue
        hs.sort(key=lambda h: -h['model_prob'])
        out[f"R{r['rnum']}C{r['num']}"] = {
            'hippo': r['hippo'], 'heure': r.get('heure'),
            'chevaux': {str(h['npart']): {'nom': h['nom'], 'prob': h['model_prob'], 'rang': i + 1}
                        for i, h in enumerate(hs)}}
    if out:
        storage.write_bytes(pronos_key(day), json.dumps(out, ensure_ascii=False).encode('utf-8'),
                            content_type='application/json')
    return len(out)


def _cote_at(releves, t):
    """Dernier relevé à l'instant t ou avant (et pas plus vieux que MAX_AGE)."""
    best = None
    for ts, cotes in releves:
        if ts <= t:
            best = (ts, cotes)
    if best is None or t - best[0] > MAX_AGE:
        return None
    return best[1]


def race_bets(prono, releves, depart):
    """Paris des deux règles pour une course, ou None si les cotes manquent."""
    c30 = _cote_at(releves, depart - MIN_30)
    c2 = _cote_at(releves, depart - MIN_2)
    if not c30 or not c2:
        return None
    nums = [n for n in prono['chevaux'] if c30.get(n) and c2.get(n)]
    if len(nums) < 4:
        return None
    inv = sum(1 / c30[n] for n in nums)
    pm = sum(prono['chevaux'][n]['prob'] for n in nums) or 1
    util = {}
    for n in nums:
        x_mkt = math.log(max(1 / c30[n] / inv, 1e-4))
        x_mod = math.log(max(prono['chevaux'][n]['prob'] / pm, 1e-4))
        x_mv = math.log(c30[n] / c2[n])
        util[n] = W_MKT * x_mkt + W_MOD * x_mod + W_MV * x_mv
    m = max(util.values())
    tot = sum(math.exp(u - m) for u in util.values())
    bets = []
    for n in nums:
        p = math.exp(util[n] - m) / tot
        ch = prono['chevaux'][n]
        base = {'num': int(n), 'nom': ch['nom'], 'cote30': c30[n], 'cote2': c2[n], 'rang_modele': ch['rang']}
        if p * c30[n] >= 1.8 and c30[n] <= 10:
            bets.append({**base, 'regle': 'R1', 'proba': round(p, 4)})
        if 12 <= c30[n] < 30 and c2[n] <= 0.74 * c30[n] and ch['rang'] <= 3:
            bets.append({**base, 'regle': 'R2', 'proba': round(p, 4)})
    return bets


def _payouts(rapports, day):
    """{(réunion, course): {n°: rapport gagnant pour 1 €}} pour les courses dont les rapports sont connus."""
    import pandas as pd
    r = rapports[pd.to_numeric(rapports['date'], errors='coerce') == int(day)]
    out = {}
    for row in r.itertuples():
        try:
            key = (int(row.num_reunion), int(row.num_course))
        except (TypeError, ValueError):
            continue
        d = out.setdefault(key, {})
        g = pd.to_numeric(row.rapport_gagnant, errors='coerce')
        if pd.notna(g) and pd.notna(row.num_pmu):
            d[int(row.num_pmu)] = float(g) / 100
    return out


def evaluate(storage, day, rapports):
    """Applique les règles à une journée passée ; écrit paper/AAAAMMJJ.json."""
    if not storage.exists(pronos_key(day)):
        return None
    pronos = json.loads(storage.read_bytes(pronos_key(day)))
    cotes = live.load_day(storage, day).get('courses') or {}
    pay = _payouts(rapports, day)
    courses, bets, skipped = 0, [], 0
    for key, prono in pronos.items():
        c = cotes.get(key)
        rnum, num = (int(x) for x in key[1:].split('C'))
        if not c or not c.get('depart') or (rnum, num) not in pay:
            skipped += 1
            continue
        b = race_bets(prono, c.get('releves') or [], c['depart'])
        if b is None:
            skipped += 1
            continue
        courses += 1
        for x in b:
            x.update({'course': key, 'hippo': prono['hippo'], 'heure': prono.get('heure'),
                      'gain': round(pay[(rnum, num)].get(x['num'], 0.0), 2)})
            bets.append(x)
    res = {'jour': day, 'courses_evaluees': courses, 'courses_ignorees': skipped, 'paris': bets}
    storage.write_bytes(paper_key(day), json.dumps(res, ensure_ascii=False).encode('utf-8'),
                        content_type='application/json')
    return res


def summary(storage):
    """Bilan cumulé de tous les jours suivis (pour le dashboard)."""
    days = sorted(k.split('/')[-1][:8] for k in storage.list('paper/') if k.endswith('.json'))
    regles = {k: {'libelle': v, 'paris': 0, 'gagnes': 0, 'mise': 0.0, 'gains': 0.0} for k, v in REGLES.items()}
    derniers, courses = [], 0
    for day in days:
        d = json.loads(storage.read_bytes(paper_key(day)))
        courses += d.get('courses_evaluees', 0)
        for b in d.get('paris') or []:
            s = regles.setdefault(b['regle'], {'libelle': b['regle'], 'paris': 0, 'gagnes': 0, 'mise': 0.0, 'gains': 0.0})
            s['paris'] += 1
            s['gagnes'] += int(b['gain'] > 0)
            s['mise'] += 1
            s['gains'] += b['gain']
            derniers.append({**b, 'jour': day})
    for s in regles.values():
        s['roi'] = round((s['gains'] / s['mise'] - 1) * 100, 1) if s['mise'] else None
        s['gains'] = round(s['gains'], 2)
    return {'debut': days[0] if days else None, 'jours': len(days), 'courses': courses,
            'regles': regles, 'derniers': derniers[-60:][::-1]}
