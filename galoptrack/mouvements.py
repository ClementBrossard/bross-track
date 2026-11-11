"""Expérience « mouvements » : les mouvements de cote annoncent-ils quelque
chose que la cote ne dit pas déjà, au point de justifier un pari ?

Lecture seule. Trois sources, toutes croisées avec les vrais rapports PMU
(simple gagnant et simple placé, 1 € par pari) :
  1. l'historique long (table cotes_hist, plat) : cote ~30 min avant le
     départ -> cote au départ, coupé en deux périodes pour voir si un effet
     tient dans le temps ;
  2. les relevés toutes les 5 min (cotes/live, depuis leur mise en route) :
     cote du premier relevé du jour -> 30 min avant, et 30 min -> 2 min avant ;
  3. le suivi « pour de faux » des règles R1 / R2 (paper/).

Le gain est toujours celui du rapport final : jouer sur un mouvement des
dernières minutes suppose de jouer à la toute dernière minute."""

import logging

import numpy as np
import pandas as pd

from . import live, paper, tables

log = logging.getLogger(__name__)

MV_BINS = [0, 0.70, 0.87, 1.15, 1.40, np.inf]
MV_LABELS = ['très joué (−30 % ou plus)', 'joué (−13 à −30 %)', 'stable', 'lâché (+15 à +40 %)',
             'très lâché (+40 % ou plus)']
BANDS = ((0, 5, 'cote ≤ 5'), (5, 10, '5 à 10'), (10, 20, '10 à 20'), (20, 1e9, 'plus de 20'))
MIN_N = 30


def _payouts(storage):
    """Rapports pour 1 € (gagnant, placé), par course et numéro ; plus
    l'ensemble des courses dont les rapports sont connus."""
    rap = tables.read_table(storage, 'rapports')
    rap = rap.assign(date=pd.to_numeric(rap['date'], errors='coerce'),
                     num_reunion=pd.to_numeric(rap['num_reunion'], errors='coerce'),
                     num_course=pd.to_numeric(rap['num_course'], errors='coerce'),
                     num_pmu=pd.to_numeric(rap['num_pmu'], errors='coerce'),
                     pg=pd.to_numeric(rap['rapport_gagnant'], errors='coerce') / 100,
                     pp=pd.to_numeric(rap['rapport_place'], errors='coerce') / 100)
    ok = rap[rap['pg'].notna() | rap['pp'].notna()].dropna(subset=['date', 'num_course', 'num_pmu'])
    ok = ok.drop_duplicates(['date', 'code_hippo', 'num_course', 'num_pmu'])
    return ok


def _cell(x):
    if len(x) < MIN_N:
        return '—'
    return f"{(x['pay_g'].mean() - 1) * 100:+.0f} € ({len(x)})"


def _grid(d, early, late, title, note):
    """Tranche de cote (au moment le plus tôt) x mouvement -> gain gagnant."""
    d = d[(d[early] > 1) & (d[late] > 1)].copy()
    d['mv'] = pd.cut(d[late] / d[early], MV_BINS, labels=MV_LABELS)
    md = [f"### {title}", "", note, "",
          "Gain net en simple gagnant pour 100 € misés (nombre de paris entre parenthèses ; « — » : moins "
          f"de {MIN_N} paris).", "",
          "| Cote | " + " | ".join(MV_LABELS) + " | tous |", "|---|" + "---:|" * (len(MV_LABELS) + 1)]
    for lo, hi, lab in BANDS:
        b = d[(d[early] > lo) & (d[early] <= hi)]
        md.append(f"| {lab} | " + " | ".join(_cell(b[b['mv'] == m]) for m in MV_LABELS) + f" | {_cell(b)} |")
    md.append("| **toutes** | " + " | ".join(_cell(d[d['mv'] == m]) for m in MV_LABELS) + f" | {_cell(d)} |")
    return md, d


def _stability(d, title):
    """Même chose, en deux périodes successives, avec % gagnants et placé."""
    dates = sorted(d['date'].unique())
    if len(dates) < 4:
        return []
    mid = dates[len(dates) // 2]
    md = [f"### {title} : l'effet tient-il dans le temps ?", "",
          f"Période A : avant {mid} ; période B : à partir de {mid}.", "",
          "| Mouvement | A : paris | A : % gagnants | A : gagnant | A : placé | B : paris | B : % gagnants | "
          "B : gagnant | B : placé |", "|---|" + "---:|" * 8]

    def stats(x):
        if len(x) < MIN_N:
            return "— | — | — | —"
        return (f"{len(x)} | {(x['pay_g'] > 0).mean():.0%} | {(x['pay_g'].mean() - 1) * 100:+.1f} € | "
                f"{(x['pay_p'].mean() - 1) * 100:+.1f} €")
    for m in MV_LABELS + ['tous']:
        sel = d if m == 'tous' else d[d['mv'] == m]
        md.append(f"| {m} | {stats(sel[sel['date'] < mid])} | {stats(sel[sel['date'] >= mid])} |")
    return md


def _history(storage, pay):
    h = tables.read_table(storage, 'cotes_hist')
    if not len(h):
        return ["## 1. Historique long", "", "Table cotes_hist vide.", ""]
    h = h.assign(date=pd.to_numeric(h['date'], errors='coerce'),
                 num_course=pd.to_numeric(h['num_course'], errors='coerce'),
                 num_pmu=pd.to_numeric(h['num_pmu'], errors='coerce'),
                 c30=pd.to_numeric(h['cote_reference'], errors='coerce'),
                 cfin=pd.to_numeric(h['cote_directe'], errors='coerce'))
    h = h.dropna(subset=['date', 'num_course', 'num_pmu']).drop_duplicates(
        ['date', 'code_hippo', 'num_course', 'num_pmu'])
    races = pay[['date', 'code_hippo', 'num_course']].drop_duplicates()
    h = h.merge(races, on=['date', 'code_hippo', 'num_course'], how='inner')
    h = h.merge(pay[['date', 'code_hippo', 'num_course', 'num_pmu', 'pg', 'pp']],
                on=['date', 'code_hippo', 'num_course', 'num_pmu'], how='left')
    h['pay_g'], h['pay_p'] = h['pg'].fillna(0.0), h['pp'].fillna(0.0)
    n_r = h.groupby(['date', 'code_hippo', 'num_course']).ngroups
    md = ["## 1. Historique long : cote 30 min avant → cote au départ (plat)", "",
          f"{n_r} courses, {len(h)} chevaux, du {int(h['date'].min())} au {int(h['date'].max())}.", ""]
    g, d = _grid(h, 'c30', 'cfin', "Gain selon le mouvement des 30 dernières minutes",
                 "Tranche = cote 30 min avant ; mouvement = cote au départ / cote 30 min avant.")
    md += g + [""] + _stability(d, "Historique long") + [""]
    gp = h['grosse_prise'].astype(str).str.lower().isin(['true', '1', '1.0'])
    if gp.any():
        md += ["### Indicateur « grosse prise » du PMU (au départ)", "",
               "| | Paris | % gagnants | Gagnant | Placé |", "|---|---:|---:|---:|---:|"]
        for lab, x in (("signalé « grosse prise »", h[gp]), ("non signalé", h[~gp])):
            if len(x):
                md.append(f"| {lab} | {len(x)} | {(x['pay_g'] > 0).mean():.0%} | "
                          f"{(x['pay_g'].mean() - 1) * 100:+.1f} € | {(x['pay_p'].mean() - 1) * 100:+.1f} € |")
        md.append("")
    return md


def _live(storage, pay):
    days = sorted({k.split('/')[-1][:8] for k in storage.list('cotes/live/') if k.endswith('.json.gz')})
    rows = []
    for day in days:
        courses = live.load_day(storage, day).get('courses') or {}
        p = pay[pay['date'] == int(day)]
        for key, c in courses.items():
            try:
                rnum, num = (int(x) for x in key[1:].split('C'))
            except ValueError:
                continue
            rel, dep = c.get('releves') or [], c.get('depart')
            pc = p[(pd.to_numeric(p['num_reunion'], errors='coerce') == rnum) & (p['num_course'] == num)]
            if not rel or not dep or not len(pc):
                continue
            first = rel[0][1]
            c30 = paper._cote_at(rel, dep - paper.MIN_30)
            c2 = paper._cote_at(rel, dep - paper.MIN_2)
            pg = dict(zip(pc['num_pmu'].astype(int), pc['pg'].fillna(0.0)))
            pp = dict(zip(pc['num_pmu'].astype(int), pc['pp'].fillna(0.0)))
            for n, cm in first.items():
                rows.append({'date': int(day), 'course': f"{day}_{key}", 'disc': c.get('disc'),
                             'c_matin': cm, 'c30': (c30 or {}).get(n), 'c2': (c2 or {}).get(n),
                             'pay_g': pg.get(int(n), 0.0), 'pay_p': pp.get(int(n), 0.0)})
    md = ["## 2. Relevés toutes les 5 minutes (depuis leur mise en route)", ""]
    if not rows:
        return md + ["Aucun relevé exploitable avec des rapports connus.", ""]
    d = pd.DataFrame(rows)
    for c in ('c_matin', 'c30', 'c2'):
        d[c] = pd.to_numeric(d[c], errors='coerce')
    md += [f"{d['date'].nunique()} jours ({min(days)} → {max(days)}), {d['course'].nunique()} courses "
           f"(plat et obstacle), {len(d)} chevaux.", "",
           "**Attention** : quelques semaines seulement. Une case n'a de sens qu'avec plusieurs centaines de paris.", ""]
    g1, _ = _grid(d.dropna(subset=['c_matin', 'c30']), 'c_matin', 'c30',
                  "Du premier relevé du jour à 30 min avant le départ",
                  "Tranche = cote du premier relevé ; mouvement = cote 30 min avant / cote du premier relevé.")
    g2, d2 = _grid(d.dropna(subset=['c30', 'c2']), 'c30', 'c2', "De 30 min à 2 min avant le départ",
                   "Tranche = cote 30 min avant ; mouvement = cote 2 min avant / cote 30 min avant.")
    return md + g1 + [""] + g2 + [""] + _stability(d2, "Relevés, 30 → 2 min") + [""]


def _paper(storage):
    s = paper.summary(storage)
    md = ["## 3. Suivi « pour de faux » des règles R1 et R2", ""]
    if not s.get('jours'):
        return md + ["Pas encore de journée évaluée.", ""]
    md += [f"Depuis le {s['debut']} : {s['jours']} jours, {s['courses']} courses évaluées.", "",
           "| Règle | Paris | Gagnés | Gain net pour 100 € |", "|---|---:|---:|---:|"]
    for k, r in s['regles'].items():
        roi = '—' if r['roi'] is None else f"{r['roi']:+.1f} €"
        md.append(f"| {k} : {r['libelle']} | {r['paris']} | {r['gagnes']} | {roi} |")
    return md + [""]


def run_mouvements(storage):
    from .experiment import _publish
    pay = _payouts(storage)
    md = ["# Mouvements de cotes : annoncent-ils quelque chose que la cote ne dit pas ?", "",
          "Repère : jouer tous les chevaux perd à peu près le prélèvement du PMU (environ 15 à 25 % selon le "
          "pari). Un mouvement n'est intéressant que s'il perd nettement moins que les autres cases de la même "
          "tranche de cote, et encore plus s'il gagne, sur les deux périodes.", ""]
    md += _history(storage, pay)
    md += _live(storage, pay)
    md += _paper(storage)
    return _publish(md)
