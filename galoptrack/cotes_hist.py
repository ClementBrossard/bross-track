"""Historique des cotes PMU des courses de plat déjà collectées : cote de
référence (dernière relevée ~30 min avant le départ) et cote directe (au
départ), avec leur heure et la tendance affichée par le PMU.

Table à part (cotes_hist) : ne modifie aucune table existante. Reprend là où
le dernier passage s'est arrêté (les courses déjà traitées sont sautées)."""

import logging
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime

import pandas as pd

from . import config, pmu, tables

log = logging.getLogger(__name__)

KEYS = ['date', 'code_hippo', 'num_course', 'num_pmu']


def _rapport(p, field):
    r = p.get(field) or {}
    if r.get('typePari') != 'SIMPLE_GAGNANT':
        return None, None, None, None
    return r.get('rapport'), r.get('dateRapport'), r.get('nombreIndicateurTendance'), r.get('favoris')


def _course_rows(key):
    d, hippo, reunion, course = key
    day = datetime.strptime(str(d), '%Y%m%d').date()
    base = {'date': d, 'code_hippo': hippo, 'num_reunion': reunion, 'num_course': course}
    rows = []
    for p in pmu.fetch_participants(day, reunion, course):
        if not p.get('nom'):
            continue
        cr, hr, tr, fr = _rapport(p, 'dernierRapportReference')
        cd, hd, _, _ = _rapport(p, 'dernierRapportDirect')
        rows.append({**base, 'num_pmu': p.get('numPmu'), 'nom_cheval': p['nom'].strip().upper(),
                     'cote_reference': cr, 'heure_reference': hr, 'tendance_reference': tr,
                     'favori_reference': fr, 'cote_directe': cd, 'heure_directe': hd,
                     'grosse_prise': (p.get('dernierRapportDirect') or {}).get('grossePrise')})
    time.sleep(config.PAUSE_ENTRE_REQ)
    # course sans participants : ligne témoin pour ne pas la redemander
    return rows or [{**base, 'num_pmu': 0, 'nom_cheval': ''}]


def run(storage, max_minutes=330, chunk=400):
    ch = tables.read_table(storage, 'chevaux')
    disc = ch['discipline'].fillna('').astype(str).str.upper() if 'discipline' in ch else ''
    ch = ch[(disc == '') | (disc == pmu.PLAT)]
    keys = ch[['date', 'code_hippo', 'num_reunion', 'num_course']].dropna().copy()
    for c in ('date', 'num_reunion', 'num_course'):
        keys[c] = pd.to_numeric(keys[c], errors='coerce')
    keys = keys.dropna().astype({'date': int, 'num_reunion': int, 'num_course': int}).drop_duplicates()
    done = tables.read_table(storage, 'cotes_hist')
    done_keys = set()
    if len(done):
        done_keys = set(zip(pd.to_numeric(done['date']).astype(int), done['code_hippo'].astype(str),
                            pd.to_numeric(done['num_course']).astype(int)))
    todo = [tuple(k) for k in keys.sort_values('date', ascending=False).itertuples(index=False)
            if (k.date, str(k.code_hippo), k.num_course) not in done_keys]
    log.info("Courses de plat : %d, déjà faites : %d, à faire : %d", len(keys), len(done_keys), len(todo))
    start, n_done = time.time(), 0
    for i in range(0, len(todo), chunk):
        if (time.time() - start) / 60 > max_minutes:
            log.info("Temps écoulé : arrêt, la suite au prochain passage")
            break
        part = todo[i:i + chunk]
        with ThreadPoolExecutor(max_workers=config.WORKERS) as ex:
            rows = [r for res in ex.map(_course_rows, part) for r in res]
        tables.append_rows(storage, 'cotes_hist', rows, KEYS)
        n_done += len(part)
        log.info("%d / %d courses (dernière date : %s)", n_done, len(todo), part[-1][0])
    left = len(todo) - n_done
    out = f"Historique des cotes : {n_done} courses traitées, {left} restantes."
    log.info(out)
    return out
