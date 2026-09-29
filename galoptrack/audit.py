"""Audit des hippodromes de galop français : pour chaque hippodrome vu dans les
programmes PMU, code retenu, nombre de courses de plat / d'obstacles, courses
annoncées trackées, et test réel des PDF France Galop (avec d'autres codes
candidats si le code retenu ne répond pas)."""

import logging
import re
from datetime import timedelta

from . import collect, config, pmu
from .hippos import CODES_FRANCE, reunion_code

log = logging.getLogger(__name__)


def _pdf_ok(date_galop, code, num_course):
    url = f"{config.TRACKING_BASE}/{date_galop}{code}{int(num_course):02d}_last_times_fr.pdf"
    try:
        r = collect._pdf_session.get(url, timeout=config.TIMEOUT_PDF)
    except Exception:
        return False
    return r.status_code == 200 and r.headers.get('content-type', '').startswith('application/pdf')


def audit(end, n_days=365, step=2, pdf_tests=3):
    stats = {}
    for i in range(1, n_days + 1, step):
        d = end - timedelta(days=i)
        prog = pmu.fetch_programme(d)
        if not prog:
            continue
        for reunion in prog.get('programme', {}).get('reunions', []):
            if ((reunion.get('pays') or {}).get('code') or '').upper() != 'FRA':
                continue
            courses = [(c, pmu.discipline_galop(c)) for c in reunion.get('courses', [])]
            courses = [(c, disc) for c, disc in courses if disc]
            if not courses:
                continue
            hippo = reunion.get('hippodrome') or {}
            nom = (hippo.get('libelleLong') or hippo.get('libelleCourt') or '').upper()
            code_pmu = hippo.get('codeHippodrome') or hippo.get('code', '')
            s = stats.setdefault(nom, {'nom': nom, 'code': reunion_code(reunion), 'code_pmu': code_pmu,
                                       'jours': 0, 'plat': 0, 'obstacles': 0, 'obst_trackees': 0,
                                       'plat_trackees': 0, 'pdf_tests': 0, 'pdf_ok': 0, 'pdf_autre_code': set(),
                                       'plat_pdf_tests': 0, 'plat_pdf_ok': 0,
                                       'derniere': ''})
            s['jours'] += 1
            s['derniere'] = max(s['derniere'], config.yyyymmdd(d))
            for c, disc in courses:
                key = 'plat' if disc == pmu.PLAT else 'obstacles'
                s[key] += 1
                if c.get('courseTrackee'):
                    s['plat_trackees' if key == 'plat' else 'obst_trackees'] += 1
            # Test PDF sur une course d'obstacles (de préférence annoncée trackée)
            obst = [c for c, disc in courses if disc != pmu.PLAT]
            if obst and s['pdf_tests'] < pdf_tests:
                c = next((c for c in obst if c.get('courseTrackee')), obst[0])
                dg, num = config.yyyymmdd(d), c.get('numOrdre')
                s['pdf_tests'] += 1
                if any(_pdf_ok(dg, code, num) for code in collect.pdf_codes(s['code'])):
                    s['pdf_ok'] += 1
                else:
                    court = re.sub(r"^HIPPODROME( DE LA| DE| DES| DU| D')?\s*", '', nom).replace(' ', '')
                    for alt in {code_pmu, court[:3], court[:2] + court[-1:]} - {s['code'], ''}:
                        if _pdf_ok(dg, alt, num):
                            s['pdf_autre_code'].add(alt)
            # Même test sur une course de plat : le code marche-t-il ici ?
            plat = [c for c, disc in courses if disc == pmu.PLAT]
            if plat and s['plat_pdf_tests'] < pdf_tests:
                s['plat_pdf_tests'] += 1
                if any(_pdf_ok(config.yyyymmdd(d), code, plat[0].get('numOrdre'))
                       for code in collect.pdf_codes(s['code'])):
                    s['plat_pdf_ok'] += 1
        log.info("%s audité", config.yyyymmdd(d))
    return sorted(stats.values(), key=lambda s: (-s['obstacles'], -s['plat']))


def report(rows):
    lines = [f"{'Hippodrome (PMU)':32s} {'code':5s} {'PMU':5s} {'connu':5s} {'jours':>5s} {'plat':>5s} "
             f"{'obst':>5s} {'PDF obst':>8s} {'PDF plat':>8s}  autre code PDF"]
    for s in rows:
        lines.append(f"{s['nom'][:32]:32s} {s['code']:5s} {s['code_pmu']:5s} "
                     f"{'oui' if s['code'] in CODES_FRANCE else 'NON':5s} {s['jours']:5d} {s['plat']:5d} "
                     f"{s['obstacles']:5d} {s['pdf_ok']:4d}/{s['pdf_tests']:<3d} {s['plat_pdf_ok']:4d}/{s['plat_pdf_tests']:<3d}  "
                     f"{','.join(sorted(s['pdf_autre_code']))}")
    return '\n'.join(lines)
