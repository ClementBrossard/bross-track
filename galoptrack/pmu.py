"""Client de l'API PMU (programme, participants, rapports définitifs).

Reprend la logique validée des notebooks scraper / enrichissement / cotes /
rapports, en un seul endroit."""

import logging
import time
from datetime import date

import requests

from . import config
from .hippos import CODES_FRANCE, reunion_code

log = logging.getLogger(__name__)

_session = requests.Session()
_session.headers.update(config.HEADERS)
_programme_cache = {}


def get_json(url, timeout=config.TIMEOUT_API, max_retries=3, pause=2):
    """GET JSON avec petit retry (l'API PMU est parfois instable). None si
    404 / vide / échec après les tentatives."""
    for tentative in range(1, max_retries + 1):
        try:
            r = _session.get(url, timeout=timeout)
            if r.status_code == 200 and r.text.strip():
                return r.json()
            if r.status_code in (204, 404):
                return None
        except (requests.RequestException, ValueError):
            pass
        if tentative < max_retries:
            time.sleep(pause)
    return None


def fetch_programme(d: date):
    """Programme PMU du jour (offline puis online). Seuls les succès sont mis
    en cache (un échec en cache bloquerait tout retry)."""
    date_pmu = config.ddmmyyyy(d)
    if date_pmu in _programme_cache:
        return _programme_cache[date_pmu]
    for base in (config.PMU_OFFLINE, config.PMU_ONLINE):
        data = get_json(f"{base}/{date_pmu}", max_retries=2)
        if data and data.get('programme', {}).get('reunions'):
            _programme_cache[date_pmu] = data
            return data
    return None


def _type_piste(c):
    # PMU ne renseigne pas toujours typePiste -> repli sur le champ parcours
    parcours = (c.get('parcours') or '').upper()
    return c.get('typePiste') or ('PSF' if 'SABLE' in parcours else ('HERBE' if 'HERBE' in parcours else ''))


def list_courses_plat(d: date):
    """Courses de plat françaises du jour, avec toutes les infos de niveau
    course utilisées par le tracking et l'enrichissement."""
    prog = fetch_programme(d)
    if not prog:
        return []
    courses = []
    for reunion in prog.get('programme', {}).get('reunions', []):
        code_hippo = reunion_code(reunion)
        if not code_hippo or code_hippo not in CODES_FRANCE:
            continue
        num_reunion = reunion.get('numOfficiel') or reunion.get('numReunion') or reunion.get('numero', 0)
        if not num_reunion:
            continue
        for c in reunion.get('courses', []):
            disc = (c.get('specialite') or c.get('discipline') or '').upper()
            if 'PLAT' not in disc:
                continue
            if 'ANNUL' in (c.get('statut') or '').upper():
                continue
            num_course = c.get('numOrdre', 0)
            if not num_course:
                continue
            penetro = c.get('penetrometre') or {}
            courses.append({
                'code_hippo': code_hippo,
                'num_reunion': num_reunion,
                'num_course': num_course,
                'trackee': c.get('courseTrackee', False),
                'libelle': c.get('libelle', ''),
                # tracking_data.csv
                'terrain': penetro.get('intitule', ''),
                'penetrometre_valeur': penetro.get('valeurMesure', ''),
                'type_piste_brut': c.get('typePiste', ''),
                # chevaux_data.csv
                'categorie_particularite': c.get('categorieParticularite', ''),
                'allocation_totale': c.get('montantPrix', ''),
                'allocation_1er': c.get('montantOffert1er', ''),
                'condition_age': c.get('conditionAge', ''),
                'condition_sexe': c.get('conditionSexe', ''),
                'distance_course': c.get('distance', ''),
                'type_piste': _type_piste(c),
                'duree_course_ms': c.get('dureeCourse', ''),
            })
    return courses


def fetch_participants(d: date, num_reunion, num_course):
    date_pmu = config.ddmmyyyy(d)
    for base in (config.PMU_OFFLINE, config.PMU_ONLINE):
        data = get_json(f"{base}/{date_pmu}/R{num_reunion}/C{num_course}/participants", max_retries=2)
        if data and data.get('participants'):
            return data['participants']
    return []


def fetch_rapports_definitifs(d: date, num_reunion, num_course):
    date_pmu = config.ddmmyyyy(d)
    return get_json(f"{config.PMU_OFFLINE}/{date_pmu}/R{num_reunion}/C{num_course}/rapports-definitifs")


def cote_directe(p):
    """Cote DIRECT SIMPLE_GAGNANT (la plus proche du départ), '' si absente
    — identique au notebook cotes."""
    rapport = p.get('dernierRapportDirect') or {}
    if rapport.get('typePari') == 'SIMPLE_GAGNANT':
        return rapport.get('rapport', '')
    return ''
