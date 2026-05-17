"""
BROSS&TRACK — Fonctions métier (partagées entre app.py et GitHub Actions)
"""

import io
import re
import threading
from datetime import datetime

import pdfplumber
import requests

_cache = {}
_lock  = threading.Lock()

HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}

PMU_OFFLINE   = "https://offline.turfinfo.api.pmu.fr/rest/client/7/programme"
PMU_ONLINE    = "https://online.turfinfo.api.pmu.fr/rest/client/61/programme"
TRACKING_BASE = "https://www7.france-galop.com/Casaques/Tracking"

HIPPO_NAME_TO_CODE = {
    "CHANTILLY": "CHA", "DEAUVILLE": "DEA", "LONGCHAMP": "LPA",
    "PARIS-LONGCHAMP": "LPA", "PARISLONGCHAMP": "LPA",
    "SAINT-CLOUD": "SAI", "MAISONS-LAFFITTE": "MAI",
    "EVRY": "EVR", "ÉVRY": "EVR", "COMPIEGNE": "COM",
    "COMPIÈGNE": "COM", "CLAIREFONTAINE": "CLF",
    "LE LION D'ANGERS": "LLA", "LION D'ANGERS": "LLA",
    "VICHY": "VIC", "NANTES": "NAR", "STRASBOURG": "STR",
    "LYON PARILLY": "LYO", "LYON-PARILLY": "LYO",
    "LYON LA SOIE": "LLS", "MARSEILLE": "MAR",
    "TOULOUSE": "TOU", "ANGERS": "ANG",
    "CAGNES": "CAG", "CAGNES-SUR-MER": "CAG",
    "BORDEAUX": "BOR", "BORDEAUX LE BOUSCAT": "BOR",
    "FONTAINEBLEAU": "FON",
}

def hippo_to_code(nom: str) -> str:
    nom_up = nom.upper().strip()
    if nom_up in HIPPO_NAME_TO_CODE:
        return HIPPO_NAME_TO_CODE[nom_up]
    for key, code in HIPPO_NAME_TO_CODE.items():
        if key in nom_up or nom_up in key:
            return code
    return ""


# ══════════════════════════════════════════════════════════════════════════════
#  Helpers dates
# ══════════════════════════════════════════════════════════════════════════════

def ts_to_date_pmu(ts_ms: int, tz_offset_ms: int = 3600000) -> str:
    ts_local = (ts_ms + tz_offset_ms) / 1000
    return datetime.utcfromtimestamp(ts_local).strftime("%d%m%Y")

def ts_to_date_galop(ts_ms: int, tz_offset_ms: int = 3600000) -> str:
    ts_local = (ts_ms + tz_offset_ms) / 1000
    return datetime.utcfromtimestamp(ts_local).strftime("%Y%m%d")

def ts_to_display(ts_ms: int, tz_offset_ms: int = 3600000) -> str:
    ts_local = (ts_ms + tz_offset_ms) / 1000
    return datetime.utcfromtimestamp(ts_local).strftime("%d/%m/%Y")

def cached(key, fn):
    with _lock:
        if key in _cache:
            return _cache[key]
    result = fn()
    if result is not None:
        with _lock:
            _cache[key] = result
    return result


# ══════════════════════════════════════════════════════════════════════════════
#  PMU — Programme
# ══════════════════════════════════════════════════════════════════════════════

def fetch_programme(date_pmu: str) -> dict | None:
    def _fetch():
        for url in [f"{PMU_OFFLINE}/{date_pmu}", f"{PMU_ONLINE}/{date_pmu}"]:
            try:
                r = requests.get(url, headers=HEADERS, timeout=15)
                if r.status_code == 200:
                    data = r.json()
                    if data.get("programme", {}).get("reunions"):
                        return data
            except Exception:
                pass
        return None
    return cached(f"prog_{date_pmu}", _fetch)


def get_courses_plat(date_pmu: str) -> list:
    prog = fetch_programme(date_pmu)
    if not prog:
        return []

    courses = []
    for reunion in prog.get("programme", {}).get("reunions", []):
        hippo       = reunion.get("hippodrome", {})
        code_hippo  = hippo.get("codeHippodrome", "")
        nom_hippo   = hippo.get("libelleLong", hippo.get("libelleCourt", ""))
        num_reunion = reunion.get("numOfficiel", reunion.get("numExterneReunion", 0))

        for c in reunion.get("courses", []):
            if c.get("specialite", "").upper() != "PLAT" and c.get("discipline", "").upper() != "PLAT":
                continue
            parcours   = c.get("parcours", "").upper()
            type_piste = c.get("typePiste", "").upper()
            if "SABLE" in parcours or "PSF" in parcours or "FIBRE" in parcours or type_piste == "PSF":
                surface = "PSF"
            else:
                surface = "GAZON"
            courses.append({
                "num_reunion":    num_reunion,
                "num_course":     c.get("numOrdre", 0),
                "num_externe":    c.get("numExterne", c.get("numOrdre", 0)),
                "code_hippo":     code_hippo,
                "nom_hippo":      nom_hippo,
                "libelle":        c.get("libelle", ""),
                "libelle_court":  c.get("libelleCourt", ""),
                "heure":          c.get("heureDepart", 0),
                "distance":       c.get("distance", 0),
                "terrain":        c.get("etatTerrain", c.get("terrain", (c.get("penetrometre") or {}).get("intitule", ""))),
                "surface":        surface,
                "nb_partants":    c.get("nombreDeclaresPartants", 0),
                "dotation":       c.get("montantPrix", 0),
                "conditions":     c.get("conditions", ""),
                "course_trackee": c.get("courseTrackee", False),
                "partants":       _parse_partants(c),
            })
    return courses


def _parse_partants(course: dict) -> list:
    out = []
    for p in course.get("participants", []):
        out.append({
            "num":          p.get("numPmu", 0),
            "nom":          p.get("nom", ""),
            "jockey":       p.get("driver", p.get("jockey", "")),
            "entraineur":   p.get("entraineur", ""),
            "age":          str(p.get("age", "")),
            "sexe":         p.get("sexe", ""),
            "corde":        str(p.get("placeCorde", "")),
            "poids":        str(p.get("poids", "")),
            "musique":      p.get("musique", ""),
            "proprietaire": p.get("proprietaire", ""),
            "nb_courses":   p.get("nombreCourses", p.get("nbCourses", None)),
            "nb_victoires": p.get("nombreVictoires", p.get("nbVictoires", None)),
            "nb_places":    p.get("nombrePlaces", p.get("nbPlaces", None)),
        })
    return sorted(out, key=lambda x: x["num"])


# ══════════════════════════════════════════════════════════════════════════════
#  PMU — Performances détaillées + Participants
# ══════════════════════════════════════════════════════════════════════════════

def fetch_perfs(date_pmu: str, num_reunion: int, num_course: int) -> list:
    def _fetch():
        url = f"{PMU_ONLINE}/{date_pmu}/R{num_reunion}/C{num_course}/performances-detaillees/pretty"
        try:
            r = requests.get(url, headers=HEADERS, timeout=20)
            if r.status_code == 200:
                return r.json()
        except Exception:
            pass
        return None
    data = cached(f"perfs_{date_pmu}_R{num_reunion}C{num_course}", _fetch)
    return (data or {}).get("participants", [])


def fetch_participants(date_pmu: str, num_reunion: int, num_course: int) -> dict:
    def _fetch():
        url = f"{PMU_OFFLINE}/{date_pmu}/R{num_reunion}/C{num_course}/participants"
        try:
            r = requests.get(url, headers=HEADERS, timeout=15)
            if r.status_code == 200:
                return r.json()
        except Exception:
            pass
        return None
    data = cached(f"participants_{date_pmu}_R{num_reunion}C{num_course}", _fetch)
    if not data:
        return {}
    result = {}
    for p in data.get("participants", []):
        num = p.get("numPmu", 0)
        result[num] = {
            "jockey":       p.get("driver", ""),
            "entraineur":   p.get("entraineur", ""),
            "age":          str(p.get("age", "")),
            "sexe":         p.get("sexe", ""),
            "corde":        str(p.get("placeCorde", "")),
            "poids":        f"{p['handicapPoids']/10:.1f}" if p.get("handicapPoids") else "",
            "musique":      p.get("musique", ""),
            "proprietaire": p.get("proprietaire", ""),
            "nb_courses":   p.get("nombreCourses", p.get("nbCourses", None)),
            "nb_victoires": p.get("nombreVictoires", p.get("nbVictoires", None)),
            "nb_places":    p.get("nombrePlaces", p.get("nbPlaces", None)),
        }
    return result


# ══════════════════════════════════════════════════════════════════════════════
#  Résolution course passée → numOrdre
# ══════════════════════════════════════════════════════════════════════════════

def resolve_course_from_prix(date_ts_ms: int, nom_prix: str, hippo_hint: str, tz_offset_ms: int = 3600000) -> dict | None:
    date_pmu   = ts_to_date_pmu(date_ts_ms, tz_offset_ms)
    date_galop = ts_to_date_galop(date_ts_ms, tz_offset_ms)
    date_disp  = ts_to_display(date_ts_ms, tz_offset_ms)
    prog = fetch_programme(date_pmu)
    if not prog:
        return None

    nom_prix_up = nom_prix.upper().strip()
    mots_prix   = [m for m in nom_prix_up.split() if len(m) > 3]
    best_match  = None
    best_score  = 0

    for reunion in prog.get("programme", {}).get("reunions", []):
        hippo        = reunion.get("hippodrome", {})
        code_hippo   = hippo.get("codeHippodrome", "")
        lib_long     = hippo.get("libelleLong", "").upper()
        lib_court_h  = hippo.get("libelleCourt", "").upper()

        if hippo_hint:
            hint_up = hippo_hint.upper()
            if (code_hippo != hint_up and
                hint_up not in lib_long and
                hint_up not in lib_court_h):
                continue

        num_reunion = reunion.get("numOfficiel", 1)

        for c in reunion.get("courses", []):
            libelle   = c.get("libelle", "").upper().strip()
            lib_court = c.get("libelleCourt", "").upper().strip()
            code_hippo_c = c.get("hippodrome", {}).get("codeHippodrome", "") or code_hippo

            score = 0
            if nom_prix_up == libelle or nom_prix_up == lib_court:
                score = 100
            elif nom_prix_up in libelle or libelle in nom_prix_up:
                score = 80
            elif nom_prix_up in lib_court or lib_court in nom_prix_up:
                score = 70
            else:
                mots_lib = [m for m in libelle.split() if len(m) > 3]
                communs  = sum(1 for m in mots_prix if any(m in ml or ml in m for ml in mots_lib))
                if communs > 0:
                    score = 40 + communs * 10

            if score > best_score:
                best_score = score
                best_match = {
                    "num_course":     c.get("numOrdre", 0),
                    "num_reunion":    num_reunion,
                    "code_hippo":     code_hippo_c,
                    "course_trackee": c.get("courseTrackee", False),
                    "date_galop":     date_galop,
                    "date_display":   date_disp,
                    "libelle":        c.get("libelle", ""),
                }

    return best_match if best_score >= 40 else None


# ══════════════════════════════════════════════════════════════════════════════
#  France Galop — Tracking PDF
# ══════════════════════════════════════════════════════════════════════════════

def fetch_tracking_pdf(date_galop: str, code_hippo: str, num_course: int) -> bytes | None:
    def _fetch():
        fname = f"{date_galop}{code_hippo}{num_course:02d}_last_times_fr.pdf"
        url   = f"{TRACKING_BASE}/{fname}"
        try:
            r = requests.get(url, headers=HEADERS, timeout=15)
            if r.status_code == 200:
                return r.content
        except Exception:
            pass
        return None
    return cached(f"pdf_{date_galop}_{code_hippo}_{num_course:02d}", _fetch)


def parse_tracking_for_horse(pdf_bytes: bytes, nom_cheval: str) -> dict | None:
    try:
        with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
            pages_text = [(page.extract_text(layout=True) or "") for page in pdf.pages]
            nom_up   = nom_cheval.upper().strip()
            mots_nom = [m for m in nom_up.split() if len(m) > 2]
            horse_page_obj   = _find_horse_page_obj(pdf.pages, nom_up, mots_nom)
            positions_course = _extract_positions_from_page_obj(horse_page_obj) if horse_page_obj else []
            vitesses_course  = _extract_vitesses_from_page_obj(horse_page_obj)  if horse_page_obj else []
    except Exception:
        return None

    full_text = "\n".join(pages_text)
    lines     = full_text.split("\n")

    nom_up   = nom_cheval.upper().strip()
    mots_nom = [m for m in nom_up.split() if len(m) > 2]

    course_info    = {}
    troncon_labels = []

    for line in lines[:30]:
        m = re.search(r"(\d{3,5})\s*m", line)
        if m and not course_info.get("distance_m"):
            course_info["distance_m"] = int(m.group(1))
        m = re.search(r"(Bon|Souple|Lourd|Très lourd|Léger)[^(]*\(([\d.]+)\)", line, re.I)
        if m:
            course_info["terrain"]      = m.group(1)
            course_info["penetrometre"] = float(m.group(2))
        if not troncon_labels:
            labels = re.findall(r"(DEP\s*[-–]\s*\d+m|\d+m\s*[-–]\s*\d+m|\d+m\s*[-–]\s*ARR)", line, re.I)
            if len(labels) >= 2:
                troncon_labels = [l.replace(" ", "").replace("–", "-") for l in labels]

    best_line  = None
    best_score = 0
    for line in lines:
        score = sum(1 for m in mots_nom if m in line.upper())
        if score > best_score and re.search(r"\d{2}:\d{2}\.\d{2}", line):
            best_score = score
            best_line  = line

    if not best_line or best_score == 0:
        return None

    cheval = {"course_info": course_info, "troncons": []}

    m = re.search(r"(\d+)['’](\d{2})[\"]{1,2}(\d{2})", best_line)
    if m:
        mn, sec, cs = int(m.group(1)), int(m.group(2)), int(m.group(3))
        cheval["temps_officiel"]     = f"{mn}\'{sec:02d}\"{cs:02d}"
        cheval["temps_officiel_sec"] = round(mn * 60 + sec + cs / 100, 2)

    m = re.search(r"(\d{2,3}[.,]\d)\s*km", best_line, re.I)
    if m:
        cheval["vitesse_max_kmh"] = float(m.group(1).replace(",", "."))

    m = re.search(r"(\d{3,5})[,.](\d{2})\s*$", best_line.strip())
    if m:
        cheval["distance_parcourue"] = float(f"{m.group(1)}.{m.group(2)}")

    raw_troncons = []
    for i, (mn, sec, cs) in enumerate(re.findall(r"(\d{2}):(\d{2})\.(\d{2})", best_line)):
        t_sec = int(mn) * 60 + int(sec) + int(cs) / 100
        label = troncon_labels[i] if i < len(troncon_labels) else f"T{i+1}"
        raw_troncons.append({"index": i+1, "label": label, "temps": f"{mn}:{sec}.{cs}", "temps_sec": round(t_sec, 2)})

    temps_off_sec = cheval.get("temps_officiel_sec", 0)
    filtered = []
    for t in raw_troncons:
        if temps_off_sec and abs(t["temps_sec"] - temps_off_sec) < 0.1:
            continue
        if filtered and t["temps_sec"] < filtered[-1]["temps_sec"] * 0.7:
            cheval["temps_600m"]     = t["temps"]
            cheval["temps_600m_sec"] = t["temps_sec"]
            continue
        filtered.append(t)

    troncons_secteur = []
    for i, t in enumerate(filtered):
        duree_sec = t["temps_sec"] if i == 0 else round(t["temps_sec"] - filtered[i-1]["temps_sec"], 2)
        mn_d  = int(duree_sec // 60)
        sec_d = duree_sec % 60
        troncons_secteur.append({
            "index":     i + 1,
            "label":     t["label"],
            "temps":     f"{mn_d:02d}:{sec_d:05.2f}",
            "temps_sec": duree_sec,
            "cumul":     t["temps"],
            "cumul_sec": t["temps_sec"],
        })
    cheval["troncons"] = troncons_secteur

    cheval_page = _find_horse_page(pages_text, nom_up, mots_nom)
    if cheval_page:
        _parse_horse_detail_page(cheval_page, cheval)

    if positions_course:
        cheval["positions_troncons"] = positions_course
        for i, t in enumerate(cheval.get("troncons", [])):
            if i < len(positions_course):
                t["position"] = positions_course[i]

    if vitesses_course:
        cheval["vitesses_troncons"] = vitesses_course
        for i, t in enumerate(cheval.get("troncons", [])):
            if i < len(vitesses_course):
                t["vitesse_kmh"] = vitesses_course[i]

    _compute_indicators(cheval)
    return cheval


def _find_horse_page(pages_text: list, nom_up: str, mots_nom: list) -> str | None:
    best_page  = None
    best_score = 0
    for page_text in pages_text[1:]:
        score = sum(1 for m in mots_nom if m in page_text.upper())
        if score > best_score and "Vitesse moyenne" in page_text and "Position" in page_text:
            best_score = score
            best_page  = page_text
    return best_page if best_score > 0 else None


def _find_horse_page_obj(pdf_pages, nom_up: str, mots_nom: list):
    best_page  = None
    best_score = 0
    for page in pdf_pages[1:]:
        text = page.extract_text(layout=True) or ""
        score = sum(1 for m in mots_nom if m in text.upper())
        if score > best_score and "Vitesse moyenne" in text and "Position" in text:
            best_score = score
            best_page  = page
    return best_page if best_score > 0 else None


def _extract_positions_from_page_obj(page) -> list:
    words = page.extract_words()
    pos_y = None
    for w in words:
        if w["text"] == "Position":
            pos_y = w["top"]
            break
    if not pos_y:
        return []
    nums_with_x = []
    for w in words:
        if pos_y - 150 < w["top"] < pos_y + 100:
            if re.match(r"^\d{1,2}$", w["text"]) and int(w["text"]) <= 20:
                nums_with_x.append((float(w["x0"]), int(w["text"])))
    nums_with_x.sort(key=lambda t: t[0])
    return [n for _, n in nums_with_x]


def _extract_vitesses_from_page_obj(page) -> list:
    words = page.extract_words()
    vitesse_y = None
    for w in words:
        if w["text"] == "Vitesse":
            y = w["top"]
            ligne = [x for x in words if abs(x["top"] - y) < 3]
            vals = [x for x in ligne
                    if re.match(r"^\d{2,3}(?:[,\.]\d)?$", x["text"])
                    and 40 <= float(x["text"].replace(",", ".")) <= 90]
            if len(vals) >= 4:
                vitesse_y = y
                break
    if vitesse_y is None:
        return []
    ligne = [w for w in words if abs(w["top"] - vitesse_y) < 5]
    vals = [(float(w["x0"]), float(w["text"].replace(",", ".")))
            for w in ligne
            if re.match(r"^\d{2,3}(?:[,\.]\d)?$", w["text"])
            and 40 <= float(w["text"].replace(",", ".")) <= 90]
    vals.sort(key=lambda t: t[0])
    return [v for _, v in vals]


def _parse_horse_detail_page(page_text: str, cheval: dict) -> None:
    m = re.search(r"redk\s*:?\s*(\d+)[’'](\d{2})[”\"](\d{2})", page_text)
    if m:
        mn, sec, cs = int(m.group(1)), int(m.group(2)), int(m.group(3))
        cheval["redk"]     = f"{mn}'{sec:02d}\"{cs:02d}"
        cheval["redk_sec"] = round(mn * 60 + sec + cs / 100, 2)

    m = re.search(r"Vitesse maximale\s+([\d,\.]+)", page_text)
    if m:
        cheval["vitesse_max_kmh"] = float(m.group(1).replace(",", "."))

    m = re.search(r"Vitesse moyenne\s+([\d,\.]+)\s*$", page_text, re.M)
    if m:
        cheval["vitesse_moyenne_kmh"] = float(m.group(1).replace(",", "."))

    m = re.search(r"Tronçon le plus rapide\s+([\d:,.]+)\s+\(tronçon ([^)]+)\)", page_text)
    if m:
        cheval["troncon_plus_rapide"]       = m.group(1)
        cheval["troncon_plus_rapide_label"] = m.group(2).strip()

    vitesses  = _extract_row_values(page_text, r"Vitesse moyenne\s*\(km/h\)")
    positions = _extract_row_values(page_text, r"Position\s*en course")
    foulees   = _extract_row_values(page_text, r"Nombre de foulées")

    for i, t in enumerate(cheval.get("troncons", [])):
        if i < len(vitesses):  t["vitesse_kmh"] = vitesses[i]
        if i < len(positions): t["position"]    = int(positions[i])
        if i < len(foulees):   t["foulees"]     = int(foulees[i])

    if vitesses:  cheval["vitesses_troncons"]  = vitesses
    if positions: cheval["positions_troncons"] = [int(p) for p in positions]


def _extract_row_values(page_text: str, label_pattern: str) -> list:
    m = re.search(label_pattern + r"[^\n]*\n([^\n]+)", page_text)
    if not m:
        return []
    row  = m.group(1)
    vals = re.findall(r"(\d{1,3}(?:[,.]\d)?)", row)
    result = []
    for v in vals:
        try:
            result.append(float(v.replace(",", ".")))
        except ValueError:
            pass
    return result


def _compute_indicators(cheval: dict) -> None:
    positions = cheval.get("positions_troncons", [])
    vitesses  = cheval.get("vitesses_troncons",  [])

    if len(positions) >= 2:
        remontee = positions[0] - positions[-1]
        cheval["remontee_places"] = remontee
        cheval["score_remontee"]  = min(10, max(0, round(remontee * 1.5)))

    if positions:
        top3 = sum(1 for p in positions if p <= 3)
        cheval["pct_top3"]      = round(top3 / len(positions) * 100)
        cheval["profil_leader"] = cheval["pct_top3"] >= 60

    if len(vitesses) >= 4:
        vit_moy = sum(vitesses) / len(vitesses)
        vit_fin = sum(vitesses[-3:]) / 3
        cheval["score_finish"] = round((vit_fin / vit_moy - 1) * 100, 1)

    if len(vitesses) >= 3:
        vits_utiles = vitesses[1:]
        cheval["regularite"] = round(max(vits_utiles) - min(vits_utiles), 1)

    if len(vitesses) >= 4:
        vit_milieu = sum(vitesses[1:-2]) / max(1, len(vitesses) - 3)
        vit_fin2   = sum(vitesses[-2:]) / 2
        cheval["chute_finale"] = round(vit_milieu - vit_fin2, 1)


# ══════════════════════════════════════════════════════════════════════════════
#  Pipeline tracking historique
# ══════════════════════════════════════════════════════════════════════════════

def get_horse_tracking_history(courses_passees: list, nom_cheval: str, max_courses: int = 6) -> list:
    results    = []
    tentatives = 0

    for course in courses_passees:
        if len(results) >= max_courses:
            break
        tentatives += 1
        if tentatives > 40:
            break

        date_ts    = course.get("date", 0)
        tz_offset  = course.get("timezoneOffset", 3600000)
        nom_prix   = course.get("nomPrix", "")
        hippo_pmu  = course.get("hippodrome", "")
        discipline = course.get("discipline", "").upper()

        if discipline in ("TROT_ATTELE", "TROT_MONTE", "ATTELE", "MONTE"):
            continue

        entry = {
            "date_display":   ts_to_display(date_ts, tz_offset),
            "nom_prix":       nom_prix,
            "hippodrome":     hippo_pmu,
            "distance":       course.get("distance", 0),
            "terrain":        course.get("etatTerrain", ""),
            "tracking":       None,
            "course_trackee": False,
            "resolved":       False,
        }

        for p in course.get("participants", []):
            if p.get("itsHim"):
                entry["classement"] = p.get("place", {}).get("place", "?")
                entry["jockey"]     = p.get("nomJockey", "")
                break

        resolved = resolve_course_from_prix(date_ts, nom_prix, hippo_pmu, tz_offset)
        if resolved and resolved.get("num_course"):
            code_h = resolved["code_hippo"] or hippo_to_code(hippo_pmu)
            entry.update({"resolved": True, "course_trackee": resolved["course_trackee"],
                          "code_hippo": code_h, "num_course": resolved["num_course"],
                          "libelle": resolved["libelle"], "resolution_strategy": "programme_pmu"})
            pdf = fetch_tracking_pdf(resolved["date_galop"], code_h, resolved["num_course"])
            if pdf:
                tracking = parse_tracking_for_horse(pdf, nom_cheval)
                if tracking:
                    entry["tracking"] = tracking
                    entry["has_tracking_data"] = True
        else:
            code_hippo = hippo_to_code(hippo_pmu)
            date_galop = ts_to_date_galop(date_ts, tz_offset)
            if code_hippo:
                entry["code_hippo"] = code_hippo
                for num_c in range(1, 11):
                    pdf = fetch_tracking_pdf(date_galop, code_hippo, num_c)
                    if not pdf:
                        continue
                    tracking = parse_tracking_for_horse(pdf, nom_cheval)
                    if tracking:
                        entry.update({"tracking": tracking, "has_tracking_data": True,
                                      "num_course": num_c, "course_trackee": True, "resolved": True,
                                      "resolution_strategy": "scan_brute"})
                        break

        results.append(entry)

    return results


def count_with_tracking(results):
    return sum(1 for r in results if r.get("has_tracking_data"))
