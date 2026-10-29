"""Cotes en direct (utilisé par le site sur Render : bibliothèque standard
seulement, ni pandas ni requests).

- live_odds : cotes simple gagnant du moment pour une course ;
- snapshot  : relevé des cotes de toutes les courses de galop françaises du
  jour, déclenché toutes les 5 à 10 minutes par cron-job.org. Les relevés
  d'une journée sont dans cotes/live/AAAAMMJJ.json.gz :
    {"courses": {"R1C3": {"hippo", "disc", "depart",
                          "releves": [[horodatage ms, {"n°": cote}], ...]}}}
  Courses qui partent dans plus de 3 h : un relevé toutes les 30 min
  seulement ; plus aucun relevé une fois la course courue (statut PMU) ou
  5 min après l'heure de départ."""

import gzip
import json
import time
import urllib.request
from datetime import datetime

from . import config
from .hippos import reunion_code_today

AHEAD_MS = 3 * 3600 * 1000      # au-delà : relevé espacé
SPACED_MS = 30 * 60 * 1000
AFTER_START_MS = 5 * 60 * 1000   # marge pour un départ retardé (cote finale)
# Statuts PMU d'une course déjà courue (ou qui ne le sera pas) : cotes figées
FINISHED = ("FIN_COURSE", "ARRIVEE", "ANNULEE", "COURSE_ARRETEE")


def race_over(statut, depart, now_ms):
    """Course partie : statut PMU de fin, ou 5 min après l'heure de départ."""
    if any(s in str(statut or "").upper() for s in FINISHED):
        return True
    return bool(depart) and now_ms > depart + AFTER_START_MS


def day_key(day):
    return f"cotes/live/{day}.json.gz"


def _json(url, timeout=8):
    req = urllib.request.Request(url, headers={**config.HEADERS, "Accept": "application/json"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read().decode("utf-8"))


def _rapport(p, field):
    r = p.get(field) or {}
    if r.get("typePari") != "SIMPLE_GAGNANT":
        return None
    try:
        return float(r.get("rapport")) or None
    except (TypeError, ValueError):
        return None


def _ddmmyyyy(day):
    return f"{day[6:8]}{day[4:6]}{day[0:4]}"


def participants(day, reunion, course):
    data = _json(f"{config.PMU_ONLINE}/{_ddmmyyyy(day)}/R{reunion}/C{course}/participants")
    out = []
    for p in data.get("participants") or []:
        if not p.get("nom"):
            continue
        direct = p.get("dernierRapportDirect") or {}
        out.append({
            "num": p.get("numPmu"),
            "nom": p["nom"].strip().upper(),
            "cote": _rapport(p, "dernierRapportDirect"),
            "cote_ref": _rapport(p, "dernierRapportReference"),
            "heure_cote": direct.get("dateRapport"),
            "non_partant": p.get("statut") == "NON_PARTANT",
            "place": _place(p.get("ordreArrivee")),
        })
    return out


def _place(v):
    try:
        return int(v) or None
    except (TypeError, ValueError):
        return None


def live_odds(day, reunion, course):
    """Cotes du moment d'une course, heure de départ si l'API la donne."""
    out = {"partants": participants(day, reunion, course), "lu_a": int(time.time() * 1000)}
    try:
        info = _json(f"{config.PMU_ONLINE}/{_ddmmyyyy(day)}/R{reunion}/C{course}")
        out["depart"] = info.get("heureDepart")
        out["statut"] = info.get("statut")
        out["duree"] = info.get("dureeCourse")
    except Exception:
        pass
    out["arrivee_definitive"] = "DEFINITIVE" in str(out.get("statut") or "").upper()
    out["terminee"] = race_over(out.get("statut"), out.get("depart"), out["lu_a"])
    return out


def _galop(c):
    s = " ".join(str(c.get(k) or "") for k in ("specialite", "discipline")).upper()
    if "PLAT" in s:
        return "PLAT"
    for mot, disc in (("HAIE", "HAIES"), ("STEEPLE", "STEEPLE"), ("CROSS", "CROSS"), ("OBSTACLE", "OBSTACLE")):
        if mot in s:
            return disc
    return None


def load_day(storage, day):
    key = day_key(day)
    if not storage.exists(key):
        return {"courses": {}}
    return json.loads(gzip.decompress(storage.read_bytes(key)).decode("utf-8"))


def snapshot(storage, now_ms=None, day=None):
    """Un relevé de toutes les courses de galop françaises du jour qui ne sont
    pas encore parties. Retourne un petit résumé."""
    now_ms = now_ms or int(time.time() * 1000)
    day = day or datetime.fromtimestamp(now_ms / 1000, config.TZ).strftime("%Y%m%d")
    prog = _json(f"{config.PMU_ONLINE}/{_ddmmyyyy(day)}", timeout=15)
    data = load_day(storage, day)
    done = skipped = errors = 0
    for reunion in (prog.get("programme") or {}).get("reunions") or []:
        if ((reunion.get("pays") or {}).get("code") or "").upper() != "FRA":
            continue
        for c in reunion.get("courses") or []:
            disc = _galop(c)
            num_c, depart = c.get("numOrdre"), c.get("heureDepart")
            if disc is None or not num_c or not depart:
                continue
            if race_over(c.get("statut"), depart, now_ms):
                continue
            key = f"R{reunion['numOfficiel']}C{num_c}"
            entry = data["courses"].setdefault(key, {"hippo": reunion_code_today(reunion), "disc": disc,
                                                     "releves": []})
            entry["depart"] = depart
            last = entry["releves"][-1][0] if entry["releves"] else 0
            if depart - now_ms > AHEAD_MS and now_ms - last < SPACED_MS:
                skipped += 1
                continue
            try:
                parts = participants(day, reunion["numOfficiel"], num_c)
            except Exception:
                errors += 1
                continue
            cotes = {str(p["num"]): p["cote"] for p in parts if p["cote"] and not p["non_partant"]}
            if cotes:
                entry["releves"].append([now_ms, cotes])
                done += 1
            time.sleep(0.2)
    storage.write_bytes(day_key(day), gzip.compress(json.dumps(data, separators=(",", ":")).encode("utf-8")),
                        content_type="application/gzip")
    return {"jour": day, "courses_relevees": done, "espacees": skipped, "erreurs": errors}


def history(data, reunion, course):
    """{n°: [[horodatage ms, cote], ...]} pour une course."""
    entry = (data.get("courses") or {}).get(f"R{reunion}C{course}") or {}
    out = {}
    for ts, cotes in entry.get("releves") or []:
        for num, cote in cotes.items():
            out.setdefault(num, []).append([ts, cote])
    return out
