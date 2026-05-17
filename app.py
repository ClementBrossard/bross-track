"""
BROSS&TRACK — Backend Flask
"""

import base64
import json
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from functools import wraps

import requests
from flask import Flask, jsonify, render_template, request, session, redirect

from core import (
    get_courses_plat, fetch_perfs, fetch_participants,
    get_horse_tracking_history, count_with_tracking,
    fetch_tracking_pdf, parse_tracking_for_horse,
    ts_to_date_pmu, ts_to_date_galop,
)

app = Flask(__name__)
app.secret_key = "br0ss_tr4ck_s3cr3t_k3y_2024"
PASSWORD = "BROSSARDTRACKWIN"

GITHUB_PAT  = os.environ.get("GITHUB_PAT", "")
GITHUB_REPO = "ClementBrossard/bross-track"
DATA_BRANCH = "data"
GITHUB_HEADERS = {
    "Authorization": f"Bearer {GITHUB_PAT}",
    "Accept": "application/vnd.github.v3+json",
}


def login_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if not session.get("authenticated"):
            return redirect("/login")
        return f(*args, **kwargs)
    return decorated


# ══════════════════════════════════════════════════════════════════════════════
#  GitHub cache helpers
# ══════════════════════════════════════════════════════════════════════════════

def _cache_path(date_galop: str, code_hippo: str, num_course: int) -> str:
    return f"data/{date_galop}/{code_hippo}_C{num_course:02d}.json"


def get_race_cache(date_galop: str, code_hippo: str, num_course: int) -> dict | None:
    if not GITHUB_PAT:
        return None
    url = f"https://api.github.com/repos/{GITHUB_REPO}/contents/{_cache_path(date_galop, code_hippo, num_course)}"
    try:
        r = requests.get(url, headers=GITHUB_HEADERS, params={"ref": DATA_BRANCH}, timeout=5)
        if r.status_code == 200:
            content = base64.b64decode(r.json()["content"]).decode()
            return json.loads(content)
    except Exception:
        pass
    return None


def trigger_github_parse(date_pmu: str, date_galop: str, code_hippo: str,
                          num_reunion: int, num_course: int) -> bool:
    if not GITHUB_PAT:
        return False
    url = f"https://api.github.com/repos/{GITHUB_REPO}/actions/workflows/parse_course.yml/dispatches"
    payload = {
        "ref": "main",
        "inputs": {
            "date_pmu":    date_pmu,
            "date_galop":  date_galop,
            "code_hippo":  code_hippo,
            "num_reunion": str(num_reunion),
            "num_course":  str(num_course),
        },
    }
    try:
        r = requests.post(url, json=payload, headers=GITHUB_HEADERS, timeout=10)
        return r.status_code == 204
    except Exception:
        return False


# ══════════════════════════════════════════════════════════════════════════════
#  Login
# ══════════════════════════════════════════════════════════════════════════════

LOGIN_PAGE = """<!DOCTYPE html>
<html lang="fr">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>BROSS&TRACK — Accès</title>
<style>
  * { box-sizing: border-box; margin: 0; padding: 0; }
  body {
    background: #0a0c10;
    min-height: 100vh;
    display: flex;
    align-items: center;
    justify-content: center;
    font-family: 'DM Mono', monospace;
  }
  .box {
    background: #12151c;
    border: 1px solid #2a2d35;
    border-top: 3px solid #b8860b;
    border-radius: 8px;
    padding: 48px 40px;
    width: 100%;
    max-width: 380px;
    text-align: center;
  }
  .logo {
    font-family: sans-serif;
    font-weight: 900;
    font-size: 22px;
    letter-spacing: 3px;
    color: #d4a017;
    margin-bottom: 8px;
  }
  .sub {
    font-size: 11px;
    letter-spacing: 2px;
    color: #5a5d6a;
    margin-bottom: 36px;
  }
  input[type=password] {
    width: 100%;
    background: #0a0c10;
    border: 1px solid #2a2d35;
    border-radius: 4px;
    color: #f5f2eb;
    padding: 12px 14px;
    font-family: 'DM Mono', monospace;
    font-size: 14px;
    letter-spacing: 3px;
    text-align: center;
    outline: none;
    margin-bottom: 16px;
  }
  input[type=password]:focus { border-color: #b8860b; }
  button {
    width: 100%;
    background: #b8860b;
    color: #0a0c10;
    border: none;
    border-radius: 4px;
    padding: 12px;
    font-weight: 700;
    font-size: 13px;
    letter-spacing: 2px;
    cursor: pointer;
    font-family: sans-serif;
  }
  button:hover { background: #d4a017; }
  .error {
    color: #c0392b;
    font-size: 11px;
    margin-top: 12px;
    letter-spacing: 1px;
  }
</style>
</head>
<body>
<div class="box">
  <div class="logo">BROSS&amp;TRACK</div>
  <div class="sub">ANALYSE · TRACKING · COMPARATIF</div>
  <form method="POST" action="/login">
    <input type="password" name="password" placeholder="MOT DE PASSE" autofocus>
    <button type="submit">ACCÉDER</button>
    {error}
  </form>
</div>
</body>
</html>"""


# ══════════════════════════════════════════════════════════════════════════════
#  Routes
# ══════════════════════════════════════════════════════════════════════════════

@app.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        if request.form.get("password") == PASSWORD:
            session["authenticated"] = True
            return redirect("/")
        return LOGIN_PAGE.replace("{error}", '<div class="error">Mot de passe incorrect.</div>')
    return LOGIN_PAGE.replace("{error}", "")


@app.route("/logout")
def logout():
    session.clear()
    return redirect("/login")


@app.route("/")
@login_required
def index():
    return render_template("index.html")


@app.route("/api/programme")
@login_required
def api_programme():
    jour = request.args.get("jour", datetime.now().strftime("%d%m%Y"))
    return jsonify({"jour": jour, "courses": get_courses_plat(jour)})


@app.route("/api/course/partants")
@login_required
def api_partants():
    jour     = request.args.get("jour", datetime.now().strftime("%d%m%Y"))
    reunion  = int(request.args.get("reunion", 1))
    course_n = int(request.args.get("course", 1))

    perfs = fetch_perfs(jour, reunion, course_n)
    if not perfs:
        return jsonify({"error": "Impossible de récupérer les performances", "partants": []})

    participants_info = fetch_participants(jour, reunion, course_n)

    courses_du_jour = get_courses_plat(jour)
    course_ref = next(
        (c for c in courses_du_jour if c["num_reunion"] == reunion and c["num_course"] == course_n),
        None
    )

    def traiter_partant(p):
        nom  = p.get("nomCheval", "")
        num  = p.get("numPmu", 0)
        info = participants_info.get(num, {})

        nb_courses   = info.get("nb_courses")   or p.get("nombreCourses",   p.get("nbCourses",   None))
        nb_victoires = info.get("nb_victoires") or p.get("nombreVictoires", p.get("nbVictoires", None))
        nb_places    = info.get("nb_places")    or p.get("nombrePlaces",    p.get("nbPlaces",    None))

        return {
            "num":        num, "nom": nom,
            "jockey":     info.get("jockey") or p.get("nomJockey", ""),
            "entraineur": info.get("entraineur") or p.get("entraineur", ""),
            "age":        info.get("age") or str(p.get("age", "")),
            "sexe":       info.get("sexe") or p.get("sexe", ""),
            "poids":      info.get("poids") or "",
            "corde":      info.get("corde") or "",
            "musique":    info.get("musique") or p.get("musique", ""),
            "nb_courses":   nb_courses,
            "nb_victoires": nb_victoires,
            "nb_places":    nb_places,
            "historique": [],
            "nb_courses_trackers": 0,
        }

    partants_out = []
    with ThreadPoolExecutor(max_workers=8) as executor:
        futures = {executor.submit(traiter_partant, p): p for p in perfs}
        for future in as_completed(futures):
            try:
                partants_out.append(future.result())
            except Exception:
                pass

    if course_ref and not course_ref.get("terrain") and perfs:
        terrain_perf = perfs[0].get("etatTerrain", perfs[0].get("terrain", ""))
        if terrain_perf:
            course_ref["terrain"] = terrain_perf

    return jsonify({
        "partants": sorted(partants_out, key=lambda x: x["num"]),
        "course_ref": course_ref,
    })


@app.route("/api/horse/tracking")
@login_required
def api_horse_tracking():
    jour       = request.args.get("jour", datetime.now().strftime("%d%m%Y"))
    reunion    = int(request.args.get("reunion", 1))
    course_n   = int(request.args.get("course", 1))
    nom        = request.args.get("nom", "").strip()
    max_hist   = int(request.args.get("max_hist", 6))
    date_galop = request.args.get("date_galop", "")
    code_hippo = request.args.get("code_hippo", "").upper()

    if not nom:
        return jsonify({"error": "nom requis", "historique": [], "nb_courses_trackers": 0})

    # Vérifie le cache GitHub en priorité
    if date_galop and code_hippo:
        cache = get_race_cache(date_galop, code_hippo, course_n)
        if cache:
            horse_data = cache.get("horses", {}).get(nom)
            if horse_data:
                return jsonify({"nom": nom, "from_cache": True, **horse_data})

    # Parse à la volée
    perfs = fetch_perfs(jour, reunion, course_n)
    horse_perf = next(
        (p for p in perfs if p.get("nomCheval", "").upper().strip() == nom.upper()),
        None
    )
    if not horse_perf:
        return jsonify({"error": "Cheval non trouvé", "historique": [], "nb_courses_trackers": 0})

    historique = get_horse_tracking_history(horse_perf.get("coursesCourues", []), nom, max_courses=max_hist)
    return jsonify({
        "nom": nom,
        "from_cache": False,
        "historique": historique,
        "nb_courses_trackers": count_with_tracking(historique),
    })


@app.route("/api/course/cache-status")
@login_required
def api_cache_status():
    date_galop = request.args.get("date_galop", "")
    code_hippo = request.args.get("hippo", "").upper()
    course_n   = int(request.args.get("course", 1))
    if not (date_galop and code_hippo):
        return jsonify({"cached": False})
    data = get_race_cache(date_galop, code_hippo, course_n)
    return jsonify({"cached": data is not None})


@app.route("/api/course/parse", methods=["POST"])
@login_required
def api_parse_course():
    body       = request.get_json() or {}
    jour       = body.get("jour", "")
    reunion    = int(body.get("reunion", 1))
    course_n   = int(body.get("course", 1))
    date_galop = body.get("date_galop", "")
    code_hippo = body.get("code_hippo", "").upper()

    if not all([jour, date_galop, code_hippo]):
        return jsonify({"error": "Paramètres manquants"}), 400
    if not GITHUB_PAT:
        return jsonify({"error": "GITHUB_PAT non configuré"}), 503

    ok = trigger_github_parse(jour, date_galop, code_hippo, reunion, course_n)
    if ok:
        return jsonify({"status": "triggered"})
    return jsonify({"error": "Impossible de déclencher le workflow GitHub"}), 500


@app.route("/api/tracking/pdf")
@login_required
def api_tracking_pdf():
    date_g   = request.args.get("date")
    hippo    = request.args.get("hippo", "").upper()
    course_n = int(request.args.get("course", 1))
    cheval   = request.args.get("cheval", "")
    pdf = fetch_tracking_pdf(date_g, hippo, course_n)
    if not pdf:
        return jsonify({"found": False})
    if cheval:
        return jsonify({"found": True, "cheval": cheval, "tracking": parse_tracking_for_horse(pdf, cheval)})
    return jsonify({"found": True, "size_kb": len(pdf) // 1024})


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    print("\n🏇  BROSS&TRACK — démarrage")
    print(f"   → http://localhost:{port}\n")
    app.run(debug=False, host="0.0.0.0", port=port)
