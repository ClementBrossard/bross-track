"""
GalopTrack — site web (Render)

Sert, derrière un mot de passe, les pages générées chaque jour par le
pipeline (GitHub Actions) et stockées sur Cloudflare R2 :
  /          -> /dashboard
  /dashboard    dashboard GalopTrack (dashboards/latest.html)
  /sante        contrôle de santé des données (dashboards/sante.html)
  /api/cotes/<AAAAMMJJ>/<réunion>/<course>
                cotes PMU en direct d'une course (JSON, mis en cache 20 s)

Variables d'environnement : APP_PASSWORD, SECRET_KEY, R2_ACCOUNT_ID,
R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY, R2_BUCKET.
"""

import gzip
import hmac
import json
import os
import time
import urllib.request
from functools import wraps

from flask import Flask, jsonify, redirect, request, session

app = Flask(__name__)
app.secret_key = os.environ.get("SECRET_KEY") or os.urandom(32)
if not os.environ.get("SECRET_KEY"):
    app.logger.warning("SECRET_KEY non défini : les sessions ne tiendront pas entre deux workers")
PASSWORD = os.environ.get("APP_PASSWORD", "")
if not PASSWORD:
    app.logger.warning("APP_PASSWORD non défini : connexion impossible tant qu'il n'est pas configuré")

REFRESH_S = 300
_page_caches = {}


def login_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if not session.get("authenticated"):
            return redirect("/login")
        return f(*args, **kwargs)
    return decorated


def _load_page(key):
    """Relit une page générée sur R2 si elle a changé (vérifié au plus toutes
    les 5 minutes). Seule la version compressée est gardée en mémoire (le
    dashboard pèse plusieurs dizaines de Mo, l'offre gratuite de Render
    n'a que 512 Mo). Retourne les octets gzip, ou None."""
    now = time.time()
    c = _page_caches.setdefault(key, {"gz": None, "checked": 0.0, "etag": None})
    if c["gz"] is not None and now - c["checked"] < REFRESH_S:
        return c["gz"]
    c["checked"] = now
    try:
        from galoptrack.storage import get_storage
        st = get_storage()
        if hasattr(st, "s3"):
            head = st.s3.head_object(Bucket=st.bucket, Key=key)
            if head["ETag"] == c["etag"] and c["gz"] is not None:
                return c["gz"]
            c["etag"] = head["ETag"]
        elif not st.exists(key):
            return None
        data = st.read_bytes(key)
        c["gz"] = gzip.compress(data, compresslevel=6)
        del data
    except Exception as e:  # stockage non configuré / fichier absent
        app.logger.warning("Page %s indisponible : %s", key, e)
    return c["gz"]


def _serve_generated(key, missing_msg):
    gz = _load_page(key)
    if gz is None:
        return (missing_msg, 503)
    if "gzip" in request.headers.get("Accept-Encoding", ""):
        return app.response_class(gz, mimetype="text/html",
                                  headers={"Content-Encoding": "gzip", "Cache-Control": "private, max-age=300"})
    return app.response_class(gzip.decompress(gz), mimetype="text/html")


LOGIN_PAGE = """<!DOCTYPE html>
<html lang="fr"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>GalopTrack</title>
<style>
:root{--bg:#f6f3ea;--paper:#fffdf8;--ink:#221f1a;--soft:#6b6357;--line:#ddd6c4;--green:#1f3d2e;--red:#a8402c}
@media (prefers-color-scheme: dark){:root{--bg:#16140f;--paper:#211e18;--ink:#eee8da;--soft:#a79e8e;
--line:#3a352b;--green:#9fc9ad;--red:#f08c78}}
*{box-sizing:border-box;margin:0;padding:0}
body{background:var(--bg);color:var(--ink);min-height:100vh;display:flex;align-items:center;
justify-content:center;font:14px/1.45 Inter,system-ui,sans-serif;padding:16px}
.box{background:var(--paper);border:1px solid var(--line);border-top:3px solid var(--green);border-radius:8px;
padding:40px 32px;width:100%;max-width:360px;text-align:center}
.logo{font-weight:700;font-size:24px;color:var(--green);margin-bottom:4px}
.sub{font-size:12px;color:var(--soft);margin-bottom:28px}
input{width:100%;background:var(--bg);border:1px solid var(--line);border-radius:6px;color:var(--ink);
padding:12px;font-size:15px;text-align:center;margin-bottom:12px}
input:focus{outline:2px solid var(--green);border-color:transparent}
button{width:100%;background:var(--green);color:var(--paper);border:0;border-radius:6px;padding:12px;
font-weight:600;font-size:14px;cursor:pointer}
.error{color:var(--red);font-size:13px;margin-top:12px}
</style></head><body>
<div class="box">
  <div class="logo">GalopTrack</div>
  <div class="sub">Tracking · Modèle · Courses du jour</div>
  <form method="POST" action="/login">
    <input type="password" name="password" placeholder="Mot de passe" autofocus>
    <button type="submit">Accéder</button>
    {error}
  </form>
</div>
</body></html>"""


@app.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        given = request.form.get("password", "")
        if PASSWORD and hmac.compare_digest(given, PASSWORD):
            session["authenticated"] = True
            session.permanent = True
            return redirect("/dashboard")
        return LOGIN_PAGE.replace("{error}", '<div class="error">Mot de passe incorrect.</div>')
    return LOGIN_PAGE.replace("{error}", "")


@app.route("/logout")
def logout():
    session.clear()
    return redirect("/login")


@app.route("/")
@login_required
def index():
    return redirect("/dashboard")


@app.route("/dashboard")
@login_required
def dashboard_page():
    return _serve_generated("dashboards/latest.html",
                            "Dashboard pas encore généré (voir le workflow GitHub « Pipeline — quotidien »).")


@app.route("/sante")
@login_required
def sante_page():
    return _serve_generated("dashboards/sante.html",
                            "Contrôle de santé pas encore généré (voir le workflow GitHub « Pipeline — quotidien »).")


PMU_LIVE = "https://online.turfinfo.api.pmu.fr/rest/client/61/programme"
LIVE_CACHE_S = 20
_live_cache = {}


def _pmu_json(url):
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0 GalopTrack", "Accept": "application/json"})
    with urllib.request.urlopen(req, timeout=8) as r:
        return json.loads(r.read().decode("utf-8"))


def _rapport(p, field):
    r = p.get(field) or {}
    if r.get("typePari") != "SIMPLE_GAGNANT":
        return None
    try:
        return float(r.get("rapport")) or None
    except (TypeError, ValueError):
        return None


def live_odds(day, reunion, course):
    """Cotes simple gagnant du moment pour une course (API PMU en ligne)."""
    ddmmyyyy = f"{day[6:8]}{day[4:6]}{day[0:4]}"
    base = f"{PMU_LIVE}/{ddmmyyyy}/R{reunion}/C{course}"
    data = _pmu_json(f"{base}/participants")
    partants = []
    for p in data.get("participants") or []:
        if not p.get("nom"):
            continue
        direct = p.get("dernierRapportDirect") or {}
        partants.append({
            "num": p.get("numPmu"),
            "nom": p["nom"].strip().upper(),
            "cote": _rapport(p, "dernierRapportDirect"),
            "cote_ref": _rapport(p, "dernierRapportReference"),
            "heure_cote": direct.get("dateRapport"),
            "non_partant": p.get("statut") == "NON_PARTANT",
        })
    out = {"partants": partants, "lu_a": int(time.time() * 1000)}
    try:  # heure de départ : facultative
        info = _pmu_json(base)
        out["depart"] = info.get("heureDepart")
        out["statut"] = info.get("statut")
    except Exception:
        pass
    return out


@app.route("/api/cotes/<day>/<int:reunion>/<int:course>")
def api_cotes(day, reunion, course):
    if not session.get("authenticated"):
        return jsonify({"erreur": "non connecté"}), 401
    if not (len(day) == 8 and day.isdigit()):
        return jsonify({"erreur": "date invalide"}), 400
    key = (day, reunion, course)
    hit = _live_cache.get(key)
    if hit and time.time() - hit[0] < LIVE_CACHE_S:
        return jsonify(hit[1])
    try:
        out = live_odds(day, reunion, course)
    except Exception as e:
        app.logger.warning("Cotes en direct %s indisponibles : %s", key, e)
        return jsonify({"erreur": "API PMU indisponible"}), 502
    _live_cache[key] = (time.time(), out)
    if len(_live_cache) > 200:
        for k in sorted(_live_cache, key=lambda k: _live_cache[k][0])[:100]:
            _live_cache.pop(k, None)
    return jsonify(out)


@app.route("/healthz")
def healthz():
    return "ok"


if __name__ == "__main__":
    app.run(debug=True, port=5000)
