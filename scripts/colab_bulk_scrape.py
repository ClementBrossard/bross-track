"""
BROSS&TRACK — Scraping massif (Google Colab)
================================================
Construit une base de données personnelle en parsant, jour par jour sur une
période donnée, le programme PMU + les PDF de tracking France Galop pour
TOUS les chevaux de TOUTES les courses de Plat trackées.

Usage dans Colab :
  1. Colle tout ce fichier dans une cellule.
  2. Modifie DATE_DEBUT / DATE_FIN (et éventuellement UTILISER_DRIVE).
  3. Exécute la cellule. Le script est reprenable : si l'exécution
     s'interrompt (déconnexion Colab), relance la même cellule —
     les jours déjà scrapés sont automatiquement ignorés.
  4. À tout moment (même en cours de route), tu peux appeler
     consolider_donnees(DOSSIER_SORTIE) pour régénérer le CSV/Parquet
     consolidé à partir de tout ce qui a déjà été scrapé.

Stratégie : on réutilise tel quel le code déjà testé en prod dans core.py
(get_courses_plat, fetch_tracking_pdf, parse_tracking_for_horse) plutôt que
de réinventer un parseur "tous chevaux" — le programme PMU du jour donne
déjà la liste exacte des noms de chevaux par course, donc pour chaque
course trackée on télécharge le PDF une fois et on appelle
parse_tracking_for_horse() pour chaque partant.
"""

import os
import sys
import json
import time
from datetime import datetime, timedelta

# ──────────────────────────────────────────────────────────────────────────
# ÉTAPE 1 — Dépendances + récupération du code métier (core.py)
# ──────────────────────────────────────────────────────────────────────────

os.system("pip install -q pdfplumber requests")

REPO_DIR = "/content/bross-track"
if not os.path.isdir(REPO_DIR):
    os.system(f"git clone --depth 1 https://github.com/ClementBrossard/bross-track.git {REPO_DIR}")
sys.path.insert(0, REPO_DIR)

import core  # noqa: E402
from core import get_courses_plat, fetch_tracking_pdf, parse_tracking_for_horse  # noqa: E402


# ──────────────────────────────────────────────────────────────────────────
# ÉTAPE 2 — Configuration (à modifier avant de lancer)
# ──────────────────────────────────────────────────────────────────────────

DATE_DEBUT = "2026-05-01"   # AAAA-MM-JJ, inclus
DATE_FIN   = "2026-05-31"   # AAAA-MM-JJ, inclus

UTILISER_DRIVE = True       # False = stockage local /content (perdu à la fin de session)
DOSSIER_SORTIE = "/content/drive/MyDrive/BrossTrack_Data" if UTILISER_DRIVE else "/content/BrossTrack_Data"

DELAI_ENTRE_COURSES = 0.4   # secondes — politesse envers les serveurs PMU / France Galop
FORCER_RESCRAPE     = False  # True = re-scrape même les jours déjà sauvegardés


# ──────────────────────────────────────────────────────────────────────────
# ÉTAPE 3 — Montage Drive (si activé)
# ──────────────────────────────────────────────────────────────────────────

if UTILISER_DRIVE:
    from google.colab import drive
    drive.mount("/content/drive")

os.makedirs(DOSSIER_SORTIE, exist_ok=True)
os.makedirs(os.path.join(DOSSIER_SORTIE, "jours"), exist_ok=True)


# ──────────────────────────────────────────────────────────────────────────
# ÉTAPE 4 — Scraping jour par jour
# ──────────────────────────────────────────────────────────────────────────

def _daterange(d1, d2):
    cur = d1
    while cur <= d2:
        yield cur
        cur += timedelta(days=1)


def _chemin_jour(jour) -> str:
    return os.path.join(DOSSIER_SORTIE, "jours", f"{jour.isoformat()}.json")


def scraper_jour(jour) -> list:
    """Scrape un jour donné. Retourne la liste des enregistrements (un par cheval/course)."""
    date_pmu   = jour.strftime("%d%m%Y")
    date_galop = jour.strftime("%Y%m%d")

    try:
        courses = get_courses_plat(date_pmu)
    except Exception as e:
        print(f"  ✗ erreur programme {date_pmu} : {e}")
        return []

    enregistrements = []

    for course in courses:
        if not course.get("course_trackee") or not course.get("code_hippo"):
            continue

        pdf = fetch_tracking_pdf(date_galop, course["code_hippo"], course["num_course"])
        time.sleep(DELAI_ENTRE_COURSES)
        if not pdf:
            continue

        for partant in course.get("partants", []):
            nom = partant.get("nom", "")
            if not nom:
                continue
            try:
                tracking = parse_tracking_for_horse(pdf, nom)
            except Exception:
                tracking = None

            enregistrements.append({
                "date":           jour.isoformat(),
                "hippodrome":     course["nom_hippo"],
                "code_hippo":     course["code_hippo"],
                "num_reunion":    course["num_reunion"],
                "num_course":     course["num_course"],
                "libelle_course": course["libelle"],
                "distance":       course["distance"],
                "surface":        course["surface"],
                "terrain":        course["terrain"],
                "num_partant":    partant["num"],
                "cheval":         nom,
                "jockey":         partant.get("jockey", ""),
                "tracking":       tracking,
            })

    return enregistrements


def lancer_scraping():
    d1 = datetime.strptime(DATE_DEBUT, "%Y-%m-%d").date()
    d2 = datetime.strptime(DATE_FIN, "%Y-%m-%d").date()

    for jour in _daterange(d1, d2):
        chemin = _chemin_jour(jour)
        if os.path.exists(chemin) and not FORCER_RESCRAPE:
            print(f"= {jour} déjà fait, skip")
            continue

        print(f"=== {jour} ===")
        enregistrements = scraper_jour(jour)
        with open(chemin, "w", encoding="utf-8") as f:
            json.dump(enregistrements, f, ensure_ascii=False, indent=2)
        print(f"  → {len(enregistrements)} chevaux sauvegardés")

        # Évite que le cache mémoire de core.py (PDF + JSON PMU) ne grossisse
        # sans limite sur un scraping de plusieurs mois.
        core._cache.clear()


lancer_scraping()


# ──────────────────────────────────────────────────────────────────────────
# ÉTAPE 5 — Consolidation en table plate (CSV + Parquet)
# ──────────────────────────────────────────────────────────────────────────
# Peut être ré-appelée seule à tout moment, même si le scraping est
# incomplet ou a été fait sur plusieurs sessions différentes.

def consolider_donnees(dossier_sortie: str = DOSSIER_SORTIE):
    import pandas as pd

    dossier_jours = os.path.join(dossier_sortie, "jours")
    lignes = []

    for nom_fichier in sorted(os.listdir(dossier_jours)):
        if not nom_fichier.endswith(".json"):
            continue
        with open(os.path.join(dossier_jours, nom_fichier), encoding="utf-8") as f:
            enregistrements = json.load(f)

        for e in enregistrements:
            t = e.get("tracking") or {}
            lignes.append({
                "date":            e["date"],
                "hippodrome":      e["hippodrome"],
                "code_hippo":      e["code_hippo"],
                "num_reunion":     e["num_reunion"],
                "num_course":      e["num_course"],
                "libelle_course":  e["libelle_course"],
                "distance":        e["distance"],
                "surface":         e["surface"],
                "terrain":         e["terrain"],
                "num_partant":     e["num_partant"],
                "cheval":          e["cheval"],
                "jockey":          e["jockey"],
                "has_tracking":          bool(t),
                "temps_officiel":        t.get("temps_officiel"),
                "temps_officiel_sec":    t.get("temps_officiel_sec"),
                "temps_600m_sec":        t.get("temps_600m_sec"),
                "vitesse_max_kmh":       t.get("vitesse_max_kmh"),
                "vitesse_moyenne_kmh":   t.get("vitesse_moyenne_kmh"),
                "redk_sec":              t.get("redk_sec"),
                "troncon_plus_rapide":       t.get("troncon_plus_rapide"),
                "troncon_plus_rapide_label": t.get("troncon_plus_rapide_label"),
                "remontee_places": t.get("remontee_places"),
                "score_remontee":  t.get("score_remontee"),
                "pct_top3":        t.get("pct_top3"),
                "profil_leader":   t.get("profil_leader"),
                "score_finish":    t.get("score_finish"),
                "regularite":      t.get("regularite"),
                "chute_finale":    t.get("chute_finale"),
                "positions_troncons": ",".join(str(p) for p in t.get("positions_troncons", [])),
                "vitesses_troncons":  ",".join(str(v) for v in t.get("vitesses_troncons", [])),
            })

    df = pd.DataFrame(lignes)
    chemin_csv     = os.path.join(dossier_sortie, "base_donnees.csv")
    chemin_parquet = os.path.join(dossier_sortie, "base_donnees.parquet")
    df.to_csv(chemin_csv, index=False)
    df.to_parquet(chemin_parquet, index=False)
    print(f"Consolidé : {len(df)} lignes → {chemin_csv}")
    return df


df = consolider_donnees()
df.head()
