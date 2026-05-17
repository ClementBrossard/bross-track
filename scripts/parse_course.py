"""
Script de parsing d'une course — exécuté par GitHub Actions.
Produit un fichier JSON avec le tracking de tous les chevaux.
"""

import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core import (
    fetch_perfs,
    get_horse_tracking_history,
    count_with_tracking,
)

date_pmu   = os.environ["DATE_PMU"]
date_galop = os.environ["DATE_GALOP"]
code_hippo = os.environ["CODE_HIPPO"]
num_reunion = int(os.environ["NUM_REUNION"])
num_course  = int(os.environ["NUM_COURSE"])
output_file = os.environ.get("OUTPUT_FILE", "output.json")

print(f"Parsing {code_hippo} C{num_course:02d} — {date_galop}")

perfs = fetch_perfs(date_pmu, num_reunion, num_course)
if not perfs:
    print("Aucune performance trouvée, abandon.")
    sys.exit(1)

result = {
    "date_galop":  date_galop,
    "date_pmu":    date_pmu,
    "code_hippo":  code_hippo,
    "num_reunion": num_reunion,
    "num_course":  num_course,
    "horses":      {},
}

for p in perfs:
    nom = p.get("nomCheval", "")
    if not nom:
        continue
    print(f"  → {nom}...")
    historique = get_horse_tracking_history(p.get("coursesCourues", []), nom, max_courses=6)
    result["horses"][nom] = {
        "historique":          historique,
        "nb_courses_trackers": count_with_tracking(historique),
    }

with open(output_file, "w", encoding="utf-8") as f:
    json.dump(result, f, ensure_ascii=False, indent=2)

print(f"Done — {len(result['horses'])} chevaux parsés → {output_file}")
