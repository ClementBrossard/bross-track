# GalopTrack — pipeline automatisé

Les notebooks Colab (scraper, enrichissement, cotes, rapports, train_model,
dashboard_generator) sont remplacés par le package `galoptrack/`, qui tourne
tout seul sur GitHub Actions. Les données vivent sur Cloudflare R2, le
dashboard est servi par l'app Render (`/dashboard`, derrière le login).

```
GitHub Actions ──(lit/écrit)──> Cloudflare R2 <──(lit)── App Render /dashboard
  quotidien 07:40 : collecte J-3..J-1 + dashboard du jour
  quotidien 12:40 : dashboard seul (cotes à jour)
  mensuel (le 1er) : réentraînement du modèle + dashboard
```

## Contenu du stockage R2

| Chemin | Contenu |
|---|---|
| `data/tracking.csv.gz` | ex-`tracking_data.csv` |
| `data/troncons.csv.gz` | ex-`troncons_data.csv` |
| `data/chevaux.csv.gz` | ex-`chevaux_data.csv` **+ colonne `cote_directe`** (ex-`cotes_data.csv`) |
| `data/rapports.csv.gz`, `data/rapports_combines.csv.gz` | rapports définitifs |
| `model/current.json` | version du modèle en production |
| `model/vN/` | `race_model.txt`, `calibration.json`, `backtest.json`, `meta.json` |
| `dashboards/latest.html` | dernier dashboard généré |
| `logs/` | résumé de chaque collecte, rapport de migration |

## Mise en place (une seule fois)

### 1. Cloudflare R2
1. Créer un compte sur <https://dash.cloudflare.com> → **R2 Object Storage** (carte bancaire demandée, offre gratuite jusqu'à 10 Go).
2. **Create bucket** → nom : `galoptrack`.
3. **Manage R2 API Tokens** → **Create API token** → permission *Object Read & Write*, limitée au bucket `galoptrack`.
4. Noter : *Account ID*, *Access Key ID*, *Secret Access Key*.

### 2. Secrets GitHub
Repo → *Settings* → *Secrets and variables* → *Actions* → *New repository secret* :
`R2_ACCOUNT_ID`, `R2_ACCESS_KEY_ID`, `R2_SECRET_ACCESS_KEY`, `R2_BUCKET` (= `galoptrack`).

### 3. Variables d'environnement Render
Service web → *Environment* : les 4 mêmes variables R2, plus
`APP_PASSWORD` (nouveau mot de passe) et `SECRET_KEY` (longue chaîne aléatoire).

### 4. Migration de l'historique
1. Ouvrir `notebooks/migration_drive_vers_r2.ipynb` dans Colab, ajouter les 4 secrets R2 (icône 🔑), exécuter. Le Drive n'est pas modifié.
2. GitHub → *Actions* → **Pipeline — migration de l'historique Drive** → *Run workflow*. Le rapport avant/après s'affiche dans le résumé du run.
3. **Pipeline — rattrapage d'une période** :
   - du lendemain de ton dernier run Colab (ex. `2026-09-11`) à hier, sources par défaut ;
   - puis cotes manquantes : `2026-06-21` → `2026-09-10`, sources `participants`.
4. **Pipeline — quotidien** → *Run workflow* : génère le premier dashboard.

À partir de là tout est automatique. Les workflows planifiés ne s'exécutent
que depuis la branche par défaut du repo (`main`) : il faut donc que ce code y
soit fusionné.

## Commandes (aussi utilisables en local)

```bash
pip install -r requirements-pipeline.txt
python -m galoptrack daily                         # collecte J-3..J-1 + dashboard
python -m galoptrack collect --from 2026-09-11 --to 2026-09-26
python -m galoptrack dashboard [--no-today]
python -m galoptrack train [--no-promote]
python -m galoptrack status
```
Sans variables R2, tout est écrit dans `./galoptrack_data/` (mode local).

## Garanties

- **Features identiques au notebook** : `tests/test_pipeline.py` compare, sur
  des données synthétiques, les 37 features au code exact de la cellule 6 du
  notebook d'entraînement.
- **Pas de doublons** : chaque table est écrite « par course » ; re-collecter
  une journée remplace ses lignes (une course collectée avant le départ est
  complétée par son arrivée le lendemain).
- **Pas de fuite** : une course du jour déjà présente dans l'historique n'est
  jamais re-scorée, et les courses du jour passent toujours après l'historique
  dans les calculs cumulés.
- **Garde-fou modèle** : une nouvelle version n'est mise en production que si
  son taux de réussite top-1 en validation ≥ 20 % (`GALOPTRACK_MIN_HIT_RATE`).
  Pour revenir à une version : modifier `model/current.json`.
- **Test réel** : le workflow *Pipeline — test réel PMU / France Galop* collecte
  une vraie journée à chaque modification du code.
