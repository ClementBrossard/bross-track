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
| `cotes/live/AAAAMMJJ.json.gz` | cotes de la journée relevées toutes les 5 à 10 min (courbes du site) |

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

### 5. Déclenchement à l'heure pile (cron-job.org)
GitHub lance les tâches planifiées avec des heures de retard le matin (voire
les saute). Le vrai déclencheur est donc externe : cron-job.org appelle l'API
GitHub à l'heure dite (fuseau Europe/Paris, donc pas de décalage été/hiver).

1. GitHub → *Settings* → *Developer settings* → *Fine-grained tokens* → jeton
   limité au repo `bross-track`, permission **Actions : Read and write** seule.
2. cron-job.org, deux tâches `POST https://api.github.com/repos/ClementBrossard/bross-track/actions/workflows/pipeline-daily.yml/dispatches`
   avec les en-têtes `Accept: application/vnd.github+json`,
   `Authorization: Bearer <jeton>`, `X-GitHub-Api-Version: 2022-11-28` :
   - 07:40 → corps `{"ref":"main","inputs":{"mode":"daily"}}`
   - 12:40 → corps `{"ref":"main","inputs":{"mode":"dashboard"}}`
   Réponse attendue : `204`.

Les crons GitHub restent en secours : les créneaux du matin passent en
`--if-needed` et ne font rien si la collecte du jour a déjà tourné.

### 6. Relevé des cotes toutes les 5 minutes (cron-job.org → GitHub)
Le workflow **Pipeline — relevé des cotes** relève les cotes de toutes les
courses de galop du jour toutes les 5 minutes, à l'heure pile, en boucle (au
plus ~6 h par lancement, limite de GitHub). Il est lancé par cron-job.org, avec
le même jeton et les mêmes en-têtes que la tâche du quotidien (§5) :
`POST https://api.github.com/repos/ClementBrossard/bross-track/actions/workflows/pipeline-releve-cotes.yml/dispatches`

| Heure (Paris) | Corps |
|---|---|
| 08:00 | `{"ref":"main","inputs":{"jusqua":"13:50"}}` |
| 13:50 | `{"ref":"main","inputs":{"jusqua":"19:40"}}` |
| 19:40 | `{"ref":"main","inputs":{"jusqua":"21:55"}}` |

Réponse attendue : `204`. Les relevés sont dans `cotes/live/AAAAMMJJ.json.gz` ;
le site les lit pour les courbes.

(Ancienne méthode, abandonnée : cron-job.org appelait directement
`/api/releve-cotes` sur Render, qui a fini par bloquer ces appels avec des
erreurs 429. La route existe toujours, protégée par `RELEVE_CLE`.)

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

## Sauvegardes et retour en arrière
- Chaque rattrapage sauvegarde d'abord les tables dans `backups/<date-heure>-avant-rattrapage/` (R2).
- **Pipeline — sauvegarde / restauration** : `lister`, `sauvegarder`, ou `restaurer` une sauvegarde
  (nom affiché par `lister`) ; la restauration refait d'abord une sauvegarde, puis régénère le dashboard.
- Le code, lui, est historisé par git : le bouton *Revert* d'une PR fusionnée crée une PR qui l'annule.
  Revenir à un code sans obstacles impose aussi de restaurer des tables sans obstacles.

## Modèle Profil (à part, tracking seul)

`galoptrack/profil.py` : un modèle séparé de la production, construit
uniquement sur les tronçons — chiffre de vitesse corrigé du parcours et du
terrain du jour, temps perdu sur les gênes, style, finish, rythme de course,
profil de chaque cheval et scénario de la course du jour.

Évaluation (lecture seule, rien n'est écrit dans R2) : *Actions* →
**Pipeline — expériences modèle** → *Run workflow* → expérience `profil`.
Le rapport (dans le résumé du run) compare, sur 4 périodes de test, le modèle
Profil seul, la v9 en production et la v9 + Profil, puis regarde ce que vaut
le n°1 du modèle Profil selon son rang dans les cotes (vrais rapports PMU).
