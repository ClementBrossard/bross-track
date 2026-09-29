"""Tables de données (équivalents des CSV du Drive), stockées en CSV gzip.

On garde le format CSV (compressé) plutôt que Parquet : relues avec
pd.read_csv, les colonnes ont exactement les mêmes types qu'avec les
notebooks, ce qui garantit des features identiques.

Écriture en « upsert par course » : quand on re-collecte une course, toutes
ses lignes sont remplacées. C'est ce qui permet de collecter une course avant
le départ puis de la compléter avec l'arrivée le lendemain, sans doublon
(bug du notebook enrichissement, qui ré-ajoutait tout à chaque relance)."""

import gzip
import io

import pandas as pd

from .tracking_pdf import COLS_MAIN, COLS_TRONC

COLS_CHEVAUX = [
    'date', 'code_hippo', 'num_reunion', 'num_course', 'nom_cheval',
    'id_cheval',
    'jockey', 'entraineur', 'proprietaire', 'eleveur',
    'age', 'sexe', 'numero_partant', 'corde', 'poids_kg', 'musique', 'oeilleres',
    'nom_pere', 'nom_mere',
    'nb_courses_carriere', 'nb_victoires_carriere', 'nb_places_carriere',
    'gains_carriere', 'handicap_valeur',
    'categorie_particularite',
    'allocation_totale', 'allocation_1er',
    'condition_age', 'condition_sexe',
    'distance_course', 'type_piste', 'duree_course_ms', 'penetrometre',
    'ordre_arrivee', 'ecart_precedent', 'commentaire_course',
    # anciennement cotes_data.csv (même appel API /participants)
    'cote_directe',
    # PLAT / HAIES / STEEPLE / CROSS (vide dans l'historique = plat)
    'discipline',
]

COLS_RAPPORTS = [
    'date', 'code_hippo', 'num_reunion', 'num_course', 'nom_cheval', 'num_pmu',
    'rapport_gagnant', 'nb_gagnants_gagnant',
    'rapport_place', 'nb_gagnants_place',
]

COLS_COMBINES = [
    'date', 'code_hippo', 'num_reunion', 'num_course',
    'type_pari', 'libelle', 'combinaison',
    'dividende_pour_un_euro', 'dividende_pour_mise_base', 'mise_base', 'nombre_gagnants',
]

SCHEMAS = {
    'tracking': COLS_MAIN + ['discipline'],
    'troncons': COLS_TRONC,
    'chevaux': COLS_CHEVAUX,
    'rapports': COLS_RAPPORTS,
    'rapports_combines': COLS_COMBINES,
}

RACE_KEY = ['date', 'code_hippo', 'num_reunion', 'num_course']


def table_key(name):
    return f"data/{name}.csv.gz"


def normalize(df):
    """Aller-retour CSV : donne aux colonnes les mêmes types que pd.read_csv
    sur les fichiers historiques."""
    buf = df.to_csv(index=False)
    return pd.read_csv(io.StringIO(buf), low_memory=False)


def to_csv_gz(df):
    return gzip.compress(df.to_csv(index=False).encode('utf-8'), compresslevel=6)


def read_table(storage, name):
    key = table_key(name)
    if not storage.exists(key):
        return pd.DataFrame(columns=SCHEMAS[name])
    return pd.read_csv(io.BytesIO(storage.read_bytes(key)), compression='gzip', low_memory=False)


def write_table(storage, name, df):
    storage.write_bytes(table_key(name), to_csv_gz(df), content_type='application/gzip')


def race_keys(df):
    """Tuples (date, code_hippo, num_reunion, num_course) normalisés."""
    if df.empty:
        return pd.Series([], dtype=object)
    date = pd.to_numeric(df['date'], errors='coerce').fillna(-1).astype('int64')
    reunion = pd.to_numeric(df['num_reunion'], errors='coerce').fillna(0).astype('int64')
    course = pd.to_numeric(df['num_course'], errors='coerce').fillna(-1).astype('int64')
    hippo = df['code_hippo'].astype(str)
    return pd.Series(list(zip(date, hippo, reunion, course)), index=df.index)


def upsert_races(storage, name, rows, existing=None):
    """Remplace toutes les lignes des courses présentes dans `rows`.
    Les courses sans nouvelle ligne ne sont jamais touchées."""
    if not rows:
        return existing if existing is not None else read_table(storage, name), 0
    cols = SCHEMAS[name]
    new = normalize(pd.DataFrame(rows, columns=cols))
    if existing is None:
        existing = read_table(storage, name)
    if len(existing):
        keep = ~race_keys(existing).isin(set(race_keys(new)))
        existing = existing[keep]
    # Colonnes manquantes dans un ancien fichier (ex: cote_directe)
    for c in cols:
        if c not in existing.columns:
            existing[c] = pd.NA
    merged = pd.concat([existing[cols], new[cols]], ignore_index=True) if len(existing) else new[cols]
    merged = normalize(merged)
    merged = merged.sort_values('date', kind='stable').reset_index(drop=True)
    write_table(storage, name, merged)
    return merged, len(new)
