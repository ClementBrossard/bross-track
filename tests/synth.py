"""Données synthétiques au format exact des CSV historiques (tracking_data,
troncons_data, chevaux_data, rapports...), pour tester le pipeline sans
réseau."""

import io
from datetime import date, timedelta

import numpy as np
import pandas as pd

from galoptrack.tables import COLS_CHEVAUX, COLS_COMBINES, COLS_RAPPORTS
from galoptrack.tracking_pdf import COLS_MAIN, COLS_TRONC

HIPPOS = [('CHA', 1), ('DEA', 2), ('LPA', 3), ('SAI', 4)]
TERRAINS = ['Bon', 'Bon souple', 'Souple', 'Très souple', 'Lourd', 'PSF STANDARD']
LABELS_1600 = ['DEP-1400m', '1400m-1200m', '1200m-1000m', '1000m-800m', '800m-600m',
               '600m-400m', '400m-200m', '200m-ARR']


def _roundtrip(df):
    return pd.read_csv(io.StringIO(df.to_csv(index=False)), low_memory=False)


def make_raw(n_days=150, seed=0, start=date(2025, 3, 1)):
    rng = np.random.default_rng(seed)
    n_horses = 220
    ability = rng.normal(0, 1, n_horses)
    names = [f"CHEVAL {i:03d}" for i in range(n_horses)]
    jockeys = [f"J. JOCKEY{i}" for i in range(25)]
    trainers = [f"E. ENTRAINEUR{i}" for i in range(15)]
    hist = {i: [] for i in range(n_horses)}
    ncourses = np.zeros(n_horses, dtype=int)
    nwins = np.zeros(n_horses, dtype=int)
    nplaces = np.zeros(n_horses, dtype=int)
    gains = np.zeros(n_horses)

    tr_rows, tc_rows, ch_rows, rap_rows, comb_rows = [], [], [], [], []
    for day_i in range(n_days):
        d = start + timedelta(days=day_i)
        dint = int(d.strftime('%Y%m%d'))
        for code, reunion in [HIPPOS[day_i % 4], HIPPOS[(day_i + 1) % 4]][: 1 + day_i % 2]:
            terrain = TERRAINS[rng.integers(len(TERRAINS))]
            for num_course in range(1, 4):
                field = rng.choice(n_horses, size=int(rng.integers(8, 13)), replace=False)
                perf = ability[field] + rng.normal(0, 0.8, len(field))
                order = field[np.argsort(-perf)]
                arrivee = {h: k + 1 for k, h in enumerate(order)}
                tracked = (num_course != 3)
                alloc = float(rng.choice([15000, 22000, 30000, 50000]))
                quinte = (num_course == 1 and reunion == 1)
                for npm, h in enumerate(field, start=1):
                    pa = arrivee[h]
                    mus = ''.join(f"{p}p" for p in hist[h][-6:][::-1]) or None
                    ch_rows.append({
                        'date': dint, 'code_hippo': code, 'num_reunion': reunion, 'num_course': num_course,
                        'nom_cheval': names[h], 'id_cheval': f"ID{h}",
                        'jockey': jockeys[(h + day_i) % len(jockeys)], 'entraineur': trainers[h % len(trainers)],
                        'proprietaire': 'P', 'eleveur': 'E', 'age': 3 + h % 5, 'sexe': ['MALES', 'FEMELLES', 'HONGRES'][h % 3],
                        'numero_partant': npm, 'corde': npm, 'poids_kg': 55 + h % 5, 'musique': mus,
                        'oeilleres': ['SANS_OEILLERES', 'OEILLERES_AUSTRALIENNES'][h % 2],
                        'nom_pere': 'PERE', 'nom_mere': 'MERE',
                        'nb_courses_carriere': ncourses[h], 'nb_victoires_carriere': nwins[h],
                        'nb_places_carriere': nplaces[h], 'gains_carriere': gains[h],
                        'handicap_valeur': round(30 + 5 * ability[h] + rng.normal(0, 1), 1),
                        'categorie_particularite': 'HANDICAP' if num_course == 2 else 'COURSE_A_CONDITIONS',
                        'allocation_totale': alloc, 'allocation_1er': alloc / 2,
                        'condition_age': 'TROIS_ANS_ET_PLUS', 'condition_sexe': 'TOUS_CHEVAUX',
                        'distance_course': 1600, 'type_piste': 'PSF' if terrain.startswith('PSF') else 'HERBE',
                        'duree_course_ms': 98000, 'penetrometre': terrain,
                        'ordre_arrivee': pa, 'ecart_precedent': '1 L' if pa > 1 else '',
                        'commentaire_course': '',
                    })
                    if tracked:
                        base_speed = 58 + ability[h] * 0.8 + rng.normal(0, 0.5)
                        cumul, tr_speeds, positions = 0.0, [], []
                        for k, lbl in enumerate(LABELS_1600):
                            v = base_speed + (2.0 * (k >= 5)) + rng.normal(0, 0.6)
                            t = 200 / (v / 3.6)
                            cumul += t
                            posk = int(np.clip(round(pa + (len(LABELS_1600) - k) * rng.normal(0, 0.6)), 1, len(field)))
                            tc_rows.append({
                                'date': dint, 'code_hippo': code, 'num_reunion': reunion, 'num_course': num_course,
                                'nom_cheval': names[h], 'troncon_index': k + 1, 'troncon_label': lbl,
                                'temps': '', 'temps_sec': round(t, 2), 'cumul_sec': round(cumul, 2),
                                'vitesse_kmh': round(v, 1), 'position': posk, 'foulees': 30,
                            })
                            tr_speeds.append(round(v, 1))
                            positions.append(posk)
                        tr_rows.append({
                            'date': dint, 'code_hippo': code, 'num_reunion': reunion, 'num_course': num_course,
                            'nom_cheval': names[h], 'position_arrivee': pa,
                            'temps_officiel': '', 'temps_officiel_sec': round(cumul, 2),
                            'temps_600m': '', 'temps_600m_sec': 30.0, 'temps_200m_final': '',
                            'temps_200m_final_sec': 11.5, 'redk': '', 'redk_sec': 60.0,
                            'redk_premier': '', 'redk_premier_sec': 59.0,
                            'vitesse_max_kmh': max(tr_speeds), 'vitesse_moyenne_kmh': round(np.mean(tr_speeds), 1),
                            'distance_m': 1600, 'terrain': terrain, 'penetrometre': '3,1',
                            'type_piste': 'PSF' if terrain.startswith('PSF') else 'HERBE',
                            'troncon_plus_rapide': '', 'troncon_plus_rapide_label': '',
                            'dep_label': 'DEP-1400m', 'dep_temps_sec': 13.0, 'dep_dist_m': 200,
                            'dep_temps_400_eq': 26.0, 'positions_troncons': str(positions),
                            'vitesses_troncons': str(tr_speeds), 'nb_troncons': len(LABELS_1600),
                        })
                    # rapports (numéros PMU = ordre dans le champ)
                    if pa == 1:
                        rap_rows.append({'date': dint, 'code_hippo': code, 'num_reunion': reunion,
                                         'num_course': num_course, 'nom_cheval': names[h], 'num_pmu': npm,
                                         'rapport_gagnant': 450, 'nb_gagnants_gagnant': 10,
                                         'rapport_place': 180, 'nb_gagnants_place': 30})
                    elif pa <= 3:
                        rap_rows.append({'date': dint, 'code_hippo': code, 'num_reunion': reunion,
                                         'num_course': num_course, 'nom_cheval': names[h], 'num_pmu': npm,
                                         'rapport_gagnant': None, 'nb_gagnants_gagnant': None,
                                         'rapport_place': 250, 'nb_gagnants_place': 20})
                    hist[h].append(min(pa, 9))
                    ncourses[h] += 1
                    nwins[h] += int(pa == 1)
                    nplaces[h] += int(pa <= 3)
                    gains[h] += alloc / pa if pa <= 5 else 0
                npm_of = {h: k for k, h in enumerate(field, start=1)}
                top3 = [npm_of[h] for h in order[:3]]
                for a, b in [(top3[0], top3[1]), (top3[0], top3[2]), (top3[1], top3[2])]:
                    comb_rows.append({'date': dint, 'code_hippo': code, 'num_reunion': reunion,
                                      'num_course': num_course, 'type_pari': 'COUPLE_PLACE',
                                      'libelle': 'Couplé Placé', 'combinaison': f"{a}-{b}",
                                      'dividende_pour_un_euro': 320, 'dividende_pour_mise_base': 320,
                                      'mise_base': 100, 'nombre_gagnants': 5})
                if quinte:
                    comb_rows.append({'date': dint, 'code_hippo': code, 'num_reunion': reunion,
                                      'num_course': num_course, 'type_pari': 'QUINTE_PLUS',
                                      'libelle': 'Désordre',
                                      'combinaison': '-'.join(str(npm_of[h]) for h in order[:5]),
                                      'dividende_pour_un_euro': 5000, 'dividende_pour_mise_base': 10000,
                                      'mise_base': 200, 'nombre_gagnants': 3})

    return {
        'tracking': _roundtrip(pd.DataFrame(tr_rows, columns=COLS_MAIN)),
        'troncons': _roundtrip(pd.DataFrame(tc_rows, columns=COLS_TRONC)),
        'chevaux': _roundtrip(pd.DataFrame(ch_rows, columns=[c for c in COLS_CHEVAUX if c != 'cote_directe'])),
        'rapports': _roundtrip(pd.DataFrame(rap_rows, columns=COLS_RAPPORTS)),
        'rapports_combines': _roundtrip(pd.DataFrame(comb_rows, columns=COLS_COMBINES)),
    }
