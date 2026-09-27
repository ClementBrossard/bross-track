"""Construction de `races_list` à partir des tables tracking / tronçons /
chevaux — cellules 3, 4, 5 et 5b du générateur de dashboard, reprises à
l'identique. Utilisée à la fois par l'entraînement et par le dashboard, pour
que les deux travaillent exactement sur les mêmes courses."""

import numpy as np
import pandas as pd

KEY = ['date', 'code_hippo', 'num_course', 'nom_cheval']


def safe_num(v, nd=None):
    if pd.isna(v):
        return None
    v = float(v)
    if nd is not None:
        v = round(v, nd)
        if v == int(v):
            return int(v)
    return v


def safe_int(v):
    if pd.isna(v):
        return None
    try:
        return int(v)
    except Exception:
        return None


def safe_str(v):
    if pd.isna(v):
        return None
    s = str(v).strip()
    return s if s else None


def norm_pa(v):
    """Place à l'arrivée en texte : '1' et non '1.0' (le dashboard repère le
    vainqueur par pa === '1'). Les valeurs non numériques sont gardées."""
    if v is None or (not isinstance(v, str) and pd.isna(v)):
        return None
    try:
        f = float(v)
        if f == int(f):
            return str(int(f))
    except (TypeError, ValueError):
        pass
    s = str(v).strip()
    return s if s else None


def clean_tables(tr, tc, ch):
    """Cellule 3 : conversion des clés, retrait des lignes corrompues,
    dédoublonnage cheval x course."""
    tr, tc, ch = tr.copy(), tc.copy(), ch.copy()
    for d in (tr, tc, ch):
        d['date'] = pd.to_numeric(d['date'], errors='coerce')
        d['num_course'] = pd.to_numeric(d['num_course'], errors='coerce')

    tr = tr[tr['code_hippo'].astype(str).str.match(r'^[A-Z\-]+$', na=False)].copy()
    tr = tr.dropna(subset=['date', 'num_course'])
    tr['date'] = tr['date'].astype('int64')
    tr['num_course'] = tr['num_course'].astype('int64')

    tc = tc.dropna(subset=['date', 'num_course'])
    tc['date'] = tc['date'].astype('int64')
    tc['num_course'] = tc['num_course'].astype('int64')

    ch = ch.dropna(subset=['date', 'num_course'])
    ch['date'] = ch['date'].astype('int64')
    ch['num_course'] = ch['num_course'].astype('int64')

    tr = tr.drop_duplicates(subset=KEY, keep='first')
    ch = ch.drop_duplicates(subset=KEY, keep='first')
    return tr, tc, ch


def build_races(tr, tc, ch):
    """Retourne (races_list, labels_array, ch_nettoyee)."""
    tr, tc, ch = clean_tables(tr, tc, ch)

    # Cellule 4 — tronçons par cheval x course
    tc_sorted = tc.sort_values(KEY + ['troncon_index'])
    troncons_by_horse = {}
    for k, grp in tc_sorted.groupby(KEY):
        troncons_by_horse[k] = grp[['temps_sec', 'cumul_sec', 'vitesse_kmh', 'position', 'foulees',
                                    'troncon_label']].values.tolist()

    # Cellule 5 — fusion tracking + chevaux
    m = tr.merge(ch, on=KEY, how='left', suffixes=('', '_ch'))

    label_dict = {}

    def lbl_idx(lbl):
        if lbl is None or (isinstance(lbl, float) and pd.isna(lbl)):
            return -1
        if lbl not in label_dict:
            label_dict[lbl] = len(label_dict)
        return label_dict[lbl]

    races = {}
    for _, r in m.iterrows():
        nr = safe_int(r.get('num_reunion')) or 0
        race_key = f"{int(r['date'])}_{r['code_hippo']}_R{nr}_{int(r['num_course'])}"
        if race_key not in races:
            races[race_key] = {
                'id': race_key,
                'date': int(r['date']),
                'hippo': r['code_hippo'],
                'rnum': nr,
                'num': int(r['num_course']),
                'dist': safe_int(r['distance_m']),
                'terrain': safe_str(r['terrain']),
                'penetro': safe_str(r['penetrometre']),
                'piste': safe_str(r['type_piste']),
                'cat': safe_str(r.get('categorie_particularite')),
                'alloc': safe_num(r.get('allocation_totale')),
                'cond_age': safe_str(r.get('condition_age')),
                'horses': []
            }

        hk = (int(r['date']), r['code_hippo'], int(r['num_course']), r['nom_cheval'])
        tron_arr = []
        for t in troncons_by_horse.get(hk, []):
            time_s, cum_s, v_s, pos_s, foul_s, lbl_s = t
            tron_arr.append([
                safe_num(time_s, 2), safe_num(cum_s, 2), safe_num(v_s, 1),
                safe_int(pos_s), safe_int(foul_s), lbl_idx(lbl_s)
            ])

        vits = [t[2] for t in tron_arr if t[2] is not None]
        poss = [t[3] for t in tron_arr if t[3] is not None]

        # v4p : vitesse moyenne sur les 2 premiers tronçons (distance réelle / temps)
        feat_400_premier = None
        if len(tron_arr) >= 2:
            v1, t1 = tron_arr[0][2], tron_arr[0][0]
            v2, t2 = tron_arr[1][2], tron_arr[1][0]
            if v1 is not None and v2 is not None and t1 is not None and t2 is not None and (t1 + t2) > 0:
                d1 = v1 / 3.6 * t1
                d2 = v2 / 3.6 * t2
                feat_400_premier = round((d1 + d2) / (t1 + t2) * 3.6, 1)

        feat_200_premier = tron_arr[0][0] if tron_arr and tron_arr[0][0] is not None else None

        ratio_early_late = None
        if len(vits) >= 4:
            early = np.mean(vits[:2])
            late = np.mean(vits[-2:])
            if early > 0:
                ratio_early_late = round(float(late / early), 3)

        delta_pos = (poss[0] - poss[-1]) if len(poss) >= 2 else None
        regularite = round(float(np.std(vits)), 2) if len(vits) >= 2 else None

        races[race_key]['horses'].append({
            'nom': r['nom_cheval'],
            'pa': norm_pa(r['position_arrivee']),
            'jk': safe_str(r.get('jockey')),
            'en': safe_str(r.get('entraineur')),
            'ag': safe_int(r.get('age')),
            'sx': safe_str(r.get('sexe')),
            'npart': safe_int(r.get('numero_partant')),
            'cd': safe_int(r.get('corde')),
            'pd': safe_num(r.get('poids_kg'), 1),
            'mu': safe_str(r.get('musique')),
            'oe': safe_str(r.get('oeilleres')),
            'nc': safe_int(r.get('nb_courses_carriere')),
            'nv': safe_int(r.get('nb_victoires_carriere')),
            'np': safe_int(r.get('nb_places_carriere')),
            'gc': safe_num(r.get('gains_carriere')),
            'hv': safe_num(r.get('handicap_valeur'), 1),
            'to': safe_num(r['temps_officiel_sec'], 2),
            't6': safe_num(r['temps_600m_sec'], 2),
            't2f': safe_num(r['temps_200m_final_sec'], 2),
            'vm': safe_num(r['vitesse_max_kmh'], 1),
            'vy': safe_num(r['vitesse_moyenne_kmh'], 1),
            'd4': safe_num(r.get('dep_temps_400_eq'), 2),
            'v4p': feat_400_premier,
            't2p': feat_200_premier,
            'rel': ratio_early_late,
            'dp': delta_pos,
            'rg': regularite,
            'pmn': min(poss) if poss else None,
            'pmx': max(poss) if poss else None,
            'tr': tron_arr,
        })

    races_list = list(races.values())

    def pos_sort_key(h):
        try:
            return (0, int(h['pa']))
        except Exception:
            return (1, 999)

    for rr in races_list:
        rr['horses'].sort(key=pos_sort_key)
        rr['tracked'] = True

    # Cellule 5b — courses sans tracking (depuis chevaux seul)
    tracked_keys = {(rr['date'], rr['hippo'], rr['rnum'], rr['num']) for rr in races_list}
    races_nt = {}
    for _, r in ch.iterrows():
        date_i = int(r['date'])
        hippo = str(r['code_hippo'])
        nr = int(r['num_reunion']) if pd.notna(r.get('num_reunion')) else 0
        num_c = int(r['num_course'])
        if (date_i, hippo, nr, num_c) in tracked_keys:
            continue
        if pd.isna(r.get('ordre_arrivee')) or str(r.get('ordre_arrivee', '')).strip() == '':
            continue
        race_key = f"NT_{date_i}_{hippo}_R{nr}_{num_c}"
        if race_key not in races_nt:
            races_nt[race_key] = {
                'id': race_key, 'date': date_i, 'hippo': hippo, 'rnum': nr, 'num': num_c,
                'dist': int(r['distance_course']) if pd.notna(r.get('distance_course')) else None,
                'terrain': str(r['penetrometre']).strip() if pd.notna(r.get('penetrometre')) else None,
                'penetro': str(r['penetrometre']).strip() if pd.notna(r.get('penetrometre')) else None,
                'piste': str(r['type_piste']).strip() if pd.notna(r.get('type_piste')) else None,
                'cat': str(r['categorie_particularite']).strip() if pd.notna(r.get('categorie_particularite')) else None,
                'alloc': float(r['allocation_totale']) if pd.notna(r.get('allocation_totale')) else None,
                'cond_age': str(r['condition_age']).strip() if pd.notna(r.get('condition_age')) else None,
                'tracked': False,
                'horses': []
            }
        races_nt[race_key]['horses'].append({
            'nom': str(r['nom_cheval']).strip().upper(),
            'pa': str(int(r['ordre_arrivee'])) if pd.notna(r.get('ordre_arrivee')) else None,
            'jk': str(r['jockey']).strip() if pd.notna(r.get('jockey')) else None,
            'en': str(r['entraineur']).strip() if pd.notna(r.get('entraineur')) else None,
            'ag': int(r['age']) if pd.notna(r.get('age')) else None,
            'sx': str(r['sexe']).strip() if pd.notna(r.get('sexe')) else None,
            'npart': int(r['numero_partant']) if pd.notna(r.get('numero_partant')) else None,
            'cd': int(r['corde']) if pd.notna(r.get('corde')) else None,
            'pd': float(r['poids_kg']) if pd.notna(r.get('poids_kg')) else None,
            'mu': str(r['musique']).strip() if pd.notna(r.get('musique')) else None,
            'ec': str(r['ecart_precedent']).strip() if pd.notna(r.get('ecart_precedent')) else None,
            # Utilisés par le modèle (cellule 5b du notebook d'entraînement)
            'oe': None,
            'nc': int(r['nb_courses_carriere']) if pd.notna(r.get('nb_courses_carriere')) else None,
            'nv': int(r['nb_victoires_carriere']) if pd.notna(r.get('nb_victoires_carriere')) else None,
            'np': int(r['nb_places_carriere']) if pd.notna(r.get('nb_places_carriere')) else None,
            'gc': float(r['gains_carriere']) if pd.notna(r.get('gains_carriere')) else None,
            'hv': float(r['handicap_valeur']) if pd.notna(r.get('handicap_valeur')) else None,
            'tr': [], 'vy': None, 'vm': None, 't6': None, 't2f': None,
            'v4p': None, 't2p': None, 'rel': None, 'dp': None,
            'rg': None, 'pmn': None, 'pmx': None, 'to': None,
        })

    for rr in races_nt.values():
        rr['horses'].sort(key=lambda h: (int(h['pa']) if h['pa'] and h['pa'].isdigit() else 999))

    races_list.extend(races_nt.values())
    races_list.sort(key=lambda rr: (rr['date'], rr['hippo'], rr['num']))

    labels_array = [None] * len(label_dict)
    for k, v in label_dict.items():
        labels_array[v] = k
    return races_list, labels_array, ch
