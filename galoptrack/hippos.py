"""Table unique des hippodromes (auparavant copiée dans 5 notebooks, avec des
différences : le notebook cotes codait Lyon La Soie 'LLS' au lieu de 'LSO' et
ne connaissait pas une dizaine d'hippodromes)."""

# Nom hippodrome PMU -> code France Galop (clé de jointure de toutes les tables)
HIPPO_NAME_TO_CODE = {
    'CHANTILLY': 'CHA', 'DEAUVILLE': 'DEA', 'LONGCHAMP': 'LPA',
    'PARIS-LONGCHAMP': 'LPA', 'PARISLONGCHAMP': 'LPA',
    'SAINT-CLOUD': 'SAI', 'MAISONS-LAFFITTE': 'MAI',
    'EVRY': 'EVR', 'ÉVRY': 'EVR', 'COMPIEGNE': 'COM',
    'COMPIÈGNE': 'COM', 'CLAIREFONTAINE': 'CLF',
    "LE LION D'ANGERS": 'LLA', "LION D'ANGERS": 'LLA',
    'VICHY': 'VIC', 'NANTES': 'NAR', 'STRASBOURG': 'STR',
    'LYON PARILLY': 'LYO', 'LYON-PARILLY': 'LYO',
    'LYON LA SOIE': 'LSO', 'MARSEILLE': 'MAR',
    'TOULOUSE': 'TOU', 'ANGERS': 'ANG',
    'CAGNES': 'CAG', 'CAGNES-SUR-MER': 'CAG',
    'BORDEAUX': 'BOR', 'BORDEAUX LE BOUSCAT': 'BOR',
    'FONTAINEBLEAU': 'FON',
    'AIX': 'AIX', 'AIX-LES-BAINS': 'AIX', 'AIX LES BAINS': 'AIX',
    'AMIENS': 'AMI',
    'ARGENTAN': 'ARG',
    'BRASSAC': 'BRA', 'BRASSAC-LES-MINES': 'BRA',
    'CRAON': 'CRA',
    'CROS-DE-CAGNES': 'CRO', 'CROS DE CAGNES': 'CRO',
    'DAX': 'DAX',
    'DIEPPE': 'DIE',
    'LA TESTE': 'LAT', 'LA TESTE DE BUCH': 'LAT',
    'LE MANS': 'MAN',
    'MOULINS': 'MOU',
    'PAU': 'PAU',
    'PLUMAUDAN': 'PLB',
    'SAINTES': 'S-M', 'SAINTES-MARIE': 'S-M',
    'SALON': 'SAL', 'SALON-DE-PROVENCE': 'SAL',
    'TARBES': 'TAR',
}

# Hippodromes français retenus pour l'enrichissement / les rapports
CODES_FRANCE = {
    'CHA', 'DEA', 'LPA', 'SAI', 'MAI', 'EVR', 'COM', 'CLF', 'LLA', 'VIC', 'NAR', 'STR',
    'LYO', 'LSO', 'MAR', 'TOU', 'ANG', 'CAG', 'BOR', 'FON', 'AIX', 'AMI', 'ARG', 'BRA',
    'CRA', 'CRO', 'DAX', 'DIE', 'LAT', 'MAN', 'MOU', 'PAU', 'PLB', 'S-M', 'SAL', 'TAR',
}

# Certains hippodromes utilisent un autre code dans l'URL des PDF France Galop
CODE_PMU_TO_PDF = {
    'LPA': 'LON',  # Longchamp
    'CLF': 'CLA',  # Clairefontaine
    'LSO': 'VIL',  # Lyon La Soie
    'MAR': 'BOR',  # Marseille Borély
    'BOR': 'BOU',  # Bordeaux le Bouscat
    'TOU': 'CEP',  # Toulouse Cépière
    'NAR': 'PET',  # Nantes La Petite Brosse
}

# Anciens codes rencontrés dans l'historique -> code canonique
LEGACY_CODE_FIXES = {
    'LLS': 'LSO',  # notebook cotes
}


def hippo_to_code(nom: str) -> str:
    """Nom PMU -> code France Galop, '' si inconnu."""
    nom_up = (nom or '').upper().strip()
    if nom_up in HIPPO_NAME_TO_CODE:
        return HIPPO_NAME_TO_CODE[nom_up]
    for key, code in HIPPO_NAME_TO_CODE.items():
        if key in nom_up or nom_up in key:
            return code
    return ''


def reunion_code(reunion: dict) -> str:
    """Code hippodrome d'une réunion du programme PMU (logique scraper /
    enrichissement : mapping par nom, sinon code PMU brut)."""
    hippo = reunion.get('hippodrome', {}) or {}
    nom_hippo = hippo.get('libelleLong', hippo.get('libelleCourt', ''))
    code_pmu = hippo.get('codeHippodrome') or hippo.get('code', '')
    return hippo_to_code(nom_hippo) or code_pmu


def reunion_code_today(reunion: dict) -> str:
    """Variante du générateur de dashboard (cellule 6b) : repli sur les 3
    premières lettres du nom si l'hippodrome n'est pas dans la table."""
    hippo = reunion.get('hippodrome', {}) or {}
    nom = hippo.get('libelleLong') or hippo.get('libelleCourt', '')
    code = hippo_to_code(nom)
    if code:
        return code
    nom_up = (nom or '').upper().strip()
    return nom_up[:3] if len(nom_up) >= 3 else nom_up


def pdf_codes(code_hippo: str) -> list:
    return list(dict.fromkeys([CODE_PMU_TO_PDF.get(code_hippo, code_hippo), code_hippo]))
