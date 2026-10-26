"""Versions du modèle dans le stockage :

Nom d'une version : v<recette>[_<réentraînement>]. La recette = la liste des
variables (config.MODEL_RECIPE) ; chaque réentraînement de la même recette
ajoute _1, _2... Exemple : v8 (37 variables), v8_1 (même recette, réentraînée
le 01/10), v9 (recette v8 + performances corrigées du poids), v9_1...

  model/current.json            -> {"version": "v8_1"}  (modèle en production)
  model/<version>/race_model.txt   LightGBM
  model/<version>/calibration.json calibration isotonique (seuils)
  model/<version>/backtest.json    résultats de validation (onglet Résultats)
  model/<version>/meta.json        métriques, date d'entraînement...

La calibration est stockée en JSON (seuils de la régression isotonique) et
non en pickle : un pickle scikit-learn peut casser d'une version à l'autre."""

import json
import re

import numpy as np


class Calibrator:
    """Équivalent exact de IsotonicRegression(out_of_bounds='clip').predict."""

    def __init__(self, x, y):
        self.x = np.asarray(x, dtype=float)
        self.y = np.asarray(y, dtype=float)

    @classmethod
    def from_isotonic(cls, iso):
        return cls(iso.X_thresholds_, iso.y_thresholds_)

    def predict(self, v):
        return np.interp(np.asarray(v, dtype=float), self.x, self.y)

    def to_json(self):
        return {'x': self.x.tolist(), 'y': self.y.tolist()}

    @classmethod
    def from_json(cls, d):
        return cls(d['x'], d['y'])


def _j(storage, key):
    return json.loads(storage.read_bytes(key).decode('utf-8'))


def _w(storage, key, obj):
    storage.write_bytes(key, json.dumps(obj, ensure_ascii=False, separators=(',', ':')).encode('utf-8'),
                        content_type='application/json')


def current_version(storage):
    key = 'model/current.json'
    return _j(storage, key)['version'] if storage.exists(key) else None


def set_current(storage, version):
    _w(storage, 'model/current.json', {'version': version})


_VERSION = re.compile(r'v(\d+)(?:_(\d+))?')


def parse_version(v):
    """'v8' -> (8, 0), 'v8_2' -> (8, 2), autre -> None."""
    m = _VERSION.fullmatch(str(v))
    return (int(m.group(1)), int(m.group(2) or 0)) if m else None


def list_versions(storage):
    names = {k.split('/')[1] for k in storage.list('model/') if k.count('/') >= 2}
    return sorted(names, key=lambda v: parse_version(v) or (-1, -1))


def next_version(storage, recipe):
    """Premier entraînement d'une recette : son nom (ex. 'v9') ; ensuite
    v9_1, v9_2..."""
    major = parse_version(recipe)[0]
    minors = [parse_version(v)[1] for v in list_versions(storage)
              if parse_version(v) and parse_version(v)[0] == major]
    return recipe if not minors else f"{recipe}_{max(minors) + 1}"


def recipe_features(storage, recipe):
    """Variables de la première version existante de cette recette, ou None."""
    major = parse_version(recipe)[0]
    for v in list_versions(storage):
        pv = parse_version(v)
        if pv and pv[0] == major and storage.exists(f"model/{v}/meta.json"):
            return _j(storage, f"model/{v}/meta.json").get('features')
    return None


def rename_version(storage, src, dst):
    """Renomme une version (copie, mise à jour de meta.json et du pointeur de
    production, puis suppression de l'ancien nom). Retourne le nb de fichiers."""
    if parse_version(dst) is None:
        raise ValueError(f"Nom invalide : {dst} (attendu v8, v8_1...)")
    keys = storage.list(f"model/{src}/")
    if not keys:
        raise ValueError(f"Version introuvable : {src}")
    if storage.list(f"model/{dst}/"):
        raise ValueError(f"La version {dst} existe déjà")
    for k in keys:
        data = storage.read_bytes(k)
        if k.endswith('/meta.json'):
            meta = json.loads(data.decode('utf-8'))
            meta.update(version=dst, renamed_from=src)
            data = json.dumps(meta, ensure_ascii=False, separators=(',', ':')).encode('utf-8')
        storage.write_bytes(f"model/{dst}/" + k[len(f"model/{src}/"):], data)
    if current_version(storage) == src:
        set_current(storage, dst)
    for k in keys:
        storage.delete(k)
    return len(keys)


def save_version(storage, version, model_txt, calibrator, backtest, meta):
    base = f"model/{version}"
    storage.write_bytes(f"{base}/race_model.txt", model_txt.encode('utf-8'), content_type='text/plain')
    _w(storage, f"{base}/calibration.json", calibrator.to_json())
    if backtest is not None:
        _w(storage, f"{base}/backtest.json", backtest)
    _w(storage, f"{base}/meta.json", meta)


def write_meta(storage, version, meta):
    _w(storage, f"model/{version}/meta.json", meta)


def load_version(storage, version):
    """Retourne (booster, calibrator, backtest|None, meta)."""
    import lightgbm as lgb

    base = f"model/{version}"
    booster = lgb.Booster(model_str=storage.read_bytes(f"{base}/race_model.txt").decode('utf-8'))
    calib = Calibrator.from_json(_j(storage, f"{base}/calibration.json"))
    backtest = _j(storage, f"{base}/backtest.json") if storage.exists(f"{base}/backtest.json") else None
    meta = _j(storage, f"{base}/meta.json") if storage.exists(f"{base}/meta.json") else {}
    return booster, calib, backtest, meta
