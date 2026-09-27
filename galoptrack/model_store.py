"""Versions du modèle dans le stockage :

  model/current.json            -> {"version": "v7"}  (modèle en production)
  model/<version>/race_model.txt   LightGBM
  model/<version>/calibration.json calibration isotonique (seuils)
  model/<version>/backtest.json    résultats de validation (onglet Résultats)
  model/<version>/meta.json        métriques, date d'entraînement...

La calibration est stockée en JSON (seuils de la régression isotonique) et
non en pickle : un pickle scikit-learn peut casser d'une version à l'autre."""

import json

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


def list_versions(storage):
    return sorted({k.split('/')[1] for k in storage.list('model/') if k.count('/') >= 2},
                  key=lambda v: int(v[1:]) if v[1:].isdigit() else -1)


def next_version(storage):
    nums = [int(v[1:]) for v in list_versions(storage) if v[1:].isdigit()]
    return f"v{max(nums) + 1 if nums else 1}"


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
