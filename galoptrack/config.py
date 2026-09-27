"""Configuration centralisée (anciennement dupliquée en tête de chaque notebook)."""

import os
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

# Les serveurs GitHub Actions / Render sont en UTC : toute notion de « jour »
# (courses du jour, J-1...) doit être calculée à l'heure de Paris.
TZ = ZoneInfo("Europe/Paris")

PMU_OFFLINE = "https://offline.turfinfo.api.pmu.fr/rest/client/7/programme"
PMU_ONLINE = "https://online.turfinfo.api.pmu.fr/rest/client/61/programme"
TRACKING_BASE = "https://www7.france-galop.com/Casaques/Tracking"
HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}

TIMEOUT_API = 20
TIMEOUT_PDF = 15
WORKERS = 4
PAUSE_ENTRE_REQ = 0.3

# Nombre de jours passés re-collectés à chaque run quotidien : les PDF de
# tracking et les rapports définitifs sont parfois publiés avec retard.
LOOKBACK_DAYS = int(os.environ.get("GALOPTRACK_LOOKBACK_DAYS", "3"))

# Garde-fou : un modèle réentraîné n'est promu en production que si son
# taux de réussite top-1 sur la validation atteint au moins ce seuil.
MIN_HIT_RATE_TOP1 = float(os.environ.get("GALOPTRACK_MIN_HIT_RATE", "0.20"))


def now_paris() -> datetime:
    return datetime.now(TZ)


def today_paris() -> date:
    return now_paris().date()


def yyyymmdd(d: date) -> str:
    return d.strftime("%Y%m%d")


def ddmmyyyy(d: date) -> str:
    return d.strftime("%d%m%Y")


def parse_date(s: str) -> date:
    """Accepte 'YYYY-MM-DD', 'YYYYMMDD', 'today', 'yesterday'."""
    s = str(s).strip().lower()
    if s == "today":
        return today_paris()
    if s == "yesterday":
        return today_paris() - timedelta(days=1)
    if "-" in s:
        return date.fromisoformat(s)
    return date(int(s[0:4]), int(s[4:6]), int(s[6:8]))


def date_range(start: date, end: date):
    d = start
    while d <= end:
        yield d
        d += timedelta(days=1)
