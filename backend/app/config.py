import os
from zoneinfo import ZoneInfo


def _env_int(name, default, minimum=None, maximum=None):
    try:
        value = int(os.environ.get(name, default))
    except (TypeError, ValueError):
        value = default
    if minimum is not None:
        value = max(minimum, value)
    if maximum is not None:
        value = min(maximum, value)
    return value


def _env_bool(name, default=False):
    value = os.environ.get(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
FRONTEND_DIST_DIR = os.path.join(PROJECT_ROOT, "frontend_dist")

DB_PATH = os.path.abspath(os.environ.get("DB_PATH", os.path.join(PROJECT_ROOT, "weather_data.db")))
PORT = _env_int("PORT", 3000, 1, 65535)
LOG_FILE = os.environ.get("LOG_FILE", "").strip()
LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO").upper()
CHILE_TZ = ZoneInfo("America/Santiago")
DRAGONFLY_URL = os.environ.get("DRAGONFLY_URL", "")
SOCKETIO_CORS_ORIGINS = os.environ.get("SOCKETIO_CORS_ORIGINS", "").strip() or None
MAX_CONTENT_LENGTH = _env_int("MAX_CONTENT_LENGTH", 64 * 1024, 1024, 1024 * 1024)
DEFAULT_RANGE_LIMIT = _env_int("DEFAULT_RANGE_LIMIT", 1000, 1, 5000)
MAX_RANGE_LIMIT = _env_int("MAX_RANGE_LIMIT", 5000, DEFAULT_RANGE_LIMIT, 20000)
MAX_EXPORT_LIMIT = _env_int("MAX_EXPORT_LIMIT", 50000, MAX_RANGE_LIMIT, 100000)
MAX_RANGE_OFFSET = _env_int("MAX_RANGE_OFFSET", 1_000_000, 0)
STATION_STALE_AFTER_SECONDS = _env_int("STATION_STALE_AFTER_SECONDS", 3 * 60 * 60, 60)
MIN_FREE_DISK_BYTES = _env_int("MIN_FREE_DISK_BYTES", 128 * 1024 * 1024, 0)
ENABLE_DIAGNOSTIC_ROUTES = _env_bool("ENABLE_DIAGNOSTIC_ROUTES", False)
INGEST_API_KEY = os.environ.get("INGEST_API_KEY", "")
TRUST_PROXY_COUNT = _env_int("TRUST_PROXY_COUNT", 1, 0, 4)

SQLITE_JOURNAL_MODE = os.environ.get("SQLITE_JOURNAL_MODE", "DELETE").strip().upper()
if SQLITE_JOURNAL_MODE not in {"DELETE", "WAL"}:
    SQLITE_JOURNAL_MODE = "DELETE"

if os.path.dirname(DB_PATH):
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
