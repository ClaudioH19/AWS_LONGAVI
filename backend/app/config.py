import os
from zoneinfo import ZoneInfo

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
FRONTEND_DIST_DIR = os.path.join(PROJECT_ROOT, "frontend_dist")

DB_PATH = os.path.abspath(os.environ.get("DB_PATH", os.path.join(PROJECT_ROOT, "weather_data.db")))
PORT = int(os.environ.get("PORT", 3000))
LOG_FILE = os.environ.get("LOG_FILE", "weather_server.log")
CHILE_TZ = ZoneInfo("America/Santiago")
DRAGONFLY_URL = os.environ.get("DRAGONFLY_URL", "")
SOCKETIO_CORS_ORIGINS = os.environ.get("SOCKETIO_CORS_ORIGINS", "*")

if os.path.dirname(DB_PATH):
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
