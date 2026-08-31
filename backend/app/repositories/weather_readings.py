import logging
import os
import sqlite3
import threading

from ..config import DB_PATH, PROJECT_ROOT, SQLITE_JOURNAL_MODE

logger = logging.getLogger(__name__)

DB_INIT_LOCK = threading.Lock()
DB_INITIALIZED = False


def get_conn():
    ensure_db_ready()
    conn = sqlite3.connect(DB_PATH, timeout=10)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout=10000")
    return conn


def init_db():
    global DB_INITIALIZED
    with DB_INIT_LOCK:
        if DB_INITIALIZED:
            return

        conn = sqlite3.connect(DB_PATH, timeout=10)
        conn.row_factory = sqlite3.Row
        conn.execute(f"PRAGMA journal_mode={SQLITE_JOURNAL_MODE}")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS weather_readings (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                received_at     TEXT NOT NULL,
                raw_json        TEXT NOT NULL,
                normalized_json TEXT
            )
            """
        )
        conn.execute("CREATE INDEX IF NOT EXISTS idx_received ON weather_readings(received_at)")

        cols = [c[1] for c in conn.execute("PRAGMA table_info(weather_readings)").fetchall()]
        if "normalized_json" not in cols:
            try:
                conn.execute("ALTER TABLE weather_readings ADD COLUMN normalized_json TEXT")
                logger.info("Added normalized_json column to weather_readings")
            except Exception as error:
                logger.warning("Could not add normalized_json column: %s", error)

        conn.commit()
        conn.close()
        DB_INITIALIZED = True
        logger.info("DB ready: %s", os.path.abspath(DB_PATH))


def ensure_db_ready():
    if DB_INITIALIZED:
        return
    init_db()


def get_db_file_size():
    try:
        return os.path.getsize(DB_PATH)
    except OSError:
        return 0


def get_db_directory():
    return os.path.dirname(DB_PATH) or PROJECT_ROOT


def get_db_status():
    return {
        "db_path": DB_PATH,
        "db_exists": os.path.exists(DB_PATH),
        "db_directory": get_db_directory(),
        "db_size_bytes": get_db_file_size(),
        "db_storage": "sqlite",
        "db_initialized": DB_INITIALIZED,
    }


def save_reading(raw_payload_text, normalized_json=None, received_at=None):
    conn = get_conn()
    try:
        conn.execute(
            "INSERT INTO weather_readings (received_at, raw_json, normalized_json) VALUES (?, ?, ?)",
            (received_at, raw_payload_text, normalized_json),
        )
        conn.commit()
    finally:
        conn.close()


def build_query(desde=None, hasta=None, device=None, limit=None, offset=0):
    conditions, params = [], []
    if desde:
        conditions.append("received_at >= ?")
        params.append(desde)
    if hasta:
        conditions.append("received_at <= ?")
        params.append(f"{hasta} 23:59:59")
    if device:
        conditions.append("json_valid(raw_json) AND json_extract(raw_json, '$.DeviceID') = ?")
        params.append(device)

    query = "SELECT id, received_at, raw_json, normalized_json FROM weather_readings"
    if conditions:
        query += " WHERE " + " AND ".join(conditions)
    query += " ORDER BY received_at DESC, id DESC"
    if limit is not None:
        query += " LIMIT ? OFFSET ?"
        params.extend([limit, offset])
    return query, params


def fetch_latest_reading():
    conn = get_conn()
    row = conn.execute(
        "SELECT id, received_at, raw_json, normalized_json FROM weather_readings ORDER BY id DESC LIMIT 1"
    ).fetchone()
    conn.close()
    return row


def fetch_range(desde=None, hasta=None, device=None, limit=None, offset=0):
    query, params = build_query(desde, hasta, device, limit, offset)
    conn = get_conn()
    rows = conn.execute(query, params).fetchall()
    conn.close()
    return rows


def fetch_last_raw_json():
    conn = get_conn()
    row = conn.execute("SELECT raw_json FROM weather_readings ORDER BY id DESC LIMIT 1").fetchone()
    conn.close()
    return row["raw_json"] if row else None


def count_readings():
    conn = get_conn()
    total = conn.execute("SELECT COUNT(*) FROM weather_readings").fetchone()[0]
    conn.close()
    return total


def fetch_devices():
    conn = get_conn()
    rows = conn.execute(
        """
        SELECT
            json_extract(raw_json, '$.DeviceID')      AS DeviceID,
            json_extract(raw_json, '$.DeviceType')    AS DeviceType,
            json_extract(raw_json, '$.DeviceVersion') AS DeviceVersion,
            COUNT(*)         AS registros,
            MIN(received_at) AS primer_dato,
            MAX(received_at) AS ultimo_dato
        FROM weather_readings
        WHERE json_valid(raw_json)
        GROUP BY DeviceID, DeviceType, DeviceVersion
        """
    ).fetchall()
    conn.close()
    return rows


def fetch_health_stats():
    conn = get_conn()
    total = conn.execute("SELECT COUNT(*) FROM weather_readings").fetchone()[0]
    last = conn.execute("SELECT MAX(received_at) FROM weather_readings").fetchone()[0]
    conn.close()
    return total, last


def check_db_connection():
    conn = get_conn()
    try:
        conn.execute("SELECT 1").fetchone()
    finally:
        conn.close()
