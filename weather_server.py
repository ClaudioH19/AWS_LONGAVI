"""
Weather Station Server
- Receives weather payloads via POST /weather
- Serves dashboard in GET /
- Normalizes channels with payload.js mapping
"""

from flask import Flask, request, jsonify, Response, send_from_directory
import csv
import io
import json
import logging
import os
import re
import sqlite3
import threading
from datetime import datetime
from zoneinfo import ZoneInfo

app = Flask(__name__)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
FRONTEND_DIST_DIR = os.path.join(BASE_DIR, "frontend_dist")

DB_PATH = os.environ.get("DB_PATH", "weather_data.db")
PORT = int(os.environ.get("PORT", 3000))
LOG_FILE = os.environ.get("LOG_FILE", "weather_server.log")

if os.path.dirname(DB_PATH):
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.FileHandler(LOG_FILE), logging.StreamHandler()],
)
logger = logging.getLogger(__name__)

LAST_PAYLOAD_RAW = None
LAST_PAYLOAD_LOCK = threading.Lock()

PAYLOAD_MAP = {}
CHILE_TZ = ZoneInfo("America/Santiago")
CHANNEL_KEY_RE = re.compile(r"^ch\d+$", re.IGNORECASE)
UNQUOTED_KEY_RE = re.compile(r'([{\s,])([A-Za-z_][A-Za-z0-9_]*)\s*:')
TRAILING_COMMA_RE = re.compile(r",\s*([}\]])")
JS_COMMENT_LINE_RE = re.compile(r"//.*?$", re.MULTILINE)
JS_COMMENT_BLOCK_RE = re.compile(r"/\*.*?\*/", re.DOTALL)

FIXED_VARIABLE_ALIASES = {
    "Hum": {
        "",
        "hum",
        "humidity",
        "humedad",
        "rh",
        "hr",
    },
    "Vel": {
        "ch0",
        "vel",
        "windspeed",
        "windvelocity",
        "velocidad",
        "viento",
        "anemo",
    },
    "Dir": {
        "ch1",
        "dir",
        "direccion",
        "winddirection",
    },
    "Temp": {
        "ch2",
        "temp",
        "temperature",
        "temperatura",
    },
    "Precip": {
        "ch3",
        "precip",
        "rain",
        "rain24h",
        "lluvia",
        "precipitacion",
        "precipitation",
    },
    "Rad": {
        "ch4",
        "rad",
        "solar",
        "solarradiation",
        "radiation",
        "radiacion",
        "irradiance",
    },
}

METADATA_KEYS = {"DeviceID", "DeviceType", "DeviceVersion", "Timestamp"}


def _extract_js_object(text, start_idx=0):
    i = text.find("{", start_idx)
    if i == -1:
        return None
    depth = 0
    for j in range(i, len(text)):
        if text[j] == "{":
            depth += 1
        elif text[j] == "}":
            depth -= 1
            if depth == 0:
                return text[i : j + 1]
    return None


def _to_json_object_text(js_object_text):
    text = js_object_text.strip()
    text = re.sub(JS_COMMENT_BLOCK_RE, "", text)
    text = re.sub(JS_COMMENT_LINE_RE, "", text)
    text = re.sub(UNQUOTED_KEY_RE, r'\1"\2":', text)
    text = text.replace("'", '"')
    text = re.sub(TRAILING_COMMA_RE, r"\1", text)
    return text


def load_payload_map():
    global PAYLOAD_MAP
    possible_paths = [
        os.path.join(BASE_DIR, "payload.js"),
        os.path.join(BASE_DIR, "frontend", "payload.js"),
        os.path.join(BASE_DIR, "frontend", "src", "payload.js"),
    ]

    for path in possible_paths:
        try:
            if not os.path.isfile(path):
                continue
            with open(path, "r", encoding="utf-8") as file:
                text = file.read()

            idx = text.find("const payload")
            if idx == -1:
                idx = 0
            obj_text = _extract_js_object(text, idx)
            if not obj_text:
                continue

            try:
                PAYLOAD_MAP = json.loads(obj_text)
            except Exception:
                PAYLOAD_MAP = json.loads(_to_json_object_text(obj_text))

            logger.info("Payload map loaded from %s: %s", path, list(PAYLOAD_MAP.keys()))
            return
        except Exception as error:
            logger.warning("Could not parse payload map at %s: %s", path, error)

    PAYLOAD_MAP = {}
    logger.info("payload.js not found or invalid. Using empty payload map.")


def is_channel_key(key):
    return bool(CHANNEL_KEY_RE.match(key or ""))


def normalize_key_token(value):
    if value is None:
        return ""
    text = str(value).strip().lower()
    return re.sub(r"[^a-z0-9]+", "", text)


def resolve_fixed_variable(*candidates):
    for candidate in candidates:
        token = normalize_key_token(candidate)
        if token == "":
            return "Hum"
        for fixed_name, aliases in FIXED_VARIABLE_ALIASES.items():
            alias_tokens = {normalize_key_token(alias) for alias in aliases}
            if token in alias_tokens:
                return fixed_name
    return None


def get_chile_now_text():
    return datetime.now(CHILE_TZ).strftime("%Y-%m-%d %H:%M:%S")


def normalize_payload(raw_payload):
    """
    Normalize using PAYLOAD_MAP.
    - If key exists in mapping, mapped label is used.
    - Unmapped channel keys like ch5 are kept only in raw_json (not in normalized output).
    - Non-channel keys (DeviceID, Timestamp...) are preserved.
    """
    if not isinstance(raw_payload, dict):
        return {}

    normalized = {}
    for raw_key, value in raw_payload.items():
        key = "" if raw_key is None else str(raw_key)
        mapped_label = str(PAYLOAD_MAP.get(key, "")).strip()

        fixed_variable = resolve_fixed_variable(key, mapped_label)
        if fixed_variable:
            if fixed_variable not in normalized:
                normalized[fixed_variable] = value
            continue

        if key in METADATA_KEYS:
            normalized[key] = value
            continue

        if is_channel_key(key):
            # Unmapped channels are intentionally ignored from normalized output.
            continue

        fallback_label = mapped_label or key
        if fallback_label not in normalized:
            normalized[fallback_label] = value
    return normalized


load_payload_map()


def get_conn():
    conn = sqlite3.connect(DB_PATH, timeout=10)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    conn = get_conn()
    conn.execute("PRAGMA journal_mode=WAL")
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
    logger.info("DB ready: %s", os.path.abspath(DB_PATH))


def save_reading(raw=None, raw_text=None, received_at=None):
    if raw_text is not None:
        raw_payload_text = raw_text
    else:
        raw_payload_text = json.dumps(raw or {}, ensure_ascii=False)

    normalized_json = None
    try:
        if isinstance(raw, dict):
            normalized = normalize_payload(raw)
            normalized_json = json.dumps(normalized, ensure_ascii=False)
    except Exception as error:
        logger.warning("Error creating normalized_json: %s", error)

    conn = get_conn()
    conn.execute(
        "INSERT INTO weather_readings (received_at, raw_json, normalized_json) VALUES (?, ?, ?)",
        (received_at or get_chile_now_text(), raw_payload_text, normalized_json),
    )
    conn.commit()
    conn.close()


def parse_params():
    return (
        request.args.get("desde"),
        request.args.get("hasta"),
        request.args.get("device"),
        request.args.get("limit", type=int),
    )


def build_query(desde=None, hasta=None, device=None, limit=None):
    conditions, params = [], []
    if desde:
        conditions.append("received_at >= ?")
        params.append(desde)
    if hasta:
        conditions.append("received_at <= ?")
        params.append(f"{hasta} 23:59:59")
    if device:
        conditions.append("json_extract(raw_json, '$.DeviceID') = ?")
        params.append(device)

    query = "SELECT id, received_at, raw_json, normalized_json FROM weather_readings"
    if conditions:
        query += " WHERE " + " AND ".join(conditions)
    query += " ORDER BY received_at DESC"
    if limit:
        query += f" LIMIT {limit}"
    return query, params


def flatten_rows(rows):
    result = []
    for row_data in rows:
        row = dict(row_data)
        raw_payload = {}
        normalized_payload = {}

        try:
            raw_payload = json.loads(row.get("raw_json") or "{}")
        except Exception:
            raw_payload = {}

        try:
            normalized_payload = json.loads(row.get("normalized_json") or "{}")
        except Exception:
            normalized_payload = {}

        # Canonical source is raw_json; normalized_json only complements missing fields.
        payload = normalize_payload(raw_payload)
        normalized_fallback = normalize_payload(normalized_payload)
        for key, value in normalized_fallback.items():
            if key not in payload or payload[key] in ("", None):
                payload[key] = value

        normalized_result = {"id": row["id"], "received_at": row["received_at"]}
        for label, value in payload.items():
            if label not in normalized_result:
                normalized_result[label] = value
        result.append(normalized_result)
    return result


@app.route("/")
def dashboard():
    return send_from_directory(FRONTEND_DIST_DIR, "index.html")


@app.route("/<path:path>")
def frontend_files(path):
    file_path = os.path.join(FRONTEND_DIST_DIR, path)
    if os.path.isfile(file_path):
        return send_from_directory(FRONTEND_DIST_DIR, path)
    return send_from_directory(FRONTEND_DIST_DIR, "index.html")


@app.route("/weather", methods=["POST"])
def receive_weather():
    try:
        raw_text = request.get_data(as_text=True)
        logger.info("Payload raw: %s", raw_text)

        global LAST_PAYLOAD_RAW
        with LAST_PAYLOAD_LOCK:
            LAST_PAYLOAD_RAW = raw_text

        received_at = get_chile_now_text()
        raw = None
        if raw_text:
            try:
                raw = json.loads(raw_text)
            except Exception as error:
                logger.warning("Error parsing JSON body: %s", error)

        save_reading(raw=raw, raw_text=raw_text, received_at=received_at)

        if raw:
            logger.info(
                "Received | device=%s device_ts=%s received_at_chile=%s",
                raw.get("DeviceID", "?"),
                raw.get("Timestamp", "?"),
                received_at,
            )
        else:
            logger.info("Received | payload is not valid JSON")

        return jsonify({"status": "ok"}), 200
    except Exception as error:
        logger.error("Error receiving weather payload: %s", error)
        return jsonify({"status": "error", "msg": str(error)}), 500


@app.route("/weather/raw", methods=["GET"])
def get_last_raw():
    with LAST_PAYLOAD_LOCK:
        body = LAST_PAYLOAD_RAW

    if not body:
        return Response("", status=204, mimetype="text/plain")

    try:
        parsed = json.loads(body)
        return Response(json.dumps(parsed, ensure_ascii=False), mimetype="application/json")
    except Exception:
        return Response(body, mimetype="text/plain")


@app.route("/weather/raw/db", methods=["GET"])
def get_last_raw_db():
    conn = get_conn()
    row = conn.execute("SELECT raw_json FROM weather_readings ORDER BY id DESC LIMIT 1").fetchone()
    conn.close()

    if not row:
        return Response("", status=204, mimetype="text/plain")

    raw_json = row["raw_json"]
    try:
        parsed = json.loads(raw_json)
        return Response(json.dumps(parsed, ensure_ascii=False), mimetype="application/json")
    except Exception:
        return Response(raw_json, mimetype="text/plain")


@app.route("/weather/latest", methods=["GET"])
def get_latest():
    conn = get_conn()
    row = conn.execute(
        "SELECT id, received_at, raw_json, normalized_json FROM weather_readings ORDER BY id DESC LIMIT 1"
    ).fetchone()
    conn.close()

    if not row:
        return jsonify({"msg": "sin datos"}), 404
    return jsonify(flatten_rows([row])[0]), 200


@app.route("/weather/range", methods=["GET"])
def get_range():
    desde, hasta, device, limit = parse_params()
    query, params = build_query(desde, hasta, device, limit)
    conn = get_conn()
    rows = conn.execute(query, params).fetchall()
    conn.close()

    data = flatten_rows(rows)
    return jsonify({"total": len(data), "data": data}), 200


@app.route("/weather/count", methods=["GET"])
def get_count():
    conn = get_conn()
    total = conn.execute("SELECT COUNT(*) FROM weather_readings").fetchone()[0]
    conn.close()
    return jsonify({"total": total}), 200


@app.route("/weather/devices", methods=["GET"])
def get_devices():
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
        GROUP BY DeviceID, DeviceType, DeviceVersion
        """
    ).fetchall()
    conn.close()
    return jsonify([dict(r) for r in rows]), 200


@app.route("/weather/export/csv", methods=["GET"])
def export_csv():
    desde, hasta, device, limit = parse_params()
    query, params = build_query(desde, hasta, device, limit)
    conn = get_conn()
    rows = conn.execute(query, params).fetchall()
    conn.close()

    if not rows:
        return jsonify({"msg": "sin datos para el periodo"}), 404

    data = flatten_rows(rows)
    fieldnames = list(dict.fromkeys(k for row in data for k in row.keys()))

    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=fieldnames, extrasaction="ignore", restval="")
    writer.writeheader()
    writer.writerows(data)

    filename = f"weather_{desde or 'all'}.csv"
    return Response(
        output.getvalue(),
        mimetype="text/csv",
        headers={"Content-Disposition": f"attachment; filename={filename}"},
    )


@app.route("/weather/export/json", methods=["GET"])
def export_json_file():
    desde, hasta, device, limit = parse_params()
    query, params = build_query(desde, hasta, device, limit)
    conn = get_conn()
    rows = conn.execute(query, params).fetchall()
    conn.close()

    if not rows:
        return jsonify({"msg": "sin datos para el periodo"}), 404

    data = flatten_rows(rows)
    filename = f"weather_{desde or 'all'}.json"
    return Response(
        json.dumps(data, indent=2, ensure_ascii=False),
        mimetype="application/json",
        headers={"Content-Disposition": f"attachment; filename={filename}"},
    )


@app.route("/health", methods=["GET"])
def health():
    conn = get_conn()
    total = conn.execute("SELECT COUNT(*) FROM weather_readings").fetchone()[0]
    last = conn.execute("SELECT MAX(received_at) FROM weather_readings").fetchone()[0]
    conn.close()
    return jsonify(
        {
            "status": "ok",
            "server_time_utc": datetime.utcnow().isoformat(),
            "server_time_chile": datetime.now(CHILE_TZ).isoformat(),
            "db_total_registros": total,
            "ultimo_registro": last,
        }
    ), 200


if __name__ == "__main__":
    init_db()
    logger.info("=" * 55)
    logger.info("Port: %s  DB: %s", PORT, DB_PATH)
    logger.info("Dashboard  -> GET  /")
    logger.info("Datalogger -> POST /weather")
    logger.info("API        -> GET  /weather/latest | /range | /count | /devices")
    logger.info("Download   -> GET  /weather/export/csv | /export/json")
    logger.info("Health     -> GET  /health")
    logger.info("=" * 55)
    app.run(host="0.0.0.0", port=PORT, debug=False)
