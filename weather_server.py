"""
Weather Station Server
- Recibe datos del datalogger via POST /weather
- Sirve dashboard en GET /
- Sin asumir nombres de campos del dispositivo
"""

from flask import Flask, request, jsonify, Response, send_file
import sqlite3
import os
import logging
import json
import csv
import io
from datetime import datetime

app = Flask(__name__)

DB_PATH  = os.environ.get("DB_PATH",  "weather_data.db")
PORT     = int(os.environ.get("PORT",  3000))
LOG_FILE = os.environ.get("LOG_FILE", "weather_server.log")

if os.path.dirname(DB_PATH):
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


# ============================================================
# DB
# ============================================================

def get_conn():
    conn = sqlite3.connect(DB_PATH, timeout=10)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    conn = get_conn()
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS weather_readings (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            received_at TEXT NOT NULL,
            raw_json    TEXT NOT NULL
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_received ON weather_readings(received_at)")
    conn.commit()
    conn.close()
    logger.info(f"DB lista: {os.path.abspath(DB_PATH)}")
    logger.info("WAL mode activo")


def save_reading(raw: dict):
    conn = get_conn()
    conn.execute(
        "INSERT INTO weather_readings (received_at, raw_json) VALUES (?, ?)",
        (datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"), json.dumps(raw))
    )
    conn.commit()
    conn.close()


# ============================================================
# HELPERS
# ============================================================

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
        params.append(hasta + " 23:59:59")
    if device:
        conditions.append("json_extract(raw_json, '$.DeviceID') = ?")
        params.append(device)

    q = "SELECT id, received_at, raw_json FROM weather_readings"
    if conditions:
        q += " WHERE " + " AND ".join(conditions)
    q += " ORDER BY received_at DESC"
    if limit:
        q += f" LIMIT {limit}"
    return q, params


def flatten_rows(rows):
    result = []
    for r in rows:
        row     = dict(r)
        payload = json.loads(row.get("raw_json") or "{}")
        result.append({"id": row["id"], "received_at": row["received_at"], **payload})
    return result


# ============================================================
# DASHBOARD
# ============================================================

@app.route("/")
def dashboard():
    return send_file("dashboard.html")


# ============================================================
# RECEPCIÓN
# ============================================================

@app.route("/weather", methods=["POST"])
def receive_weather():
    try:
        raw = request.get_json(force=True)
        if not raw:
            logger.warning("Request sin JSON válido")
            return jsonify({"status": "error", "msg": "no json"}), 400
        logger.info(f"Recibido | device={raw.get('DeviceID','?')} ts={raw.get('Timestamp','?')}")
        save_reading(raw)
        return jsonify({"status": "ok"}), 200
    except Exception as e:
        logger.error(f"Error: {e}")
        return jsonify({"status": "error", "msg": str(e)}), 500


# ============================================================
# API
# ============================================================

@app.route("/weather/latest", methods=["GET"])
def get_latest():
    conn = get_conn()
    row  = conn.execute(
        "SELECT id, received_at, raw_json FROM weather_readings ORDER BY id DESC LIMIT 1"
    ).fetchone()
    conn.close()
    if not row:
        return jsonify({"msg": "sin datos"}), 404
    return jsonify(flatten_rows([row])[0]), 200


@app.route("/weather/range", methods=["GET"])
def get_range():
    desde, hasta, device, limit = parse_params()
    q, p = build_query(desde, hasta, device, limit)
    conn  = get_conn()
    rows  = conn.execute(q, p).fetchall()
    conn.close()
    data  = flatten_rows(rows)
    return jsonify({"total": len(data), "data": data}), 200


@app.route("/weather/count", methods=["GET"])
def get_count():
    conn  = get_conn()
    total = conn.execute("SELECT COUNT(*) FROM weather_readings").fetchone()[0]
    conn.close()
    return jsonify({"total": total}), 200


@app.route("/weather/devices", methods=["GET"])
def get_devices():
    conn = get_conn()
    rows = conn.execute("""
        SELECT
            json_extract(raw_json, '$.DeviceID')      AS DeviceID,
            json_extract(raw_json, '$.DeviceType')    AS DeviceType,
            json_extract(raw_json, '$.DeviceVersion') AS DeviceVersion,
            COUNT(*)         AS registros,
            MIN(received_at) AS primer_dato,
            MAX(received_at) AS ultimo_dato
        FROM weather_readings
        GROUP BY DeviceID, DeviceType, DeviceVersion
    """).fetchall()
    conn.close()
    return jsonify([dict(r) for r in rows]), 200


@app.route("/weather/export/csv", methods=["GET"])
def export_csv():
    desde, hasta, device, limit = parse_params()
    q, p = build_query(desde, hasta, device, limit)
    conn  = get_conn()
    rows  = conn.execute(q, p).fetchall()
    conn.close()

    if not rows:
        return jsonify({"msg": "sin datos para el período"}), 404

    data       = flatten_rows(rows)
    fieldnames = list(dict.fromkeys(k for row in data for k in row.keys()))

    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=fieldnames, extrasaction="ignore", restval="")
    writer.writeheader()
    writer.writerows(data)

    filename = f"weather_{desde or 'all'}.csv"
    return Response(
        output.getvalue(),
        mimetype="text/csv",
        headers={"Content-Disposition": f"attachment; filename={filename}"}
    )


@app.route("/weather/export/json", methods=["GET"])
def export_json_file():
    desde, hasta, device, limit = parse_params()
    q, p = build_query(desde, hasta, device, limit)
    conn  = get_conn()
    rows  = conn.execute(q, p).fetchall()
    conn.close()

    if not rows:
        return jsonify({"msg": "sin datos para el período"}), 404

    data     = flatten_rows(rows)
    filename = f"weather_{desde or 'all'}.json"
    return Response(
        json.dumps(data, indent=2, ensure_ascii=False),
        mimetype="application/json",
        headers={"Content-Disposition": f"attachment; filename={filename}"}
    )


@app.route("/health", methods=["GET"])
def health():
    conn   = get_conn()
    total  = conn.execute("SELECT COUNT(*) FROM weather_readings").fetchone()[0]
    ultimo = conn.execute("SELECT MAX(received_at) FROM weather_readings").fetchone()[0]
    conn.close()
    return jsonify({
        "status":             "ok",
        "server_time_utc":    datetime.utcnow().isoformat(),
        "db_total_registros": total,
        "ultimo_registro":    ultimo,
    }), 200


# ============================================================
# MAIN
# ============================================================

if __name__ == "__main__":
    init_db()
    logger.info("=" * 55)
    logger.info(f"Puerto : {PORT}  DB : {DB_PATH}")
    logger.info("Dashboard  → GET  /")
    logger.info("Datalogger → POST /weather")
    logger.info("API        → GET  /weather/latest | /range | /count | /devices")
    logger.info("Descarga   → GET  /weather/export/csv | /export/json")
    logger.info("Health     → GET  /health")
    logger.info("=" * 55)
    app.run(host="0.0.0.0", port=PORT, debug=False)