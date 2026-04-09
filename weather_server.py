"""
Weather Station Server
- Recibe datos del datalogger via POST /weather
- Sirve dashboard en GET /
- Sin asumir nombres de campos del dispositivo
"""

from flask import Flask, request, jsonify, Response, send_from_directory
import sqlite3
import os
import logging
import json
import csv
import io
from datetime import datetime
import threading

app = Flask(__name__)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
FRONTEND_DIST_DIR = os.path.join(BASE_DIR, "frontend_dist")

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

# Último payload recibido (crudo) para debugging
LAST_PAYLOAD_RAW = None
LAST_PAYLOAD_LOCK = threading.Lock()

# Payload mapping cargada desde payload.js (frontend)
PAYLOAD_MAP = {}


def _extract_js_object(text, start_idx=0):
    """Extrae el primer objeto JS {...} comenzando en start_idx y devuelve el string del objeto.
    Maneja balanceo de llaves de forma simple.
    """
    i = text.find('{', start_idx)
    if i == -1:
        return None
    depth = 0
    for j in range(i, len(text)):
        if text[j] == '{':
            depth += 1
        elif text[j] == '}':
            depth -= 1
            if depth == 0:
                return text[i:j+1]
    return None


def load_payload_map():
    """Intenta cargar el mapping desde payload.js (workspace root)."""
    global PAYLOAD_MAP
    possible_paths = [
        os.path.join(BASE_DIR, 'payload.js'),
        os.path.join(BASE_DIR, 'frontend', 'payload.js'),
        os.path.join(BASE_DIR, 'frontend', 'src', 'payload.js'),
    ]
    for p in possible_paths:
        try:
            if not os.path.isfile(p):
                continue
            with open(p, 'r', encoding='utf-8') as f:
                txt = f.read()
            # localizar const payload = { ... }
            idx = txt.find('const payload')
            if idx == -1:
                idx = 0
            obj_text = _extract_js_object(txt, idx)
            if not obj_text:
                continue
            # obj_text debería ser JSON válido (usa comillas dobles en el repo actual)
            try:
                PAYLOAD_MAP = json.loads(obj_text)
                logger.info(f'Payload map cargado desde {p}: {list(PAYLOAD_MAP.keys())}')
                return
            except Exception as e:
                # intentar reemplazar comillas simples por dobles como fallback
                try_text = obj_text.replace("'", '"')
                try:
                    PAYLOAD_MAP = json.loads(try_text)
                    logger.info(f'Payload map cargado (fallback) desde {p}')
                    return
                except Exception:
                    logger.warning(f'No pude parsear objeto JS en {p}: {e}')
        except Exception:
            continue
    logger.info('No se encontró payload.js, usando mapping vacío')


# Cargar mapping al inicio
load_payload_map()


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
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            received_at     TEXT NOT NULL,
            raw_json        TEXT NOT NULL,
            normalized_json TEXT
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_received ON weather_readings(received_at)")
    # Si la tabla existía sin la columna normalized_json, añadirla
    cols = [c[1] for c in conn.execute("PRAGMA table_info(weather_readings)").fetchall()]
    if 'normalized_json' not in cols:
        try:
            conn.execute("ALTER TABLE weather_readings ADD COLUMN normalized_json TEXT")
            logger.info('Añadida columna normalized_json a weather_readings')
        except Exception as e:
            logger.warning(f'Error añadiendo columna normalized_json: {e}')

    conn.commit()
    conn.close()
    logger.info(f"DB lista: {os.path.abspath(DB_PATH)}")
    logger.info("WAL mode activo")


def save_reading(raw: dict = None, raw_text: str = None):
    """Guarda una lectura en la BD.
    - Si `raw_text` se proporciona, se guarda tal cual (payload crudo).
    - Si no, se serializa `raw` (aplicando la renombración de key vacía).
    """
    # Guardar raw_text tal cual en raw_json y una versión normalizada en normalized_json
    if raw_text is not None:
        raw_payload_text = raw_text
    else:
        raw_payload_text = json.dumps(raw or {})

    normalized_json = None
    try:
        if raw and isinstance(raw, dict):
            # si hay key vacía y el mapping tiene una etiqueta para "", renombrarla
            if "" in raw and "" in PAYLOAD_MAP:
                label = PAYLOAD_MAP.get("")
                raw[label] = raw.pop("")

            normalized = {}
            for k, v in raw.items():
                label = PAYLOAD_MAP.get(k, k)
                normalized[label] = v
            normalized_json = json.dumps(normalized, ensure_ascii=False)
    except Exception as e:
        logger.warning(f'Error generando normalized_json: {e}')

    conn = get_conn()
    conn.execute(
        "INSERT INTO weather_readings (received_at, raw_json, normalized_json) VALUES (?, ?, ?)",
        (datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"), raw_payload_text, normalized_json)
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

    q = "SELECT id, received_at, raw_json, normalized_json FROM weather_readings"
    if conditions:
        q += " WHERE " + " AND ".join(conditions)
    q += " ORDER BY received_at DESC"
    if limit:
        q += f" LIMIT {limit}"
    return q, params


def flatten_rows(rows):
    result = []
    for r in rows:
        row = dict(r)
        payload = {}
        # Preferir la versión normalizada si existe en la BD
        if row.get('normalized_json'):
            try:
                payload = json.loads(row.get('normalized_json') or "{}")
            except Exception:
                payload = {}
        else:
            try:
                raw_payload = json.loads(row.get('raw_json') or "{}")
            except Exception:
                raw_payload = {}
            # Mapear keys usando PAYLOAD_MAP (si existe)
            payload = {}
            for k, v in raw_payload.items():
                label = PAYLOAD_MAP.get(k, k)
                payload[label] = v

        # Normalizar claves a lower-case para evitar duplicados case-insensitive,
        # pero mantener la etiqueta original (casing) para mostrar en frontend
        normalized_map = {}
        for k, v in payload.items():
            if isinstance(k, str):
                key_norm = k.lower()
            else:
                key_norm = k
            if key_norm not in normalized_map:
                normalized_map[key_norm] = (k, v)

        # id y received_at se mantienen con su nombre original
        normalized_result = {"id": row["id"], "received_at": row["received_at"]}
        for key_norm, (label, value) in normalized_map.items():
            if label not in normalized_result:
                normalized_result[label] = value
        result.append(normalized_result)
    return result


# ============================================================
# DASHBOARD
# ============================================================

@app.route("/")
def dashboard():
    return send_from_directory(FRONTEND_DIST_DIR, "index.html")


@app.route("/<path:path>")
def frontend_files(path):
    file_path = os.path.join(FRONTEND_DIST_DIR, path)
    if os.path.isfile(file_path):
        return send_from_directory(FRONTEND_DIST_DIR, path)
    return send_from_directory(FRONTEND_DIST_DIR, "index.html")


# ============================================================
# RECEPCIÓN
# ============================================================

@app.route("/weather", methods=["POST"])
def receive_weather():

    try:
        # Registrar cuerpo bruto para inspección del payload entrante
        raw_text = request.get_data(as_text=True)
        logger.info(f"Payload raw: {raw_text}")

        # Guardar último payload crudo en memoria para endpoint de debug
        global LAST_PAYLOAD_RAW
        with LAST_PAYLOAD_LOCK:
            LAST_PAYLOAD_RAW = raw_text

        raw = None
        if raw_text:
            try:
                raw = json.loads(raw_text)
            except Exception as e:
                logger.warning(f"Error parsing JSON body: {e}")

        # Guardar SIEMPRE el payload crudo en la BD (raw_text). Además se
        # conserva la variable `raw` si el JSON pudo parsearse.
        save_reading(raw=raw, raw_text=raw_text)

        if raw:
            logger.info(f"Recibido | device={raw.get('DeviceID','?')} ts={raw.get('Timestamp','?')}")
        else:
            logger.info("Recibido | payload no JSON o no parseable")

        return jsonify({"status": "ok"}), 200
    except Exception as e:
        logger.error(f"Error: {e}")
        return jsonify({"status": "error", "msg": str(e)}), 500
    
@app.route("/weather/raw", methods=["GET"])
def get_last_raw():
    """Devuelve el último payload crudo recibido (debug).
    Si es JSON válido se devuelve como JSON, si no como texto plano.
    """
    with LAST_PAYLOAD_LOCK:
        s = LAST_PAYLOAD_RAW

    if not s:
        return Response("", status=204, mimetype="text/plain")

    try:
        parsed = json.loads(s)
        return Response(json.dumps(parsed, ensure_ascii=False), mimetype="application/json")
    except Exception:
        return Response(s, mimetype="text/plain")


@app.route("/weather/raw/db", methods=["GET"])
def get_last_raw_db():
    """Devuelve el último `raw_json` almacenado en la BD (debug).
    Si es JSON válido se devuelve como JSON, si no como texto plano.
    """
    conn = get_conn()
    row = conn.execute("SELECT raw_json FROM weather_readings ORDER BY id DESC LIMIT 1").fetchone()
    conn.close()

    if not row:
        return Response("", status=204, mimetype="text/plain")

    raw_json = row["raw_json"] if isinstance(row, dict) or hasattr(row, '__getitem__') else row[0]
    try:
        parsed = json.loads(raw_json)
        return Response(json.dumps(parsed, ensure_ascii=False), mimetype="application/json")
    except Exception:
        return Response(raw_json, mimetype="text/plain")


# ============================================================
# API
# ============================================================

@app.route("/weather/latest", methods=["GET"])
def get_latest():
    conn = get_conn()
    row  = conn.execute(
        "SELECT id, received_at, raw_json, normalized_json FROM weather_readings ORDER BY id DESC LIMIT 1"
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