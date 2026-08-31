import json
import hmac
import logging
import shutil
import sqlite3
import threading
from datetime import datetime, timezone

from flask import Blueprint, Response, abort, jsonify, request

from ..config import (
    CHILE_TZ,
    DEFAULT_RANGE_LIMIT,
    ENABLE_DIAGNOSTIC_ROUTES,
    INGEST_API_KEY,
    MAX_EXPORT_LIMIT,
    MAX_RANGE_LIMIT,
    MAX_RANGE_OFFSET,
    MIN_FREE_DISK_BYTES,
    STATION_STALE_AFTER_SECONDS,
)
from ..repositories.weather_readings import (
    check_db_connection,
    count_readings,
    fetch_devices,
    fetch_health_stats,
    fetch_last_raw_json,
    fetch_latest_reading,
    fetch_range,
    get_db_directory,
    save_reading,
)
from ..services.exporters import rows_to_csv, rows_to_json
from ..services.readings import flatten_rows, prepare_reading, validate_weather_payload
from ..services.time import get_chile_now_text
from ..realtime import socketio

bp = Blueprint("weather", __name__)
logger = logging.getLogger(__name__)

LAST_PAYLOAD_RAW = None
LAST_PAYLOAD_LOCK = threading.Lock()


def _parse_date(name):
    value = request.args.get(name)
    if not value:
        return None
    try:
        parsed = datetime.strptime(value, "%Y-%m-%d")
    except ValueError as error:
        raise ValueError(f"{name} debe usar el formato YYYY-MM-DD.") from error
    if parsed.strftime("%Y-%m-%d") != value:
        raise ValueError(f"{name} debe usar el formato YYYY-MM-DD.")
    return value


def _parse_bounded_int(name, default, minimum, maximum):
    value = request.args.get(name)
    if value in (None, ""):
        return default
    try:
        parsed = int(value)
    except ValueError as error:
        raise ValueError(f"{name} debe ser un número entero.") from error
    if parsed < minimum or parsed > maximum:
        raise ValueError(f"{name} debe estar entre {minimum} y {maximum}.")
    return parsed


def parse_params(maximum_limit=MAX_RANGE_LIMIT):
    desde = _parse_date("desde")
    hasta = _parse_date("hasta")
    if desde and hasta and desde > hasta:
        raise ValueError("desde no puede ser posterior a hasta.")

    device = (request.args.get("device") or "").strip() or None
    if device and len(device) > 128:
        raise ValueError("device supera el largo máximo permitido.")

    return {
        "desde": desde,
        "hasta": hasta,
        "device": device,
        "limit": _parse_bounded_int("limit", DEFAULT_RANGE_LIMIT, 1, maximum_limit),
        "offset": _parse_bounded_int("offset", 0, 0, MAX_RANGE_OFFSET),
    }


def _parse_json_object(raw_text):
    if not raw_text.strip():
        raise ValueError("El cuerpo de la solicitud está vacío.")

    def reject_constant(value):
        raise ValueError(f"Constante JSON no válida: {value}")

    def reject_duplicate_keys(pairs):
        result = {}
        for key, value in pairs:
            if key in result:
                raise ValueError(f"Clave JSON duplicada: {key or '<vacía>'}")
            result[key] = value
        return result

    raw = json.loads(
        raw_text,
        parse_constant=reject_constant,
        object_pairs_hook=reject_duplicate_keys,
    )
    validate_weather_payload(raw)
    return raw


def _diagnostics_required():
    if not ENABLE_DIAGNOSTIC_ROUTES:
        abort(404)


def _invalid_params_response(error):
    return jsonify({"error": "invalid_query", "message": str(error)}), 400


@bp.route("/weather", methods=["POST"])
def receive_weather():
    if INGEST_API_KEY:
        provided_key = request.headers.get("X-Weather-Key", "")
        if not hmac.compare_digest(provided_key, INGEST_API_KEY):
            return jsonify({"error": "unauthorized", "message": "Credencial de estación inválida."}), 401

    try:
        raw_text = request.get_data(as_text=True)
        raw = _parse_json_object(raw_text)

        if ENABLE_DIAGNOSTIC_ROUTES:
            global LAST_PAYLOAD_RAW
            with LAST_PAYLOAD_LOCK:
                LAST_PAYLOAD_RAW = raw_text

        received_at = get_chile_now_text()
        raw_payload_text, normalized_json = prepare_reading(raw=raw, raw_text=raw_text)
        save_reading(
            raw_payload_text=raw_payload_text,
            normalized_json=normalized_json,
            received_at=received_at,
        )
        latest_row = fetch_latest_reading()
        latest_reading = flatten_rows([latest_row])[0] if latest_row else None
        if latest_reading:
            try:
                socketio.emit(
                    "weather:reading",
                    {
                        "reading": latest_reading,
                        "last_received_at": received_at,
                        "server_time_chile": received_at,
                    },
                )
            except Exception as error:
                logger.warning("Could not publish realtime reading: %s", error)

        logger.info(
            "Received | device=%s device_ts=%s bytes=%s received_at_chile=%s",
            raw.get("DeviceID", "?"),
            raw.get("Timestamp", "?"),
            len(raw_text.encode("utf-8")),
            received_at,
        )

        return jsonify({"status": "ok"}), 200
    except (json.JSONDecodeError, ValueError) as error:
        logger.warning("Rejected weather payload: %s", error)
        return jsonify({"error": "invalid_payload", "message": str(error)}), 400
    except sqlite3.OperationalError as error:
        logger.error("Database unavailable while receiving payload: %s", error)
        return (
            jsonify({"error": "database_unavailable", "message": "Base de datos temporalmente no disponible."}),
            503,
            {"Retry-After": "5"},
        )
    except Exception as error:
        logger.exception("Unexpected error receiving weather payload: %s", error)
        return jsonify({"error": "internal_error", "message": "No se pudo procesar la lectura."}), 500


@bp.route("/weather/raw", methods=["GET"])
def get_last_raw():
    _diagnostics_required()
    with LAST_PAYLOAD_LOCK:
        body = LAST_PAYLOAD_RAW

    if not body:
        return Response("", status=204, mimetype="text/plain")

    try:
        parsed = json.loads(body)
        return Response(json.dumps(parsed, ensure_ascii=False), mimetype="application/json")
    except Exception:
        return Response(body, mimetype="text/plain")


@bp.route("/weather/raw/db", methods=["GET"])
def get_last_raw_db():
    _diagnostics_required()
    raw_json = fetch_last_raw_json()

    if not raw_json:
        return Response("", status=204, mimetype="text/plain")

    try:
        parsed = json.loads(raw_json)
        return Response(json.dumps(parsed, ensure_ascii=False), mimetype="application/json")
    except Exception:
        return Response(raw_json, mimetype="text/plain")


@bp.route("/weather/latest", methods=["GET"])
def get_latest():
    row = fetch_latest_reading()

    if not row:
        return jsonify({"msg": "sin datos"}), 404
    return jsonify(flatten_rows([row])[0]), 200


@bp.route("/weather/range", methods=["GET"])
def get_range():
    try:
        params = parse_params()
    except ValueError as error:
        return _invalid_params_response(error)

    rows = fetch_range(**{**params, "limit": params["limit"] + 1})
    has_more = len(rows) > params["limit"]
    data = flatten_rows(rows[: params["limit"]])
    return jsonify(
        {
            "total": len(data),
            "limit": params["limit"],
            "offset": params["offset"],
            "has_more": has_more,
            "data": data,
        }
    ), 200


@bp.route("/weather/count", methods=["GET"])
def get_count():
    _diagnostics_required()
    return jsonify({"total": count_readings()}), 200


@bp.route("/weather/devices", methods=["GET"])
def get_devices():
    _diagnostics_required()
    return jsonify([dict(row) for row in fetch_devices()]), 200


@bp.route("/weather/export/csv", methods=["GET"])
def export_csv():
    try:
        params = parse_params(MAX_EXPORT_LIMIT)
    except ValueError as error:
        return _invalid_params_response(error)
    rows = fetch_range(**params)

    if not rows:
        return jsonify({"msg": "sin datos para el periodo"}), 404

    data = flatten_rows(rows)
    filename = f"weather_{params['desde'] or 'all'}.csv"
    return Response(
        rows_to_csv(data),
        mimetype="text/csv",
        headers={"Content-Disposition": f"attachment; filename={filename}"},
    )


@bp.route("/weather/export/json", methods=["GET"])
def export_json_file():
    try:
        params = parse_params(MAX_EXPORT_LIMIT)
    except ValueError as error:
        return _invalid_params_response(error)
    rows = fetch_range(**params)

    if not rows:
        return jsonify({"msg": "sin datos para el periodo"}), 404

    data = flatten_rows(rows)
    filename = f"weather_{params['desde'] or 'all'}.json"
    return Response(
        rows_to_json(data),
        mimetype="application/json",
        headers={"Content-Disposition": f"attachment; filename={filename}"},
    )


@bp.route("/health", methods=["GET"])
def health():
    total, last = fetch_health_stats()
    return jsonify(
        {
            "status": "ok",
            "server_time_utc": datetime.now(timezone.utc).isoformat(),
            "server_time_chile": datetime.now(CHILE_TZ).isoformat(),
            "db_total_registros": total,
            "ultimo_registro": last,
        }
    ), 200


@bp.route("/health/live", methods=["GET"])
def health_live():
    return jsonify({"status": "ok"}), 200


@bp.route("/health/ready", methods=["GET"])
def health_ready():
    try:
        check_db_connection()
        free_bytes = shutil.disk_usage(get_db_directory()).free
        if free_bytes < MIN_FREE_DISK_BYTES:
            return jsonify({"status": "unavailable", "reason": "low_disk_space"}), 503
        return jsonify({"status": "ready"}), 200
    except Exception as error:
        logger.error("Readiness check failed: %s", error)
        return jsonify({"status": "unavailable", "reason": "database"}), 503


@bp.route("/status/station", methods=["GET"])
def station_status():
    _, last = fetch_health_stats()
    if not last:
        return jsonify({"status": "no_data", "last_received_at": None, "age_seconds": None}), 200

    try:
        last_date = datetime.strptime(last, "%Y-%m-%d %H:%M:%S").replace(tzinfo=CHILE_TZ)
        age_seconds = max(0, int((datetime.now(CHILE_TZ) - last_date).total_seconds()))
    except ValueError:
        return jsonify({"status": "unknown", "last_received_at": last, "age_seconds": None}), 200

    status = "stale" if age_seconds > STATION_STALE_AFTER_SECONDS else "online"
    return jsonify({"status": status, "last_received_at": last, "age_seconds": age_seconds}), 200
