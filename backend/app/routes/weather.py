import json
import logging
import threading
from datetime import datetime

from flask import Blueprint, Response, jsonify, request

from ..config import CHILE_TZ
from ..repositories.weather_readings import (
    count_readings,
    fetch_devices,
    fetch_health_stats,
    fetch_last_raw_json,
    fetch_latest_reading,
    fetch_range,
    get_db_status,
    save_reading,
)
from ..services.exporters import rows_to_csv, rows_to_json
from ..services.readings import flatten_rows, prepare_reading
from ..services.time import get_chile_now_text
from ..realtime import socketio

bp = Blueprint("weather", __name__)
logger = logging.getLogger(__name__)

LAST_PAYLOAD_RAW = None
LAST_PAYLOAD_LOCK = threading.Lock()


def parse_params():
    return (
        request.args.get("desde"),
        request.args.get("hasta"),
        request.args.get("device"),
        request.args.get("limit", type=int),
    )


@bp.route("/weather", methods=["POST"])
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


@bp.route("/weather/raw", methods=["GET"])
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


@bp.route("/weather/raw/db", methods=["GET"])
def get_last_raw_db():
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
    desde, hasta, device, limit = parse_params()
    rows = fetch_range(desde, hasta, device, limit)
    data = flatten_rows(rows)
    return jsonify({"total": len(data), "data": data}), 200


@bp.route("/weather/count", methods=["GET"])
def get_count():
    return jsonify({"total": count_readings()}), 200


@bp.route("/weather/devices", methods=["GET"])
def get_devices():
    return jsonify([dict(row) for row in fetch_devices()]), 200


@bp.route("/weather/export/csv", methods=["GET"])
def export_csv():
    desde, hasta, device, limit = parse_params()
    rows = fetch_range(desde, hasta, device, limit)

    if not rows:
        return jsonify({"msg": "sin datos para el periodo"}), 404

    data = flatten_rows(rows)
    filename = f"weather_{desde or 'all'}.csv"
    return Response(
        rows_to_csv(data),
        mimetype="text/csv",
        headers={"Content-Disposition": f"attachment; filename={filename}"},
    )


@bp.route("/weather/export/json", methods=["GET"])
def export_json_file():
    desde, hasta, device, limit = parse_params()
    rows = fetch_range(desde, hasta, device, limit)

    if not rows:
        return jsonify({"msg": "sin datos para el periodo"}), 404

    data = flatten_rows(rows)
    filename = f"weather_{desde or 'all'}.json"
    return Response(
        rows_to_json(data),
        mimetype="application/json",
        headers={"Content-Disposition": f"attachment; filename={filename}"},
    )


@bp.route("/health", methods=["GET"])
def health():
    total, last = fetch_health_stats()
    db_status = get_db_status()
    return jsonify(
        {
            "status": "ok",
            "server_time_utc": datetime.utcnow().isoformat(),
            "server_time_chile": datetime.now(CHILE_TZ).isoformat(),
            "db_total_registros": total,
            "ultimo_registro": last,
            **db_status,
        }
    ), 200
