import json
import logging
import math

from .weather_normalizer import normalize_payload

logger = logging.getLogger(__name__)
WEATHER_KEYS = {"Hum", "Vel", "Dir", "Temp", "Precip", "Rad"}
METADATA_KEYS = {"DeviceID", "DeviceType", "DeviceVersion", "Timestamp"}


def validate_weather_payload(raw):
    if not isinstance(raw, dict) or not raw:
        raise ValueError("El payload debe ser un objeto JSON no vacío.")

    normalized = normalize_payload(raw)
    for key in METADATA_KEYS.intersection(raw):
        value = raw[key]
        if value is None:
            continue
        if isinstance(value, (dict, list, tuple, bool)):
            raise ValueError(f"El campo {key} debe ser un valor simple.")
        if len(str(value)) > 256:
            raise ValueError(f"El campo {key} supera el largo máximo permitido.")

    present_weather_keys = WEATHER_KEYS.intersection(normalized)
    if not present_weather_keys:
        raise ValueError("El payload no contiene variables meteorológicas reconocidas.")

    for key in present_weather_keys:
        value = normalized[key]
        if isinstance(value, bool):
            raise ValueError(f"La variable {key} debe ser numérica.")
        try:
            numeric_value = float(value)
        except (TypeError, ValueError) as error:
            raise ValueError(f"La variable {key} debe ser numérica.") from error
        if not math.isfinite(numeric_value):
            raise ValueError(f"La variable {key} debe ser finita.")

    return normalized


def prepare_reading(raw=None, raw_text=None):
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

    return raw_payload_text, normalized_json


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
