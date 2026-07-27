import json
import logging
import os
import re

from ..config import PROJECT_ROOT

logger = logging.getLogger(__name__)

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
    possible_paths = [
        os.path.join(PROJECT_ROOT, "payload.js"),
        os.path.join(PROJECT_ROOT, "frontend", "payload.js"),
        os.path.join(PROJECT_ROOT, "frontend", "src", "payload.js"),
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
                payload_map = json.loads(obj_text)
            except Exception:
                payload_map = json.loads(_to_json_object_text(obj_text))

            logger.info("Payload map loaded from %s: %s", path, list(payload_map.keys()))
            return payload_map
        except Exception as error:
            logger.warning("Could not parse payload map at %s: %s", path, error)

    logger.info("payload.js not found or invalid. Using empty payload map.")
    return {}


def is_channel_key(key):
    return bool(CHANNEL_KEY_RE.match(key or ""))


def normalize_key_token(value):
    if value is None:
        return ""
    text = str(value).strip().lower()
    return re.sub(r"[^a-z0-9]+", "", text)


def resolve_fixed_variable(*candidates):
    for candidate in candidates:
        if candidate is None:
            continue
        candidate_text = str(candidate).strip()
        token = normalize_key_token(candidate)
        if token == "":
            if candidate_text == "":
                return "Hum"
            continue
        for fixed_name, aliases in FIXED_VARIABLE_ALIASES.items():
            alias_tokens = {normalize_key_token(alias) for alias in aliases}
            if token in alias_tokens:
                return fixed_name
    return None


def normalize_payload(raw_payload):
    if not isinstance(raw_payload, dict):
        return {}

    normalized = {}
    for raw_key, value in raw_payload.items():
        key = "" if raw_key is None else str(raw_key)
        mapped_label = str(PAYLOAD_MAP.get(key, "")).strip()

        fixed_variable = resolve_fixed_variable(key)
        if not fixed_variable and mapped_label:
            fixed_variable = resolve_fixed_variable(mapped_label)
        if fixed_variable:
            if fixed_variable not in normalized:
                normalized[fixed_variable] = value
            continue

        if key in METADATA_KEYS:
            normalized[key] = value
            continue

        if is_channel_key(key):
            continue

        fallback_label = mapped_label or key
        if fallback_label not in normalized:
            normalized[fallback_label] = value
    return normalized


PAYLOAD_MAP = load_payload_map()
