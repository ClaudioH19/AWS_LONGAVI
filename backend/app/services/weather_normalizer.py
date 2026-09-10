import logging
import re

from ..models.station_payload import NORMALIZED_FIELD_NAMES, OPTIONAL_METADATA_FIELDS

logger = logging.getLogger(__name__)

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
        if key in NORMALIZED_FIELD_NAMES:
            normalized[NORMALIZED_FIELD_NAMES[key]] = value
            continue

        fixed_variable = resolve_fixed_variable(key)
        if fixed_variable:
            if fixed_variable not in normalized:
                normalized[fixed_variable] = value
            continue

        if key in OPTIONAL_METADATA_FIELDS:
            normalized[key] = value
            continue
    return normalized
