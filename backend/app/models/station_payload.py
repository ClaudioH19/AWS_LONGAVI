"""Contrato controlado del JSON emitido por la estación meteorológica.

Este módulo es la única fuente de verdad para la ingesta.  No se permite
inferir campos a partir de etiquetas arbitrarias ni ejecutar/interpretar
archivos suministrados por un cliente.
"""

from __future__ import annotations

import math
import re
from datetime import datetime
from numbers import Real

TELEMETRY_FIELDS = ("", "ch0", "ch1", "ch2", "ch3", "ch4")
OPTIONAL_METADATA_FIELDS = ("DeviceID", "DeviceType", "DeviceVersion", "Timestamp")

# Mantiene los nombres que ya consume el frontend y los datos históricos.
NORMALIZED_FIELD_NAMES = {
    "": "Hum",
    "ch0": "Vel",
    "ch1": "Dir",
    "ch2": "Temp",
    "ch3": "Precip",
    "ch4": "Rad",
}

METADATA_MAX_LENGTH = 128
NUMERIC_TEXT_RE = re.compile(r"^[+-]?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?$")
CHANNEL_FIELD_RE = re.compile(r"^ch(?:0|[1-9]\d*)$")
LEGACY_TIMESTAMP_RE = re.compile(
    r"^(?P<year>\d{4})-(?P<month>\d{2})-(?P<day_of_year>\d{3}) "
    r"(?P<time>\d{2}:\d{2}:\d{2})$"
)
# IDs/versiones solo admiten caracteres de identificación habituales. Esto
# excluye HTML, comillas, controles y prefijos de fórmula al exportar CSV.
SAFE_METADATA_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:/ -]*$")
TIMESTAMP_FORMATS = (
    "%Y-%m-%d %H:%M:%S",
    "%Y-%m-%dT%H:%M:%S",
)


def validate_station_payload(raw: object) -> dict:
    """Valida el contrato exacto de la estación y retorna el payload tipado.

    El datalogger puede serializar canales como números o texto numérico. Los
    canales adicionales siguen el patrón ``chN`` y se conservan en el JSON
    original, aunque sólo los canales conocidos se normalicen para el panel.
    Otros nombres continúan prohibidos para impedir que un POST sea usado como
    almacén genérico de JSON, HTML o scripts.
    """
    if not isinstance(raw, dict) or not raw:
        raise ValueError("El payload debe ser un objeto JSON no vacío.")

    channel_fields = {
        field
        for field in raw
        if isinstance(field, str) and CHANNEL_FIELD_RE.fullmatch(field)
    }
    allowed_fields = set(TELEMETRY_FIELDS).union(OPTIONAL_METADATA_FIELDS, channel_fields)
    unexpected = set(raw).difference(allowed_fields)
    if unexpected:
        raise ValueError("El payload contiene campos no permitidos.")
    missing = set(TELEMETRY_FIELDS).difference(raw)
    if missing:
        raise ValueError("El payload no contiene todos los canales de la estación.")

    for field in set(TELEMETRY_FIELDS).union(channel_fields):
        value = raw[field]
        if isinstance(value, bool) or isinstance(value, (dict, list, tuple)):
            raise ValueError(f"El canal {field or 'humedad'} debe ser numérico.")
        if isinstance(value, str) and not NUMERIC_TEXT_RE.fullmatch(value.strip()):
            raise ValueError(f"El canal {field or 'humedad'} debe ser numérico.")
        if not isinstance(value, (str, Real)):
            raise ValueError(f"El canal {field or 'humedad'} debe ser numérico.")
        try:
            numeric_value = float(value)
        except (TypeError, ValueError) as error:
            raise ValueError(f"El canal {field or 'humedad'} debe ser numérico.") from error
        if not math.isfinite(numeric_value):
            raise ValueError(f"El canal {field or 'humedad'} debe ser finito.")

    for field in OPTIONAL_METADATA_FIELDS:
        if field not in raw or raw[field] is None:
            continue
        value = raw[field]
        if not isinstance(value, str) or not value or len(value) > METADATA_MAX_LENGTH:
            raise ValueError(f"El campo {field} debe ser texto no vacío y corto.")
        if not SAFE_METADATA_RE.fullmatch(value):
            raise ValueError(f"El campo {field} contiene caracteres no permitidos.")

    timestamp = raw.get("Timestamp")
    if timestamp:
        calendar_timestamp = any(
            _valid_timestamp(timestamp, fmt) for fmt in TIMESTAMP_FORMATS
        )
        if not calendar_timestamp and not _valid_legacy_timestamp(timestamp):
            raise ValueError(
                "Timestamp debe usar fecha calendario o el formato legado con día del año."
            )
    return raw


def _valid_timestamp(value: str, fmt: str) -> bool:
    try:
        datetime.strptime(value, fmt)
        return True
    except ValueError:
        return False


def _valid_legacy_timestamp(value: str) -> bool:
    """Valida YYYY-MM-DDD asegurando que DDD pertenezca al mes declarado."""
    match = LEGACY_TIMESTAMP_RE.fullmatch(value)
    if not match:
        return False
    try:
        parsed = datetime.strptime(
            f"{match['year']}-{match['day_of_year']} {match['time']}",
            "%Y-%j %H:%M:%S",
        )
    except ValueError:
        return False
    return parsed.month == int(match["month"])
