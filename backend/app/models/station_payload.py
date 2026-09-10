"""Contrato inmutable del JSON emitido por la estación meteorológica.

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
ALLOWED_FIELDS = frozenset((*TELEMETRY_FIELDS, *OPTIONAL_METADATA_FIELDS))

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
# IDs/versiones solo admiten caracteres de identificación habituales. Esto
# excluye HTML, comillas, controles y prefijos de fórmula al exportar CSV.
SAFE_METADATA_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:/ -]*$")
TIMESTAMP_FORMATS = ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S")


def validate_station_payload(raw: object) -> dict:
    """Valida el contrato exacto de la estación y retorna el payload tipado.

    El datalogger puede serializar canales como números o texto numérico. Los
    nombres y la cantidad de canales, en cambio, son fijos para impedir que un
    POST sea usado como almacén genérico de JSON, HTML o scripts.
    """
    if not isinstance(raw, dict) or not raw:
        raise ValueError("El payload debe ser un objeto JSON no vacío.")

    unexpected = set(raw).difference(ALLOWED_FIELDS)
    if unexpected:
        raise ValueError("El payload contiene campos no permitidos.")
    missing = set(TELEMETRY_FIELDS).difference(raw)
    if missing:
        raise ValueError("El payload no contiene todos los canales de la estación.")

    for field in TELEMETRY_FIELDS:
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
        if not any(_valid_timestamp(timestamp, fmt) for fmt in TIMESTAMP_FORMATS):
            raise ValueError("Timestamp debe usar YYYY-MM-DD HH:MM:SS.")
    return raw


def _valid_timestamp(value: str, fmt: str) -> bool:
    try:
        datetime.strptime(value, fmt)
        return True
    except ValueError:
        return False
