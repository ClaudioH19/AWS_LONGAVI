#!/bin/sh
# Informe de solo lectura para ejecutar por un administrador de la VPS.
set -eu

APP_DIR="${BIOVISION_APP_DIR:-/opt/biovision/current}"
cd "$APP_DIR"

docker compose exec -T --user appuser weather-server sqlite3 -readonly /data/weather_data.db <<'SQL'
.mode column
.headers on
PRAGMA integrity_check;
SELECT COUNT(*) AS total_readings, MIN(received_at) AS first_received_at, MAX(received_at) AS last_received_at FROM weather_readings;
SELECT COUNT(*) AS malformed_json FROM weather_readings WHERE NOT json_valid(raw_json);
SELECT COUNT(*) AS missing_station_channels FROM weather_readings
WHERE json_valid(raw_json) AND (json_type(raw_json, '$.""') IS NULL OR json_type(raw_json, '$.ch0') IS NULL OR json_type(raw_json, '$.ch1') IS NULL OR json_type(raw_json, '$.ch2') IS NULL OR json_type(raw_json, '$.ch3') IS NULL OR json_type(raw_json, '$.ch4') IS NULL);
SELECT substr(received_at, 1, 10) AS day, COUNT(*) AS readings FROM weather_readings GROUP BY day ORDER BY day DESC LIMIT 14;
SQL
