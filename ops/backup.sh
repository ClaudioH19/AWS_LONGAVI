#!/bin/sh
set -eu

APP_DIR="${BIOVISION_APP_DIR:-/opt/biovision/current}"
BACKUP_DIR="${BIOVISION_BACKUP_DIR:-/srv/biovision/backups}"
timestamp="$(date -u +%Y%m%dT%H%M%SZ)"
filename="weather_data_${timestamp}.db"
container_path="/data/.${filename}.tmp"
host_path="$BACKUP_DIR/$filename"

mkdir -p "$BACKUP_DIR"
cd "$APP_DIR"

cleanup() {
  docker compose exec -T --user appuser weather-server rm -f "$container_path" >/dev/null 2>&1 || true
}
trap cleanup EXIT INT TERM

docker compose exec -T --user appuser weather-server sqlite3 /data/weather_data.db ".backup '$container_path'"
integrity="$(docker compose exec -T --user appuser weather-server sqlite3 "$container_path" "PRAGMA integrity_check;")"
if [ "$integrity" != "ok" ]; then
  printf 'Backup inválido: %s\n' "$integrity" >&2
  exit 1
fi

docker compose cp "weather-server:$container_path" "$host_path"
sha256sum "$host_path" > "$host_path.sha256"
printf 'Backup verificado: %s\n' "$host_path"
