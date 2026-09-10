#!/bin/sh
set -eu

APP_DIR="${BIOVISION_APP_DIR:-/opt/biovision/current}"
BACKUP_DIR="${BIOVISION_BACKUP_DIR:-/srv/biovision/backups}"
RETENTION_COUNT="${BIOVISION_BACKUP_RETENTION_COUNT:-7}"
backup_day="$(date -u +%Y%m%d)"
filename="weather_data_${backup_day}.db"
container_path="/data/.${filename}.tmp"
host_path="$BACKUP_DIR/$filename"
host_tmp="$BACKUP_DIR/.${filename}.tmp"

mkdir -p "$BACKUP_DIR"
case "$RETENTION_COUNT" in
  ''|*[!0-9]*) printf 'BIOVISION_BACKUP_RETENTION_COUNT debe ser entero positivo\n' >&2; exit 2 ;;
esac
[ "$RETENTION_COUNT" -ge 7 ] || { printf 'La retención mínima es 7 backups.\n' >&2; exit 2; }
cd "$APP_DIR"

cleanup() {
  docker compose exec -T --user appuser weather-server rm -f "$container_path" >/dev/null 2>&1 || true
  rm -f -- "$host_tmp" "$host_tmp.sha256"
}
trap cleanup EXIT INT TERM

docker compose exec -T --user appuser weather-server sqlite3 /data/weather_data.db ".backup '$container_path'"
integrity="$(docker compose exec -T --user appuser weather-server sqlite3 "$container_path" "PRAGMA integrity_check;")"
if [ "$integrity" != "ok" ]; then
  printf 'Backup inválido: %s\n' "$integrity" >&2
  exit 1
fi

docker compose cp "weather-server:$container_path" "$host_tmp"
checksum="$(sha256sum "$host_tmp" | awk '{print $1}')"
printf '%s  %s\n' "$checksum" "$filename" > "$host_tmp.sha256"
mv -f -- "$host_tmp" "$host_path"
mv -f -- "$host_tmp.sha256" "$host_path.sha256"
# Conserva las copias verificadas más recientes; nunca toca otros archivos.
old_backups="$(find "$BACKUP_DIR" -maxdepth 1 -type f -name 'weather_data_*.db' -printf '%T@ %p\n' | sort -nr | awk -v keep="$RETENTION_COUNT" 'NR > keep {sub(/^[^ ]+ /, ""); print}')"
if [ -n "$old_backups" ]; then
  while IFS= read -r old_backup; do
    [ -n "$old_backup" ] || continue
    rm -f -- "$old_backup" "$old_backup.sha256"
  done <<EOF
$old_backups
EOF
fi
printf 'Backup verificado: %s\n' "$host_path"
