#!/bin/sh
set -eu

APP_DIR="${BIOVISION_APP_DIR:-/opt/biovision/current}"
HEALTH_URL="${BIOVISION_HEALTH_URL:-http://127.0.0.1:3000/health/live}"
STATE_DIR="${BIOVISION_WATCHDOG_STATE_DIR:-/var/lib/biovision-watchdog}"
FAILURES_BEFORE_RESTART="${BIOVISION_FAILURES_BEFORE_RESTART:-3}"
COOLDOWN_SECONDS="${BIOVISION_RESTART_COOLDOWN_SECONDS:-600}"

FAILURE_FILE="$STATE_DIR/failures"
RESTART_FILE="$STATE_DIR/last_restart"
mkdir -p "$STATE_DIR"

log_message() {
  logger -t biovision-watchdog "$1"
  printf '%s\n' "$1"
}

if curl --fail --silent --show-error --max-time 5 "$HEALTH_URL" >/dev/null; then
  printf '0\n' > "$FAILURE_FILE"
  exit 0
fi

failures=0
if [ -f "$FAILURE_FILE" ]; then
  failures="$(cat "$FAILURE_FILE")"
fi
case "$failures" in
  *[!0-9]*|'') failures=0 ;;
esac
failures=$((failures + 1))
printf '%s\n' "$failures" > "$FAILURE_FILE"
log_message "liveness falló ($failures/$FAILURES_BEFORE_RESTART)"

if [ "$failures" -lt "$FAILURES_BEFORE_RESTART" ]; then
  exit 0
fi

now="$(date +%s)"
last_restart=0
if [ -f "$RESTART_FILE" ]; then
  last_restart="$(cat "$RESTART_FILE")"
fi
case "$last_restart" in
  *[!0-9]*|'') last_restart=0 ;;
esac

if [ $((now - last_restart)) -lt "$COOLDOWN_SECONDS" ]; then
  log_message "reinicio omitido por período de enfriamiento"
  exit 0
fi

log_message "reiniciando únicamente weather-server"
if (cd "$APP_DIR" && docker compose restart weather-server); then
  printf '%s\n' "$now" > "$RESTART_FILE"
  printf '0\n' > "$FAILURE_FILE"
  log_message "reinicio solicitado correctamente"
else
  log_message "falló el reinicio; se requiere intervención"
  exit 1
fi
