#!/bin/sh
# Despliegue conservador: preserva siempre el volumen de producción existente.
set -eu

SCRIPT_DIR="$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)"
DEFAULT_APP_DIR="$(CDPATH= cd -- "$SCRIPT_DIR/.." && pwd)"
APP_DIR="${BIOVISION_APP_DIR:-$DEFAULT_APP_DIR}"
SEED_DB="$APP_DIR/weather_data.db"
cd "$APP_DIR"

PROXY_NETWORK="${PROXY_DOCKER_NETWORK:-}"
if [ -z "$PROXY_NETWORK" ] && [ -f "$APP_DIR/.env" ]; then
  PROXY_NETWORK="$(sed -n 's/^PROXY_DOCKER_NETWORK=//p' "$APP_DIR/.env" | tail -n 1)"
fi

case "$PROXY_NETWORK" in
  '') USE_PROXY_NETWORK=false ;;
  *[!A-Za-z0-9_.-]*)
    printf 'PROXY_DOCKER_NETWORK contiene caracteres no válidos; se usará la red interna.\n' >&2
    USE_PROXY_NETWORK=false
    ;;
  *)
    if docker network inspect "$PROXY_NETWORK" >/dev/null 2>&1; then
      USE_PROXY_NETWORK=true
      printf 'Usando red Docker externa del proxy: %s\n' "$PROXY_NETWORK"
    else
      printf 'La red Docker externa %s no existe; se usará la red interna.\n' "$PROXY_NETWORK" >&2
      USE_PROXY_NETWORK=false
    fi
    ;;
esac

if [ ! -f "$APP_DIR/.env" ]; then
  printf 'Falta %s/.env; copiar .env.example, protegerlo y configurarlo.\n' "$APP_DIR" >&2
  exit 2
fi

if [ ! -f "$SEED_DB" ]; then
  printf 'No existe la semilla requerida: %s\n' "$SEED_DB" >&2
  exit 2
fi

docker compose build weather-server
docker compose run --rm --no-deps --entrypoint python weather-server -m unittest discover -s backend/tests -v

# La condición se evalúa dentro del volumen. Una DB ya existente jamás se
# reemplaza, incluso si la semilla local es más nueva.
docker compose run --rm --no-deps \
  -v "$SEED_DB:/seed/weather_data.db:ro" \
  --entrypoint sh weather-server -ceu '
    if [ -e /data/weather_data.db ]; then
      echo "Base existente detectada: se conserva sin cambios."
      exit 0
    fi
    cp /seed/weather_data.db /data/weather_data.db
    chown 10001:10001 /data/weather_data.db
    echo "Base inicial sembrada en el volumen."
  '

docker compose run --rm --no-deps --entrypoint python weather-server -c \
  'import sqlite3,sys; c=sqlite3.connect("file:/data/weather_data.db?mode=ro", uri=True); r=c.execute("PRAGMA integrity_check").fetchone()[0]; print("DB integrity:", r); sys.exit(0 if r == "ok" else 1)'
docker compose up -d --no-deps weather-server

if [ "$USE_PROXY_NETWORK" = true ]; then
  if docker network inspect "$PROXY_NETWORK" --format '{{json .Containers}}' | grep -q '"weather-server"'; then
    printf 'weather-server ya está conectado a %s.\n' "$PROXY_NETWORK"
  else
    docker network connect "$PROXY_NETWORK" weather-server
    printf 'weather-server conectado a %s.\n' "$PROXY_NETWORK"
  fi
fi

docker compose ps weather-server
