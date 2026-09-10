#!/bin/sh
# Despliegue conservador: preserva siempre el volumen de producción existente.
set -eu

SCRIPT_DIR="$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)"
DEFAULT_APP_DIR="$(CDPATH= cd -- "$SCRIPT_DIR/.." && pwd)"
APP_DIR="${BIOVISION_APP_DIR:-$DEFAULT_APP_DIR}"
SEED_DB="$APP_DIR/weather_data.db"
cd "$APP_DIR"

INITIALIZE_EMPTY_DB="${INITIALIZE_EMPTY_DATABASE:-}"
if [ -z "$INITIALIZE_EMPTY_DB" ] && [ -f "$APP_DIR/.env" ]; then
  INITIALIZE_EMPTY_DB="$(sed -n 's/^INITIALIZE_EMPTY_DATABASE=//p' "$APP_DIR/.env" | tail -n 1)"
fi
case "$INITIALIZE_EMPTY_DB" in
  true|false|'') : ;;
  *)
    printf 'INITIALIZE_EMPTY_DATABASE debe ser true o false.\n' >&2
    exit 2
    ;;
esac

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

docker compose build weather-server
docker compose run --rm --no-deps --entrypoint python weather-server -m unittest discover -s backend/tests -v

# La DB del volumen siempre prevalece. La semilla sólo se copia en el primer
# despliegue; una DB vacía sólo se crea con autorización explícita.
if docker compose run --rm --no-deps --entrypoint sh weather-server -ceu '[ -e /data/weather_data.db ]'; then
  printf 'Base existente detectada en el volumen: se conserva sin cambios.\n'
elif [ -f "$SEED_DB" ]; then
  docker compose run --rm --no-deps \
    -v "$SEED_DB:/seed/weather_data.db:ro" \
    --entrypoint sh weather-server -ceu '
      cp /seed/weather_data.db /data/weather_data.db
      chown 10001:10001 /data/weather_data.db
      echo "Base inicial sembrada en el volumen."
    '
elif [ "$INITIALIZE_EMPTY_DB" = true ]; then
  docker compose run --rm --no-deps --entrypoint python weather-server -c \
    'from backend.app.repositories.weather_readings import init_db; init_db(); print("Base vacía inicializada.")'
else
  printf 'No existe weather_data.db ni una DB en el volumen.\n' >&2
  printf 'Copie una base existente o defina INITIALIZE_EMPTY_DATABASE=true sólo para una VPS nueva.\n' >&2
  exit 2
fi

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
