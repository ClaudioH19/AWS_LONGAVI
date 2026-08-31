#!/bin/sh
set -e

if [ "$(id -u)" = "0" ]; then
  chown appuser:appuser /data
  find /data -maxdepth 1 -type f -name 'weather_data.db*' -exec chown appuser:appuser {} \;
  RUN_AS="gosu appuser"
else
  RUN_AS=""
fi

echo "Inicializando base de datos..."
$RUN_AS python -c "from backend.app.repositories.weather_readings import init_db; init_db()"

echo "Iniciando servidor..."
exec $RUN_AS gunicorn \
  --bind 0.0.0.0:${PORT:-3000} \
  --worker-class gthread \
  --workers 1 \
  --threads ${GUNICORN_THREADS:-50} \
  --timeout ${GUNICORN_TIMEOUT:-45} \
  --graceful-timeout ${GUNICORN_GRACEFUL_TIMEOUT:-30} \
  --keep-alive 5 \
  --max-requests ${GUNICORN_MAX_REQUESTS:-2000} \
  --max-requests-jitter ${GUNICORN_MAX_REQUESTS_JITTER:-200} \
  --limit-request-line 4094 \
  --limit-request-fields 50 \
  --limit-request-field_size 8190 \
  --worker-tmp-dir /tmp \
  --no-control-socket \
  --access-logfile - \
  --error-logfile - \
  backend.app.main:app
