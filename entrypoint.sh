#!/bin/sh
set -e

echo "Inicializando base de datos..."
python -c "from weather_server import init_db; init_db()"

echo "Iniciando servidor..."
exec gunicorn \
  --bind 0.0.0.0:${PORT:-3000} \
  --workers 1 \
  --threads 4 \
  --timeout 60 \
  --access-logfile - \
  --error-logfile - \
  weather_server:app