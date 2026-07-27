#!/bin/sh
set -e

echo "Inicializando base de datos..."
python -c "from backend.app.repositories.weather_readings import init_db; init_db()"

echo "Iniciando servidor..."
exec gunicorn \
  --bind 0.0.0.0:${PORT:-3000} \
  --worker-class gthread \
  --workers 1 \
  --threads 100 \
  --timeout 60 \
  --access-logfile - \
  --error-logfile - \
  backend.app.main:app
