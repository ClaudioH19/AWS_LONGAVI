# BioVision

Panel público de monitoreo para una estación meteorológica. El backend Flask recibe y persiste lecturas en SQLite; el frontend React presenta estado, gráficos, histórico y exportaciones.

## Desarrollo y verificación

```sh
docker compose build weather-server
docker compose up -d
docker compose ps
```

El servicio se publica por defecto en `0.0.0.0:3000`, compatible con acceso directo y con proxies ejecutados en otro contenedor. Si el proxy HTTPS de la VPS se ejecuta directamente en el host, puede limitarse a loopback definiendo `BIND_ADDRESS=127.0.0.1` en `.env`.

Comprobaciones principales:

```sh
docker compose run --rm --entrypoint python weather-server -m unittest discover -s backend/tests -v
curl --fail http://127.0.0.1:3000/health/live
curl --fail http://127.0.0.1:3000/health/ready
```

La base se almacena en el volumen Docker estable `aws_longavi_weather-data`; no forma parte de la imagen. Nunca ejecutar `docker compose down -v` en producción.

La guía completa de despliegue, rutas, alertas, backups y recuperación está en [docs/OPERATIONS.md](docs/OPERATIONS.md).
