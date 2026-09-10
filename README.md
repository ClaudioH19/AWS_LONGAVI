# BioVision

Panel público de monitoreo para una estación meteorológica. El backend Flask recibe y persiste lecturas en SQLite; el frontend React presenta estado, gráficos, histórico y exportaciones.

## Despliegue en VPS

La base real `weather_data.db` se conserva en la raíz como semilla del primer
despliegue. No la renombres ni la sustituyas. Copia `.env.example` a `.env`,
ajusta recursos. La estación puede enviar su payload JSON sin modificarlo ni
agregar encabezados. `INGEST_ALLOWED_IPS` es opcional y sólo se usa si más
adelante se dispone de una IP pública estable.

```sh
cp .env.example .env
chmod 600 .env
# editar .env y ajustar recursos/red si corresponde
sudo sh ops/deploy.sh
```

`ops/deploy.sh` detecta automáticamente la raíz del repositorio, crea el
volumen, copia `weather_data.db` sólo si el volumen aún
no tiene una base, construye la imagen, ejecuta las pruebas y deja el servicio
activo. Nunca sobrescribe una base existente. No ejecutar `docker compose down
-v` en producción.

Para una VPS nueva sin datos históricos, definir
`INITIALIZE_EMPTY_DATABASE=true` en `.env`; el script creará el esquema SQLite
vacío. Para restaurar o preservar datos, dejarlo en `false` y copiar antes una
`weather_data.db` válida.

Si Nginx Proxy Manager corre en Docker en la misma VPS, obtener su red con
`docker network ls` y definirla en `.env`, por ejemplo
`PROXY_DOCKER_NETWORK=npm_default`. El despliegue la usa sólo si existe; vacía
o inexistente no interrumpe el despliegue y conserva la red Docker interna. No
se requiere un segundo archivo Compose: el script conecta el contenedor ya
levantado a la red externa cuando corresponde.

## Desarrollo y verificación

```sh
docker compose build weather-server
docker compose run --rm --no-deps --entrypoint python weather-server -m unittest discover -s backend/tests -v
docker compose up -d weather-server
docker compose ps
```

El servicio se publica de forma segura en `127.0.0.1:3000` para un proxy HTTPS
ejecutado en el host. Usar `BIND_ADDRESS=0.0.0.0` sólo si el proxy vive en otro
contenedor y el firewall impide acceso directo al puerto.

Comprobaciones principales:

```sh
curl --fail http://127.0.0.1:3000/health/live
curl --fail http://127.0.0.1:3000/health/ready
```

La base se almacena en el volumen Docker estable `aws_longavi_weather-data`; no forma parte de la imagen. Nunca ejecutar `docker compose down -v` en producción.

La guía completa de despliegue, rutas, seguridad, consulta administrativa,
calidad, backups diarios y recuperación está en
[docs/OPERATIONS.md](docs/OPERATIONS.md).
