# Operación de BioVision en VPS

## Estados independientes

- `/health/live`: el proceso HTTP responde. Es la señal usada para detectar bloqueos.
- `/health/ready`: SQLite responde y existe la reserva mínima de disco.
- `/status/station`: informa `online`, `stale`, `no_data` o `unknown`. Una estación sin lecturas no vuelve *unhealthy* al backend.
- `/health`: compatibilidad con el panel; no revela rutas internas.

## Política de rutas en el proxy

Públicas de lectura: `/`, assets, `/weather/latest`, `/weather/range`, `/weather/export/csv`, `/weather/export/json`, `/health`, `/status/station` y `/socket.io`.

La estación no necesita agregar credenciales ni modificar su payload. Si en el
futuro dispone de una IP pública estable, se puede habilitar la capa adicional
`INGEST_ALLOWED_IPS` con una IP o CIDR, por ejemplo
`INGEST_ALLOWED_IPS=203.0.113.42/32`. Dejarla vacía permite estaciones con IP
dinámica.

Cuando no hay IP estable ni credencial posible, no se puede autenticar el
origen del POST: un tercero podría imitar el JSON. En ese caso el proxy debe
aplicar límite de tasa específico a `POST /weather`, cuerpo máximo de 64 KiB y
TLS. La ingesta acepta únicamente `Content-Type: application/json` y el
contrato controlado de estación: canales `""`, `ch0` a `ch4` (todos numéricos) y,
opcionalmente, `DeviceID`, `DeviceType`, `DeviceVersion` y `Timestamp`. Los
canales futuros con forma `chN` se validan como numéricos y se conservan en el
JSON original, aunque el panel sólo normaliza `ch0` a `ch4`. Se rechazan otros
campos extra, JSON duplicado, arreglos/objetos, valores no finitos,
texto no numérico y contenido que intente usar el endpoint como subida de
archivos o scripts. El payload original aceptado se conserva sin transformarlo
en `raw_json`. `Timestamp` admite fecha calendario (`YYYY-MM-DD`) y el formato
legado del datalogger (`YYYY-MM-DDD`, donde `DDD` es el día del año); en este
último se valida que el día del año pertenezca al mes declarado.

La CSP mantiene scripts exclusivamente en el mismo origen. `style-src` permite
estilos inline porque Boneyard calcula en ejecución la geometría responsive de
sus esqueletos; no se permite código inline ni orígenes externos.

Bloquear externamente `/health/live`, `/health/ready`, `/weather/raw`, `/weather/raw/db`, `/weather/count`, `/weather/devices` y cualquier ruta futura `/internal/*`. Las rutas diagnósticas están deshabilitadas además por aplicación.

Configurar en el proxy. Si se ejecuta directamente en el host, usar `BIND_ADDRESS=127.0.0.1`; si vive en otro contenedor, usar una red Docker compartida o mantener `BIND_ADDRESS=0.0.0.0` y proteger el puerto con el firewall:

Para Nginx Proxy Manager en Docker, definir opcionalmente
`PROXY_DOCKER_NETWORK` en `.env` con el nombre mostrado por `docker network
ls`. `ops/deploy.sh` valida que exista y conecta el contenedor
`weather-server` a ella después de levantarlo; si no existe o queda vacía,
funciona con la red interna sin requerir cambios ni otro archivo Compose. En
NPM, usar `weather-server` como host de destino y `3000` como puerto sólo
cuando la red externa se haya conectado. De otro modo, si NPM corre en el host,
usar `127.0.0.1:3000`.

- upstream `127.0.0.1:3000`;
- WebSocket para `/socket.io`;
- cuerpo máximo de 64 KiB en `POST /weather`;
- límites de frecuencia para `/weather/range` y `/weather/export/*`;
- timeouts de conexión y respuesta;
- propagación de `X-Request-ID`;
- TLS, HSTS y logs de acceso con rotación.

## Recuperación automática

Docker reinicia el contenedor cuando el proceso termina. El watchdog cubre el caso diferente en que el proceso sigue vivo pero no responde:

1. consulta liveness cada 30 segundos;
2. exige tres fallos consecutivos;
3. reinicia únicamente `weather-server`;
4. aplica 10 minutos de enfriamiento;
5. deja evidencia en journald mediante la etiqueta `biovision-watchdog`.

Instalación del timer:

```sh
sudo install -m 0644 ops/watchdog.sh /opt/biovision/current/ops/watchdog.sh
sudo install -m 0644 ops/systemd/biovision-watchdog.service /etc/systemd/system/
sudo install -m 0644 ops/systemd/biovision-watchdog.timer /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now biovision-watchdog.timer
```

Ajustar `BIOVISION_APP_DIR` mediante un override de systemd si el repositorio vive en otra ubicación. No usar simultáneamente otro sistema que también reinicie el mismo contenedor por healthcheck.

El watchdog nunca debe reiniciar por `status/station=stale`. Tampoco debe repetir reinicios ante disco lleno o corrupción; esos incidentes requieren liberar espacio o restaurar un backup.

## Backups

`ops/backup.sh` usa la API `.backup` de SQLite, comprueba integridad y genera SHA-256. El destino predeterminado es `/srv/biovision/backups`.

El volumen tiene el nombre estable `aws_longavi_weather-data` por defecto. `DATA_VOLUME_NAME` permite cambiarlo de forma explícita, pero no debe modificarse durante una actualización ordinaria: otro nombre conecta un volumen distinto y la aplicación parecerá no tener datos.

```sh
sudo BIOVISION_BACKUP_DIR=/srv/biovision/backups sh ops/backup.sh
```

Programar un backup diario, copiarlo fuera de la VPS y probar restauración periódicamente. El script mantiene una copia verificada por día UTC y conserva los siete días más recientes (configurable con `BIOVISION_BACKUP_RETENTION_COUNT`, nunca menor a 7), junto a sus checksums; una ejecución repetida el mismo día reemplaza atómicamente sólo esa copia diaria. Mantener además una copia externa de la VPS.

Instalación del timer diario incluido:

```sh
sudo install -m 0644 ops/systemd/biovision-backup.service /etc/systemd/system/
sudo install -m 0644 ops/systemd/biovision-backup.timer /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now biovision-backup.timer
sudo systemctl list-timers biovision-backup.timer
```

Los scripts detectan la raíz del repositorio al ejecutarse desde cualquier
ubicación. Para systemd, si la aplicación o los respaldos están en otra ruta,
crear un override del servicio para `BIOVISION_APP_DIR` y
`BIOVISION_BACKUP_DIR`.

Antes de restaurar: detener ingreso, conservar una copia del volumen actual, verificar el checksum y probar `PRAGMA integrity_check`. No sobrescribir la base activa mientras el contenedor está escribiendo.

## Calidad y consulta administrativa

La política mínima es: validar el contrato antes de persistir, no modificar lecturas históricas desde la aplicación, ejecutar comprobación de integridad y conteos diariamente, y revisar el informe cuando existan canales faltantes, JSON malformado o una brecha inesperada de lecturas. El administrador puede ejecutar el informe estrictamente de solo lectura:

```sh
sudo BIOVISION_APP_DIR=/opt/biovision/current sh /opt/biovision/current/ops/data-quality.sh
```

Para una consulta puntual, usar siempre modo solo lectura: `docker compose exec -T --user appuser weather-server sqlite3 -readonly /data/weather_data.db`. Antes de una restauración, detener la ingesta, respaldar el volumen actual y verificar `PRAGMA integrity_check`; nunca reemplazar la base activa mientras recibe lecturas.

## Siembra segura y despliegue

El volumen Docker es la fuente de datos en producción. La copia
`weather_data.db` se usa solamente para el primer despliegue. `ops/deploy.sh`
conserva siempre `/data/weather_data.db` si ya existe; si falta, usa la semilla
local. En una VPS totalmente nueva se puede definir
`INITIALIZE_EMPTY_DATABASE=true` para crear una base con el esquema vacío. Esta
opción no recupera lecturas históricas y no debe usarse como reemplazo de un
respaldo.

```sh
sudo DATA_VOLUME_NAME=aws_longavi_weather-data sh ops/deploy.sh
```

El script construye, ejecuta los tests y recrea sólo `weather-server`. Definir `DATA_VOLUME_NAME` si se cambió del nombre por defecto. Antes de actualizar, ejecutar un backup verificado.

## Alertas mínimas

- liveness o disponibilidad HTTPS fallando por 90 segundos;
- cualquier OOM o reinicio inesperado;
- más de un reinicio en 15 minutos;
- errores 5xx superiores al 2% durante 5 minutos;
- latencia p95 superior a 1 segundo;
- memoria superior al 80% durante 10 minutos;
- disco al 75% (advertencia) y 85% (crítico);
- backup con más de 25 horas;
- crecimiento anormal del archivo SQLite;
- estación sin datos durante varios intervalos, como alerta separada.

El monitor de disponibilidad debe ejecutarse fuera de esta VPS. El monitor del host debe recopilar CPU, memoria, disco, inodos, OOM, estado Docker y reinicios.

## Actualizaciones y rollback

1. Ejecutar backup verificado.
2. Construir la nueva imagen y ejecutar tests.
3. Etiquetar la imagen con el commit o versión.
4. Recrear únicamente `weather-server`.
5. Verificar liveness, readiness, carga del panel y una lectura real.
6. Ante fallo, volver a la etiqueta anterior sin reemplazar el volumen.

Nunca usar `docker compose down -v` ni eliminar el volumen durante despliegues ordinarios.
