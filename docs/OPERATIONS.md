# Operación de BioVision en VPS

## Estados independientes

- `/health/live`: el proceso HTTP responde. Es la señal usada para detectar bloqueos.
- `/health/ready`: SQLite responde y existe la reserva mínima de disco.
- `/status/station`: informa `online`, `stale`, `no_data` o `unknown`. Una estación sin lecturas no vuelve *unhealthy* al backend.
- `/health`: compatibilidad con el panel; no revela rutas internas.

## Política de rutas en el proxy

Públicas de lectura: `/`, assets, `/weather/latest`, `/weather/range`, `/weather/export/csv`, `/weather/export/json`, `/health`, `/status/station` y `/socket.io`.

`POST /weather` debe restringirse por IP de origen. Si esto no es posible, definir `INGEST_API_KEY` y enviar el mismo valor en `X-Weather-Key` desde la estación.

Bloquear externamente `/health/live`, `/health/ready`, `/weather/raw`, `/weather/raw/db`, `/weather/count`, `/weather/devices` y cualquier ruta futura `/internal/*`. Las rutas diagnósticas están deshabilitadas además por aplicación.

Configurar en el proxy:

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
sudo BIOVISION_APP_DIR=/opt/biovision/current \
  BIOVISION_BACKUP_DIR=/srv/biovision/backups \
  sh /opt/biovision/current/ops/backup.sh
```

Programar un backup diario, copiarlo fuera de la VPS y probar restauración periódicamente. El script no elimina backups antiguos: la retención debe definirse explícitamente en el almacenamiento externo.

Antes de restaurar: detener ingreso, conservar una copia del volumen actual, verificar el checksum y probar `PRAGMA integrity_check`. No sobrescribir la base activa mientras el contenedor está escribiendo.

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
