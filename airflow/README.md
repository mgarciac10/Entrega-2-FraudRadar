# Airflow

**Responsable:** Luis Moreno · **Par:** Juan José Gómez

Airflow no transporta datos: dispara jobs batch de `spark/batch/`. Los trabajos de streaming siguen
independientes y continuos en Docker. Los DAGs usan `spark-submit` desde el contenedor del scheduler
hacia `spark://spark-master:7077`.

| DAG | Frecuencia | Tareas |
|---|---|---|
| `lakehouse_15min` | cada 15 min | `check_bronze_freshness` → `bronze_to_silver` → `extract_alert_status` → `build_dimensions` → `silver_to_gold` → `data_quality_checks` |
| `daily_batch` | 02:00 UTC | `check_catalog_inputs` → `ingest_catalogs` → `build_user_profile` → `optimize_and_vacuum` |

Configuración común: 2 reintentos con 2 min de espera, `max_active_runs=1`, `catchup=False`.
El sensor considera fresco Bronze si el último commit Delta tiene menos de 10 minutos. Si una tarea
falla, las siguientes quedan en `upstream_failed` y Gold no se publica con datos incompletos.

## Desarrollo local sin AWS

1. Copiar `.env.example` a `.env`; mantener `LAKE_URI=/data/lake`,
   `LANDING_URI=/data/catalogs`, `CHECKPOINT_URI=/data/checkpoints`,
   `ALERTS_SOURCE=local-json` y el fixture `tests/fixtures/alerts.json`.
2. Desde la raíz del repositorio, generar los catálogos que espera `daily_batch`:

   ```bash
   python producer/generate_catalogs.py --users 20000 --out data/catalogs
   ```

   El volumen `./data:/data` hace que esos mismos archivos estén disponibles en Airflow como
   `/data/catalogs/*.csv`. `tests/run_spark_checks.sh` usa `data/test_spark/` aislado y no genera
   estos catálogos de ejecución.
3. Con Docker Desktop activo, desde Git Bash en Windows:

   ```bash
   export MSYS_NO_PATHCONV=1
   docker compose --profile local --profile producer --profile spark --profile airflow up -d --build
   ```

   Compose configura permisos de grupo compartidos entre el driver Spark de Airflow y los
   ejecutores. Después de actualizar esta configuración, recrear los servicios afectados para
   aplicarla. El worker usa 2 GB por defecto para Docker Desktop; se puede aumentar si Docker tiene
   más memoria asignada. La imagen de Airflow usa Python 3.10, igual que los workers Spark; mantener
   la misma versión menor evita `PYTHON_VERSION_MISMATCH` en los workers PySpark.

   Si `data/lake` o `data/checkpoints` ya fueron creados por la configuración anterior, reparar sus
   permisos locales una vez:

   ```bash
   docker compose exec -u 0:0 spark-worker chmod -R g+rwX /data/lake /data/checkpoints
   docker compose exec -u 0:0 spark-worker sh -c 'find /data/lake /data/checkpoints -type d -exec chmod g+s {} +'
   ```

   Estos comandos solo modifican los volúmenes locales; no acceden a AWS.
4. En Airflow, habilitar y ejecutar primero `daily_batch`; su tarea `check_catalog_inputs` valida
   que estén los cuatro CSV antes de iniciar Spark. Luego iniciar el stream de Bronze y confirmar
   que ya escribe commits Delta recientes:

   ```bash
   docker compose --profile local --profile producer --profile spark --profile streaming up -d producer ingest-bronze
   docker compose --profile streaming logs --tail=100 ingest-bronze
   ```

   El sensor `check_bronze_freshness` espera hasta 10 minutos por un commit de los últimos 10 minutos.
   Si no aparece, comprobar que Kafka y `producer` estén activos y que `ingest-bronze` no esté fallando.
   El DAG no inicia esos servicios por sí mismo. Para probar tareas puntuales con
   `airflow tasks test`, esperar a que no haya un DAG run activo (o pausarlo): la prueba manual
   puede compartir los checkpoints con una ejecución del scheduler y causar conflictos. Después
   de corregir una ejecución fallida, dispara un nuevo DAG run en Airflow; el run anterior
   permanece fallido en el historial.
5. Solo después de que Bronze esté fresco, habilitar y ejecutar `lakehouse_15min`.
   `detect-fraud` no es necesario para ese DAG; iniciarlo únicamente después de que exista
   `silver/user_profile`:

   ```bash
   docker compose --profile local --profile producer --profile spark --profile airflow --profile streaming up -d detect-fraud
   ```

   La interfaz está en `http://localhost:8081`; iniciar sesión con `AIRFLOW_WWW_USER_USERNAME` y
   `AIRFLOW_WWW_USER_PASSWORD` del `.env`. En PowerShell, usar `docker compose` desde una terminal
   donde Docker Desktop esté en el `PATH`; en Git Bash mantener `MSYS_NO_PATHCONV=1`.

Airflow escribe su metastore en PostgreSQL local y sus DAGs solo usan JSON local para el estado de
alertas. No se hacen llamadas a AWS. La interfaz se publica solo en `127.0.0.1`.

### Reintentar el run de `lakehouse_15min`

El run `scheduled__2026-11-09T04:15:00+00:00` se completó correctamente después de reintentar
`extract_alert_status` con la imagen actual; las tres tareas posteriores también terminaron en
`success`. `daily_batch` se volvió a ejecutar y `optimize_and_vacuum` terminó en `success`. Esta
sección queda como procedimiento para una futura instancia que necesite reintento:

1. Deja el DAG pausado para evitar nuevos runs periódicos y abre Airflow en `http://localhost:8081`.
2. En **Grid**, abre el run de las 04:15 y selecciona solo la instancia de
   `extract_alert_status`; usa **Clear** para reintentarlo. No limpies las tareas previas que ya
   están en `success`. El scheduler volverá a ejecutar esa tarea y las posteriores.
3. Espera a que `build_dimensions`, `silver_to_gold` y `data_quality_checks` terminen en
   `success`. Si hay un error nuevo, leer el log de esa instancia antes de volver a limpiar.
4. En `daily_batch`, crea un DAG run nuevo (no reutilices los runs fallidos antiguos) y verifica
   `check_catalog_inputs`, `ingest_catalogs`, `build_user_profile` y `optimize_and_vacuum`.

La suite `bash tests/run_spark_checks.sh` también se ejecutó en local y terminó con `Todo bien`.

Si prefieres hacer una ejecución nueva de `lakehouse_15min`, primero vuelve a iniciar `producer` e
`ingest-bronze` y confirma que Bronze recibe commits recientes; después dispara un run manual. No
ejecutes `airflow tasks test` ni otra prueba con Spark mientras el scheduler tenga un DAG run activo.

## Ejecución en AWS

Para una sesión aprobada, configurar en `.env` el bucket y las rutas S3 (`S3_BUCKET` o
`LAKE_URI=s3a://<bucket>/lake`, `LANDING_URI=s3a://<bucket>/landing`,
`CHECKPOINT_URI=s3a://<bucket>/checkpoints`), `ALERTS_SOURCE=dynamodb`,
`KAFKA_BOOTSTRAP_SERVERS` con los brokers TLS de MSK, `KAFKA_SECURITY_PROTOCOL=SSL` y un
`STREAM_SESSION` nuevo. `ALERTS_SINK=dynamodb` es necesario para que el job de fraude escriba en la
tabla real. Las credenciales deben venir del rol de la EC2; no se guardan llaves en el repositorio.
En la EC2 de Spark, ajustar `SPARK_WORKER_MEMORY` y `SPARK_WORKER_CORES` a la capacidad asignada;
los valores `2g`/`2` del `.env.example` son conservadores para Docker Desktop. Para la EC2
`t3.xlarge` del diseño actual, volver a partir de `SPARK_WORKER_MEMORY=10g` y
`SPARK_WORKER_CORES=3`, y ajustar después de medir el uso real.

La infraestructura y el productor tienen scripts de sesión documentados en `infra/README.md`.
El despliegue y validación de extremo a extremo de Spark/Airflow en las EC2 aún no están verificados
y requieren preparar/publicar las imágenes y el código, probar MSK/S3/DynamoDB y correr ambos DAGs.
Después de escribir Gold en S3, registrar las tablas Glue con el comando de `sql/README.md` y validar
las consultas en Athena. No se usó AWS para las pruebas locales.
