# spark

Jobs de Spark 3.5 con Delta Lake. Corren en contenedores sobre EC2.

## streaming/ (continuos, `restart: always`)

| Job | Responsable | Entrada → salida | Qué hace |
|---|---|---|---|
| `ingest_bronze.py` | Juan José Gómez | MSK → `bronze.transactions` | Append crudo con topic, partición y offset. Trigger 15 s, exactly-once con checkpoint en S3 |
| `detect_fraud.py` | Juan José Vargas | MSK → DynamoDB `fraud_alerts` | Deduplica por `transaction_id` (watermark 10 min), join con `silver.user_profile`, ventanas de 5/10/30 min por usuario y 4 reglas. `foreachBatch` con escritura condicional por `alert_id` |

## batch/ (disparados por Airflow)

| Job | Responsable | Frecuencia | Qué hace |
|---|---|---|---|
| `ingest_catalogs.py` | Mateo García | diario | CSV de `landing/` → `bronze.catalogs` |
| `bronze_to_silver.py` | Juan José Gómez | 15 min | Lectura incremental de Bronze (`trigger(availableNow=True)`), valida esquema, deduplica con MERGE, enriquece con usuario y comercio, rechazados a `silver.transactions_rejected` |
| `build_user_profile.py` | Juan José Vargas | diario | Promedio y número de transacciones de los últimos 30 días por usuario → `silver.user_profile` |
| `silver_to_gold.py` | Luis Moreno | 15 min | Dimensiones y MERGE de `fact_transaction` (con `is_fraud_injected` y `attack_id`) y `fact_alert` (con `trigger_event_ts`; estado de alertas leído de DynamoDB) |

### Cómo correr los jobs de Vargas

- **`build_user_profile.py`** (`spark-submit ... build_user_profile.py [--as-of 2026-11-08T00:00:00] [--run-id <id>]`): sin `--as-of` usa la hora actual; con la misma fecha da el mismo resultado. Lee `silver/transactions` y `bronze/catalogs/perfil_inicial` y reescribe `silver/user_profile` completo (`user_id`, `avg_amount_30d`, `n_tx_30d`, `profile_source` = real | mezcla | inicial, `as_of`).
- **`detect_fraud.py`** (argumentos `--session`, `--trigger`, `--starting-offsets`, `--sink`; variables `STREAM_SESSION`, `DETECT_TRIGGER`, `ALERTS_SINK` = dynamodb | console, `ALERTS_TABLE`, `DYNAMODB_ENDPOINT_URL`): se detiene si no existe `silver/user_profile`. Una ráfaga o un monto acumulado que se sostiene es **una** alerta (el `window_start` queda fijo en el primer disparo). La lógica de las reglas está en `fraud_rules.py` (Python puro) y es la que prueba `tests/test_fraud_rules.py` contra `tests/reference_rules.py`.
- **Orden:** `ingest_catalogs` → `bronze_to_silver` → `build_user_profile` → `detect_fraud`. La imagen necesita `pandas` y `pyarrow` (estado por usuario con `applyInPandasWithState`): hay que reconstruirla.
- **Herramienta del analista:** `analyst/alertas.py` (`listar`, `abiertas`, `marcar`, `resumen`), ver el ADR-04.

## common/

Esquemas (evento JSON, Silver, alerta) y configuración compartida. Los esquemas son el contrato entre integrantes: cambiarlos requiere avisar al equipo.

## Cómo trabajar un job (patrón de `ingest_catalogs`)

- **Sesión y rutas:** todo job usa `spark_session.get_spark()` y `lake_path()`. Las ubicaciones salen de `LAKE_URI`, `LANDING_URI` y `CHECKPOINT_URI` (o de `S3_BUCKET`), así el mismo código corre en AWS (`s3a://...`) y en local (`/data/...`).
- **Imagen:** `spark/Dockerfile` trae Spark 3.5.3, Delta 3.2.1, S3A y el conector de Kafka ya descargados. El código se monta (no se copia): editar un job no obliga a reconstruir.
- **Clúster local:** `docker compose --profile spark up -d --build` (master + worker; UI en http://localhost:8080). Los jobs se lanzan con `spark-submit --master spark://spark-master:7077`. Probado con `ingest_catalogs`: la aplicación termina `FINISHED` y escribe las 4 tablas Delta.
- **Prueba de un job:** `tests/run_spark_checks.sh` (ejemplo para `ingest_catalogs`: filas, linaje, tipos, idempotencia). Cada job nuevo debería traer su verificación equivalente.
- **Idempotencia:** `ingest_catalogs` usa `replaceWhere` por `_run_id`: un reintento de Airflow reemplaza solo su corrida. Los jobs de streaming se apoyan en el checkpoint, y los de Silver y Gold en `MERGE`.

### Trampas conocidas

- **Git Bash en Windows convierte rutas:** `/opt/spark/...` y variables como `LAKE_URI=/data/lake` se vuelven `C:/Program Files/Git/...`. Hay que activar `MSYS_NO_PATHCONV=1` antes de `docker compose up` **y** de `exec`: las variables se fijan al crear los contenedores, así que si se exportaron sin la bandera hay que recrearlos. El síntoma es `No FileSystem for scheme "C"`.
- **Delta en disco local** falla con `ChecksumException` si se usa el sistema de archivos local por defecto; `get_spark()` ya fija `RawLocalFileSystem`. En S3 no aplica.
- **Credenciales en la EC2:** vienen del rol de la instancia. Los contenedores están un salto detrás del host, así que la plantilla de la sesión fija `HttpPutResponseHopLimit: 2`; con el valor por defecto (1) Spark no leería S3 ni DynamoDB. **Esto aún no se probó en una EC2 real.**
- **Checkpoints de streaming:** al recrear MSK los offsets del checkpoint ya no existen. Usar una ruta de checkpoint nueva por sesión.
- **Memoria del worker:** por defecto 10 GB (pensado para la EC2 de 16 GB). En un portátil, `SPARK_WORKER_MEMORY=2g SPARK_WORKER_CORES=2`.

