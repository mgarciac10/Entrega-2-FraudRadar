# tests

Verificaciones de Spark (`bash tests/run_spark_checks.sh`): `ingest_catalogs`, `bronze_to_silver` (validación, rechazados, MERGE sin duplicados y relectura completa de Bronze), `build_user_profile` (mezcla con el perfil inicial, ventana de 30 días), `detect_fraud` (streaming real con archivos como fuente, en varios micro-batches) y `silver_to_gold` (dimensiones, hechos, idempotencia, linaje y chequeos de calidad). `spark_check_ingest_bronze.py` compara Kafka con Bronze y se usa en la prueba de matar el job.

Pruebas de las reglas de fraude y de las transformaciones, más las pruebas de requisitos no funcionales que se muestran en la demo:

- Matar un job de Spark y verificar que Silver y las alertas no cambian al reiniciar (exactly-once, RNF-03).
- 200 ev/s durante 10 min y pico de 1.000 ev/s (RNF-01).
- Latencia evento → alerta menor a 2 min (RNF-04).

## Pruebas existentes

| Archivo | Qué prueba | Cómo correrla |
|---|---|---|
| `test_catalogs.py` | Catálogos (integridad, determinismo), contrato del evento y umbrales | `python -m unittest tests.test_catalogs -v` (solo biblioteca estándar) |
| `test_producer.py` | Generador y simulador: contrato, cada ataque supera el umbral de su regla, proporción de fraude, tráfico normal | `python -m unittest tests.test_producer -v` (solo biblioteca estándar) |
| `test_fraud_rules.py` | Lógica de las 4 reglas (`spark/streaming/fraud_rules.py`) contra `reference_rules.py` con tráfico aleatorio, episodios (una ráfaga = una alerta) y estado serializado entre lotes | `python -m unittest tests.test_fraud_rules -v` (solo biblioteca estándar) |
| `test_analyst_alertas.py` | Herramienta del analista (`analyst/alertas.py`): listar, marcar sin crear alertas fantasma, resumen | `python -m unittest tests.test_analyst_alertas -v` (requiere `boto3` y `moto`; si falta moto se omite) |
| `test_notify_alert.py` | Lambda `notify_alert`: publica solo INSERT, asunto ASCII y lote parcial | `python -m unittest tests.test_notify_alert -v` (requiere `boto3`) |
| `test_register_gold_tables.py` | Conversión de tipos Delta a Athena y selección del commit Delta actual para Glue (sin conexión AWS) | `python -m unittest tests.test_register_gold_tables -v` |

`tests/reference_rules.py` es la implementación de **referencia** de las 4 reglas en Python puro: la especificación ejecutable de lo que debe detectar `detect_fraud`, para compararla con la versión en Spark.

**Integración continua:** `.github/workflows/checks.yml` corre en GitHub (pestaña *Actions*) las pruebas unitarias y `tests/run_spark_checks.sh` en cada push y pull request. Sirve a quien no tiene Docker en su computador: el resultado de las verificaciones de Spark queda en el repositorio.

Las pruebas de humo contra AWS están en `infra/scripts/` (`test_dynamodb.sh` y `test_notify.sh`).
