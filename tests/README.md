# tests

Verificaciones de Spark (`bash tests/run_spark_checks.sh`): `ingest_catalogs` y `bronze_to_silver` (validación, rechazados, MERGE sin duplicados y relectura completa de Bronze). `spark_check_ingest_bronze.py` compara Kafka con Bronze y se usa en la prueba de matar el job.

Pruebas de las reglas de fraude y de las transformaciones, más las pruebas de requisitos no funcionales que se muestran en la demo:

- Matar un job de Spark y verificar que Silver y las alertas no cambian al reiniciar (exactly-once, RNF-03).
- 200 ev/s durante 10 min y pico de 1.000 ev/s (RNF-01).
- Latencia evento → alerta menor a 2 min (RNF-04).

## Pruebas existentes

| Archivo | Qué prueba | Cómo correrla |
|---|---|---|
| `test_catalogs.py` | Catálogos (integridad, determinismo), contrato del evento y umbrales | `python -m unittest tests.test_catalogs -v` (solo biblioteca estándar) |
| `test_producer.py` | Generador y simulador: contrato, cada ataque supera el umbral de su regla, proporción de fraude, tráfico normal | `python -m unittest tests.test_producer -v` (solo biblioteca estándar) |
| `test_notify_alert.py` | Lambda `notify_alert`: publica solo INSERT, asunto ASCII y lote parcial | `python -m unittest tests.test_notify_alert -v` (requiere `boto3`) |

`tests/reference_rules.py` es la implementación de **referencia** de las 4 reglas en Python puro: la especificación ejecutable de lo que debe detectar `detect_fraud`, para compararla con la versión en Spark.

Las pruebas de humo contra AWS están en `infra/scripts/` (`test_dynamodb.sh` y `test_notify.sh`).
