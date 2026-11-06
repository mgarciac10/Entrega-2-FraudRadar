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
| `silver_to_gold.py` | Luis Moreno | 15 min | Dimensiones y MERGE de `fact_transaction` (con `is_fraud_injected`) y `fact_alert` (estado de alertas leído de DynamoDB) |

## common/

Esquemas (evento JSON, Silver, alerta) y configuración compartida. Los esquemas son el contrato entre integrantes: cambiarlos requiere avisar al equipo.
