# ADR-03 · Delta Lake en S3 y modelo dimensional (constelación) en Gold

- **Estado:** vigente, con ajustes al ADR-03 del E1 (`time_key` en `fact_alert`, `is_fraud_injected` en `fact_transaction` y estado de alertas leído de DynamoDB).
- **Fecha:** 2026-11-05

## Contexto

- RNF-03 exige exactly-once hacia el almacenamiento, y RNF-06 exige auditar versiones y linaje.
- Las preguntas 5 a 7 son agregaciones por fecha, franja horaria, regla, ciudad, canal y comercio, sobre dos tipos de evento con grano distinto: transacciones y alertas.
- El estado del arte recomienda medir precisión y recall por regla (sección 2).

## Alternativas consideradas

- **Apache Iceberg.** Es equivalente en transacciones ACID y *time travel*, y tiene mejor soporte multimotor. Se descarta porque el curso trabajó Delta (S4) y su integración con Spark es más directa (MERGE, fuente y destino de streaming, `availableNow`).
- **Tabla ancha única en Gold.** Es más simple de consultar, pero repite los atributos de usuario y comercio en millones de filas y no admite dos granos distintos.
- **Dos esquemas estrella independientes.** No permiten comparar transacciones y alertas con los mismos filtros (*drill-across*) sin duplicar dimensiones.

## Decisión

Delta Lake en S3 para las tres capas, registrado en el Glue Data Catalog y consultado con Athena, que lee tablas Delta de forma nativa.

Gold es una **constelación**: dos tablas de hechos que comparten dimensiones conformadas.

| Hecho | Grano | Llaves | Medidas y atributos |
|---|---|---|---|
| `fact_transaction` | una transacción | fecha, hora, usuario, comercio, ubicación, canal | `amount_cop`, `is_flagged`, `is_fraud_injected`, `attack_id`, `kafka_session`, `kafka_partition`, `kafka_offset` |
| `fact_alert` | una alerta | fecha, hora, usuario, regla, ubicación | `amount_at_risk_cop`, `n_transactions`, `detection_latency_s`, `trigger_event_ts`, `transaction_ids`. Dimensión degenerada: `status` |

**Dimensiones:** `dim_date`, `dim_time`, `dim_user`, `dim_merchant`, `dim_location`, `dim_channel` y `dim_rule`. Las compartidas son fecha, hora, usuario y ubicación.

**Carga:**
- `extract_alert_status` copia DynamoDB a `silver.alert_status` (JSON local en desarrollo); Spark es el único escritor de Delta.
- `silver_to_gold` construye dimensiones tipo 1 con llaves SHA-256 determinísticas y carga los hechos con MERGE por llave de negocio. Reejecutar Airflow no duplica filas.
- `fact_transaction` se particiona por fecha e indica si la transacción aparece en los `transaction_ids` de una alerta.
- `fact_transaction` conserva sesión, partición y offset Kafka para reconstruir el linaje (RNF-06).
- `fact_alert` conserva `transaction_ids`, estado, latencia y la ubicación del evento que disparó la alerta para auditoría y consultas Q5–Q9.
- `data_quality_checks` valida unicidad, llaves foráneas, dominios y medidas antes de finalizar el DAG.
- `sql/athena/register_gold_tables.py` registra la versión Delta actual de las nueve tablas de Gold en Glue; Athena las consulta en modo lectura.

**Con `is_fraud_injected` y `attack_id`**, Gold calcula:
- La precisión y el recall de cada regla, cruzando las transacciones marcadas por el productor con las alertadas.
- El **fraude evitable**: de cada ataque, el monto posterior a la primera alerta (`trigger_event_ts`). Es la métrica de valor del proyecto, porque FraudRadar no detiene la primera transacción de un ataque pero sí corta los pasos siguientes (query Q8).

## Consecuencias

- **Más código en los jobs Gold.** Hay que mantener dimensiones y llaves sustitutas.
- **Sin historial en las dimensiones.** Son SCD tipo 1: no conservan el historial de cambios (por ejemplo, del nivel KYC).
- **Archivos pequeños.** Los micro-batches generan muchos archivos chicos, así que `optimize_and_vacuum` corre a diario.
- **Athena solo lee.** Todas las escrituras pasan por Spark.
- **`is_fraud_injected` solo existe con datos sintéticos.** Con datos reales se reemplazaría por las etiquetas tardías y el feedback del analista.
- **Estado desfasado.** El estado de las alertas en Gold puede ir hasta 15 min atrás de las decisiones del analista.
- **Etiquetas de prueba.** Q8 y Q9 dependen del `attack_id` y `attack_pattern` sintéticos; no representan una etiqueta disponible en producción en tiempo real.

## Cambio respecto al E1

- `fact_alert` gana la llave `time_key`. Sin ella, la pregunta 5 (franja horaria) no se podía responder.
- `fact_transaction` gana `is_fraud_injected` y `attack_id`, y `fact_alert` gana `trigger_event_ts`: decisiones del equipo tomadas a partir del estado del arte para medir precisión, recall y fraude evitable.
- El estado de las alertas viene de DynamoDB en lugar de MongoDB (ADR-04).

## Fuentes

- [`docs/estado_del_arte.md`](../estado_del_arte.md), sección 2 (medición de falsos positivos y negativos)
