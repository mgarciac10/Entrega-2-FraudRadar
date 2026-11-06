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
| `fact_transaction` | una transacción | fecha, hora, usuario, comercio, ubicación, canal | `amount_cop`, `is_flagged`, `is_fraud_injected` |
| `fact_alert` | una alerta | fecha, hora, usuario, regla, ubicación | `amount_at_risk_cop`, `n_transactions`, `detection_latency_s`. Dimensión degenerada: `status` |

**Dimensiones:** `dim_date`, `dim_time`, `dim_user`, `dim_merchant`, `dim_location`, `dim_channel` y `dim_rule`. Las compartidas son fecha, hora, usuario y ubicación.

**Carga:**
- `silver_to_gold` carga Gold con MERGE por llave de negocio. Reejecutar la misma corrida de Airflow no duplica filas.
- El estado de cada alerta se lee de DynamoDB.
- `fact_transaction` se particiona por fecha.

**Con `is_fraud_injected`**, Gold calcula la precisión y el recall de cada regla, cruzando las transacciones marcadas por el productor con las alertadas.

## Consecuencias

- **Más código en `silver_to_gold`.** Hay que mantener dimensiones y llaves sustitutas.
- **Sin historial en las dimensiones.** Son SCD tipo 1: no conservan el historial de cambios (por ejemplo, del nivel KYC).
- **Archivos pequeños.** Los micro-batches generan muchos archivos chicos, así que `optimize_and_vacuum` corre a diario.
- **Athena solo lee.** Todas las escrituras pasan por Spark.
- **`is_fraud_injected` solo existe con datos sintéticos.** Con datos reales se reemplazaría por las etiquetas tardías y el feedback del analista.
- **Estado desfasado.** El estado de las alertas en Gold puede ir hasta 15 min atrás de las decisiones del analista.

## Cambio respecto al E1

- `fact_alert` gana la llave `time_key`. Sin ella, la pregunta 5 (franja horaria) no se podía responder.
- `fact_transaction` gana `is_fraud_injected`, una decisión del equipo tomada a partir del estado del arte.
- El estado de las alertas viene de DynamoDB en lugar de MongoDB (ADR-04).

## Fuentes

- [`docs/estado_del_arte.md`](../estado_del_arte.md), sección 2 (medición de falsos positivos y negativos)
