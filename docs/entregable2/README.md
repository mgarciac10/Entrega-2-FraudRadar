# Entregable 2 · FraudRadar

Índice de todo lo que pide la guía del proyecto final (secciones 5 y 9) para el E2, con el enlace a
cada elemento y a la evidencia de cada criterio de la rúbrica. Estado al 2026-11-10.

## Checklist de la guía (sección 9)

| Elemento | Dónde está | Estado |
|---|---|---|
| README con instrucciones para correr el pipeline desde cero | [`README.md`](../../README.md) | Listo |
| `docker-compose.yml` en la raíz | [`docker-compose.yml`](../../docker-compose.yml) | Listo |
| Bitácora de delegación de agentes de IA | [`docs/bitacora_ia.md`](../bitacora_ia.md) | Al día (filas 1 a 28); falta que el equipo complete la columna "Decisión del equipo" |
| Slides de la presentación (máximo 15) | `docs/entregable2/` (este directorio) | Pendiente |
| El pipeline corre antes de S16 (ensayo obligatorio el día anterior) | Corrida completa en AWS el 2026-11-10 ([README §6](../../README.md)) | Corrió; falta el ensayo |
| Cada integrante puede responder sobre cualquier parte | [`docs/expo.md`](../expo.md), [ADRs](../adr/README.md) | Preparación individual |

## Presentación y demo (S16: 10 + 8 + 7 minutos)

| Elemento | Dónde está |
|---|---|
| Slides | `docs/entregable2/` (pendiente) |
| Diagrama de arquitectura final | [`docs/arquitectura/arquitectura.png`](../arquitectura/arquitectura.png) ([editable](../arquitectura/arquitectura.drawio)) |
| Puntos de la exposición y números medidos | [`docs/expo.md`](../expo.md) (sección 7: mediciones en AWS) |
| Cambios frente al E1 y su justificación (criterio "Reflexión sobre decisiones") | [`docs/cambios_vs_E1.md`](../cambios_vs_E1.md) y la sección "Cambio respecto al E1" de cada [ADR](../adr/README.md) |
| Guía de la demo en vivo | [README §6](../../README.md) (sesión en AWS, paso a paso) |
| Video del recorrido completo (3 min 26 s) | Enlace pendiente. El archivo no va en git (pesa demasiado); se genera desde `video/editor` con Remotion |

**Sobre el video:** la guía exige que el pipeline corra en vivo durante la sesión, y la rúbrica le da
0 % a "una demo que es un video pregrabado sin justificación". El video sirve como respaldo si AWS
falla ese día y para mostrar partes que no caben en 8 minutos; no reemplaza la demo en vivo.

## Evidencia por criterio de la rúbrica (repositorio)

### Estructura del repositorio (10 %)

- Carpetas por componente: [`producer/`](../../producer), [`spark/`](../../spark) (`streaming/`, `batch/`, `common/`), [`airflow/`](../../airflow), [`lambda/`](../../lambda), [`sql/athena/`](../../sql/athena), [`infra/`](../../infra), [`analyst/`](../../analyst), [`tests/`](../../tests).
- [`README.md`](../../README.md): pruebas, pipeline local y sesión en AWS paso a paso.
- [`docker-compose.yml`](../../docker-compose.yml) con perfiles `local`, `producer`, `spark`, `streaming` y `airflow`: el mismo archivo corre en local y en las EC2.

### Streaming funcional (20 %)

Productor → Kafka (MSK) → consumidores con procesamiento, en tiempo real:

- Productor: [`producer/producer.py`](../../producer/producer.py) (idempotente, 4 patrones de fraude etiquetados).
- Kafka: Amazon MSK, topic `transactions` con 6 particiones, RF 2 y llave `user_id` ([ADR-01](../adr/adr-01-msk.md), [`session.yaml`](../../infra/cloudformation/session.yaml)).
- Consumidores Spark Structured Streaming: [`ingest_bronze.py`](../../spark/streaming/ingest_bronze.py) (MSK → Bronze) y [`detect_fraud.py`](../../spark/streaming/detect_fraud.py) (MSK → alertas en DynamoDB) ([ADR-02](../adr/adr-02-spark-structured-streaming.md)).
- Manejo de errores: eventos inválidos descartados con motivo, deduplicación por `transaction_id`, checkpoints en S3 y escritura condicional por `alert_id`. Al matar el job con `kill -9`, retoma en el lote siguiente sin alertas repetidas (probado en AWS).
- Pruebas: [`spark_check_detect_fraud.py`](../../tests/spark_check_detect_fraud.py), [`spark_check_ingest_bronze.py`](../../tests/spark_check_ingest_bronze.py), [`test_fraud_rules.py`](../../tests/test_fraud_rules.py) contra la especificación [`reference_rules.py`](../../tests/reference_rules.py).
- Medido en AWS: latencia evento → alerta con p50 de 36 a 40 s y máximo de 70 s en operación normal (meta: < 2 min).

### Spark con transformaciones (20 %)

Transformaciones no triviales, documentadas en [`spark/README.md`](../../spark/README.md):

- **Ventanas y estado por usuario:** [`detect_fraud.py`](../../spark/streaming/detect_fraud.py) + [`fraud_rules.py`](../../spark/streaming/fraud_rules.py). `applyInPandasWithState` con ventanas deslizantes de 5, 10 y 30 min, watermark, deduplicación y episodios (una ráfaga = una alerta).
- **Join stream-estático:** cada transacción se une con el perfil de 30 días del usuario (`silver.user_profile`) para la regla de monto atípico.
- **Agregación con GROUP BY:** [`build_user_profile.py`](../../spark/batch/build_user_profile.py) calcula el promedio de 30 días por usuario y lo mezcla con el perfil inicial.
- **Joins y MERGE:** [`bronze_to_silver.py`](../../spark/batch/bronze_to_silver.py) valida contra el contrato, deduplica con MERGE y enriquece con usuarios, comercios y ciudades.
- **Modelo dimensional:** [`silver_to_gold.py`](../../spark/batch/silver_to_gold.py) construye dimensiones y hechos con MERGE incremental.

### Lakehouse Bronze / Silver / Gold (20 %)

- Delta Lake en S3 con las tres capas: Bronze (crudo de Kafka con su offset), Silver (validado, deduplicado y enriquecido) y Gold ([ADR-03](../adr/adr-03-delta-lake-gold.md)).
- Gold dimensional en constelación: 7 dimensiones y 2 hechos (`fact_transaction`, `fact_alert`), registrados en Glue con [`register_gold_tables.py`](../../sql/athena/register_gold_tables.py).
- Consultas de negocio en Athena ([`sql/athena/`](../../sql/athena), [`sql/README.md`](../../sql/README.md)):
  - Q5: alertas y monto en riesgo por regla, ciudad y franja.
  - Q6: canal y comercio con más monto sospechoso.
  - Q7: falsos positivos según el analista.
  - Q8: fraude evitable frente al reporte diario.
  - Q9: precisión y recall por regla.
- Probado en AWS: las cinco consultas responden. Q9 da recall de 1,0 en las cuatro reglas; Q8 da 72 % evitable en monto acumulado.

### Orquestación (15 %)

- DAGs en [`airflow/dags/`](../../airflow/dags) ([ADR-05](../adr/adr-05-airflow.md), [`airflow/README.md`](../../airflow/README.md)):
  - [`lakehouse_15min`](../../airflow/dags/lakehouse_15min.py): sensor de frescura de Bronze → `bronze_to_silver` → `extract_alert_status` → `build_dimensions` → `silver_to_gold` → `data_quality_checks`.
  - [`daily_batch`](../../airflow/dags/daily_batch.py): catálogos → perfil de usuario → mantenimiento Delta.
- Falla limpia: si Bronze no está fresco, el sensor falla y las tareas siguientes quedan en `upstream_failed`. Si [`data_quality_checks`](../../spark/batch/data_quality_checks.py) encuentra llaves repetidas, llaves foráneas sin dimensión o etiquetas inconsistentes, la corrida falla.
- Airflow solo dispara: los datos los mueve Spark.
- Probado en AWS: las dos corridas de `lakehouse_15min` en verde, incluida la calidad.

### Componente libre justificado (15 %)

**DynamoDB + Lambda + SNS para las alertas** ([ADR-04](../adr/adr-04-dynamodb-sns.md)). Reemplaza a MongoDB del E1; la justificación del cambio está en el ADR y en [`cambios_vs_E1.md`](../cambios_vs_E1.md).

- Tabla `fraud_alerts` con escritura condicional por `alert_id` determinístico (idempotencia) e índice `user_id-status-index` ([`persistent.yaml`](../../infra/cloudformation/persistent.yaml)).
- DynamoDB Streams → [`lambda/notify_alert`](../../lambda/notify_alert/handler.py) → SNS: correo al analista, solo en INSERT.
- [`analyst/alertas.py`](../../analyst/alertas.py): el analista confirma o descarta; esa decisión alimenta Q7.
- Pruebas: [`test_notify_alert.py`](../../tests/test_notify_alert.py), [`test_analyst_alertas.py`](../../tests/test_analyst_alertas.py). Probado en AWS: las alertas llegan por correo, y el estado marcado aparece en Gold y en Q7.

## Defensa individual (40 % de la presentación)

- Responsables y pares de defensa: [`README.md`](../../README.md), sección "Equipo".
- Decisiones con fuente: [ADRs](../adr/README.md) y [`docs/estado_del_arte.md`](../estado_del_arte.md).
- Preguntas típicas de la guía y dónde está la respuesta:
  - **"¿Cómo garantizan que un evento no se procese dos veces si Spark falla a mitad?"** Checkpoints en S3, MERGE por `transaction_id` en Silver y escritura condicional por `alert_id` en DynamoDB ([ADR-02](../adr/adr-02-spark-structured-streaming.md), [ADR-04](../adr/adr-04-dynamodb-sns.md)), más la prueba de caída en AWS.
  - **"Posición CAP del componente más crítico":** MSK configurado como CP (RF 2, `min.insync.replicas=2`) ([ADR-01](../adr/adr-01-msk.md)) y DynamoDB ([ADR-04](../adr/adr-04-dynamodb-sns.md)).
  - **"Partition key de la tabla NoSQL":** `alert_id` y el índice `user_id-status-index` ([ADR-04](../adr/adr-04-dynamodb-sns.md)).
  - **"Si el volumen se multiplicara por 100":** respuesta por preparar. Puntos de partida: [ADR-01](../adr/adr-01-msk.md) (más brokers y particiones), el costo fijo por micro-batch de `detect_fraud` y el atraso de Gold medidos en [`docs/expo.md`](../expo.md) (sección 7), y la sección 8 de ese mismo archivo.
