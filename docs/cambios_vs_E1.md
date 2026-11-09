# Cambios de arquitectura respecto al E1

Origen: asesoría con el profesor (noviembre 2026). Diagrama nuevo en [`arquitectura/arquitectura.drawio`](arquitectura/arquitectura.drawio) (exportado a PNG y SVG).

| Componente | E1 | Ahora | Motivo |
|---|---|---|---|
| Plataforma | AWS Academy Learner Lab | Cuenta AWS propia con créditos (USD 100) | Permite servicios gestionados (MSK) |
| Ingestión streaming | Kafka en Docker sobre EC2 | Amazon MSK, 2 brokers `kafka.t3.small` | Sugerencia del profesor; sin operar brokers |
| Almacén de alertas | MongoDB (`fraud.alerts`) | DynamoDB `fraud_alerts` | Serverless; escritura condicional por `alert_id` |
| Aviso al analista | Consulta en Compass / mongosh | DynamoDB Streams → Lambda → SNS (correo) | Sugerencia del profesor; avisa solo alertas nuevas (sin duplicados) |
| Bronze → Silver | Streaming continuo (job 2) | Batch de Airflow cada 15 min (`bronze_to_silver`, trigger `availableNow`) | El profesor pidió que Airflow orqueste Bronze → Silver → Gold |
| Detección de fraude | Job 2 leía Bronze | `detect_fraud` lee MSK directamente | No depende de Bronze; menor latencia |
| Ingesta de catálogos | "Tarea Airflow load_catalogs" en la capa de ingestión | Job Spark `ingest_catalogs` disparado por Airflow | Airflow orquesta; no transporta datos |
| Dónde corre Airflow | EC2 t3.medium, con el productor | EC2 de Spark (t3.xlarge) | El driver de los batch corre en el scheduler; los ejecutores deben alcanzarlo por la red de Docker |
| Gold | `fact_alert` sin hora | `fact_alert` con `time_key` (`dim_time` compartida) | Pregunta 5 pide franja horaria |
| Machine learning | Fuera del alcance | Sigue fuera; se justifica con el estado del arte como trabajo futuro | Decisión del equipo |

## Pendiente en el documento

- Reescribir ADR-01 (MSK), ADR-04 (DynamoDB + SNS y su posición CAP), ADR-05 (DAGs nuevos) y ADR-06 (cuenta con créditos).
- Ajustar RF-07, RF-08, RF-10, RF-11, RNF-02, RNF-07, la tabla 2.3 y la Sección 3.
- Sección 5: cambiar las menciones a MongoDB y a los riesgos del Learner Lab.
- Estado del arte: latencia de detección (¿2 min basta?), falsos positivos/negativos, reglas vs. ML.
- Verificar que el plan de la cuenta nueva permite crear MSK. MSK no se puede pausar, así que hay que borrarlo al final de cada sesión.
