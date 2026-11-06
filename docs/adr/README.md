# Decisiones de arquitectura (ADRs)

Un ADR (*Architecture Decision Record*) deja por escrito una decisión técnica y por qué se tomó. Tiene cuatro secciones, vistas en S10: contexto, alternativas consideradas, decisión y consecuencias. Además, cada ADR de este repositorio tiene una sección "Cambio respecto al E1", que sirve de base para el criterio "Reflexión sobre decisiones" de la defensa.

La versión original está en el E1 ([`docs/entregable1/e1_fraudradar.pdf`](../entregable1/e1_fraudradar.pdf), Sección 4).

| ADR | Decisión | Cambio frente al E1 |
|---|---|---|
| [01](adr-01-msk.md) | Amazon MSK como motor de ingestión en streaming | Reemplaza Kafka propio en EC2 |
| [02](adr-02-spark-structured-streaming.md) | Spark Structured Streaming (micro-batch) | Silver pasa a batch; la detección lee de MSK |
| [03](adr-03-delta-lake-gold.md) | Delta Lake en S3 y constelación en Gold | `time_key` en `fact_alert`; `is_fraud_injected` en `fact_transaction` |
| [04](adr-04-dynamodb-sns.md) | DynamoDB y SNS para alertas (componente libre) | Reemplaza MongoDB |
| [05](adr-05-airflow.md) | Apache Airflow para la orquestación | Airflow orquesta Bronze → Silver → Gold y deja la capa de ingestión |
| [06](adr-06-despliegue.md) | Cuenta AWS con créditos, servicios gestionados por uso y EC2 con Docker | Reemplaza el Learner Lab |

Si una decisión vuelve a cambiar durante la implementación, se actualiza el ADR y se registra el motivo en "Cambio respecto al E1".
