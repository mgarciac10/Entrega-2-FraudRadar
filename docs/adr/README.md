# Decisiones de arquitectura (ADRs)

Cada ADR va en su propio archivo (`adr-01-....md`) con las cuatro secciones vistas en S10: contexto, alternativas consideradas, decisión y consecuencias. Cada uno debe citar conceptos del curso.

La versión original está en el E1 ([`docs/entregable1/e1_fraudradar.pdf`](../entregable1/e1_fraudradar.pdf), Sección 4). Varios cambiaron después de la asesoría (ver [`../cambios_vs_E1.md`](../cambios_vs_E1.md)).

| ADR | Decisión | Estado |
|---|---|---|
| 01 | Motor de ingestión en streaming: Kafka → **Amazon MSK** | Por reescribir |
| 02 | Spark Structured Streaming (micro-batch) para el procesamiento | Vigente; ajustar: Silver pasa a batch orquestado |
| 03 | Delta Lake en S3 y modelo dimensional (constelación) en Gold | Vigente; agregar `time_key` en `fact_alert` |
| 04 | Componente libre: MongoDB → **DynamoDB + SNS** para alertas | Por reescribir (incluye posición CAP) |
| 05 | Apache Airflow para la orquestación | Por reescribir (DAGs nuevos) |
| 06 | Despliegue: Learner Lab → **cuenta AWS con créditos** | Por reescribir |
