# sql

**Responsable:** Luis Moreno · **Par:** Juan José Gómez

`athena/`: consultas de negocio para las tablas Delta de Gold y `register_gold_tables.py`, que registra sus esquemas y versión Delta actual en Glue. Una de estas queries se corre en vivo en la demo.

| Query | Pregunta de negocio |
|---|---|
| `q5_alertas_por_regla_ciudad_franja.sql` | ¿Cuántas alertas y cuánto monto en riesgo hubo por regla, ciudad y franja horaria? |
| `q6_canal_comercio_sospechoso.sql` | ¿Qué canal y qué categoría de comercio concentran el mayor monto sospechoso? |
| `q7_falsos_positivos_por_regla.sql` | ¿Qué porcentaje de las alertas revisadas de cada regla fue descartado como falso positivo? |
| `q8_fraude_evitable.sql` | De cada ataque inyectado, ¿cuánto monto ocurrió antes de la primera alerta (no evitable) y cuánto después (evitable si el analista reacciona)? Compara contra el reporte diario actual |
| `q9_precision_recall_por_regla.sql` | Precisión y recall exactos por regla, cruzando `attack_pattern` (qué regla debía detectar cada ataque) con las alertas |

**Q8 es la métrica de valor del proyecto.** FraudRadar no detiene la primera transacción de un ataque; Q8 mide cuánto monto fraudulento ocurre después de la primera alerta y lo compara con el corte diario. Se muestra en la demo junto a la latencia evento → alerta.

Definición: para cada `attack_id`, la primera alerta se vincula mediante `transaction_ids` y `trigger_event_ts`. `fraudradar_evitable_cop` suma transacciones fraudulentas posteriores al primer disparo; sin alerta, el monto evitable es cero. El escenario de reporte diario usa como corte la medianoche siguiente al inicio del ataque.

Q7 calcula falsos positivos solo sobre alertas revisadas: `descartada` es falso positivo y `confirmada` es positivo confirmado. `abierta` no entra al denominador. Q9 calcula precisión por alerta y recall por ataque distinto, usando `is_fraud_injected`, `attack_id` y `attack_pattern`. Como los ataques se solapan a propósito (un monto acumulado grande también es atípico), da dos precisiones: `precision` (la alerta cae sobre fraude real de cualquier patrón: lo que le importa al analista) y `precision_patron` (cae sobre un ataque de su propio patrón). Y dos recall: `recall` (ataques detectados por su propia regla) y `recall_cualquier` (detectados por alguna regla, lo que mide Q8).

Las cinco consultas se validaron en Spark SQL sobre el Gold de `tests/spark_check_silver_to_gold.py`, cambiando solo `CROSS JOIN UNNEST` por `CROSS JOIN LATERAL explode`. Falta correrlas en Athena.

Registro en AWS, una vez que `gold/` exista en S3:

```bash
docker compose --profile spark --profile airflow exec -T airflow-scheduler \
  python /opt/fraudradar/sql/athena/register_gold_tables.py \
  --database fraudradar --lake-uri s3a://<bucket>/lake
```

El script requiere permisos de lectura en S3 y escritura en Glue, disponibles desde el rol de la
EC2. Las consultas asumen la base `fraudradar`; si se usa otro nombre, actualizar su prefijo.
No debe ejecutarse durante el desarrollo local; las consultas Q5–Q9 son archivos SQL para Athena
(Athena no se usa en local).
