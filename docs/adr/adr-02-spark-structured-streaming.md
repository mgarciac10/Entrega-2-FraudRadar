# ADR-02 · Spark Structured Streaming (micro-batch) para el procesamiento

- **Estado:** vigente, con ajustes al ADR-02 del E1 (Silver pasa a batch orquestado y la detección lee de MSK).
- **Fecha:** 2026-11-05

## Contexto

- RNF-04 pide alertas en menos de 2 minutos (p95), no en milisegundos. El estado del arte confirma que las latencias de milisegundos solo se necesitan en el camino de autorización, que está fuera del alcance (estado del arte, sección 1).
- RF-06 requiere ventanas con estado por usuario: 5, 10 y 30 minutos.
- RF-09 y RF-10 son procesos batch que también necesitan un motor distribuido.
- En la asesoría se pidió que Airflow orqueste las transiciones Bronze → Silver → Gold (ADR-05).

## Alternativas consideradas

- **Apache Flink.** Procesa evento a evento con latencia de milisegundos y maneja el estado con más detalle. Se descarta porque la latencia requerida no lo exige y porque obligaría a operar un segundo motor: Spark se necesita igual para el batch.
- **Solo batch.** No cumple la restricción de streaming de la guía ni la meta de 2 minutos.
- **Un solo job de streaming que haga todo** (Bronze, Silver y alertas). Fue la idea del E1. Ahora Silver lo construye Airflow, y separar la detección deja el camino caliente libre de la latencia de Bronze.

## Decisión

Spark 3.5 Structured Streaming con trigger de 15 s, en dos jobs continuos que leen el topic `transactions` con checkpoints independientes:

| Job | Qué hace | Garantía |
|---|---|---|
| `ingest_bronze` | Escribe en `bronze.transactions` el evento crudo más topic, partición, offset y hora de ingesta | Exactly-once: offsets en el checkpoint y commit transaccional de Delta |
| `detect_fraud` | Deduplica por `transaction_id` (watermark de 10 min), hace un join stream-estático con `silver.user_profile`, calcula ventanas por usuario y evalúa las 4 reglas con umbrales leídos de configuración | Modo update con `foreachBatch` y escritura idempotente en DynamoDB (ADR-04) |

Sobre el diseño de `detect_fraud`:
- Lee de MSK y no de Bronze, para no sumar la latencia de commit de Bronze a la del camino caliente.
- Emite en modo update: dispara la alerta apenas una ventana supera el umbral, sin esperar a que cierre.
- No lee el campo `is_fraud_injected`. Ese campo solo existe para evaluar las reglas en Gold.

**Silver se construye en batch.** `bronze_to_silver` usa la misma API con `trigger(availableNow=True)`: procesa lo nuevo de Bronze desde su checkpoint y termina. Mantiene exactly-once y lo dispara Airflow cada 15 min. La misma API de DataFrames sirve para streaming y batch, y el código corre igual en local (docker-compose) y en EC2.

## Consecuencias

- **Latencia mínima.** Es del orden del trigger (segundos). Se mide en la demo, y debería quedar bastante por debajo de 2 min.
- **Eventos tardíos.** Los que llegan con más de 10 min de retraso quedan en Bronze pero no entran a las ventanas de detección.
- **Emisiones repetidas.** En modo update una misma ventana puede emitirse varias veces. Por eso la escritura de alertas tiene que ser idempotente (ADR-04).
- **Doble lectura del topic.** Los dos jobs leen el mismo topic con grupos de consumidores distintos. La carga sobre MSK es baja con este volumen.
- **Validación mínima en `detect_fraud`.** Como no lee Silver, aplica su propia deduplicación y validación de esquema. El esquema se comparte desde `spark/common/` para no duplicar lógica.
- **Silver deja de ser casi en tiempo real.** Puede quedar hasta 15 min atrás. No afecta las alertas, que salen de MSK.

## Cambio respecto al E1

| | E1 | Ahora |
|---|---|---|
| Job 2 | `silver_and_alerts`: leía Bronze y escribía Silver y alertas | `detect_fraud`: lee MSK y solo genera alertas |
| Silver | Streaming continuo | Batch incremental (`availableNow`) disparado por Airflow |
| Umbrales | En el código | En configuración, para poder calibrarlos con la pregunta 7 |

## Fuentes

- [`docs/estado_del_arte.md`](../estado_del_arte.md): secciones 1 (latencia por capas) y 4 (reglas y umbrales)
