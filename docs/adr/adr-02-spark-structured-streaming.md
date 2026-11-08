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
- No lee `is_fraud_injected` ni `attack_id`. Esos campos solo existen para evaluar las reglas en Gold.
- Registra en cada alerta `trigger_event_ts`: el `event_ts` de la transacción que hizo saltar la regla.
- **Las reglas se evalúan con un operador con estado por usuario** (`applyInPandasWithState`; la lógica está en `spark/streaming/fraud_rules.py`). Cada usuario tiene una "libreta" con sus transacciones de los últimos 30 min y cada transacción nueva se evalúa contra ella, igual que `tests/reference_rules.py`. Se prefirió a ventanas de agregación por dos razones: coincide exactamente con la especificación (una ventana deslizante agregada da cortes distintos) y el viaje imposible compara con la transacción *anterior* del usuario, lo que no es una agregación.
- **Un ataque es un solo episodio.** En ráfaga y monto acumulado, la regla se sigue cumpliendo en cada transacción nueva mientras la ventana siga caliente (una ráfaga de 12 transacciones la dispara 7 veces). El primer disparo abre el episodio y fija `window_start`; los siguientes lo reutilizan, así que `alert_id` es el mismo y no se generan 7 alertas ni 7 correos. El episodio termina cuando la regla deja de cumplirse.

**Silver se construye en batch.** `bronze_to_silver` usa la misma API con `trigger(availableNow=True)`: procesa lo nuevo de Bronze desde su checkpoint y termina. Mantiene exactly-once y lo dispara Airflow cada 15 min. La misma API de DataFrames sirve para streaming y batch, y el código corre igual en local (docker-compose) y en EC2.

## Consecuencias

- **Latencia mínima.** Es del orden del trigger (segundos). Se mide en la demo, y debería quedar bastante por debajo de 2 min.
- **Eventos tardíos.** Los que llegan con más de 10 min de retraso quedan en Bronze pero no entran a las ventanas de detección. Los que llegan tarde pero dentro del watermark se evalúan, aunque no vuelven a disparar ventanas que ya se evaluaron sin ellos.
- **Estado en Python.** `applyInPandasWithState` serializa a pandas/Arrow (la imagen incluye `pandas` y `pyarrow`) y es más lento que las ventanas nativas. Si el pico de 3.000 ev/s no cupiera en el lab, la alternativa es reescribir la libreta en Scala o con ventanas nativas; se mide con RNF-01.
- **Viaje imposible sin distancia.** La evidencia guarda las dos ciudades y los minutos entre ellas; no calcula la distancia (la regla de referencia no la usa).
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
