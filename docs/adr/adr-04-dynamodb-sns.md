# ADR-04 · DynamoDB y SNS para las alertas (componente libre)

- **Estado:** vigente. Reemplaza al ADR-04 del E1 (MongoDB).
- **Fecha:** 2026-11-05

## Contexto

- **RF-07:** cada alerta se guarda con su regla, la evidencia y los `transaction_id` que la dispararon.
- **RF-08:** el analista consulta las alertas de un usuario y las marca como confirmadas o descartadas.
- **Pregunta 7:** la tasa de falsos positivos por regla necesita ese estado en Gold.
- **Aviso activo:** en la asesoría el profesor señaló que al analista le sirve más que le llegue la alerta (SNS) que tener que consultar MongoDB.
- **Emisiones repetidas:** `detect_fraud` emite en modo update, así que una misma ventana puede llegar varias veces. La escritura debe ser idempotente y el aviso no debe repetirse.
- **Evidencia variable:** cada regla produce evidencia con estructura distinta. La ráfaga guarda conteo y ventana; el viaje imposible, dos ciudades y la distancia; el monto atípico, el promedio y el factor.
- **Presupuesto:** el componente no debe cobrar mientras está ocioso (ADR-06).

## Alternativas consideradas

- **MongoDB** (decisión del E1). Tiene documentos flexibles y consultas ad hoc. En contra:
  - Corre en EC2: es un servidor más que operar y pagar.
  - No avisa por sí solo. Los change streams requieren otro proceso consumidor.
  - No se integra con IAM.
- **Solo SNS.** Avisa, pero no guarda historial ni estado, así que RF-08 y la pregunta 7 serían imposibles.
- **Que Spark publique directo en SNS** dentro de `foreachBatch`. Es más simple, pero un micro-batch reintentado reenviaría correos de alertas ya notificadas.
- **Amazon RDS (PostgreSQL).** Da consistencia fuerte y SQL, pero cobra por hora de instancia y su esquema rígido encaja mal con la evidencia variable.
- **ElastiCache (Redis).** Tiene latencia mínima, pero el nodo cobra por hora y la durabilidad en memoria no sirve como registro de alertas.

## Decisión

### Almacenamiento: tabla DynamoDB `fraud_alerts` en modo on-demand

- **Llave primaria:** `alert_id = hash(user_id, regla, inicio de ventana)`. Es determinística: la misma alerta siempre produce el mismo `alert_id`.
- **Atributos:** `user_id`, `rule`, `window_start`, `evidence` (mapa con estructura distinta por regla), `transaction_ids`, `amount_at_risk_cop`, `status`, `trigger_event_ts` y `created_at`.
- **Escritura condicional** (`attribute_not_exists(alert_id)`): la primera escritura crea la alerta. Las reemisiones de la misma ventana fallan la condición y se ignoran. El resultado es exactly-once efectivo en la tabla.
- **Índice secundario global** `user_id` + `status`, para la consulta del analista ("alertas abiertas de este usuario").

### Aviso: DynamoDB Streams → Lambda `notify_alert` → topic SNS `fraud-alerts`

- El *event source mapping* filtra solo los eventos `INSERT`. Una alerta nueva genera un único aviso; los cambios de estado no generan avisos.
- SNS entrega por correo a los analistas suscritos.

### Feedback del analista

El analista marca cada alerta como confirmada o descartada sobre DynamoDB (consola, PartiQL o un script de apoyo). `silver_to_gold` lee ese estado cada 15 min para `fact_alert`.

### Posición CAP

DynamoDB deja elegir la consistencia por operación:
- **Escrituras condicionales:** se evalúan contra la versión más reciente del ítem. Eso es lo que evita alertas duplicadas, y es el lado CP.
- **Consultas del analista por el índice global:** son eventualmente consistentes, porque los índices globales no admiten lecturas fuertes. Un retraso de milisegundos es aceptable para un humano. Es el lado AP.

### Por qué esta llave de partición

`alert_id` es un hash, así que reparte las escrituras de forma uniforme. Usar `user_id` como llave concentraría en una partición caliente las escrituras de los usuarios en ráfaga, que son justamente los sospechosos.

## Consecuencias

- **Avisos duplicados poco probables.** Lambda procesa los eventos de DynamoDB Streams *al menos una vez*: si falla después de publicar, puede reenviar un correo. Se acepta porque un aviso duplicado es inofensivo y uno perdido no; el registro en DynamoDB sigue siendo exacto. Se mitiga con respuesta de lote parcial en Lambda.
- **Evidencia del primer disparo.** La evidencia guardada corresponde al momento en que la regla se disparó por primera vez. Las transacciones posteriores de la misma ventana se ven en Silver y Gold.
- **Consultas limitadas a llaves e índices.** El análisis ad hoc se hace en Athena sobre Gold, no en DynamoDB.
- **SNS por correo no es una herramienta de gestión de casos.** El analista recibe el aviso y trabaja la alerta en DynamoDB. Una interfaz propia queda como trabajo futuro.
- **Estado en dos lugares.** El estado vive en DynamoDB y en Gold, y Gold puede ir hasta 15 min atrás.
- **Costo:** se paga por solicitud. El volumen de alertas (~2 % de los eventos) es bajo, y Lambda y SNS caben en el uso gratuito con este volumen.

## Cambio respecto al E1

Se reemplaza MongoDB por DynamoDB + SNS. La guía exige justificar este cambio, porque el componente libre ya no es el propuesto en el E1. Las razones:

1. El analista recibe un aviso activo en lugar de tener que consultar, como sugirió el profesor.
2. DynamoDB Streams permite avisar solo una vez por alerta nueva. Con MongoDB haría falta un consumidor de change streams propio.
3. Es serverless y cobra por uso, cuando MongoDB necesitaba una EC2 encendida.

Se mantiene lo que sostenía la decisión original: documentos con evidencia flexible, `alert_id` determinístico, escritura idempotente y consulta por usuario.

## Fuentes

- [AWS Lambda con DynamoDB Streams: procesamiento al menos una vez y respuesta de lote parcial](https://docs.aws.amazon.com/lambda/latest/dg/with-ddb.html)
- [`docs/estado_del_arte.md`](../estado_del_arte.md), sección 2 (precisión de las alertas como métrica principal)
