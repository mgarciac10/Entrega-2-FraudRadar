# ADR-01 · Amazon MSK como motor de ingestión en streaming

- **Estado:** vigente. Reemplaza al ADR-01 del E1 (Kafka propio en EC2).
- **Fecha:** 2026-11-05

## Contexto

- RF-01 y RNF-01: el diseño debe soportar picos de 3.000 eventos/s. En la demo se miden 200 ev/s sostenidos y un pico de 1.000 ev/s.
- RNF-05: si un job de Spark cae, debe poder releer los eventos que no alcanzó a procesar.
- RF-06: las reglas por ventana necesitan los eventos de un mismo usuario en orden.
- En la asesoría el profesor sugirió un servicio gestionado (MSK o Kinesis). La cuenta AWS con créditos permite usarlo (ADR-06).

## Alternativas consideradas

- **Kafka propio en Docker sobre EC2** (decisión del E1). Era la opción más barata dentro del Learner Lab, pero obliga a operar el broker y deja un punto único de falla.
- **Amazon Kinesis Data Streams.** Es gestionado y escala por shards. Se descarta por tres razones:
  - Spark de código abierto no trae una fuente de Kinesis; existe un conector de AWS Labs que se instala aparte.
  - El código no sería el mismo en desarrollo local (docker-compose con Kafka) y en AWS.
  - El curso trabajó Kafka (S6–S7).
- **MSK Serverless.** No hay brokers que dimensionar, pero cobra un cargo fijo por hora de clúster más cargos por partición. Para un clúster pequeño sale más caro que dos brokers `kafka.t3.small`.

## Decisión

Usar **MSK provisionado** con Kafka 3.x y 2 brokers `kafka.t3.small`, uno en cada una de 2 zonas de disponibilidad (MSK exige al menos un broker por zona).

**Topic `transactions`:**
- 6 particiones con `user_id` como llave. Todos los eventos de un usuario caen en la misma partición y conservan su orden.
- Retención de 72 h.
- Factor de replicación 2, `min.insync.replicas=2` y `unclean.leader.election.enable=false`.

**Productor:** idempotente (`enable.idempotence=true`, `acks=all`). Los reintentos no duplican eventos en el log.

**Posición CAP: CP.** Con `acks=all` y `min.insync.replicas=2`, si un broker cae el clúster rechaza escrituras antes que aceptar datos sin réplica. El productor reintenta durante `delivery.timeout.ms` (2 min). Perder unos segundos de disponibilidad es aceptable porque FraudRadar monitorea después del pago y no está en el camino de autorización (estado del arte, sección 1).

**Desarrollo local:** se usa Kafka en docker-compose (perfil `local`). Como el protocolo es el mismo, el código de Spark y del productor no cambia.

## Consecuencias

- **Costo.** Unos USD 0,09 por hora por los dos brokers (USD 0,0456 cada uno, según fuentes de terceros), más el almacenamiento. Un clúster provisionado no se puede pausar, así que se crea y se destruye en cada sesión de trabajo (ADR-06).
- **Checkpoints al recrear el clúster.** Al crear un clúster nuevo, los checkpoints de Spark apuntan a offsets que ya no existen. Cada sesión usa una ruta de checkpoint nueva para los jobs de streaming. No hay duplicados, porque los eventos del clúster nuevo son nuevos; lo que ya se procesó está en Bronze, que vive en S3.
- **Capacidad limitada.** `kafka.t3.small` es una instancia de ráfaga para cargas pequeñas. El diseño escala a 3.000 ev/s agregando brokers o subiendo el tipo de instancia, sin cambiar código. En el lab solo se demuestra el rango de RNF-01.
- **Disponibilidad reducida.** Con factor de replicación 2, la caída de un broker detiene las escrituras. Producción usaría 3 brokers en 3 zonas con `min.insync.replicas=2`, que tolera una caída sin perder disponibilidad.
- **Particiones fijas.** Cambiar el número de particiones después rompe el mapeo llave → partición, así que las 6 particiones se fijan desde el inicio.

## Cambio respecto al E1

Antes, Kafka corría en Docker sobre EC2 dentro del Learner Lab. Ahora se usa MSK: el profesor lo sugirió, la cuenta con créditos lo permite, y así el equipo no opera el broker. Se mantiene todo lo que sostenía la decisión original: 6 particiones por `user_id`, productor idempotente y configuración CP.

## Fuentes

- [Amazon MSK: crear un clúster provisionado (subredes y brokers por zona)](https://docs.aws.amazon.com/msk/latest/developerguide/create-cluster-console.html)
- [Amazon MSK pricing](https://aws.amazon.com/msk/pricing/) y precio de `kafka.t3.small` según [Airbyte](https://airbyte.com/data-engineering-resources/apache-kafka-pricing)
- [`docs/estado_del_arte.md`](../estado_del_arte.md), sección 1 (FraudRadar como monitoreo posterior)
