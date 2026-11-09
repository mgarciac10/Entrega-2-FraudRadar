# Puntos para la exposición

Insumo de la presentación (10 min + demo de 8 + defensa de 7). Solo entra lo que vale la pena decir en voz alta. Se agrega a medida que aparece algo pertinente; lo que no se pueda sustentar con un dato, no entra.

Estructura sugerida por la guía: problema (1-2) · arquitectura (3-4) · decisiones con resultados reales (5-8) · qué cambiaríamos (9-10).

## 1. El problema y el posicionamiento

- **Hoy** los analistas de PagaYa revisan reportes diarios; la meta es alertar en **menos de 2 minutos** (p95).
- **Dónde estamos:** monitoreo posterior a la transacción, no calificación en la autorización (esa es otra capa, de milisegundos: Visa ~1 ms, Revolut < 50 ms).
- **Límite que decimos nosotros antes de que pregunten:** no detenemos la primera transacción de un ataque ni una sola transferencia grande. Sí cortamos los pasos siguientes y damos tiempo de congelar fondos (Pix, Bre-B, Reino Unido). **Q8 mide ese valor** en lugar de afirmarlo: monto fraudulento antes y después de la primera alerta.
- **Escala creíble:** Nequi reporta ~700 M de transacciones al mes y picos de 67 M en un día de quincena (agosto de 2025). PagaYa (12 M/día) es plausible.

## 2. Los datos: el productor

- **Mezcla de tráfico:** 70 % pagos, 22 % transferencias P2P, 8 % retiros. Montos alrededor del promedio de cada usuario; los usuarios normales no viajan.
- **4 ataques**, cada uno diseñado para superar el umbral de su regla: ráfaga (6 a 8 transferencias en < 4 min), monto acumulado (> $2 M en < 8 min), monto atípico (6 a 10 veces el promedio), viaje imposible (ciudad a > 400 km en minutos).
- **2 % de fraude medido sobre eventos.** La ráfaga domina el conteo de eventos (6 a 8 por ataque) aunque no sea el patrón más frecuente.
- **El fraude está etiquetado** (`is_fraud_injected`, `attack_id`, `attack_pattern`): con datos reales nadie sabe qué fue fraude; con sintéticos sí, así que medimos **precisión y recall exactos por regla**. Mismo enfoque que PaySim.
- **Hallazgo: el universo de usuarios debe crecer con la tasa.** Con 500 usuarios a 50 ev/s hay más de 1.000 ráfagas por puro azar (probado). Regla: usuarios ≥ tasa × 600.
- **Falsos positivos naturales:** el tráfico normal dispara monto atípico en el 0,18 % de los eventos. Es lo que alimenta la pregunta 7.
- **Productor idempotente** (`acks=all`): los reintentos no duplican eventos en Kafka.
- **Validado en MSK real:** topic con 6 particiones, RF 2, `min.insync.replicas=2`. La llave `user_id` mantiene en orden los eventos de cada usuario; con 717 mensajes el reparto fue de 108 a 142 por partición.

## 3. Decisiones que cambiaron frente al E1 (y por qué)

Esto es la "Reflexión sobre decisiones" (20 % de la defensa). Detalle en `docs/cambios_vs_E1.md` y los ADRs.

| Antes | Ahora | Motivo (no "era más difícil") |
|---|---|---|
| MongoDB para alertas | DynamoDB + SNS | El analista recibe el aviso en lugar de consultar; Streams permite avisar una sola vez por alerta nueva; serverless |
| Kafka en EC2 | Amazon MSK | Gestionado; sugerido en la asesoría |
| Silver por streaming | Silver por batch (Airflow) | Airflow orquesta Bronze → Silver → Gold; la detección lee de MSK y no depende de Bronze |
| Learner Lab | Cuenta AWS con créditos | Permite MSK, DynamoDB, Lambda y SNS |
| Reglas con umbrales en el código | Umbrales en configuración | Calibrarlos con la tasa de falsos positivos |

## 4. Cosas que fallaron y qué aprendimos

Sirven como respuesta a "¿qué problemas tuvieron?"; cada una enseñó algo de sistemas distribuidos o de la nube.

- **KRaft descartado:** MSK rechaza `kafka.t3.small` con KRaft ("Unsupported InstanceType"); se usa ZooKeeper por presupuesto. Costo de elegir broker pequeño.
- **Región bloqueada:** una política de la organización solo permite `us-east-2`; en `us-east-1` todo daba `AccessDenied` aunque el plan fuera pagado.
- **Reemplazo de presupuestos:** cambiar el correo de un recurso con nombre fijo en CloudFormation obliga a reemplazarlo y falla. Se separaron los correos de costos y de alertas.
- **Configuración CP vs. recomendación de AWS:** RF 2 con `min.insync.replicas` 2 bloquea escrituras mientras MSK parcha un broker. Es una elección consciente (preferimos bloquear antes que perder o duplicar), pero va contra la guía de alta disponibilidad de AWS. Producción usaría 3 brokers.
- **Los offsets de Kafka no son una llave única:** cada MSK nuevo los reinicia en 0. Bronze guarda `_session`, y el linaje de un evento es `(_session, partición, offset)`.
- **Una ráfaga disparaba la regla varias veces:** una ráfaga de 12 transacciones cumple "más de 5 en 5 min" desde la 6.ª, o sea 7 disparos = 7 alertas y 7 correos de un solo ataque. Se resolvió con **episodios**: el primer disparo fija el inicio de la ventana y los siguientes comparten el mismo `alert_id`. Una libreta por usuario con estado (`applyInPandasWithState`) en vez de ventanas de agregación, porque coincide con la especificación y el viaje imposible compara con la transacción anterior.
- **El límite de gasto de AWS pausó el proyecto:** el límite mensual estaba en USD 20 y, con MSK y las EC2 encendidas unas horas, AWS pronosticó USD 165 para el mes y pausó los recursos, aunque lo gastado era USD 0,28 y la factura a pagar USD 0. Aprendizaje: el límite actúa sobre el **pronóstico** y no sobre lo gastado, y el pronóstico extrapola el ritmo del momento. Es también el único freno real contra un cobro en tarjeta (los presupuestos solo avisan).
- **Un ataque no generaba alerta:** el episodio de `monto_acumulado` solo se cerraba con una transacción que no cumpliera la regla; una sola transacción grande, 20 min después de otro episodio, heredaba la alerta vieja. Lo encontró la auditoría y hoy lo cubre una prueba.
- **Airflow no podía ir en otra máquina que Spark:** el driver de cada batch corre en el scheduler y los ejecutores se conectan de vuelta a él. Con Airflow en la t3.medium y Spark en la t3.xlarge, esa conexión no existe; Airflow pasó a la EC2 de Spark.
- **El pipeline completo no cabe en un portátil:** con todo encendido Docker se quedó sin memoria (8 GB) y dejó de responder. La integración se prueba en AWS y la demo va grabada.
- **Correos duplicados posibles:** Lambda entrega los eventos de DynamoDB Streams al menos una vez; el registro es exacto pero un correo podría repetirse. Aceptado y documentado.

## 5. Costo y operación

- Pipeline completo encendido ≈ **USD 0,32/h**; con USD 100 de crédito habría que dejarlo ~13 días seguidos para agotarlo. MSK no se puede pausar: se crea y se destruye por sesión (25 min de espera).
- Presupuestos con alertas a USD 50 y 80, un presupuesto aparte que avisa si algo llega a la tarjeta, y `session_status.sh` para no dejar nada encendido.
- Desarrollo local con Kafka propio; AWS solo para integrar y para la demo.

## 6. Para la demo

- Correo de alerta llegando en vivo; consulta Q8 (fraude evitable) y Q5 a Q9 en Athena.
- Matar un job de Spark y mostrar que no hay duplicados al reiniciar (exactly-once).
- DAG de Airflow en verde.
- **Cuidado:** a 50 ev/s salen ~20 correos por minuto. Demo de alertas a 10-20 ev/s; pruebas de carga con la suscripción de SNS pausada.

## 7. Números reales por medir (completar durante las pruebas)

- [ ] Latencia evento → alerta, p50 y p95 (meta: p95 < 2 min)
- [ ] Throughput sostenido: 200 ev/s por 10 min y pico de 1.000 ev/s por 60 s (consumer lag vuelve a < 30 s en 2 min)
- [x] Evento → Bronze en local: como máximo ~15 s (el trigger) en operación normal. Tras matar `ingest_bronze` ~1 min, el primer lote al reiniciar recuperó 1.197 eventos sin pérdidas ni duplicados (Kafka = Bronze = 6.039). Falta medirlo en AWS
- [ ] Evento → Gold (meta ≤ 20 min)
- [ ] Precisión y recall por regla, medidos con las etiquetas (Q9)
- [ ] Fraude evitable (Q8): porcentaje del monto fraudulento posterior a la primera alerta, por patrón
- [ ] Tiempo de recuperación tras reiniciar una EC2 (meta < 10 min)

## 8. Qué cambiaríamos con más tiempo

- Modelo de machine learning con las etiquetas y el feedback del analista (AutoGluon/SageMaker), junto a las reglas.
- Calificación en la autorización (capa 1) para detener la primera transacción.
- MSK con 3 brokers en 3 zonas y KRaft; autenticación IAM en lugar de solo grupo de seguridad.
- Cola de mensajes fallidos en la Lambda y resumen agrupado de alertas en lugar de un correo por alerta.
