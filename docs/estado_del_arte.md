# Estado del arte en detección de fraude

Este documento responde las preguntas de la asesoría de noviembre: si 2 minutos es suficiente, cómo se miden los falsos positivos y negativos, reglas frente a machine learning, de dónde salen las reglas y qué tan real es el caso. Las fuentes están al final; las cifras de proveedores se marcan como tales.

## Resumen

- La industria detecta fraude en **dos capas**. La primera califica cada transacción **antes de autorizarla**, en milisegundos. La segunda **monitorea después de la transacción**, en segundos o minutos, y alimenta a analistas, congelamiento de cuentas y mecanismos de recuperación. FraudRadar está en la segunda capa a propósito: no autoriza pagos.
- **2 minutos sirve para la segunda capa, no para la primera.** No impide que salga la primera transferencia fraudulenta, pero sí corta las siguientes y da tiempo de congelar fondos en la cuenta destino. Ese es el modelo que siguen los reguladores de pagos inmediatos (Brasil con Pix y Reino Unido).
- **Los sistemas reales combinan reglas y modelos.** Para FraudRadar las reglas son la decisión correcta: no hay etiquetas reales, y un modelo entrenado con nuestro propio generador sintético solo aprendería a reconocer el generador.
- **Las cuatro reglas corresponden a patrones estándar de la industria**: velocidad (ráfaga y monto acumulado), desviación del comportamiento del cliente (monto atípico) y viaje imposible. Los umbrales son supuestos iniciales y se calibran con la tasa de falsos positivos (pregunta 7).
- **La escala supuesta es realista.** Nequi reporta ~700 millones de transacciones al mes y picos de 67 millones en un día de quincena. PagaYa (12 millones/día) equivale a la mitad del promedio de Nequi.

## 1. ¿Es suficiente detectar en menos de 2 minutos?

### Capa 1: calificación en la autorización (milisegundos)

| Sistema | Latencia reportada | Tipo de fuente |
|---|---|---|
| Visa Advanced Authorization | Evalúa más de 500 atributos en ~1 ms | Comunicado de Visa |
| Mastercard Decision Intelligence | < 300 ms de ida y vuelta completa (~50 ms la evaluación) | Prensa secundaria, sin confirmar |
| Revolut Sherlock | < 50 ms, mientras el datáfono muestra "procesando" | Revolut y su proveedor de base de datos |

Estos sistemas están **en el camino de la autorización**: si el modelo dice "fraude", el pago no ocurre. Para eso se necesita una API síncrona con un presupuesto de pocos milisegundos y un almacén de features en memoria. Es otra arquitectura, que no es la que exige el curso (streaming + lakehouse).

### Capa 2: monitoreo posterior (segundos a minutos)

Varios patrones de fraude **solo existen como secuencia**: una ráfaga de transferencias o una cuenta que se vacía en varios movimientos. Ninguna transacción aislada es sospechosa; el patrón aparece al agregar por usuario en una ventana de tiempo. Detectarlo en minutos tiene valor por tres razones:

1. **Corta el resto del ataque.** Si una cuenta tomada empieza a vaciarse, alertar después de la segunda o tercera transferencia evita las siguientes. El problema del caso de negocio es que hoy los analistas revisan reportes diarios.
2. **Permite recuperar fondos en pagos inmediatos.** Con pagos irrevocables, la única defensa después del pago es congelar el dinero en la cuenta destino antes de que se mueva. En Brasil, el MED 2.0 del Pix (Resolución BCB 493/2025) rastrea y bloquea en cadena los fondos de una transacción fraudulenta. Un artículo reporta que la efectividad de recuperación pasaría de menos de 7 % a hasta 80 %. En Reino Unido, desde octubre de 2024 los bancos pueden retener hasta 4 días hábiles un pago saliente si sospechan fraude.
3. **Colombia acaba de entrar en este escenario.** Bre-B, el sistema de pagos inmediatos del Banco de la República, está disponible para todos los usuarios desde el 6 de octubre de 2025. Los analistas citan la suplantación de identidad y la irreversibilidad como los principales riesgos.

### Conclusión para FraudRadar

Se mantiene la meta de **p95 < 2 minutos** (RNF-04), presentada como sistema de monitoreo posterior y no de prevención. Con trigger de 15 s, la latencia real debería ser bastante menor; se mide en la demo.

**Limitación que hay que decir en la defensa:** FraudRadar no detiene la primera transacción de un ataque ni una única transferencia grande fraudulenta. Para eso hace falta la capa 1, que queda como trabajo futuro.

**Cómo se demuestra el valor.** Con `is_fraud_injected` y `attack_id`, Gold mide por ataque cuánto monto fraudulento ocurrió antes de la primera alerta (no evitable) y cuánto después (evitable si el analista reacciona), y lo compara con el escenario actual de reporte diario (query Q8). Así el valor se mide y no solo se afirma.

Frase para la defensa: "FraudRadar no autoriza ni bloquea: es la capa de monitoreo posterior. No evita la primera transacción de un ataque, pero corta los pasos siguientes y da tiempo de congelar fondos. Frente al reporte diario que hoy tiene PagaYa, reduce la ventana de pérdida de horas a minutos, y lo medimos con Q8."

## 2. Falsos positivos y falsos negativos

### Cómo se miden

- **Precisión de las alertas** (de las alertas generadas, cuántas eran fraude). Es la métrica que más importa a los investigadores, porque cada falso positivo consume tiempo de un analista. Dal Pozzolo et al. (2018) la proponen como métrica principal.
- **Recall o tasa de detección** (del fraude real, cuánto se alertó). Los falsos negativos son el fraude que pasa sin alerta.
- **Razón falsos positivos : verdaderos positivos.** Varios proveedores la prefieren porque se traduce en carga de trabajo del equipo.

No hay un benchmark independiente de estos valores, y cada fuente los define distinto (por alerta, por transacción o por rechazo). Los datos disponibles sirven como orden de magnitud:

| Dato | Interpretación | Fuente |
|---|---|---|
| 90–95 % de las alertas de monitoreo transaccional basado en reglas son falsos positivos | 9 a 19 falsas por cada verdadera. Es AML, no fraude de pagos, y es un dato de 2017 citado por un proveedor | PwC vía FluxForce |
| ~30 % de las predicciones de fraude de Revolut Sherlock son correctas | ~2 falsas por cada verdadera, con machine learning | TechHQ |
| Ejemplo de FICO con razón 15:1 | Ilustrativo, no es un benchmark | Unit21 |

### Por qué las etiquetas son difíciles en la realidad

Dal Pozzolo et al. (2018) muestran tres problemas:
- **Etiquetas tardías:** el fraude se confirma días o semanas después, por una investigación o un reclamo.
- **Interacción alerta–feedback:** solo se etiqueta lo que se alertó, lo que sesga el entrenamiento.
- **Concept drift:** los clientes cambian de hábitos y los defraudadores de estrategia.

El Fraud Detection Handbook (Le Borgne et al., 2022) desplaza una semana las ventanas de riesgo justamente porque las etiquetas no llegan a tiempo.

### Qué hace FraudRadar

- **Pregunta 7:** la tasa de falsos positivos por regla se calcula con el feedback del analista (alerta confirmada o descartada en DynamoDB, que llega a `fact_alert`).
- **Ventaja de los datos sintéticos:** el productor sabe qué transacciones son fraude inyectado. Si esa etiqueta viaja con el evento (oculta para las reglas), Gold puede calcular **precisión y recall exactos por regla**, algo imposible con datos reales. **Decisión del equipo (2026-11-05):** se agrega el campo `is_fraud_injected` al evento y a `fact_transaction`. `detect_fraud` no lo lee; solo se usa en Gold para medir las reglas.

## 3. Reglas, machine learning o híbrido

### Qué usa la industria

Los sistemas grandes son **híbridos**. Stripe Radar asigna un puntaje de riesgo con machine learning y permite reglas propias, incluidas reglas que combinan el puntaje con otras señales. Sus reglas de velocidad cuentan cargos por cliente, tarjeta o email en ventanas por hora, día o semana. Visa y Revolut usan modelos para la decisión en línea.

La literatura académica resume por qué el problema es difícil para el machine learning: desbalance extremo de clases, concept drift y latencia de verificación (Dal Pozzolo et al., 2018).

### Por qué FraudRadar usa reglas

1. **No hay etiquetas reales.** Un modelo entrenado con el fraude que inyecta nuestro productor aprendería a reconocer el generador, no el fraude. Sus métricas serían altas y sin significado.
2. **Explicabilidad.** El analista recibe "6 transacciones en 5 minutos", no un puntaje de 0,87. Eso permite actuar y auditar.
3. **Arranque en frío.** Las reglas funcionan desde el primer evento. Un modelo necesita historia etiquetada.
4. **El servicio gestionado de AWS ya no es opción.** Amazon Fraud Detector no acepta clientes nuevos desde el 7 de noviembre de 2025. AWS recomienda AutoGluon con SageMaker para construir modelos propios.

### Por qué la arquitectura queda lista para machine learning

- Las features de las reglas (conteo y monto por usuario en ventanas, promedio de 30 días) son las mismas features base que propone el Fraud Detection Handbook para entrenar modelos.
- El feedback del analista acumula etiquetas en Gold.

Trabajo futuro: entrenar un modelo con esas features y etiquetas (por ejemplo con AutoGluon), cargarlo en `detect_fraud` y usar su puntaje junto con las reglas. Siguiendo a Dal Pozzolo, conviene tratar por separado el feedback de los analistas y las etiquetas tardías.

## 4. Justificación de las cuatro reglas

| Regla (umbral inicial) | Patrón de la industria | Respaldo |
|---|---|---|
| Ráfaga: > 5 transacciones en 5 min | Regla de velocidad por conteo. Detecta prueba de tarjetas y vaciado rápido de cuentas | Velocity rules de Stripe Radar; features `CUSTOMER_ID_NB_TX_nDAY_WINDOW` del Handbook |
| Monto acumulado: > $2.000.000 en 10 min | Regla de velocidad por monto: varias transacciones moderadas que suman mucho | Velocity rules de Stripe Radar; agregación de transacciones por cliente (Whitrow et al., 2008, citado por Bahnsen et al., 2016) |
| Monto atípico: > 5 × el promedio de 30 días del usuario | Desviación del comportamiento de gasto del cliente | Features `CUSTOMER_ID_AVG_AMOUNT_30DAY_WINDOW` del Handbook; Bahnsen et al. (2016) muestran que los modelos que ignoran el comportamiento del cliente pierden patrones |
| Viaje imposible: 2 ciudades en < 30 min | Velocidad geográfica imposible entre dos eventos de la misma cuenta | Detección *impossible travel* de Microsoft Entra ID Protection, que además ignora VPN y ubicaciones conocidas para reducir falsos positivos |

**Sobre los umbrales:** son supuestos del equipo, no cifras de la industria. En sistemas reales cada umbral se ajusta midiendo cuántos falsos positivos produce. Esa es justamente la pregunta 7. Por eso los umbrales deben ser configuración y no estar fijos en el código, y en la defensa deben presentarse así.

## 5. ¿Qué tan real es el caso?

- **Escala:** a agosto de 2025, Nequi reportaba 26 millones de clientes (~20 millones activos a diario), cerca de 700 millones de transacciones al mes (~23 millones/día, ~270/s en promedio) y hasta 67 millones en un día de quincena. PagaYa (8 millones de usuarios, 12 millones de transacciones/día y pico de diseño de 3.000/s) es una billetera de segundo nivel plausible.
- **Contexto regulatorio y de mercado:**
  - Bre-B opera desde octubre de 2025 con pagos inmediatos e irrevocables.
  - La Superintendencia Financiera registró un récord de 2,76 millones de quejas bancarias en 2025. La prensa las asocia en parte con transacciones no reconocidas y fallas de canales digitales.
  - No se encontraron cifras oficiales recientes de fraude por canal digital. El indicador más citado de Asobancaria es de 2021. Esto es un vacío de información que conviene mencionar.
- **Datos sintéticos como práctica aceptada:** PaySim (Lopez-Rojas et al., 2016) simula transacciones de dinero móvil con fraude inyectado, a partir de registros reales de un operador. Se creó porque no hay datos públicos del sector. Es el mismo enfoque de `producer.py`.

## 6. Implicaciones para el diseño

| # | Implicación | Estado |
|---|---|---|
| 1 | Presentar FraudRadar como monitoreo posterior (capa 2) y la calificación en línea como trabajo futuro | Aplicar en la presentación y en el ADR-02 |
| 2 | Agregar `is_fraud_injected` y `attack_id` al evento y a `fact_transaction`, y `trigger_event_ts` a `fact_alert`, para medir precisión, recall y fraude evitable | Aprobado: aplicar en `producer.py`, `spark/common/` y `silver_to_gold` |
| 3 | Umbrales de las reglas como configuración, no en el código | Aplicar en `detect_fraud` |
| 4 | Machine learning como trabajo futuro, con AutoGluon/SageMaker como ruta de AWS | Documentar en el ADR correspondiente |
| 5 | Medir en la demo la latencia real evento → alerta (p50 y p95) | Ya está en RNF-04 |

## Fuentes

**Académicas y técnicas**
- Dal Pozzolo, A., Boracchi, G., Caelen, O., Alippi, C. y Bontempi, G. (2018). Credit Card Fraud Detection: A Realistic Modeling and a Novel Learning Strategy. *IEEE TNNLS*. [PDF](https://boracchi.faculty.polimi.it/docs/2017_FraudsTNNLS.pdf)
- Le Borgne, Y.-A., Siblini, W., Lebichot, B. y Bontempi, G. (2022). *Reproducible Machine Learning for Credit Card Fraud Detection – Practical Handbook*. [Baseline feature transformation](https://fraud-detection-handbook.github.io/fraud-detection-handbook/Chapter_3_GettingStarted/BaselineFeatureTransformation.html)
- Bahnsen, A. C. et al. (2016). Feature Engineering Strategies for Credit Card Fraud Detection. *Expert Systems with Applications*. [PDF](https://albahnsen.github.io/files/Feature%20Engineering%20Strategies%20for%20Credit%20Card%20Fraud%20Detection_published.pdf)
- Lopez-Rojas, E. A., Elmir, A. y Axelsson, S. (2016). PaySim: A financial mobile money simulator for fraud detection. *EMSS 2016*. [Actas](https://msc-les.org/proceedings/emss/emss2016/emss2016_249.html)

**Industria y documentación**
- Visa: [Visa prevents more than $700 million in fraud…](https://www.visa.com.au/about-visa/newsroom/press-releases/visa-prevents-more-than-700-million-in-fraud-from-disrupting-australian-businesses.html) (Advanced Authorization, ~1 ms)
- Stripe: [Rules reference (velocity rules)](https://docs.stripe.com/radar/rules/reference) y [Risk evaluation](https://stripe.com/nl-be/docs/radar/risk-evaluation)
- Revolut Sherlock: [Couchbase, caso Revolut](https://www.couchbase.com/customers/revolut/) y [TechHQ](https://techhq.com/?p=192761) (fuentes del proveedor)
- Microsoft: [Entra ID Protection, risk detections](https://learn.microsoft.com/en-us/entra/id-protection/concept-identity-protection-risks)
- AWS: [Amazon Fraud Detector availability change](https://docs.aws.amazon.com/frauddetector/latest/ug/frauddetector-availability-change.html)
- Latencia de referencia en autorización (proveedor): [Redis, real-time fraud detection](https://redis.io/blog/real-time-fraud-detection/)
- Falsos positivos (proveedores, usar como orden de magnitud): [FluxForce](https://www.fluxforce.ai/statistics/false-positive-rates-transaction-monitoring), [Unit21](https://unit21.ai/blog/when-1-1-3-matching-the-right-data-to-prevent-fraud)

**Regulación y pagos inmediatos**
- Reino Unido: [FCA FG24/6, retención de pagos hasta 4 días hábiles](https://fca.org.uk/publication/finalised-guidance/fg24-6.pdf)
- Brasil: [Forbes Brasil, MED 2.0 del Pix](https://forbes.com.br/forbes-money/2026/02/med-2-0-do-bc-entra-em-vigor-e-permite-bloqueio-de-pix-fraudulento-entenda-regras/), [Finsiders, MED 2.0](https://finsidersbrasil.com.br/pagamentos/pix/com-med-2-0-bc-quer-diminuir-falsos-positivos-e-rastrear-laranjas/)
- Colombia, Bre-B: [Banco de la República](https://banrep.gov.co/es/noticias/bre-b-semana-crece-expectativa-entrada-operacion-bre-b), [Noticias Caracol](https://www.noticiascaracol.com/economia/bre-b-inicia-operacion-controlada-en-colombia-el-sistema-que-transformara-los-pagos-inmediatos-rg10), [Portafolio, riesgos de fraude con Bre-B](https://www.portafolio.co/economia/finanzas/estos-son-los-cinco-riesgos-de-fraude-que-crecerian-con-la-expansion-de-bre-b-en-colombia-495777)

**Mercado colombiano**
- [La República, entrevista al CEO de Nequi (29 de agosto de 2025)](https://www.larepublica.co/finanzas/entrevista-a-andres-vasquez-ceo-de-nequi-4211528)
- [Infobae, 2,76 millones de quejas bancarias en 2025](https://www.infobae.com/colombia/2026/02/26/explota-la-inconformidad-bancaria-en-colombia-276-millones-de-quejas-y-bancolombia-lidera-el-ranking/)
- [Asobancaria, Banca & Economía 1291 (2021)](https://www.asobancaria.com/wp-content/uploads/2021/08/1291_BE.pdf)
