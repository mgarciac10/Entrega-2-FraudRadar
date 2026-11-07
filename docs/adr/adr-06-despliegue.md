# ADR-06 · Despliegue: cuenta AWS con créditos, servicios gestionados por uso y EC2 con Docker

- **Estado:** vigente. Reemplaza al ADR-06 del E1 (AWS Academy Learner Lab).
- **Fecha:** 2026-11-05

## Contexto

- En la asesoría se propuso abrir una cuenta AWS nueva para usar sus créditos. Las cuentas nuevas reciben USD 100 al registrarse y pueden ganar hasta USD 100 más con actividades.
- El plan gratuito dura 6 meses. Algunos servicios exigen pasar al plan pagado, y al hacerlo se conservan los créditos.
- El Learner Lab del E1 tenía sesiones de 4 h, servicios restringidos e instancias de hasta tamaño *large*.
- La guía exige un `docker-compose.yml` que levante el pipeline desde cero.
- Todo el proyecto, incluida la demo, debe caber en el crédito.

## Alternativas consideradas

- **Seguir en el Learner Lab** (decisión del E1). Sin costo, pero sin MSK y con sesiones que apagan las instancias.
- **Todo gestionado** (MSK, EMR Serverless o Glue para Spark, MWAA para Airflow). Sin servidores que operar, pero MWAA y los jobs de streaming gestionados cobran por hora mientras existen o corren. Con USD 100 no alcanza para desarrollo y demo.
- **Todo local en portátiles.** Sin costo, pero sin S3, DynamoDB, Lambda, SNS ni Athena, y con más RAM de la que tiene un portátil típico.

## Decisión

Un esquema mixto en `us-east-2` (Ohio), la región habilitada para el proyecto de la cuenta (en `us-east-1` una política de la organización deniega las llamadas):

| Tipo | Servicios | Por qué |
|---|---|---|
| Gestionados que cobran por uso o almacenamiento | S3, DynamoDB on-demand, Lambda, SNS, Glue Data Catalog, Athena | Casi no cuestan cuando el pipeline está quieto |
| Gestionado con costo por hora | Amazon MSK, 2 × `kafka.t3.small` | Sugerencia del profesor (ADR-01) |
| EC2 con Docker | Spark en t3.xlarge; Airflow y productor en t3.medium | Mismo `docker-compose.yml` en local y en AWS (ADR-02 y ADR-05) |

Reglas de operación:

- **Credenciales:** las instancias usan un rol IAM de instancia, sin llaves en el código ni en `.env`.
- **Infraestructura por sesión:** MSK y las EC2 se crean al empezar cada sesión de trabajo y se destruyen al terminar, con los scripts de `infra/`. S3 y DynamoDB persisten entre sesiones.
- **Alertas de costo:** AWS Budgets avisa al 50 % y al 80 % del crédito.

**Costo estimado con todo encendido:** unos USD 0,30 por hora (MSK ~0,09, t3.xlarge ~0,17, t3.medium ~0,04, precios on-demand). Eso da unas 300 horas de pipeline encendido con USD 100, más el almacenamiento y las consultas, que son marginales a este volumen.

## Consecuencias

- **Sin alta disponibilidad real:** un solo nodo de Spark, un solo Airflow y MSK con factor de replicación 2 (ADR-01).
- **Cada sesión es una prueba de recuperación.** Los jobs de streaming arrancan con checkpoints nuevos porque el clúster de MSK es nuevo (ADR-01), y Bronze, Silver y Gold continúan en S3.
- **Más código de infraestructura.** Crear y destruir por sesión exige scripts confiables; MSK tarda varios minutos en crearse.
- **Riesgo pendiente:** hay que verificar que el plan gratuito permita crear MSK. Si no, se pasa al plan pagado, que conserva los créditos.
- **Cuatro sistemas que entender.** El equipo opera Spark, Airflow, la infraestructura y los servicios gestionados, y debe poder explicarlos todos en la defensa.

## Cambio respecto al E1

Se deja el Learner Lab por una cuenta con créditos. Esto permite MSK, DynamoDB, Lambda y SNS, que es lo que pidió la asesoría. Se mantiene la idea central del E1: datos y checkpoints en S3, cómputo en EC2 con el mismo docker-compose, y apagar todo fuera de las sesiones.

## Fuentes

- [AWS Free Tier FAQs (créditos y plan gratuito)](https://aws.amazon.com/free/faqs/)
- [AWS Free Tier Terms](https://aws.amazon.com/free/terms)
- [Amazon MSK pricing](https://aws.amazon.com/msk/pricing/)
