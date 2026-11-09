# FraudRadar

Detección de fraude casi en tiempo real para una billetera digital colombiana (caso simulado "PagaYa").
Proyecto final de ST1630 Sistemas Intensivos en Datos, Universidad EAFIT, 2026-2.

Los analistas de riesgo necesitan enterarse de patrones de fraude (ráfagas de transacciones, montos atípicos, viajes imposibles) en menos de 2 minutos. Los jefes de riesgo y el equipo de producto necesitan analítica histórica actualizada cada 15 minutos.

![Arquitectura](docs/arquitectura/arquitectura.png)

Diagrama editable: [`docs/arquitectura/arquitectura.drawio`](docs/arquitectura/arquitectura.drawio). Los cambios respecto al Entregable 1 están en [`docs/cambios_vs_E1.md`](docs/cambios_vs_E1.md).

## Arquitectura en una línea por capa

| Capa | Componentes |
|---|---|
| Fuente | `producer.py` (transacciones sintéticas con fraude inyectado) · CSV diarios del core en `S3 landing/` |
| Ingestión | Amazon MSK, topic `transactions` (6 particiones, key `user_id`) · job Spark `ingest_catalogs` |
| Procesamiento | Spark Structured Streaming: `ingest_bronze`, `detect_fraud` · Spark batch: `bronze_to_silver`, `build_user_profile`, `silver_to_gold` |
| Almacenamiento | Delta Lake en S3 (Bronze / Silver / Gold en constelación) · DynamoDB `fraud_alerts` |
| Consumo | DynamoDB Streams → Lambda → SNS (alerta al analista) · Athena + Glue Data Catalog (preguntas 5 a 9) |
| Orquestación | Apache Airflow: DAGs `lakehouse_15min` y `daily_batch` |

## Estructura del repositorio

```
├── docker-compose.yml   servicios locales (Spark, Airflow, productor; Kafka local para desarrollo)
├── .env.example         variables de entorno; copiar a .env
├── infra/               crear y destruir la infraestructura AWS de cada sesión
├── producer/            productor sintético y generador de catálogos
├── analyst/             herramienta del analista: listar y marcar alertas en DynamoDB
├── spark/
│   ├── streaming/       ingest_bronze, detect_fraud
│   ├── batch/           ingest_catalogs, bronze_to_silver, build_user_profile, extract_alert_status,
│   │                    silver_to_gold, data_quality_checks, optimize_and_vacuum
│   └── common/          esquemas y configuración compartida
├── airflow/             DAGs y servicio Airflow local con PostgreSQL
├── lambda/notify_alert/ DynamoDB Streams → SNS
├── sql/athena/          registro en Glue y queries Q5–Q9
├── tests/
└── docs/
    ├── entregable1/     PDF y diagrama tal como se entregaron
    ├── entregable2/     slides de la sustentación
    ├── arquitectura/    diagrama vigente
    ├── adr/             decisiones de arquitectura
    ├── estado_del_arte.md
    ├── expo.md          puntos para la exposición
    ├── cambios_vs_E1.md
    └── bitacora_ia.md   bitácora de delegación a agentes de IA
```

## Equipo

| Integrante | Responsable de | Par de defensa |
|---|---|---|
| Mateo García Carreño | Infraestructura, ingestión (MSK, productor, catálogos), `docker-compose.yml`, README | Juan José Vargas |
| Juan José Gómez | Streaming a Bronze y transición Bronze → Silver | Luis Moreno |
| Juan José Vargas | Reglas de fraude, alertas (DynamoDB, Lambda, SNS), `build_user_profile` | Mateo García |
| Luis Moreno Gutiérrez | Gold, Athena, DAGs de Airflow, chequeos de calidad | Juan José Gómez |

Cada PR lo aprueba el par de defensa de quien lo abre.

## Cómo correr el pipeline desde cero

> **Estado:** la infraestructura, el productor, los catálogos y los jobs de Bronze/Silver/alertas están implementados. Gold, las consultas Q5–Q9, los DAGs, el registro Glue y los chequeos de calidad también están escritos. Las tareas locales de catálogo, Bronze → Silver, alertas, Gold, calidad y perfil se validaron en Docker; el registro Glue/Athena todavía requiere AWS y no se ejecutó. No se requiere AWS para las pruebas locales.

### 1. Requisitos

- Python 3.11 o superior y Docker Desktop (con el motor corriendo).
- Git Bash en Windows (los scripts son de bash; en PowerShell `bash` puede apuntar a WSL).
- La AWS CLI v2 solo se requiere para desplegar/probar recursos AWS; **no hace falta AWS para desarrollar ni probar en local**.

### 2. Configuración

Para desarrollo local, copiar `.env.example` a `.env` y dejar:

```dotenv
LAKE_URI=/data/lake
LANDING_URI=/data/catalogs
CHECKPOINT_URI=/data/checkpoints
ALERTS_SINK=console
ALERTS_SOURCE=local-json
```

En Git Bash, la copia inicial es `cp .env.example .env`.

No ejecutar `aws login`, `session_up.sh` ni scripts de despliegue durante estas pruebas.

### 3. Infraestructura persistente (opcional, solo para AWS)

> **No ejecutar para desarrollar ni probar en local.** La creación de recursos en AWS puede generar
> cargos; el flujo local de la sección siguiente no requiere AWS.

```bash
bash infra/scripts/deploy_persistent.sh     # presupuestos, bucket S3, DynamoDB, SNS y Lambda
```

Confirma la suscripción de correo de SNS (llega un mensaje de AWS) y prueba el aviso con `bash infra/scripts/test_notify.sh`.

### 4. Desarrollo local, sin AWS

```bash
python producer/generate_catalogs.py --users 20000 --out data/catalogs
export MSYS_NO_PATHCONV=1
docker compose --profile local --profile producer --profile spark --profile airflow up -d --build
bash tests/run_spark_checks.sh
python -m unittest discover tests
```

Los chequeos de Spark se ejecutan sin MSK, DynamoDB, Glue ni Athena; el runner construye la imagen
Spark y crea un lake de prueba bajo `data/test_spark/`. Corren un job a la vez: es la forma de
probar en un portátil.

**Pipeline integrado en local.** Necesita más de 8 GB para Docker: con Kafka, productor, Spark, los
dos streaming, DynamoDB Local y Airflow encendidos se usan ~5,7 GB, y al lanzar Airflow los batch
un portátil con 8 GB para Docker se satura. La prueba integrada de referencia se hace en AWS
(sección 5). Si la máquina alcanza, en el `.env`: `LAKE_URI=/data/lake`,
`LANDING_URI=/data/catalogs`, `CHECKPOINT_URI=/data/checkpoints` y
`DYNAMODB_ENDPOINT_URL=http://dynamodb-local:8000`.

```bash
docker compose --profile local --profile producer --profile spark --profile streaming --profile airflow up -d --build
# En Airflow (http://localhost:8081): activar y lanzar daily_batch (catálogos y perfil inicial)
# y, cuando termine, activar lakehouse_15min
```

`ingest-bronze` corre continuo y escribe `lake/bronze/transactions`. `detect-fraud` se reinicia solo
hasta que `daily_batch` crea `silver/user_profile` (funciona con el lake vacío) y luego escribe en
DynamoDB Local; ver las alertas o marcarlas desde tu máquina con
`DYNAMODB_ENDPOINT_URL=http://localhost:8000 python analyst/alertas.py abiertas`. El DAG de 15 minutos
exige Bronze actualizado en los últimos 10 minutos.

Los pasos y verificaciones de Gold, Airflow y calidad están documentados en
[`airflow/README.md`](airflow/README.md), [`spark/README.md`](spark/README.md) y
[`sql/README.md`](sql/README.md). La registración Glue/Athena solo se ejecuta en AWS, después de que
Gold exista en S3.

### 5. Sesión en AWS (cobra por hora: MSK y 2 EC2)

Crear el stack lo decide el dueño de la cuenta: revisar el costo y avisar al equipo antes. En tu
máquina el `.env` solo necesita el perfil, la región y `S3_BUCKET`: la configuración de cada EC2 la
escriben los scripts (rutas `s3a://`, brokers TLS de MSK, `STREAM_SESSION` nueva, alertas en
DynamoDB, worker de 4 cupos y 6 GB). Las EC2 usan su rol de instancia: nada de llaves en `.env`.

Topología: la t3.medium corre el productor; la t3.xlarge corre Spark, `ingest-bronze`,
`detect-fraud` y Airflow (el driver de los batch corre en el scheduler y los ejecutores deben
alcanzarlo, ADR-05).

```bash
bash infra/scripts/session_up.sh                 # pide confirmación; MSK tarda 20-30 min
bash infra/scripts/test_session.sh               # prueba de humo (7 verificaciones)
bash infra/scripts/push_code.sh                  # código a las dos EC2
bash infra/scripts/producer_aws.sh topic         # topic transactions: 6 particiones, RF 2
bash infra/scripts/producer_aws.sh start 20      # catálogos a S3 landing/ y productor a 20 ev/s
bash infra/scripts/pipeline_aws.sh start         # Spark, streaming y Airflow en la EC2 de Spark
bash infra/scripts/pipeline_aws.sh dag daily_batch      # catálogos a Bronze y perfil inicial
bash infra/scripts/pipeline_aws.sh dag lakehouse_15min  # cuando daily_batch termine
bash infra/scripts/pipeline_aws.sh status        # contenedores y corridas de los DAG
bash infra/scripts/pipeline_aws.sh register      # tablas de Gold en Glue (tras el primer Gold)
```

`detect-fraud` se reinicia solo hasta que `daily_batch` crea `silver/user_profile`; después
escribe en `fraud_alerts` y llegan los correos. Las interfaces se ven con reenvío de puertos de
Session Manager (requiere el plugin de Session Manager de la CLI), por ejemplo Airflow:

```bash
aws ssm start-session --profile fraudradar --region us-east-2 --target <SparkInstanceId> --document-name AWS-StartPortForwardingSession --parameters portNumber=8081,localPortNumber=8081
```

**Al terminar: `bash infra/scripts/session_down.sh`** (borra MSK y las EC2; los datos quedan en S3 y DynamoDB). `bash infra/scripts/session_status.sh` muestra si hay algo encendido y el costo aproximado.

### 6. Estado de pruebas y pendientes

En Docker local se validaron la ingesta de los cuatro catálogos, Bronze → Silver, las tareas de
alertas, dimensiones y hechos Gold (incremental y en hora de Colombia), los chequeos de calidad y
`build_user_profile` (también con el lake vacío); `bash tests/run_spark_checks.sh` da "Todo bien".
Contra DynamoDB Local: escritura condicional de alertas, reemisión ignorada, marcado con
`analyst/alertas.py` y extracción a `silver/alert_status`. En el flujo integrado local, `daily_batch`
corrió con el lake vacío y `detect-fraud` escribió alertas en DynamoDB Local; la corrida completa de
`lakehouse_15min` saturó el portátil (8 GB para Docker) y queda para AWS. Q5 a Q9 se validaron en
Spark SQL; falta Athena. Pendiente en AWS: Spark contra MSK por TLS y S3, Airflow en la EC2 de
Spark, Glue y Athena.

## Checklist del Entregable 2

- [ ] PR final con todo el código antes de las 11:59 PM del día anterior a S16
- [ ] README con instrucciones para correr el pipeline desde cero
- [ ] `docker-compose.yml` (o equivalente) en la raíz
- [ ] Bitácora de delegación de agentes de IA en `docs/bitacora_ia.md`
- [ ] Slides de la presentación en `docs/entregable2/`
- [ ] El pipeline corre antes de S16 (ensayo obligatorio el día anterior)
- [ ] Cada integrante puede responder preguntas sobre cualquier parte del sistema
