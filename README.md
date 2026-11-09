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
Spark y crea un lake de prueba bajo `data/test_spark/`. Para el pipeline integrado:

```bash
docker compose --profile local --profile producer --profile spark --profile airflow up -d --build
docker compose --profile local --profile producer --profile spark --profile airflow exec -T spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 /opt/fraudradar/spark/batch/ingest_catalogs.py
docker compose --profile local --profile producer --profile spark --profile airflow up -d ingest-bronze
# Esperar a que Bronze reciba eventos; luego correr en el orden indicado:
docker compose --profile spark exec -T spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 /opt/fraudradar/spark/batch/bronze_to_silver.py
docker compose --profile spark exec -T spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 /opt/fraudradar/spark/batch/build_user_profile.py
# Solo después de que silver/user_profile exista, arrancar el segundo job de streaming:
docker compose --profile streaming up -d detect-fraud
```

`ingest-bronze` corre continuo y escribe `lake/bronze/transactions`; `detect-fraud` necesita `silver/user_profile`.
El productor permanece activo mientras corre el pipeline. En el `.env.example`, `ALERTS_SINK=console`
y `ALERTS_SOURCE=local-json` evitan llamadas de alertas a AWS. La interfaz Airflow queda en
`http://localhost:8081` (usuario/clave locales definidos en `.env`); habilitar los DAGs pausados y
dispararlos manualmente. El DAG de 15 minutos exige Bronze actualizado en los últimos 10 minutos.

Los pasos y verificaciones de Gold, Airflow y calidad están documentados en
[`airflow/README.md`](airflow/README.md), [`spark/README.md`](spark/README.md) y
[`sql/README.md`](sql/README.md). La registración Glue/Athena solo se ejecuta en AWS, después de que
Gold exista en S3.

### 5. Sesión en AWS (cobra por hora: MSK y 2 EC2)

La configuración local anterior no crea ni usa recursos AWS. Para una sesión AWS se requiere
autorización del dueño de la cuenta; revisar el costo y avisar al equipo antes de iniciar recursos.
En Git Bash, autenticar el perfil `fraudradar` en `us-east-2` y completar `.env` con el bucket
persistente (`S3_BUCKET`), `ALERTS_SOURCE=dynamodb`, `ALERTS_SINK=dynamodb`, los brokers TLS de MSK
(`KAFKA_BOOTSTRAP_SERVERS` y `KAFKA_SECURITY_PROTOCOL=SSL`) y un `STREAM_SESSION` único. No guardar
credenciales AWS en `.env`: las EC2 deben usar su rol de instancia. Como `.env.example` tiene
valores conservadores para Docker Desktop, en la EC2 Spark también hay que ajustar
`SPARK_WORKER_MEMORY` y `SPARK_WORKER_CORES` a la capacidad de esa instancia (para la `t3.xlarge`
del diseño, el punto de partida anterior era `10g` y `3` núcleos; validar con carga real).

```bash
bash infra/scripts/session_up.sh                 # pide confirmación; MSK tarda 20-30 min
bash infra/scripts/test_session.sh               # prueba de humo (7 verificaciones)
bash infra/scripts/producer_aws.sh push          # envía el código a la EC2
bash infra/scripts/producer_aws.sh topic         # topic transactions: 6 particiones, RF 2
bash infra/scripts/producer_aws.sh start 20      # productor a 20 ev/s contra MSK
bash infra/scripts/producer_aws.sh check         # verifica que los mensajes llegan
```

**Al terminar: `bash infra/scripts/session_down.sh`** (borra MSK y las EC2; los datos quedan en S3 y DynamoDB). `bash infra/scripts/session_status.sh` muestra si hay algo encendido y el costo aproximado.

Estos comandos crean y validan la infraestructura y el productor en MSK; no equivalen a validar el
pipeline completo de Spark/Airflow en AWS. Esa integración sigue pendiente: hay que desplegar el
código e imágenes de Spark/Airflow a las EC2 y probar S3, MSK por TLS, DynamoDB, los DAGs y Glue/
Athena en una sesión autorizada. No se ha usado AWS para el trabajo local descrito arriba.

### 6. Estado de pruebas y pendientes

En Docker local se validaron la ingesta de los cuatro catálogos, Bronze → Silver, las tareas de
alertas, dimensiones y hechos Gold, los chequeos de calidad y `build_user_profile`. Los DAG runs de
`lakehouse_15min` y `daily_batch` finalizaron correctamente, incluido `optimize_and_vacuum`; también
pasaron `python -m compileall`, las 48 pruebas unitarias (7 omitidas), la configuración de Compose y
`bash tests/run_spark_checks.sh` ("Todo bien"). Las consultas Q5–Q9 aún deben revisarse con el
equipo y ejecutarse en Athena. El flujo Spark/Airflow en AWS, Glue y Athena requiere una sesión
aprobada y sigue pendiente.

## Checklist del Entregable 2

- [ ] PR final con todo el código antes de las 11:59 PM del día anterior a S16
- [ ] README con instrucciones para correr el pipeline desde cero
- [ ] `docker-compose.yml` (o equivalente) en la raíz
- [ ] Bitácora de delegación de agentes de IA en `docs/bitacora_ia.md`
- [ ] Slides de la presentación en `docs/entregable2/`
- [ ] El pipeline corre antes de S16 (ensayo obligatorio el día anterior)
- [ ] Cada integrante puede responder preguntas sobre cualquier parte del sistema
