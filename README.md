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
├── infra/               crear y destruir la infraestructura AWS de cada sesión (infra/local: DynamoDB Local)
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
    ├── entregable2/     índice de la entrega del E2: slides, video y evidencia por criterio
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

> **Estado (2026-11-10):** el pipeline completo corrió de punta a punta en AWS: productor → MSK →
> `detect_fraud` → DynamoDB → correo, y MSK → Bronze → Silver → Gold en S3 orquestado por Airflow,
> con Glue y las consultas Q5–Q9 en Athena. Resultados medidos en [`docs/expo.md`](docs/expo.md)
> (sección 7). Índice de la entrega: [`docs/entregable2/README.md`](docs/entregable2/README.md).

Hay dos formas de correrlo:

| | Local (Docker) | AWS (sesión) |
|---|---|---|
| Kafka | contenedor `kafka` | Amazon MSK |
| Lake | `data/lake` | S3 |
| Alertas | DynamoDB Local (sin correo) | DynamoDB + Lambda + SNS (correo) |
| Gold consultable | Spark SQL | Glue + Athena |
| Costo | 0 | ~USD 0,32 por hora |
| Para qué | desarrollar y probar cada job | la demo y la prueba integrada |

### 1. Requisitos

- Docker Desktop con el motor corriendo, y Git Bash en Windows (los scripts son de bash; en
  PowerShell `bash` puede apuntar a WSL).
- Python 3.11 o superior. Para `analyst/alertas.py` contra AWS: `pip install boto3 "botocore[crt]"`
  (el perfil `fraudradar` usa `aws login`, que necesita `botocore[crt]`).
- Solo para AWS: la AWS CLI v2 con el perfil `fraudradar` en `us-east-2` y el
  [plugin de Session Manager](https://docs.aws.amazon.com/systems-manager/latest/userguide/install-plugin-windows.html)
  para ver Airflow y Spark en el navegador.

### 2. Configuración

```bash
cp .env.example .env
```

- **Local:** en el `.env`, `LAKE_URI=/data/lake`, `LANDING_URI=/data/catalogs`,
  `CHECKPOINT_URI=/data/checkpoints` y `DYNAMODB_ENDPOINT_URL=http://dynamodb-local:8000`.
- **AWS:** en tu máquina basta con `AWS_PROFILE`, `AWS_REGION` y `S3_BUCKET`. El `.env` de cada EC2
  lo escriben los scripts.
- En Git Bash: `export MSYS_NO_PATHCONV=1` antes de `docker compose` (si no, convierte `/data/...`
  en `C:/Program Files/Git/...`).

### 3. Pruebas (local, sin AWS)

```bash
python -m unittest discover tests      # reglas, productor, catálogos, Lambda, analista, registro en Glue
bash tests/run_spark_checks.sh         # cada job de Spark con Delta real, uno a la vez
```

`run_spark_checks.sh` construye la imagen de Spark y prueba `ingest_catalogs`, `bronze_to_silver`
(validación, rechazados, MERGE, relectura completa), `build_user_profile` (incluido el lake vacío),
`detect_fraud` (Structured Streaming real contra la especificación `tests/reference_rules.py`) y
`silver_to_gold` (carga incremental, hora de Colombia, calidad).

### 4. Pipeline integrado en local

Necesita más de 8 GB para Docker: con todo encendido se usan ~5,7 GB, y cuando Airflow lanza los
batch un portátil con 8 GB se satura. Si la máquina no alcanza, la prueba integrada se hace en AWS
(sección 6).

```bash
python producer/generate_catalogs.py --users 20000 --out data/catalogs
docker compose --profile local --profile producer --profile spark --profile streaming --profile airflow up -d --build
```

1. Airflow en http://localhost:8081 (usuario y clave en el `.env`): activar `daily_batch` (carga los
   catálogos y crea el perfil, aunque el lake esté vacío).
2. `detect-fraud` se reinicia solo hasta que existe `silver/user_profile`; luego escribe en DynamoDB Local.
   Ver o marcar alertas: `DYNAMODB_ENDPOINT_URL=http://localhost:8000 python analyst/alertas.py abiertas`.
3. Cuando `daily_batch` termine, activar `lakehouse_15min` (exige Bronze actualizado en los últimos 10 min).

Detalles de cada parte: [`spark/README.md`](spark/README.md), [`airflow/README.md`](airflow/README.md)
y [`sql/README.md`](sql/README.md).

### 5. Infraestructura persistente (AWS, una vez)

```bash
bash infra/scripts/deploy_persistent.sh     # presupuestos, bucket S3, DynamoDB, SNS y Lambda
```

Confirmar la suscripción de correo de SNS y probar el aviso con `bash infra/scripts/test_notify.sh`.

### 6. Sesión en AWS (cobra por hora: MSK y 2 EC2)

Crear el stack lo decide el dueño de la cuenta: revisar el costo, avisar al equipo y verificar que el
límite de gasto del proyecto sea de al menos USD 160 (AWS pausa los recursos si el pronóstico lo
supera). Topología: la t3.medium corre el productor; la t3.xlarge corre Spark, `ingest-bronze`,
`detect-fraud` y Airflow (el driver de los batch corre en el scheduler y los ejecutores deben
alcanzarlo, ADR-05).

```bash
bash infra/scripts/session_up.sh                        # pide confirmación; MSK tarda 20-30 min
bash infra/scripts/test_session.sh                      # prueba de humo (8 verificaciones)
bash infra/scripts/push_code.sh                         # código a las dos EC2
bash infra/scripts/producer_aws.sh topic                # topic transactions: 6 particiones, RF 2
bash infra/scripts/producer_aws.sh start 10             # catálogos a S3 landing/ y productor a 10 ev/s
bash infra/scripts/pipeline_aws.sh start                # Spark, streaming y Airflow en la EC2 de Spark
bash infra/scripts/pipeline_aws.sh dag daily_batch      # catálogos a Bronze y perfil inicial
bash infra/scripts/pipeline_aws.sh dag lakehouse_15min  # cuando build_user_profile termine
bash infra/scripts/pipeline_aws.sh register             # tablas de Gold en Glue (tras el primer Gold)
```

- **Correos:** a 10 ev/s salen unas 4 alertas por minuto. La primera vez llegan de golpe las del
  atraso acumulado mientras se creaba el perfil.
- **Seguimiento:** `pipeline_aws.sh status` (contenedores y corridas de los DAG) y
  `pipeline_aws.sh logs detect-fraud 5m | grep lote` (alertas por micro-batch).
- **Prueba de caída:** `pipeline_aws.sh kill detect-fraud` mata el job con `kill -9`; Docker lo
  levanta y retoma desde su checkpoint sin alertas repetidas.
- **Interfaces:** reenvío de puertos de Session Manager, por ejemplo Airflow en http://localhost:8081:

```bash
aws ssm start-session --profile fraudradar --region us-east-2 --target <SparkInstanceId> --document-name AWS-StartPortForwardingSession --parameters portNumber=8081,localPortNumber=8081
```

- **Consultas:** en Athena (Query editor → base de datos `fraudradar`), los archivos de `sql/athena/`.
  La primera vez, la ubicación de resultados es `s3://<bucket>/athena-results/`.
- **Analista:** `python analyst/alertas.py abiertas` y
  `python analyst/alertas.py marcar <alert_id> confirmada|descartada --analista <nombre>`.

**Al terminar: `bash infra/scripts/session_down.sh`** (borra MSK y las EC2; el lake en S3, las
alertas en DynamoDB y las tablas de Glue se conservan). `bash infra/scripts/session_status.sh`
muestra si queda algo encendido y el costo aproximado.

### 7. Estado de pruebas y pendientes

Probado en AWS el 2026-11-10 (mediciones en [`docs/expo.md`](docs/expo.md), sección 7):

- Los dos DAG en verde, incluido `data_quality_checks`; Q5 a Q9 responden en Athena.
- Latencia evento → alerta: p50 de 36 a 40 s en operación normal, máximo 70 s (meta: < 2 min).
- Recall de 1,0 en las cuatro reglas; fraude evitable de 72 % en monto acumulado frente al reporte diario.
- Prueba de caída de `detect-fraud`: retoma en el lote siguiente, sin duplicados.

Pendiente:

- Evento → Gold tarda ~26 min (meta: 20): `build_dimensions` reescribe `dim_user` completa en cada
  corrida, y `optimize_and_vacuum` tarda más de 45 min sobre 21 tablas en S3 y compite con el DAG de 15 min.
- Prueba de carga (200 ev/s por 10 min y pico de 1.000 ev/s, RNF-01).

## Checklist del Entregable 2

- [ ] PR final con todo el código antes de las 11:59 PM del día anterior a S16
- [x] README con instrucciones para correr el pipeline desde cero
- [x] `docker-compose.yml` en la raíz
- [x] Bitácora de delegación de agentes de IA en [`docs/bitacora_ia.md`](docs/bitacora_ia.md)
- [ ] Slides de la presentación en `docs/entregable2/`
- [x] El pipeline corre de punta a punta (AWS, 2026-11-10)
- [ ] Ensayo obligatorio el día anterior a S16
- [ ] Cada integrante puede responder preguntas sobre cualquier parte del sistema
