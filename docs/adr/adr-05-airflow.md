# ADR-05 · Apache Airflow para la orquestación

- **Estado:** vigente. Reemplaza al ADR-05 del E1 (DAGs distintos y Airflow en la capa de ingestión).
- **Fecha:** 2026-11-05

## Contexto

- La guía exige que el pipeline esté orquestado: cada paso se dispara solo, según sus dependencias, y falla limpiamente si algo anterior falla.
- En la asesoría el profesor pidió dos cosas:
  - Que Airflow orqueste las transiciones Bronze → Silver → Gold.
  - Revisar el uso de Airflow en la ingestión. En el E1 aparecía una "tarea Airflow" como mecanismo de ingesta, y Airflow no debe transportar datos.
- RF-09 a RF-11 son procesos batch periódicos: perfil diario, Gold cada 15 min y chequeo de frescura.

## Alternativas consideradas

- **Cron en EC2.** No maneja dependencias, reintentos ni da visibilidad del estado.
- **AWS Step Functions.** Es gestionado, pero se modela en JSON (ASL) y no se vio en el curso.
- **Amazon MWAA.** Es Airflow gestionado, con ícono nativo y sin servidor que operar. Pero cobra por hora mientras el entorno existe, aunque no corra nada, y con USD 100 de crédito ese costo pesa (ADR-06).
- **Airflow en EC2 con Docker** (elegida). Es el mismo Airflow, en el mismo docker-compose que pide la guía.

## Decisión

Airflow 2.x en Docker con LocalExecutor, en la EC2 de Spark (t3.xlarge), dentro del mismo docker-compose que el clúster. Las tareas ejecutan `spark-submit` desde el scheduler contra el master de Spark. El driver de cada batch corre dentro de `airflow-scheduler` y los ejecutores se conectan de vuelta a él por la red de Docker; con Airflow en otra EC2 esa conexión no existe sin fijar puertos y abrir red, por eso van juntos. Airflow no mueve datos: solo dispara jobs y verifica condiciones. El mismo compose permite desarrollar en local con PostgreSQL, sin AWS.

| DAG | Horario | Tareas |
|---|---|---|
| `lakehouse_15min` | cada 15 min | `check_bronze_freshness` → `bronze_to_silver` → `extract_alert_status` → `build_dimensions` → `silver_to_gold` → `data_quality_checks` |
| `daily_batch` | diario | `ingest_catalogs` → `build_user_profile` → `optimize_and_vacuum` |

- `check_bronze_freshness` es un sensor: exige que el último commit de `bronze.transactions` tenga menos de 10 min (mtime de los JSON Delta en local/S3). Si falla, las tareas siguientes quedan en `upstream_failed` y Gold no se construye con datos incompletos (RF-11).
- `extract_alert_status` copia DynamoDB a Silver; en local lee un JSON de prueba y no llama a AWS.
- `build_dimensions` y `silver_to_gold` cargan las dimensiones y los dos hechos; `data_quality_checks` bloquea la corrida si falla una regla de calidad.
- `optimize_and_vacuum` compacta Delta a diario y conserva la retención mínima de 168 horas.
- `ingest_catalogs` es un job de Spark que lee los CSV de `S3 landing/` y escribe Bronze. Airflow solo lo dispara.
- **Configuración común:** 2 reintentos con 2 min de espera, `max_active_runs=1` para evitar dos MERGE simultáneos sobre la misma tabla, y `catchup=False`.
- **Los jobs de streaming no dependen de Airflow.** Corren continuos (`restart: always` en Docker), y Airflow solo verifica su salud mediante el sensor de frescura.

## Consecuencias

- **Latencia de Silver.** Silver queda hasta 15 min atrás y Gold hasta 20 min (15 min del horario más ~5 de ejecución). No afecta las alertas, que salen de MSK (ADR-02).
- **Corridas encadenadas.** Con `max_active_runs=1`, una corrida lenta retrasa la siguiente.
- **Más operación.** El equipo opera Airflow (scheduler, webserver y Postgres de metadatos).
- **Dependencia de red del driver.** En el modo client, `spark-submit` se ejecuta en `airflow-scheduler`; el master y los workers deben poder alcanzar ese host. Por eso Airflow comparte EC2 y red de Docker con Spark.
- **Una máquina más cargada.** Spark, los dos jobs de streaming y Airflow comparten la t3.xlarge (16 GB): worker de 4 cupos y 6 GB, ejecutores de streaming de 1 GB.
- **Sin dueño para el streaming.** El ciclo de vida de los jobs continuos queda en Docker, no en Airflow. Si un job de streaming muere, Airflow lo detecta indirectamente porque Bronze deja de actualizarse.

## Cambio respecto al E1

| | E1 | Ahora |
|---|---|---|
| Bronze → Silver | Streaming, fuera de Airflow | Tarea `bronze_to_silver` del DAG cada 15 min |
| Ingesta de catálogos | "Tarea Airflow `load_catalogs`" dibujada en la capa de ingestión | Job Spark `ingest_catalogs`, disparado por Airflow |
| Sensor de frescura | Sobre Silver | Sobre Bronze, porque ahora Silver lo produce el propio DAG |
| Estado de alertas | Extraído de MongoDB | Extraído de DynamoDB (ADR-04) |
| Dónde corre | EC2 t3.medium, junto al productor | EC2 de Spark (t3.xlarge): el driver de los batch corre en el scheduler y los ejecutores deben alcanzarlo |
