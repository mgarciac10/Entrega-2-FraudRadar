# infra

**Responsable:** Mateo García · **Par:** Juan José Vargas

Scripts para crear y destruir la infraestructura AWS de cada sesión de trabajo. MSK no se puede pausar, así que se borra al terminar para cuidar los créditos (USD 100).

Recursos:

| Recurso | Configuración |
|---|---|
| Amazon MSK | 2 brokers `kafka.t3.small`, topic `transactions` (6 particiones, RF 2, `min.insync.replicas=2`) |
| EC2 | t3.xlarge (Spark) y t3.medium (Airflow + productor), con rol IAM de instancia |
| S3 | bucket con `landing/`, `lake/` (Bronze, Silver, Gold) y `checkpoints/`. Persiste entre sesiones |
| DynamoDB | tabla `fraud_alerts` (on-demand, PK `alert_id`, GSI `user_id` + `status`, Streams activado) |
| Lambda + SNS | `notify_alert` y topic `fraud-alerts` con suscripción de correo |
| Glue + Athena | base de datos `fraudradar` y bucket de resultados |
| AWS Budgets | alertas al 50 % y 80 % del crédito |

Herramienta: **AWS CloudFormation**. El estado vive en AWS, así que cualquier integrante puede borrar un stack desde la consola si alguien olvida apagar. Se usan dos stacks:

- **Persistente:** S3, DynamoDB, SNS, Lambda, Glue, Budgets e IAM. Se crea una vez.
- **Por sesión:** MSK y las EC2. Se crea al empezar a trabajar y se borra al terminar.

Perfil de AWS CLI del proyecto: `fraudradar` (región `us-east-2`: es la única habilitada en la cuenta del proyecto).

## Cómo usarlo

Requiere el perfil `fraudradar` autenticado (`aws login --profile fraudradar`) en `us-east-2`.

```bash
cp .env.example .env   # y completar BUDGET_EMAIL y ALERT_EMAIL
bash infra/scripts/deploy_persistent.sh
```

En Windows hay que anteponer `bash` (un `.sh` solo con su ruta se abre en el editor) y usar Git Bash: en PowerShell, `bash` puede apuntar a WSL, que no ve el perfil de AWS.

Los scripts leen el `.env` de la raíz (está en `.gitignore`), así que el correo no queda en el repositorio. El script crea o actualiza el stack `fraudradar-persistent`. `bash infra/scripts/destroy_persistent.sh` lo borra (pide confirmación).

### Stack persistente (`cloudformation/persistent.yaml`)

Se construye por partes, una por commit:

| Parte | Recursos | Cómo se prueba |
|---|---|---|
| 1 | Presupuestos `fraudradar-consumo-credito` y `fraudradar-cargo-real` | `aws budgets describe-budgets --account-id <cuenta> --profile fraudradar` |
| 2 | Bucket `fraudradar-<cuenta>-us-east-2` | Escribir, leer y borrar un objeto de prueba (ver abajo) |
| 3 | Tabla DynamoDB `fraud_alerts` con Streams | `bash infra/scripts/test_dynamodb.sh` |
| 4 | Topic SNS `fraud-alerts`, Lambda `notify_alert` y su event source mapping | `bash infra/scripts/test_notify.sh` y confirmar el correo |

El **stack por sesión** (`cloudformation/session.yaml`) es aparte y se describe más abajo.

Los presupuestos:

- **`fraudradar-consumo-credito`** excluye los créditos y mide el gasto bruto contra el crédito de USD 100. Avisa al 50 % y 80 % del consumo y cuando la proyección supera el 100 %. Un presupuesto normal descuenta los créditos y marcaría siempre USD 0.
- **`fraudradar-cargo-real`** incluye créditos e impuestos y avisa si lo que iría a la tarjeta supera USD 1. Puede saltar por el IVA, porque los créditos no cubren impuestos.

### Bucket de S3

Un solo bucket con prefijos `landing/`, `lake/` (Bronze, Silver y Gold), `checkpoints/` y `athena-results/` (los resultados se borran a los 30 días). Tiene bloqueo de acceso público, cifrado SSE-S3 y solo acepta HTTPS.

Tiene `DeletionPolicy: Retain`: si se borra el stack, el bucket y sus datos **se conservan**. Para eliminarlo del todo al final del proyecto hay que vaciarlo y borrarlo a mano:

```bash
aws s3 rb s3://fraudradar-<cuenta>-us-east-2 --force --profile fraudradar
```

Prueba de lectura y escritura:

```bash
aws s3 cp .env.example s3://fraudradar-<cuenta>-us-east-2/landing/prueba.txt --profile fraudradar
aws s3 ls s3://fraudradar-<cuenta>-us-east-2/landing/ --profile fraudradar
aws s3 rm s3://fraudradar-<cuenta>-us-east-2/landing/prueba.txt --profile fraudradar
```

### Tabla `fraud_alerts`

- **Llave:** `alert_id` (hash de usuario, regla e inicio de ventana). Reparte las escrituras de forma uniforme y hace que la misma alerta siempre tenga el mismo id.
- **Modo:** on-demand (se paga por solicitud; sin costo fijo).
- **Índice global `user_id-status-index`:** consulta del analista ("alertas abiertas de este usuario"). Es eventualmente consistente.
- **Streams `NEW_IMAGE`:** alimenta la Lambda `notify_alert`, que avisa solo de los INSERT.

`bash infra/scripts/test_dynamodb.sh` verifica cuatro cosas: la primera escritura condicional crea la alerta, reescribirla es rechazado (idempotencia), el índice la encuentra y Streams está activado. Deja la tabla vacía al terminar.

### Aviso por SNS y Lambda `notify_alert`

Flujo: DynamoDB Streams → Lambda → topic SNS `fraud-alerts` → correo del analista.

- **El código vive en `lambda/notify_alert/handler.py`.** `deploy_persistent.sh` lo empaqueta, lo sube a `s3://<bucket>/artifacts/lambda/` con el hash del código en el nombre y se lo pasa al stack. Si el código no cambia, la función no se actualiza.
- **Solo los INSERT disparan aviso** (filtro del event source mapping): cambiar el estado de una alerta no genera otro correo.
- **Sin ventana de espera** (`MaximumBatchingWindowInSeconds: 0`) para no sumar latencia, y con respuesta de lote parcial: solo se reintentan los registros que fallan.
- **Al menos una vez:** Lambda puede entregar un registro dos veces, y en un caso raro llegaría un correo duplicado. Se acepta (ADR-04).
- **Sin cola de mensajes fallidos (DLQ):** si un registro falla las 3 veces, se descarta y queda el error en CloudWatch (14 días). Agregar una DLQ queda como mejora.
- **Dos correos distintos:** `BUDGET_EMAIL` (dueño de la cuenta, avisos de costo) y `ALERT_EMAIL` (analista, alertas de fraude). Cambiar `BUDGET_EMAIL` obliga a CloudFormation a reemplazar los presupuestos y falla por el nombre fijo; `ALERT_EMAIL` se puede cambiar sin problema (la suscripción nueva debe confirmarse). Hay que **confirmar la suscripción** con el enlace del correo "AWS Notification - Subscription Confirmation"; sin eso SNS no entrega nada.

Trampas conocidas de Git Bash en Windows al escribir scripts con la CLI de AWS:

- **Conversión de rutas:** un argumento que empieza por `/` (por ejemplo, el log group `/aws/lambda/...`) se reescribe como `C:/Program Files/Git/...`. Se evita anteponiendo `MSYS_NO_PATHCONV=1` a ese comando.
- **Paginación:** con `--query` la CLI evalúa la consulta por página, así que `length(events)` devuelve un número por página. Para contar, se cuentan ocurrencias en el texto.
- **No ocultar errores** (`2>/dev/null`) en las pruebas: un error silenciado se ve como "0 resultados".

Pruebas:

```bash
python -m unittest tests.test_notify_alert -v   # unitarias, sin AWS (requiere boto3)
bash infra/scripts/test_notify.sh               # extremo a extremo, con AWS
```

## Stack por sesión (MSK y EC2)

Es lo único que cobra por hora, así que se crea al empezar a trabajar y **se borra al terminar**. Los datos no se pierden: viven en el stack persistente (S3 y DynamoDB).

| Script | Qué hace |
|---|---|
| `session_up.sh` | Muestra el costo estimado, pide confirmación (`si`) y crea el stack. MSK tarda 20-30 min |
| `test_session.sh` | Prueba de humo: MSK activo, EC2 en línea, Docker, Kafka por TLS y permisos de S3 y DynamoDB |
| `session_status.sh` | Muestra si hay algo encendido y el costo aproximado de la sesión. **Úsalo antes de cerrar el portátil** |
| `session_down.sh` | Borra el stack y confirma que no queda nada cobrando |

Recursos:

| Recurso | Detalle | Costo aprox. por hora |
|---|---|---|
| Amazon MSK | 2 brokers `kafka.t3.small` (uno por zona), Kafka 3.9 en modo ZooKeeper, 10 GiB cada uno | USD 0,09 |
| EC2 Spark | `t3.xlarge`, disco de 40 GB | USD 0,17 |
| EC2 Airflow y productor | `t3.medium`, disco de 30 GB | USD 0,04 |
| IPv4 públicas y discos | | USD 0,02 |
| **Total** | | **~USD 0,32** (unas 300 h con USD 100) |

Precios on-demand de referencia en us-east-2; hay que confirmarlos en la factura real.

Decisiones de seguridad y operación:

- **Sin SSH y sin puertos abiertos en las EC2.** Se entra por Session Manager: en la consola, EC2 → Connect → Session Manager (no requiere instalar nada). Con la CLI, `aws ssm send-command` no requiere el plugin.
- **Kafka solo por TLS (9094)** y solo desde las EC2 del proyecto (grupo de seguridad). No hay autenticación de cliente; en producción se usaría IAM o mTLS.
- **Kafka 3.9 en modo ZooKeeper, no KRaft:** en esta cuenta, `kafka.t3.small` solo se acepta en modo ZooKeeper; con `3.9.x.kraft` MSK devuelve `Unsupported InstanceType` (solo ofrece m5, m7g y express, bastante más caros). Para el cliente no cambia nada: se conecta por los mismos brokers TLS. KRaft queda como mejora si el presupuesto lo permite.
- **Configuración CP de MSK:** `default.replication.factor=2`, `min.insync.replicas=2`, `unclean.leader.election.enable=false`, retención de 72 h, `auto.create.topics.enable=false` (ver ADR-01).
- **El topic `transactions` (6 particiones, llave `user_id`) no se crea aquí:** se crea desde una EC2 con el cliente de Kafka, en el siguiente paso.
- **Rol de las EC2 de mínimo privilegio:** leer y escribir solo en el bucket del proyecto y en la tabla `fraud_alerts`, más SSM.
- **IMDSv2 obligatorio** en las EC2.
- **Los checkpoints de Spark** deben usar una ruta nueva por sesión, porque el clúster de MSK nuevo no conserva offsets (ADR-01).

