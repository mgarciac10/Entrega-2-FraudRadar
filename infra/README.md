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
cp .env.example .env   # y completar ALERT_EMAIL
bash infra/scripts/deploy_persistent.sh
```

En Windows hay que anteponer `bash` (un `.sh` solo con su ruta se abre en el editor) y usar Git Bash: en PowerShell, `bash` puede apuntar a WSL, que no ve el perfil de AWS.

Los scripts leen el `.env` de la raíz (está en `.gitignore`), así que el correo no queda en el repositorio. El script crea o actualiza el stack `fraudradar-persistent`. `bash infra/scripts/destroy_persistent.sh` lo borra (pide confirmación).

### Stack persistente (`cloudformation/persistent.yaml`)

Se construye por partes, una por commit:

| Parte | Recursos | Cómo se prueba |
|---|---|---|
| 1 | Presupuestos `fraudradar-consumo-credito` y `fraudradar-cargo-real` | `aws budgets describe-budgets --account-id <cuenta> --profile fraudradar` |

Los presupuestos:

- **`fraudradar-consumo-credito`** excluye los créditos y mide el gasto bruto contra el crédito de USD 100. Avisa al 50 % y 80 % del consumo y cuando la proyección supera el 100 %. Un presupuesto normal descuenta los créditos y marcaría siempre USD 0.
- **`fraudradar-cargo-real`** incluye créditos e impuestos y avisa si lo que iría a la tarjeta supera USD 1. Puede saltar por el IVA, porque los créditos no cubren impuestos.

