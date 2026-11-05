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

Herramienta (CloudFormation, Terraform o scripts de AWS CLI): por definir.
