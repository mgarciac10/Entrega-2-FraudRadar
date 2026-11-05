# lambda

**Responsable:** Juan José Vargas · **Par:** Mateo García

`notify_alert/`: función que lee DynamoDB Streams de `fraud_alerts` y publica en el topic SNS `fraud-alerts` solo los eventos `INSERT`. Una alerta reescrita por un reintento de Spark no genera un segundo correo.
