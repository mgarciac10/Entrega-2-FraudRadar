# lambda

**Responsable:** Juan José Vargas · **Par:** Mateo García

`notify_alert/handler.py`: función que lee DynamoDB Streams de `fraud_alerts` y publica en el topic SNS `fraud-alerts` solo los eventos `INSERT`. Una alerta reescrita por un reintento de Spark no genera un segundo correo, porque la escritura condicional de `detect_fraud` la rechaza y no llega al stream.

- Runtime Python 3.12; usa `boto3`, que ya viene en el entorno de Lambda.
- El asunto del correo es ASCII y de máximo 100 caracteres (límite de SNS).
- Respuesta de lote parcial: si un registro falla, solo ese se reintenta.
- Se despliega con `bash infra/scripts/deploy_persistent.sh` (ver `infra/README.md`).
- Pruebas unitarias: `tests/test_notify_alert.py`.
