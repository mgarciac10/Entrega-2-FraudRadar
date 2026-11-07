"""Pruebas unitarias de lambda/notify_alert/handler.py (sin llamar a AWS)."""
import os
import sys
import unittest
from unittest import mock

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "lambda", "notify_alert"))
os.environ.setdefault("TOPIC_ARN", "arn:aws:sns:us-east-2:000000000000:fraud-alerts")
os.environ.setdefault("AWS_DEFAULT_REGION", "us-east-2")

# El handler crea el cliente de SNS al importarse; se parchea para que la prueba
# no cargue credenciales ni llame a AWS.
with mock.patch("boto3.client"):
    import handler  # noqa: E402


def record(event_name="INSERT", seq="1", rule="rafaga", user="u-1"):
    return {
        "eventName": event_name,
        "dynamodb": {
            "SequenceNumber": seq,
            "NewImage": {
                "alert_id": {"S": f"a-{seq}"},
                "user_id": {"S": user},
                "rule": {"S": rule},
                "status": {"S": "abierta"},
                "amount_at_risk_cop": {"N": "350000"},
                "transaction_ids": {"L": [{"S": "t-1"}, {"S": "t-2"}]},
                "evidence": {"M": {"n_transactions": {"N": "6"}}},
            },
        },
    }


class NotifyAlertTest(unittest.TestCase):
    def setUp(self):
        self.sns = mock.Mock()
        patcher = mock.patch.object(handler, "sns", self.sns)
        patcher.start()
        self.addCleanup(patcher.stop)

    def test_publica_una_alerta_nueva(self):
        result = handler.lambda_handler({"Records": [record()]}, None)
        self.assertEqual(result, {"batchItemFailures": []})
        self.sns.publish.assert_called_once()
        kwargs = self.sns.publish.call_args.kwargs
        self.assertEqual(kwargs["TopicArn"], os.environ["TOPIC_ARN"])
        self.assertIn("rafaga", kwargs["Subject"])
        self.assertIn("350000", kwargs["Message"])
        self.assertIn("t-1, t-2", kwargs["Message"])

    def test_ignora_eventos_que_no_son_insert(self):
        handler.lambda_handler({"Records": [record("MODIFY"), record("REMOVE")]}, None)
        self.sns.publish.assert_not_called()

    def test_asunto_es_ascii_y_corto(self):
        subject, _ = handler.build_message({"rule": "ráfaga" * 40, "user_id": "u-ñ"})
        self.assertTrue(subject.isascii())
        self.assertLessEqual(len(subject), 100)

    def test_lote_parcial_reintenta_solo_el_que_falla(self):
        self.sns.publish.side_effect = [None, RuntimeError("sns caído"), None]
        batch = {"Records": [record(seq="1"), record(seq="2"), record(seq="3")]}
        result = handler.lambda_handler(batch, None)
        self.assertEqual(result, {"batchItemFailures": [{"itemIdentifier": "2"}]})
        self.assertEqual(self.sns.publish.call_count, 3)


if __name__ == "__main__":
    unittest.main()
