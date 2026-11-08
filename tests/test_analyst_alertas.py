"""Pruebas de analyst/alertas.py contra una DynamoDB simulada (moto).

Requiere `boto3` y `moto` (pip install boto3 moto); si falta moto, se omiten.
La tabla se crea igual que en infra/cloudformation/persistent.yaml: llave alert_id e índice user_id + status.
"""
import contextlib
import io
import os
import sys
import unittest
from unittest import mock

ROOT = os.path.join(os.path.dirname(__file__), "..")
sys.path.insert(0, os.path.join(ROOT, "analyst"))

try:
    import boto3
    from moto import mock_aws
except ImportError:  # pragma: no cover
    boto3 = mock_aws = None

if boto3:
    import alertas


def alert(n, user="u1", rule="rafaga", status="abierta", amount=100_000, created="2026-11-08T12:00:00.000Z"):
    return {"alert_id": f"a{n}", "user_id": user, "rule": rule, "status": status, "amount_at_risk_cop": amount,
            "transaction_ids": [f"tx-{n}"], "trigger_event_ts": created, "created_at": created, "evidence": {"n": n}}


@unittest.skipUnless(boto3 and mock_aws, "faltan boto3 y moto")
class AnalystTools(unittest.TestCase):
    def setUp(self):
        for k, v in {"AWS_ACCESS_KEY_ID": "x", "AWS_SECRET_ACCESS_KEY": "x", "AWS_DEFAULT_REGION": "us-east-2",
                     "AWS_REGION": "us-east-2"}.items():
            os.environ[k] = v
        self.mock = mock_aws()
        self.mock.start()
        ddb = boto3.resource("dynamodb", region_name="us-east-2")
        self.table = ddb.create_table(
            TableName="fraud_alerts", BillingMode="PAY_PER_REQUEST",
            KeySchema=[{"AttributeName": "alert_id", "KeyType": "HASH"}],
            AttributeDefinitions=[{"AttributeName": "alert_id", "AttributeType": "S"},
                                  {"AttributeName": "user_id", "AttributeType": "S"},
                                  {"AttributeName": "status", "AttributeType": "S"}],
            GlobalSecondaryIndexes=[{"IndexName": "user_id-status-index",
                                     "KeySchema": [{"AttributeName": "user_id", "KeyType": "HASH"},
                                                   {"AttributeName": "status", "KeyType": "RANGE"}],
                                     "Projection": {"ProjectionType": "ALL"}}])
        for a in (alert(1, "u1", created="2026-11-08T12:00:00.000Z"),
                  alert(2, "u1", "monto_atipico", created="2026-11-08T12:05:00.000Z"),
                  alert(3, "u1", status="descartada", created="2026-11-08T11:00:00.000Z"),
                  alert(4, "u2", "viaje_imposible", amount=500_000, created="2026-11-08T12:10:00.000Z")):
            self.table.put_item(Item=a)

    def tearDown(self):
        self.mock.stop()

    def test_list_by_user_and_status(self):
        self.assertEqual([a["alert_id"] for a in alertas.list_alerts(self.table, "u1")], ["a2", "a1", "a3"])  # recientes primero
        self.assertEqual([a["alert_id"] for a in alertas.list_alerts(self.table, "u1", "abierta")], ["a2", "a1"])
        self.assertEqual(alertas.list_alerts(self.table, "nadie"), [])

    def test_open_alerts_only_open_and_limited(self):
        self.assertEqual([a["alert_id"] for a in alertas.open_alerts(self.table)], ["a4", "a2", "a1"])
        self.assertEqual(len(alertas.open_alerts(self.table, limit=2)), 2)

    def test_mark_changes_status_and_index(self):
        before = alertas.mark(self.table, "a1", "confirmada", "juanjo")
        self.assertEqual(before, "abierta")
        item = self.table.get_item(Key={"alert_id": "a1"})["Item"]
        self.assertEqual((item["status"], item["reviewed_by"]), ("confirmada", "juanjo"))
        self.assertIn("reviewed_at", item)
        self.assertEqual(item["rule"], "rafaga")  # no pierde los demás atributos
        self.assertEqual([a["alert_id"] for a in alertas.list_alerts(self.table, "u1", "abierta")], ["a2"])
        self.assertEqual([a["alert_id"] for a in alertas.list_alerts(self.table, "u1", "confirmada")], ["a1"])
        # el analista puede cambiar de opinión
        self.assertEqual(alertas.mark(self.table, "a1", "descartada"), "confirmada")

    def test_mark_unknown_alert_does_not_create_a_ghost(self):
        with self.assertRaises(LookupError):
            alertas.mark(self.table, "no-existe", "confirmada")
        self.assertEqual(self.table.scan()["Count"], 4)

    def test_mark_rejects_invalid_status(self):
        with self.assertRaises(ValueError):
            alertas.mark(self.table, "a1", "borrada")
        self.assertEqual(self.table.get_item(Key={"alert_id": "a1"})["Item"]["status"], "abierta")

    def test_summary(self):
        s = alertas.summary(self.table)
        self.assertEqual(s["total"], 4)
        self.assertEqual(s["por_estado"], {"abierta": 3, "descartada": 1})
        self.assertEqual(s["por_regla"]["rafaga"], 2)
        self.assertEqual(s["monto_en_riesgo_abiertas_cop"], 700_000)

    def test_cli(self):
        with mock.patch.object(alertas, "get_table", return_value=self.table):
            out = io.StringIO()
            with contextlib.redirect_stdout(out):
                alertas.main(["marcar", "a4", "descartada", "--analista", "ana"])
                alertas.main(["listar", "--user", "u2"])
            self.assertIn("a4: abierta -> descartada", out.getvalue())
            self.assertIn("descartada", out.getvalue().split("\n", 1)[1])
            with self.assertRaises(SystemExit):
                alertas.main(["marcar", "no-existe", "confirmada"])


if __name__ == "__main__":
    unittest.main()
