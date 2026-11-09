import json
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "sql" / "athena"))
from register_gold_tables import _athena_type, _latest_delta_metadata


class FakeBody:
    def __init__(self, value):
        self.value = value

    def read(self):
        return self.value.encode("utf-8")


class FakeS3:
    def __init__(self):
        self.commits = {
            "lake/gold/fact_alert/_delta_log/00000000000000000000.json": json.dumps({
                "metaData": {
                    "schemaString": json.dumps({
                        "type": "struct",
                        "fields": [
                            {"name": "alert_id", "type": "string"},
                            {"name": "transaction_ids", "type": {
                                "type": "array", "elementType": "string", "containsNull": False,
                            }},
                        ],
                    }),
                },
            }),
            "lake/gold/fact_alert/_delta_log/00000000000000000001.json": json.dumps({
                "commitInfo": {"operation": "MERGE"},
            }),
        }

    def get_paginator(self, operation):
        if operation != "list_objects_v2":
            raise AssertionError(operation)
        return self

    def paginate(self, Bucket, Prefix):
        if Bucket != "test-bucket" or Prefix != "lake/gold/fact_alert/_delta_log/":
            raise AssertionError((Bucket, Prefix))
        return [{"Contents": [{"Key": key} for key in self.commits]}]

    def get_object(self, Bucket, Key):
        if Bucket != "test-bucket":
            raise AssertionError(Bucket)
        return {"Body": FakeBody(self.commits[Key])}


class RegisterGoldTablesTest(unittest.TestCase):
    def test_maps_delta_types_to_athena_types(self):
        self.assertEqual(_athena_type("long"), "bigint")
        self.assertEqual(_athena_type("timestamp"), "timestamp")
        self.assertEqual(_athena_type({"type": "array", "elementType": "string"}), "array<string>")

    def test_uses_latest_commit_and_recovers_schema_from_prior_commit(self):
        location, columns = _latest_delta_metadata(FakeS3(), "test-bucket", "lake/gold/fact_alert")
        self.assertEqual(
            location,
            "s3://test-bucket/lake/gold/fact_alert/_delta_log/00000000000000000001.json",
        )
        self.assertEqual(columns, [
            {"Name": "alert_id", "Type": "string"},
            {"Name": "transaction_ids", "Type": "array<string>"},
        ])


if __name__ == "__main__":
    unittest.main()
