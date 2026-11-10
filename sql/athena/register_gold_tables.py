"""Registra las tablas Delta de Gold en Glue para consulta de solo lectura con Athena.

Solo se ejecuta en AWS, después de escribir Gold en S3:
  python sql/athena/register_gold_tables.py --database fraudradar --lake-uri s3a://BUCKET/lake
"""
import argparse
import json
import os
import re
from urllib.parse import urlparse

TABLES = (
    "dim_date", "dim_time", "dim_user", "dim_merchant", "dim_location", "dim_channel",
    "dim_rule", "fact_transaction", "fact_alert",
)


def _athena_type(delta_type):
    if isinstance(delta_type, str):
        primitive = {
            "string": "string",
            "long": "bigint",
            "integer": "int",
            "short": "smallint",
            "byte": "tinyint",
            "double": "double",
            "float": "float",
            "boolean": "boolean",
            "date": "date",
            "timestamp": "timestamp",
        }
        if delta_type in primitive:
            return primitive[delta_type]
        if delta_type.startswith("decimal("):
            return delta_type
        raise ValueError(f"Tipo Delta no soportado por el registro: {delta_type}")
    if delta_type.get("type") == "array":
        return f"array<{_athena_type(delta_type['elementType'])}>"
    if delta_type.get("type") == "map":
        return f"map<{_athena_type(delta_type['keyType'])},{_athena_type(delta_type['valueType'])}>"
    if delta_type.get("type") == "struct":
        fields = ",".join(
            f"{field['name']}:{_athena_type(field['type'])}" for field in delta_type["fields"]
        )
        return f"struct<{fields}>"
    raise ValueError(f"Tipo Delta no soportado por el registro: {delta_type}")


def _s3_uri(bucket, key):
    return f"s3://{bucket}/{key}"


def _latest_delta_metadata(s3, bucket, table_prefix):
    prefix = f"{table_prefix}/_delta_log/"
    commits = []
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        commits.extend(
            item["Key"] for item in page.get("Contents", [])
            if re.fullmatch(r"\d{20}\.json", item["Key"].rsplit("/", 1)[-1])
        )
    commits.sort()
    if not commits:
        raise FileNotFoundError(f"No hay commits Delta JSON en s3://{bucket}/{prefix}")

    schema = None
    for key in reversed(commits):
        body = s3.get_object(Bucket=bucket, Key=key)["Body"].read().decode("utf-8")
        for line in body.splitlines():
            action = json.loads(line)
            metadata = action.get("metaData")
            if metadata:
                schema = json.loads(metadata["schemaString"])
                break
        if schema is not None:
            break
    if schema is None:
        raise ValueError(f"No se encontró metaData de Delta en s3://{bucket}/{prefix}")
    columns = [
        {"Name": field["name"], "Type": _athena_type(field["type"])}
        for field in schema["fields"]
    ]
    return _s3_uri(bucket, commits[-1]), columns


def register(database, lake_uri, region):
    import boto3

    parsed = urlparse(lake_uri)
    if parsed.scheme not in {"s3", "s3a"} or not parsed.netloc:
        raise ValueError("--lake-uri debe ser s3:// o s3a://<bucket>/<prefijo>")
    bucket = parsed.netloc
    lake_prefix = parsed.path.strip("/")
    glue = boto3.client("glue", region_name=region)
    s3 = boto3.client("s3", region_name=region)

    try:
        glue.get_database(Name=database)
    except glue.exceptions.EntityNotFoundException:
        glue.create_database(DatabaseInput={"Name": database})

    for table in TABLES:
        prefix = "/".join(part for part in (lake_prefix, "gold", table) if part)
        metadata_location, columns = _latest_delta_metadata(s3, bucket, prefix)
        table_input = {
            "Name": table,
            "TableType": "EXTERNAL_TABLE",
            "StorageDescriptor": {
                "Columns": columns,
                "Location": _s3_uri(bucket, prefix),
                "InputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
                "OutputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
                "SerdeInfo": {
                    "SerializationLibrary": "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe",
                    "Parameters": {"serialization.format": "1"},
                },
            },
            "Parameters": {
                "EXTERNAL": "TRUE",
                "classification": "delta",
                "table_type": "DELTA",
                "metadata_location": metadata_location,
            },
        }
        try:
            glue.get_table(DatabaseName=database, Name=table)
        except glue.exceptions.EntityNotFoundException:
            glue.create_table(DatabaseName=database, TableInput=table_input)
        else:
            glue.update_table(DatabaseName=database, TableInput=table_input)
        print(f"registrada {database}.{table} -> {metadata_location}")


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--database", default=os.environ.get("GLUE_DATABASE", "fraudradar"))
    parser.add_argument("--lake-uri", default=os.environ.get("LAKE_URI"))
    parser.add_argument("--region", default=os.environ.get("AWS_REGION", "us-east-2"))
    args = parser.parse_args()
    if not args.lake_uri:
        parser.error("define --lake-uri o LAKE_URI con la ubicación s3a:// del lake")
    register(args.database, args.lake_uri, args.region)


if __name__ == "__main__":
    main()
