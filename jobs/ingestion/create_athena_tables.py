"""
create_athena_tables.py
-----------------------
Tạo Glue Data Catalog database + external tables trỏ vào
refined Parquet trên S3 → query được bằng Athena ngay.

Run (local, cần AWS credentials):
  python jobs/create_athena_tables.py

Hoặc sau khi transform xong, gọi từ pipeline script.
"""

import sys
import time
import os

from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(__file__), '.env'))

import boto3
from botocore.exceptions import ClientError

sys.path.insert(0, os.path.dirname(__file__))
from config import configuration

# ── Config ────────────────────────────────────────────────────────────────────
REGION      = configuration['AWS_REGION']
BUCKET      = configuration['AWS_BUCKET_NAME']
DB_NAME     = "smartcity_db"
OUTPUT_LOC  = f"s3://{BUCKET}/athena-results/"

# ── DDL templates ─────────────────────────────────────────────────────────────

TABLES = {
    "vehicle_data": {
        "s3_path": f"s3://{BUCKET}/refined/vehicle_data/",
        "ddl": f"""
CREATE EXTERNAL TABLE IF NOT EXISTS {DB_NAME}.vehicle_data (
    id         STRING,
    vehicle_id STRING,
    timestamp  TIMESTAMP,
    location   STRING,
    speed      DOUBLE,
    direction  STRING,
    make       STRING,
    model      STRING,
    year       INT,
    fuelType   STRING,
    hour       INT,
    speed_kmh  DOUBLE,
    is_ev      BOOLEAN
)
PARTITIONED BY (date STRING)
STORED AS PARQUET
LOCATION 's3://{BUCKET}/refined/vehicle_data/'
TBLPROPERTIES ('parquet.compress'='SNAPPY');
""",
    },
    "gps_data": {
        "s3_path": f"s3://{BUCKET}/refined/gps_data/",
        "ddl": f"""
CREATE EXTERNAL TABLE IF NOT EXISTS {DB_NAME}.gps_data (
    id          STRING,
    vehicle_id  STRING,
    timestamp   TIMESTAMP,
    speed       DOUBLE,
    direction   STRING,
    vehicleType STRING,
    hour        INT
)
PARTITIONED BY (date STRING)
STORED AS PARQUET
LOCATION 's3://{BUCKET}/refined/gps_data/'
TBLPROPERTIES ('parquet.compress'='SNAPPY');
""",
    },
    "traffic_data": {
        "s3_path": f"s3://{BUCKET}/refined/traffic_data/",
        "ddl": f"""
CREATE EXTERNAL TABLE IF NOT EXISTS {DB_NAME}.traffic_data (
    id         STRING,
    vehicle_id STRING,
    camera_id  STRING,
    location   STRING,
    timestamp  TIMESTAMP
)
PARTITIONED BY (date STRING)
STORED AS PARQUET
LOCATION 's3://{BUCKET}/refined/traffic_data/'
TBLPROPERTIES ('parquet.compress'='SNAPPY');
""",
    },
    "weather_data": {
        "s3_path": f"s3://{BUCKET}/refined/weather_data/",
        "ddl": f"""
CREATE EXTERNAL TABLE IF NOT EXISTS {DB_NAME}.weather_data (
    id               STRING,
    vehicle_id       STRING,
    location         STRING,
    timestamp        TIMESTAMP,
    temperature      DOUBLE,
    weatherCondition STRING,
    precipitation    DOUBLE,
    windSpeed        DOUBLE,
    humidity         INT,
    airQualityIndex  DOUBLE,
    heat_index       DOUBLE,
    aqi_category     STRING
)
PARTITIONED BY (date STRING)
STORED AS PARQUET
LOCATION 's3://{BUCKET}/refined/weather_data/'
TBLPROPERTIES ('parquet.compress'='SNAPPY');
""",
    },
    "emergency_data": {
        "s3_path": f"s3://{BUCKET}/refined/emergency_data/",
        "ddl": f"""
CREATE EXTERNAL TABLE IF NOT EXISTS {DB_NAME}.emergency_data (
    id               STRING,
    vehicle_id       STRING,
    incidentId       STRING,
    type             STRING,
    timestamp        TIMESTAMP,
    location         STRING,
    status           STRING,
    description      STRING,
    is_active        BOOLEAN,
    is_real_incident BOOLEAN
)
PARTITIONED BY (date STRING)
STORED AS PARQUET
LOCATION 's3://{BUCKET}/refined/emergency_data/'
TBLPROPERTIES ('parquet.compress'='SNAPPY');
""",
    },
}


# ── Helpers ───────────────────────────────────────────────────────────────────

def run_query(athena, sql: str, description: str) -> str:
    """Submit Athena query, poll until done, return QueryExecutionId."""
    resp = athena.start_query_execution(
        QueryString=sql,
        QueryExecutionContext={"Database": DB_NAME},
        ResultConfiguration={"OutputLocation": OUTPUT_LOC},
    )
    qid = resp["QueryExecutionId"]
    print(f"    [{description}] submitted — id={qid}")

    for _ in range(60):  # max 60 × 2s = 2 min
        state = athena.get_query_execution(QueryExecutionId=qid)[
            "QueryExecution"]["Status"]["State"]
        if state in ("SUCCEEDED", "FAILED", "CANCELLED"):
            break
        time.sleep(2)

    if state != "SUCCEEDED":
        reason = athena.get_query_execution(QueryExecutionId=qid)[
            "QueryExecution"]["Status"].get("StateChangeReason", "")
        raise RuntimeError(f"Query {qid} ended with {state}: {reason}")

    print(f"    [{description}] SUCCEEDED")
    return qid


def ensure_database(athena) -> None:
    run_query(
        athena,
        f"CREATE DATABASE IF NOT EXISTS {DB_NAME}",
        f"CREATE DATABASE {DB_NAME}",
    )


def load_partitions(athena, table: str) -> None:
    run_query(
        athena,
        f"MSCK REPAIR TABLE {DB_NAME}.{table}",
        f"MSCK REPAIR {table}",
    )


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    print(f"\n{'='*55}")
    print(f"  SmartCity — Create Athena Tables")
    print(f"  Region : {REGION}")
    print(f"  Bucket : {BUCKET}")
    print(f"  DB     : {DB_NAME}")
    print(f"{'='*55}\n")

    athena = boto3.client(
        "athena",
        region_name=REGION,
        aws_access_key_id=configuration['AWS_ACCESS_KEY'],
        aws_secret_access_key=configuration['AWS_SECRET_KEY'],
    )

    # 1. Create database
    print("[1/3] Creating database...")
    ensure_database(athena)

    # 2. Create tables
    print("\n[2/3] Creating tables...")
    results = {}
    for table_name, meta in TABLES.items():
        print(f"\n  → {table_name}")
        try:
            run_query(athena, meta["ddl"], f"CREATE TABLE {table_name}")
            results[table_name] = "created"
        except Exception as exc:
            print(f"  ERROR: {exc}")
            results[table_name] = f"FAIL — {exc}"

    # 3. Load partitions
    print("\n[3/3] Loading partitions (MSCK REPAIR)...")
    for table_name in TABLES:
        if results.get(table_name) == "created":
            try:
                load_partitions(athena, table_name)
            except Exception as exc:
                print(f"  WARN: {table_name} partition repair failed — {exc}")

    # Summary
    print(f"\n{'='*55}")
    print("  Summary")
    print(f"{'='*55}")
    for t, s in results.items():
        icon = "✓" if s == "created" else "✗"
        print(f"  {icon}  {t:<22}  {s}")
    print(f"\n  Athena output : {OUTPUT_LOC}")
    print(f"  Query in console: https://{REGION}.console.aws.amazon.com/athena\n")


if __name__ == "__main__":
    main()