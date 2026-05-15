"""
transform_parquet.py
--------------------
Đọc raw Parquet từ S3 (ghi bởi spark_city.py),
áp dụng transformations, ghi vào S3 refined layer.

Run (trong container):
  spark-submit --master local[*] \
    --py-files /opt/spark/jobs/config.py \
    /opt/spark/jobs/transformations/transform_parquet.py
"""

import sys
import os

# ── Load .env trước khi import config ────────────────────────────────────────
from dotenv import load_dotenv
load_dotenv('/opt/spark/jobs/.env')

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, TimestampType, IntegerType, DoubleType
)

sys.path.insert(0, '/opt/spark/jobs')
from config import configuration

# ── Constants ─────────────────────────────────────────────────────────────────
RAW_BUCKET     = f"s3a://{configuration['AWS_BUCKET_NAME']}"
REFINED_BUCKET = f"s3a://{configuration['AWS_BUCKET_NAME']}/refined"

TOPICS = [
    'vehicle_data',
    'gps_data',
    'traffic_data',
    'weather_data',
    'emergency_data',
]


# ── Schemas ───────────────────────────────────────────────────────────────────

SCHEMAS: dict = {
    'vehicle_data': StructType([
        StructField("id",         StringType(),    True),
        StructField("vehicle_id", StringType(),    True),
        StructField("timestamp",  TimestampType(), True),
        StructField("location",   StringType(),    True),
        StructField("speed",      DoubleType(),    True),
        StructField("direction",  StringType(),    True),
        StructField("make",       StringType(),    True),
        StructField("model",      StringType(),    True),
        StructField("year",       IntegerType(),   True),
        StructField("fuelType",   StringType(),    True),
    ]),
    'gps_data': StructType([
        StructField("id",          StringType(),    True),
        StructField("vehicle_id",  StringType(),    True),
        StructField("timestamp",   TimestampType(), True),
        StructField("speed",       DoubleType(),    True),
        StructField("direction",   StringType(),    True),
        StructField("vehicleType", StringType(),    True),
    ]),
    'traffic_data': StructType([
        StructField("id",         StringType(),    True),
        StructField("vehicle_id", StringType(),    True),
        StructField("camera_id",  StringType(),    True),
        StructField("location",   StringType(),    True),
        StructField("timestamp",  TimestampType(), True),
        StructField("snapshot",   StringType(),    True),
    ]),
    'weather_data': StructType([
        StructField("id",               StringType(),    True),
        StructField("vehicle_id",       StringType(),    True),
        StructField("location",         StringType(),    True),
        StructField("timestamp",        TimestampType(), True),
        StructField("temperature",      DoubleType(),    True),
        StructField("weatherCondition", StringType(),    True),
        StructField("precipitation",    DoubleType(),    True),
        StructField("windSpeed",        DoubleType(),    True),
        StructField("humidity",         IntegerType(),   True),
        StructField("airQualityIndex",  DoubleType(),    True),
    ]),
    'emergency_data': StructType([
        StructField("id",          StringType(),    True),
        StructField("vehicle_id",  StringType(),    True),
        StructField("incidentId",  StringType(),    True),
        StructField("type",        StringType(),    True),
        StructField("timestamp",   TimestampType(), True),
        StructField("location",    StringType(),    True),
        StructField("status",      StringType(),    True),
        StructField("description", StringType(),    True),
    ]),
}


# ── Spark session ─────────────────────────────────────────────────────────────

def create_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("SmartCity-Transform-Parquet")
        .config("spark.hadoop.fs.s3a.impl",
                "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.access.key",
                configuration['AWS_ACCESS_KEY'])
        .config("spark.hadoop.fs.s3a.secret.key",
                configuration['AWS_SECRET_KEY'])
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
        .getOrCreate()
    )


# ── Read helpers ──────────────────────────────────────────────────────────────

def read_raw(spark: SparkSession, topic: str) -> DataFrame:
    path = f"{RAW_BUCKET}/data/{topic}"
    print(f"  Reading  : {path}")
    return spark.read.schema(SCHEMAS[topic]).parquet(path)


# ── Topic-specific transformations ────────────────────────────────────────────

def transform_vehicle(df: DataFrame) -> DataFrame:
    return (
        df
        .dropDuplicates(["id"])
        .filter(F.col("speed").between(0, 200))
        .withColumn("date",       F.to_date("timestamp"))
        .withColumn("hour",       F.hour("timestamp"))
        .withColumn("speed_kmh",  F.round("speed", 1))
        .withColumn("is_ev",      F.col("fuelType") == "Electric")
    )


def transform_gps(df: DataFrame) -> DataFrame:
    return (
        df
        .dropDuplicates(["id"])
        .filter(F.col("speed").between(0, 200))
        .withColumn("date", F.to_date("timestamp"))
        .withColumn("hour", F.hour("timestamp"))
    )


def transform_traffic(df: DataFrame) -> DataFrame:
    return (
        df
        .dropDuplicates(["id"])
        .withColumn("date", F.to_date("timestamp"))
        # snapshot là Base64 placeholder — drop khỏi refined để tiết kiệm storage
        .drop("snapshot")
    )


def transform_weather(df: DataFrame) -> DataFrame:
    return (
        df
        .dropDuplicates(["id"])
        .filter(F.col("temperature").between(-10, 60))
        .filter(F.col("humidity").between(0, 100))
        .withColumn("date",        F.to_date("timestamp"))
        .withColumn("heat_index",  F.round(
            F.col("temperature") + 0.33 * F.col("humidity") / 10 - 4.0, 1
        ))
        .withColumn("aqi_category",
            F.when(F.col("airQualityIndex") <= 50,  "Good")
             .when(F.col("airQualityIndex") <= 100, "Moderate")
             .when(F.col("airQualityIndex") <= 150, "Unhealthy for Sensitive")
             .otherwise("Unhealthy")
        )
    )


def transform_emergency(df: DataFrame) -> DataFrame:
    return (
        df
        .dropDuplicates(["id"])
        .withColumn("date",        F.to_date("timestamp"))
        .withColumn("is_active",   F.col("status") == "Active")
        .withColumn("is_real_incident",
                    F.col("type") != "None")
    )


TRANSFORMERS = {
    'vehicle_data':   transform_vehicle,
    'gps_data':       transform_gps,
    'traffic_data':   transform_traffic,
    'weather_data':   transform_weather,
    'emergency_data': transform_emergency,
}


# ── Write helper ──────────────────────────────────────────────────────────────

def write_refined(df: DataFrame, topic: str) -> None:
    out = f"{REFINED_BUCKET}/{topic}"
    print(f"  Writing  : {out}")
    (
        df.write
        .mode("overwrite")
        .partitionBy("date")
        .parquet(out)
    )
    count = df.count()
    print(f"  Rows     : {count:,}")


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    spark = create_spark()
    spark.sparkContext.setLogLevel("WARN")

    print(f"\n{'='*55}")
    print(f"  SmartCity — Transform Parquet")
    print(f"  Bucket : {configuration['AWS_BUCKET_NAME']}")
    print(f"{'='*55}")

    results = {}
    for topic in TOPICS:
        print(f"\n[{topic}]")
        try:
            raw = read_raw(spark, topic)
            refined = TRANSFORMERS[topic](raw)
            write_refined(refined, topic)
            results[topic] = "OK"
        except Exception as exc:
            print(f"  ERROR: {exc}")
            results[topic] = f"FAIL — {exc}"

    print(f"\n{'='*55}")
    print("  Summary")
    print(f"{'='*55}")
    for topic, status in results.items():
        icon = "✓" if status == "OK" else "✗"
        print(f"  {icon}  {topic:<20}  {status}")
    print()

    spark.stop()


if __name__ == "__main__":
    main()