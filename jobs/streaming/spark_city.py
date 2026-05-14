from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, IntegerType, DoubleType

from config import configuration

# S3 bucket
BUCKET = "s3a://smartcity-data-pipeline"


def main():
    spark = SparkSession.builder.appName("SmartCityStreaming") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.access.key", configuration.get('AWS_ACCESS_KEY')) \
        .config("spark.hadoop.fs.s3a.secret.key", configuration.get('AWS_SECRET_KEY')) \
        .config('spark.hadoop.fs.s3a.aws.credentials.provider',
                'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider') \
        .getOrCreate()

    spark.sparkContext.setLogLevel('WARN')

    # ── Schemas ────────────────────────────────────────────────────────────────

    vehicleSchema = StructType([
        StructField("id", StringType(), True),
        StructField("vehicle_id", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("location", StringType(), True),
        StructField("speed", DoubleType(), True),
        StructField("direction", StringType(), True),
        StructField("make", StringType(), True),
        StructField("model", StringType(), True),
        StructField("year", IntegerType(), True),
        StructField("fuelType", StringType(), True),
    ])

    gpsSchema = StructType([
        StructField("id", StringType(), True),
        StructField("vehicle_id", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("speed", DoubleType(), True),
        StructField("direction", StringType(), True),
        StructField("vehicleType", StringType(), True),
    ])

    trafficSchema = StructType([
        StructField("id", StringType(), True),
        StructField("vehicle_id", StringType(), True),
        StructField("camera_id", StringType(), True),
        StructField("location", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("snapshot", StringType(), True),
    ])

    weatherSchema = StructType([
        StructField("id", StringType(), True),
        StructField("vehicle_id", StringType(), True),
        StructField("location", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("temperature", DoubleType(), True),
        StructField("weatherCondition", StringType(), True),
        StructField("precipitation", DoubleType(), True),
        StructField("windSpeed", DoubleType(), True),
        StructField("humidity", IntegerType(), True),
        StructField("airQualityIndex", DoubleType(), True),
    ])

    emergencySchema = StructType([
        StructField("id", StringType(), True),
        StructField("vehicle_id", StringType(), True),
        StructField("incidentId", StringType(), True),
        StructField("type", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("location", StringType(), True),
        StructField("status", StringType(), True),
        StructField("description", StringType(), True),
    ])

    # ── Helpers ────────────────────────────────────────────────────────────────

    def read_kafka_topic(topic: str, schema: StructType) -> DataFrame:
        return (
            spark.readStream
            .format('kafka')
            .option('kafka.bootstrap.servers', 'broker:29092')
            .option('subscribe', topic)
            .option('startingOffsets', 'latest')
            .option('failOnDataLoss', 'false')
            .load()
            .selectExpr('CAST(value AS STRING)')
            .select(from_json(col('value'), schema).alias('data'))
            .select('data.*')
            .withWatermark('timestamp', '2 minutes')
        )

    def stream_writer(df: DataFrame, topic: str):
        """Write a streaming DataFrame to S3 as Parquet, with checkpoint."""
        return (
            df.writeStream
            .format('parquet')
            .option('checkpointLocation', f"{BUCKET}/checkpoints/{topic}")
            .option('path', f"{BUCKET}/data/{topic}")
            .outputMode('append')
            .start()
        )

    # ── Read from Kafka ────────────────────────────────────────────────────────

    vehicleDF   = read_kafka_topic('vehicle_data',   vehicleSchema)
    gpsDF       = read_kafka_topic('gps_data',       gpsSchema)
    trafficDF   = read_kafka_topic('traffic_data',   trafficSchema)
    weatherDF   = read_kafka_topic('weather_data',   weatherSchema)
    emergencyDF = read_kafka_topic('emergency_data', emergencySchema)

    # ── Write to S3 ───────────────────────────────────────────────────────────

    queries = [
        stream_writer(vehicleDF,   'vehicle_data'),
        stream_writer(gpsDF,       'gps_data'),
        stream_writer(trafficDF,   'traffic_data'),
        stream_writer(weatherDF,   'weather_data'),
        stream_writer(emergencyDF, 'emergency_data'),
    ]

    for q in queries:
        q.awaitTermination()


if __name__ == "__main__":
    main()