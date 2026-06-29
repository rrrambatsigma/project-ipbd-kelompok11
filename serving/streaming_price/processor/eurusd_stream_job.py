import os

from dotenv import load_dotenv
from loguru import logger
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from schema import EURUSD_SCHEMA

load_dotenv()

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "market.eurusd.raw")

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin123")
MINIO_BUCKET_MARKET = os.getenv("MINIO_BUCKET_MARKET", "market-processed")

SILVER_PATH = f"s3a://{MINIO_BUCKET_MARKET}/eurusd/silver"
GOLD_PATH = f"s3a://{MINIO_BUCKET_MARKET}/eurusd/gold_window_1m"
CHECKPOINT_SILVER = f"s3a://{MINIO_BUCKET_MARKET}/checkpoints/eurusd_silver"
CHECKPOINT_GOLD = f"s3a://{MINIO_BUCKET_MARKET}/checkpoints/eurusd_gold_1m"


def create_spark_session() -> SparkSession:
    spark = (
        SparkSession.builder
        .appName("EURUSDStreamingProcessor")
        .master("local[*]")

        # Kafka + S3A dependencies
        .config(
            "spark.jars.packages",
            ",".join([
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1",
                "org.apache.hadoop:hadoop-aws:3.3.4",
                "com.amazonaws:aws-java-sdk-bundle:1.12.262",
            ])
        )

        # MinIO / S3A
        .config("spark.hadoop.fs.s3a.endpoint", f"http://{MINIO_ENDPOINT}")
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
        )

        # Small local project tuning
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")
    return spark


def main():
    logger.info("=" * 60)
    logger.info("EUR/USD Spark Structured Streaming Processor")
    logger.info(f"Kafka bootstrap : {KAFKA_BOOTSTRAP_SERVERS}")
    logger.info(f"Kafka topic     : {KAFKA_TOPIC}")
    logger.info(f"Silver path     : {SILVER_PATH}")
    logger.info(f"Gold path       : {GOLD_PATH}")
    logger.info("=" * 60)

    spark = create_spark_session()

    raw = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "latest")
        .load()
    )

    parsed = (
        raw
        .selectExpr("CAST(value AS STRING) AS json_value")
        .select(F.from_json(F.col("json_value"), EURUSD_SCHEMA).alias("data"))
        .select("data.*")
    )

    silver = (
        parsed
        .withColumn("event_ts", F.to_timestamp("event_time"))
        .withColumn("ingestion_ts", F.to_timestamp("ingestion_time"))
        .filter(F.col("canonical_symbol") == "EUR/USD")
        .filter(F.col("price").isNotNull())
        .filter(F.col("price") > 0)
        .filter(F.col("event_ts").isNotNull())
        .withColumn("date", F.to_date("event_ts"))
    )

    # Raw/clean tick-level stream
    silver_query = (
        silver.writeStream
        .format("parquet")
        .outputMode("append")
        .option("path", SILVER_PATH)
        .option("checkpointLocation", CHECKPOINT_SILVER)
        .partitionBy("date")
        .start()
    )

    # 1-minute aggregation stream
    gold = (
        silver
        .withWatermark("event_ts", "2 minutes")
        .groupBy(F.window("event_ts", "1 minute"))
        .agg(
            F.count("*").alias("tick_count"),
            F.avg("price").alias("avg_price"),
            F.min("price").alias("min_price"),
            F.max("price").alias("max_price"),
            F.first("price").alias("open_price"),
            F.last("price").alias("close_price"),
        )
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "tick_count",
            "open_price",
            "close_price",
            "avg_price",
            "min_price",
            "max_price",
        )
        .withColumn("date", F.to_date("window_start"))
    )

    gold_query = (
        gold.writeStream
        .format("parquet")
        .outputMode("append")
        .option("path", GOLD_PATH)
        .option("checkpointLocation", CHECKPOINT_GOLD)
        .partitionBy("date")
        .start()
    )

    logger.success("Streaming queries started.")
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
