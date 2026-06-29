from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType
)

EURUSD_SCHEMA = StructType([
    StructField("symbol", StringType(), True),
    StructField("canonical_symbol", StringType(), True),
    StructField("instrument_type", StringType(), True),
    StructField("source", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("bid", DoubleType(), True),
    StructField("ask", DoubleType(), True),
    StructField("volume", DoubleType(), True),
    StructField("event_time", StringType(), True),
    StructField("ingestion_time", StringType(), True),
    StructField("quality_status", StringType(), True),
])
