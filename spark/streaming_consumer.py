from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, current_timestamp
from pyspark.sql.types import (
    StructType, StructField, StringType,
    DoubleType, IntegerType, BooleanType
)

spark = (
    SparkSession.builder
    .appName("ShipmentKafkaToBronze")
    .master("local[*]")
    .config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,"
        "io.delta:delta-spark_2.12:3.2.0"
    )
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
)

schema = StructType([
    StructField("event_id", StringType()),
    StructField("shipment_id", StringType()),
    StructField("driver_id", StringType()),
    StructField("vehicle_id", StringType()),
    StructField("warehouse", StringType()),
    StructField("destination", StringType()),
    StructField("status", StringType()),
    StructField("temperature_c", DoubleType()),
    StructField("vehicle_speed_kmh", IntegerType()),
    StructField("fuel_level_pct", IntegerType()),
    StructField("is_fragile", BooleanType()),
    StructField("estimated_delivery_minutes", IntegerType()),
    StructField("event_time", StringType())
])

raw_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "shipment_events")
    .option("startingOffsets", "earliest")
    .load()
)

print("raw_df ============",raw_df)

parsed_df = (
    raw_df
    .select(
        col("key").cast("string").alias("kafka_key"),
        col("value").cast("string").alias("raw_json"),
        col("topic").alias("kafka_topic"),
        col("partition").alias("kafka_partition"),
        col("offset").alias("kafka_offset"),
        col("timestamp").alias("kafka_timestamp")
    )
    .withColumn("data", from_json(col("raw_json"), schema))
    .select(
        "kafka_key",
        "raw_json",
        "kafka_topic",
        "kafka_partition",
        "kafka_offset",
        "kafka_timestamp",
        col("data.*")
    )
    .withColumn("ingestion_timestamp", current_timestamp())
)

print("parsed data ================",  parsed_df)

query = (
    parsed_df.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", "delta/checkpoints/bronze_shipments")
    .start("delta/bronze/shipments")
)

query.awaitTermination()