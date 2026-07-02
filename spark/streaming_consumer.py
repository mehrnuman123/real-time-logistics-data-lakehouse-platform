from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, current_timestamp
from pyspark.sql.types import (
    StructType, StructField, StringType,
    DoubleType, IntegerType, BooleanType
)


from pyspark.sql import SparkSession

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
    StructField("elapsed_minutes", IntegerType()),
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



parsed_df = (
    raw_df
    .select(
        #converts binary to string
        col("key").cast("string").alias("kafka_key"),
        col("value").cast("string").alias("raw_json"),
        # renaming to meaningful names 
        col("topic").alias("kafka_topic"),
        col("partition").alias("kafka_partition"),
        col("offset").alias("kafka_offset"),
        col("timestamp").alias("kafka_timestamp")
    )
    # parses the JSON according to your schema, creates new column data
    .withColumn("data", from_json(col("raw_json"), schema))
    # data is still single struct and also it contains all the fields inside it,
    .select(
        "kafka_key",
        "raw_json",
        "kafka_topic",
        "kafka_partition",
        "kafka_offset",
        "kafka_timestamp",
          # col("data.*") expands the struct into individual columns.
        col("data.*")
        
    )
    # new column
    .withColumn("ingestion_timestamp", current_timestamp())
)







def process_batch(df, batch_id):
    print(f"\n========== Batch {batch_id} ==========")

    offsets = df.select("kafka_offset").collect()

    if offsets:
        print(f"Processing offsets {offsets[0][0]} -> {offsets[-1][0]}")

    (
        df.write
        .format("delta")
        .mode("append")
        .save("delta/bronze/shipments")
    )

query = (
    parsed_df.writeStream
    .foreachBatch(process_batch)
    .option("checkpointLocation", "delta/checkpoints/bronze_shipments")
    .start()
)

query.awaitTermination()



