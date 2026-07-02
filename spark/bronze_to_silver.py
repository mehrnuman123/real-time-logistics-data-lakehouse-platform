from spark_session import spark
from pyspark.sql.functions import col, to_timestamp, when

df = spark.read.format("delta").load("delta/bronze/shipments")

silver_df = (
    df
    .dropDuplicates(["event_id"])
    .withColumn("event_timestamp", to_timestamp(col("event_time")))
    .filter(col("event_id").isNotNull())
    .filter(col("shipment_id").isNotNull())
    .filter(col("status").isNotNull())
    .filter(col("fuel_level_pct").between(0, 100))
    .filter(col("vehicle_speed_kmh") >= 0)
    .withColumn(
        "delivery_progress",
        when(col("status") == "CREATED", 1)
        .when(col("status") == "PICKED_UP", 2)
        .when(col("status") == "IN_TRANSIT", 3)
        .when(col("status") == "ARRIVED_AT_HUB", 4)
        .when(col("status") == "OUT_FOR_DELIVERY", 5)
        .when(col("status") == "DELIVERED", 6)
        .otherwise(0)
    )
    .withColumn(
        "is_delayed",
        col("elapsed_minutes") > col("estimated_delivery_minutes")
    )
    .withColumn(
        "fuel_risk",
        when(col("fuel_level_pct") < 15, "LOW_FUEL").otherwise("NORMAL")
    )
    .withColumn(
        "speed_risk",
        when(col("vehicle_speed_kmh") > 100, "HIGH_SPEED").otherwise("NORMAL")
    )
    .withColumn(
        "temperature_risk",
        when(col("temperature_c") < 0, "COLD_RISK")
        .when(col("temperature_c") > 22, "HEAT_RISK")
        .otherwise("NORMAL")
    )
)

silver_df.write.format("delta").mode("overwrite").save("delta/silver/shipments")

print("Silver rebuilt")
print("Rows:", silver_df.count())

spark.stop()