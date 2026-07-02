from spark_session import spark
from pyspark.sql.functions import col, monotonically_increasing_id

df = spark.read.format("delta").load("delta/silver/shipments")

dim_driver = (
    df.select("driver_id")
    .dropDuplicates()
    .filter(col("driver_id").isNotNull())
    .withColumn("driver_sk", monotonically_increasing_id())
)

dim_vehicle = (
    df.select("vehicle_id")
    .dropDuplicates()
    .filter(col("vehicle_id").isNotNull())
    .withColumn("vehicle_sk", monotonically_increasing_id())
)

dim_warehouse = (
    df.select("warehouse")
    .dropDuplicates()
    .filter(col("warehouse").isNotNull())
    .withColumn("warehouse_sk", monotonically_increasing_id())
)

dim_destination = (
    df.select("destination")
    .dropDuplicates()
    .filter(col("destination").isNotNull())
    .withColumn("destination_sk", monotonically_increasing_id())
)

fact_shipment_events = (
    df
    .join(dim_driver, "driver_id", "left")
    .join(dim_vehicle, "vehicle_id", "left")
    .join(dim_warehouse, "warehouse", "left")
    .join(dim_destination, "destination", "left")
    .select(
        "event_id",
        "shipment_id",
        "driver_sk",
        "vehicle_sk",
        "warehouse_sk",
        "destination_sk",
        "status",
        "delivery_progress",
        "temperature_c",
        "vehicle_speed_kmh",
        "fuel_level_pct",
        "is_fragile",
        "estimated_delivery_minutes",
        "elapsed_minutes",
        "is_delayed",
        "event_timestamp",
        "fuel_risk",
        "speed_risk",
        "temperature_risk"
    )
)

dim_driver.write.format("delta").mode("overwrite").save("delta/gold/dim_driver")
dim_vehicle.write.format("delta").mode("overwrite").save("delta/gold/dim_vehicle")
dim_warehouse.write.format("delta").mode("overwrite").save("delta/gold/dim_warehouse")
dim_destination.write.format("delta").mode("overwrite").save("delta/gold/dim_destination")
fact_shipment_events.write.format("delta").mode("overwrite").save("delta/gold/fact_shipment_events")

print("Gold star schema rebuilt")

spark.stop()