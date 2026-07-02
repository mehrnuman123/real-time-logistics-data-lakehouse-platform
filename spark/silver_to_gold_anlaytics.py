from spark_session import spark
from pyspark.sql.functions import count, avg, max, min, col, when, countDistinct


df = spark.read.format("delta").load("delta/silver/shipments")



warehouse_kpi = (
    df.groupBy("warehouse")
    .agg(
        countDistinct("shipment_id").alias("total_shipments"),
        count("*").alias("total_events"),
        avg("estimated_delivery_minutes").alias("avg_est_delivery_minutes"),
        avg("temperature_c").alias("avg_temperature")
    )
)


status_summary = (
    df.groupBy("status")
    .agg(
        countDistinct("shipment_id").alias("shipments_count"),
        count("*").alias("events_count")
    )
)

driver_performance = (
    df.groupBy("driver_id")
    .agg(
        countDistinct("shipment_id").alias("shipments_handled"),
        avg("vehicle_speed_kmh").alias("avg_speed"),
        avg("fuel_level_pct").alias("avg_fuel"),
        count("*").alias("total_events")
    )
)


vehicle_health = (
    df.groupBy("vehicle_id")
    .agg(
        avg("fuel_level_pct").alias("avg_fuel"),
        max("vehicle_speed_kmh").alias("max_speed"),
        count(when(col("fuel_level_pct") < 15, True)).alias("low_fuel_events"),
        count(when(col("vehicle_speed_kmh") > 100, True)).alias("high_speed_events")
    )
)

warehouse_kpi.write.format("delta").mode("overwrite").save("delta/gold/warehouse_kpi")

status_summary.write.format("delta").mode("overwrite").save("delta/gold/status_summary")

driver_performance.write.format("delta").mode("overwrite").save("delta/gold/driver_performance")

vehicle_health.write.format("delta").mode("overwrite").save("delta/gold/vehicle_health")


warehouse_kpi.show()
status_summary.show()
driver_performance.show()
vehicle_health.show()

print("Gold layer created successfully")




spark.stop()