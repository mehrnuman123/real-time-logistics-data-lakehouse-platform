from spark_session import spark
from pyspark.sql.functions import count, countDistinct, avg, sum, when, col

fact = spark.read.format("delta").load("delta/gold/fact_shipment_events")
warehouse = spark.read.format("delta").load("delta/gold/dim_warehouse")
driver = spark.read.format("delta").load("delta/gold/dim_driver")
vehicle = spark.read.format("delta").load("delta/gold/dim_vehicle")

warehouse_kpi = (
    fact.join(warehouse, "warehouse_sk")
    .groupBy("warehouse")
    .agg(
        countDistinct("shipment_id").alias("total_shipments"),
        count(when(col("status") == "DELIVERED", True)).alias("delivered_shipments"),
        count(when(col("is_delayed") == True, True)).alias("delayed_events"),
        avg("elapsed_minutes").alias("avg_elapsed_minutes")
    )
)

driver_kpi = (
    fact.join(driver, "driver_sk")
    .groupBy("driver_id")
    .agg(
        countDistinct("shipment_id").alias("shipments_handled"),
        count(when(col("status") == "DELIVERED", True)).alias("completed_deliveries"),
        avg("vehicle_speed_kmh").alias("avg_speed"),
        avg("fuel_level_pct").alias("avg_fuel")
    )
)

vehicle_kpi = (
    fact.join(vehicle, "vehicle_sk")
    .groupBy("vehicle_id")
    .agg(
        avg("fuel_level_pct").alias("avg_fuel"),
        count(when(col("fuel_risk") == "LOW_FUEL", True)).alias("low_fuel_events"),
        count(when(col("speed_risk") == "HIGH_SPEED", True)).alias("high_speed_events")
    )
)

status_kpi = (
    fact.groupBy("status")
    .agg(
        count("*").alias("total_events"),
        countDistinct("shipment_id").alias("total_shipments")
    )
)

warehouse_kpi.write.format("delta").mode("overwrite").save("delta/gold/marts/warehouse_kpi")
driver_kpi.write.format("delta").mode("overwrite").save("delta/gold/marts/driver_kpi")
vehicle_kpi.write.format("delta").mode("overwrite").save("delta/gold/marts/vehicle_kpi")
status_kpi.write.format("delta").mode("overwrite").save("delta/gold/marts/status_kpi")

print("Gold KPI marts created")

spark.stop()