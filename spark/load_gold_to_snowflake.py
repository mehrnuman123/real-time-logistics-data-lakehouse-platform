from spark_session import spark
from dotenv import load_dotenv
import os

load_dotenv()

sf_options = {
    "sfURL": f"{os.getenv('SNOWFLAKE_ACCOUNT')}.snowflakecomputing.com",
    "sfUser": os.getenv("SNOWFLAKE_USER"),
    "sfPassword": os.getenv("SNOWFLAKE_PASSWORD"),
    "sfDatabase": os.getenv("SNOWFLAKE_DATABASE"),
    "sfSchema": os.getenv("SNOWFLAKE_SCHEMA"),
    "sfWarehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
    "sfRole": os.getenv("SNOWFLAKE_ROLE"),
}

tables = {
    "DIM_DRIVER": "delta/gold/dim_driver",
    "DIM_VEHICLE": "delta/gold/dim_vehicle",
    "DIM_WAREHOUSE": "delta/gold/dim_warehouse",
    "DIM_DESTINATION": "delta/gold/dim_destination",
    "DIM_STATUS": "delta/gold/dim_status",
    "DIM_DATE": "delta/gold/dim_date",
    "FACT_SHIPMENT_EVENTS": "delta/gold/fact_shipment_events",
    "WAREHOUSE_KPI": "delta/gold/marts/warehouse_kpi",
    "DRIVER_KPI": "delta/gold/marts/driver_kpi",
    "VEHICLE_KPI": "delta/gold/marts/vehicle_kpi",
    "STATUS_KPI": "delta/gold/marts/status_kpi",
    "DAILY_KPI": "delta/gold/marts/daily_kpi",
}

for table_name, path in tables.items():
    print(f"Loading {table_name}...")

    df = spark.read.format("delta").load(path)

    (
        df.write
        .format("snowflake")
        .options(**sf_options)
        .option("dbtable", table_name)
        .mode("overwrite")
        .save()
    )

    print(f"{table_name} loaded")

spark.stop()