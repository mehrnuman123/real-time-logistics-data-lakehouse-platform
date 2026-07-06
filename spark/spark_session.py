from pyspark.sql import SparkSession
spark = (
    SparkSession.builder
    .appName("BronzeToSilverShipments")
    .master("local[*]")
    .config(
        "spark.jars.packages",
        "io.delta:delta-spark_2.12:3.2.0",
        "net.snowflake:spark-snowflake_2.12:2.16.0,"
        "net.snowflake:snowflake-jdbc:3.16.1,"
    )
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
)
