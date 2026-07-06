from spark_session import spark
from pyspark.sql.functions import count, avg, max, min, col, when, countDistinct

fact = spark.read.format("delta").load("delta/gold/fact_shipment_events")

fact.show(10)
fact.printSchema()


spark.stop()