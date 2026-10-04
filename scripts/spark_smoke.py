"""Minimal pinned-runtime integration check."""
from pyspark.sql import SparkSession

spark = (SparkSession.builder.master("local[2]").appName("harbortrace-smoke")
         .config("spark.ui.enabled", "false").getOrCreate())
try:
    assert spark.range(5).count() == 5
    print(f"Spark {spark.version}: range(5).count() = 5")
finally:
    spark.stop()
