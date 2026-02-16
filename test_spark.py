from pyspark.sql import SparkSession
import os

print(f"JAVA_HOME: {os.environ.get('JAVA_HOME')}")
print(f"SPARK_HOME: {os.environ.get('SPARK_HOME')}")

try:
    spark = SparkSession.builder.appName("Test").getOrCreate()
    print(f"Spark Version: {spark.version}")
    spark.stop()
except Exception as e:
    print(f"Error: {e}")
