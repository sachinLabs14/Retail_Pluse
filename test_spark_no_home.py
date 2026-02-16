import os
import sys

# Unset SPARK_HOME to force use of pip-installed pyspark
if 'SPARK_HOME' in os.environ:
    del os.environ['SPARK_HOME']

from pyspark.sql import SparkSession

print(f"JAVA_HOME: {os.environ.get('JAVA_HOME')}")

try:
    spark = SparkSession.builder.appName("Test").getOrCreate()
    print(f"Spark Version: {spark.version}")
    spark.stop()
except Exception as e:
    import traceback
    traceback.print_exc()
