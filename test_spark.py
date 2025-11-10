from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .master("local[*]") \
    .appName("local_test") \
    .getOrCreate()

print("Spark version:", spark.version)

# small DataFrame test
df = spark.range(5)
df.show()

spark.stop()


