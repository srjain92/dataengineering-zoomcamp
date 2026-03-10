from pyspark.sql import SparkSession

# This initializes your local cluster
spark = SparkSession.builder \
    .master("local[*]") \
    .appName('installation_test') \
    .getOrCreate()

# Create some test data
data = [("Spark", "Installed"), ("Zoomcamp", "2026")]
df = spark.createDataFrame(data, ["Tool", "Status"])

# Action: This triggers the Spark engine
df.show()

print(f"Congratulations! Spark version {spark.version} is live.")
spark.stop()