from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, lit
import os
from datetime import datetime, timedelta
import random

# Initialize Spark session with Delta Lake
builder = SparkSession.builder \
    .appName("TestDataCreation") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

# Initialize Delta Lake with the builder
from delta import configure_spark_with_delta_pip
builder = configure_spark_with_delta_pip(builder)

# Create the Spark session
spark = builder.getOrCreate()

# Set log level to WARN to reduce output
spark.sparkContext.setLogLevel("WARN")

# Create sample data
categories = ["food and drinks", "shopping", "transport", "bills", "entertainment"]
users = [f"user_{i}" for i in range(1, 6)]

# Generate data for the last 60 days
data = []
for i in range(1000):
    user = random.choice(users)
    category = random.choice(categories)
    amount = round(random.uniform(5, 500), 2)
    days_ago = random.randint(0, 60)
    timestamp = (datetime.now() - timedelta(days=days_ago)).strftime("%Y-%m-%d %H:%M:%S")
    data.append((user, timestamp, amount, category))

# Create DataFrame
df = spark.createDataFrame(data, ["user_id", "timestamp", "amount", "category"])

# Convert timestamp string to timestamp type
df = df.withColumn("timestamp", to_timestamp(col("timestamp")))

# Create output directory
output_dir = os.path.join(os.path.dirname(__file__), "..", "data", "delta", "transactions")
os.makedirs(os.path.dirname(output_dir), exist_ok=True)

# Write as Delta table
df.write.format("delta").mode("overwrite").save(output_dir)

print(f"Sample Delta table created at: {output_dir}")
print(f"Sample data count: {df.count()}")

# Create a view for the Delta table
df.createOrReplaceTempView("transactions")

# Show sample data
print("\nSample data:")
df.show(5)
