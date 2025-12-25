from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, FloatType, DoubleType

# Define schema to match your Python Producer exactly
schema = StructType([
    StructField("event_id", StringType(), True),
    StructField("match_id", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("team", StringType(), True),
    StructField("player", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("details", StringType(), True),
    StructField("location", StructType([
        StructField("x", DoubleType(), True),
        StructField("y", DoubleType(), True)
    ]), True)
])

# Initialize Spark Session with Kafka and Postgres JARs
# Note: In production, these JARs must be available to the Spark cluster
spark = SparkSession.builder \
    .appName("Football_Streaming_Processor") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# 1. Read from Kafka (INTERNAL network)
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "football-events") \
    .option("startingOffsets", "earliest") \
    .load()

# 2. Extract and Flatten the JSON
parsed_df = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select(
        col("data.event_id"),
        col("data.timestamp"),
        col("data.team"),
        col("data.player"),
        col("data.event_type"),
        col("data.details"),
        col("data.location.x").alias("loc_x"),
        col("data.location.y").alias("loc_y")
    )

# 3. Write to Postgres
def write_to_postgres(batch_df, batch_id):
    if batch_df.count() > 0:
        print(f"--- Batch {batch_id} ---")
        print(f"Processing {batch_df.count()} events...")
        batch_df.show(5) # Preview in logs
        
        batch_df.write \
            .format("jdbc") \
            .option("url", "jdbc:postgresql://postgres:5432/football") \
            .option("dbtable", "football_events") \
            .option("user", "football_user") \
            .option("password", "football_pass") \
            .option("driver", "org.postgresql.Driver") \
            .mode("append") \
            .save()

# Start the Stream
query = parsed_df.writeStream \
    .foreachBatch(write_to_postgres) \
    .start()

query.awaitTermination()
