import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

# --- 1. Schema Definition ---
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

# --- 2. Smart Spark Session ---
# This looks for the Master, but defaults to local if the cluster is unreachable
spark = SparkSession.builder \
    .appName("Football_Streaming_Processor") \
    .config("spark.sql.streaming.checkpointLocation", "/tmp/spark-checkpoints") \
    .getOrCreate()

# Reduce logging noise so you can see your data
spark.sparkContext.setLogLevel("ERROR")

# --- 3. Kafka Ingestion ---
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "football-events") \
    .option("startingOffsets", "earliest") \
    .option("failOnDataLoss", "false") \
    .load()

# --- 4. Transformation ---
parsed_df = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select(
        col("data.event_id"),
        to_timestamp(col("data.timestamp"), "yyyy-MM-dd HH:mm:ss").alias("timestamp"),
        col("data.team"),
        col("data.player"),
        col("data.event_type"),
        col("data.details"),
        col("data.location.x").alias("loc_x"),
        col("data.location.y").alias("loc_y")
    )

# --- 5. Postgres Sink Logic ---
def write_to_postgres(batch_df, batch_id):
    if batch_df.count() > 0:
        batch_df.persist()
        
        # Show data in console for immediate feedback
        print(f"DEBUG: Batch {batch_id} received. Writing {batch_df.count()} rows to Postgres.")
        batch_df.show(5) 
        
        try:
            batch_df.write \
                .format("jdbc") \
                .option("url", "jdbc:postgresql://postgres:5432/football") \
                .option("dbtable", "football_events") \
                .option("user", "football_user") \
                .option("password", "football_pass") \
                .option("driver", "org.postgresql.Driver") \
                .mode("append") \
                .save()
            print("Successfully saved to Postgres.")
        except Exception as e:
            print(f"Postgres Write Error: {e}")
            
        batch_df.unpersist()
    else:
        print(f"Batch {batch_id} is empty. Waiting for football events...")

# --- 6. Execution ---
query = parsed_df.writeStream \
    .foreachBatch(write_to_postgres) \
    .trigger(processingTime='5 seconds') \
    .start()

query.awaitTermination()
