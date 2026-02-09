import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

# -------------------------------------------------
# 1. Schema Definition
# -------------------------------------------------
schema = StructType([
    StructField("event_id", StringType(), True),
    StructField("match_id", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("team", StringType(), True),
    StructField("player", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("details", StringType(), True),
    StructField(
        "location",
        StructType([
            StructField("x", DoubleType(), True),
            StructField("y", DoubleType(), True)
        ]),
        True
    )
])

# -------------------------------------------------
# 2. Spark Session
# -------------------------------------------------
spark = (
    SparkSession.builder
    .appName("Football_Streaming_Processor")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

# -------------------------------------------------
# 3. Kafka Ingestion
# -------------------------------------------------
raw_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka:9092")
    .option("subscribe", "football-events")
    .option("startingOffsets", "earliest")
    .option("failOnDataLoss", "false")
    .option("kafka.group.id", "football-streaming-group")
    .load()
)

# -------------------------------------------------
# 4. Transformation
# -------------------------------------------------
parsed_df = (
    raw_df
    .selectExpr("CAST(value AS STRING)")
    .select(from_json(col("value"), schema).alias("data"))
    .select(
        col("data.event_id"),
        col("data.match_id"),
        to_timestamp(col("data.timestamp"), "yyyy-MM-dd HH:mm:ss").alias("event_time"),
        col("data.team"),
        col("data.player"),
        col("data.event_type"),
        col("data.details"),
        col("data.location.x").alias("loc_x"),
        col("data.location.y").alias("loc_y")
    )
    .filter(col("event_time").isNotNull())
)

# -------------------------------------------------
# 5. Postgres Sink Logic
# -------------------------------------------------
def write_to_postgres(batch_df, batch_id):
    row_count = batch_df.count()

    if row_count == 0:
        print(f"Batch {batch_id} is empty. Waiting for events...")
        return

    print(f"Batch {batch_id} received with {row_count} rows")
    batch_df.show(5, truncate=False)

    try:
        (
            batch_df.write
            .format("jdbc")
            .option("url", "jdbc:postgresql://postgres:5432/football")
            .option("dbtable", "football_events")
            .option("user", "football_user")
            .option("password", "football_pass")
            .option("driver", "org.postgresql.Driver")
            .mode("append")
            .save()
        )
        print(f"Batch {batch_id} successfully written to Postgres")
    except Exception as e:
        print(f"Postgres write error in batch {batch_id}: {e}")

# -------------------------------------------------
# 6. Streaming Execution
# -------------------------------------------------
query = (
    parsed_df.writeStream
    .foreachBatch(write_to_postgres)
    .trigger(processingTime="5 seconds")
    .option("checkpointLocation", "/tmp/spark-checkpoints/football-events")
    .start()
)

query.awaitTermination()
