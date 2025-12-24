from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col

# Using a flexible schema to avoid NULL results
from pyspark.sql.types import StructType, StructField, StringType, MapType

# Define a minimal but strict schema for the StatsBomb structure
schema = StructType([
    StructField("type", StructType([StructField("name", StringType(), True)]), True),
    StructField("team", StructType([StructField("name", StringType(), True)]), True),
    StructField("player", StructType([StructField("name", StringType(), True)]), True)
])

spark = SparkSession.builder.appName("StatsBomb_Debugger").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# 1. 'earliest' ensures we read data already sitting in Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "football-events") \
    .option("startingOffsets", "earliest") \
    .load()

# 2. Extract and Flatten
# Note: StatsBomb uses type -> name for 'Pass'
parsed_df = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select(
        col("data.type.name").alias("event_type"),
        col("data.team.name").alias("team_name"),
        col("data.player.name").alias("player_name")
    )

def write_to_postgres(batch_df, batch_id):
    if batch_df.count() > 0:
        print(f"Writing {batch_df.count()} rows to Postgres...")
        batch_df.write \
            .format("jdbc") \
            .option("url", "jdbc:postgresql://postgres:5432/football") \
            .option("dbtable", "statsbomb_events") \
            .option("user", "football_user") \
            .option("password", "football_pass") \
            .option("driver", "org.postgresql.Driver") \
            .mode("append") \
            .save()

query = parsed_df.writeStream.foreachBatch(write_to_postgres).start()
query.awaitTermination()