import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, lower, window, from_unixtime
from pyspark.sql.types import (
    StructType, StructField, StringType, TimestampType, LongType
)

# ----------------- Spark Session -----------------
spark = (
    SparkSession.builder
    .appName("KafkaUserActivityConsumer")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# ----------------- Kafka Config -----------------
KAFKA_BROKER = "localhost:9092"
TOPIC = "user_activity"

# ----------------- JDBC Config -----------------
POSTGRES_URL = "jdbc:postgresql://localhost:5433/test_db"
POSTGRES_USER = os.getenv("DB_USER")
POSTGRES_PASSWORD = os.getenv("DB_PASSWORD")
POSTGRES_TABLE = "spark.user_activity"
POSTGRES_TABLE_VIEWED = "spark.user_activity_viewed"

# ----------------- Schema -----------------
event_schema = StructType([
    StructField("event_type", StringType(), True),
    StructField("url", StringType(), True)
])

activity_schema = StructType([
    StructField("id", StringType(), True),
    StructField("date", LongType(), True),
    StructField("event", event_schema, True)
])

# ----------------- Read Stream -----------------
df_raw = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BROKER)
    .option("subscribe", TOPIC)
    .option("startingOffsets", "latest")
    .load()
)

# Deserialize JSON
df_parsed = df_raw.selectExpr("CAST(value AS STRING) as json") \
    .select(from_json(col("json"), activity_schema).alias("data"))

df_clean = df_parsed.select(
    col("data.id").alias("id"),
    (from_unixtime(col("data.date") / 1000)).cast(TimestampType()).alias("date"),
    lower(col("data.event.event_type")).alias("event_type"),
)

# ----------------- Split Events -----------------
keep_list = ["liked", "commented"]

df_filtered = df_clean.filter(col("event_type").isin(keep_list))
#df_viewed_only = df_clean.filter(~col("event_type").isin(keep_list))  # all other events
df_count = df_filtered.withWatermark("date", "10 seconds") \
        .groupBy("id", "event_type", window(col("date"), "1 minute")).count()
# ----------------- Write Helper -----------------
def write_to_postgres(batch_df, batch_id, table):
    (
        batch_df.write
        .format("jdbc")
        .option("url", POSTGRES_URL)
        .option("dbtable", table)
        .option("user", POSTGRES_USER)
        .option("password", POSTGRES_PASSWORD)
        .option("driver", "org.postgresql.Driver")
        .mode("append")
        .save()
    )

# ----------------- Start Queries -----------------
query1 = (
    df_count.writeStream
    .foreachBatch(
        lambda df, id: (
            print(f"Processing batch {id} with {df.count()} rows")
            or write_to_postgres(df, id, POSTGRES_TABLE)
        )
    )
    .outputMode("append")
    .option("checkpointLocation", "/tmp/spark-checkpoints/user_activity")
    .start()
)


# query2 = (
#     df_viewed_only.writeStream
#     .foreachBatch(lambda df, id: write_to_postgres(df, id, POSTGRES_TABLE_VIEWED))
#     .outputMode("append")
#     .option("checkpointLocation", "/tmp/spark-checkpoints/user_activity_viewed")
#     .start()
# )

query1.awaitTermination()
# query2.awaitTermination()
