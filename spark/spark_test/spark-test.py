import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, LongType, TimestampType
from pyspark.sql.functions import col, to_utc_timestamp, lower, window
from chispa.dataframe_comparer import assert_df_equality

# ---------------- Spark Session Fixture ----------------
@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder \
        .appName("spark-test") \
        .config("spark.sql.session.timeZone", "UTC") \
        .master("local[*]") \
        .getOrCreate()

# ---------------- Define Schemas ----------------
event_schema = StructType([
    StructField("event_type", StringType(), True),
    StructField("url", StringType(), True)
])

activity_schema = StructType([
    StructField("id", StringType(), True),
    StructField("date", LongType(), True),  # epoch millis
    StructField("event", event_schema, True)
])

# ---------------- Test Logic ----------------
def test_activity_processing(spark):
    # 1. Dummy input data (epoch millis for date)
    input_data = [
        ("user1", 1692922334000, {"event_type": "LIKED", "url": "https://x.com/1"}),
        ("user1", 1692922365000, {"event_type": "COMMENTED", "url": "https://x.com/2"}),
        ("user1", 1692922365000, {"event_type": "LIKED", "url": "https://x.com/2"}),
        ("user2", 1692922399000, {"event_type": "LIKED", "url": "https://x.com/3"}),
        ("user3", 1692922401000, {"event_type": "VIEWED", "url": "https://x.com/4"}),  # should be filtered out
    ]

    df_parsed = spark.createDataFrame(input_data, schema=activity_schema)

    # 2. Transform: convert date, lowercase event_type
    df_clean = df_parsed.select(
        col("id"),
        (to_utc_timestamp((col("date") / 1000).cast("timestamp"), "UTC")).alias("date"),
        lower(col("event.event_type")).alias("event_type")
    )

    # 3. Filter only liked/commented
    keep_list = ["liked", "commented"]
    df_filtered = df_clean.filter(col("event_type").isin(keep_list))

    # 4. Aggregate with watermark & 1-min window
    df_count = df_filtered.withWatermark("date", "10 seconds") \
        .groupBy("id", "event_type", window(col("date"), "1 minute")).count()

    # 5. Expected result
    expected_data = [
        ("user1", "liked", "2023-08-25 00:12:00", "2023-08-25 00:13:00", 2),
        ("user1", "commented", "2023-08-25 00:12:00", "2023-08-25 00:13:00", 1),
        ("user2", "liked", "2023-08-25 00:13:00", "2023-08-25 00:14:00", 1)
    ]

    expected_schema = StructType([
        StructField("id", StringType(), True),
        StructField("event_type", StringType(), True),
        StructField("window_start", StringType(), True),
        StructField("window_end", StringType(), True),
        StructField("count", LongType(), True)
    ])

    expected_df = spark.createDataFrame(expected_data, schema=expected_schema)

    # Flatten window struct for comparison
    actual_df = df_count.select(
        col("id"),
        col("event_type"),
        col("window.start").cast("string").alias("window_start"),
        col("window.end").cast("string").alias("window_end"),
        col("count")
    )

    # 6. Assert using chispa
    assert_df_equality(actual_df, expected_df, ignore_row_order=True, ignore_nullable=True)
