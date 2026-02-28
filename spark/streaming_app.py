# Write your streaming app code here

import os
import sys

# Windows setup
if os.name == 'nt':
    os.environ["HADOOP_HOME"] = r"C:\hadoop"
    os.environ["PATH"] = r"C:\hadoop\bin;" + os.environ["PATH"]

ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(ROOT_DIR)
from schema import event_schema

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, to_timestamp, window,
    avg, count, when, lit
)

BOOTSTRAP_SERVERS = "localhost:9092"

spark = SparkSession.builder \
    .appName("AdvancedSparkStreaming") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.2") \
    .config("spark.sql.shuffle.partitions", "2") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# ─────────────────────────────────────────
# 1. Read raw stream from Kafka
# ─────────────────────────────────────────
df_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS) \
    .option("subscribe", "events_raw") \
    .option("startingOffsets", "earliest") \
    .load()

df_string = df_raw.select(col("value").cast("string").alias("raw"))

# ─────────────────────────────────────────
# 2. Parse JSON safely
# ─────────────────────────────────────────
df_parsed = df_string.select(
    col("raw"),
    from_json(col("raw"), event_schema).alias("data")
)

# ─────────────────────────────────────────
# 3. Separate valid vs invalid events
# ─────────────────────────────────────────
df_valid = df_parsed.filter(
    col("data.device_id").isNotNull() &
    col("data.event_time").isNotNull() &
    col("data.temperature").isNotNull() &
    (col("data.temperature") != -999) &       
    (col("data.temperature") > -50) &       
    (col("data.temperature") < 100) &        
    col("data.country").isNotNull() &
    (col("data.country") != "") &          
    (col("data.country").rlike("^[a-zA-Z ]+$"))  
).select(
    col("data.device_id"),
    to_timestamp(col("data.event_time")).alias("event_time"),
    col("data.temperature"),
    col("data.country")
)

df_invalid = df_parsed.filter(
    col("data.device_id").isNull() |
    col("data.event_time").isNull() |
    col("data.temperature").isNull() |
    (col("data.temperature") == -999) |
    (col("data.temperature") <= -50) |
    (col("data.temperature") >= 100) |
    col("data.country").isNull() |
    (col("data.country") == "") |
    (~col("data.country").rlike("^[a-zA-Z ]+$"))
).select(col("raw").alias("invalid_event"))

# ─────────────────────────────────────────
# 4. Apply Watermark for event time
# ─────────────────────────────────────────
df_watermarked = df_valid.withWatermark("event_time", "10 minutes")

# ─────────────────────────────────────────
# 5. Windowed aggregations
# ─────────────────────────────────────────
# Average temperature per device per 10-min window
df_avg_temp = df_watermarked \
    .groupBy(
        window(col("event_time"), "10 minutes"),
        col("device_id")
    ).agg(
        avg("temperature").alias("avg_temperature"),
        count("*").alias("event_count")
    )

# Event count per country per 10-min window
df_country_count = df_watermarked \
    .groupBy(
        window(col("event_time"), "10 minutes"),
        col("country")
    ).agg(
        count("*").alias("event_count")
    )

# ─────────────────────────────────────────
# 6. Write sinks
# ─────────────────────────────────────────
OUTPUT_DIR = os.path.join(ROOT_DIR, "..", "output")
CHECKPOINT_DIR = os.path.join(ROOT_DIR, "..", "checkpoints")

# Console - valid events (debug)
q1 = df_valid.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", False) \
    .start()

# Console - invalid events (debug)
q2 = df_invalid.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", False) \
    .start()

# File sink - avg temperature (Parquet)
q3 = df_avg_temp.writeStream \
    .outputMode("append") \
    .format("parquet") \
    .option("path", os.path.join(OUTPUT_DIR, "avg_temperature")) \
    .option("checkpointLocation", os.path.join(CHECKPOINT_DIR, "avg_temp")) \
    .start()

# File sink - country counts (Parquet)
q4 = df_country_count.writeStream \
    .outputMode("append") \
    .format("parquet") \
    .option("path", os.path.join(OUTPUT_DIR, "country_counts")) \
    .option("checkpointLocation", os.path.join(CHECKPOINT_DIR, "country_counts")) \
    .start()

# Kafka sink - valid events
q5 = df_valid.select(
    col("device_id"),
    col("event_time").cast("string"),
    col("temperature"),
    col("country")
).selectExpr("to_json(struct(*)) AS value") \
    .writeStream \
    .outputMode("append") \
    .format("kafka") \
    .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS) \
    .option("topic", "events_valid") \
    .option("checkpointLocation", os.path.join(CHECKPOINT_DIR, "valid_kafka")) \
    .start()

# Kafka sink - invalid events
q6 = df_invalid.selectExpr("invalid_event AS value") \
    .writeStream \
    .outputMode("append") \
    .format("kafka") \
    .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS) \
    .option("topic", "events_invalid") \
    .option("checkpointLocation", os.path.join(CHECKPOINT_DIR, "invalid_kafka")) \
    .start()

spark.streams.awaitAnyTermination()
