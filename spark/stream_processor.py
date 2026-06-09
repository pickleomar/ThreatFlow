"""
Spark Structured Streaming job.

Reads the 'security-events' Kafka topic in micro-batches, detects anomalies,
writes alerts + counters to PostgreSQL, and archives raw events to Parquet.

Submit from inside the spark container:

  docker compose exec spark spark-submit \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.postgresql:postgresql:42.7.3 \
    /app/stream_processor.py
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, count, lit, when
from pyspark.sql.types import StructType, StringType, DoubleType, IntegerType

# Inside Docker, reach Kafka/Postgres by service name
KAFKA_BOOTSTRAP = "kafka:9094"
TOPIC = "security-events"
PG_URL = "jdbc:postgresql://postgres:5432/securitydb"
PG_PROPS = {"user": "secu", "password": "secret", "driver": "org.postgresql.Driver"}
PARQUET_PATH = "/data/parquet"
CHECKPOINT = "/data/checkpoints"

BLACKLIST = ["10.66.66.6", "45.137.21.9", "185.220.101.1"]
FAIL_THRESHOLD = 5  # >5 failed logins / IP / minute -> alert


def write_to_postgres(df, table):
    (df.write
       .mode("append")
       .jdbc(PG_URL, table, properties=PG_PROPS))


def main():
    spark = (SparkSession.builder
             .appName("SecuStream")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")

    schema = (StructType()
              .add("ip", StringType())
              .add("action", StringType())
              .add("port", IntegerType())
              .add("ts", DoubleType()))

    raw = (spark.readStream
           .format("kafka")
           .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
           .option("subscribe", TOPIC)
           .option("startingOffsets", "latest")
           .load())

    events = (raw
              .select(from_json(col("value").cast("string"), schema).alias("d"))
              .select("d.*")
              .withColumn("event_time", col("ts").cast("timestamp")))

    # ---- Archive raw events to Parquet (replayable history) ----
    (events.writeStream
        .format("parquet")
        .option("path", PARQUET_PATH)
        .option("checkpointLocation", f"{CHECKPOINT}/parquet")
        .outputMode("append")
        .start())

    # ---- Detection 1: failed-login bursts per IP per 1-min window ----
    fail_alerts = (events
                   .filter(col("action") == "login_fail")
                   .withWatermark("event_time", "2 minutes")
                   .groupBy(window(col("event_time"), "1 minute"), col("ip"))
                   .agg(count("*").alias("count"))
                   .filter(col("count") > FAIL_THRESHOLD)
                   .select(
                       col("window.start").alias("window_start"),
                       col("window.end").alias("window_end"),
                       col("ip"),
                       lit("failed_login_burst").alias("rule"),
                       col("count"),
                       when(col("count") > 10, lit("high")).otherwise(lit("medium")).alias("severity"),
                   ))

    def sink_alerts(batch_df, batch_id):
        if not batch_df.rdd.isEmpty():
            write_to_postgres(batch_df, "alerts")
            print(f"[spark] batch {batch_id}: wrote {batch_df.count()} alert(s)")

    (fail_alerts.writeStream
        .outputMode("update")
        .foreachBatch(sink_alerts)
        .option("checkpointLocation", f"{CHECKPOINT}/alerts")
        .start())

    # ---- Detection 2: any event from a blacklisted IP ----
    bl_alerts = (events
                 .filter(col("ip").isin(BLACKLIST))
                 .withWatermark("event_time", "2 minutes")
                 .groupBy(window(col("event_time"), "1 minute"), col("ip"))
                 .agg(count("*").alias("count"))
                 .select(
                     col("window.start").alias("window_start"),
                     col("window.end").alias("window_end"),
                     col("ip"),
                     lit("blacklisted_ip").alias("rule"),
                     col("count"),
                     lit("high").alias("severity"),
                 ))

    (bl_alerts.writeStream
        .outputMode("update")
        .foreachBatch(lambda df, bid: write_to_postgres(df, "alerts") if not df.rdd.isEmpty() else None)
        .option("checkpointLocation", f"{CHECKPOINT}/blacklist")
        .start())

    # ---- Counters: events per action per 1-min window (dashboard charts) ----
    counts = (events
              .withWatermark("event_time", "2 minutes")
              .groupBy(window(col("event_time"), "1 minute"), col("action"))
              .agg(count("*").alias("count"))
              .select(
                  col("window.start").alias("window_start"),
                  col("window.end").alias("window_end"),
                  col("action"),
                  col("count"),
              ))

    (counts.writeStream
        .outputMode("update")
        .foreachBatch(lambda df, bid: write_to_postgres(df, "event_counts") if not df.rdd.isEmpty() else None)
        .option("checkpointLocation", f"{CHECKPOINT}/counts")
        .start())

    print("[spark] streaming started. Waiting for events...")
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
