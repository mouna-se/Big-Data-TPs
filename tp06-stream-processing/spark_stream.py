
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    from_json,
    to_timestamp,
    window,
    date_format
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType
)

# --------------------------------------------------
# 1. Create Spark Session
# --------------------------------------------------

spark = (
    SparkSession.builder
    .appName("CampusRealTimeAnalytics")
    .config("spark.sql.session.timeZone", "Africa/Algiers")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# --------------------------------------------------
# 2. Define JSON Schema
# --------------------------------------------------

schema = StructType([
    StructField("eventId", IntegerType(), True),
    StructField("studentId", StringType(), True),
    StructField("eventType", StringType(), True),
    StructField("department", StringType(), True),
    StructField("eventTime", StringType(), True)
])

# --------------------------------------------------
# 3. Read Events from Kafka
# --------------------------------------------------

raw = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "campus-events")
    .option("startingOffsets", "earliest")
    .load()
)

# --------------------------------------------------
# 4. Parse JSON and Convert Event Time
# --------------------------------------------------

parsed = (
    raw.selectExpr("CAST(value AS STRING) AS json")
    .select(from_json(col("json"), schema).alias("e"))
    .select("e.*")
    .withColumn("event_ts", to_timestamp(col("eventTime")))
    .filter(col("event_ts").isNotNull())
)

# Keep only check-in events
checkins = parsed.filter(col("eventType") == "check-in")

# --------------------------------------------------
# 5. Task 4: Department Analytics
# --------------------------------------------------

counts = (
    checkins
    .withWatermark("event_ts", "30 seconds")
    .groupBy(
        window(col("event_ts"), "1 minute"),
        col("department")
    )
    .count()
)

query1 = (
    counts.writeStream
    .queryName("department-analytics")
    .outputMode("update")
    .format("console")
    .option("truncate", "false")
    .option("checkpointLocation", "./checkpoint/final-counts")
    .start()
)

print("Department Analytics query started!")

# --------------------------------------------------
# 6. Task 5: Late Check-in Business Alert
# --------------------------------------------------
# Producer timestamps are UTC.
# Campus timezone is Africa/Algiers (UTC+1).
# A check-in is late if local time is strictly
# later than 08:15:00.

late_checkins = (
    checkins
    .filter(
        date_format(col("event_ts"), "HH:mm:ss") > "08:15:00"
    )
    .select(
        col("studentId"),
        col("department"),
        col("eventTime"),
        date_format(
            col("event_ts"),
            "yyyy-MM-dd HH:mm:ss"
        ).alias("eventTime_Algeria")
    )
)

query2 = (
    late_checkins.writeStream
    .queryName("late-checkin-alerts")
    .outputMode("append")
    .format("console")
    .option("truncate", "false")
    .option("checkpointLocation", "./checkpoint/alerts-task5-final")
    .start()
)

print("Late Check-in Alert query started!")

# --------------------------------------------------
# 7. Keep Both Queries Running
# --------------------------------------------------

spark.streams.awaitAnyTermination()


