# TP06 --- Real-Time Stream Processing with Kafka and Spark

### Task 1 --- Prepare the Kafka Topic and Python Environment

1.  Reused the Kafka broker created in TP05
2.  Created the `campus-events` topic with three partitions and
    replication factor 1 (create topic.png).
3.  Described the topic to verify its configuration (verify topic.png).
4.  Created a Python virtual environment in the TP06 project folder (install and activate.png).
5.  Installed the Python dependencies (install and activate.png).

``` powershell
docker exec -it tp05-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists --topic campus-events --partitions 3 --replication-factor 1 

docker exec -it tp05-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic campus-events 
```

``` powershell
python -m venv .venv
.venv\Scripts\Activate.ps1 
python -m pip install -r requirements.txt 
```

### Task 2 --- Generate Campus Events (Kafka Producer)

The `producer.py` script creates synthetic JSON events containing:

-   `eventId`
-   `studentId`
-   `eventType`
-   `department`
-   `eventTime`

The producer was run from the activated Windows virtual environment (producer.png):

``` powershell
python producer.py 
```

### Task 3 --- Build the Spark Structured Streaming Application

SSpark was **not installed directly on Windows**. Instead, the application was run
inside a Docker container using the official `apache/spark:3.5.3` image, with the
matching Kafka connector for Spark 3.5.3 / Scala 2.12
(`org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3`).

I created `spark_stream.py`, which implements the Spark application.

Then I Run the application (run_spark.png)
``` powershell
docker run --rm -it --network container:tp05-kafka -v "D:\Big-Data-TPs\tp06-stream-processing:/opt/tp06" -w /opt/tp06 apache/spark:3.5.3 /opt/spark/bin/spark-submit --conf spark.jars.ivy=/tmp/.ivy2 --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 spark_stream.py 
```

The Spark application was started first. The producer was then started
in a second PowerShell terminal. This order allows the streaming query
to be ready before the new events are published.

### Task 4 --- Experiments with Triggers, Windows, and Checkpointing

The experiments examine how changing one streaming configuration affects
the output.

#### Experiment A --- Trigger interval

A processing-time trigger of five seconds was added before `.start()`:

``` python
.trigger(processingTime="5 seconds")
```
(result_A.png)

#### Experiment B --- Window size

The window duration was changed from one minute to two minutes:

``` python
window(col("event_ts"), "2 minutes")
```
(result_B.png)

#### Experiment C --- Sliding window

The window expression was changed to:

``` python
window(col("event_ts"), "2 minutes", "1 minute")
```
(result_C.png)

#### Experiment D --- Watermark

The watermark was changed from 30 seconds to two minutes:

``` python
.withWatermark("event_ts", "2 minutes")
```

(result_D.png)

#### Experiment E --- Recovery and checkpointing

The Spark application was stopped with `Ctrl+C` and restarted using the
same checkpoint location. The checkpoint directory was kept between the
stop and restart.

(result_E_before.png)

### Task 6 --- Flink SQL: Event-Time Window Analysis

The optional Flink implementation was **not performed**. Only the SQL
query was studied and the four required questions were answered.

1.  **Which field defines event time?**\
    `eventTime` defines event time. It represents the timestamp at which
    the event actually occurred.

2.  **What does the watermark declaration mean?**\
    `WATERMARK FOR eventTime AS eventTime - INTERVAL '30' SECOND`
    defines a watermark 30 seconds behind the event-time value. It
    allows Flink to account for events that arrive out of order within
    the configured delay and helps determine when event-time windows can
    progress. It does not guarantee that every event arriving later than
    the watermark will be included.

3.  **What is the duration of each tumbling window?**\
    Each tumbling window lasts one minute. Tumbling windows have a fixed
    duration and do not overlap.

4.  **Why are `window_start` and `window_end` useful in the output?**\
    They identify the beginning and end of the time interval represented
    by each aggregation row. They make it clear which time window the
    department's check-in count belongs to.

No Flink installation, Kafka SQL connector installation, or Flink query
execution was carried out because the implementation is explicitly
optional.

## 6. Report Questions and Answers

### 1. What is the difference between Kafka's role and Spark/Flink's role in the pipeline?

Kafka is the event transport and storage layer. It receives, stores, and
distributes the events through topics and partitions. Spark or Flink is
the stream-processing engine: it reads the events, applies
transformations and event-time logic, maintains state, and produces
analytical results.

### 2. Explain how Spark Structured Streaming represents a stream and updates a result incrementally.

Structured Streaming represents incoming data as an unbounded table. New
records are processed incrementally, and Spark updates the result table
as new data arrives. In this practical work, the result is grouped by
event-time window and department.

### 3. What is a micro-batch? Give one advantage and one trade-off.

A micro-batch is a small group of records processed together during one
streaming execution. An advantage is that it reuses Spark's distributed
batch-processing engine and provides scalable processing. A trade-off is
that processing and scheduling batches can add latency compared with
processing each event immediately.

### 4. What is the purpose of the Kafka source options `startingOffsets` and `subscribe`?

`subscribe` specifies the Kafka topic or topics from which Spark reads
data. `startingOffsets` specifies where a new query starts reading, for
example `earliest` to begin with the earliest available offsets or
`latest` to begin with new records. When a checkpoint already exists,
recovery uses the offsets recorded in that checkpoint.

### 5. Why is the JSON schema explicitly defined before parsing the event values?

The schema tells Spark the expected field names and data types. It
allows Spark to parse JSON consistently, access fields by name, detect
invalid or incompatible values, and avoid relying on schema inference
for every message.

### 6. Distinguish event time and processing time using one event from your experiment.

Event time is the timestamp inside the event describing when the
check-in occurred. Processing time is when Spark processes the message.
For example, an event with `eventTime` equal to 07:15:20 UTC represents
08:15:20 in Algeria, even if Spark processes it several seconds later.

### 7. What is a watermark? Does it guarantee that no late event will be lost? Explain.

A watermark is an estimate of event-time progress used to manage late
data and state cleanup. It does not guarantee that no late event will be
lost. Events arriving later than the watermark threshold may be excluded
from a window's result, depending on the query and output mode.

### 8. Compare tumbling, sliding, and session windows.

-   **Tumbling window:** fixed-duration, non-overlapping intervals. Each
    event belongs to one window of the chosen duration.
-   **Sliding window:** fixed-duration intervals that advance by a
    specified slide. Windows can overlap, so one event may belong to
    multiple windows.
-   **Session window:** groups events based on activity separated by
    gaps shorter than a configured inactivity gap. The window length
    depends on the event activity rather than a single fixed duration.

### 9. What information does a checkpoint preserve, and why must its location remain stable during a recovery test?

A checkpoint preserves streaming progress, including source offsets and
the state information required by the query. Its location must remain
stable because Spark uses it to identify the previous query progress and
restore state when the application restarts. Deleting it can cause the
query to lose its recovery information or start as a new query.

### 10. Give two differences between Spark Structured Streaming and Flink based on the lecture.

1.  Spark Structured Streaming commonly uses a micro-batch execution
    model (with continuous processing available for limited cases),
    whereas Flink is designed around continuous record-at-a-time stream
    processing.
2.  Both engines support event time, watermarks, windows, and stateful
    operations, but they expose and configure these concepts
    differently. Spark uses the Structured Streaming DataFrame/Dataset
    API, while Flink supports APIs and Flink SQL with explicit table and
    watermark definitions.

### 11. Which engine would you select for this smart-campus scenario and why? Justify your answer using latency, event-time handling, state, ecosystem, and team skills.

For this practical implementation, I would select **Spark Structured
Streaming** because it was successfully configured with the existing
Kafka broker and used to build the required event-time window
aggregation. It provides watermarking and stateful window operations,
integrates with the Spark ecosystem, and is suitable for the team's
current Python/Spark skills.

Flink would also be a strong option, especially for applications that
require low-latency, continuous event processing and advanced event-time
stream processing. However, it would require an additional Flink
environment and a compatible Kafka SQL connector. Considering the tools
already configured and the implementation completed in this TP, Spark is
the more practical choice for this project.

## 7. Problems Encountered and Solutions

-   **Kafka image/network access:** The existing Kafka broker from TP05
    was reused instead of creating another broker. Spark was connected
    to the same Docker network namespace as the Kafka container.

-   **Spark Kafka connector:** The connector coordinate was matched to
    Spark 3.5.3 and Scala 2.12:
    `org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3`.

-   **Starting offsets and checkpointing:** The alert query was
    configured to read from `earliest` with a separate fresh checkpoint
    location during testing. Separate queries use separate checkpoint
    directories.
