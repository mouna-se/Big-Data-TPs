# TP05: Real-Time Data Streaming and Processing Using Apache Kafka
 
---
 
## Task 1: Deploy Kafka with Docker Compose
 
The file compose.yaml was created, then:
 
```
docker compose up -d (photo1)
docker compose ps (photo1)
docker compose logs --tail=50 kafka (photo2)(photo1)
```
 
The container tp05-kafka is running.
 
---
 
## Task 2: Install the Python Client
The `requirements.txt` file was created. Then, a virtual environment was created and activated, and the required dependencies were installed.
```
python -m venv .venv (photo3)
.venv\Scripts\Activate.ps1 (photo3)
python -m pip install -r requirements.txt (photo3)
```
The installation of `kafka-python==2.0.2` succeeded, but `producer.py` failed with `ModuleNotFoundError: No module named 'kafka.vendor.six.moves'`, because this version is not compatible with Python 3.12+. I therefore changed the installation command to:

```
pip uninstall kafka-python -y
pip install --upgrade kafka-python (photo4)
```
 
The version in the lab sheet (`kafka-python==2.0.2`) does not work with Python 3.12+, so requirements.txt contains a compatible version (see Problems Encountered).
 
---
 
## Task 3: Producer
 
Prepared file: producer.py.
 
```
python producer.py (photo5)
```
 
30 events were sent.
 
---
 
## Task 4: Consumer
 
Prepared file: consumer.py (reads the events, prints each measurement, and prints an alert when the temperature is >= 35 C).
 
The consumer was started first, then the producer:
 
```
python consumer.py (photo 6)

```
 
---
 
## Task 5: Inspect Kafka Topics
 
```
docker exec -it tp05-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists --topic campus-sensors --partitions 3 --replication-factor 1 (photo 7)
docker exec -it tp05-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic campus-sensors (photo 7)
docker exec -it tp05-kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --list (photo 7)
```
---
 
## Task 6: Tasks for Further Investigation
 
### 6.1 Send 100 events instead of 30
 
In producer.py, `range(1, 31)` was changed to `range(1, 101)`.
 
```
python consumer.py (photo 8)
python producer.py (photo 9)
```
 
| | 30 events | 100 events | Theory |
|---|---|---|---|
| Events received | 30 | 100 | |
| Sending time | about 30 s | about 100 s | |
| ALERT events (>= 35 C) | 10 (33.3%) | 32 (32%) | 29.2% |
| Average temperature | 29.64 C | 29.99 C | 30 C |

### 6.2 Running average temperature
 
consumer.py was modified to keep `total` and `count`; for each message: `total += temperature`, `count += 1`, `average = total / count`.
 
```
python consumer.py (photo 10)
python producer.py 
```
 
### 6.3 Second consumer: different group_id vs same group
 
Topic `exp-groups` (3 partitions) and 30 events were used.
 
**Same group**:

- In producer.py and consumer.py, the topic was changed to a new topic created for this experiment:

```
TOPIC = "exp-same2"
```

- In consumer.py, the group was set to:

```
group_id="same-group",
```
```
docker exec -it tp05-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists --topic exp-same2 --partitions 3 --replication-factor 1 (photo16)
python consumer.py (terminal 2)(phtoto13)
python consumer.py (terminal 3)(phtoto14)
python producer.py (photo15)
```
 
Consumer 1 received 21 events and consumer 2 received 9 events (total 30, no duplicates). The partitions were divided between the two consumers.
 
**Different groups** (two consumers with different group_id on a topic with 3 partitions, 30 events):

```
TOPIC = "exp-diff"
```

- In consumer.py, `group_id` was set to `group-A` for the first consumer, then changed to `group-B` before starting the second consumer:

```
group_id="group-A",
group_id="group-B",
```

**Commands:**

```
docker exec -it tp05-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists --topic exp-diff --partitions 3 --replication-factor 1 (photo17)
python consumer.py (terminal 2, group-A)(photo18)
python consumer.py (terminal 3, group-B)(photo19)
python producer.py (photo17)
```

The two consumers were started first and both displayed `Listening to topic: exp-diff`; then the producer was started. Both consumers were stopped with Ctrl+C after the producer finished.

Consumer A (group-A) received 30 events and consumer B (group-B) received 30 events.
 
### 6.4 One partition vs three partitions

- In producer.py and consumer.py, the topic was changed to a new topic with one partition:

```
TOPIC = "exp-1p"
```

- In consumer.py, the group was set to:

```
group_id="same-1p",
```

**Commands:**

```
docker exec -it tp05-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists --topic exp-1p --partitions 1 --replication-factor 1 (photo20)
docker exec -it tp05-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic exp-1p (photo20)
python consumer.py (terminal 2)(photo21)
python consumer.py (terminal 3)(photo22)
python producer.py (photo20)
```

The same consumer.py was started in two terminals (same group), and the producer was started after both consumers displayed `Listening to topic: exp-1p`. Both consumers were stopped with Ctrl+C after the producer finished.

Topic description of `exp-1p`:

```
Topic: exp-1p   PartitionCount: 1   ReplicationFactor: 1
  Partition: 0   Leader: 1   Replicas: 1   Isr: 1
```

**Comparison with the 3-partition topic (experiment 6.3):**

The same consumer.py and the same procedure (two consumers in the same group, then the producer with 30 events) were used in both experiments. Only the topic changed.

| | 1 partition (`exp-1p`) | 3 partitions (`exp-same2`) |
|---|---|---|
| PartitionCount (topic description) | 1 | 3 |
| ReplicationFactor (topic description) | 1 | 1 |
| Events received by the first consumer | 30 | 21 |
| Events received by the second consumer | 0 | 9 |
| Total received | 30 | 30 |

**What changes:** with 3 partitions the 30 events were shared between the two consumers (21 and 9), while with 1 partition one consumer received all the 30 events and the second consumer received none (Events=0). This is the expected behaviour of a consumer group: a partition is read by only one consumer of the group, so with one partition the second consumer has nothing to read.

**What does not change in this single-broker setup:** there is still one broker and a replication factor of 1 (see the topic descriptions). The total number of received events is 30 in both cases, so no event was lost.

### 6.5 Add the field campus_building and filter one building
 
In producer.py, this line was added to each event:
 
```
"campus_building": random.choice(["A", "B", "C"]),
```
 
In consumer.py, `BUILDING = "A"` and the events from other buildings are skipped:
 
```
if event.get("campus_building") != BUILDING:
    continue
```
 
```
python consumer.py (photo 11)
python producer.py (photo 12)
```
 
Out of 30 events, __ belonged to building A and were displayed. `.get()` is used because the older events in the topic do not have this field. The filtering is done in the consumer, not by Kafka.
 
### 6.6 Optional: save events to a CSV file
 
consumer.py writes the displayed events to `events.csv` with the columns event_id, sensor_id, temperature_c, humidity_pct, and timestamp.

---
 
## Task 7: Answers for the Report
 
**1. What problem does Apache Kafka solve in a data-streaming architecture?**
 
Kafka separates the data producers from the data consumers. It transports, stores and distributes a continuous flow of events in real time, so several applications can read the same events independently, instead of using static files that delay the analysis.
 
**2. Define producer, consumer, broker, topic, partition, offset, and consumer group.**
 
- Producer: an application that publishes messages to a topic.
- Consumer: an application that reads messages from a topic.
- Broker: a Kafka server that stores the messages and serves the producers and consumers.
- Topic: a named category (stream) in which messages are organised.
- Partition: an ordered subdivision of a topic; it is the unit of parallelism.
- Offset: the sequence number of a message inside a partition.
- Consumer group: a set of consumers that share the reading of a topic; each partition is read by only one consumer of the group.

**3. What is the difference between a Kafka topic and a partition?**
 
A topic is a logical category of messages. A partition is a physical subdivision of the topic that stores a part of its messages in a fixed order and allows parallel reading and scaling.
 
**4. Why does the consumer use auto_offset_reset='earliest'? When does this setting apply?**
 
So that the consumer starts from the oldest available message. It applies only when the group has no committed offset (a new group) or when the stored offset is no longer valid. If an offset is already committed, the consumer continues from it.
 
**5. What is the purpose of the consumer group ID? What happens if a new group ID is used?**
 
The group ID identifies the group; Kafka uses it to store the committed offsets and to share the partitions between the consumers of the group. A new group ID has no stored offset, so auto_offset_reset applies and, with `earliest`, the group reads the whole topic again, independently of the other groups.
 
**6. Why is JSON a suitable format for this exercise? Give one limitation.**
 
JSON is readable by humans, simple, flexible, and supported by all languages. Limitation: it is verbose and slower than binary formats (Avro, Protobuf), and it does not enforce a strict schema, so changes in the fields may go unnoticed.
 
**7. Explain how Docker Compose simplifies deployment compared with starting services manually.**
 
The whole configuration is described in one reproducible file and started with one command (`docker compose up -d`). There is no manual installation of Kafka and Java, the environment is the same for all students, and stopping and cleaning are easy.
 
**8. What are the limitations of this single-broker educational setup compared with a production Kafka cluster?**
 
- No fault tolerance: replication factor 1, so a broker failure stops everything and can lose data.
- No real horizontal scaling: all partitions are on one machine.
- No security: PLAINTEXT listeners, no encryption or authentication.
- One node is both broker and controller.
- No monitoring or tuning, and the `localhost` advertised listener cannot serve remote clients.

**9. Evidence**

Running container: photo1. Topic description: photo7. Producer output: photo5. Consumer output: photo6.
---
 
## Cleanup
 
```
docker compose stop
docker compose down 
```
 
---
 
## Problems Encountered and Solutions
 
**1. `empty compose file` when running `docker compose up -d`**
 
The compose.yaml file was empty (the content was not saved). I recreated the file with the correct content, saved it.
 
**2. `ModuleNotFoundError: No module named 'kafka.vendor.six.moves'` when running producer.py**
 
The version `kafka-python==2.0.2` is not compatible with Python 3.12. I replaced it with a compatible version (`pip install --upgrade kafka-python`) and updated requirements.txt. The code did not change.
 