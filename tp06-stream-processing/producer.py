
import json
import time
import random
from datetime import datetime, timedelta, timezone
from kafka import KafkaProducer

TOPIC = "campus-events"

DEPARTMENTS = [
    "Computer Science",
    "Mathematics",
    "Physics"
]

EVENT_TYPES = [
    "check-in",
    "open-course"
]

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda value: json.dumps(value).encode("utf-8"),
    acks="all"
)

base = datetime.now(timezone.utc).replace(microsecond=0)

total_events = 100
start_time = time.perf_counter()

try:
    for i in range(1, total_events + 1):

        student = f"S{random.randint(100, 999)}"
        department = random.choice(DEPARTMENTS)
        event_type = random.choice(EVENT_TYPES)

        event_time = base + timedelta(seconds=i)

        event = {
            "eventId": i,
            "studentId": student,
            "eventType": event_type,
            "department": department,
            "eventTime": event_time.strftime(
                "%Y-%m-%dT%H:%M:%S.%f"
            )[:-3]
        }

        producer.send(TOPIC, value=event)
        print("Sent:", event)

    producer.flush()

finally:
    producer.close()

elapsed = time.perf_counter() - start_time

print("\n===== Producer Statistics =====")
print("Total events:", total_events)
print("Elapsed time:", round(elapsed, 3), "seconds")
print("Ingestion rate:", round(total_events / elapsed, 2), "events/second")
