import json
import random
import time
from datetime import datetime, timezone
from kafka import KafkaProducer

TOPIC = "student-events"

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda value: json.dumps(value).encode("utf-8"),
    acks="all",
)

try:
    for event_id in range(1, 11):
        event = {
            "event_id": event_id,
            "student_id": f"student-{random.randint(1, 5):02d}",
            "score": round(random.uniform(8, 20), 2),
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }

        producer.send(TOPIC, value=event)
        print("Sent:", event)
        time.sleep(1)

    producer.flush()

finally:
    producer.close()