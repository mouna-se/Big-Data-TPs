import json
from kafka import KafkaConsumer

TOPIC = "student-events"

consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers="localhost:9092",
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id="student-group",
    client_id="student-consumer-2",
    value_deserializer=lambda value: json.loads(
        value.decode("utf-8")
    ),
)

print(f"Listening to topic: {TOPIC}")

try:
    for message in consumer:
        event = message.value
        print("Received:", event)

except KeyboardInterrupt:
    print("Consumer stopped by user.")

finally:
    consumer.close()