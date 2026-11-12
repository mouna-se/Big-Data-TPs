import json 
from kafka import KafkaConsumer 
 
TOPIC = "campus-sensors" 
 
consumer = KafkaConsumer( 
    TOPIC, 
    bootstrap_servers="localhost:9092", 
    auto_offset_reset="earliest", 
    enable_auto_commit=True, 
    group_id="tp05-campus-monitor", 
    value_deserializer=lambda value: json.loads(value.decode("utf-8")), 
) 

total = 0.0
count = 0
 
print(f"Listening to topic: {TOPIC}") 
try:
    for message in consumer:
        event = message.value
        temperature = event["temperature_c"]
 
        # Update running average
        total += temperature
        count += 1
        average = total / count
 
        print(
            f'{event["timestamp"]} | {event["sensor_id"]} | '
            f'T={temperature}°C | RH={event["humidity_pct"]}%'
        )
        print(f"Running average temperature: {average:.2f}°C (messages: {count})")
 
        if temperature >= 35:
            print("ALERT: High temperature detected!")
except KeyboardInterrupt:
    print("Consumer stopped by user.")
finally:
    consumer.close()
 