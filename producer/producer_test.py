from kafka import KafkaProducer
import json

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

producer.send(
    "match-events",
    {
        "match_id": "TEST_MATCH",
        "champion": "Ahri"
    }
)

producer.flush()

print("Message sent")