from kafka import KafkaProducer
import json

class MatchKafkaProducer:
    def __init__(
            self,
            bootstrap_server: str = "localhost:9092"
    ):
        self.producer = KafkaProducer(bootstrap_servers=bootstrap_server, value_serializer = lambda v: json.dumps(v).encode("utf-8"))

    def send_match_event(
            self,
            topic: str,
            event: dict
    ):
        self.producer.send(topic=topic, event=event)
        self.producer.flush()


