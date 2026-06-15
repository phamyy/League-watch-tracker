from kafka import KafkaConsumer
import json
import psycopg2


class AnalyticsConsumer:
    
    def __init__(self):
        self.consumer = KafkaConsumer(
            "match-events",
            bootstrap_servers="localhost:9092",
            auto_offset_reset="earliest",
            value_deserializer=lambda m: json.loads(
                m.decode("utf-8")
            )
        )

        self.connection = psycopg2.connect(
            host="localhost",
            port=5433,
            database="riot_analytics",
            user="riot",
            password="riotpassword"
        )
        self.cursor = self.connection.cursor()


    def print_messages_in_queue(self):
        while True:
            try:
                for msg in self.consumer:
                    print(f"message: {msg.value}")
                    print(f"match_id: {msg.value['match_id']}")
                    print(f"payload: {msg.value['payload']}")
            except Exception as error:
                print(f"Consumer error: {error}")

    def insert_data(self):
        while True:
            try:
                for msg in self.consumer:
                    self.cursor.execute("""
                    INSERT INTO raw_matches (match_id, payload)
                    VALUES (%s, %s)
                    ON CONFLICT (match_id) DO NOTHING
                    """, (
                    msg.value['match_id'],
                    json.dumps(msg.value['payload'])
                    ))
                    print(f"data has been pushed for match_id: {msg.value['match_id']}")
                    self.connection.commit()
            except Exception as error:
                print(f"Consumer error: {error}")