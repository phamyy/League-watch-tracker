from kafka import KafkaConsumer
import json
import psycopg2
import time


class AnalyticsConsumer:
    
    def __init__(self):
        self.consumer = self.connect_to_kafka()
        self.connection = self.connect_to_postgresql()
        self.cursor = self.connection.cursor()

    def connect_to_kafka(self):
        while True:
            try:
                consumer = KafkaConsumer(
                    "match-events",
                    bootstrap_servers="kafka:9092",
                    auto_offset_reset="earliest",
                    value_deserializer=lambda m: json.loads(
                        m.decode("utf-8")
                    )
                )
                print('Consumer connected to Kafka')
                return consumer
            
            except Exception as e:
                print ('Consumer waiting for Kafka')
                time.sleep(5)

    def connect_to_postgresql(self):
        while True:
            try:
                connection = psycopg2.connect(
                    host="postgres",
                    port=5432,
                    database="riot_analytics",
                    user="riot",
                    password="riotpassword"
                )
                print('Consumer connected to Postgresql')
                return connection
            except Exception as e:
                print('Consumer Postgresql unavailable')
                time.sleep(5)

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

def run_consumer():
    consumer = AnalyticsConsumer()
    consumer.insert_data()

if __name__ == "__main__":
    run_consumer()