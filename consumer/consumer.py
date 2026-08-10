from kafka import KafkaConsumer
import json
import psycopg2
import time
import requests
from .config import databricks_access_token, databricks_hostname,warehouse_id, databricks_http_path,supabase_url
from databricks import sql

INSERT_SQL = """
        INSERT INTO raw_matches_bronze(match_id, payload)
        VALUES (
                %s,
                %s
            )
        ON CONFLICT(match_id)
        DO NOTHING; 
        """


class AnalyticsConsumer:
    
    def __init__(self):
        self.consumer = self.connect_to_kafka()
        self.connection = self.connect_to_database()
        self.cursor = self.connection.cursor()


    def connect_to_kafka(self):
        while True:
            try:
                consumer = KafkaConsumer(
                    "match-events",
                    bootstrap_servers='kafka:9092',
                    auto_offset_reset='earliest',
                    group_id='riot-match-consumer-group',
                    enable_auto_commit=False,
                    value_deserializer=lambda m: json.loads(
                        m.decode("utf-8")
                    )
                )
                
                print('Consumer connected to Kafka')
                return consumer
            
            except Exception as e:
                print ('Consumer waiting for Kafka')
                time.sleep(5)

    def connect_to_database(self):
        while True:
            try:
                connection = psycopg2.connect(supabase_url)
                print('CONSUMER CONNECTED TO SUPABASE')
                return connection
            except Exception as e:
                print('CONSUMER SUPABASE CONNECTION UNAVAILABLE')
                print(repr(supabase_url))
                print(F"ERROR: {e}")
                time.sleep(5)

    def poll_batch(self):

        records = self.consumer.poll(
            timeout_ms=10000,
            max_records=20
        )

        batch = []

        for _, messages in records.items():

            for message in messages:

                batch.append(message.value)

        return batch
    
    def write_batch(self, batch):

        for record in batch:

            self.cursor.execute(
                INSERT_SQL,
                (
                    record["match_id"],
                    json.dumps(record["payload"])
                )
            )

        self.connection.commit()

    def start(self):

        while True:
            try:
                records = self.poll_batch()

                if len(records) == 0:
                    print("WAITING FOR MESSAGES")
                    continue
                
                print(f"Received {len(records)} messages")

                self.write_batch(records)

                self.consumer.commit()

                print(f"Committed {len(records)} records")

            except psycopg2.OperationalError as oe:
                print(oe)
                try:
                    self.connection.close()
                except:
                    pass
                self.connection = self.connect_to_database()
                self.cursor = self.connection.cursor()
                
            except Exception as error:
                print(f"Consumer error: {error}")
                if self.connection:
                    self.connection.rollback()
                
                raise
    

def run_consumer():
    consumer = AnalyticsConsumer()
    consumer.start()

if __name__ == "__main__":
    run_consumer()