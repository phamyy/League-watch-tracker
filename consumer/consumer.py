from kafka import KafkaConsumer
import json
import psycopg2
import time
import requests
from .config import databricks_access_token, databricks_hostname,warehouse_id, databricks_http_path
from databricks import sql

class AnalyticsConsumer:
    
    def __init__(self):
        self.consumer = self.connect_to_kafka()
        # self.connection = self.connect_to_postgresql()
        # self.cursor = self.connection.cursor()

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
    
    """
    Want to see if I can migrate the database from kubernetes to Databricks lakehouse. 

    1. Need to set up databricks DB instance
    2. Create the tables for it 
    3. See if there's a better way to dump the data instead of just dumping the JSON. (Could also maybe do the transformation upstream in databricks)
    """

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
                    with sql.connect(server_hostname = databricks_hostname,
                        http_path       = databricks_http_path,
                        access_token    = databricks_access_token) as connection:
                            
                            cursor = connection.cursor()

                            cursor.execute(
                                """
                                INSERT INTO workspace.league_tracker.raw_matches_bronze
                                (match_id, payload)
                                VALUES (?, ?)
                                """,
                                (
                                    msg.value["match_id"],
                                    json.dumps(msg.value["payload"])
                                )
                            )
                            print(f"data has been pushed to databricks for match_id: {msg.value['match_id']}")
                            connection.commit()

            except Exception as error:
                print(f"Consumer error: {error}")
    

def run_consumer():
    consumer = AnalyticsConsumer()
    consumer.insert_data()

if __name__ == "__main__":
    run_consumer()