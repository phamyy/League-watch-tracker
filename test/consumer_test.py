import os
from databricks.sdk import WorkspaceClient
# from databricks.sdk.errors import Unauthenticated, PermissionDenied
# from consumer.config import databricks_access_token, databricks_hostname,warehouse_id
import requests
import json
from kafka import KafkaConsumer
import psycopg2
# from ..consumer.config import supabase_url

# def send_to_databricks():
#     try:
#         url = f"https://{databricks_hostname}/api/2.0/sql/statements"

#         headers = {
#             "Authorization": f"Bearer {databricks_access_token}"
#         }

#         payload = {
#             "warehouse_id": warehouse_id,
#             "statement": """
#                 INSERT INTO workspace.league_tracker.raw_matches_bronze(match_id)
#                 VALUES (:match_id)
#             """,
#             "parameters": [
#                 {"name": "match_id", "value": "test"}
#             ]
#         }

#         response = requests.post(
#                 url,
#                 headers=headers,
#                 json=payload,
#             )

#         print(f"Status: {response.status_code}")
#         print(response.json())

#         response.raise_for_status()

#         print(response.json())
#     except Exception as e:
#         print(f"There was an error sending data to databricks: {e}")
    
# send_to_databricks()


# try:
#     consumer = KafkaConsumer(
#         "match-events",
#         bootstrap_servers="localhost:9092",
#         auto_offset_reset="earliest",
#         value_deserializer=lambda m: json.loads(
#             m.decode("utf-8")
#         )
#     )
#     print('Consumer connected to Kafka')

#     for msg in consumer:
#         print(msg.value["match_id"])
#         print(json.dumps(msg.value["payload"]))

# except Exception as e:
#     print ('Consumer waiting for Kafka')
def test_insert():
    INSERT_SQL = """
        INSERT INTO raw_matches_bronze(match_id, payload)
        VALUES (
                %s,
                %s
            )
        ON CONFLICT(match_id)
        DO NOTHING; 
        """
    
    try:

        connection = psycopg2.connect('postgresql://postgres:20Front61!082002@db.bckirwktfdcglcobdias.supabase.co:5432/postgres')
        cursor = connection.cursor()

        cursor.execute(
            INSERT_SQL,
            ('TEST_1',
            json.dumps({
                "name": "John",
                "age": 30,
                "isStudent": "false",
                "skills": ["reading", "coding"],
                "address": {
                    "city": "New York",
                    "zip": "10001"
                }
            })
            )
        )

        connection.commit()
    except Exception as e:
        print(F"ERROR:{e}")

if __name__ == "__main__":
    test_insert()