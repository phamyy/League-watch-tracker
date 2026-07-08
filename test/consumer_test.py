import os
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import Unauthenticated, PermissionDenied
from consumer.config import databricks_access_token, databricks_hostname,warehouse_id
import requests
import json

def send_to_databricks():
    try:
        url = f"https://{databricks_hostname}/api/2.0/sql/statements"

        headers = {
            "Authorization": f"Bearer {databricks_access_token}"
        }

        payload = {
            "warehouse_id": warehouse_id,
            "statement": """
                INSERT INTO workspace.league_tracker.raw_matches_bronze(match_id)
                VALUES (:match_id)
            """,
            "parameters": [
                {"name": "match_id", "value": "test"}
            ]
        }

        response = requests.post(
                url,
                headers=headers,
                json=payload,
            )

        print(f"Status: {response.status_code}")
        print(response.json())

        response.raise_for_status()

        print(response.json())
    except Exception as e:
        print(f"There was an error sending data to databricks: {e}")
    
send_to_databricks()