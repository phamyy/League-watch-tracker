from dotenv import load_dotenv
import os

load_dotenv()

databricks_access_token = os.getenv("DATABRICKS_PAT")
databricks_hostname = os.getenv("HOSTNAME")
warehouse_id = os.getenv("WAREHOUSE_ID")