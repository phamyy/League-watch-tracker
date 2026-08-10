from dotenv import load_dotenv
import os

load_dotenv()

databricks_access_token = os.getenv("DATABRICKS_PAT")
databricks_hostname = os.getenv("HOSTNAME")
warehouse_id = os.getenv("WAREHOUSE_ID")
databricks_http_path = os.getenv("DATABRICKS_HTTP_PATH")
supabase_url = os.getenv("SUPABASE_URL")