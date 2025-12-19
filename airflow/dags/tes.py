import os
import sys
from dotenv import load_dotenv

# add path to sql folder SEBELUM import db_connect
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../../sql/ingestion"))

load_dotenv()

from airflow.dags.ingestion.db_connect import engine


# test connection to verify db_connect from engine
def test_db_connection():
    with engine.connect() as connection:
        result = connection.execute("SELECT current_user, current_database();")
        print(result.fetchone())


if __name__ == "__main__":
    test_db_connection()
