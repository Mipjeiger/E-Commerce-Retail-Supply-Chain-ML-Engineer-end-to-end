import os
from sqlalchemy import create_engine
from dotenv import load_dotenv

"""RUN db connection in AIRFLOW environment"""

# Load .env from airflow folder (.env)
env_path = os.path.join(os.path.dirname(__file__), "../../../.env")
load_dotenv(dotenv_path=env_path)

DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_NAME = os.getenv("DB_NAME")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")

engine = create_engine(
    f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
)

print(
    f"[AIRFLOW] Connecting to PostgreSQL at {DB_HOST}:{DB_PORT} with user '{DB_USER}' to database '{DB_NAME}'"
)


# Test connection function
def test_connection():
    """Test database connection."""
    try:
        with engine.connect() as connection:
            # test 1: basic query connection
            result = connection.execute("SELECT 1;")
            print(f"Test 1 - Basic query: {result.fetchone()}")

            # Test 2: Get database info
            result = connection.execute("SELECT current_user, current_database();")
            user, db = result.fetchone()
            print(f"Test 2 - DB Info: User: {user}, Database: {db}")

            # Test 3: List tables
            result = connection.execute(
                """
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = 'public'
                LIMIT 5;"""  # limit to 5 tables for brevity
            )
            tables = result.fetchall()
            print(f"Test 3 - Tables: {[table[0] for table in tables]}")
            print("[AIRFLOW] All tests passed successfully.")
            return True
    except Exception as e:
        print(f"[AIRFLOW] Connection failed: {e}")
        return False


# usage
if __name__ == "__main__":
    # Test connection
    test_connection()
