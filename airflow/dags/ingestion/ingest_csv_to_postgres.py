from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import os
import sys

# Add path untuk import db_connect
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# Import db_connect dari folder yang sama (ingestion/)
try:
    from db_connect import engine

    print("✅ Successfully imported db_connect")
except Exception as e:
    print(f"❌ Failed to import db_connect: {e}")
    raise

# Path ke CSV files (relative dari root project)
BASE_DIR = os.path.join(os.path.dirname(__file__), "../../../")

CSV_MAP = {
    "products": os.path.join(BASE_DIR, "data/products.csv"),
    "suppliers": os.path.join(BASE_DIR, "data/suppliers.csv"),
    "sales": os.path.join(BASE_DIR, "data/sales.csv"),
    "purchase_orders": os.path.join(BASE_DIR, "data/purchase_orders.csv"),
    "daily_inventory": os.path.join(BASE_DIR, "data/daily_inventory.csv"),
    "inventory_snapshot": os.path.join(BASE_DIR, "data/inventory_snapshot.csv"),
}


def ingest(table):
    """Ingest CSV data to PostgreSQL"""
    try:
        csv_path = CSV_MAP[table]
        print(f"Reading CSV from: {csv_path}")

        if not os.path.exists(csv_path):
            raise FileNotFoundError(f"CSV file not found: {csv_path}")

        df = pd.read_csv(csv_path)
        print(f"Loaded {len(df)} rows from {table}")

        df.to_sql(table, engine, schema="raw", if_exists="replace", index=False)
        print(f"✅ Successfully ingested {table} to database")

    except Exception as e:
        print(f"❌ Error ingesting {table}: {e}")
        raise


# check connectivity task for debugging
def check_connectivity():
    """Check database connectivity"""
    try:
        with engine.connect() as connection:
            result = connection.execute(
                "SELECT current_user, current_database(), version();"
            )
            row = result.fetchone()
            print(f"✅ Connected to database: {row}")
            return row
    except Exception as e:
        print(f"❌ Database connection failed: {e}")
        raise


with DAG(
    dag_id="csv_ingestion",
    description="Ingest CSV files to PostgreSQL raw schema",
    start_date=datetime(2025, 12, 19),
    schedule="@daily",  # Gunakan 'schedule' bukan 'schedule_interval'
    catchup=False,
    tags=["ingestion", "csv", "postgres"],
    default_args={
        "owner": "airflow",
        "retries": 1,
    },
) as dag:

    # task for checking connectivity
    test_db_connection = PythonOperator(
        task_id="check_db_connection", python_callable=check_connectivity
    )

    # iterate over CSV_MAP to create tasks dynamically
    for table in CSV_MAP:
        ingest_task = PythonOperator(
            task_id=f"ingest_{table}", python_callable=ingest, op_args=[table]
        )

        # set task dependencies
        test_db_connection >> ingest_task
