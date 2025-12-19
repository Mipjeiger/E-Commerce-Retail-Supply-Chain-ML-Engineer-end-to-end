from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
from db-connect import engine

CSV_MAP = {
    "products": "data/products.csv",
    "suppliers": "data/suppliers.csv",
    "sales": "data/sales.csv",
    "purchase_orders": "data/purchase_orders.csv",
    "daily_inventory": "data/daily_inventory.csv",
    "inventory_snapshot": "data/inventory_snapshot.csv",
}


def ingest(table):
    df = pd.read_csv(CSV_MAP[table])
    df.to_sql(
        table,
        engine,
    )
