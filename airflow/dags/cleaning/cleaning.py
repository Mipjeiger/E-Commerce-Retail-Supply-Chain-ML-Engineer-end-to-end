import pandas as pd
from dotenv import load_dotenv
import os
import sys
from airflow import DAG

"""Data cleaning script to clean ingested data and store in 'cleaned' schema."""

# Add path to ingestion folder to import db_connect
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../ingestion"))

# Import db_connect from ingestion folder
from db_connect import engine


def clean_data():
    with engine.connect() as connection:
        df_sales = pd.read_sql("SELECT * FROM raw.sales", connection)
        df_products = pd.read_sql("SELECT * FROM raw.products", connection)
        df_daily_inventory = pd.read_sql("SELECT * FROM raw.daily_invetory", connection)
        df_inventory_snapshot = pd.read_sql(
            "SELECT * FROM raw.inventory_snapshot", connection
        )
        df_suppliers = pd.read_sql("SELECT * FROM raw.suppliers", connection)
        df_purchase_orders = pd.read_sql(
            "SELECT * FROM raw.purchase_orders", connection
        )

        # cleaning sales data
        df_sales["event_name"].fillna("No Event", inplace=True)

        # cleaning products data
        df_products["parent_sku"].fillna("No Parent", inplace=True)
        df_products["shelf_life_months"].fillna(
            df_products["shelf_life_months"].median(), inplace=True
        )

        # cleaning purchase orders data
        df_purchase_orders.dropna(inplace=True)

        # cleaning daily inventory data
        df_daily_inventory.dropna(inplace=True)

        # cleaning suppliers data
        df_suppliers.dropna(inplace=True)

        # cleaning inventory snapshot data
        df_inventory_snapshot.dropna(inplace=True)

        # write back to cleaned schema
        df_sales.to_sql(
            "sales", engine, schema="cleaned", if_exists="replace", index=False
        )
        df_products.to_sql(
            "products", engine, schema="cleaned", if_exists="replace", index=False
        )
        df_daily_inventory.to_sql(
            "daily_inventory",
            engine,
            schema="cleaned",
            if_exists="replace",
            index=False,
        )
        df_inventory_snapshot.to_sql(
            "inventory_snapshot",
            engine,
            schema="cleaned",
            if_exists="replace",
            index=False,
        )
        df_suppliers.to_sql(
            "suppliers", engine, schema="cleaned", if_exists="replace", index=False
        )
        df_purchase_orders.to_sql(
            "purchase_orders",
            engine,
            schema="cleaned",
            if_exists="replace",
            index=False,
        )


if __name__ == "__main__":
    clean_data()
    print("Data cleaning completed and saved to 'cleaned' schema.")
