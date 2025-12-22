import pandas as pd
from dotenv import load_dotenv
import os
import sys
from sqlalchemy import text
from airflow import DAG

"""Data cleaning script to clean ingested data and store in 'cleaned' schema."""

# Add path to ingestion folder to import db_connect
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../ingestion"))

# Import db_connect from ingestion folder
from db_connect import engine


def clean_data():
    """Clean raw data and store in 'cleaned' schema"""
    # Read data using engine (tidak perlu connection context untuk read)
    df_sales = pd.read_sql("SELECT * FROM raw.sales", engine)
    df_products = pd.read_sql("SELECT * FROM raw.products", engine)
    df_daily_inventory = pd.read_sql("SELECT * FROM raw.daily_inventory", engine)
    df_inventory_snapshot = pd.read_sql("SELECT * FROM raw.inventory_snapshot", engine)
    df_suppliers = pd.read_sql("SELECT * FROM raw.suppliers", engine)
    df_purchase_orders = pd.read_sql("SELECT * FROM raw.purchase_orders", engine)

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

    # write back to cleaned schema -gunakan engine langsung
    print("Writing cleaned data to database...")
    df_sales.to_sql("sales", engine, schema="clean", if_exists="replace", index=False)
    df_products.to_sql(
        "products", engine, schema="clean", if_exists="replace", index=False
    )
    df_daily_inventory.to_sql(
        "daily_inventory",
        engine,
        schema="clean",
        if_exists="replace",
        index=False,
    )
    df_inventory_snapshot.to_sql(
        "inventory_snapshot",
        engine,
        schema="clean",
        if_exists="replace",
        index=False,
    )
    df_suppliers.to_sql(
        "suppliers", engine, schema="clean", if_exists="replace", index=False
    )
    df_purchase_orders.to_sql(
        "purchase_orders",
        engine,
        schema="clean",
        if_exists="replace",
        index=False,
    )

    print("✅ Data cleaning completed successfully!")
