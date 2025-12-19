import pandas as pd


sales = pd.read_csv("data/sales.csv")
products = pd.read_csv("data/products.csv")
daily_inventory = pd.read_csv("data/daily_inventory.csv")
inventory_snapshot = pd.read_csv("data/inventory_snapshot.csv")
purchase_orders = pd.read_csv("data/purchase_orders.csv")
suppliers = pd.read_csv("data/suppliers.csv")


# check missing value in datasets
def missing_report(df, name):
    missing = df.isnull().sum()
    missing = missing[missing > 0]
    print(f"Missing value report for {name}:")
    print(missing)


missing_report(sales, "sales")
missing_report(products, "products")
missing_report(daily_inventory, "daily_inventory")
missing_report(inventory_snapshot, "inventory_snapshot")
missing_report(purchase_orders, "purchase_orders")
missing_report(suppliers, "suppliers")
