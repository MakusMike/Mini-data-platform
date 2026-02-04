import pandas as pd
from sqlalchemy import create_engine

DB_USER = "datauser"
DB_PASSWORD = "datapass"
DB_HOST = "localhost"
DB_PORT = "5432"
DB_NAME = "datadb"

engine = create_engine(
    f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
)

files = {
    "customers": "data/customers.csv",
    "products": "data/products.csv",
    "orders": "data/orders.csv"
}

for table_name, file_path in files.items():
    print(f"Loading {file_path} > table {table_name}")
    df = pd.read_csv(file_path)
    df.to_sql(table_name, engine, if_exists="replace", index=False)
    print(f"Table {table_name} ready\n")

print("Whole data loaded to Postgres")
