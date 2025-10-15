# Import necessary libraries
import os
from sqlalchemy import create_engine
import pandas as pd

# ---- Connection config (override with env vars if you like) ----
MYSQL_USER = os.getenv("MYSQL_USER", "citizix_user")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD", "An0thrS3crt")
MYSQL_DB = os.getenv("MYSQL_DATABASE", "scikey")

# If your Python runs on the host machine:
MYSQL_HOST = os.getenv("MYSQL_HOST", "127.0.0.1")
MYSQL_PORT = int(os.getenv("MYSQL_PORT", "5326"))

# If your Python runs in another container on the same docker-compose network,
# set these env vars instead:
#   MYSQL_HOST=db
#   MYSQL_PORT=3306

DB_URL = (
    f"mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}"
    f"@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DB}?charset=utf8mb4"
)

engine = create_engine(DB_URL, pool_pre_ping=True, future=True)

def load_data(df: pd.DataFrame, table_name: str = "EcommerceData", if_exists: str = "replace"):
    """
    Load a pandas DataFrame into a MySQL table.

    :param df: DataFrame to write
    :param table_name:   Destination table name
    :param if_exists:    'fail' | 'replace' | 'append'
    """
    # Ensure DataFrame exists
    if df is None or df.empty:
        raise ValueError("ecommerce_df is empty or None.")

    # Write to MySQL
    with engine.begin() as conn:  # transaction-safe
        df.to_sql(
            name=table_name,
            con=conn,
            if_exists=if_exists,   # change to 'append' if you want to keep rows
            index=False,
            chunksize=1000,
            method="multi"
        )
    print("Data successfully written to MySQL.")