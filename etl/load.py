import os

import pandas as pd
from psycopg import Connection, connect


def drop_tables(connection: Connection):
    try:
        cursor = connection.cursor()
        cursor.execute("DROP TABLE IF EXISTS sales")
        cursor.execute("DROP TABLE IF EXISTS customers")
        cursor.execute("DROP TABLE IF EXISTS sales_summary")
        cursor.execute("DROP TABLE IF EXISTS product_ranking")
        connection.commit()
    except Exception as e:
        connection.rollback()
        print(e)
        raise


def create_tables(connection: Connection):
    drop_tables(connection)
    try:
        cursor = connection.cursor()

        with open("data/db.sql", "r", encoding="utf-8") as f:
            sql_script = f.read()

        cursor.execute(sql_script)
        connection.commit()
    except Exception as e:
        connection.rollback()
        print(e)
        raise


def load_dataframe(table: str, df: pd.DataFrame, connection: Connection):
    cur = connection.cursor()
    try:
        cur.execute(f"DELETE FROM {table}")
    except Exception as e:
        print(e)
        raise

    try:
        df.to_sql(table, connection)

    except Exception as e:
        print(e)
        raise


def get_connection() -> Connection:
    try:
        postgres_user = os.getenv("POSTGRES_USER")
        postgres_password = os.getenv("POSTGRES_PASSWORD")
        postgres_host = os.getenv("POSTGRES_HOST")
        postgres_port = os.getenv("POSTGRES_PORT")
        postgres_db = os.getenv("POSTGRES_DB")
        database_url = f"postgresql://{postgres_user}:{postgres_password}@{postgres_host}:{postgres_port}/{postgres_db}"
        return connect(database_url)
    except Exception as e:
        print(e)
        raise
