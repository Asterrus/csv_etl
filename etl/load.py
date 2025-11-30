import logging
import os

import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.engine import Connection, Engine

logger = logging.getLogger(__name__)


def drop_tables(connection: Connection):
    try:
        connection.exec_driver_sql("DROP TABLE IF EXISTS sales")
        connection.exec_driver_sql("DROP TABLE IF EXISTS customers")
        connection.exec_driver_sql("DROP TABLE IF EXISTS sales_summary")
        connection.exec_driver_sql("DROP TABLE IF EXISTS product_ranking")
    except Exception as e:
        logger.error(f"Error dropping tables: {e}")
        raise


def create_tables(connection: Connection):
    drop_tables(connection)
    try:
        with open("data/db.sql", "r", encoding="utf-8") as f:
            sql_script = f.read()

        connection.exec_driver_sql(sql_script)
        logger.info("Tables created successfully")
    except Exception as e:
        logger.error(f"Error creating tables: {e}")
        raise


def load_dataframe(table: str, df: pd.DataFrame, connection: Connection):
    try:
        connection.exec_driver_sql(f"DELETE FROM {table}")
    except Exception as e:
        logger.error(f"Error deleting from {table}: {e}")
        raise

    try:
        df.to_sql(
            table,
            con=connection,
            if_exists="append",
            index=False,
            method="multi",
        )
        logger.info(f"Loaded {len(df)} rows into {table}")
    except Exception as e:
        logger.error(f"Error loading data into {table}: {e}")
        raise


def get_engine() -> Engine:
    try:
        postgres_user = os.getenv("POSTGRES_USER")
        postgres_password = os.getenv("POSTGRES_PASSWORD")
        postgres_host = os.getenv("POSTGRES_HOST")
        postgres_port = os.getenv("POSTGRES_PORT")
        postgres_db = os.getenv("POSTGRES_DB")
        database_url = (
            f"postgresql+psycopg://{postgres_user}:{postgres_password}@"
            f"{postgres_host}:{postgres_port}/{postgres_db}"
        )
        engine = create_engine(database_url, future=True)
        logger.info("Database engine created successfully")
        return engine
    except Exception as e:
        logger.error(f"Error creating database engine: {e}")
        raise
