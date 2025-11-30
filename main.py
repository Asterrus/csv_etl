import logging

from dotenv import load_dotenv

from etl.extract import read_csv
from etl.load import create_tables, get_connection, load_dataframe
from etl.transform.customers_processor import CustomersProcessor
from etl.transform.sales_customers_data_processor import SalesCustomersDataProcessor
from etl.transform.sales_processor import SalesProcessor

load_dotenv()
logger = logging.getLogger(__name__)


def main():
    connection = get_connection()
    create_tables(connection)

    processor = SalesCustomersDataProcessor(
        sales_processor=SalesProcessor(read_csv("data/sales.csv")),
        customers_processor=CustomersProcessor(read_csv("data/customers.csv")),
    )

    processor.validate()
    logger.info("Data validation completed.")
    processor.process()
    logger.info("Data processing completed.")

    load_dataframe("customers", processor.customers_processor.data, connection)
    load_dataframe("sales", processor.sales_processor.data, connection)
    load_dataframe("sales_summary", processor.sales_summary_df, connection)
    load_dataframe("product_ranking", processor.product_ranking_df, connection)

    logger.info("Data loading completed.")


if __name__ == "__main__":
    main()
