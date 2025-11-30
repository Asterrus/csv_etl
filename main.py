import logging

from dotenv import load_dotenv

from etl.extract import read_csv
from etl.load import create_tables, get_engine
from etl.transform.customers_processor import CustomersProcessor
from etl.transform.sales_customers_data_processor import SalesCustomersDataProcessor
from etl.transform.sales_processor import SalesProcessor

load_dotenv()
logger = logging.getLogger(__name__)


def main():
    engine = get_engine()
    logger.info("Starting ETL pipeline")

    with engine.begin() as connection:
        create_tables(connection)

        processor = SalesCustomersDataProcessor(
            sales_processor=SalesProcessor(read_csv("data/sales.csv")),
            customers_processor=CustomersProcessor(read_csv("data/customers.csv")),
        )

        processor.validate()
        logger.info("Data validation completed.")

        processor.process()
        logger.info("Data processing completed.")

        processor.load_data(connection)
        logger.info("Data loading completed.")

    logger.info("ETL pipeline completed successfully")
    print("ETL pipeline completed successfully")


if __name__ == "__main__":
    main()
