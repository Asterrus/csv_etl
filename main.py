import logging

from dotenv import load_dotenv

from etl.extract import read_csv
from etl.load import create_tables, get_engine, load_dataframe
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

        assert processor.sales_summary_df is not None
        assert processor.product_ranking_df is not None

        load_dataframe("customers", processor.customers_processor.data, connection)
        load_dataframe("sales", processor.sales_processor.data, connection)
        load_dataframe("sales_summary", processor.sales_summary_df, connection)
        load_dataframe("product_ranking", processor.product_ranking_df, connection)

        logger.info("Data loading completed.")

    logger.info("ETL pipeline completed successfully")
    print("ETL pipeline completed successfully")


if __name__ == "__main__":
    main()
