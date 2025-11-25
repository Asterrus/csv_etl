from etl.extract import read_csv
from etl.transform.customers_processor import CustomersProcessor
from etl.transform.sales_customers_data_processor import SalesCustomersDataProcessor
from etl.transform.sales_processor import SalesProcessor


def main():
    processor = SalesCustomersDataProcessor(
        sales_processor=SalesProcessor(read_csv("data/sales.csv")),
        customers_processor=CustomersProcessor(read_csv("data/customers.csv")),
    )
    processor.validate()
    processor.process()
    print("Data processing completed.")
    print("Sales:")
    print(processor.sales_processor.data)
    print("Customers:")
    print(processor.customers_processor.data)


if __name__ == "__main__":
    main()
