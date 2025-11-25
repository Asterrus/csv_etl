from etl.transform.customers_processor import CustomersProcessor
from etl.transform.sales_processor import SalesProcessor


class SalesCustomersDataProcessor:
    def __init__(
        self, sales_processor: SalesProcessor, customers_processor: CustomersProcessor
    ):
        self.sales_processor = sales_processor
        self.customers_processor = customers_processor

    def validate(self):
        self.sales_processor.validate()
        self.customers_processor.validate()

    def process(self):
        self.sales_processor.process()
        self.customers_processor.process()
