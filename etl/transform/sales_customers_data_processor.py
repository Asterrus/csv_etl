import pandas as pd

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

    def _create_sales_summary_df(self) -> pd.DataFrame:
        summary = self.sales_processor.data.groupby("category", as_index=False).agg(
            total_sales=("order_id", "count"),
            total_quantity=("quantity", "sum"),
            average_order_value=("total_price", "sum"),
            period_date=(
                "month",
                "max",  # Тут не уверен
            ),
        )

        return summary  # type: ignore[reportReturnType]

    def process(self):
        self.sales_processor.process()
        self.customers_processor.process()

        sales_summary_df = self._create_sales_summary_df()
