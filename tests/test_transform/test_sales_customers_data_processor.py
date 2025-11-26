import pandas as pd

from etl.transform.customers_processor import CustomersProcessor
from etl.transform.sales_customers_data_processor import SalesCustomersDataProcessor
from etl.transform.sales_processor import SalesProcessor


class TestSalesCustomerDataProcessor:
    def test_create_sales_summary_df(self):
        sales_df = pd.DataFrame(
            {
                "order_id": [1, 2, 3],
                "customer_id": [1, 2, 3],
                "product_id": [101, 102, 103],
                "quantity": [2, 3, 1],
                "price": [10, 20, 30],
                "category": ["A", "B", "B"],
                "month": ["Jan", "Feb", "Mar"],
                "total_price": [20, 60, 30],
            }
        )
        sales_processor = SalesProcessor(sales_df)

        customers_processor = CustomersProcessor(pd.DataFrame())
        processor = SalesCustomersDataProcessor(sales_processor, customers_processor)
        result_df = processor._create_sales_summary_df()
        print(result_df)
        expected = pd.DataFrame(
            {
                "category": ["A", "B"],
                "total_sales": [1, 2],
                "total_quantity": [2, 4],
                "average_order_value": [20, 90],
                "period_date": ["Jan", "Mar"],
            }
        )
        pd.testing.assert_frame_equal(result_df, expected)
