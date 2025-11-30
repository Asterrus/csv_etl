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
                "product_id": [1, 2, 3],
                "quantity": [2, 3, 1],
                "price": [10, 20, 30],
                "category": ["A", "B", "B"],
                "month": ["2022-01", "2022-02", "2022-03"],
                "total_price": [20, 60, 30],
            }
        )
        sales_processor = SalesProcessor(sales_df)

        customers_processor = CustomersProcessor(pd.DataFrame())
        processor = SalesCustomersDataProcessor(sales_processor, customers_processor)
        result_df = processor._create_sales_summary_df()
        expected = pd.DataFrame(
            {
                "category": ["A", "B"],
                "total_sales": [1, 2],
                "total_quantity": [2, 4],
                "average_order_value": [20, 45.0],
                "period_date": [
                    pd.to_datetime("2022-01-01").date(),
                    pd.to_datetime("2022-03-01").date(),
                ],
            }
        )
        pd.testing.assert_frame_equal(result_df, expected)

    def test_create_product_ranking_df(self):
        # TODO Проверить влияние на тесты лишних данных
        sales_df = pd.DataFrame(
            {
                "order_id": [1, 2, 3, 4, 5, 6, 7],
                "customer_id": [1, 1, 1, 1, 1, 1, 1],
                "product_id": [1, 2, 3, 4, 5, 1, 6],
                "product_name": ["1", "2", "3", "4", "5", "1", "6"],
                "quantity": [1, 2, 3, 4, 5, 6, 1],
                "price": [10, 20, 30, 40, 50, 60, 1],
                "category": ["A", "A", "A", "A", "A", "A", "A"],
                "month": [
                    "2022-01",
                    "2022-01",
                    "2022-01",
                    "2022-01",
                    "2022-01",
                    "2022-01",
                    "2022-01",
                ],
                "total_price": [10, 40, 90, 160, 250, 360, 1],  # TODO Возможно float?
            }
        )
        sales_processor = SalesProcessor(sales_df)
        customers_processor = CustomersProcessor(pd.DataFrame())
        processor = SalesCustomersDataProcessor(sales_processor, customers_processor)
        result_df = processor._create_product_ranking_df()
        expected = pd.DataFrame(
            {
                "product_id": [1, 5, 4, 3, 2],
                "product_name": ["1", "5", "4", "3", "2"],
                "total_sold": [7, 5, 4, 3, 2],
                "total_revenue": [370, 250, 160, 90, 40],
                "rank_position": [1, 2, 3, 4, 5],
            }
        )
        pd.testing.assert_frame_equal(result_df, expected)

    def test_create_average_bill_by_region_df(self):
        sales_df = pd.DataFrame(
            {
                "order_id": [1, 2, 3],
                "customer_id": ["1", "2", "2"],
                "product_id": [1, 2, 3],
                "quantity": [2, 3, 1],
                "price": [10, 20, 30],
                "category": ["A", "B", "B"],
                "month": ["2022-01", "2022-02", "2022-03"],
                "total_price": [20, 60, 30],
            }
        )
        sales_processor = SalesProcessor(sales_df)
        customers_df = pd.DataFrame(
            {
                "customer_id": ["1", "2"],
                "region": ["1", "2"],
            }
        )
        customers_processor = CustomersProcessor(customers_df)
        processor = SalesCustomersDataProcessor(sales_processor, customers_processor)
        result_df = processor._create_average_bill_by_region_df()

        expected = pd.DataFrame(
            {
                "region": ["1", "2"],
                "average_bill": [20.0, 45.0],
            }
        )
        pd.testing.assert_frame_equal(result_df, expected)
