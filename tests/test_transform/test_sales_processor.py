import pandas as pd

from etl.transform.sales_processor import SalesProcessor


class TestSalesProcessorValidation:
    def test_order_date_validation(self):
        data = pd.DataFrame({"order_date": ["2022-01-01", "invalid_date"]})
        processor = SalesProcessor(data)
        processor._validate_order_date()
        expected = pd.DataFrame({"order_date": [pd.Timestamp("2022-01-01"), pd.NaT]})
        pd.testing.assert_frame_equal(processor.data, expected)

    def test_remove_duplicates(self):
        data = pd.DataFrame({"id": [1, 2, 2], "value": [10, 20, 20]})
        processor = SalesProcessor(data)
        processor._remove_duplicates()
        expected = pd.DataFrame({"id": [1, 2], "value": [10, 20]})
        pd.testing.assert_frame_equal(processor.data, expected)

    def test_quantity_validation(self):
        data = pd.DataFrame({"quantity": ["10", "invalid"]})
        processor = SalesProcessor(data)
        processor._validate_quantity()
        expected = pd.DataFrame({"quantity": [10, float("nan")]})
        pd.testing.assert_frame_equal(processor.data, expected)

    def test_unit_price_validation(self):
        data = pd.DataFrame({"unit_price": ["10.5", "invalid"]})
        processor = SalesProcessor(data)
        processor._validate_unit_price()
        expected = pd.DataFrame({"unit_price": [10.5, float("nan")]})
        pd.testing.assert_frame_equal(processor.data, expected)


class TestSalesProcessor:
    def test_add_total_price(self):
        data = pd.DataFrame(
            {
                "quantity": [2, 3, float("nan"), 4],
                "unit_price": [10, 20, 1, float("nan")],
            }
        )
        processor = SalesProcessor(data)
        processor._add_total_price()
        expected = pd.DataFrame(
            {
                "quantity": [2, 3, float("nan"), 4],
                "unit_price": [10, 20, 1, float("nan")],
                "total_price": [20, 60, float("nan"), float("nan")],
            }
        )
        pd.testing.assert_frame_equal(processor.data, expected)

    def test_add_month_of_order(self):
        data = pd.DataFrame(
            {
                "order_date": [
                    pd.Timestamp("2022-01-01"),
                    pd.Timestamp("2024-05-01"),
                    pd.NaT,
                ],
            }
        )
        processor = SalesProcessor(data)
        processor._add_month_of_order()
        expected = pd.DataFrame(
            {
                "order_date": [
                    pd.Timestamp("2022-01-01"),
                    pd.Timestamp("2024-05-01"),
                    pd.NaT,
                ],
                "month": [
                    "2022-01",
                    "2024-05",
                    None,
                ],
            }
        )
        pd.testing.assert_frame_equal(processor.data, expected)

    def test_process(self):
        data = pd.DataFrame(
            {
                "order_date": [
                    pd.Timestamp("2022-01-01"),
                ],
                "quantity": [2],
                "unit_price": [10],
            }
        )
        processor = SalesProcessor(data)
        processor.process()
        assert "total_price" in processor.data.columns
        assert "month" in processor.data.columns
