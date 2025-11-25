import pandas as pd

from etl.transform.sales_processor import SalesProcessor


class TestSalesProcessorValidation:
    def test_order_date_validation(self):
        data = pd.DataFrame({"order_date": ["2022-01-01", "invalid_date"]})
        processor = SalesProcessor(data)
        processor._validate_order_date()
        expected = pd.DataFrame({"order_date": [pd.to_datetime("2022-01-01"), pd.NaT]})
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
        expected = pd.DataFrame({"quantity": [10, pd.to_numeric(pd.NA)]})
        pd.testing.assert_frame_equal(processor.data, expected)

    def test_unit_price_validation(self):
        data = pd.DataFrame({"unit_price": ["10.5", "invalid"]})
        processor = SalesProcessor(data)
        processor._validate_unit_price()
        expected = pd.DataFrame({"unit_price": [10.5, pd.to_numeric(pd.NA)]})
        pd.testing.assert_frame_equal(processor.data, expected)


class TestSalesProcessor:
    pass
