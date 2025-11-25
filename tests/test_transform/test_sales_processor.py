import pandas as pd

from etl.transform.sales_processor import SalesProcessor


class TestSalesProcessor:
    def test_remove_duplicates(self):
        data = pd.DataFrame({"id": [1, 2, 2], "value": [10, 20, 20]})
        processor = SalesProcessor(data)
        processor.process()
        expected = pd.DataFrame({"id": [1, 2], "value": [10, 20]})
        pd.testing.assert_frame_equal(processor.data, expected)
