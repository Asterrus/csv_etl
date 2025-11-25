import pandas as pd
import pytest

from etl.transform.customers_processor import CustomersProcessor
from exceptions import InvalidEmailError


class TestCustomersProcessorValidation:
    def test_valid_email(self):
        customers = pd.DataFrame({"email": ["test@example.com"]})
        processor = CustomersProcessor(customers)
        processor.validate()

    def test_invalid_email(self):
        customers = pd.DataFrame({"email": ["invalid_email!"]})
        processor = CustomersProcessor(customers)
        with pytest.raises(InvalidEmailError) as exc_info:
            processor.validate()
            assert "invalid_email!" in str(exc_info.value)


class TestCustomersProcessor:
    def test_add_customer_days_column(self):
        customers = pd.DataFrame({"registration_date": ["2025-01-01"]})
        processor = CustomersProcessor(customers)
        processor._add_customer_days_column(today=pd.Timestamp("2025-01-10"))
        expected = pd.DataFrame(
            {"registration_date": pd.to_datetime(["2025-01-01"]), "customer_days": [9]}
        )
        pd.testing.assert_frame_equal(processor.data, expected)
