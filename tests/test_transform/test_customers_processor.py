import pandas as pd
import pytest

from etl.transform.customers_processor import CustomersProcessor
from exceptions import InvalidEmailError


@pytest.fixture
def today() -> pd.Timestamp:
    return pd.Timestamp("2025-01-10")


class TestCustomersProcessorValidation:
    def test_valid_email(self):
        customers = pd.DataFrame({"email": ["test@example.com"]})
        processor = CustomersProcessor(customers)
        processor._validate_email()

    def test_invalid_email(self):
        customers = pd.DataFrame({"email": ["invalid_email!"]})
        processor = CustomersProcessor(customers)
        with pytest.raises(InvalidEmailError) as exc_info:
            processor._validate_email()
            assert "invalid_email!" in str(exc_info.value)

    def test_registration_date_validation(self):
        customers = pd.DataFrame({"registration_date": ["invalid_date", "2025-01-01"]})
        processor = CustomersProcessor(customers)
        processor._validate_registration_date()
        expected = pd.DataFrame(
            {"registration_date": [pd.NaT, pd.Timestamp("2025-01-01")]}
        )
        pd.testing.assert_frame_equal(processor.data, expected)


class TestCustomersProcessor:
    def test_add_customer_days_column(self, today: pd.Timestamp):
        valid_date = pd.Timestamp("2025-01-01")
        invalid_date = pd.NaT
        customers = pd.DataFrame({"registration_date": [valid_date, invalid_date]})
        processor = CustomersProcessor(customers)
        processor._add_customer_days_column(today=today)
        expected = pd.DataFrame(
            {
                "registration_date": [valid_date, invalid_date],
                "customer_days": [
                    9.0,
                    float("nan"),
                ],
            }
        )
        pd.testing.assert_frame_equal(processor.data, expected)
