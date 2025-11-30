import pytest
from dotenv import load_dotenv

load_dotenv()


@pytest.fixture
def customers_csv_file_path() -> str:
    return "data/customers.csv"


@pytest.fixture
def sales_csv_file_path() -> str:
    return "data/sales.csv"
