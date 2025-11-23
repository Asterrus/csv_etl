import pytest

from exceptions import CSVReadError
from utils import read_csv


class TestCSVRead:
    def test_read_customers_csv(self, customers_csv_file_path: str):
        read_csv(customers_csv_file_path)

    def test_read_sales_csv(self, sales_csv_file_path: str):
        read_csv(sales_csv_file_path)

    def test_file_not_found(self):
        with pytest.raises(CSVReadError):
            read_csv("data/non_existent.csv")

    def test_bad_csv(self, tmp_path):
        bad_csv_file_path = tmp_path / "bad_csv.csv"
        with open(bad_csv_file_path, "w") as f:
            f.write("")

        with pytest.raises(CSVReadError):
            read_csv(bad_csv_file_path)
