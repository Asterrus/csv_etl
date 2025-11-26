import pandas as pd


class SalesProcessor:
    def __init__(self, sales: pd.DataFrame):
        self.data = sales.copy()

    def _validate_order_date(self):
        self.data["order_date"] = pd.to_datetime(
            self.data["order_date"], errors="coerce"
        )

    def _remove_duplicates(self):
        self.data.drop_duplicates(inplace=True)

    def _validate_quantity(self):
        self.data["quantity"] = pd.to_numeric(self.data["quantity"], errors="coerce")

    def _validate_unit_price(self):
        self.data["unit_price"] = pd.to_numeric(
            self.data["unit_price"], errors="coerce"
        )

    def validate(self):
        self._remove_duplicates()
        self._validate_order_date()
        self._validate_quantity()
        self._validate_unit_price()

    def _add_total_price(self):
        self.data["total_price"] = self.data["quantity"] * self.data["unit_price"]

    def _add_month_of_order(self):
        self.data["month"] = self.data["order_date"].dt.month

    def process(self):
        self._add_total_price()
        self._add_month_of_order()
