import pandas as pd

from exceptions import InvalidEmailError


class CustomersProcessor:
    def __init__(self, customers: pd.DataFrame):
        self.customers = customers.copy()

    def validate(self):
        email_pattern = r"^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$"
        valid_mask = self.customers["email"].str.match(email_pattern)

        if not valid_mask.all():
            invalid_emails = self.customers.loc[~valid_mask, "email"].tolist()
            raise InvalidEmailError(f"Найдены невалидные email: {invalid_emails}")

    def _add_customer_days_column(self, today: pd.Timestamp):
        self.customers["registration_date"] = pd.to_datetime(
            self.customers["registration_date"]
        )
        self.customers["customer_days"] = (
            today - self.customers["registration_date"]
        ).dt.days

    def process(self):
        self._add_customer_days_column(today=pd.Timestamp.today())
