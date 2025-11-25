import pandas as pd

from exceptions import InvalidEmailError


class CustomersProcessor:
    def __init__(self, customers: pd.DataFrame):
        self.data = customers.copy()

    def validate(self):
        email_pattern = r"^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$"
        valid_mask = self.data["email"].str.match(email_pattern)

        if not valid_mask.all():
            invalid_emails = self.data.loc[~valid_mask, "email"].tolist()
            raise InvalidEmailError(f"Найдены невалидные email: {invalid_emails}")

    def _add_customer_days_column(self, today: pd.Timestamp):
        self.data["registration_date"] = pd.to_datetime(self.data["registration_date"])
        self.data["customer_days"] = (today - self.data["registration_date"]).dt.days

    def process(self):
        self._add_customer_days_column(today=pd.Timestamp.today())
