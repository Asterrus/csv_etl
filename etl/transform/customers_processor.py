import pandas as pd

from exceptions import InvalidEmailError


def validate_email(df: pd.DataFrame):
    email_pattern = r"^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$"
    valid_mask = df["email"].str.match(email_pattern)

    if not valid_mask.all():
        invalid_emails = df.loc[~valid_mask, "email"].tolist()
        raise InvalidEmailError(f"Найдены невалидные email: {invalid_emails}")


class CustomersProcessor:
    def __init__(self, customers: pd.DataFrame):
        self.data = customers.copy()

    def _validate_email(self):
        validate_email(self.data)

    def _validate_registration_date(self):
        self.data["registration_date"] = pd.to_datetime(
            self.data["registration_date"], errors="coerce", format="%Y-%m-%d"
        )

    def validate(self):
        self._validate_email()
        self._validate_registration_date()

    def _add_customer_days_column(self, today: pd.Timestamp):
        self.data["customer_days"] = (today - self.data["registration_date"]).dt.days

    def process(self):
        self._add_customer_days_column(today=pd.Timestamp.today())
