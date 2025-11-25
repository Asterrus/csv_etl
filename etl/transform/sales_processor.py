import pandas as pd


class SalesProcessor:
    def __init__(self, sales: pd.DataFrame):
        self.sales = sales.copy()

    def validate(self):
        pass
