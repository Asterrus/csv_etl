import pandas as pd


class SalesProcessor:
    def __init__(self, sales: pd.DataFrame):
        self.data = sales.copy()

    def _remove_duplicates(self):
        self.data.drop_duplicates(inplace=True)

    def _validate(self):
        pass

    def process(self):
        self._remove_duplicates()
