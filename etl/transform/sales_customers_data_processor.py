import pandas as pd

from etl.transform.customers_processor import CustomersProcessor
from etl.transform.sales_processor import SalesProcessor


class SalesCustomersDataProcessor:
    def __init__(
        self, sales_processor: SalesProcessor, customers_processor: CustomersProcessor
    ):
        self.sales_processor = sales_processor
        self.customers_processor = customers_processor

    def validate(self):
        self.sales_processor.validate()
        self.customers_processor.validate()

    def _create_sales_summary_df(self) -> pd.DataFrame:
        """сводная таблица продаж по категориям товаров"""
        summary = self.sales_processor.data.groupby(
            "category",
            as_index=False,
        ).agg(
            total_sales=("order_id", "count"),
            total_quantity=("quantity", "sum"),
            average_order_value=("total_price", "mean"),
            period_date=(
                "month",
                "max",  # Тут не уверен
            ),
        )

        return summary  # type: ignore[reportReturnType]

    def _create_product_ranking_df(self) -> pd.DataFrame:
        """5 самых популярных товаров"""
        product_ranking = (
            self.sales_processor.data.groupby(
                by="product_id",
                as_index=False,
            )
            .agg(
                product_name=("product_name", "first"),
                total_sold=("quantity", "sum"),
                total_revenue=("total_price", "sum"),
            )
            .sort_values("total_sold", ascending=False)
        ).reset_index(drop=True)
        product_ranking["rank_position"] = (
            product_ranking["total_sold"]
            .rank(method="first", ascending=False)
            .astype(int)
        )
        product_ranking = product_ranking.head(5)
        return product_ranking

    def _create_average_bill_by_region_df(self):
        result = (
            pd.merge(
                left=self.sales_processor.data,
                right=self.customers_processor.data,
                on="customer_id",
                how="left",
            )
            .groupby(
                by="region",
                as_index=False,
            )
            .agg(average_bill=("total_price", "mean"))
        )
        return result

    def process(self):
        self.sales_processor.process()
        self.customers_processor.process()

        sales_summary_df = self._create_sales_summary_df()
        product_ranking_df = self._create_product_ranking_df()
        average_bill_by_region_df = self._create_average_bill_by_region_df()
