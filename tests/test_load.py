from etl.load import create_tables, get_engine


class TestLoad:
    def test_get_engine(self):
        engine = get_engine()
        assert engine is not None

    def test_create_tables(self):
        engine = get_engine()
        with engine.begin() as connection:
            create_tables(connection)
            result = connection.exec_driver_sql(
                """
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = 'public';
                """
            ).fetchall()
        tables = {row[0] for row in result}
        expected_tables = {"sales", "customers", "sales_summary", "product_ranking"}
        missing = expected_tables - tables
        assert not missing, f"В базе не найдены таблицы: {missing}"
