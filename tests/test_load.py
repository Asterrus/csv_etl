from etl.load import create_tables, get_connection


class TestLoad:
    def test_get_connection(self):
        conn = get_connection()
        assert conn

    def test_create_tables(self):
        conn = get_connection()
        create_tables(conn)
        cur = conn.cursor()
        cur.execute("""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'public';
        """)
        tables = {row[0] for row in cur.fetchall()}
        expected_tables = {"sales", "customers", "sales_summary", "product_ranking"}
        missing = expected_tables - tables
        assert not missing, f"В базе не найдены таблицы: {missing}"
