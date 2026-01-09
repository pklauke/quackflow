from quackflow.sql import extract_tables


class TestExtractTables:
    def test_simple_select(self):
        sql = "SELECT * FROM orders"
        assert extract_tables(sql) == {"orders"}

    def test_join(self):
        sql = "SELECT * FROM orders JOIN users ON orders.user_id = users.id"
        assert extract_tables(sql) == {"orders", "users"}

    def test_subquery(self):
        sql = "SELECT * FROM (SELECT * FROM orders) AS sub"
        assert extract_tables(sql) == {"orders"}

    def test_cte(self):
        sql = """
            WITH filtered AS (SELECT * FROM orders WHERE amount > 100)
            SELECT * FROM filtered
        """
        assert extract_tables(sql) == {"orders", "filtered"}

    def test_window_function(self):
        sql = "SELECT * FROM HOP('events', 'ts', INTERVAL '1 minute')"
        assert extract_tables(sql) == {"events"}

    def test_window_with_aggregation(self):
        sql = """
            SELECT window_start, COUNT(*) as cnt
            FROM HOP('orders', 'event_time', INTERVAL '10 minutes')
            GROUP BY window_start
        """
        assert extract_tables(sql) == {"orders"}

    def test_multiple_tables_and_window(self):
        sql = """
            SELECT o.*, p.name
            FROM HOP('orders', 'ts', INTERVAL '1 minute') o
            JOIN products p ON o.product_id = p.id
        """
        assert extract_tables(sql) == {"orders", "products"}
