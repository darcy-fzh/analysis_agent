import os
import logging

import pandas as pd
import pymysql
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS orders (
    id INT PRIMARY KEY AUTO_INCREMENT,
    order_date DATE NOT NULL,
    customer_id INT NOT NULL,
    channel VARCHAR(20) NOT NULL,
    region VARCHAR(50),
    gmv DECIMAL(10, 2) NOT NULL,
    order_amount DECIMAL(10, 2) NOT NULL,
    product_category VARCHAR(50),
    payment_method VARCHAR(20),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS customers (
    id INT PRIMARY KEY AUTO_INCREMENT,
    name VARCHAR(100),
    email VARCHAR(100),
    registration_date DATE,
    customer_segment VARCHAR(20),
    region VARCHAR(50)
);

CREATE TABLE IF NOT EXISTS products (
    id INT PRIMARY KEY AUTO_INCREMENT,
    name VARCHAR(100),
    category VARCHAR(50),
    price DECIMAL(10, 2),
    cost DECIMAL(10, 2)
);
"""

SEED_ORDERS = """
INSERT IGNORE INTO orders (id, order_date, customer_id, channel, region, gmv, order_amount, product_category, payment_method) VALUES
(1, '2024-01-15', 1, 'web', '华东', 100.00, 100.00, 'electronics', 'credit_card'),
(2, '2024-01-15', 2, 'app', '华北', 150.00, 150.00, 'clothing', 'alipay'),
(3, '2024-01-16', 1, 'store', '华东', 200.00, 200.00, 'electronics', 'wechat_pay'),
(4, '2024-01-16', 3, 'web', '华南', 75.00, 75.00, 'books', 'credit_card'),
(5, '2024-01-17', 2, 'app', '华北', 120.00, 120.00, 'clothing', 'alipay'),
(6, '2024-01-18', 4, 'web', '华东', 300.00, 300.00, 'electronics', 'credit_card'),
(7, '2024-01-19', 1, 'store', '华东', 80.00, 80.00, 'accessories', 'cash'),
(8, '2024-01-20', 5, 'app', '华南', 220.00, 220.00, 'electronics', 'alipay');
"""

SEED_CUSTOMERS = """
INSERT IGNORE INTO customers (id, name, email, registration_date, customer_segment, region) VALUES
(1, '张三', 'zhangsan@example.com', '2023-12-01', 'VIP', '华东'),
(2, '李四', 'lisi@example.com', '2024-01-01', 'Regular', '华北'),
(3, '王五', 'wangwu@example.com', '2024-01-10', 'New', '华南'),
(4, '赵六', 'zhaoliu@example.com', '2023-11-15', 'VIP', '华东'),
(5, '孙七', 'sunqi@example.com', '2024-01-05', 'Regular', '华南');
"""

SEED_PRODUCTS = """
INSERT IGNORE INTO products (id, name, category, price, cost) VALUES
(1, 'iPhone 15', 'electronics', 6999.00, 5000.00),
(2, '羽绒服', 'clothing', 899.00, 500.00),
(3, 'Python编程', 'books', 99.00, 30.00),
(4, '蓝牙耳机', 'electronics', 299.00, 150.00),
(5, '运动鞋', 'clothing', 599.00, 300.00);
"""

DANGEROUS_KEYWORDS = [
    "INSERT", "UPDATE", "DELETE", "DROP", "ALTER", "TRUNCATE",
    "CREATE", "GRANT", "REVOKE", "EXEC", "EXECUTE", "LOAD",
]


class DatabaseManager:
    def __init__(self):
        self._conn = None

    def _get_conn(self) -> pymysql.Connection:
        if self._conn is None or not self._conn.open:
            self._conn = pymysql.connect(
                host=os.environ["DB_HOST"],
                port=int(os.environ.get("DB_PORT", 3306)),
                user=os.environ["DB_USER"],
                password=os.environ["DB_PASSWORD"],
                database=os.environ["DB_NAME"],
                charset="utf8mb4",
                cursorclass=pymysql.cursors.DictCursor,
            )
        return self._conn

    def test_connection(self) -> bool:
        try:
            conn = self._get_conn()
            conn.ping()
            return True
        except Exception as e:
            logger.error("DB connection failed: %s", e)
            return False

    def init_schema(self) -> None:
        """Create tables and seed sample data if tables are empty."""
        conn = self._get_conn()
        with conn.cursor() as cur:
            for stmt in SCHEMA_SQL.strip().split(";"):
                stmt = stmt.strip()
                if stmt:
                    cur.execute(stmt)

            cur.execute("SELECT COUNT(*) AS cnt FROM orders")
            if cur.fetchone()["cnt"] == 0:
                cur.execute(SEED_ORDERS)

            cur.execute("SELECT COUNT(*) AS cnt FROM customers")
            if cur.fetchone()["cnt"] == 0:
                cur.execute(SEED_CUSTOMERS)

            cur.execute("SELECT COUNT(*) AS cnt FROM products")
            if cur.fetchone()["cnt"] == 0:
                cur.execute(SEED_PRODUCTS)

            conn.commit()
        logger.info("Schema initialized and seed data inserted.")

    def execute_query(self, sql: str) -> pd.DataFrame:
        conn = self._get_conn()
        with conn.cursor() as cur:
            cur.execute(sql)
            rows = cur.fetchall()
        return pd.DataFrame(rows) if rows else pd.DataFrame()

    def get_schema_context(self) -> str:
        """Return schema DDL and sample rows for LLM prompt context."""
        conn = self._get_conn()
        parts = [SCHEMA_SQL.strip(), "", "-- Sample data:"]

        for table in ("orders", "customers", "products"):
            with conn.cursor() as cur:
                cur.execute(f"SELECT * FROM {table} LIMIT 3")
                rows = cur.fetchall()
            if rows:
                df = pd.DataFrame(rows)
                parts.append(f"-- {table}:")
                parts.append(df.to_markdown(index=False))
                parts.append("")

        return "\n".join(parts)

    def get_table_stats(self) -> dict[str, int]:
        """Return row counts for all tables."""
        conn = self._get_conn()
        stats = {}
        with conn.cursor() as cur:
            for table in ("orders", "customers", "products"):
                cur.execute(f"SELECT COUNT(*) AS cnt FROM {table}")
                stats[table] = cur.fetchone()["cnt"]
        return stats

    @staticmethod
    def validate_sql(sql: str) -> tuple[bool, str]:
        """Check that SQL is a safe SELECT statement."""
        upper = sql.strip().upper()
        if not upper.startswith("SELECT"):
            return False, "只允许 SELECT 查询"
        for kw in DANGEROUS_KEYWORDS:
            # Use word-boundary check
            import re
            if re.search(rf"\b{kw}\b", upper):
                return False, f"SQL 包含禁止的关键字: {kw}"
        return True, ""
