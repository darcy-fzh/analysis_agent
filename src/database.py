import logging
import os
import random
from datetime import date, timedelta

import pandas as pd
import pymysql
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS orders (
    id INT PRIMARY KEY AUTO_INCREMENT,
    order_date CHAR(8) NOT NULL,
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
    registration_date CHAR(8),
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

DANGEROUS_KEYWORDS = [
    "INSERT", "UPDATE", "DELETE", "DROP", "ALTER", "TRUNCATE",
    "CREATE", "GRANT", "REVOKE", "EXEC", "EXECUTE", "LOAD",
]

# ── data generators ──────────────────────────────────────────────

SURNAMES = ["张", "李", "王", "赵", "孙", "周", "吴", "郑", "冯", "陈",
            "褚", "卫", "蒋", "沈", "韩", "杨", "朱", "秦", "许", "何"]
GIVENS = ["伟", "芳", "娜", "敏", "静", "丽", "强", "磊", "洋", "勇",
          "艳", "杰", "军", "秀英", "明", "超", "平", "华", "刚", "飞"]

CHANNELS = ["web", "app", "store", "miniprogram"]
REGIONS = ["华东", "华北", "华南", "华中", "西南", "西北", "东北"]
CATEGORIES = ["electronics", "clothing", "books", "accessories", "home",
              "sports", "beauty", "food", "toys", "office"]
PAYMENTS = ["credit_card", "alipay", "wechat_pay", "cash", "unionpay"]
SEGMENTS = ["VIP", "Regular", "New"]

PRODUCT_POOL = [
    ("iPhone 15", "electronics", 6999.00, 5000.00),
    ("MacBook Pro", "electronics", 14999.00, 11000.00),
    ("蓝牙耳机", "electronics", 299.00, 150.00),
    ("平板电脑", "electronics", 3499.00, 2200.00),
    ("智能手表", "electronics", 2599.00, 1600.00),
    ("羽绒服", "clothing", 899.00, 500.00),
    ("运动鞋", "clothing", 599.00, 300.00),
    ("牛仔裤", "clothing", 399.00, 180.00),
    ("T恤", "clothing", 129.00, 60.00),
    ("连衣裙", "clothing", 459.00, 220.00),
    ("Python编程", "books", 99.00, 30.00),
    ("机器学习实战", "books", 89.00, 28.00),
    ("数据分析入门", "books", 69.00, 20.00),
    ("三体全集", "books", 128.00, 60.00),
    ("活着", "books", 45.00, 15.00),
    ("双肩包", "accessories", 199.00, 100.00),
    ("太阳镜", "accessories", 299.00, 120.00),
    ("腰带", "accessories", 159.00, 70.00),
    ("台灯", "home", 249.00, 130.00),
    ("落地扇", "home", 399.00, 220.00),
    ("瑜伽垫", "sports", 99.00, 40.00),
    ("哑铃组", "sports", 349.00, 180.00),
    ("面霜", "beauty", 259.00, 100.00),
    ("巧克力礼盒", "food", 168.00, 90.00),
    ("积木套装", "toys", 299.00, 150.00),
    ("办公椅", "office", 1299.00, 700.00),
]


def _ymd(d: date) -> str:
    return d.strftime("%Y%m%d")


def _generate_customers(count: int = 100) -> list[tuple]:
    rows = []
    for i in range(1, count + 1):
        surname = random.choice(SURNAMES)
        given = random.choice(GIVENS) + (random.choice(GIVENS) if random.random() < 0.3 else "")
        name = surname + given
        email = f"user{i:04d}@example.com"
        reg_date = _ymd(date(2022, 1, 1) + timedelta(days=random.randint(0, 1095)))
        segment = random.choices(SEGMENTS, weights=[15, 55, 30])[0]
        region = random.choice(REGIONS)
        rows.append((i, name, email, reg_date, segment, region))
    return rows


def _generate_orders(count: int = 3000, customer_count: int = 100) -> list[tuple]:
    rows = []
    start = date(2024, 1, 1)
    end = date(2026, 4, 28)
    days_range = (end - start).days
    product_count = len(PRODUCT_POOL)

    for i in range(1, count + 1):
        order_date = start + timedelta(days=random.randint(0, days_range))
        customer_id = random.randint(1, customer_count)
        channel = random.choices(CHANNELS, weights=[35, 30, 20, 15])[0]
        region = random.choice(REGIONS)
        product_idx = random.randint(0, product_count - 1)
        category = PRODUCT_POOL[product_idx][1]
        base_price = PRODUCT_POOL[product_idx][2]
        quantity = random.choices([1, 2, 3], weights=[70, 20, 10])[0]
        gmv = round(base_price * quantity * random.uniform(0.85, 1.0), 2)
        order_amount = gmv
        payment_method = random.choice(PAYMENTS)
        rows.append((
            i, _ymd(order_date), customer_id, channel, region,
            gmv, order_amount, category, payment_method,
        ))
    return rows


def _generate_products() -> list[tuple]:
    return [(i, *p) for i, p in enumerate(PRODUCT_POOL, start=1)]


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
            conn.commit()

        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) AS cnt FROM products")
            if cur.fetchone()["cnt"] == 0:
                products = _generate_products()
                cur.executemany(
                    "INSERT INTO products (id, name, category, price, cost) "
                    "VALUES (%s, %s, %s, %s, %s)",
                    products,
                )
                conn.commit()
                logger.info("Seeded %d products.", len(products))

            cur.execute("SELECT COUNT(*) AS cnt FROM customers")
            if cur.fetchone()["cnt"] == 0:
                customers = _generate_customers(100)
                cur.executemany(
                    "INSERT INTO customers (id, name, email, registration_date, "
                    "customer_segment, region) VALUES (%s, %s, %s, %s, %s, %s)",
                    customers,
                )
                conn.commit()
                logger.info("Seeded %d customers.", len(customers))

            cur.execute("SELECT COUNT(*) AS cnt FROM orders")
            if cur.fetchone()["cnt"] == 0:
                orders = _generate_orders(3000, 100)
                cur.executemany(
                    "INSERT INTO orders (id, order_date, customer_id, channel, "
                    "region, gmv, order_amount, product_category, payment_method) "
                    "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)",
                    orders,
                )
                conn.commit()
                logger.info("Seeded %d orders.", len(orders))

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
            import re
            if re.search(rf"\b{kw}\b", upper):
                return False, f"SQL 包含禁止的关键字: {kw}"
        return True, ""
