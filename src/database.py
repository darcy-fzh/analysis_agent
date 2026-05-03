from __future__ import annotations

import logging
import os
import random
import re
from datetime import date, timedelta
from decimal import Decimal

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.engine import Engine

from src.config import ConnectionConfig

load_dotenv()

logger = logging.getLogger(__name__)

SCHEMA_SQL_MYSQL = """
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

CREATE TABLE IF NOT EXISTS query_history (
    id INT PRIMARY KEY AUTO_INCREMENT,
    question TEXT NOT NULL,
    sql_text TEXT,
    result_rows INT,
    error TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
"""

SCHEMA_SQL_POSTGRESQL = """
CREATE TABLE IF NOT EXISTS orders (
    id SERIAL PRIMARY KEY,
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
    id SERIAL PRIMARY KEY,
    name VARCHAR(100),
    email VARCHAR(100),
    registration_date CHAR(8),
    customer_segment VARCHAR(20),
    region VARCHAR(50)
);

CREATE TABLE IF NOT EXISTS products (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100),
    category VARCHAR(50),
    price DECIMAL(10, 2),
    cost DECIMAL(10, 2)
);

CREATE TABLE IF NOT EXISTS query_history (
    id SERIAL PRIMARY KEY,
    question TEXT NOT NULL,
    sql_text TEXT,
    result_rows INT,
    error TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
"""

SCHEMA_SQL_SQLITE = """
CREATE TABLE IF NOT EXISTS orders (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    order_date TEXT NOT NULL,
    customer_id INTEGER NOT NULL,
    channel TEXT NOT NULL,
    region TEXT,
    gmv REAL NOT NULL,
    order_amount REAL NOT NULL,
    product_category TEXT,
    payment_method TEXT,
    created_at TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS customers (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT,
    email TEXT,
    registration_date TEXT,
    customer_segment TEXT,
    region TEXT
);

CREATE TABLE IF NOT EXISTS products (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT,
    category TEXT,
    price REAL,
    cost REAL
);

CREATE TABLE IF NOT EXISTS query_history (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    question TEXT NOT NULL,
    sql_text TEXT,
    result_rows INTEGER,
    error TEXT,
    created_at TEXT DEFAULT (datetime('now'))
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


def _generate_customers(count: int = 100) -> list[dict]:
    rows = []
    for i in range(1, count + 1):
        surname = random.choice(SURNAMES)
        given = random.choice(GIVENS) + (random.choice(GIVENS) if random.random() < 0.3 else "")
        name = surname + given
        email = f"user{i:04d}@example.com"
        reg_date = _ymd(date(2022, 1, 1) + timedelta(days=random.randint(0, 1095)))
        segment = random.choices(SEGMENTS, weights=[15, 55, 30])[0]
        region = random.choice(REGIONS)
        rows.append({
            "id": i, "name": name, "email": email,
            "registration_date": reg_date, "customer_segment": segment,
            "region": region,
        })
    return rows


def _generate_orders(count: int = 3000, customer_count: int = 100) -> list[dict]:
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
        rows.append({
            "id": i, "order_date": _ymd(order_date),
            "customer_id": customer_id, "channel": channel,
            "region": region, "gmv": gmv, "order_amount": gmv,
            "product_category": category, "payment_method": random.choice(PAYMENTS),
        })
    return rows


def _generate_products() -> list[dict]:
    return [{"id": i, "name": p[0], "category": p[1],
             "price": p[2], "cost": p[3]}
            for i, p in enumerate(PRODUCT_POOL, start=1)]


class DatabaseManager:
    """Database manager using SQLAlchemy for multi-dialect support."""

    def __init__(self, config: ConnectionConfig | None = None):
        self._config = config or ConnectionConfig.from_env()
        self._engine: Engine | None = None

    @property
    def engine(self) -> Engine:
        if self._engine is None:
            connect_args: dict = {}
            if self._config.db_type == "sqlite":
                connect_args["check_same_thread"] = False
            self._engine = create_engine(
                self._config.sqlalchemy_url,
                pool_size=5,
                max_overflow=10,
                pool_recycle=3600,
                connect_args=connect_args,
            )
        return self._engine

    @property
    def db_type(self) -> str:
        return self._config.db_type

    @property
    def config(self) -> ConnectionConfig:
        return self._config

    def test_connection(self) -> bool:
        try:
            with self.engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            return True
        except Exception as e:
            logger.error("DB connection failed: %s", e)
            return False

    def _schema_ddl(self) -> str:
        """Return the DDL appropriate for the current dialect."""
        schema_map = {
            "mysql": SCHEMA_SQL_MYSQL,
            "postgresql": SCHEMA_SQL_POSTGRESQL,
            "sqlite": SCHEMA_SQL_SQLITE,
        }
        return schema_map.get(self.db_type, SCHEMA_SQL_SQLITE)

    def init_schema(self) -> None:
        """Create tables and seed sample data if tables are empty."""
        ddl = self._schema_ddl()

        with self.engine.begin() as conn:
            for stmt in ddl.strip().split(";"):
                stmt = stmt.strip()
                if stmt:
                    conn.execute(text(stmt))

        with self.engine.connect() as conn:
            result = conn.execute(text("SELECT COUNT(*) AS cnt FROM products"))
            row = result.fetchone()
            if row is not None and row[0] == 0:
                products = _generate_products()
                pd.DataFrame(products).to_sql(
                    "products", self.engine, if_exists="append", index=False,
                )
                logger.info("Seeded %d products.", len(products))

            result = conn.execute(text("SELECT COUNT(*) AS cnt FROM customers"))
            row = result.fetchone()
            if row is not None and row[0] == 0:
                customers = _generate_customers(100)
                pd.DataFrame(customers).to_sql(
                    "customers", self.engine, if_exists="append", index=False,
                )
                logger.info("Seeded %d customers.", len(customers))

            result = conn.execute(text("SELECT COUNT(*) AS cnt FROM orders"))
            row = result.fetchone()
            if row is not None and row[0] == 0:
                orders = _generate_orders(3000, 100)
                pd.DataFrame(orders).to_sql(
                    "orders", self.engine, if_exists="append", index=False,
                )
                logger.info("Seeded %d orders.", len(orders))

        logger.info("Schema initialized and seed data inserted.")

    def execute_query(self, sql: str) -> pd.DataFrame:
        try:
            df = pd.read_sql_query(sql, self.engine)
        except Exception:
            # Fallback: raw connection for complex queries
            with self.engine.connect() as conn:
                result = conn.execute(text(sql))
                rows = result.fetchall()
                if not rows:
                    return pd.DataFrame()
                df = pd.DataFrame(rows, columns=list(result.keys()))
        # Convert Decimal columns to float (relevant for MySQL/PostgreSQL)
        for col in df.columns:
            if df[col].dtype == object and len(df) > 0:
                sample = df[col].dropna()
                if not sample.empty and isinstance(sample.iloc[0], Decimal):
                    df[col] = pd.to_numeric(df[col], errors="ignore")
        return df

    def get_schema_context(self) -> str:
        """Return schema DDL and sample rows for LLM prompt context."""
        insp = inspect(self.engine)
        parts = []

        for table_name in insp.get_table_names():
            if table_name == "query_history":
                continue

            # Build CREATE TABLE representation from inspector
            columns = insp.get_columns(table_name)
            cols_repr = []
            for col in columns:
                nullable = "" if col.get("nullable", True) else " NOT NULL"
                default = f" DEFAULT {col['default']}" if col.get("default") else ""
                pk = " PRIMARY KEY" if col.get("primary_key") else ""
                cols_repr.append(
                    f"  {col['name']} {col['type']}{nullable}{default}{pk}"
                )

            parts.append(f"CREATE TABLE {table_name} (")
            parts.append(",\n".join(cols_repr))
            parts.append(");\n")

            # Sample rows
            with self.engine.connect() as conn:
                result = conn.execute(
                    text(f"SELECT * FROM {table_name} LIMIT 3"))
                rows = result.fetchall()
            if rows:
                df = pd.DataFrame(rows, columns=list(result.keys()))
                parts.append(f"-- {table_name} sample:")
                parts.append(df.to_markdown(index=False))
                parts.append("")

        return "\n".join(parts)

    def save_query(self, question: str, sql: str | None = None,
                   row_count: int | None = None, error: str | None = None) -> None:
        """Save a query to the history table."""
        with self.engine.begin() as conn:
            conn.execute(
                text(
                    "INSERT INTO query_history "
                    "(question, sql_text, result_rows, error) "
                    "VALUES (:q, :s, :r, :e)"
                ),
                {"q": question, "s": sql, "r": row_count, "e": error},
            )

    def get_recent_queries(self, limit: int = 10) -> list[dict]:
        """Return recent queries from history."""
        with self.engine.connect() as conn:
            result = conn.execute(
                text(
                    "SELECT id, question, sql_text, result_rows, error, created_at "
                    "FROM query_history ORDER BY id DESC LIMIT :lim"
                ),
                {"lim": limit},
            )
            return [dict(row._mapping) for row in result.fetchall()]

    def delete_query(self, query_id: int) -> None:
        """Delete a single query from history by id."""
        with self.engine.begin() as conn:
            conn.execute(
                text("DELETE FROM query_history WHERE id = :id"),
                {"id": query_id},
            )

    def clear_query_history(self) -> None:
        """Delete all query history records."""
        with self.engine.begin() as conn:
            conn.execute(text("DELETE FROM query_history"))

    def get_table_stats(self) -> dict[str, int]:
        """Return row counts for all tables."""
        stats = {}
        with self.engine.connect() as conn:
            for table in ("orders", "customers", "products", "query_history"):
                result = conn.execute(
                    text(f"SELECT COUNT(*) AS cnt FROM {table}"))
                row = result.fetchone()
                if row is not None:
                    stats[table] = row[0]
        return stats

    def get_tables(self) -> list[dict]:
        """List all base tables with metadata."""
        insp = inspect(self.engine)
        tables = []
        for name in insp.get_table_names():
            try:
                with self.engine.connect() as conn:
                    result = conn.execute(
                        text(f"SELECT COUNT(*) AS cnt FROM {name}"))
                    row = result.fetchone()
                    row_count = row[0] if row is not None else 0
            except Exception:
                row_count = 0
            tables.append({
                "TABLE_NAME": name,
                "TABLE_ROWS": row_count,
                "TABLE_COMMENT": "",
                "CREATE_TIME": None,
                "UPDATE_TIME": None,
            })
        return tables

    def get_columns(self, table_name: str) -> list[dict]:
        """Return column metadata for a given table."""
        insp = inspect(self.engine)
        columns = []
        for col in insp.get_columns(table_name):
            columns.append({
                "COLUMN_NAME": col["name"],
                "DATA_TYPE": str(col["type"]),
                "IS_NULLABLE": "YES" if col.get("nullable", True) else "NO",
                "COLUMN_DEFAULT": str(col["default"]) if col.get("default") else None,
                "COLUMN_COMMENT": "",
                "COLUMN_KEY": "PRI" if col.get("primary_key") else "",
            })
        return columns

    def get_latest_partition(self, table_name: str) -> str | None:
        """Return the maximum value of a date-like column as partition marker."""
        insp = inspect(self.engine)
        columns = insp.get_columns(table_name)

        date_types = {"DATE", "DATETIME", "TIMESTAMP", "CHAR"}
        date_col = None
        for col in columns:
            col_type = str(col["type"]).upper()
            if any(dt in col_type for dt in date_types):
                if col["name"] in ("order_date", "registration_date", "created_at"):
                    date_col = col["name"]
                    break
                if date_col is None:
                    date_col = col["name"]

        if not date_col:
            return None

        try:
            with self.engine.connect() as conn:
                result = conn.execute(
                    text(f'SELECT MAX("{date_col}") AS latest FROM {table_name}')
                    if self.db_type == "postgresql"
                    else text(
                        f"SELECT MAX({date_col}) AS latest FROM {table_name}")
                )
                row = result.fetchone()
                if row is not None and row[0] is not None:
                    return str(row[0])
        except Exception:
            pass
        return None

    def get_database_name(self) -> str:
        """Return the current database name."""
        return self._config.database

    @staticmethod
    def validate_sql(sql: str) -> tuple[bool, str]:
        """Check that SQL is a safe SELECT statement."""
        upper = sql.strip().upper()
        if not upper.startswith("SELECT"):
            return False, "Only SELECT queries are allowed"
        for kw in DANGEROUS_KEYWORDS:
            if re.search(rf"\b{kw}\b", upper):
                return False, f"SQL contains forbidden keyword: {kw}"
        return True, ""
