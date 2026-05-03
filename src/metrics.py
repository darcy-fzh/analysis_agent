"""Pre-built ratio metrics with safe SQL templates using CAST + NULLIF."""
from __future__ import annotations


METRICS = [
    {
        "name": "GMV per Customer",
        "description": "Total GMV divided by count of distinct customers",
        "sql": """
SELECT
    CAST(SUM(gmv) AS DECIMAL(15,2)) AS total_gmv,
    COUNT(DISTINCT customer_id) AS unique_customers,
    CAST(SUM(gmv) AS DECIMAL(15,4)) / NULLIF(COUNT(DISTINCT customer_id), 0) AS gmv_per_customer
FROM orders
""",
        "chart": "bar",
    },
    {
        "name": "Avg Order Value",
        "description": "Total order amount divided by number of orders",
        "sql": """
SELECT
    CAST(SUM(order_amount) AS DECIMAL(15,2)) AS total_amount,
    COUNT(*) AS order_count,
    CAST(SUM(order_amount) AS DECIMAL(15,4)) / NULLIF(COUNT(*), 0) AS avg_order_value
FROM orders
""",
        "chart": "bar",
    },
    {
        "name": "GMV by Channel",
        "description": "Total GMV grouped by sales channel",
        "sql": """
SELECT
    channel,
    CAST(SUM(gmv) AS DECIMAL(15,2)) AS total_gmv,
    COUNT(*) AS order_count
FROM orders
GROUP BY channel
ORDER BY total_gmv DESC
""",
        "chart": "bar",
    },
    {
        "name": "GMV by Region",
        "description": "Total GMV grouped by region",
        "sql": """
SELECT
    region,
    CAST(SUM(gmv) AS DECIMAL(15,2)) AS total_gmv,
    COUNT(*) AS order_count
FROM orders
GROUP BY region
ORDER BY total_gmv DESC
""",
        "chart": "bar",
    },
    {
        "name": "GMV by Month",
        "description": "Monthly GMV trend",
        "sql": """
SELECT
    SUBSTR(order_date, 1, 6) AS year_month,
    CAST(SUM(gmv) AS DECIMAL(15,2)) AS total_gmv,
    COUNT(*) AS order_count
FROM orders
GROUP BY SUBSTR(order_date, 1, 6)
ORDER BY year_month
""",
        "chart": "line",
    },
    {
        "name": "Repeat Purchase Rate",
        "description": "Customers with more than 1 order divided by total customers",
        "sql": """
SELECT
    COUNT(DISTINCT customer_id) AS total_customers,
    SUM(CASE WHEN order_count > 1 THEN 1 ELSE 0 END) AS repeat_customers,
    CAST(
        SUM(CASE WHEN order_count > 1 THEN 1 ELSE 0 END) AS DECIMAL(15,4)
    ) / NULLIF(COUNT(DISTINCT customer_id), 0) AS repeat_purchase_rate
FROM (
    SELECT customer_id, COUNT(*) AS order_count
    FROM orders
    GROUP BY customer_id
) t
""",
        "chart": "bar",
    },
    {
        "name": "Top 10 Customers by GMV",
        "description": "Top 10 customers ranked by total GMV",
        "sql": """
SELECT
    c.name,
    CAST(SUM(o.gmv) AS DECIMAL(15,2)) AS total_gmv,
    COUNT(*) AS order_count
FROM orders o
JOIN customers c ON o.customer_id = c.id
GROUP BY c.id, c.name
ORDER BY total_gmv DESC
LIMIT 10
""",
        "chart": "bar",
    },
]


def get_metric_list() -> list[dict]:
    """Return all available metrics (name, description, chart type)."""
    return [{"name": m["name"], "description": m["description"], "chart": m["chart"]} for m in METRICS]


def build_sql(name: str) -> str | None:
    """Return the pre-built SQL for a metric, or None if not found."""
    for m in METRICS:
        if m["name"] == name:
            return m["sql"].strip()
    return None


def get_chart_type(name: str) -> str:
    """Return the recommended chart type for a metric."""
    for m in METRICS:
        if m["name"] == name:
            return m["chart"]
    return "bar"
