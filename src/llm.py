import logging
import os
import re

from dashscope import Generation
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

SYSTEM_PROMPT = """You are a professional MySQL SQL query expert. Generate correct MySQL SELECT queries based on the provided database schema and natural language questions.

## Rules
1. Only generate SELECT statements. Never generate INSERT/UPDATE/DELETE/DROP/ALTER/TRUNCATE/CREATE.
2. Return ONLY the raw SQL statement — no explanations, no comments, no markdown code fences.
3. Use exact table and column names as defined in the schema.
4. For aggregation queries (SUM, AVG, COUNT), always use meaningful aliases (AS).
5. For ratio/rate calculations, use CAST(... AS DECIMAL(15,4)) for precision, e.g.: CAST(SUM(gmv) AS DECIMAL(15,4)) / NULLIF(COUNT(DISTINCT customer_id), 0)
6. Use NULLIF to handle divide-by-zero cases.
7. Default LIMIT is 1000 rows. If the user asks for "all" data, set LIMIT 100.
8. order_date and registration_date are CHAR(8) strings in yyyymmdd format. Use string comparison for dates, e.g.: WHERE order_date >= '20240101'
9. If the question cannot be converted to SQL, return: -- CANNOT_CONVERT"""


class LLMService:
    def __init__(self, model: str | None = None):
        self.model = model or os.environ.get("DASHSCOPE_MODEL", "deepseek-v4-pro")

    def generate_sql(self, question: str, schema_context: str) -> str:
        """Convert natural language question to SQL."""
        user_message = f"""## Database Schema

{schema_context}

## User Question

{question}

Generate SQL query:"""

        try:
            response = Generation.call(
                model=self.model,
                messages=[
                    {"role": "system", "content": SYSTEM_PROMPT},
                    {"role": "user", "content": user_message},
                ],
                api_key=os.environ.get("DASHSCOPE_API_KEY"),
                base_url=os.environ.get("DASHSCOPE_BASE_URL"),
            )

            if response.status_code != 200:
                logger.error(
                    "Dashscope API error: code=%s message=%s",
                    response.status_code,
                    response.message,
                )
                raise RuntimeError(f"API call failed: {response.message}")

            raw = response.output.choices[0].message.content.strip()
            logger.info("LLM raw response: %s", raw)

            sql = self._extract_sql(raw)

            if sql == "-- CANNOT_CONVERT":
                raise ValueError("Unable to convert this question to SQL. Please try a more specific query.")

            return sql

        except ValueError:
            raise
        except Exception as e:
            logger.exception("LLM generation failed")
            raise RuntimeError(f"SQL generation failed: {e}") from e

    def generate_insight(self, question: str, sql: str, df_markdown: str) -> str:
        """Generate a plain-English summary of query results."""
        user_message = f"""## User Question
{question}

## SQL Executed
```sql
{sql}
```

## Query Results (top rows)
{df_markdown}

Write a concise, plain-English summary of what the data shows. Highlight the key numbers, trends, or patterns. Keep it under 4 sentences. Do NOT repeat the SQL. Just explain the findings."""

        try:
            response = Generation.call(
                model=self.model,
                messages=[
                    {"role": "system", "content": "You are a data analyst. Explain query results in clear, concise plain English. Focus on the key numbers and what they mean."},
                    {"role": "user", "content": user_message},
                ],
                api_key=os.environ.get("DASHSCOPE_API_KEY"),
                base_url=os.environ.get("DASHSCOPE_BASE_URL"),
            )

            if response.status_code != 200:
                logger.error(
                    "Insight API error: code=%s message=%s",
                    response.status_code,
                    response.message,
                )
                return ""

            return response.output.choices[0].message.content.strip()

        except Exception as e:
            logger.exception("Insight generation failed")
            return ""

    @staticmethod
    def _extract_sql(raw: str) -> str:
        """Extract pure SQL from LLM response, handling markdown code blocks."""
        match = re.search(r"```(?:sql)?\s*\n?(.*?)\n?```", raw, re.DOTALL | re.IGNORECASE)
        if match:
            return match.group(1).strip()
        return raw.strip()
