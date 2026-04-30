import hashlib
import logging
import os
import re
import time

from dashscope import Generation
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)


def _retry_with_backoff(max_retries=3, base_delay=1.0):
    """Decorator: retry on transient failure with exponential backoff.

    Does NOT retry ValueError (semantic errors like CANNOT_CONVERT).
    """
    def decorator(func):
        def wrapper(*args, **kwargs):
            last_exc = None
            for attempt in range(max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except ValueError:
                    raise
                except Exception as e:
                    last_exc = e
                    if attempt < max_retries:
                        delay = base_delay * (2 ** attempt)
                        logger.warning(
                            "LLM call failed (attempt %d/%d), retrying in %.1fs: %s",
                            attempt + 1, max_retries + 1, delay, e,
                        )
                        time.sleep(delay)
            raise last_exc
        return wrapper
    return decorator

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
        self._sql_cache: dict[str, str] = {}
        self._sql_cache_hits = 0

    def _sql_cache_key(self, question: str, schema_context: str) -> str:
        return hashlib.sha256(
            f"{question}||{hashlib.sha256(schema_context.encode()).hexdigest()}".encode()
        ).hexdigest()

    @_retry_with_backoff(max_retries=3, base_delay=1.0)
    def generate_sql(self, question: str, schema_context: str) -> str:
        """Convert natural language question to SQL."""
        cache_key = self._sql_cache_key(question, schema_context)
        if cache_key in self._sql_cache:
            self._sql_cache_hits += 1
            logger.info("LLM SQL cache hit (total: %d)", self._sql_cache_hits)
            return self._sql_cache[cache_key]

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

            # Store in local cache
            self._sql_cache[cache_key] = sql
            if len(self._sql_cache) > 500:
                self._sql_cache.pop(next(iter(self._sql_cache)))

            return sql

        except ValueError:
            raise
        except Exception as e:
            logger.exception("LLM generation failed")
            raise RuntimeError(f"SQL generation failed: {e}") from e

    @_retry_with_backoff(max_retries=3, base_delay=1.0)
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
