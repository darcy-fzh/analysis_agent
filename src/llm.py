import logging
import os
import re

from dashscope import Generation
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

SYSTEM_PROMPT = """你是一个专业的 MySQL SQL 查询专家。根据用户提供的数据库表结构和自然语言问题，生成正确的 MySQL SELECT 查询语句。

## 规则
1. 只生成 SELECT 语句，禁止任何 INSERT/UPDATE/DELETE/DROP/ALTER/TRUNCATE/CREATE 操作
2. 只返回纯 SQL 语句，不要加任何解释、注释或 markdown 代码块标记
3. 使用正确的表名和列名，严格匹配 schema 中定义的名称
4. 对于聚合查询（如 SUM, AVG, COUNT），始终使用有意义的别名（AS）
5. 对于比例/比率计算（如客单价），使用 CAST(... AS DECIMAL(15,4)) 确保精度，例如：CAST(SUM(gmv) AS DECIMAL(15,4)) / NULLIF(COUNT(DISTINCT customer_id), 0)
6. 对于包含中文字符的查询条件，使用正确的字符串比较
7. 如果问题无法转换为 SQL，返回：-- CANNOT_CONVERT
8. 使用 NULLIF 处理分母为零的情况
9. LIMIT 默认不超过 1000 行
10. 如果用户问"所有"/"全部"数据，LIMIT 设为 100"""


class LLMService:
    def __init__(self, model: str = "deepseek-v4-pro"):
        self.model = model

    def generate_sql(self, question: str, schema_context: str) -> str:
        """Convert natural language question to SQL using Alibaba Bailian."""
        user_message = f"""## 数据库 Schema

{schema_context}

## 用户问题

{question}

请生成 SQL 查询："""

        try:
            response = Generation.call(
                model=self.model,
                messages=[
                    {"role": "system", "content": SYSTEM_PROMPT},
                    {"role": "user", "content": user_message},
                ],
                api_key=os.environ.get("DASHSCOPE_API_KEY"),
            )

            if response.status_code != 200:
                logger.error(
                    "Dashscope API error: code=%s message=%s",
                    response.status_code,
                    response.message,
                )
                raise RuntimeError(f"API 调用失败: {response.message}")

            raw = response.output.choices[0].message.content.strip()
            logger.info("LLM raw response: %s", raw)

            sql = self._extract_sql(raw)

            if sql == "-- CANNOT_CONVERT":
                raise ValueError("无法将当前问题转换为 SQL 查询，请尝试更具体的表述。")

            return sql

        except ValueError:
            raise
        except Exception as e:
            logger.exception("LLM generation failed")
            raise RuntimeError(f"SQL 生成失败: {e}") from e

    @staticmethod
    def _extract_sql(raw: str) -> str:
        """Extract pure SQL from LLM response, handling markdown code blocks."""
        # Strip markdown code fences if present
        match = re.search(r"```(?:sql)?\s*\n?(.*?)\n?```", raw, re.DOTALL | re.IGNORECASE)
        if match:
            return match.group(1).strip()
        return raw.strip()
