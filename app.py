import logging
import sys

import streamlit as st

from src.cache import QueryCache
from src.database import DatabaseManager
from src.llm import LLMService

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)


@st.cache_resource
def get_db() -> DatabaseManager:
    return DatabaseManager()


@st.cache_resource
def get_llm() -> LLMService:
    return LLMService()


@st.cache_resource
def get_cache() -> QueryCache:
    return QueryCache(ttl=300)


def render_sidebar(db: DatabaseManager, cache: QueryCache) -> None:
    with st.sidebar:
        st.header("系统状态")

        if db.test_connection():
            st.success("数据库已连接")
        else:
            st.error("数据库连接失败")

        st.divider()

        st.subheader("数据概览")
        try:
            stats = db.get_table_stats()
            for table, count in stats.items():
                st.metric(label=table, value=count)
        except Exception:
            st.warning("无法加载表信息")

        st.divider()

        st.subheader("缓存")
        cache_stats = cache.stats()
        col1, col2 = st.columns(2)
        with col1:
            st.metric("命中", cache_stats["hits"])
        with col2:
            st.metric("未命中", cache_stats["misses"])
        st.caption(f"缓存条目: {cache_stats['size']} | TTL: {cache_stats['ttl']}s")

        if st.button("清除缓存"):
            cache.clear()
            st.rerun()

        st.divider()

        with st.expander("数据库 Schema"):
            try:
                schema = db.get_schema_context()
                st.code(schema, language="sql")
            except Exception:
                st.warning("Schema 不可用")


def render_main(db: DatabaseManager, llm: LLMService, cache: QueryCache) -> None:
    st.title("数据分析 AI Agent")
    st.caption("用自然语言提问，AI 自动生成 SQL 并查询数据库")

    if "query_history" not in st.session_state:
        st.session_state.query_history = []

    question = st.chat_input("输入你的数据分析问题...")

    if question:
        st.session_state.query_history.append(question)

    for idx, q in enumerate(reversed(st.session_state.query_history)):
        with st.chat_message("user"):
            st.write(q)

        with st.chat_message("assistant"):
            try:
                schema = db.get_schema_context()
                schema_version = str(hash(schema))

                cached_result = cache.get(q, schema_version)
                if cached_result is not None:
                    st.caption("从缓存返回")
                    sql, df = cached_result
                else:
                    with st.spinner("AI 正在生成 SQL..."):
                        sql = llm.generate_sql(q, schema)

                    valid, err_msg = DatabaseManager.validate_sql(sql)
                    if not valid:
                        st.error(err_msg)
                        continue

                    with st.spinner("正在查询数据库..."):
                        df = db.execute_query(sql)
                    cache.set(q, schema_version, (sql, df))

                st.code(sql, language="sql")

                if df.empty:
                    st.info("查询结果为空")
                else:
                    st.subheader(f"查询结果 ({len(df)} 行)")
                    st.dataframe(df, use_container_width=True)

                    numeric_cols = df.select_dtypes(include="number").columns.tolist()
                    if numeric_cols:
                        with st.expander("可视化"):
                            chart_col = st.selectbox(
                                "选择数值列",
                                numeric_cols,
                                key=f"chart_{idx}",
                            )
                            dim_col = st.selectbox(
                                "选择维度列 (可选)",
                                ["(无)"] + [c for c in df.columns if c != chart_col],
                                key=f"dim_{idx}",
                            )
                            if dim_col == "(无)":
                                st.bar_chart(df[chart_col])
                            else:
                                st.bar_chart(
                                    df.set_index(dim_col)[chart_col]
                                )

            except ValueError as e:
                st.warning(str(e))
            except Exception as e:
                st.error(f"查询失败: {e}")
                logger.exception("Query pipeline failed")


def main() -> None:
    st.set_page_config(
        page_title="数据分析 AI Agent",
        page_icon="",
        layout="wide",
    )

    db = get_db()
    llm = get_llm()
    cache = get_cache()

    try:
        db.init_schema()
    except Exception as e:
        st.warning("数据库初始化失败，请检查连接配置")
        st.code(str(e))

    render_sidebar(db, cache)
    render_main(db, llm, cache)


if __name__ == "__main__":
    main()
