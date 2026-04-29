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

CUSTOM_CSS = """
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600&display=swap');
html, body, [class*="css"] {
    font-family: 'Inter', -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
}
</style>
"""


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
        st.header("System Status")

        if db.test_connection():
            st.success("Database Connected")
        else:
            st.error("Database Disconnected")

        st.divider()

        st.subheader("Data Overview")
        try:
            stats = db.get_table_stats()
            for table, count in stats.items():
                st.metric(label=table, value=count)
        except Exception:
            st.warning("Unable to load table info")

        st.divider()

        st.subheader("Cache")
        cache_stats = cache.stats()
        col1, col2 = st.columns(2)
        with col1:
            st.metric("Hits", cache_stats["hits"])
        with col2:
            st.metric("Misses", cache_stats["misses"])
        st.caption(f"Entries: {cache_stats['size']} | TTL: {cache_stats['ttl']}s")

        if st.button("Clear Cache"):
            cache.clear()
            st.rerun()

        st.divider()

        with st.expander("Database Schema"):
            try:
                schema = db.get_schema_context()
                st.code(schema, language="sql")
            except Exception:
                st.warning("Schema unavailable")


def render_main(db: DatabaseManager, llm: LLMService, cache: QueryCache) -> None:
    st.title("Data Analysis AI Agent")
    st.caption("Ask questions in natural language — AI generates SQL and queries the database")

    if "query_history" not in st.session_state:
        st.session_state.query_history = []

    question = st.chat_input("Ask a data question...")

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
                    st.caption("Returned from cache")
                    sql, df = cached_result
                else:
                    with st.spinner("AI is generating SQL..."):
                        sql = llm.generate_sql(q, schema)

                    valid, err_msg = DatabaseManager.validate_sql(sql)
                    if not valid:
                        st.error(err_msg)
                        continue

                    with st.spinner("Querying database..."):
                        df = db.execute_query(sql)
                    cache.set(q, schema_version, (sql, df))

                st.code(sql, language="sql")

                if df.empty:
                    st.info("Query returned no results")
                else:
                    st.subheader(f"Results ({len(df)} rows)")
                    st.dataframe(df, use_container_width=True)

                    numeric_cols = df.select_dtypes(include="number").columns.tolist()
                    if numeric_cols:
                        with st.expander("Chart"):
                            chart_col = st.selectbox(
                                "Select numeric column",
                                numeric_cols,
                                key=f"chart_{idx}",
                            )
                            dim_col = st.selectbox(
                                "Select dimension column (optional)",
                                ["(none)"] + [c for c in df.columns if c != chart_col],
                                key=f"dim_{idx}",
                            )
                            if dim_col == "(none)":
                                st.bar_chart(df[chart_col])
                            else:
                                st.bar_chart(
                                    df.set_index(dim_col)[chart_col]
                                )

            except ValueError as e:
                st.warning(str(e))
            except Exception as e:
                st.error(f"Query failed: {e}")
                logger.exception("Query pipeline failed")


def main() -> None:
    st.set_page_config(
        page_title="Data Analysis AI Agent",
        page_icon="",
        layout="wide",
    )

    st.markdown(CUSTOM_CSS, unsafe_allow_html=True)

    db = get_db()
    llm = get_llm()
    cache = get_cache()

    try:
        db.init_schema()
    except Exception as e:
        st.warning("Database initialization failed — check connection config")
        st.code(str(e))

    render_sidebar(db, cache)
    render_main(db, llm, cache)


if __name__ == "__main__":
    main()
