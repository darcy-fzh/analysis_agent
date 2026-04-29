import logging
import sys

import streamlit as st

from src.cache import QueryCache
from src.database import DatabaseManager
from src.llm import LLMService
from src.metrics import get_metric_list, build_sql, get_chart_type

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
    font-family:
        "Inter",
        system-ui,
        -apple-system,
        BlinkMacSystemFont,
        "Segoe UI",
        Roboto,
        Oxygen,
        Ubuntu,
        Cantarell,
        "Open Sans",
        "Helvetica Neue",
        sans-serif;
    font-size: 14px;
    -webkit-font-smoothing: antialiased;
    -moz-osx-font-smoothing: grayscale;
}

/* Code blocks — DeepSeek monospace stack */
code, pre, [data-testid="stCodeBlock"] code {
    font-family:
        Menlo,
        Monaco,
        Consolas,
        "Cascadia Mono",
        "Ubuntu Mono",
        "JetBrains Mono",
        "Fira Code",
        "Roboto Mono",
        "Courier New",
        Courier,
        monospace;
}

/* Rounded inputs — DeepSeek uses 8px/10px */
[data-testid="stTextInput"] input,
[data-testid="stSelectbox"] div[data-baseweb="select"] > div,
textarea {
    border-radius: 8px !important;
}

/* Buttons */
button[kind], [data-testid="baseButton-secondary"] {
    border-radius: 8px !important;
    font-weight: 500 !important;
}

/* Focus ring */
input:focus-visible, textarea:focus-visible, [role="combobox"]:focus-visible {
    box-shadow: 0 0 0 2px rgba(59, 130, 246, 0.5) !important;
    border-radius: 8px;
}

/* Sidebar — dark bg like DeepSeek */
[data-testid="stSidebar"] {
    background-color: #f8f9fa;
}

/* Dataframe */
[data-testid="stTable"] table, [data-testid="stDataFrame"] table {
    font-size: 13px;
}

/* Metric cards */
[data-testid="stMetric"] {
    background: transparent;
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


def _render_result(df, sql: str, key_suffix: str) -> None:
    """Render query result display (SQL, dataframe, chart) without re-executing."""
    st.code(sql, language="sql")

    if df.empty:
        st.info("Query returned no results")
        return

    st.subheader(f"Results ({len(df)} rows)")
    st.dataframe(df, use_container_width=True)

    numeric_cols = df.select_dtypes(include="number").columns.tolist()
    if numeric_cols:
        with st.expander("Chart"):
            chart_type = st.selectbox(
                "Chart type",
                ["bar", "line", "area", "pie"],
                key=f"chart_type_{key_suffix}",
            )
            chart_col = st.selectbox(
                "Select numeric column",
                numeric_cols,
                key=f"chart_{key_suffix}",
            )
            dim_col = st.selectbox(
                "Select dimension column (optional)",
                ["(none)"] + [c for c in df.columns if c != chart_col],
                key=f"dim_{key_suffix}",
            )

            chart_data = (
                df.set_index(dim_col)[chart_col]
                if dim_col != "(none)"
                else df[chart_col]
            )

            if chart_type == "bar":
                st.bar_chart(chart_data)
            elif chart_type == "line":
                st.line_chart(chart_data)
            elif chart_type == "area":
                st.area_chart(chart_data)
            elif chart_type == "pie":
                if dim_col == "(none)":
                    st.caption("Select a dimension column for pie charts")
                else:
                    import matplotlib.pyplot as plt

                    fig, ax = plt.subplots()
                    ax.pie(chart_data, labels=chart_data.index, autopct="%1.1f%%")
                    ax.axis("equal")
                    st.pyplot(fig)
                    plt.close(fig)


def _execute_question(
    db: DatabaseManager,
    llm: LLMService,
    cache: QueryCache,
    q: str,
    use_metric_sql: str | None = None,
) -> None:
    """Run a single question through the pipeline and render the result."""
    try:
        schema = db.get_schema_context()
        schema_version = str(hash(schema))

        cached_result = cache.get(q, schema_version)
        if cached_result is not None:
            st.caption("Returned from cache")
            sql, df = cached_result
        elif use_metric_sql:
            sql = use_metric_sql
            valid, err_msg = DatabaseManager.validate_sql(sql)
            if not valid:
                st.error(err_msg)
                return
            with st.spinner("Running metric query..."):
                df = db.execute_query(sql)
            cache.set(q, schema_version, (sql, df))
        else:
            with st.spinner("AI is generating SQL..."):
                sql = llm.generate_sql(q, schema)

            valid, err_msg = DatabaseManager.validate_sql(sql)
            if not valid:
                st.error(err_msg)
                db.save_query(q, error=err_msg)
                return

            with st.spinner("Querying database..."):
                df = db.execute_query(sql)
            cache.set(q, schema_version, (sql, df))

        _render_result(df, sql, str(hash(q)))

        if df.empty:
            db.save_query(q, sql=sql, row_count=0)
        else:
            db.save_query(q, sql=sql, row_count=len(df))

        # Persist for chart widget interactions across reruns
        st.session_state.last_result = {
            "sql": sql,
            "df": df,
            "question": q,
            "from_cache": cached_result is not None,
        }

    except ValueError as e:
        st.warning(str(e))
        db.save_query(q, error=str(e))
    except Exception as e:
        st.error(f"Query failed: {e}")
        db.save_query(q, error=str(e))
        logger.exception("Query pipeline failed")


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
            tables = db.get_tables()
            st.metric("Tables", len(tables))
            search = st.text_input(
                "Search tables",
                key="table_search",
                placeholder="Type to search...",
                label_visibility="collapsed",
            )

            filtered = [
                t for t in tables
                if not search or search.lower() in t["TABLE_NAME"].lower()
            ]

            if search:
                if filtered:
                    for t in filtered:
                        hint = f"📊 {t['TABLE_NAME']}"
                        if t.get("TABLE_ROWS") is not None:
                            hint += f"  ·  {t['TABLE_ROWS']:,} rows"
                        if st.button(hint, key=f"ac_{t['TABLE_NAME']}", use_container_width=True):
                            st.session_state.sidebar_selected_table = t["TABLE_NAME"]
                            st.session_state.table_search = ""
                            st.rerun()
                else:
                    st.caption("No tables match")
            else:
                for t in tables:
                    tbl_name = t["TABLE_NAME"]
                    label = f"📊 {tbl_name}"
                    if t.get("TABLE_ROWS") is not None:
                        label += f"  ({t['TABLE_ROWS']:,} rows)"
                    if st.button(label, key=f"tbl_{tbl_name}", use_container_width=True):
                        if st.session_state.get("sidebar_selected_table") == tbl_name:
                            st.session_state.sidebar_selected_table = None
                        else:
                            st.session_state.sidebar_selected_table = tbl_name
                        st.rerun()

            selected = st.session_state.get("sidebar_selected_table")
            if selected:
                st.divider()
                st.subheader(f"{selected}")
                table_info = next(
                    (t for t in tables if t["TABLE_NAME"] == selected), {}
                )
                owner = db.get_database_name()
                st.caption(f"**Owner:** {owner}")
                if table_info.get("TABLE_COMMENT"):
                    st.caption(f"**Description:** {table_info['TABLE_COMMENT']}")
                st.caption(
                    f"**Rows:** {table_info.get('TABLE_ROWS', '?'):,}  |  "
                    f"**Updated:** {table_info.get('UPDATE_TIME', '—')}"
                )
                latest = db.get_latest_partition(selected)
                if latest:
                    st.caption(f"**Latest partition:** {latest}")

                columns = db.get_columns(selected)
                if columns:
                    col_data = [
                        {
                            "Column": c["COLUMN_NAME"],
                            "Type": c["DATA_TYPE"],
                            "Nullable": c["IS_NULLABLE"],
                            "Key": c.get("COLUMN_KEY") or "",
                            "Default": str(c.get("COLUMN_DEFAULT") or ""),
                        }
                        for c in columns
                    ]
                    st.dataframe(col_data, use_container_width=True, hide_index=True)

                if st.button("Close", key="close_tbl", use_container_width=True):
                    st.session_state.sidebar_selected_table = None
                    st.rerun()
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

        with st.expander("Metrics"):
            for m in get_metric_list():
                label = f"{m['name']} — {m['description']}"
                if st.button(label, key=f"metric_{m['name']}", use_container_width=True):
                    st.session_state.pending_question = m["name"]
                    st.session_state.pending_metric_sql = build_sql(m["name"])
                    st.rerun()

        st.divider()

        st.subheader("Query History")
        try:
            recent = db.get_recent_queries(10)
            for row in recent:
                label = row["question"][:60] + ("..." if len(row["question"]) > 60 else "")
                status = "ERR" if row["error"] else f"{row['result_rows'] or 0}r"
                if st.button(
                    f"[{status}] {label}",
                    key=f"hist_{row['created_at']}",
                    use_container_width=True,
                ):
                    st.session_state.pending_question = row["question"]
                    st.session_state.pending_metric_sql = None
                    st.rerun()
        except Exception:
            st.caption("History unavailable")

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

    question = st.chat_input("Ask a data question...")

    # Handle metric or history clicks (rerouted via session state)
    if "pending_question" in st.session_state and st.session_state.pending_question:
        question = st.session_state.pending_question
        metric_sql = st.session_state.pending_metric_sql
        st.session_state.pending_question = None
        st.session_state.pending_metric_sql = None
    else:
        metric_sql = None

    if question:
        with st.chat_message("user"):
            st.write(question)
        with st.chat_message("assistant"):
            _execute_question(db, llm, cache, question, use_metric_sql=metric_sql)
    elif "last_result" in st.session_state:
        result = st.session_state.last_result
        with st.chat_message("user"):
            st.write(result["question"])
        with st.chat_message("assistant"):
            if result.get("from_cache"):
                st.caption("Returned from cache")
            _render_result(result["df"], result["sql"], str(hash(result["question"])))


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
