import hashlib
import html
import logging
import os
import sys

import pandas as pd

import streamlit as st

from src.cache import QueryCache
from src.database import DatabaseManager
from src.llm import LLMService
from src.metrics import get_metric_list, build_sql

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)

CUSTOM_CSS = """
<style>
/* ═══════════════════════════════════════════════════════════════════
   Apple-inspired design system — soft shadows, generous radii,
   SF-style typography, card-based layouts, frosted sidebar.
   ═══════════════════════════════════════════════════════════════════ */

/* ── Global typography ─────────────────────────────────────────── */
html, body, [class*="css"] {
    font-family:
        -apple-system, BlinkMacSystemFont,
        "SF Pro Display", "SF Pro Text",
        system-ui,
        "Segoe UI", Roboto, Ubuntu, Cantarell,
        "Noto Sans", sans-serif,
        "Helvetica Neue", Arial;
    font-size: 15px;
    -webkit-font-smoothing: antialiased;
    -moz-osx-font-smoothing: grayscale;
}

/* ── Code blocks ───────────────────────────────────────────────── */
code, pre, [data-testid="stCodeBlock"] code {
    font-family:
        "SF Mono", Menlo, Monaco, Consolas,
        "Cascadia Mono", "Ubuntu Mono",
        "JetBrains Mono", "Fira Code",
        "Roboto Mono", "Courier New", Courier, monospace;
    font-size: 13px;
}

/* ── Cards & containers — soft shadow, generous radius ─────────── */
[data-testid="stExpander"] {
    border-radius: 14px !important;
    border: none !important;
    box-shadow: 0 1px 3px rgba(0,0,0,0.06), 0 1px 2px rgba(0,0,0,0.04) !important;
    margin-bottom: 12px !important;
}
[data-testid="stExpander"] > div:first-child {
    border-radius: 14px !important;
}

/* ── Inputs & selects — pill-like, soft focus ──────────────────── */
[data-testid="stTextInput"] input,
[data-testid="stSelectbox"] div[data-baseweb="select"] > div,
textarea {
    border-radius: 12px !important;
    border: 1px solid rgba(0,0,0,0.08) !important;
    box-shadow: 0 1px 2px rgba(0,0,0,0.03) !important;
    transition: box-shadow 0.15s ease, border-color 0.15s ease !important;
}
/* Chat input: keep Streamlit's native single-border container,
   remove duplicate border from inner textarea */
[data-testid="stChatInput"] textarea {
    border: none !important;
    box-shadow: none !important;
}
input:focus-visible, textarea:focus-visible {
    box-shadow: 0 0 0 3px rgba(0,122,255,0.15) !important;
    border-color: rgba(0,122,255,0.3) !important;
    outline: none !important;
}
[data-testid="stChatInput"] textarea:focus-visible {
    box-shadow: none !important;
    border-color: transparent !important;
}

/* ── Buttons — pill-shaped, soft ───────────────────────────────── */
button[kind], [data-testid="baseButton-secondary"] {
    border-radius: 12px !important;
    font-weight: 500 !important;
    transition: all 0.12s ease !important;
}
button[kind]:active {
    transform: scale(0.98);
}

/* ── Sidebar — frosted glass feel ──────────────────────────────── */
[data-testid="stSidebar"] {
    background: rgba(245,245,247,0.92) !important;
    backdrop-filter: blur(20px) saturate(180%) !important;
    -webkit-backdrop-filter: blur(20px) saturate(180%) !important;
    border-right: 1px solid rgba(0,0,0,0.06) !important;
}
[data-testid="stSidebar"] [data-testid="stTextInput"] [data-baseweb="input"] {
    border: none !important;
    box-shadow: none !important;
    background: rgba(0,0,0,0.04) !important;
    border-radius: 12px !important;
}
[data-testid="stSidebar"] [data-testid="stTextInput"] input {
    border: none !important;
    box-shadow: none !important;
    background: transparent !important;
}
[data-testid="stSidebar"] button[kind] {
    padding: 6px 10px !important;
    border-radius: 10px !important;
    font-size: 13px !important;
    font-weight: 450 !important;
    transition: background 0.1s ease !important;
    display: flex !important;
    align-items: center !important;
    justify-content: flex-start !important;
    text-align: left !important;
    min-width: unset !important;
}

/* ── Query history row ─────────────────────────────────────────── */
[data-testid="stSidebar"] [data-testid="stHorizontalBlock"]:has(button) {
    align-items: center !important;
}
[data-testid="stSidebar"] [data-testid="stHorizontalBlock"]:has(button) > div:last-child {
    flex: 0 0 24px !important;
    min-width: 24px !important;
    max-width: 24px !important;
}
[data-testid="stSidebar"] [data-testid="stHorizontalBlock"]:has(button) > div:last-child button {
    padding: 0 !important;
    font-size: 12px !important;
    line-height: 1 !important;
    min-height: 24px !important;
    height: 24px !important;
    width: 24px !important;
    border-radius: 12px !important;
    opacity: 0.4 !important;
    transition: opacity 0.15s ease !important;
}
[data-testid="stSidebar"] [data-testid="stHorizontalBlock"]:has(button) > div:last-child button:hover {
    opacity: 1 !important;
}
[data-testid="stSidebar"] [data-testid="stHorizontalBlock"]:has(button) > div:first-child {
    flex: 1 1 0 !important;
    min-width: 0 !important;
    overflow: hidden !important;
}

/* ── Dataframe — clean, light rows ─────────────────────────────── */
[data-testid="stTable"] table, [data-testid="stDataFrame"] table {
    font-size: 13px;
    border-radius: 12px !important;
    overflow: hidden !important;
}

/* ── Metric cards — clean ──────────────────────────────────────── */
[data-testid="stMetric"] {
    background: rgba(0,0,0,0.02) !important;
    border-radius: 14px !important;
    padding: 12px 16px !important;
}

/* ── H1 title — Apple-style large title ────────────────────────── */
h1 {
    font-size: 28px !important;
    font-weight: 700 !important;
    letter-spacing: -0.022em;
    color: var(--text-color);
    margin-bottom: 2px !important;
}

/* ── Subtitle / caption ────────────────────────────────────────── */
h1 + div {
    opacity: 0.5;
    font-size: 15px !important;
    font-weight: 400 !important;
    margin-top: 0 !important;
    padding-top: 0 !important;
}

/* ── Subheaders — Apple-style section titles ───────────────────── */
h3 {
    font-size: 17px !important;
    font-weight: 600 !important;
    letter-spacing: -0.01em;
}

/* ── Hide Streamlit's built-in running/stop indicator ──────────── */
[data-testid="stStatusWidget"] {
    display: none !important;
}

/* ── Chat message container — no avatars, transparent ──────────── */
[data-testid="stChatMessage"] {
    flex-direction: row !important;
}
[data-testid="stChatMessageAvatar"] {
    display: none !important;
}
[data-testid="stChatMessage"] > div {
    background: transparent !important;
    border: none !important;
    box-shadow: none !important;
    padding: 0 !important;
}

/* ── Push content above fixed chat input ───────────────────────── */
[data-testid="stMain"] .stMainBlockContainer {
    padding-bottom: 90px !important;
}

/* ── Stop bar — floating pill with soft shadow ─────────────────── */
[data-st-key="stop_bar"],
[data-testid="stMain"] [data-testid="stHorizontalBlock"]:has([data-testid="baseButton-tertiary"]) {
    background: rgba(255,255,255,0.85) !important;
    backdrop-filter: blur(12px) saturate(160%) !important;
    -webkit-backdrop-filter: blur(12px) saturate(160%) !important;
    border: none !important;
    border-radius: 16px !important;
    padding: 6px 0 !important;
    margin: 8px 0 !important;
    box-shadow: 0 2px 8px rgba(0,0,0,0.06), 0 0 0 0.5px rgba(0,0,0,0.06) !important;
}
[data-st-key="stop_bar"] [data-testid="stHorizontalBlock"] {
    background: transparent !important;
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


@st.cache_data(ttl=300, show_spinner=False)
def _cached_schema_context(db_hash: str) -> str:
    """Cache schema context to avoid repeated DB queries.

    db_hash ensures invalidation when the database changes.
    """
    db = get_db()
    return db.get_schema_context()


def _render_user(text: str) -> None:
    """Render user message — iMessage-style blue bubble, right-aligned."""
    escaped = html.escape(text)
    st.markdown(
        f'<div style="display:flex;justify-content:flex-end;margin:8px 0;">'
        f'<div style="background:#007AFF;color:#fff;border-radius:8px;'
        f'padding:10px 16px;max-width:75%;width:fit-content;text-align:left;'
        f'font-size:15px;font-weight:450;line-height:1.4;'
        f'box-shadow:0 1px 3px rgba(0,122,255,0.2);">'
        f'{escaped}</div></div>',
        unsafe_allow_html=True,
    )


def _render_result(df, sql: str, key_suffix: str, insight: str = "") -> None:
    """Render query result display (insight, SQL, dataframe, chart) without re-executing."""
    if insight:
        st.subheader("Summary")
        st.markdown(insight)
        st.divider()

    with st.expander("SQL"):
        st.code(sql, language="sql")

    if df.empty:
        st.info("Query returned no results")
        return

    st.subheader("Results")
    st.caption(f"{len(df)} rows returned")
    st.dataframe(df, use_container_width=True)

    numeric_cols = df.select_dtypes(include="number").columns.tolist()
    if numeric_cols:
        with st.expander("Chart"):
            import plotly.express as px

            chart_type = st.selectbox(
                "Chart type",
                ["bar", "line", "area", "scatter", "pie", "box", "histogram"],
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

            col1, col2 = st.columns(2)
            with col1:
                chart_title = st.text_input(
                    "Chart title", "",
                    key=f"chart_title_{key_suffix}",
                    placeholder="Optional title",
                )
            with col2:
                chart_color = st.color_picker(
                    "Color", "#1f77b4",
                    key=f"chart_color_{key_suffix}",
                )

            x_col = dim_col if dim_col != "(none)" else None

            try:
                if chart_type == "bar":
                    fig = px.bar(
                        df, x=x_col, y=chart_col, title=chart_title or None,
                        color_discrete_sequence=[chart_color],
                    )
                elif chart_type == "line":
                    fig = px.line(
                        df, x=x_col, y=chart_col, title=chart_title or None,
                        color_discrete_sequence=[chart_color],
                        markers=True,
                    )
                elif chart_type == "area":
                    fig = px.area(
                        df, x=x_col, y=chart_col, title=chart_title or None,
                        color_discrete_sequence=[chart_color],
                    )
                elif chart_type == "scatter":
                    fig = px.scatter(
                        df, x=x_col, y=chart_col, title=chart_title or None,
                        color_discrete_sequence=[chart_color],
                    )
                elif chart_type == "pie":
                    if x_col is None:
                        st.caption("Select a dimension column for pie charts")
                        fig = None
                    else:
                        fig = px.pie(
                            df, names=x_col, values=chart_col,
                            title=chart_title or None,
                            color_discrete_sequence=[chart_color],
                        )
                elif chart_type == "box":
                    fig = px.box(
                        df, x=x_col, y=chart_col, title=chart_title or None,
                        color_discrete_sequence=[chart_color],
                    )
                elif chart_type == "histogram":
                    fig = px.histogram(
                        df, x=chart_col, title=chart_title or None,
                        color_discrete_sequence=[chart_color],
                    )
                else:
                    fig = None

                if fig is not None:
                    fig.update_layout(
                        margin=dict(l=0, r=0, t=40 if chart_title else 20, b=0),
                        template="plotly_white",
                    )
                    st.plotly_chart(fig, use_container_width=True)
            except Exception:
                st.warning("Chart rendering failed — try different columns")

def _stop_requested() -> bool:
    """Check if user clicked stop, and show a message if so."""
    if st.session_state.get("stop_requested"):
        st.info("Analysis stopped")
        return True
    return False


def _gen_insight(llm: LLMService, question: str, sql: str, df) -> str:
    """Generate a plain-English summary of query results, with spinner."""
    if df.empty:
        return ""
    preview = df.head(20).to_markdown(index=False)
    with st.spinner("AI is analyzing results..."):
        return llm.generate_insight(question, sql, preview)


def _execute_question(
    db: DatabaseManager,
    llm: LLMService,
    cache: QueryCache,
    q: str,
    use_metric_sql: str | None = None,
) -> None:
    """Run a single question through the pipeline and render the result."""
    try:
        db_hash = hashlib.sha256(
            f"{os.environ.get('DB_TYPE','mysql')}:{os.environ.get('DB_HOST','')}:"
            f"{os.environ.get('DB_NAME','')}".encode()
        ).hexdigest()[:12]
        schema = _cached_schema_context(db_hash)
        schema_version = hashlib.sha256(schema.encode()).hexdigest()

        cached_result = cache.get(q, schema_version)
        if cached_result is not None:
            st.caption("Returned from cache")
            sql, df, insight = cached_result
        elif use_metric_sql:
            sql = use_metric_sql
            valid, err_msg = DatabaseManager.validate_sql(sql)
            if not valid:
                st.error(err_msg)
                return
            with st.spinner("Running metric query..."):
                df = db.execute_query(sql)
            if _stop_requested():
                return
            insight = _gen_insight(llm, q, sql, df)
            cache.set(q, schema_version, (sql, df, insight))
        else:
            with st.spinner("AI is generating SQL..."):
                sql = llm.generate_sql(q, schema)

            if _stop_requested():
                return

            valid, err_msg = DatabaseManager.validate_sql(sql)
            if not valid:
                st.error(err_msg)
                db.save_query(q, error=err_msg)
                return

            with st.spinner("Querying database..."):
                df = db.execute_query(sql)
            if _stop_requested():
                return
            insight = _gen_insight(llm, q, sql, df)
            cache.set(q, schema_version, (sql, df, insight))

        _render_result(df, sql, hashlib.sha256(q.encode()).hexdigest()[:12], insight=insight)

        if df.empty:
            db.save_query(q, sql=sql, row_count=0)
        else:
            db.save_query(q, sql=sql, row_count=len(df))

        # Persist for chart widget interactions across reruns
        st.session_state.last_result = {
            "sql": sql,
            "df": df,
            "insight": insight,
            "question": q,
            "from_cache": cached_result is not None,
        }

    except ValueError:
        # LLM couldn't convert to SQL — give a helpful chat response
        st.session_state.last_result = {
            "sql": "",
            "df": pd.DataFrame(),
            "insight": "",
            "question": q,
            "from_cache": False,
            "chat_response": (
                "I'm a data analysis assistant — I turn your questions "
                "into SQL and run them against the database.\n\n"
                "Try something like:\n\n"
                "• *What was the total GMV last month?*\n"
                "• *Show me top 10 customers by orders*\n"
                "• *Average order value by channel*\n"
                "• *Monthly GMV trend for 2025*\n\n"
                "Ask me anything about your data."
            ),
        }
        db.save_query(q, error="non-data question")
    except Exception as e:
        st.error(f"Query failed: {e}")
        db.save_query(q, error=str(e))
        logger.exception("Query pipeline failed")


def render_sidebar(db: DatabaseManager, cache: QueryCache) -> None:
    with st.sidebar:
        st.header("Status")

        if db.test_connection():
            st.success("Connected")
        else:
            st.error("Disconnected")

        st.caption(db.config.display_name)

        st.divider()

        st.subheader("Overview")
        try:
            tables = db.get_tables()
            st.metric("Tables", len(tables))
            search = st.text_input(
                "Search tables",
                key="table_search",
                placeholder="Search...",
                label_visibility="collapsed",
            )

            filtered = [
                t for t in tables
                if not search or search.lower() in t["TABLE_NAME"].lower()
            ]

            if search:
                if filtered:
                    for t in filtered:
                        hint = f"{t['TABLE_NAME']}"
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
                    label = f"{tbl_name}"
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
                table_rows = table_info.get("TABLE_ROWS")
                rows_display = f"{table_rows:,} rows" if table_rows else None
                if rows_display:
                    st.caption(f"{owner}  ·  {rows_display}")
                else:
                    st.caption(owner)
                latest = db.get_latest_partition(selected)
                if latest:
                    st.caption(f"Latest: {latest}")

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
        except Exception as e:
            st.warning("Unable to load table info")
            st.caption(str(e))

        st.divider()

        with st.expander("Metrics"):
            for m in get_metric_list():
                label = f"{m['name']} — {m['description']}"
                if st.button(label, key=f"metric_{m['name']}", use_container_width=True):
                    st.session_state.pending_question = m["name"]
                    st.session_state.pending_metric_sql = build_sql(m["name"])
                    st.rerun()

        st.divider()

        with st.expander("Query History"):
            try:
                recent = db.get_recent_queries(10)
                if recent:
                    if st.button("Clear All", key="clear_all_history", use_container_width=True):
                        db.clear_query_history()
                        st.rerun()
                for row in recent:
                    label = row["question"][:60] + ("..." if len(row["question"]) > 60 else "")
                    status = "ERR" if row["error"] else f"{row['result_rows'] or 0}r"
                    col1, col2 = st.columns([9, 1])
                    with col1:
                        if st.button(
                            f"[{status}] {label}",
                            key=f"hist_{row['id']}",
                            use_container_width=True,
                        ):
                            st.session_state.pending_question = row["question"]
                            st.session_state.pending_metric_sql = None
                            st.rerun()
                    with col2:
                        if st.button(
                            "✕",
                            key=f"del_{row['id']}",
                            help="Delete this entry",
                            type="tertiary",
                            use_container_width=True,
                        ):
                            db.delete_query(row["id"])
                            st.rerun()
            except Exception:
                st.caption("History unavailable")


def render_main(db: DatabaseManager, llm: LLMService, cache: QueryCache) -> None:
    st.title("Data Analysis")
    st.caption("Ask in natural language — AI generates SQL, queries the database, and visualizes results")

    # ── Extract pending question (from chat input or sidebar metric/history click) ──
    question = None
    metric_sql = None
    if st.session_state.get("pending_question"):
        question = st.session_state.pending_question
        metric_sql = st.session_state.pending_metric_sql
        st.session_state.pending_question = None
        st.session_state.pending_metric_sql = None
        st.session_state.analysis_running = True
        st.session_state.stop_requested = False
        st.session_state.current_question = question  # saved so stop can restore it

    # ── Stop button — must render OUTSIDE the `if question:` block. ──
    # During the rerun that processes the button click, `question` is None
    # (pending_question was already consumed), so the button would not appear
    # inside `if question:` → the click is lost → analysis_running stays True.
    # Rendering it here (conditional on analysis_running) ensures it survives
    # every rerun and its click is always processed.
    if st.session_state.get("analysis_running"):
        with st.container(key="stop_bar"):
            _, stop_col, _ = st.columns([4, 2, 4])
            with stop_col:
                if st.button("Stop", key="stop_btn", type="tertiary", use_container_width=True):
                    st.session_state.stop_requested = True
                    st.session_state.analysis_running = False
                    st.session_state.stopped_question = (
                        st.session_state.get("current_question", "")
                    )
                    st.session_state.pop("last_result", None)
                    st.rerun()

    # ── Render conversation ──
    if question:
        # User bubble
        _render_user(question)

        # Execute analysis
        with st.chat_message("assistant", avatar=None):
            _execute_question(db, llm, cache, question, use_metric_sql=metric_sql)

        # Analysis finished normally — save result and rerun without the stop button
        st.session_state.analysis_running = False
        st.rerun()

    elif "last_result" in st.session_state:
        result = st.session_state.last_result
        _render_user(result["question"])
        with st.chat_message("assistant", avatar=None):
            if result.get("chat_response"):
                st.markdown(result["chat_response"])
            else:
                if result.get("from_cache"):
                    st.caption("Returned from cache")
                _render_result(
                    result["df"], result["sql"],
                    hashlib.sha256(result["question"].encode()).hexdigest()[:12],
                    insight=result.get("insight", ""),
                )

    elif st.session_state.get("stopped_question"):
        _render_user(st.session_state.stopped_question)
        st.info("Analysis stopped — you can edit your question below and try again.")

    # ── Native chat input — always fixed at the viewport bottom by Streamlit ──
    # Disabled while analysis runs so the user can't queue a second question.
    is_running = bool(st.session_state.get("analysis_running"))
    prompt = st.chat_input(
        "AI is analyzing..." if is_running else "Ask a data question...",
        disabled=is_running,
    )
    if prompt and not is_running:
        st.session_state.pending_question = prompt.strip()
        st.session_state.current_question = prompt.strip()
        st.session_state.pending_metric_sql = None
        st.session_state.analysis_running = True
        st.session_state.stop_requested = False
        st.session_state.pop("stopped_question", None)  # clear stale stop state
        st.session_state.pop("last_result", None)
        st.rerun()


def main() -> None:
    st.set_page_config(
        page_title="Data Analysis",
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
