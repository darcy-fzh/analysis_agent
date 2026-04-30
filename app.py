import html
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
html, body, [class*="css"] {
    font-family:
        system-ui,
        -apple-system,
        "Segoe UI",
        Roboto,
        Ubuntu,
        Cantarell,
        "Noto Sans",
        sans-serif,
        BlinkMacSystemFont,
        "Helvetica Neue",
        Arial,
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

/* Focus ring — clean, no red */
input:focus-visible, textarea:focus-visible {
    box-shadow: 0 0 0 1px var(--secondary-background-color) !important;
    border-radius: 8px;
}

/* Compact sidebar buttons */
[data-testid="stSidebar"] button[kind] {
    padding: 0.15rem 0.35rem !important;
    display: flex !important;
    align-items: center !important;
    justify-content: center !important;
    text-align: center !important;
    min-width: unset !important;
}

/* Query history delete button — fixed narrow width, never wraps */
[data-testid="stSidebar"] [data-testid="stHorizontalBlock"]:has(button) > div:first-child {
    flex: 0 0 28px !important;
    min-width: 28px !important;
    max-width: 28px !important;
}
[data-testid="stSidebar"] [data-testid="stHorizontalBlock"]:has(button) > div:last-child {
    flex: 1 1 0 !important;
    min-width: 0 !important;
    overflow: hidden !important;
}

/* Sidebar — subtle bg, adapts to theme */
[data-testid="stSidebar"] {
    background-color: var(--secondary-background-color);
}

/* Dataframe */
[data-testid="stTable"] table, [data-testid="stDataFrame"] table {
    font-size: 13px;
}

/* Metric cards */
[data-testid="stMetric"] {
    background: transparent;
}

/* Title — adaptive to theme */
h1 {
    font-size: 24px !important;
    font-weight: 600 !important;
    letter-spacing: -0.02em;
    color: var(--text-color);
}

/* Subtitle */
h1 + div {
    opacity: 0.6;
}

/* Subheaders must be smaller than title */
h3 {
    font-size: 16px !important;
    font-weight: 500 !important;
}

/* Hide Streamlit's built-in running/stop indicator (top-right) */
[data-testid="stStatusWidget"] {
    display: none !important;
}

/* Chat message container — clean, no avatars */
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


/* Push content above fixed chat bar */
[data-testid="stMain"] .stMainBlockContainer {
    padding-bottom: 80px !important;
}

/* Chat bar send / stop button — square, compact */
[data-testid="stMain"] [data-testid="stHorizontalBlock"] [data-testid="stBaseButton-secondary"] {
    min-width: 36px !important;
    width: 36px !important;
    height: 36px !important;
    padding: 0 !important;
    border-radius: 8px !important;
    font-size: 16px !important;
}

/* Collapse the chat form's outer wrappers so they take no space in document
   flow. Use direct-child (>) selectors so we only collapse the form's own
   shells — NOT the stVerticalBlock column wrappers inside stHorizontalBlock
   (those also have data-testid="stVerticalBlock" and would become height:0
   with a descendant selector, hiding the text input and button). */
[data-testid="stMain"] [data-testid="stForm"],
[data-testid="stMain"] [data-testid="stForm"] > div,
[data-testid="stMain"] [data-testid="stForm"] > div > [data-testid="stVerticalBlock"] {
    height: 0 !important;
    min-height: 0 !important;
    border: none !important;
    padding: 0 !important;
    margin: 0 !important;
    background: transparent !important;
    box-shadow: none !important;
}

</style>
"""


# JS: fix the chat bar (stHorizontalBlock containing a stTextInput) to viewport bottom.
# Uses MutationObserver so it re-applies after every Streamlit rerun without setTimeout races.
CHAT_BAR_JS = """
<script>
(function() {
    var doc = window.parent.document;

    // Disconnect previous observer from prior reruns
    if (window.parent._chatBarObserver) {
        window.parent._chatBarObserver.disconnect();
    }
    if (window.parent._chatBarTimer) {
        clearTimeout(window.parent._chatBarTimer);
    }

    function applyFixed(el) {
        // left = sidebar right edge so we never overlap the sidebar
        var sidebar = doc.querySelector('[data-testid="stSidebar"]');
        var sidebarRight = sidebar ? sidebar.getBoundingClientRect().right : 0;

        el.style.setProperty('position',         'fixed',                             'important');
        el.style.setProperty('top',              'auto',                              'important');
        el.style.setProperty('bottom',           '0',                                 'important');
        el.style.setProperty('left',             sidebarRight + 'px',                 'important');
        el.style.setProperty('right',            '0',                                 'important');
        el.style.setProperty('z-index',          '999',                               'important');
        el.style.setProperty('padding',          '10px 16px 12px',                    'important');
        el.style.setProperty('margin',           '0',                                 'important');
        el.style.setProperty('border-top',       '1px solid rgba(128,128,128,0.15)',   'important');
        el.style.setProperty('border-left',      'none',                              'important');
        el.style.setProperty('border-right',     'none',                              'important');
        el.style.setProperty('border-bottom',    'none',                              'important');
        el.style.setProperty('border-radius',    '0',                                 'important');
        el.style.setProperty('background-color', 'var(--background-color)',           'important');
        el.style.setProperty('box-sizing',       'border-box',                        'important');
    }

    function fix() {
        var main = doc.querySelector('[data-testid="stMain"]');
        if (!main) return;
        // Find the last stHorizontalBlock inside stMain that contains a stTextInput.
        // This uniquely identifies the chat bar (chart columns use selectboxes, not text inputs).
        var blocks = main.querySelectorAll('[data-testid="stHorizontalBlock"]');
        for (var i = blocks.length - 1; i >= 0; i--) {
            if (blocks[i].querySelector('[data-testid="stTextInput"]')) {
                applyFixed(blocks[i]);
                return;
            }
        }
    }

    // Debounce so rapid DOM mutations don't thrash style recalculations
    function schedFix() {
        if (window.parent._chatBarTimer) clearTimeout(window.parent._chatBarTimer);
        window.parent._chatBarTimer = setTimeout(fix, 50);
    }

    fix();
    setTimeout(fix, 50);   // re-run after current render cycle
    window.parent._chatBarObserver = new MutationObserver(schedFix);
    window.parent._chatBarObserver.observe(doc.body, { childList: true, subtree: true });
})();
</script>
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


def _render_user(text: str) -> None:
    """Render user message — right-aligned gray bubble, fit to text."""
    escaped = html.escape(text)
    st.markdown(
        f'<div style="display:flex;justify-content:flex-end;margin:6px 0;">'
        f'<div style="background:#f3f4f6;border-radius:14px;padding:8px 14px;'
        f'max-width:75%;width:fit-content;text-align:left;">'
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
        schema = db.get_schema_context()
        schema_version = str(hash(schema))

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

        _render_result(df, sql, str(hash(q)), insight=insight)

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
                st.caption(f"**Owner:** {owner}")
                if table_info.get("TABLE_COMMENT"):
                    st.caption(f"**Description:** {table_info['TABLE_COMMENT']}")
                table_rows = table_info.get("TABLE_ROWS")
                rows_display = f"{table_rows:,}" if table_rows is not None else "?"
                updated = table_info.get("UPDATE_TIME") or "—"
                st.caption(f"**Rows:** {rows_display}  |  **Updated:** {updated}")
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
                    col1, col2 = st.columns([1, 9])
                    with col1:
                        if st.button(
                            "✕",
                            key=f"del_{row['id']}",
                            help="Delete this entry",
                            use_container_width=True,
                        ):
                            db.delete_query(row["id"])
                            st.rerun()
                    with col2:
                        if st.button(
                            f"[{status}] {label}",
                            key=f"hist_{row['id']}",
                            use_container_width=True,
                        ):
                            st.session_state.pending_question = row["question"]
                            st.session_state.pending_metric_sql = None
                            st.rerun()
            except Exception:
                st.caption("History unavailable")


def render_main(db: DatabaseManager, llm: LLMService, cache: QueryCache) -> None:
    st.title("Data Analysis AI Agent")
    st.caption("Ask questions in natural language — AI generates SQL and queries the database")

    # ── Handle pending question (from chat submit or metric/history click) ──
    question = None
    metric_sql = None
    if "pending_question" in st.session_state and st.session_state.pending_question:
        question = st.session_state.pending_question
        metric_sql = st.session_state.pending_metric_sql
        st.session_state.pending_question = None
        st.session_state.pending_metric_sql = None
        st.session_state.analysis_running = True
        st.session_state.stop_requested = False

    # ── Display conversation ──
    if question:
        _render_user(question)
    elif "last_result" in st.session_state:
        result = st.session_state.last_result
        _render_user(result["question"])
        with st.chat_message("assistant", avatar=None):
            if result.get("from_cache"):
                st.caption("Returned from cache")
            _render_result(result["df"], result["sql"], str(hash(result["question"])), insight=result.get("insight", ""))
    elif st.session_state.get("stopped_question"):
        _render_user(st.session_state.stopped_question)
        st.info("Analysis stopped — you can edit your question below and try again.")

    # ── Chat bar (fixed to viewport bottom via JS) ──
    # Rendered BEFORE _execute_question so the stop button is visible during analysis.
    is_running = bool(st.session_state.get("analysis_running"))

    if is_running:
        col_input, col_btn = st.columns([20, 1])
        with col_input:
            st.text_input(
                "chat",
                placeholder="AI is analyzing...",
                label_visibility="collapsed",
                key="chat_disabled_input",
                disabled=True,
            )
        with col_btn:
            # ■ replaces the send button while analysis runs — same pattern as ChatGPT/Gemini
            if st.button("■", key="stop_btn", help="Stop analysis"):
                st.session_state.stop_requested = True
                st.session_state.analysis_running = False  # immediately re-enable input
                st.rerun()
    else:
        cycle = st.session_state.get("chat_cycle", 0)
        with st.form(key=f"chat_{cycle}", clear_on_submit=True):
            col_input, col_btn = st.columns([20, 1])
            with col_input:
                user_input = st.text_input(
                    "chat",
                    placeholder="Ask a data question...",
                    label_visibility="collapsed",
                )
            with col_btn:
                submitted = st.form_submit_button("↑")
            if submitted and (user_input or "").strip():
                st.session_state.pending_question = user_input.strip()
                st.session_state.pending_metric_sql = None
                st.session_state.analysis_running = True
                st.session_state.stop_requested = False
                st.session_state.chat_cycle = cycle + 1
                st.rerun()

    # JS fixes the row above to the viewport bottom via MutationObserver
    st.components.v1.html(CHAT_BAR_JS, height=0)

    # ── Run analysis after chat bar is rendered (stop button already visible) ──
    if question:
        with st.chat_message("assistant", avatar=None):
            _execute_question(db, llm, cache, question, use_metric_sql=metric_sql)
        st.session_state.analysis_running = False
        if st.session_state.get("stop_requested"):
            st.session_state.stopped_question = question
        st.rerun()


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
