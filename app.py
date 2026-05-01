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

# ── i18n ─────────────────────────────────────────────────────────
LANG = {
    "en": {
        "language": "Language",
        "title": "Data Analysis",
        "caption": "Ask in natural language — AI generates SQL, queries the database, and visualizes results",
        "status": "Status",
        "connected": "Connected",
        "disconnected": "Disconnected",
        "overview": "Overview",
        "tables": "Tables",
        "search_placeholder": "Search...",
        "search_label": "Search tables",
        "no_tables_match": "No tables match",
        "close": "Close",
        "unable_load_table": "Unable to load table info",
        "metrics": "Metrics",
        "query_history": "Query History",
        "clear_all": "Clear All",
        "err": "ERR",
        "delete_entry": "Delete this entry",
        "history_unavailable": "History unavailable",
        "stop": "Stop",
        "analyzing": "AI is analyzing...",
        "ask_question": "Ask a data question...",
        "analysis_stopped": "Analysis stopped — you can edit your question below and try again.",
        "db_init_failed": "Database initialization failed — check connection config",
        "summary": "Summary",
        "sql": "SQL",
        "no_results": "Query returned no results",
        "results": "Results",
        "rows_returned": "{} rows returned",
        "rows": "rows",
        "chart": "Chart",
        "chart_type": "Chart type",
        "select_numeric": "Select numeric column",
        "select_dimension": "Select dimension column (optional)",
        "none": "(none)",
        "chart_title": "Chart title",
        "optional_title": "Optional title",
        "color": "Color",
        "pie_needs_dimension": "Select a dimension column for pie charts",
        "chart_failed": "Chart rendering failed — try different columns",
        "returned_from_cache": "Returned from cache",
        "analyzing_results": "AI is analyzing results...",
        "running_metric": "Running metric query...",
        "generating_sql": "AI is generating SQL...",
        "querying_db": "Querying database...",
        "analysis_stopped_check": "Analysis stopped",
        "query_failed": "Query failed",
        "column": "Column",
        "type": "Type",
        "nullable": "Nullable",
        "key": "Key",
        "default": "Default",
        "latest": "Latest",
        "chart_types": ["bar", "line", "area", "scatter", "pie", "box", "histogram"],
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
    },
    "zh": {
        "language": "语言",
        "title": "数据分析",
        "caption": "用自然语言提问 — AI 自动生成 SQL、查询数据库并可视化结果",
        "status": "状态",
        "connected": "已连接",
        "disconnected": "未连接",
        "overview": "数据概览",
        "tables": "数据表",
        "search_placeholder": "搜索...",
        "search_label": "搜索数据表",
        "no_tables_match": "未找到匹配的表",
        "close": "关闭",
        "unable_load_table": "无法加载表信息",
        "metrics": "指标查询",
        "query_history": "查询历史",
        "clear_all": "清空全部",
        "err": "错误",
        "delete_entry": "删除此条目",
        "history_unavailable": "历史记录不可用",
        "stop": "停止",
        "analyzing": "AI 分析中...",
        "ask_question": "输入你的数据问题...",
        "analysis_stopped": "分析已停止 — 你可以在下方编辑问题后重新提交",
        "db_init_failed": "数据库初始化失败 — 请检查连接配置",
        "summary": "摘要",
        "sql": "SQL",
        "no_results": "查询未返回任何结果",
        "results": "查询结果",
        "rows_returned": "返回 {} 行",
        "rows": "行",
        "chart": "图表",
        "chart_type": "图表类型",
        "select_numeric": "选择数值列",
        "select_dimension": "选择维度列（可选）",
        "none": "(无)",
        "chart_title": "图表标题",
        "optional_title": "可选标题",
        "color": "颜色",
        "pie_needs_dimension": "饼图需要选择一个维度列",
        "chart_failed": "图表渲染失败 — 请尝试其他列",
        "returned_from_cache": "从缓存返回",
        "analyzing_results": "AI 正在分析结果...",
        "running_metric": "正在执行指标查询...",
        "generating_sql": "AI 正在生成 SQL...",
        "querying_db": "正在查询数据库...",
        "analysis_stopped_check": "分析已停止",
        "query_failed": "查询失败",
        "column": "列名",
        "type": "类型",
        "nullable": "可空",
        "key": "键",
        "default": "默认值",
        "latest": "最新",
        "chart_types": ["柱状图", "折线图", "面积图", "散点图", "饼图", "箱线图", "直方图"],
        "chat_response": (
            "我是一个数据分析助手 — 将你的问题转换为 SQL 并查询数据库。\n\n"
            "你可以尝试以下问题：\n\n"
            "• *上个月的总 GMV 是多少？*\n"
            "• *显示订单最多的前 10 个客户*\n"
            "• *各渠道的平均订单价值*\n"
            "• *2025 年月度 GMV 趋势*\n\n"
            "向我提出任何关于数据的问题。"
        ),
    },
}


def t(key: str) -> str:
    """Return translated string for the current language."""
    lang = st.session_state.get("lang", "en")
    return LANG.get(lang, LANG["en"]).get(key, key)


def t_list(key: str) -> list:
    """Return translated list for the current language."""
    lang = st.session_state.get("lang", "en")
    return LANG.get(lang, LANG["en"]).get(key, [key])


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
[data-testid="stSidebar"] [data-testid="stTextInput"] div {
    border: none !important;
    box-shadow: none !important;
    background: transparent !important;
}
[data-testid="stSidebar"] [data-testid="stTextInput"] [data-baseweb="input"] {
    border: none !important;
    box-shadow: none !important;
    background: #ffffff !important;
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
/* ── Hide toolbar (Print, etc.) and deploy button ──────────────── */
[data-testid="stToolbar"] {
    display: none !important;
}
[data-testid="stDeployButton"] {
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

/* ── Hide native Streamlit header to reclaim top space ─────────── */
[data-testid="stHeader"] {
    display: none !important;
}

/* ── Push content above fixed chat input ───────────────────────── */
[data-testid="stMain"] .stMainBlockContainer,
[data-testid="stMainBlockContainer"],
.main .block-container,
.block-container {
    padding-top: 4px !important;
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

/* ── Top-right header controls — theme icon + lang selectbox ────── */
[data-st-key="top_ctrl_row"] {
    margin-bottom: -4px !important;
}
/* Theme button: compact, transparent, no border */
[data-st-key="top_ctrl_row"] button {
    padding: 2px 6px !important;
    height: 26px !important;
    min-height: 0 !important;
    border-radius: 6px !important;
    background: transparent !important;
    border: none !important;
    box-shadow: none !important;
    font-size: 16px !important;
    font-weight: 500 !important;
    line-height: 1 !important;
    transition: background 0.12s ease, color 0.12s ease !important;
}
[data-st-key="top_ctrl_row"] button:hover {
    background: rgba(0,0,0,0.05) !important;
}
/* Language selectbox — completely transparent, no border, no background.
   Use html+body prefix to maximise specificity and beat BaseWeb's own rules. */
html body [data-st-key="top_ctrl_row"] [data-testid="stSelectbox"] div[data-baseweb="select"] > div,
html body [data-st-key="top_ctrl_row"] [data-testid="stSelectbox"] div[data-baseweb="select"] > div:hover,
html body [data-st-key="top_ctrl_row"] [data-testid="stSelectbox"] div[data-baseweb="select"] > div:focus-within,
html body [data-st-key="top_ctrl_row"] [data-testid="stSelectbox"] div[data-baseweb="select"] > div[aria-expanded="true"],
html body [data-st-key="top_ctrl_row"] [data-testid="stSelectbox"] div[data-baseweb="select"] > div > div {
    border: none !important;
    border-color: transparent !important;
    box-shadow: none !important;
    background: transparent !important;
    background-color: transparent !important;
    min-height: 28px !important;
    outline: none !important;
}
html body [data-st-key="top_ctrl_row"] [data-testid="stSelectbox"] span {
    font-size: 13px !important;
    font-weight: 500 !important;
}
html body [data-st-key="top_ctrl_row"] [data-testid="stSelectbox"] svg {
    width: 14px !important;
    height: 14px !important;
    margin-left: 1px !important;
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
        st.subheader(t("summary"))
        st.markdown(insight)
        st.divider()

    with st.expander(t("sql")):
        st.code(sql, language="sql")

    if df.empty:
        st.info(t("no_results"))
        return

    st.subheader(t("results"))
    st.caption(t("rows_returned").format(len(df)))
    st.dataframe(df, use_container_width=True)

    numeric_cols = df.select_dtypes(include="number").columns.tolist()
    if numeric_cols:
        with st.expander(t("chart")):
            import plotly.express as px

            chart_type = st.selectbox(
                t("chart_type"),
                t_list("chart_types"),
                key=f"chart_type_{key_suffix}",
            )
            chart_col = st.selectbox(
                t("select_numeric"),
                numeric_cols,
                key=f"chart_{key_suffix}",
            )
            dim_col = st.selectbox(
                t("select_dimension"),
                [t("none")] + [c for c in df.columns if c != chart_col],
                key=f"dim_{key_suffix}",
            )

            col1, col2 = st.columns(2)
            with col1:
                chart_title = st.text_input(
                    t("chart_title"), "",
                    key=f"chart_title_{key_suffix}",
                    placeholder=t("optional_title"),
                )
            with col2:
                chart_color = st.color_picker(
                    t("color"), "#1f77b4",
                    key=f"chart_color_{key_suffix}",
                )

            x_col = dim_col if dim_col != t("none") else None

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
                        st.caption(t("pie_needs_dimension"))
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
                st.warning(t("chart_failed"))

def _stop_requested() -> bool:
    """Check if user clicked stop, and show a message if so."""
    if st.session_state.get("stop_requested"):
        st.info(t("analysis_stopped_check"))
        return True
    return False


def _gen_insight(llm: LLMService, question: str, sql: str, df) -> str:
    """Generate a plain-English summary of query results, with spinner."""
    if df.empty:
        return ""
    preview = df.head(20).to_markdown(index=False)
    with st.spinner(t("analyzing_results")):
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
            st.caption(t("returned_from_cache"))
            sql, df, insight = cached_result
        elif use_metric_sql:
            sql = use_metric_sql
            valid, err_msg = DatabaseManager.validate_sql(sql)
            if not valid:
                st.error(err_msg)
                return
            with st.spinner(t("running_metric")):
                df = db.execute_query(sql)
            if _stop_requested():
                return
            insight = _gen_insight(llm, q, sql, df)
            cache.set(q, schema_version, (sql, df, insight))
        else:
            with st.spinner(t("generating_sql")):
                sql = llm.generate_sql(q, schema)

            if _stop_requested():
                return

            valid, err_msg = DatabaseManager.validate_sql(sql)
            if not valid:
                st.error(err_msg)
                db.save_query(q, error=err_msg)
                return

            with st.spinner(t("querying_db")):
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
            "chat_response": t("chat_response"),
        }
        db.save_query(q, error="non-data question")
    except Exception as e:
        st.error(f"{t('query_failed')}: {e}")
        db.save_query(q, error=str(e))
        logger.exception("Query pipeline failed")


def render_sidebar(db: DatabaseManager, cache: QueryCache) -> None:
    with st.sidebar:
        st.header(t("status"))

        if db.test_connection():
            st.success(t("connected"))
        else:
            st.error(t("disconnected"))

        st.divider()

        st.subheader(t("overview"))
        try:
            tables = db.get_tables()
            st.metric(t("tables"), len(tables))
            search = st.text_input(
                t("search_label"),
                key="table_search",
                placeholder=t("search_placeholder"),
                label_visibility="collapsed",
            )

            filtered = [
                tbl for tbl in tables
                if not search or search.lower() in tbl["TABLE_NAME"].lower()
            ]

            if search:
                if filtered:
                    for tbl in filtered:
                        hint = f"{tbl['TABLE_NAME']}"
                        if tbl.get("TABLE_ROWS") is not None:
                            hint += f"  ·  {tbl['TABLE_ROWS']:,} {t('rows')}"
                        if st.button(hint, key=f"ac_{tbl['TABLE_NAME']}", use_container_width=True):
                            st.session_state.sidebar_selected_table = tbl["TABLE_NAME"]
                            st.session_state.table_search = ""
                            st.rerun()
                else:
                    st.caption(t("no_tables_match"))
            else:
                for tbl in tables:
                    tbl_name = tbl["TABLE_NAME"]
                    label = f"{tbl_name}"
                    if tbl.get("TABLE_ROWS") is not None:
                        label += f"  ({tbl['TABLE_ROWS']:,} {t('rows')})"
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
                    (tbl for tbl in tables if tbl["TABLE_NAME"] == selected), {}
                )
                owner = db.get_database_name()
                table_rows = table_info.get("TABLE_ROWS")
                rows_display = f"{table_rows:,} {t('rows')}" if table_rows else None
                if rows_display:
                    st.caption(f"{owner}  ·  {rows_display}")
                else:
                    st.caption(owner)
                latest = db.get_latest_partition(selected)
                if latest:
                    st.caption(f"{t('latest')}: {latest}")

                columns = db.get_columns(selected)
                if columns:
                    col_data = [
                        {
                            t("column"): c["COLUMN_NAME"],
                            t("type"): c["DATA_TYPE"],
                            t("nullable"): c["IS_NULLABLE"],
                            t("key"): c.get("COLUMN_KEY") or "",
                            t("default"): str(c.get("COLUMN_DEFAULT") or ""),
                        }
                        for c in columns
                    ]
                    st.dataframe(col_data, use_container_width=True, hide_index=True)

                if st.button(t("close"), key="close_tbl", use_container_width=True):
                    st.session_state.sidebar_selected_table = None
                    st.rerun()
        except Exception as e:
            st.warning(t("unable_load_table"))
            st.caption(str(e))

        st.divider()

        with st.expander(t("metrics")):
            for m in get_metric_list():
                label = f"{m['name']} — {m['description']}"
                if st.button(label, key=f"metric_{m['name']}", use_container_width=True):
                    st.session_state.pending_question = m["name"]
                    st.session_state.pending_metric_sql = build_sql(m["name"])
                    st.rerun()

        st.divider()

        with st.expander(t("query_history")):
            try:
                recent = db.get_recent_queries(10)
                if recent:
                    if st.button(t("clear_all"), key="clear_all_history", use_container_width=True):
                        db.clear_query_history()
                        st.rerun()
                for row in recent:
                    label = row["question"][:60] + ("..." if len(row["question"]) > 60 else "")
                    status = t("err") if row["error"] else f"{row['result_rows'] or 0}r"
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
                            help=t("delete_entry"),
                            type="tertiary",
                            use_container_width=True,
                        ):
                            db.delete_query(row["id"])
                            st.rerun()
            except Exception:
                st.caption(t("history_unavailable"))


def render_main(db: DatabaseManager, llm: LLMService, cache: QueryCache) -> None:
    is_dark = st.session_state.get("theme", "light") == "dark"
    cur_lang = st.session_state.get("lang", "en")

    # ── Dark mode CSS ────────────────────────────────────────────
    if is_dark:
        st.markdown("""<style>
/* ── Dark mode — comprehensive overrides ─────────────────────── */

/* App header bar (the native Streamlit top bar) */
header[data-testid="stHeader"],
.stApp > header {
    background: #1c1c1e !important;
    border-bottom: 1px solid rgba(255,255,255,0.06) !important;
}

/* App & main backgrounds */
[data-testid="stApp"],
[data-testid="stMain"],
[data-testid="stMainBlockContainer"],
.main .block-container {
    background: #1a1a1b !important;
    color: #e4e4e5 !important;
}

/* All text */
body, p, span, div, li, td, th, strong, em, a,
[data-testid="stText"],
[data-testid="stMarkdown"],
[data-testid="stMarkdown"] p,
[data-testid="stMarkdown"] li {
    color: #e4e4e5 !important;
}
h1, h2, h3, h4, h5, h6 { color: #f2f2f7 !important; }
label { color: rgba(228,228,229,0.75) !important; }
[data-testid="stCaption"],
[data-testid="stCaption"] p { color: rgba(228,228,229,0.45) !important; }

/* ── Sidebar ──────────────────────────────────────────────────── */
[data-testid="stSidebar"] {
    background: rgba(28,28,30,0.98) !important;
    border-right: 1px solid rgba(255,255,255,0.06) !important;
}
[data-testid="stSidebar"] * { color: #e4e4e5 !important; }
/* Sidebar search box */
[data-testid="stSidebar"] [data-testid="stTextInput"] [data-baseweb="input"] {
    background: rgba(255,255,255,0.08) !important;
    border: none !important;
}
[data-testid="stSidebar"] [data-testid="stTextInput"] input {
    color: #e4e4e5 !important;
    background: transparent !important;
}
[data-testid="stSidebar"] [data-testid="stTextInput"] input::placeholder {
    color: rgba(228,228,229,0.40) !important;
}
/* Sidebar ALL buttons (table list, metrics, history) */
[data-testid="stSidebar"] button,
[data-testid="stSidebar"] [data-testid="baseButton-secondary"],
[data-testid="stSidebar"] [data-testid="baseButton-primary"] {
    background: rgba(255,255,255,0.06) !important;
    color: #e4e4e5 !important;
    border: 1px solid rgba(255,255,255,0.07) !important;
}
[data-testid="stSidebar"] button:hover,
[data-testid="stSidebar"] [data-testid="baseButton-secondary"]:hover {
    background: rgba(255,255,255,0.11) !important;
}
[data-testid="stSidebar"] [data-testid="baseButton-tertiary"] {
    background: transparent !important;
    border: none !important;
}
/* Dividers */
hr { border-color: rgba(255,255,255,0.08) !important; opacity: 1 !important; }

/* ── Expanders ────────────────────────────────────────────────── */
[data-testid="stExpander"] {
    background: rgba(36,36,38,0.9) !important;
    border: 1px solid rgba(255,255,255,0.07) !important;
    box-shadow: 0 1px 4px rgba(0,0,0,0.35) !important;
}
[data-testid="stExpander"] summary,
[data-testid="stExpander"] summary * {
    color: #e4e4e5 !important;
    background: transparent !important;
}
[data-testid="stExpander"] summary:hover { background: rgba(255,255,255,0.04) !important; }
[data-testid="stExpander"] > div:last-child { background: transparent !important; }

/* ── Alerts / info / warning / success / error ────────────────── */
[data-testid="stAlert"],
[data-testid="stInfo"],
[data-testid="stWarning"],
[data-testid="stSuccess"],
[data-testid="stError"] {
    background: rgba(44,44,46,0.85) !important;
    border-color: rgba(255,255,255,0.08) !important;
}
[data-testid="stAlert"] *,
[data-testid="stInfo"] *,
[data-testid="stWarning"] *,
[data-testid="stSuccess"] *,
[data-testid="stError"] * { color: #e4e4e5 !important; }

/* ── Code blocks ──────────────────────────────────────────────── */
[data-testid="stCodeBlock"],
[data-testid="stCodeBlock"] pre,
[data-testid="stCodeBlock"] code {
    background: rgba(0,0,0,0.45) !important;
    color: #e4e4e5 !important;
}

/* ── DataFrames / Tables ──────────────────────────────────────── */
[data-testid="stDataFrame"] iframe,
[data-testid="stDataFrame"] > div,
[data-testid="stTable"] table {
    background: rgba(28,28,30,0.9) !important;
}
[data-testid="stDataFrame"] * { color: #e4e4e5 !important; }
[data-testid="stTable"] th, [data-testid="stTable"] td { color: #e4e4e5 !important; }

/* ── Selectbox / dropdown ─────────────────────────────────────── */
[data-testid="stSelectbox"] [data-baseweb="select"] > div {
    background: rgba(44,44,46,0.9) !important;
    border-color: rgba(255,255,255,0.08) !important;
    color: #e4e4e5 !important;
}
[data-baseweb="popover"] [data-baseweb="menu"],
[data-baseweb="popover"] ul {
    background: #2c2c2e !important;
    border-color: rgba(255,255,255,0.08) !important;
}
[data-baseweb="popover"] li,
[data-baseweb="menu"] li { color: #e4e4e5 !important; }
[data-baseweb="menu"] li:hover { background: rgba(255,255,255,0.07) !important; }

/* ── Text inputs ──────────────────────────────────────────────── */
[data-testid="stTextInput"] input {
    background: rgba(44,44,46,0.85) !important;
    color: #e4e4e5 !important;
    border-color: rgba(255,255,255,0.08) !important;
}

/* ── Metric cards ─────────────────────────────────────────────── */
[data-testid="stMetric"] {
    background: rgba(255,255,255,0.04) !important;
}
[data-testid="stMetric"] * { color: #e4e4e5 !important; }

/* ── Chat input ───────────────────────────────────────────────── */
[data-testid="stBottom"],
[data-testid="stBottom"] > div,
[data-testid="stChatInputContainer"] {
    background: #1a1a1b !important;
    border-top: 1px solid rgba(255,255,255,0.06) !important;
}
/* Target all divs inside stChatInput (deeply nested) */
[data-testid="stChatInput"],
[data-testid="stChatInput"] > div,
[data-testid="stChatInput"] div {
    background: #2c2c2e !important;
    border-color: rgba(255,255,255,0.08) !important;
}
[data-testid="stChatInput"] textarea {
    background: #2c2c2e !important;
    color: #e4e4e5 !important;
    caret-color: #e4e4e5 !important;
}
[data-testid="stChatInput"] textarea::placeholder {
    color: rgba(228,228,229,0.38) !important;
}
/* Send button keep transparent */
[data-testid="stChatInput"] button,
[data-testid="stChatInput"] button * {
    background: transparent !important;
}

/* ── Stop bar ─────────────────────────────────────────────────── */
[data-st-key="stop_bar"],
[data-testid="stMain"] [data-testid="stHorizontalBlock"]:has([data-testid="baseButton-tertiary"]) {
    background: rgba(44,44,46,0.92) !important;
    border: 1px solid rgba(255,255,255,0.06) !important;
    box-shadow: 0 2px 12px rgba(0,0,0,0.4) !important;
}

/* ── Top controls dark variant ────────────────────────────────── */
[data-st-key="top_ctrl_row"] button:hover {
    background: rgba(255,255,255,0.08) !important;
}

/* ── Color pickers ────────────────────────────────────────────── */
[data-testid="stColorPicker"] label { color: #e4e4e5 !important; }

/* ── Plotly transparent bg ────────────────────────────────────── */
.js-plotly-plot .plotly .main-svg { background: transparent !important; }
        </style>""", unsafe_allow_html=True)

    # ── Top-right controls ─────────────────────────────────────────
    _c_icon = "rgba(0,0,0,0.45)"  if not is_dark else "rgba(228,228,229,0.50)"
    _c_sel  = "rgba(0,0,0,0.65)"  if not is_dark else "rgba(228,228,229,0.75)"
    st.markdown(f"""<style>
[data-st-key="theme_btn"] button {{ color: {_c_icon} !important; }}
[data-st-key="top_ctrl_row"] [data-baseweb="select"] [data-testid="stSelectboxInputContainer"] span,
[data-st-key="top_ctrl_row"] [data-baseweb="select"] div[role="combobox"] span {{
    color: {_c_sel} !important;
}}
[data-st-key="top_ctrl_row"] [data-baseweb="select"] svg {{ fill: {_c_icon} !important; }}
</style>""", unsafe_allow_html=True)

    with st.container(key="top_ctrl_row"):
        _, h_right = st.columns([5, 1])
        with h_right:
            c_icon, c_lang = st.columns([1, 2])
            with c_icon:
                icon = "◑" if is_dark else "◐"
                if st.button(icon, key="theme_btn", type="tertiary", use_container_width=True):
                    st.session_state.theme = "light" if is_dark else "dark"
                    st.rerun()
            with c_lang:
                chosen = st.selectbox(
                    "lang",
                    ["EN", "中文"],
                    index=1 if cur_lang == "zh" else 0,
                    key="lang_sel",
                    label_visibility="collapsed",
                )
                new_lang = "zh" if chosen == "中文" else "en"
                if new_lang != cur_lang:
                    st.session_state.lang = new_lang
                    st.rerun()

    st.title(t("title"))
    st.caption(t("caption"))

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
                if st.button(t("stop"), key="stop_btn", type="tertiary", use_container_width=True):
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
                    st.caption(t("returned_from_cache"))
                _render_result(
                    result["df"], result["sql"],
                    hashlib.sha256(result["question"].encode()).hexdigest()[:12],
                    insight=result.get("insight", ""),
                )

    elif st.session_state.get("stopped_question"):
        _render_user(st.session_state.stopped_question)
        st.info(t("analysis_stopped"))

    # ── Native chat input — always fixed at the viewport bottom by Streamlit ──
    # Disabled while analysis runs so the user can't queue a second question.
    is_running = bool(st.session_state.get("analysis_running"))
    prompt = st.chat_input(
        t("analyzing") if is_running else t("ask_question"),
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
        page_title="Data Analysis",  # static — Streamlit needs this before session state
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
        st.warning(t("db_init_failed"))
        st.code(str(e))

    render_sidebar(db, cache)
    render_main(db, llm, cache)


if __name__ == "__main__":
    main()
