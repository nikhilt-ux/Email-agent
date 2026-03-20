"""
Merchandising AI Agent — Streamlit Frontend  v3.0
═══════════════════════════════════════════════════
Design: Fashion editorial dark theme — deep navy canvas, electric coral/amber/teal accents.
Typography: DM Serif Display (headings) + DM Sans (body) — editorial, characterful.
Fully synced with gmail_reader v6.0 (24 columns A–X).
"""

import os
import streamlit as st
import pandas as pd
from datetime import datetime
from googleapiclient.discovery import build

from gmail_reader import (
    authenticate,
    load_vendor_db,
    SHEET_ID,
    SHEET_TAB,
    load_existing_rows,
    process_thread,
    update_existing_row,
    append_new_rows,
    flush_error_log,
    _ensure_tab,
    LOGS_HEADERS,
    ERROR_TAB,
    ERROR_HEADERS,
    record_error,
    cache_stats,
    write_audit_log,
    col_letter,
    _build_new_row,
    _col_letter_n,
)
import gmail_reader

# ── Page config ───────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Merch AI Agent",
    page_icon="🧵",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Master CSS ────────────────────────────────────────────────────────────────
st.markdown("""
<link href="https://fonts.googleapis.com/css2?family=DM+Serif+Display:ital@0;1&family=DM+Sans:ital,opsz,wght@0,9..40,300;0,9..40,400;0,9..40,500;0,9..40,600;1,9..40,300&display=swap" rel="stylesheet">

<style>
:root {
    --navy:         #0A0F1E;
    --navy-mid:     #111827;
    --navy-card:    #161D2F;
    --navy-hover:   #1E2A42;
    --coral:        #FF5F5F;
    --coral-dim:    rgba(255,95,95,0.15);
    --amber:        #FFBE3D;
    --amber-dim:    rgba(255,190,61,0.15);
    --teal:         #3DFFD0;
    --teal-dim:     rgba(61,255,208,0.12);
    --violet:       #B76EFF;
    --violet-dim:   rgba(183,110,255,0.12);
    --sky:          #5BC8FF;
    --sky-dim:      rgba(91,200,255,0.12);
    --text-1:       #F0F4FF;
    --text-2:       #9BAAC8;
    --text-3:       #5A6A8A;
    --border:       #1F2D47;
    --border-bright:#2E4068;
}

html, body, [class*="css"] {
    font-family: 'DM Sans', sans-serif !important;
    background-color: var(--navy) !important;
    color: var(--text-1) !important;
}
.stApp { background: var(--navy) !important; }
.stApp > header { background: transparent !important; }
section[data-testid="stSidebar"] {
    background: var(--navy-mid) !important;
    border-right: 1px solid var(--border) !important;
}
section[data-testid="stSidebar"] * { color: var(--text-1) !important; }

div[data-baseweb="input"] input,
div[data-baseweb="select"] > div,
textarea {
    background: var(--navy-card) !important;
    border-color: var(--border-bright) !important;
    color: var(--text-1) !important;
    border-radius: 8px !important;
}
div[data-baseweb="select"] * {
    color: var(--text-1) !important;
    background: var(--navy-card) !important;
}
ul[role="listbox"] {
    background: var(--navy-card) !important;
    border: 1px solid var(--border-bright) !important;
    border-radius: 10px !important;
}
ul[role="listbox"] li:hover { background: var(--navy-hover) !important; }

div[data-testid="stExpander"] {
    background: var(--navy-card) !important;
    border: 1px solid var(--border) !important;
    border-radius: 12px !important;
    margin-bottom: 10px !important;
    transition: border-color 0.2s, box-shadow 0.2s;
}
div[data-testid="stExpander"]:hover {
    border-color: var(--border-bright) !important;
    box-shadow: 0 4px 24px rgba(255,95,95,0.07) !important;
}
div[data-testid="stExpander"] summary {
    font-family: 'DM Sans', sans-serif !important;
    font-weight: 500 !important;
    font-size: 14px !important;
    color: var(--text-1) !important;
    padding: 14px 18px !important;
}

hr { border-color: var(--border) !important; margin: 24px 0 !important; }

div[data-testid="stMetric"] {
    background: var(--navy-card) !important;
    border: 1px solid var(--border) !important;
    border-radius: 14px !important;
    padding: 18px 20px !important;
    transition: transform 0.15s, box-shadow 0.15s;
}
div[data-testid="stMetric"]:hover {
    transform: translateY(-2px);
    box-shadow: 0 8px 28px rgba(61,255,208,0.1) !important;
}
div[data-testid="stMetricLabel"] {
    font-size: 11px !important;
    font-weight: 600 !important;
    letter-spacing: 0.08em !important;
    text-transform: uppercase !important;
    color: var(--text-2) !important;
}
div[data-testid="stMetricValue"] {
    font-family: 'DM Serif Display', serif !important;
    font-size: 2.2rem !important;
    color: var(--text-1) !important;
}
div[data-testid="stMetricDelta"] { font-size: 12px !important; }

.stButton > button {
    font-family: 'DM Sans', sans-serif !important;
    font-weight: 600 !important;
    letter-spacing: 0.04em !important;
    border-radius: 10px !important;
    border: none !important;
    transition: all 0.2s !important;
}
.stButton > button[kind="primary"] {
    background: linear-gradient(135deg, var(--coral), #FF8C42) !important;
    color: white !important;
    box-shadow: 0 4px 20px rgba(255,95,95,0.35) !important;
}
.stButton > button[kind="primary"]:hover {
    transform: translateY(-2px) scale(1.02) !important;
    box-shadow: 0 8px 32px rgba(255,95,95,0.5) !important;
}
.stButton > button:not([kind="primary"]) {
    background: var(--navy-hover) !important;
    color: var(--text-1) !important;
    border: 1px solid var(--border-bright) !important;
}
.stButton > button:not([kind="primary"]):hover {
    background: var(--navy-card) !important;
    border-color: var(--teal) !important;
    color: var(--teal) !important;
}

div[data-testid="stProgressBar"] > div > div {
    background: linear-gradient(90deg, var(--coral), var(--amber)) !important;
    border-radius: 4px !important;
}

div[data-testid="stDataFrame"] {
    border-radius: 12px !important;
    overflow: hidden !important;
    border: 1px solid var(--border) !important;
}

div[data-baseweb="slider"] [role="slider"] { background: var(--coral) !important; }

code, pre {
    background: #0D1525 !important;
    border: 1px solid var(--border) !important;
    border-radius: 8px !important;
    font-size: 12px !important;
    color: var(--teal) !important;
}

div[data-testid="stDownloadButton"] button {
    background: var(--sky-dim) !important;
    color: var(--sky) !important;
    border: 1px solid rgba(91,200,255,0.3) !important;
    font-size: 12px !important;
}
div[data-testid="stDownloadButton"] button:hover {
    background: rgba(91,200,255,0.2) !important;
    border-color: var(--sky) !important;
    transform: none !important;
}

.stCaption { color: var(--text-3) !important; font-size: 12px !important; }

#MainMenu, footer { visibility: hidden; }
.viewerBadge_container__1QSob { display: none !important; }

/* ─── Custom components ─── */

.page-header {
    font-family: 'DM Serif Display', serif;
    font-size: 2.4rem;
    color: var(--text-1);
    margin-bottom: 4px;
    line-height: 1.2;
}
.page-sub {
    font-size: 12px;
    color: var(--text-3);
    letter-spacing: 0.08em;
    text-transform: uppercase;
    margin-bottom: 28px;
    font-weight: 500;
}
.section-title {
    font-family: 'DM Serif Display', serif;
    font-size: 1.25rem;
    color: var(--text-1);
    margin: 24px 0 14px;
}
.sidebar-logo {
    font-family: 'DM Serif Display', serif;
    font-size: 1.5rem;
    color: var(--text-1);
    line-height: 1.3;
    padding: 8px 0 20px;
}
.sidebar-logo span { color: var(--coral); }
.sys-card {
    background: var(--navy-card);
    border: 1px solid var(--border);
    border-radius: 10px;
    padding: 14px 16px;
    font-size: 12px;
}
.sys-label {
    color: var(--text-3);
    text-transform: uppercase;
    letter-spacing: 0.08em;
    font-size: 10px;
    font-weight: 600;
    margin-bottom: 8px;
}
.badge {
    display: inline-block;
    padding: 3px 11px;
    border-radius: 20px;
    font-size: 11px;
    font-weight: 600;
    letter-spacing: 0.04em;
    text-transform: uppercase;
    margin-right: 5px;
    margin-bottom: 3px;
}
.b-coral  { background: var(--coral-dim);  color: var(--coral);  border: 1px solid rgba(255,95,95,0.3); }
.b-amber  { background: var(--amber-dim);  color: var(--amber);  border: 1px solid rgba(255,190,61,0.3); }
.b-teal   { background: var(--teal-dim);   color: var(--teal);   border: 1px solid rgba(61,255,208,0.3); }
.b-violet { background: var(--violet-dim); color: var(--violet); border: 1px solid rgba(183,110,255,0.3); }
.b-sky    { background: var(--sky-dim);    color: var(--sky);    border: 1px solid rgba(91,200,255,0.3); }
.b-gray   { background: rgba(30,42,66,0.6); color: var(--text-2); border: 1px solid var(--border); }

.chase-alert {
    background: linear-gradient(135deg, #1a1400, #1e1800);
    border: 1px solid rgba(255,190,61,0.3);
    border-left: 4px solid var(--amber);
    border-radius: 0 10px 10px 0;
    padding: 12px 16px;
    margin: 5px 0;
    font-size: 13px;
    display: flex;
    align-items: center;
    gap: 12px;
    transition: background 0.15s;
}
.chase-alert:hover { background: linear-gradient(135deg, #221a00, #2a2000); }
.chase-vendor { font-weight: 600; color: var(--amber); min-width: 150px; flex-shrink: 0; }
.chase-subject { color: var(--text-2); flex: 1; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
.chase-pill {
    background: var(--amber-dim);
    color: var(--amber);
    padding: 3px 10px;
    border-radius: 10px;
    font-size: 11px;
    font-weight: 600;
    white-space: nowrap;
    flex-shrink: 0;
}

.vendor-row {
    display: flex;
    align-items: center;
    gap: 14px;
    padding: 11px 16px;
    border-radius: 10px;
    background: var(--navy-card);
    border: 1px solid var(--border);
    margin: 5px 0;
    transition: border-color 0.2s, background 0.15s;
    font-size: 13px;
}
.vendor-row:hover { border-color: var(--border-bright); background: var(--navy-hover); }
.vendor-name { font-weight: 600; color: var(--text-1); min-width: 160px; flex-shrink: 0; }
.vendor-threads { font-weight: 700; color: var(--sky); font-size: 15px; }
.vendor-reply { font-weight: 700; font-size: 14px; }
.vendor-date { margin-left: auto; color: var(--text-3); font-size: 11px; }

.detail-pill {
    display: inline-flex;
    align-items: center;
    gap: 5px;
    background: var(--navy-hover);
    border: 1px solid var(--border);
    border-radius: 7px;
    padding: 4px 11px;
    font-size: 12px;
    color: var(--text-2);
    margin: 3px 3px 3px 0;
}
.detail-pill b { color: var(--text-1); }

.ov-bullet {
    padding: 7px 0;
    border-bottom: 1px solid var(--border);
    font-size: 13.5px;
    color: var(--text-2);
    line-height: 1.7;
}
.ov-bullet:last-child { border-bottom: none; }

.ship-card {
    background: linear-gradient(135deg, #0a1a2a, #0c1e30);
    border: 1px solid rgba(61,255,208,0.2);
    border-radius: 10px;
    padding: 14px 18px;
    font-size: 13px;
}
.ship-carrier { color: var(--teal); font-weight: 600; font-size: 14px; }
.ship-awb { color: var(--text-1); font-family: 'Courier New', monospace; font-size: 13px; margin-top: 4px; }
.ship-date { color: var(--text-3); font-size: 11px; margin-top: 4px; }

.reply-label {
    font-size: 10px;
    font-weight: 700;
    letter-spacing: 0.12em;
    text-transform: uppercase;
    color: var(--sky);
    margin-bottom: 6px;
}
.reply-box {
    background: linear-gradient(135deg, #081830, #0c1f38);
    border: 1px solid rgba(91,200,255,0.25);
    border-left: 4px solid var(--sky);
    border-radius: 0 12px 12px 0;
    padding: 20px 24px;
    font-family: 'DM Sans', sans-serif;
    font-size: 13.5px;
    line-height: 1.85;
    color: var(--text-1);
    white-space: pre-wrap;
    margin: 8px 0 12px;
}

.ss-dispatched { background: var(--amber-dim)!important;  color: var(--amber)!important;  border-color: rgba(255,190,61,0.3)!important; }
.ss-received   { background: var(--teal-dim)!important;   color: var(--teal)!important;   border-color: rgba(61,255,208,0.3)!important; }
.ss-approved   { background: var(--teal-dim)!important;   color: var(--teal)!important;   border-color: rgba(61,255,208,0.3)!important; }
.ss-rejected   { background: var(--coral-dim)!important;  color: var(--coral)!important;  border-color: rgba(255,95,95,0.3)!important; }
.ss-pending    { background: var(--violet-dim)!important; color: var(--violet)!important; border-color: rgba(183,110,255,0.3)!important; }

.log-area {
    background: #050c18;
    border: 1px solid var(--border);
    border-radius: 10px;
    padding: 14px 18px;
    font-family: 'Courier New', monospace;
    font-size: 12px;
    color: var(--teal);
    line-height: 1.8;
    min-height: 120px;
    max-height: 200px;
    overflow-y: auto;
}

.config-card {
    background: var(--navy-card);
    border: 1px solid var(--border);
    border-top: 3px solid var(--teal);
    border-radius: 12px;
    padding: 20px 24px;
    margin-bottom: 22px;
}
.config-label {
    font-size: 10px;
    color: var(--text-3);
    text-transform: uppercase;
    letter-spacing: 0.1em;
    font-weight: 600;
    margin-bottom: 14px;
}
.filter-card {
    background: var(--navy-card);
    border: 1px solid var(--border);
    border-radius: 14px;
    padding: 18px 22px;
    margin-bottom: 20px;
}
.filter-label {
    font-size: 10px;
    color: var(--text-3);
    text-transform: uppercase;
    letter-spacing: 0.1em;
    font-weight: 600;
    margin-bottom: 14px;
}
</style>
""", unsafe_allow_html=True)


# ── Session state ──────────────────────────────────────────────────────────────
if "df_logs" not in st.session_state:
    st.session_state.df_logs = None


# ── Helpers ───────────────────────────────────────────────────────────────────

def get_google_services():
    creds      = authenticate()
    gmail_svc  = build("gmail",  "v1", credentials=creds)
    sheets_svc = build("sheets", "v4", credentials=creds)
    return gmail_svc, sheets_svc, creds


def load_data_from_sheets() -> pd.DataFrame:
    _, sheets_svc, _ = get_google_services()
    end_col = _col_letter_n(len(LOGS_HEADERS))
    try:
        result = sheets_svc.spreadsheets().values().get(
            spreadsheetId=SHEET_ID,
            range=f"'{SHEET_TAB}'!A:{end_col}"
        ).execute()
        rows = result.get("values", [])
        if len(rows) > 1:
            headers = rows[0]
            data    = rows[1:]
            padded  = [r + [""] * (len(headers) - len(r)) for r in data]
            df = pd.DataFrame(padded, columns=headers)
            st.session_state.df_logs = df
            return df
        return pd.DataFrame()
    except Exception as e:
        st.error(f"Failed to load data: {e}")
        return pd.DataFrame()


def fetch_all_metadata_safe(creds, thread_ids: list) -> dict:
    from concurrent.futures import ThreadPoolExecutor, as_completed
    import re
    from gmail_reader import gmail_threads_get, GMAIL_WORKERS

    def _fetch_one(cred, tid):
        svc = build("gmail", "v1", credentials=cred)
        try:
            meta        = gmail_threads_get(svc, userId="me", id=tid,
                                            format="metadata", metadataHeaders=["Subject"])
            msgs        = meta.get("messages", [])
            raw_subject = ""
            if msgs:
                for h in msgs[0].get("payload", {}).get("headers", []):
                    if h["name"].lower() == "subject":
                        raw_subject = h["value"]
                        break
            clean = re.sub(r'^(re|fwd|fw):\s*', '', raw_subject, flags=re.IGNORECASE).strip()
            return {"tid": tid, "count": len(msgs), "subject": clean, "error": None}
        except Exception as e:
            return {"tid": tid, "count": 0, "subject": "", "error": str(e)}

    results = {}
    with ThreadPoolExecutor(max_workers=GMAIL_WORKERS) as ex:
        futures = {ex.submit(_fetch_one, creds, tid): tid for tid in thread_ids}
        for fut in as_completed(futures):
            m = fut.result()
            results[m["tid"]] = m
    return results


def _safe(row, col, default=""):
    v = row.get(col, default)
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return default
    return str(v).strip() or default


def _intent_badge(intent: str) -> str:
    cls_map = {
        "chase":   "b-amber",
        "delay":   "b-coral",
        "quality": "b-coral",
        "approve": "b-teal",
        "review":  "b-teal",
        "track":   "b-sky",
        "await":   "b-sky",
        "confirm": "b-violet",
        "payment": "b-coral",
        "no action": "b-gray",
        "other":   "b-gray",
    }
    cls = "b-gray"
    for k, v in cls_map.items():
        if k in intent.lower():
            cls = v
            break
    return f'<span class="badge {cls}">{intent}</span>'


def _sample_pill(status: str) -> str:
    icons = {"Dispatched": "📤", "Received": "📥", "Approved": "✅",
             "Rejected": "❌", "Pending": "⏳", "None": "—"}
    cls_map = {"Dispatched": "ss-dispatched", "Received": "ss-received",
               "Approved": "ss-approved",     "Rejected": "ss-rejected",
               "Pending": "ss-pending"}
    icon = icons.get(status, "")
    cls  = cls_map.get(status, "")
    base = f'class="detail-pill {cls}"' if cls else 'class="detail-pill"'
    return f'<span {base}>{icon} {status}</span>'


def _dpill(label: str, value: str, color: str = "") -> str:
    if not value:
        return ""
    c = {"coral": "var(--coral)", "teal": "var(--teal)", "amber": "var(--amber)",
         "sky": "var(--sky)", "violet": "var(--violet)"}.get(color, "var(--text-1)")
    return f'<span class="detail-pill">{label}: <b style="color:{c}">{value}</b></span>'


def _opts(df, col):
    if col not in df.columns:
        return ["All"]
    return ["All"] + sorted(df[col].dropna().replace("", pd.NA).dropna().unique().tolist())


# ── Sidebar ───────────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("""
    <div class="sidebar-logo">
        Merch<span>.</span>AI<br>
        <span style="font-family:'DM Sans',sans-serif;font-size:11px;
              color:#5A6A8A;font-weight:400;letter-spacing:0.12em;
              text-transform:uppercase;">Merchandising Agent</span>
    </div>
    """, unsafe_allow_html=True)

    # Jump navigation — set by clickable dashboard metrics
    if "jump_to_viewer" not in st.session_state:
        st.session_state.jump_to_viewer  = False
    if "jump_sample_filter" not in st.session_state:
        st.session_state.jump_sample_filter = "All"

    _page_options = ["📊  Dashboard", "🔄  Sync Gmail", "🗂️  Thread Viewer"]
    _default_page = 2 if st.session_state.jump_to_viewer else 0
    page = st.radio("", _page_options, index=_default_page,
                    label_visibility="collapsed")
    # Clear jump flag once page radio has consumed it
    if st.session_state.jump_to_viewer and "Thread Viewer" in page:
        st.session_state.jump_to_viewer = False

    st.markdown("<br>", unsafe_allow_html=True)
    st.markdown('<p style="font-size:10px;color:#5A6A8A;text-transform:uppercase;letter-spacing:0.1em;font-weight:600;margin-bottom:8px;">Quick Actions</p>', unsafe_allow_html=True)
    if st.button("🔃 Refresh Data", use_container_width=True):
        st.session_state.df_logs = None
        st.rerun()

    st.markdown("<br>", unsafe_allow_html=True)
    cs = cache_stats()
    from gmail_reader import _run_token_usage
    _tok_in  = _run_token_usage.get("prompt_tokens", 0)
    _tok_out = _run_token_usage.get("completion_tokens", 0)
    _tok_total = _tok_in + _tok_out
    _tok_str = f"{_tok_total:,}" if _tok_total else "—"
    st.markdown(f"""
    <div class="sys-card">
        <div class="sys-label">System Status</div>
        <div style="color:#9BAAC8;margin-bottom:5px;">
            💾 <b style="color:#3DFFD0">{cs.get('cached_entries', 0)}</b> cached threads
        </div>
        <div style="color:#9BAAC8;margin-bottom:5px;">
            🔢 <b style="color:#FFD166">{_tok_str}</b> tokens this run
            <span style="font-size:11px;color:#5A6785;">({_tok_in:,} in / {_tok_out:,} out)</span>
        </div>
        <div style="color:#9BAAC8;">
            🤖 <b style="color:#B76EFF">GPT-5.4</b> active
        </div>
    </div>
    """, unsafe_allow_html=True)


# ════════════════════════════════════════════════════════════════════════════════
# PAGE: DASHBOARD
# ════════════════════════════════════════════════════════════════════════════════
if "Dashboard" in page:
    st.markdown('<div class="page-header">Overview Dashboard</div>', unsafe_allow_html=True)
    st.markdown('<div class="page-sub">Real-time merchandising intelligence</div>', unsafe_allow_html=True)

    df = st.session_state.df_logs if st.session_state.df_logs is not None else load_data_from_sheets()

    if df.empty:
        st.info("No data yet — head to **Sync Gmail** to pull your first threads.")
        st.stop()

    # ── KPI row ───────────────────────────────────────────────────────────────
    k1, k2, k3, k4, k5, k6 = st.columns(6)

    total = len(df)
    k1.metric("Total Threads", total)

    needs_reply = int((df.get("Reply Needed", pd.Series(dtype=str)) == "Yes").sum())
    k2.metric("Needs Reply", needs_reply,
              delta=f"{round(needs_reply / max(total, 1) * 100)}% of threads",
              delta_color="off")

    chase_n = int(df.get("Sample Reminder", pd.Series(dtype=str)).str.startswith("⚠️", na=False).sum())
    k3.metric("⚠️ Chase Alerts", chase_n,
              delta="Action required" if chase_n else "All clear",
              delta_color="inverse")

    po_ct = int((df.get("PO Number", pd.Series(dtype=str)).replace("", pd.NA).notna()).sum())
    k4.metric("Threads with PO", po_ct)

    dispatched = int((df.get("Sample Status", pd.Series(dtype=str)) == "Dispatched").sum())
    k5.metric("Dispatched Samples", dispatched)
    # Small "view" link below the metric — styled as an unobtrusive link
    k5.markdown("""<style>
        div[data-testid="column"]:nth-child(5) button[kind="secondary"] {
            background: none !important; border: none !important;
            color: #3DFFD0 !important; font-size: 11px !important;
            padding: 0 !important; margin-top: -6px !important;
            text-decoration: underline !important; cursor: pointer !important;
            box-shadow: none !important;
        }
    </style>""", unsafe_allow_html=True)
    if k5.button("↗ View threads", key="dispatched_jump", use_container_width=False):
        st.session_state.jump_to_viewer     = True
        st.session_state.jump_sample_filter = "Dispatched"
        st.rerun()

    if "Vendor Name" in df.columns:
        top_v = df[df["Vendor Name"].replace("", pd.NA).notna()]["Vendor Name"].value_counts()
        top_vendor_full = top_v.index[0] if not top_v.empty else "—"
        k6.metric(
            "Top Vendor",
            top_vendor_full,
            delta=f"{top_v.iloc[0]} threads" if not top_v.empty else "",
            help=top_vendor_full,   # hover tooltip shows full name even if truncated
        )

    st.markdown("<br>", unsafe_allow_html=True)

    # ── Chase alerts ──────────────────────────────────────────────────────────
    if "Sample Reminder" in df.columns:
        chase_df = df[df["Sample Reminder"].str.startswith("⚠️", na=False)]
        if not chase_df.empty:
            st.markdown(
                f'<div class="section-title">⚠️ Chase Alerts '
                f'<span style="font-family:\'DM Sans\',sans-serif;font-size:14px;'
                f'color:var(--amber);font-weight:400;">— {len(chase_df)} threads need attention</span></div>',
                unsafe_allow_html=True
            )
            for _, row in chase_df.iterrows():
                st.markdown(f"""
                <div class="chase-alert">
                    <span class="chase-vendor">{_safe(row,'Vendor Name','Unknown')}</span>
                    <span class="chase-subject">{_safe(row,'Subject','')[:65]}</span>
                    <span class="chase-pill">{_safe(row,'Sample Reminder','')}</span>
                </div>
                """, unsafe_allow_html=True)
            st.markdown("<br>", unsafe_allow_html=True)

    # ── Charts ────────────────────────────────────────────────────────────────
    ca, cb, cc = st.columns(3)
    with ca:
        st.markdown('<div class="section-title">By Intent</div>', unsafe_allow_html=True)
        if "Intent" in df.columns:
            ic = df["Intent"].value_counts().reset_index()
            ic.columns = ["Intent", "Count"]
            st.bar_chart(ic.set_index("Intent"), color="#FF5F5F", height=260)

    with cb:
        st.markdown('<div class="section-title">By Division</div>', unsafe_allow_html=True)
        if "Division" in df.columns:
            dc = df["Division"].value_counts().reset_index()
            dc.columns = ["Division", "Count"]
            st.bar_chart(dc.set_index("Division"), color="#3DFFD0", height=260)

    with cc:
        st.markdown('<div class="section-title">Sample Pipeline</div>', unsafe_allow_html=True)
        if "Sample Status" in df.columns:
            sc = df["Sample Status"].value_counts().reset_index()
            sc.columns = ["Status", "Count"]
            st.bar_chart(sc.set_index("Status"), color="#FFBE3D", height=260)

    st.divider()

    # ── Recent activity ───────────────────────────────────────────────────────
    st.markdown('<div class="section-title">Recent Activity</div>', unsafe_allow_html=True)
    show_cols = [c for c in
        ["Sent Date", "Vendor Name", "Subject", "Division", "Intent",
         "Reply Needed", "Sample Status", "PO Number", "AWB No"]
        if c in df.columns]
    recent = df.sort_values("Sent Date", ascending=False).head(10) if "Sent Date" in df.columns else df.head(10)
    st.dataframe(recent[show_cols], use_container_width=True, hide_index=True)

    st.divider()

    # ── Vendor breakdown ──────────────────────────────────────────────────────
    if "Vendor Name" in df.columns:
        st.markdown('<div class="section-title">Vendor Breakdown</div>', unsafe_allow_html=True)
        vdf = df[df["Vendor Name"].replace("", pd.NA).notna()]
        if not vdf.empty:
            vstats = (
                vdf.groupby("Vendor Name")
                .agg(Threads=("Subject", "count"),
                     Reply_Needed=("Reply Needed", lambda x: (x == "Yes").sum()),
                     Latest=("Sent Date", "max"))
                .sort_values("Threads", ascending=False)
                .head(12)
                .reset_index()
            )
            for _, vrow in vstats.iterrows():
                rn  = int(vrow["Reply_Needed"])
                col = "var(--coral)" if rn > 0 else "var(--teal)"
                st.markdown(f"""
                <div class="vendor-row">
                    <span class="vendor-name">{vrow['Vendor Name']}</span>
                    <span style="color:#9BAAC8;font-size:12px;">
                        <span class="vendor-threads">{vrow['Threads']}</span> threads
                    </span>
                    <span style="color:#9BAAC8;font-size:12px;">
                        Reply needed: <span class="vendor-reply" style="color:{col}">{rn}</span>
                    </span>
                    <span class="vendor-date">Last: {str(vrow.get('Latest',''))[:10]}</span>
                </div>
                """, unsafe_allow_html=True)


# ════════════════════════════════════════════════════════════════════════════════
# PAGE: SYNC GMAIL
# ════════════════════════════════════════════════════════════════════════════════
elif "Sync" in page:
    st.markdown('<div class="page-header">Sync Gmail Inbox</div>', unsafe_allow_html=True)
    st.markdown('<div class="page-sub">Pull threads · Analyse with GPT-5.4 · Write to Sheets</div>', unsafe_allow_html=True)

    st.markdown('<div class="config-card"><div class="config-label">Sync Configuration</div>', unsafe_allow_html=True)
    cfg1, cfg2, cfg3 = st.columns([3, 1, 1])
    with cfg1:
        max_threads = st.slider("Threads to fetch", 1, 200, 20,
                                help="How many inbox threads to process this run")
    with cfg2:
        st.markdown("<br>", unsafe_allow_html=True)
        dry_run = st.checkbox("🔍 Dry Run", help="Analyse but don't write to Sheets")
    with cfg3:
        st.markdown("<br><br>", unsafe_allow_html=True)
        if dry_run:
            st.markdown('<span class="badge b-amber">READ ONLY</span>', unsafe_allow_html=True)
        else:
            st.markdown('<span class="badge b-teal">WRITE MODE</span>', unsafe_allow_html=True)
    st.markdown("</div>", unsafe_allow_html=True)

    btn_col, tip_col = st.columns([1, 3])
    with btn_col:
        start = st.button("🚀 Start Sync", type="primary", use_container_width=True)
    with tip_col:
        st.markdown('<br><span style="color:#5A6A8A;font-size:13px;">GPT-5.4 analyses each thread once and caches the result — repeated runs are fast and free.</span>', unsafe_allow_html=True)

    if start:
        ph = st.empty()
        ph.markdown('<div style="color:var(--teal);font-size:13px;padding:8px 0;">⚡ Connecting…</div>', unsafe_allow_html=True)

        gmail_svc, sheets_svc, creds = get_google_services()
        gmail_reader.DRY_RUN = dry_run
        vendor_db = load_vendor_db(sheets_svc)
        if not dry_run:
            _ensure_tab(sheets_svc, SHEET_TAB, LOGS_HEADERS)
            _ensure_tab(sheets_svc, ERROR_TAB, ERROR_HEADERS)
        thread_map, subject_map = load_existing_rows(sheets_svc)

        ph.markdown('<div style="color:var(--teal);font-size:13px;padding:8px 0;">✅ Connected. Fetching thread list…</div>', unsafe_allow_html=True)
        result        = gmail_svc.users().threads().list(userId="me", maxResults=max_threads, q="in:inbox").execute()
        gmail_threads = result.get("threads", [])
        if not gmail_threads:
            st.info("No threads found in inbox.")
            st.stop()

        ph.markdown(f'<div style="color:var(--teal);font-size:13px;padding:8px 0;">✅ {len(gmail_threads)} threads found. Fetching metadata…</div>', unsafe_allow_html=True)
        all_tids = [t["id"] for t in gmail_threads]
        meta_map = fetch_all_metadata_safe(creds, all_tids)
        ph.empty()

        progress_bar = st.progress(0)
        st.markdown('<div style="font-size:12px;color:#5A6A8A;margin:-8px 0 10px;">Processing threads…</div>', unsafe_allow_html=True)
        log_ph = st.empty()

        new_rows  = []
        updated   = backfilled = added = skipped = errors = 0
        total     = len(gmail_threads)
        now_str   = datetime.now().strftime("%Y-%m-%d %H:%M")
        log_lines = []

        for i, thread in enumerate(gmail_threads, 1):
            tid        = thread["id"]
            meta       = meta_map.get(tid, {})
            curr_count = meta.get("count",   0)
            meta_subj  = meta.get("subject", "")
            subj_key   = meta_subj.lower()
            status_msg = ""

            progress_bar.progress(int(i / total * 100),
                                  text=f"[{i}/{total}]  {meta_subj[:60]}")
            try:
                if tid in thread_map:
                    existing  = thread_map[tid]
                    old_count = existing["message_count"]
                    if curr_count <= old_count:
                        skipped   += 1
                        status_msg = f"⏭  No change · {meta_subj[:48]}"
                    else:
                        row_data = process_thread(gmail_svc, tid, vendor_db)
                        if row_data:
                            update_existing_row(sheets_svc, existing["sheet_row"], row_data)
                            thread_map[tid]["message_count"] = row_data["Thread Messages"]
                            updated   += 1
                            status_msg = f"🔄  Updated · {row_data['Subject'][:48]}"

                elif subj_key and subj_key in subject_map:
                    existing  = subject_map[subj_key]
                    old_count = existing["message_count"]
                    if curr_count <= old_count:
                        if not dry_run:
                            sheets_svc.spreadsheets().values().batchUpdate(
                                spreadsheetId=SHEET_ID,
                                body={"valueInputOption": "RAW", "data": [
                                    {"range": f"'{SHEET_TAB}'!{col_letter('Thread ID')}{existing['sheet_row']}",
                                     "values": [[tid]]},
                                    {"range": f"'{SHEET_TAB}'!{col_letter('Thread Messages')}{existing['sheet_row']}",
                                     "values": [[str(curr_count)]]},
                                ]}
                            ).execute()
                        skipped    += 1
                        backfilled += 1
                        status_msg  = f"🔗  TID stored · {meta_subj[:48]}"
                    else:
                        row_data = process_thread(gmail_svc, tid, vendor_db)
                        if row_data:
                            update_existing_row(sheets_svc, existing["sheet_row"], row_data, backfill_tid=True)
                            thread_map[tid] = existing
                            thread_map[tid]["message_count"] = row_data["Thread Messages"]
                            updated    += 1
                            backfilled += 1
                            status_msg  = f"🔄  Updated+TID · {row_data['Subject'][:42]}"
                else:
                    if curr_count < 2:
                        skipped   += 1
                        status_msg = f"⏭  Single msg · {meta_subj[:48]}"
                    else:
                        row_data = process_thread(gmail_svc, tid, vendor_db)
                        if row_data is None:
                            skipped   += 1
                            status_msg = f"⏭  No body · {meta_subj[:50]}"
                        else:
                            new_rows.append(_build_new_row(row_data, now_str))
                            subject_map[row_data["Subject"].lower()] = {
                                "sheet_row": None, "message_count": curr_count,
                                "thread_id": tid,  "subject": row_data["Subject"],
                            }
                            added     += 1
                            status_msg = f"🆕  Added · {row_data['Subject'][:48]}"
            except Exception as exc:
                errors    += 1
                record_error(tid, meta_subj, "streamlit_sync", exc)
                status_msg = f"❌  Error · {meta_subj[:40]} — {str(exc)[:45]}"

            log_lines.append(status_msg)
            if i % 2 == 0 or i == total:
                def _lcolor(l):
                    if "🆕" in l:  return "#3DFFD0"
                    if "🔄" in l:  return "#5BC8FF"
                    if "❌" in l:  return "#FF5F5F"
                    return "#FFBE3D"
                html = "".join(f'<div style="color:{_lcolor(l)}">{l}</div>' for l in log_lines[-10:])
                log_ph.markdown(f'<div class="log-area">{html}</div>', unsafe_allow_html=True)

        if not dry_run:
            if new_rows:
                append_new_rows(sheets_svc, new_rows)
            flush_error_log(sheets_svc)

        cs2 = cache_stats()
        write_audit_log({
            "threads_fetched": total, "added": added, "updated": updated,
            "backfilled": backfilled, "skipped": skipped, "errors": errors,
            "cache_entries": cs2.get("cached_entries", 0),
        })

        progress_bar.progress(100, text="✅ Sync complete!")
        st.markdown("<br>", unsafe_allow_html=True)
        if dry_run:
            st.info("🔍 DRY RUN — nothing written to Google Sheets.")
        else:
            st.success("✅ Sync complete — Google Sheet updated.")

        r1, r2, r3, r4, r5 = st.columns(5)
        r1.metric("🆕 Added",      added)
        r2.metric("🔄 Updated",    updated)
        r3.metric("🔗 Backfilled", backfilled)
        r4.metric("⏭️ Skipped",    skipped)
        r5.metric("❌ Errors",     errors)
        st.session_state.df_logs = None


# ════════════════════════════════════════════════════════════════════════════════
# PAGE: THREAD VIEWER
# ════════════════════════════════════════════════════════════════════════════════
elif "Thread" in page:
    st.markdown('<div class="page-header">Thread Viewer</div>', unsafe_allow_html=True)
    st.markdown('<div class="page-sub">Browse · Filter · Review · Reply</div>', unsafe_allow_html=True)

    df = st.session_state.df_logs if st.session_state.df_logs is not None else load_data_from_sheets()

    if df.empty:
        st.info("No data found. Run a sync first.")
        st.stop()

    # ── Filters ───────────────────────────────────────────────────────────────
    st.markdown('<div class="filter-card"><div class="filter-label">Filters</div>', unsafe_allow_html=True)
    fa, fb, fc, fd, fe = st.columns(5)
    with fa: intent_f = st.selectbox("Intent",        _opts(df, "Intent"))
    with fb: reply_f  = st.selectbox("Reply Needed",  ["All", "Yes", "No"])
    with fc: vendor_f = st.selectbox("Vendor",        _opts(df, "Vendor Name"))
    with fd: div_f    = st.selectbox("Division",      _opts(df, "Division"))
    # Pre-select from jump navigation if set
    _sample_opts    = _opts(df, "Sample Status")
    _jump_sample    = st.session_state.get("jump_sample_filter", "All")
    _sample_default = _sample_opts.index(_jump_sample) if _jump_sample in _sample_opts else 0
    with fe: sample_f = st.selectbox("Sample Status", _sample_opts, index=_sample_default)
    # Consume the jump so it doesn't persist on next page switch
    st.session_state.jump_sample_filter = "All"
    st.markdown("</div>", unsafe_allow_html=True)

    fdf = df.copy()
    if intent_f  != "All" and "Intent"        in fdf.columns: fdf = fdf[fdf["Intent"]        == intent_f]
    if reply_f   != "All" and "Reply Needed"  in fdf.columns: fdf = fdf[fdf["Reply Needed"]  == reply_f]
    if vendor_f  != "All" and "Vendor Name"   in fdf.columns: fdf = fdf[fdf["Vendor Name"]   == vendor_f]
    if div_f     != "All" and "Division"      in fdf.columns: fdf = fdf[fdf["Division"]      == div_f]
    if sample_f  != "All" and "Sample Status" in fdf.columns: fdf = fdf[fdf["Sample Status"] == sample_f]

    st.markdown(
        f'<p style="color:#5A6A8A;font-size:12px;margin-bottom:16px;">'
        f'Showing <b style="color:#3DFFD0">{len(fdf)}</b> of '
        f'<b style="color:#9BAAC8">{len(df)}</b> threads</p>',
        unsafe_allow_html=True
    )

    # ── Thread cards ──────────────────────────────────────────────────────────
    for idx, row in fdf.iterrows():
        vendor   = _safe(row, "Vendor Name", "Unknown Vendor")
        subject  = _safe(row, "Subject",     "No Subject")
        sent     = _safe(row, "Sent Date",   "")[:10]
        intent   = _safe(row, "Intent",      "")
        rn       = _safe(row, "Reply Needed","No")
        reminder = _safe(row, "Sample Reminder", "")
        ss       = _safe(row, "Sample Status",   "None")

        pfx = "⚠️ " if reminder.startswith("⚠️") else ("🔴 " if rn == "Yes" else "")
        exp_label = f"{pfx}{sent}  ·  {vendor}  ·  {subject[:65]}"

        with st.expander(exp_label, expanded=False):

            # Header badges
            bdg  = _intent_badge(intent)
            bdg += (' <span class="badge b-coral">⚠ Reply Needed</span>'
                    if rn == "Yes"
                    else ' <span class="badge b-teal">✓ No Reply</span>')
            if reminder.startswith("⚠️"):
                bdg += f' <span class="badge b-amber">{reminder}</span>'
            st.markdown(f'<div style="margin-bottom:18px;">{bdg}</div>', unsafe_allow_html=True)

            col_ov, col_meta, col_right = st.columns([5, 3, 3])

            # AI Overview
            with col_ov:
                st.markdown('<div style="font-size:10px;color:#5A6A8A;text-transform:uppercase;letter-spacing:0.1em;font-weight:600;margin-bottom:10px;">AI Overview</div>', unsafe_allow_html=True)
                overview = _safe(row, "AI Overview", "")
                lines    = [l.strip() for l in overview.split("\n") if l.strip()]
                if lines:
                    bhtml = "".join(f'<div class="ov-bullet">• {l.lstrip("•–- ")}</div>' for l in lines)
                    st.markdown(bhtml, unsafe_allow_html=True)
                else:
                    st.markdown('<span style="color:#5A6A8A;font-size:13px;">No overview available.</span>', unsafe_allow_html=True)

            # Thread details
            with col_meta:
                st.markdown('<div style="font-size:10px;color:#5A6A8A;text-transform:uppercase;letter-spacing:0.1em;font-weight:600;margin-bottom:10px;">Thread Details</div>', unsafe_allow_html=True)
                pills_html = "".join(filter(None, [
                    _dpill("Division", _safe(row, "Division"),     "sky"),
                    _dpill("Style",    _safe(row, "Style No"),     "violet"),
                    _dpill("Colour",   _safe(row, "Colour"),       "coral"),
                    _dpill("PO",       _safe(row, "PO Number"),    "amber"),
                    _dpill("Msgs",     _safe(row, "Thread Messages")),
                    _dpill("Sent",     sent),
                    _dpill("Vendor",   vendor, "sky"),
                    _dpill("Class",    _safe(row, "Partner Classification")),
                    _dpill("CC",       _safe(row, "CC")),
                ]))
                st.markdown(pills_html, unsafe_allow_html=True)

                att   = _safe(row, "Attachments",  "")
                links = _safe(row, "Shared Links", "")
                if att:
                    st.markdown(f'<div style="margin-top:10px;font-size:11px;color:#5A6A8A;">📎 {att}</div>', unsafe_allow_html=True)
                if links:
                    st.markdown(f'<div style="font-size:11px;color:#5BC8FF;margin-top:4px;">🔗 {links}</div>', unsafe_allow_html=True)

            # Shipment + Sample
            with col_right:
                st.markdown('<div style="font-size:10px;color:#5A6A8A;text-transform:uppercase;letter-spacing:0.1em;font-weight:600;margin-bottom:10px;">Shipment &amp; Sample</div>', unsafe_allow_html=True)
                awb     = _safe(row, "AWB No",          "")
                carrier = _safe(row, "Shipment Company","")
                ship_dt = _safe(row, "Shipment Date",   "")
                if awb or carrier:
                    st.markdown(f"""
                    <div class="ship-card">
                        <div class="ship-carrier">📦 {carrier or 'Unknown carrier'}</div>
                        <div class="ship-awb">{awb or 'No AWB yet'}</div>
                        {"" if not ship_dt else f'<div class="ship-date">Shipped: {ship_dt}</div>'}
                    </div>
                    """, unsafe_allow_html=True)
                else:
                    st.markdown('<div style="color:#5A6A8A;font-size:12px;padding:8px 0;">No shipment info yet.</div>', unsafe_allow_html=True)

                st.markdown('<div style="margin-top:14px;font-size:12px;color:#5A6A8A;margin-bottom:6px;">Sample Status</div>', unsafe_allow_html=True)
                st.markdown(_sample_pill(ss), unsafe_allow_html=True)

            # Reply Draft
            reply = _safe(row, "Reply Draft", "")
            if reply:
                st.markdown("<br>", unsafe_allow_html=True)
                st.markdown(f'<div class="reply-label">✉ Suggested Reply Draft</div><div class="reply-box">{reply}</div>', unsafe_allow_html=True)
                dl, _ = st.columns([1, 4])
                with dl:
                    st.download_button(
                        label="📋 Download reply",
                        data=reply,
                        file_name=f"reply_{subject[:25].replace(' ', '_')}.txt",
                        mime="text/plain",
                        key=f"dl_{idx}",
                    )
            else:
                st.markdown('<div style="color:#5A6A8A;font-size:12px;margin-top:10px;">No reply draft — this thread is marked as no reply needed.</div>', unsafe_allow_html=True)