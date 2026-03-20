"""
Merchandising AI Agent — Streamlit Frontend  v4.0
═══════════════════════════════════════════════════
Redesigned UI/UX — all functions and connections preserved from v3.0
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
    page_title="Merch AI",
    page_icon="◈",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Master CSS ────────────────────────────────────────────────────────────────
st.markdown("""
<link href="https://fonts.googleapis.com/css2?family=Syne:wght@400;500;600;700;800&family=Instrument+Sans:ital,wght@0,400;0,500;0,600;1,400&family=JetBrains+Mono:wght@400;500&display=swap" rel="stylesheet">

<style>
:root {
    --ink:         #0C0C0E;
    --ink-mid:     #111115;
    --ink-card:    #16161C;
    --ink-hover:   #1E1E28;
    --ink-raised:  #1A1A22;
    --line:        #232330;
    --line-hi:     #32324A;

    --acid:        #C8FF00;
    --acid-dim:    rgba(200,255,0,0.1);
    --flame:       #FF4500;
    --flame-dim:   rgba(255,69,0,0.1);
    --ice:         #00E5FF;
    --ice-dim:     rgba(0,229,255,0.08);
    --gold:        #FFB800;
    --gold-dim:    rgba(255,184,0,0.1);
    --lilac:       #B794FF;
    --lilac-dim:   rgba(183,148,255,0.1);

    --t1: #F4F4F8;
    --t2: #8A8AA8;
    --t3: #44445A;
    --t4: #2A2A3A;

    --r: 8px;
    --r-lg: 14px;
    --r-pill: 100px;
}

*, *::before, *::after { box-sizing: border-box; }

html, body, [class*="css"] {
    font-family: 'Instrument Sans', sans-serif !important;
    background: var(--ink) !important;
    color: var(--t1) !important;
}

.stApp {
    background: var(--ink) !important;
}
.stApp > header { background: transparent !important; box-shadow: none !important; }

/* ─── Sidebar ─── */
section[data-testid="stSidebar"] {
    background: var(--ink-mid) !important;
    border-right: 1px solid var(--line) !important;
    padding-top: 0 !important;
}
section[data-testid="stSidebar"] > div { padding-top: 0 !important; }
section[data-testid="stSidebar"] * { color: var(--t1) !important; }

/* ─── Inputs ─── */
div[data-baseweb="input"] input,
textarea {
    background: var(--ink-card) !important;
    border: 1px solid var(--line) !important;
    color: var(--t1) !important;
    border-radius: var(--r) !important;
    font-family: 'Instrument Sans', sans-serif !important;
    transition: border-color 0.15s !important;
}
div[data-baseweb="input"] input:focus,
textarea:focus {
    border-color: var(--acid) !important;
    box-shadow: 0 0 0 3px var(--acid-dim) !important;
}
div[data-baseweb="select"] > div {
    background: var(--ink-card) !important;
    border: 1px solid var(--line) !important;
    border-radius: var(--r) !important;
    color: var(--t1) !important;
}
div[data-baseweb="select"] * { color: var(--t1) !important; background: var(--ink-card) !important; }
ul[role="listbox"] {
    background: var(--ink-raised) !important;
    border: 1px solid var(--line-hi) !important;
    border-radius: var(--r-lg) !important;
    padding: 4px !important;
    box-shadow: 0 16px 48px rgba(0,0,0,0.6) !important;
}
ul[role="listbox"] li:hover { background: var(--ink-hover) !important; border-radius: 6px !important; }

/* ─── Expanders ─── */
div[data-testid="stExpander"] {
    background: var(--ink-card) !important;
    border: 1px solid var(--line) !important;
    border-radius: var(--r-lg) !important;
    margin-bottom: 8px !important;
    overflow: hidden !important;
    transition: border-color 0.2s, box-shadow 0.2s !important;
}
div[data-testid="stExpander"]:hover {
    border-color: var(--line-hi) !important;
}
div[data-testid="stExpander"] summary {
    font-family: 'Instrument Sans', sans-serif !important;
    font-size: 13.5px !important;
    font-weight: 500 !important;
    color: var(--t1) !important;
    padding: 14px 20px !important;
    background: var(--ink-card) !important;
}
div[data-testid="stExpander"] summary:hover {
    background: var(--ink-hover) !important;
}

/* ─── Metrics ─── */
div[data-testid="stMetric"] {
    background: var(--ink-card) !important;
    border: 1px solid var(--line) !important;
    border-radius: var(--r-lg) !important;
    padding: 20px 22px !important;
    position: relative !important;
    overflow: hidden !important;
    transition: border-color 0.2s, transform 0.15s !important;
}
div[data-testid="stMetric"]:hover {
    border-color: var(--line-hi) !important;
    transform: translateY(-1px) !important;
}
div[data-testid="stMetricLabel"] {
    font-family: 'Instrument Sans', sans-serif !important;
    font-size: 11px !important;
    font-weight: 600 !important;
    letter-spacing: 0.1em !important;
    text-transform: uppercase !important;
    color: var(--t3) !important;
    margin-bottom: 6px !important;
}
div[data-testid="stMetricValue"] {
    font-family: 'Syne', sans-serif !important;
    font-size: 2rem !important;
    font-weight: 700 !important;
    color: var(--t1) !important;
    line-height: 1.1 !important;
}
div[data-testid="stMetricDelta"] {
    font-size: 11px !important;
    font-weight: 500 !important;
    margin-top: 4px !important;
}

/* ─── Buttons ─── */
.stButton > button {
    font-family: 'Instrument Sans', sans-serif !important;
    font-weight: 600 !important;
    font-size: 13px !important;
    letter-spacing: 0.03em !important;
    border-radius: var(--r) !important;
    transition: all 0.15s !important;
    height: 40px !important;
}
.stButton > button[kind="primary"] {
    background: var(--acid) !important;
    color: var(--ink) !important;
    border: none !important;
    box-shadow: 0 0 0 0 var(--acid-dim) !important;
}
.stButton > button[kind="primary"]:hover {
    background: #d4ff1a !important;
    box-shadow: 0 0 24px var(--acid-dim), 0 4px 16px rgba(200,255,0,0.2) !important;
    transform: translateY(-1px) !important;
}
.stButton > button[kind="primary"]:active { transform: translateY(0) scale(0.98) !important; }
.stButton > button:not([kind="primary"]) {
    background: transparent !important;
    color: var(--t2) !important;
    border: 1px solid var(--line) !important;
}
.stButton > button:not([kind="primary"]):hover {
    background: var(--ink-hover) !important;
    border-color: var(--line-hi) !important;
    color: var(--t1) !important;
}

/* ─── Progress ─── */
div[data-testid="stProgressBar"] > div > div {
    background: linear-gradient(90deg, var(--acid), #80ff80) !important;
    border-radius: 4px !important;
}
div[data-testid="stProgressBar"] > div {
    background: var(--ink-card) !important;
    border: 1px solid var(--line) !important;
    border-radius: 4px !important;
    height: 6px !important;
}

/* ─── DataFrame ─── */
div[data-testid="stDataFrame"] {
    border-radius: var(--r-lg) !important;
    border: 1px solid var(--line) !important;
    overflow: hidden !important;
}

/* ─── Slider ─── */
div[data-baseweb="slider"] [role="slider"] {
    background: var(--acid) !important;
    border: 2px solid var(--ink) !important;
    width: 18px !important;
    height: 18px !important;
}
div[data-baseweb="slider"] [data-testid="stSlider"] div[class*="track"] {
    background: var(--line) !important;
}

/* ─── Code ─── */
code, pre {
    background: var(--ink-mid) !important;
    border: 1px solid var(--line) !important;
    border-radius: var(--r) !important;
    font-family: 'JetBrains Mono', monospace !important;
    font-size: 12px !important;
    color: var(--acid) !important;
}

/* ─── Download ─── */
div[data-testid="stDownloadButton"] button {
    background: var(--ice-dim) !important;
    color: var(--ice) !important;
    border: 1px solid rgba(0,229,255,0.2) !important;
    font-size: 12px !important;
    height: 34px !important;
}
div[data-testid="stDownloadButton"] button:hover {
    background: rgba(0,229,255,0.14) !important;
    border-color: var(--ice) !important;
}

/* ─── Alerts ─── */
div[data-testid="stAlert"] {
    background: var(--ink-card) !important;
    border-radius: var(--r-lg) !important;
    border: 1px solid var(--line) !important;
}

hr { border: none !important; border-top: 1px solid var(--line) !important; margin: 28px 0 !important; }
#MainMenu, footer, .viewerBadge_container__1QSob { display: none !important; }

/* ══════════════════════════════════════
   CUSTOM COMPONENTS
══════════════════════════════════════ */

/* Page header */
.ph-eyebrow {
    font-family: 'JetBrains Mono', monospace;
    font-size: 10px;
    font-weight: 500;
    letter-spacing: 0.15em;
    text-transform: uppercase;
    color: var(--t3);
    margin-bottom: 8px;
}
.ph-title {
    font-family: 'Syne', sans-serif;
    font-size: 2.6rem;
    font-weight: 800;
    color: var(--t1);
    line-height: 1.05;
    letter-spacing: -0.02em;
    margin-bottom: 24px;
}
.ph-title em {
    font-style: normal;
    color: var(--acid);
}

/* Sidebar brand */
.sb-brand {
    padding: 28px 20px 24px;
    border-bottom: 1px solid var(--line);
    margin-bottom: 16px;
}
.sb-wordmark {
    font-family: 'Syne', sans-serif;
    font-size: 1.4rem;
    font-weight: 800;
    color: var(--t1);
    letter-spacing: -0.02em;
    display: flex;
    align-items: center;
    gap: 8px;
}
.sb-wordmark .dot {
    width: 8px;
    height: 8px;
    background: var(--acid);
    border-radius: 50%;
    display: inline-block;
    animation: pulse 2.5s ease-in-out infinite;
}
@keyframes pulse {
    0%, 100% { opacity: 1; transform: scale(1); }
    50% { opacity: 0.5; transform: scale(0.75); }
}
.sb-sub {
    font-size: 10px;
    letter-spacing: 0.12em;
    text-transform: uppercase;
    color: var(--t3);
    margin-top: 4px;
    font-weight: 500;
}

/* Section label */
.sec-label {
    font-family: 'JetBrains Mono', monospace;
    font-size: 10px;
    font-weight: 500;
    letter-spacing: 0.12em;
    text-transform: uppercase;
    color: var(--t3);
    margin: 0 0 14px;
    display: flex;
    align-items: center;
    gap: 8px;
}
.sec-label::after {
    content: '';
    flex: 1;
    height: 1px;
    background: var(--line);
}

/* Section title */
.sec-title {
    font-family: 'Syne', sans-serif;
    font-size: 1.15rem;
    font-weight: 700;
    color: var(--t1);
    margin: 28px 0 14px;
    letter-spacing: -0.01em;
}

/* Status card (sidebar) */
.status-card {
    background: var(--ink-card);
    border: 1px solid var(--line);
    border-radius: var(--r-lg);
    padding: 16px 18px;
    margin: 12px 0;
}
.status-row {
    display: flex;
    align-items: center;
    justify-content: space-between;
    font-size: 12px;
    padding: 5px 0;
    color: var(--t2);
    border-bottom: 1px solid var(--line);
}
.status-row:last-child { border-bottom: none; }
.status-val {
    font-family: 'JetBrains Mono', monospace;
    font-size: 12px;
    font-weight: 500;
    color: var(--t1);
}
.status-dot {
    width: 6px;
    height: 6px;
    border-radius: 50%;
    background: var(--acid);
    display: inline-block;
    margin-right: 6px;
    animation: pulse 2.5s ease-in-out infinite;
}

/* Badge */
.badge {
    display: inline-flex;
    align-items: center;
    gap: 4px;
    padding: 3px 10px;
    border-radius: var(--r-pill);
    font-size: 10px;
    font-weight: 700;
    letter-spacing: 0.06em;
    text-transform: uppercase;
    font-family: 'Instrument Sans', sans-serif;
}
.b-acid   { background: var(--acid-dim);  color: var(--acid);  border: 1px solid rgba(200,255,0,0.25); }
.b-flame  { background: var(--flame-dim); color: var(--flame); border: 1px solid rgba(255,69,0,0.25); }
.b-ice    { background: var(--ice-dim);   color: var(--ice);   border: 1px solid rgba(0,229,255,0.2); }
.b-gold   { background: var(--gold-dim);  color: var(--gold);  border: 1px solid rgba(255,184,0,0.25); }
.b-lilac  { background: var(--lilac-dim); color: var(--lilac); border: 1px solid rgba(183,148,255,0.25); }
.b-ghost  { background: rgba(255,255,255,0.04); color: var(--t3); border: 1px solid var(--line); }

/* Alert row (chase) */
.alert-strip {
    display: flex;
    align-items: center;
    gap: 14px;
    padding: 13px 18px;
    background: var(--ink-card);
    border: 1px solid var(--line);
    border-left: 3px solid var(--gold);
    border-radius: 0 var(--r) var(--r) 0;
    margin-bottom: 6px;
    cursor: default;
    transition: background 0.15s;
}
.alert-strip:hover { background: var(--ink-hover); }
.alert-vendor {
    font-size: 13px;
    font-weight: 600;
    color: var(--t1);
    min-width: 150px;
    flex-shrink: 0;
    white-space: nowrap;
    overflow: hidden;
    text-overflow: ellipsis;
}
.alert-subject {
    font-size: 12px;
    color: var(--t2);
    flex: 1;
    white-space: nowrap;
    overflow: hidden;
    text-overflow: ellipsis;
}
.alert-pill {
    font-size: 10px;
    font-weight: 700;
    color: var(--gold);
    background: var(--gold-dim);
    border: 1px solid rgba(255,184,0,0.25);
    padding: 3px 10px;
    border-radius: var(--r-pill);
    white-space: nowrap;
    flex-shrink: 0;
    text-transform: uppercase;
    letter-spacing: 0.05em;
}

/* Vendor row */
.vendor-strip {
    display: flex;
    align-items: center;
    gap: 16px;
    padding: 12px 18px;
    background: var(--ink-card);
    border: 1px solid var(--line);
    border-radius: var(--r);
    margin-bottom: 5px;
    transition: border-color 0.15s, background 0.15s;
    font-size: 13px;
    cursor: default;
}
.vendor-strip:hover { border-color: var(--line-hi); background: var(--ink-hover); }
.vn { font-weight: 600; color: var(--t1); min-width: 160px; flex-shrink: 0; }
.vt { font-family: 'JetBrains Mono', monospace; font-size: 14px; font-weight: 500; color: var(--ice); }
.vr { font-family: 'JetBrains Mono', monospace; font-size: 13px; font-weight: 500; }
.vd { margin-left: auto; font-size: 11px; color: var(--t3); font-family: 'JetBrains Mono', monospace; }

/* Detail pill */
.dp {
    display: inline-flex;
    align-items: center;
    gap: 5px;
    background: var(--ink-raised);
    border: 1px solid var(--line);
    border-radius: 6px;
    padding: 4px 10px;
    font-size: 11.5px;
    color: var(--t2);
    margin: 2px 2px 2px 0;
    white-space: nowrap;
}
.dp b { color: var(--t1); font-weight: 600; }
.dp-acid b { color: var(--acid); }
.dp-ice b { color: var(--ice); }
.dp-gold b { color: var(--gold); }
.dp-flame b { color: var(--flame); }
.dp-lilac b { color: var(--lilac); }

/* Sample status pill */
.ss-pill {
    display: inline-flex;
    align-items: center;
    gap: 5px;
    padding: 5px 14px;
    border-radius: var(--r-pill);
    font-size: 11px;
    font-weight: 700;
    text-transform: uppercase;
    letter-spacing: 0.06em;
    border: 1px solid;
}
.ss-Dispatched { background: var(--gold-dim);  color: var(--gold);  border-color: rgba(255,184,0,0.25); }
.ss-Received   { background: var(--ice-dim);   color: var(--ice);   border-color: rgba(0,229,255,0.2); }
.ss-Approved   { background: var(--acid-dim);  color: var(--acid);  border-color: rgba(200,255,0,0.2); }
.ss-Rejected   { background: var(--flame-dim); color: var(--flame); border-color: rgba(255,69,0,0.25); }
.ss-Pending    { background: var(--lilac-dim); color: var(--lilac); border-color: rgba(183,148,255,0.25); }
.ss-None       { background: var(--ink-raised); color: var(--t3); border-color: var(--line); }

/* Ship card */
.ship-block {
    background: var(--ink-raised);
    border: 1px solid var(--line);
    border-radius: var(--r);
    padding: 14px 16px;
}
.ship-carrier { font-size: 13px; font-weight: 600; color: var(--ice); display: flex; align-items: center; gap: 6px; }
.ship-awb { font-family: 'JetBrains Mono', monospace; font-size: 12px; color: var(--t1); margin-top: 5px; }
.ship-date { font-size: 11px; color: var(--t3); margin-top: 4px; font-family: 'JetBrains Mono', monospace; }
.ship-none { font-size: 12px; color: var(--t3); font-style: italic; }

/* Reply block */
.reply-header {
    display: flex;
    align-items: center;
    gap: 8px;
    font-size: 10px;
    font-weight: 700;
    letter-spacing: 0.1em;
    text-transform: uppercase;
    color: var(--ice);
    margin-bottom: 8px;
    font-family: 'JetBrains Mono', monospace;
}
.reply-header::before {
    content: '';
    width: 20px;
    height: 1px;
    background: var(--ice);
    opacity: 0.4;
}
.reply-body {
    background: var(--ink-raised);
    border: 1px solid var(--line);
    border-left: 3px solid var(--ice);
    border-radius: 0 var(--r) var(--r) 0;
    padding: 18px 22px;
    font-size: 13px;
    line-height: 1.85;
    color: var(--t1);
    white-space: pre-wrap;
    font-family: 'Instrument Sans', sans-serif;
}

/* Overview bullet */
.ov-line {
    display: flex;
    align-items: flex-start;
    gap: 10px;
    padding: 7px 0;
    border-bottom: 1px solid var(--line);
    font-size: 13px;
    color: var(--t2);
    line-height: 1.6;
}
.ov-line:last-child { border-bottom: none; }
.ov-line::before {
    content: '—';
    color: var(--t4);
    flex-shrink: 0;
    margin-top: 1px;
    font-family: 'JetBrains Mono', monospace;
    font-size: 11px;
}

/* Log terminal */
.terminal {
    background: var(--ink-mid);
    border: 1px solid var(--line);
    border-top: 3px solid var(--acid);
    border-radius: 0 0 var(--r) var(--r);
    padding: 14px 18px;
    font-family: 'JetBrains Mono', monospace;
    font-size: 11.5px;
    line-height: 1.85;
    min-height: 130px;
    max-height: 220px;
    overflow-y: auto;
}
.terminal-bar {
    background: var(--ink-card);
    border: 1px solid var(--line);
    border-bottom: none;
    border-radius: var(--r) var(--r) 0 0;
    padding: 8px 16px;
    display: flex;
    align-items: center;
    gap: 6px;
}
.t-dot { width: 8px; height: 8px; border-radius: 50%; }

/* Config block */
.cfg-block {
    background: var(--ink-card);
    border: 1px solid var(--line);
    border-radius: var(--r-lg);
    padding: 20px 24px 16px;
    margin-bottom: 20px;
}

/* Filter bar */
.filter-bar {
    background: var(--ink-card);
    border: 1px solid var(--line);
    border-radius: var(--r-lg);
    padding: 18px 22px;
    margin-bottom: 18px;
}

/* Section divider label */
.divlabel {
    display: inline-flex;
    align-items: center;
    gap: 8px;
    font-size: 10px;
    font-weight: 600;
    letter-spacing: 0.1em;
    text-transform: uppercase;
    color: var(--t3);
    font-family: 'JetBrains Mono', monospace;
    margin: 20px 0 10px;
}

/* Count badge */
.count-tag {
    background: var(--ink-raised);
    border: 1px solid var(--line);
    border-radius: 20px;
    padding: 2px 8px;
    font-family: 'JetBrains Mono', monospace;
    font-size: 11px;
    color: var(--t2);
}

/* KPI accent bar */
div[data-testid="stMetric"]:nth-child(1)  { border-top: 2px solid var(--acid) !important; }
div[data-testid="stMetric"]:nth-child(2)  { border-top: 2px solid var(--flame) !important; }
div[data-testid="stMetric"]:nth-child(3)  { border-top: 2px solid var(--gold) !important; }
div[data-testid="stMetric"]:nth-child(4)  { border-top: 2px solid var(--ice) !important; }
div[data-testid="stMetric"]:nth-child(5)  { border-top: 2px solid var(--lilac) !important; }
div[data-testid="stMetric"]:nth-child(6)  { border-top: 2px solid var(--t3) !important; }

/* Result metrics on sync page */
.result-grid {
    display: grid;
    grid-template-columns: repeat(5, 1fr);
    gap: 10px;
    margin-top: 20px;
}
.r-card {
    background: var(--ink-card);
    border: 1px solid var(--line);
    border-radius: var(--r-lg);
    padding: 16px 18px;
    text-align: center;
}
.r-label {
    font-size: 10px;
    font-weight: 600;
    letter-spacing: 0.08em;
    text-transform: uppercase;
    color: var(--t3);
    margin-bottom: 6px;
    font-family: 'JetBrains Mono', monospace;
}
.r-value {
    font-family: 'Syne', sans-serif;
    font-size: 1.8rem;
    font-weight: 800;
    line-height: 1;
}

/* Scrollbar */
::-webkit-scrollbar { width: 4px; height: 4px; }
::-webkit-scrollbar-track { background: transparent; }
::-webkit-scrollbar-thumb { background: var(--line-hi); border-radius: 4px; }
::-webkit-scrollbar-thumb:hover { background: var(--t3); }

/* Checkbox */
div[data-baseweb="checkbox"] label span { color: var(--t1) !important; font-size: 13px !important; }
div[data-baseweb="checkbox"] svg { stroke: var(--acid) !important; }
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
        "chase":   "b-gold",
        "delay":   "b-flame",
        "quality": "b-flame",
        "approve": "b-acid",
        "review":  "b-acid",
        "track":   "b-ice",
        "await":   "b-ice",
        "confirm": "b-lilac",
        "payment": "b-flame",
        "no action": "b-ghost",
        "other":   "b-ghost",
    }
    cls = "b-ghost"
    for k, v in cls_map.items():
        if k in intent.lower():
            cls = v
            break
    return f'<span class="badge {cls}">{intent}</span>'


def _sample_pill(status: str) -> str:
    icons = {"Dispatched":"↑", "Received":"↓", "Approved":"✓",
             "Rejected":"✕", "Pending":"◌", "None":"—"}
    icon = icons.get(status, "")
    css  = f"ss-{status}" if status in ("Dispatched","Received","Approved","Rejected","Pending") else "ss-None"
    return f'<span class="ss-pill {css}">{icon} {status}</span>'


def _dpill(label: str, value: str, color: str = "") -> str:
    if not value:
        return ""
    cls = f"dp dp-{color}" if color else "dp"
    return f'<span class="{cls}">{label} <b>{value}</b></span>'


def _opts(df, col):
    if col not in df.columns:
        return ["All"]
    return ["All"] + sorted(df[col].dropna().replace("", pd.NA).dropna().unique().tolist())


# ── Sidebar ───────────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("""
    <div class="sb-brand">
        <div class="sb-wordmark">
            <span class="dot"></span>Merch AI
        </div>
        <div class="sb-sub">Merchandising Intelligence</div>
    </div>
    """, unsafe_allow_html=True)

    if "jump_to_viewer" not in st.session_state:
        st.session_state.jump_to_viewer = False
    if "jump_sample_filter" not in st.session_state:
        st.session_state.jump_sample_filter = "All"

    _page_options = ["Dashboard", "Sync Gmail", "Thread Viewer"]
    _default_page = 2 if st.session_state.jump_to_viewer else 0
    page = st.radio("Navigation", _page_options, index=_default_page,
                    label_visibility="collapsed")
    if st.session_state.jump_to_viewer and "Thread Viewer" in page:
        st.session_state.jump_to_viewer = False

    st.markdown("<br>", unsafe_allow_html=True)

    # Refresh — clearly labelled purpose
    if st.button("↺  Refresh Data", use_container_width=True,
                 help="Clear cached sheet data and reload from Google Sheets"):
        st.session_state.df_logs = None
        st.rerun()

    st.markdown("<br>", unsafe_allow_html=True)

    cs = cache_stats()
    from gmail_reader import _run_token_usage
    _tok_in    = _run_token_usage.get("prompt_tokens", 0)
    _tok_out   = _run_token_usage.get("completion_tokens", 0)
    _tok_total = _tok_in + _tok_out
    _tok_str   = f"{_tok_total:,}" if _tok_total else "—"

    st.markdown(f"""
    <div class="status-card">
        <div style="font-size:10px;font-weight:700;letter-spacing:0.1em;
                    text-transform:uppercase;color:var(--t3);margin-bottom:10px;
                    font-family:'JetBrains Mono',monospace;">
            System Status
        </div>
        <div class="status-row">
            <span>Cached threads</span>
            <span class="status-val">{cs.get('cached_entries', 0)}</span>
        </div>
        <div class="status-row">
            <span>Tokens this run</span>
            <span class="status-val">{_tok_str}</span>
        </div>
        <div class="status-row">
            <span>In / Out</span>
            <span class="status-val" style="font-size:11px">{_tok_in:,} / {_tok_out:,}</span>
        </div>
        <div class="status-row">
            <span>Model</span>
            <span class="status-val"><span class="status-dot"></span>GPT-5.4</span>
        </div>
    </div>
    """, unsafe_allow_html=True)


# ════════════════════════════════════════════════════════════════════════════════
# PAGE: DASHBOARD
# ════════════════════════════════════════════════════════════════════════════════
if page == "Dashboard":
    st.markdown('<div class="ph-eyebrow">Overview</div>', unsafe_allow_html=True)
    st.markdown('<div class="ph-title">Merchandising <em>Intelligence</em></div>', unsafe_allow_html=True)

    df = st.session_state.df_logs if st.session_state.df_logs is not None else load_data_from_sheets()

    if df.empty:
        st.info("No data yet — go to **Sync Gmail** to pull your first threads.")
        st.stop()

    # ── KPIs ─────────────────────────────────────────────────────────────────
    total       = len(df)
    needs_reply = int((df.get("Reply Needed", pd.Series(dtype=str)) == "Yes").sum())
    chase_n     = int(df.get("Sample Reminder", pd.Series(dtype=str)).str.startswith("⚠️", na=False).sum())
    po_ct       = int((df.get("PO Number", pd.Series(dtype=str)).replace("", pd.NA).notna()).sum())
    dispatched  = int((df.get("Sample Status", pd.Series(dtype=str)) == "Dispatched").sum())

    k1, k2, k3, k4, k5, k6 = st.columns(6)
    k1.metric("Total Threads",    total)
    k2.metric("Needs Reply",      needs_reply,
              delta=f"{round(needs_reply/max(total,1)*100)}% of threads",
              delta_color="off")
    k3.metric("Chase Alerts",     chase_n,
              delta="Action required" if chase_n else "All clear",
              delta_color="inverse")
    k4.metric("Threads with PO",  po_ct)
    k5.metric("Dispatched",       dispatched)

    # "View dispatched" — purposeful deep-link button
    if k5.button("View →", key="dispatched_jump",
                 help="Open Thread Viewer filtered to Dispatched samples"):
        st.session_state.jump_to_viewer     = True
        st.session_state.jump_sample_filter = "Dispatched"
        st.rerun()

    if "Vendor Name" in df.columns:
        top_v = df[df["Vendor Name"].replace("", pd.NA).notna()]["Vendor Name"].value_counts()
        tv      = top_v.index[0] if not top_v.empty else "—"
        tv_ct   = int(top_v.iloc[0]) if not top_v.empty else 0
        k6.markdown(f"""
        <div style="background:var(--ink-card);border:1px solid var(--line);
                    border-top:2px solid var(--t3);border-radius:var(--r-lg);
                    padding:20px 22px;">
            <div style="font-size:11px;font-weight:600;letter-spacing:0.1em;
                        text-transform:uppercase;color:var(--t3);margin-bottom:8px;
                        font-family:'Instrument Sans',sans-serif;">Top Vendor</div>
            <div style="font-family:'Syne',sans-serif;font-size:1.1rem;font-weight:700;
                        color:var(--t1);line-height:1.25;word-break:break-word;">{tv}</div>
            <div style="font-size:12px;color:var(--t3);margin-top:6px;">{tv_ct} threads</div>
        </div>
        """, unsafe_allow_html=True)

    st.markdown("<br>", unsafe_allow_html=True)

    # ── Chase alerts ──────────────────────────────────────────────────────────
    if "Sample Reminder" in df.columns:
        chase_df = df[df["Sample Reminder"].str.startswith("⚠️", na=False)]
        if not chase_df.empty:
            st.markdown(
                f'<div class="sec-title">Chase Alerts '
                f'<span class="count-tag">{len(chase_df)}</span></div>',
                unsafe_allow_html=True
            )
            for _, row in chase_df.iterrows():
                st.markdown(f"""
                <div class="alert-strip">
                    <span class="alert-vendor">{_safe(row,'Vendor Name','Unknown')}</span>
                    <span class="alert-subject">{_safe(row,'Subject','')[:70]}</span>
                    <span class="alert-pill">{_safe(row,'Sample Reminder','').replace('⚠️','').strip()}</span>
                </div>
                """, unsafe_allow_html=True)
            st.markdown("<br>", unsafe_allow_html=True)

    # ── Charts ────────────────────────────────────────────────────────────────
    import plotly.graph_objects as go

    _chart_layout = dict(
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        margin=dict(l=0, r=16, t=8, b=8),
        font=dict(family="Instrument Sans, sans-serif", color="#8A8AA8", size=11),
        xaxis=dict(
            showgrid=True, gridcolor="#232330", gridwidth=0.5,
            zeroline=False, showline=False, tickfont=dict(size=11, color="#8A8AA8"),
        ),
        yaxis=dict(
            showgrid=False, zeroline=False, showline=False,
            tickfont=dict(size=11, color="#F4F4F8"), automargin=True,
        ),
        bargap=0.35,
        hoverlabel=dict(
            bgcolor="#16161C", bordercolor="#32324A",
            font=dict(family="JetBrains Mono, monospace", size=11, color="#F4F4F8"),
        ),
    )

    def _hbar(labels, values, color, title_col):
        # Sort ascending so largest is at top
        pairs  = sorted(zip(labels, values), key=lambda x: x[1])
        xlbls  = [p[0] for p in pairs]
        xvals  = [p[1] for p in pairs]
        height = max(180, len(xlbls) * 38 + 40)
        fig = go.Figure(go.Bar(
            x=xvals, y=xlbls,
            orientation="h",
            marker=dict(
                color=color,
                opacity=0.85,
                line=dict(width=0),
            ),
            hovertemplate="<b>%{y}</b><br>%{x} threads<extra></extra>",
            text=xvals,
            textposition="outside",
            textfont=dict(size=11, color="#8A8AA8", family="JetBrains Mono, monospace"),
            cliponaxis=False,
        ))
        layout = dict(**_chart_layout)
        layout["height"] = height
        fig.update_layout(**layout)
        return fig

    ca, cb, cc = st.columns(3)
    with ca:
        st.markdown('<div class="sec-title">Intent Breakdown</div>', unsafe_allow_html=True)
        if "Intent" in df.columns:
            ic = df["Intent"].value_counts().reset_index()
            ic.columns = ["Intent", "Count"]
            st.plotly_chart(
                _hbar(ic["Intent"].tolist(), ic["Count"].tolist(), "#C8FF00", ca),
                use_container_width=True, config={"displayModeBar": False}
            )
    with cb:
        st.markdown('<div class="sec-title">By Division</div>', unsafe_allow_html=True)
        if "Division" in df.columns:
            dc = df["Division"].value_counts().reset_index()
            dc.columns = ["Division", "Count"]
            st.plotly_chart(
                _hbar(dc["Division"].tolist(), dc["Count"].tolist(), "#00E5FF", cb),
                use_container_width=True, config={"displayModeBar": False}
            )
    with cc:
        st.markdown('<div class="sec-title">Sample Pipeline</div>', unsafe_allow_html=True)
        if "Sample Status" in df.columns:
            sc = df["Sample Status"].value_counts().reset_index()
            sc.columns = ["Status", "Count"]
            st.plotly_chart(
                _hbar(sc["Status"].tolist(), sc["Count"].tolist(), "#FFB800", cc),
                use_container_width=True, config={"displayModeBar": False}
            )

    st.divider()

    # ── Recent activity ───────────────────────────────────────────────────────
    st.markdown('<div class="sec-title">Recent Activity</div>', unsafe_allow_html=True)
    show_cols = [c for c in
        ["Sent Date","Vendor Name","Subject","Division","Intent",
         "Reply Needed","Sample Status","PO Number","AWB No"]
        if c in df.columns]
    recent = df.sort_values("Sent Date", ascending=False).head(10) \
              if "Sent Date" in df.columns else df.head(10)
    st.dataframe(recent[show_cols], use_container_width=True, hide_index=True)

    st.divider()

    # ── Vendor breakdown ──────────────────────────────────────────────────────
    if "Vendor Name" in df.columns:
        st.markdown('<div class="sec-title">Vendor Breakdown</div>', unsafe_allow_html=True)
        vdf = df[df["Vendor Name"].replace("", pd.NA).notna()]
        if not vdf.empty:
            vstats = (
                vdf.groupby("Vendor Name")
                .agg(Threads=("Subject","count"),
                     Reply_Needed=("Reply Needed", lambda x: (x=="Yes").sum()),
                     Latest=("Sent Date","max"))
                .sort_values("Threads", ascending=False).head(12).reset_index()
            )
            for _, vrow in vstats.iterrows():
                rn  = int(vrow["Reply_Needed"])
                col = "var(--flame)" if rn > 0 else "var(--acid)"
                st.markdown(f"""
                <div class="vendor-strip">
                    <span class="vn">{vrow['Vendor Name']}</span>
                    <span style="font-size:12px;color:var(--t3);">
                        <span class="vt">{vrow['Threads']}</span>
                        <span style="margin-left:3px;font-size:11px">threads</span>
                    </span>
                    <span style="font-size:12px;color:var(--t3);">
                        Reply needed:
                        <span class="vr" style="color:{col}">{rn}</span>
                    </span>
                    <span class="vd">Last active {str(vrow.get('Latest',''))[:10]}</span>
                </div>
                """, unsafe_allow_html=True)


# ════════════════════════════════════════════════════════════════════════════════
# PAGE: SYNC GMAIL
# ════════════════════════════════════════════════════════════════════════════════
elif page == "Sync Gmail":
    st.markdown('<div class="ph-eyebrow">Inbox Sync</div>', unsafe_allow_html=True)
    st.markdown('<div class="ph-title">Sync <em>Gmail</em></div>', unsafe_allow_html=True)

    # Config card
    st.markdown('<div class="cfg-block"><div class="sec-label">Sync Configuration</div>', unsafe_allow_html=True)
    cfg1, cfg2, cfg3 = st.columns([4, 1, 1])
    with cfg1:
        max_threads = st.slider(
            "Threads to fetch",
            min_value=1, max_value=200, value=20,
            help="Number of inbox threads to analyse this run"
        )
    with cfg2:
        st.markdown("<br>", unsafe_allow_html=True)
        dry_run = st.checkbox(
            "Dry run",
            help="Analyse threads with GPT but do not write results to Google Sheets"
        )
    with cfg3:
        st.markdown("<br><br>", unsafe_allow_html=True)
        if dry_run:
            st.markdown('<span class="badge b-gold">Read only</span>', unsafe_allow_html=True)
        else:
            st.markdown('<span class="badge b-acid">Write mode</span>', unsafe_allow_html=True)
    st.markdown("</div>", unsafe_allow_html=True)

    # Start button — clear call-to-action
    bcol, tcol = st.columns([1, 3])
    with bcol:
        start = st.button(
            "Run Sync →",
            type="primary",
            use_container_width=True,
            help="Connect to Gmail, analyse threads via GPT-5.4, and update Google Sheets"
        )
    with tcol:
        st.markdown(
            '<div style="padding-top:10px;font-size:12px;color:var(--t3);">'
            'GPT-5.4 analyses each thread once — results are cached. '
            'Repeat runs only re-process new or updated threads.'
            '</div>',
            unsafe_allow_html=True
        )

    if start:
        ph = st.empty()
        ph.markdown(
            '<div style="font-size:13px;color:var(--acid);padding:8px 0;'
            'font-family:\'JetBrains Mono\',monospace;">▶ Connecting…</div>',
            unsafe_allow_html=True
        )

        gmail_svc, sheets_svc, creds = get_google_services()
        gmail_reader.DRY_RUN = dry_run
        vendor_db = load_vendor_db(sheets_svc)
        if not dry_run:
            _ensure_tab(sheets_svc, SHEET_TAB, LOGS_HEADERS)
            _ensure_tab(sheets_svc, ERROR_TAB, ERROR_HEADERS)
        thread_map, subject_map = load_existing_rows(sheets_svc)

        ph.markdown(
            '<div style="font-size:13px;color:var(--acid);padding:8px 0;'
            'font-family:\'JetBrains Mono\',monospace;">✓ Connected — fetching thread list…</div>',
            unsafe_allow_html=True
        )
        result        = gmail_svc.users().threads().list(userId="me", maxResults=max_threads, q="in:inbox").execute()
        gmail_threads = result.get("threads", [])
        if not gmail_threads:
            st.info("No threads found in inbox.")
            st.stop()

        ph.markdown(
            f'<div style="font-size:13px;color:var(--acid);padding:8px 0;'
            f'font-family:\'JetBrains Mono\',monospace;">'
            f'✓ {len(gmail_threads)} threads found — fetching metadata…</div>',
            unsafe_allow_html=True
        )
        all_tids = [t["id"] for t in gmail_threads]
        meta_map = fetch_all_metadata_safe(creds, all_tids)
        ph.empty()

        progress_bar = st.progress(0)
        st.markdown(
            '<div style="font-size:11px;color:var(--t3);margin:-6px 0 10px;'
            'font-family:\'JetBrains Mono\',monospace;">Processing…</div>',
            unsafe_allow_html=True
        )

        # Terminal log
        st.markdown("""
        <div class="terminal-bar">
            <div class="t-dot" style="background:#ff5f57"></div>
            <div class="t-dot" style="background:#ffbd2e"></div>
            <div class="t-dot" style="background:#28ca41"></div>
            <span style="font-size:11px;color:var(--t3);margin-left:8px;
                         font-family:'JetBrains Mono',monospace;">sync.log</span>
        </div>
        """, unsafe_allow_html=True)
        log_ph = st.empty()

        new_rows  = []
        updated = backfilled = added = skipped = errors = 0
        total   = len(gmail_threads)
        now_str = datetime.now().strftime("%Y-%m-%d %H:%M")
        log_lines = []

        for i, thread in enumerate(gmail_threads, 1):
            tid        = thread["id"]
            meta       = meta_map.get(tid, {})
            curr_count = meta.get("count", 0)
            meta_subj  = meta.get("subject", "")
            subj_key   = meta_subj.lower()
            status_msg = ""

            progress_bar.progress(
                int(i / total * 100),
                text=f"[{i}/{total}]  {meta_subj[:60]}"
            )
            try:
                if tid in thread_map:
                    existing  = thread_map[tid]
                    old_count = existing["message_count"]
                    if curr_count <= old_count:
                        skipped   += 1
                        status_msg = f"  skip  {meta_subj[:52]}"
                    else:
                        row_data = process_thread(gmail_svc, tid, vendor_db)
                        if row_data:
                            update_existing_row(sheets_svc, existing["sheet_row"], row_data)
                            thread_map[tid]["message_count"] = row_data["Thread Messages"]
                            updated   += 1
                            status_msg = f"  upd   {row_data['Subject'][:52]}"

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
                        status_msg  = f"  tid   {meta_subj[:52]}"
                    else:
                        row_data = process_thread(gmail_svc, tid, vendor_db)
                        if row_data:
                            update_existing_row(sheets_svc, existing["sheet_row"], row_data, backfill_tid=True)
                            thread_map[tid] = existing
                            thread_map[tid]["message_count"] = row_data["Thread Messages"]
                            updated    += 1
                            backfilled += 1
                            status_msg  = f"  upd+  {row_data['Subject'][:46]}"
                else:
                    if curr_count < 2:
                        skipped   += 1
                        status_msg = f"  1msg  {meta_subj[:52]}"
                    else:
                        row_data = process_thread(gmail_svc, tid, vendor_db)
                        if row_data is None:
                            skipped   += 1
                            status_msg = f"  none  {meta_subj[:52]}"
                        else:
                            new_rows.append(_build_new_row(row_data, now_str))
                            subject_map[row_data["Subject"].lower()] = {
                                "sheet_row": None, "message_count": curr_count,
                                "thread_id": tid, "subject": row_data["Subject"],
                            }
                            added     += 1
                            status_msg = f"  new   {row_data['Subject'][:52]}"
            except Exception as exc:
                errors    += 1
                record_error(tid, meta_subj, "streamlit_sync", exc)
                status_msg = f"  err   {meta_subj[:40]} — {str(exc)[:38]}"

            log_lines.append((status_msg, "new" if "new" in status_msg[:7]
                              else "upd" if "upd" in status_msg[:7]
                              else "err" if "err" in status_msg[:7] else "skip"))

            if i % 2 == 0 or i == total:
                color_map = {"new": "var(--acid)", "upd": "var(--ice)",
                             "err": "var(--flame)", "skip": "var(--t3)"}
                prefix_map = {"new": "+", "upd": "~", "err": "!", "skip": "·"}
                html = "".join(
                    f'<div style="color:{color_map[t]}">{prefix_map[t]}{l}</div>'
                    for l, t in log_lines[-10:]
                )
                log_ph.markdown(f'<div class="terminal">{html}</div>', unsafe_allow_html=True)

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

        progress_bar.progress(100, text="Complete")
        st.markdown("<br>", unsafe_allow_html=True)

        if dry_run:
            st.info("Dry run — no changes written to Google Sheets.")
        else:
            st.success("Sync complete — Google Sheet updated.")

        # Result cards
        st.markdown(f"""
        <div class="result-grid">
            <div class="r-card">
                <div class="r-label">Added</div>
                <div class="r-value" style="color:var(--acid)">{added}</div>
            </div>
            <div class="r-card">
                <div class="r-label">Updated</div>
                <div class="r-value" style="color:var(--ice)">{updated}</div>
            </div>
            <div class="r-card">
                <div class="r-label">Backfilled</div>
                <div class="r-value" style="color:var(--lilac)">{backfilled}</div>
            </div>
            <div class="r-card">
                <div class="r-label">Skipped</div>
                <div class="r-value" style="color:var(--t3)">{skipped}</div>
            </div>
            <div class="r-card">
                <div class="r-label">Errors</div>
                <div class="r-value" style="color:var(--flame)">{errors}</div>
            </div>
        </div>
        """, unsafe_allow_html=True)

        st.session_state.df_logs = None


# ════════════════════════════════════════════════════════════════════════════════
# PAGE: THREAD VIEWER
# ════════════════════════════════════════════════════════════════════════════════
elif page == "Thread Viewer":
    st.markdown('<div class="ph-eyebrow">Threads</div>', unsafe_allow_html=True)
    st.markdown('<div class="ph-title">Thread <em>Viewer</em></div>', unsafe_allow_html=True)

    df = st.session_state.df_logs if st.session_state.df_logs is not None else load_data_from_sheets()

    if df.empty:
        st.info("No data found. Run a sync first.")
        st.stop()

    # ── Filter bar ────────────────────────────────────────────────────────────
    st.markdown('<div class="filter-bar"><div class="sec-label">Filter Threads</div>', unsafe_allow_html=True)
    fa, fb, fc, fd, fe = st.columns(5)
    with fa:
        intent_f = st.selectbox("Intent",       _opts(df, "Intent"),       label_visibility="visible")
    with fb:
        reply_f  = st.selectbox("Reply Needed", ["All","Yes","No"],         label_visibility="visible")
    with fc:
        vendor_f = st.selectbox("Vendor",        _opts(df, "Vendor Name"),  label_visibility="visible")
    with fd:
        div_f    = st.selectbox("Division",      _opts(df, "Division"),     label_visibility="visible")
    with fe:
        _sample_opts    = _opts(df, "Sample Status")
        _jump_sample    = st.session_state.get("jump_sample_filter", "All")
        _sample_default = _sample_opts.index(_jump_sample) if _jump_sample in _sample_opts else 0
        sample_f = st.selectbox("Sample Status", _sample_opts, index=_sample_default, label_visibility="visible")
    st.session_state.jump_sample_filter = "All"
    st.markdown("</div>", unsafe_allow_html=True)

    # Apply filters
    fdf = df.copy()
    if intent_f != "All" and "Intent"        in fdf.columns: fdf = fdf[fdf["Intent"]        == intent_f]
    if reply_f  != "All" and "Reply Needed"  in fdf.columns: fdf = fdf[fdf["Reply Needed"]  == reply_f]
    if vendor_f != "All" and "Vendor Name"   in fdf.columns: fdf = fdf[fdf["Vendor Name"]   == vendor_f]
    if div_f    != "All" and "Division"      in fdf.columns: fdf = fdf[fdf["Division"]       == div_f]
    if sample_f != "All" and "Sample Status" in fdf.columns: fdf = fdf[fdf["Sample Status"] == sample_f]

    st.markdown(
        f'<div style="font-size:12px;color:var(--t3);margin-bottom:16px;'
        f'font-family:\'JetBrains Mono\',monospace;">'
        f'{len(fdf)} of {len(df)} threads</div>',
        unsafe_allow_html=True
    )

    # ── Thread cards ──────────────────────────────────────────────────────────
    for idx, row in fdf.iterrows():
        vendor   = _safe(row, "Vendor Name",  "Unknown Vendor")
        subject  = _safe(row, "Subject",      "No Subject")
        sent     = _safe(row, "Sent Date",    "")[:10]
        intent   = _safe(row, "Intent",       "")
        rn       = _safe(row, "Reply Needed", "No")
        reminder = _safe(row, "Sample Reminder", "")
        ss       = _safe(row, "Sample Status",   "None")

        # Expander label — clear priority signals
        flag    = "! " if reminder.startswith("⚠️") else ("· " if rn == "Yes" else "  ")
        exp_label = f"{flag}{sent}  {vendor}  —  {subject[:60]}"

        with st.expander(exp_label, expanded=False):

            # Intent + status badges
            bdg = _intent_badge(intent)
            if rn == "Yes":
                bdg += ' <span class="badge b-flame">Reply needed</span>'
            else:
                bdg += ' <span class="badge b-acid">No reply</span>'
            if reminder.startswith("⚠️"):
                bdg += f' <span class="badge b-gold">Chase</span>'
            st.markdown(f'<div style="margin-bottom:20px;">{bdg}</div>', unsafe_allow_html=True)

            col_ov, col_meta, col_right = st.columns([5, 3, 3])

            # ── AI Overview ──
            with col_ov:
                st.markdown(
                    '<div class="sec-label" style="margin-top:0">AI Overview</div>',
                    unsafe_allow_html=True
                )
                overview = _safe(row, "AI Overview", "")
                lines    = [l.strip() for l in overview.split("\n") if l.strip()]
                if lines:
                    html = "".join(
                        f'<div class="ov-line">{l.lstrip("•–- ")}</div>' for l in lines
                    )
                    st.markdown(html, unsafe_allow_html=True)
                else:
                    st.markdown(
                        '<span style="color:var(--t3);font-size:13px;">No overview available.</span>',
                        unsafe_allow_html=True
                    )

            # ── Thread Details ──
            with col_meta:
                st.markdown(
                    '<div class="sec-label" style="margin-top:0">Thread Details</div>',
                    unsafe_allow_html=True
                )
                pills = "".join(filter(None, [
                    _dpill("Division", _safe(row,"Division"),          "ice"),
                    _dpill("Style",    _safe(row,"Style No"),          "lilac"),
                    _dpill("Colour",   _safe(row,"Colour"),            "flame"),
                    _dpill("PO",       _safe(row,"PO Number"),         "gold"),
                    _dpill("Msgs",     _safe(row,"Thread Messages")),
                    _dpill("Date",     sent),
                    _dpill("Vendor",   vendor,                         "ice"),
                    _dpill("Class",    _safe(row,"Partner Classification")),
                    _dpill("CC",       _safe(row,"CC")),
                ]))
                st.markdown(pills, unsafe_allow_html=True)

                att   = _safe(row, "Attachments",  "")
                links = _safe(row, "Shared Links", "")
                if att:
                    st.markdown(
                        f'<div style="margin-top:10px;font-size:11px;color:var(--t3);">↳ {att}</div>',
                        unsafe_allow_html=True
                    )
                if links:
                    st.markdown(
                        f'<div style="font-size:11px;color:var(--ice);margin-top:4px;">⎋ {links}</div>',
                        unsafe_allow_html=True
                    )

            # ── Shipment + Sample ──
            with col_right:
                st.markdown(
                    '<div class="sec-label" style="margin-top:0">Shipment & Sample</div>',
                    unsafe_allow_html=True
                )
                awb     = _safe(row, "AWB No",           "")
                carrier = _safe(row, "Shipment Company", "")
                ship_dt = _safe(row, "Shipment Date",    "")
                if awb or carrier:
                    st.markdown(f"""
                    <div class="ship-block">
                        <div class="ship-carrier">↑ {carrier or 'Unknown carrier'}</div>
                        <div class="ship-awb">{awb or 'No AWB yet'}</div>
                        {"" if not ship_dt else f'<div class="ship-date">{ship_dt}</div>'}
                    </div>
                    """, unsafe_allow_html=True)
                else:
                    st.markdown(
                        '<div class="ship-none" style="padding:10px 0;">No shipment info.</div>',
                        unsafe_allow_html=True
                    )

                st.markdown(
                    '<div style="margin-top:14px;font-size:11px;font-weight:600;'
                    'letter-spacing:0.08em;text-transform:uppercase;color:var(--t3);'
                    'margin-bottom:7px;">Sample</div>',
                    unsafe_allow_html=True
                )
                st.markdown(_sample_pill(ss), unsafe_allow_html=True)

            # ── Reply Draft ──
            reply = _safe(row, "Reply Draft", "")
            if reply:
                st.markdown("<br>", unsafe_allow_html=True)
                st.markdown(
                    f'<div class="reply-header">Suggested reply</div>'
                    f'<div class="reply-body">{reply}</div>',
                    unsafe_allow_html=True
                )
                dl, _ = st.columns([1, 4])
                with dl:
                    st.download_button(
                        label="↓  Download reply",
                        data=reply,
                        file_name=f"reply_{subject[:25].replace(' ','_')}.txt",
                        mime="text/plain",
                        key=f"dl_{idx}",
                        help="Download this reply draft as a .txt file"
                    )
            else:
                st.markdown(
                    '<div style="color:var(--t3);font-size:12px;margin-top:10px;">'
                    'No reply draft — thread marked as no action needed.</div>',
                    unsafe_allow_html=True
                )