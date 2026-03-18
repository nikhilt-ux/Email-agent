"""
Merchandising AI Agent — Streamlit Frontend  v2.0
═══════════════════════════════════════════════════
Fully synced with gmail_reader v6.0 (24 columns A–X).

CHANGES FROM v1.0:
  ✅ All 24 columns loaded from sheet (was A:R only — missed PO, Sample, Attachments, Links, Reply Draft)
  ✅ fetch_all_metadata fixed — passes creds per thread, no shared SSL socket crash
  ✅ new_rows built via _build_new_row() — no more hardcoded 18-col list
  ✅ Case 2 backfill uses col_letter() not hardcoded "O"/"N"
  ✅ Reply Draft column rendered as a copyable text area in Thread Viewer
  ✅ Sample Status / Sample Reminder / PO Number shown in Thread Viewer
  ✅ Attachments + Shared Links shown in Thread Viewer
  ✅ Dashboard metrics expanded: Sample alerts, top division, PO threads
  ✅ New "Needs Chase" tab on dashboard — threads with ⚠️ reminders
  ✅ Thread Viewer: Division filter added
  ✅ load_data_from_sheets loads full A:X range dynamically from LOGS_HEADERS
  ✅ Proper DRY_RUN threading through update_existing_row (already gated in v6 core)
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

# ── Page config ──────────────────────────────────────────────
st.set_page_config(
    page_title="Merchandising AI Agent",
    page_icon="🧵",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Custom CSS ───────────────────────────────────────────────
st.markdown("""
<style>
    /* Tighter card padding */
    div[data-testid="stExpander"] { border-radius: 10px; border: 1px solid #e0e0e0; margin-bottom: 8px; }
    /* Reply draft box */
    .reply-box { background: #f0f7ff; border-left: 3px solid #1a73e8; padding: 12px 16px;
                 border-radius: 0 8px 8px 0; font-family: monospace; font-size: 13px;
                 white-space: pre-wrap; line-height: 1.6; }
    /* Intent badge */
    .badge { display: inline-block; padding: 2px 10px; border-radius: 12px;
             font-size: 12px; font-weight: 600; }
    .badge-chase  { background:#fff3cd; color:#856404; }
    .badge-ship   { background:#d1ecf1; color:#0c5460; }
    .badge-quality{ background:#f8d7da; color:#721c24; }
    .badge-sample { background:#d4edda; color:#155724; }
    .badge-other  { background:#e2e3e5; color:#383d41; }
    /* Chase warning */
    .chase-row { background: #fff8e1; border-left: 3px solid #ffc107;
                 padding: 8px 12px; border-radius: 0 6px 6px 0; margin: 4px 0; font-size: 13px; }
    /* Metric delta override */
    div[data-testid="stMetricDelta"] { font-size: 12px; }
</style>
""", unsafe_allow_html=True)

# ── Session state ─────────────────────────────────────────────
if "df_logs" not in st.session_state:
    st.session_state.df_logs = None

# ── Helpers ───────────────────────────────────────────────────

def get_google_services():
    """Return (gmail_svc, sheets_svc, creds). Creds returned separately for thread-safe use."""
    creds      = authenticate()
    gmail_svc  = build("gmail",  "v1", credentials=creds)
    sheets_svc = build("sheets", "v4", credentials=creds)
    return gmail_svc, sheets_svc, creds


def load_data_from_sheets() -> pd.DataFrame:
    """Load ALL columns (A–X) from the Logs sheet, using LOGS_HEADERS as canonical list."""
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
            # Pad short rows to header length
            padded = [r + [""] * (len(headers) - len(r)) for r in data]
            df = pd.DataFrame(padded, columns=headers)
            st.session_state.df_logs = df
            return df
        return pd.DataFrame()
    except Exception as e:
        st.error(f"Failed to load data: {e}")
        return pd.DataFrame()


def fetch_all_metadata_safe(creds, thread_ids: list) -> dict:
    """
    Thread-safe metadata fetch — each worker builds its own gmail service.
    Fixes the SSL segfault from the v1 frontend which passed a shared gmail_svc.
    """
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


def _safe_col(df: pd.DataFrame, col: str, default="") -> pd.Series:
    """Return column if it exists, else a Series of defaults."""
    return df[col] if col in df.columns else pd.Series([default] * len(df))


def _intent_badge(intent: str) -> str:
    cls = "badge-other"
    if "Chase"    in intent: cls = "badge-chase"
    elif "Track"  in intent or "Await" in intent: cls = "badge-ship"
    elif "quality" in intent.lower(): cls = "badge-quality"
    elif "sample" in intent.lower() or "Approve" in intent: cls = "badge-sample"
    return f'<span class="badge {cls}">{intent}</span>'


# ── Sidebar ───────────────────────────────────────────────────
st.sidebar.image("https://img.icons8.com/fluency/96/sewing-machine.png", width=64)
st.sidebar.title("Merchandising\nAI Agent")
st.sidebar.markdown("---")
page = st.sidebar.radio("", ["📊 Dashboard", "🔄 Sync Gmail", "🗂️ Thread Viewer"])
st.sidebar.markdown("---")

if st.sidebar.button("🔃 Refresh Data"):
    st.session_state.df_logs = None
    st.rerun()

cs = cache_stats()
st.sidebar.caption(f"💾 LLM cache: {cs.get('cached_entries', 0)} entries")


# ════════════════════════════════════════════════════════════
# PAGE: DASHBOARD
# ════════════════════════════════════════════════════════════
if page == "📊 Dashboard":
    st.title("📊 Overview Dashboard")

    df = st.session_state.df_logs if st.session_state.df_logs is not None else load_data_from_sheets()

    if df.empty:
        st.info("No data yet — go to **Sync Gmail** to pull threads.")
        st.stop()

    # ── Top metrics row ──────────────────────────────────────
    c1, c2, c3, c4, c5, c6 = st.columns(6)

    total_threads = len(df)
    c1.metric("Total Threads", total_threads)

    reply_count = len(df[_safe_col(df, "Reply Needed") == "Yes"])
    c2.metric("Needs Reply", reply_count, delta=f"{round(reply_count/max(total_threads,1)*100)}%")

    chase_count = len(df[_safe_col(df, "Sample Reminder").str.startswith("⚠️", na=False)])
    c3.metric("⚠️ Chase Alerts", chase_count)

    po_count = len(df[_safe_col(df, "PO Number") != ""])
    c4.metric("Threads with PO", po_count)

    dispatched = len(df[_safe_col(df, "Sample Status") == "Dispatched"])
    c5.metric("Samples Dispatched", dispatched)

    if "Division" in df.columns and not df["Division"].empty:
        top_div = df["Division"].value_counts().idxmax()
        c6.metric("Top Division", top_div)

    st.divider()

    # ── Chase alerts table ───────────────────────────────────
    chase_df = df[_safe_col(df, "Sample Reminder").str.startswith("⚠️", na=False)]
    if not chase_df.empty:
        st.subheader(f"⚠️ Chase Alerts ({len(chase_df)})")
        for _, row in chase_df.iterrows():
            st.markdown(
                f'<div class="chase-row">'
                f'<b>{row.get("Vendor Name","?")}</b> — {row.get("Subject","")[:60]}'
                f'&nbsp;&nbsp;&nbsp;<span style="color:#856404">{row.get("Sample Reminder","")}</span>'
                f'</div>',
                unsafe_allow_html=True
            )
        st.divider()

    # ── Charts row ───────────────────────────────────────────
    col_a, col_b, col_c = st.columns(3)

    with col_a:
        st.subheader("By Intent")
        if "Intent" in df.columns:
            ic = df["Intent"].value_counts().reset_index()
            ic.columns = ["Intent", "Count"]
            st.bar_chart(ic.set_index("Intent"))

    with col_b:
        st.subheader("By Division")
        if "Division" in df.columns:
            dc = df["Division"].value_counts().reset_index()
            dc.columns = ["Division", "Count"]
            st.bar_chart(dc.set_index("Division"))

    with col_c:
        st.subheader("Sample Status")
        if "Sample Status" in df.columns:
            sc = df["Sample Status"].value_counts().reset_index()
            sc.columns = ["Status", "Count"]
            st.bar_chart(sc.set_index("Status"))

    st.divider()

    # ── Recent activity ──────────────────────────────────────
    st.subheader("Recent Activity")
    display_cols = [c for c in
        ["Sent Date", "Vendor Name", "Subject", "Division", "Intent",
         "Reply Needed", "Sample Status", "PO Number", "AWB No"]
        if c in df.columns]
    recent = df.sort_values("Sent Date", ascending=False).head(10) if "Sent Date" in df.columns else df.head(10)
    st.dataframe(recent[display_cols], use_container_width=True, hide_index=True)

    # ── Vendor breakdown ─────────────────────────────────────
    if "Vendor Name" in df.columns:
        st.divider()
        st.subheader("Vendor Breakdown")
        vendor_stats = (
            df[df["Vendor Name"] != ""]
            .groupby("Vendor Name")
            .agg(
                Threads=("Subject", "count"),
                Reply_Needed=("Reply Needed", lambda x: (x == "Yes").sum()),
                Latest=("Sent Date", "max"),
            )
            .sort_values("Threads", ascending=False)
            .head(15)
        )
        st.dataframe(vendor_stats, use_container_width=True)


# ════════════════════════════════════════════════════════════
# PAGE: SYNC GMAIL
# ════════════════════════════════════════════════════════════
elif page == "🔄 Sync Gmail":
    st.title("🔄 Sync Gmail Inbox")
    st.write("Fetch the latest threads, analyse with GPT, and write to your Google Sheet.")

    col_cfg1, col_cfg2 = st.columns(2)
    with col_cfg1:
        max_threads = st.slider("Max threads to fetch", 1, 200, 20)
    with col_cfg2:
        st.write("")
        st.write("")
        dry_run = st.checkbox("Dry Run (read-only — don't write to Sheets)")

    if st.button("🚀 Start Sync", type="primary"):
        with st.spinner("Connecting to Google…"):
            gmail_svc, sheets_svc, creds = get_google_services()
            gmail_reader.DRY_RUN = dry_run
            vendor_db = load_vendor_db(sheets_svc)

            if not dry_run:
                _ensure_tab(sheets_svc, SHEET_TAB, LOGS_HEADERS)
                _ensure_tab(sheets_svc, ERROR_TAB, ERROR_HEADERS)

            thread_map, subject_map = load_existing_rows(sheets_svc)

        with st.spinner("Fetching thread list…"):
            result       = gmail_svc.users().threads().list(
                userId="me", maxResults=max_threads, q="in:inbox"
            ).execute()
            gmail_threads = result.get("threads", [])

        if not gmail_threads:
            st.info("No threads found.")
            st.stop()

        st.success(f"Found **{len(gmail_threads)}** threads.")

        with st.spinner("Fetching metadata (parallel)…"):
            all_tids = [t["id"] for t in gmail_threads]
            # ✅ Thread-safe version — builds a fresh service per worker
            meta_map = fetch_all_metadata_safe(creds, all_tids)

        progress_bar = st.progress(0)
        status_area  = st.empty()
        log_lines    = []

        new_rows   = []
        updated    = backfilled = added = skipped = errors = 0
        total      = len(gmail_threads)
        now_str    = datetime.now().strftime("%Y-%m-%d %H:%M")

        for i, thread in enumerate(gmail_threads, 1):
            tid        = thread["id"]
            meta       = meta_map.get(tid, {})
            curr_count = meta.get("count",   0)
            meta_subj  = meta.get("subject", "")
            subj_key   = meta_subj.lower()
            status_msg = ""

            progress_bar.progress(int(i / total * 100),
                                  text=f"Processing {i}/{total}: {meta_subj[:50]}")

            try:
                # ── CASE 1: Thread ID already in sheet ──
                if tid in thread_map:
                    existing  = thread_map[tid]
                    old_count = existing["message_count"]
                    if curr_count <= old_count:
                        skipped   += 1
                        status_msg = f"⏭️ No change: {meta_subj[:40]}"
                    else:
                        row_data = process_thread(gmail_svc, tid, vendor_db)
                        if row_data:
                            update_existing_row(sheets_svc, existing["sheet_row"], row_data)
                            thread_map[tid]["message_count"] = row_data["Thread Messages"]
                            updated   += 1
                            status_msg = f"🔄 Updated: {row_data['Subject'][:40]}"

                # ── CASE 2: Subject match, no Thread ID ──
                elif subj_key and subj_key in subject_map:
                    existing  = subject_map[subj_key]
                    old_count = existing["message_count"]
                    if curr_count <= old_count:
                        if not dry_run:
                            # Use col_letter() — not hardcoded "O"/"N"
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
                        status_msg  = f"⏭️ TID stored: {meta_subj[:40]}"
                    else:
                        row_data = process_thread(gmail_svc, tid, vendor_db)
                        if row_data:
                            update_existing_row(sheets_svc, existing["sheet_row"], row_data, backfill_tid=True)
                            thread_map[tid] = existing
                            thread_map[tid]["message_count"] = row_data["Thread Messages"]
                            updated    += 1
                            backfilled += 1
                            status_msg  = f"🔄 Updated + TID: {row_data['Subject'][:40]}"

                # ── CASE 3: Brand new thread ──
                else:
                    if curr_count < 2:
                        skipped   += 1
                        status_msg = f"⏭️ Single msg: {meta_subj[:40]}"
                    else:
                        row_data = process_thread(gmail_svc, tid, vendor_db)
                        if row_data is None:
                            skipped   += 1
                            status_msg = f"⏭️ No body: {meta_subj[:40]}"
                        else:
                            # ✅ Use _build_new_row — all 24 cols, no hardcoded list
                            new_rows.append(_build_new_row(row_data, now_str))
                            subject_map[row_data["Subject"].lower()] = {
                                "sheet_row": None, "message_count": curr_count,
                                "thread_id": tid,  "subject": row_data["Subject"],
                            }
                            added     += 1
                            status_msg = f"🆕 Added: {row_data['Subject'][:40]}"

            except Exception as exc:
                errors += 1
                record_error(tid, meta_subj, "streamlit_sync", exc)
                status_msg = f"❌ Error: {meta_subj[:40]} — {str(exc)[:60]}"

            log_lines.append(status_msg)
            # Show last 8 log lines live
            if i % 3 == 0 or i == total:
                status_area.code("\n".join(log_lines[-8:]), language=None)

        # ── Final writes ──
        if not dry_run:
            if new_rows:
                append_new_rows(sheets_svc, new_rows)
            flush_error_log(sheets_svc)

        cs = cache_stats()
        write_audit_log({
            "threads_fetched": total,
            "added":           added,
            "updated":         updated,
            "backfilled":      backfilled,
            "skipped":         skipped,
            "errors":          errors,
            "cache_entries":   cs.get("cached_entries", 0),
        })

        progress_bar.empty()
        status_area.empty()

        if dry_run:
            st.info("🔍 DRY RUN — no data was written to Google Sheets.")
        else:
            st.success("✅ Sync Complete!")
            st.balloons()

        m1, m2, m3, m4, m5 = st.columns(5)
        m1.metric("🆕 Added",   added)
        m2.metric("🔄 Updated", updated)
        m3.metric("🔗 Backfilled", backfilled)
        m4.metric("⏭️ Skipped",  skipped)
        m5.metric("❌ Errors",   errors)

        # Force dashboard refresh
        st.session_state.df_logs = None


# ════════════════════════════════════════════════════════════
# PAGE: THREAD VIEWER
# ════════════════════════════════════════════════════════════
elif page == "🗂️ Thread Viewer":
    st.title("🗂️ Thread Viewer")

    df = st.session_state.df_logs if st.session_state.df_logs is not None else load_data_from_sheets()

    if df.empty:
        st.info("No data found. Run a sync first.")
        st.stop()

    # ── Filters ──────────────────────────────────────────────
    st.subheader("Filters")
    f1, f2, f3, f4, f5 = st.columns(5)

    with f1:
        intent_opts = ["All"] + sorted(df["Intent"].dropna().unique().tolist()) if "Intent" in df.columns else ["All"]
        intent_f    = st.selectbox("Intent", intent_opts)

    with f2:
        reply_f = st.selectbox("Reply Needed", ["All", "Yes", "No"])

    with f3:
        vendor_opts = ["All"] + sorted(df[df.get("Vendor Name", pd.Series()) != ""]["Vendor Name"].dropna().unique().tolist()) if "Vendor Name" in df.columns else ["All"]
        vendor_f    = st.selectbox("Vendor", vendor_opts)

    with f4:
        div_opts = ["All"] + sorted(df["Division"].dropna().unique().tolist()) if "Division" in df.columns else ["All"]
        div_f    = st.selectbox("Division", div_opts)

    with f5:
        sample_opts = ["All"] + sorted(df["Sample Status"].dropna().unique().tolist()) if "Sample Status" in df.columns else ["All"]
        sample_f    = st.selectbox("Sample Status", sample_opts)

    fdf = df.copy()
    if intent_f  != "All" and "Intent"        in fdf.columns: fdf = fdf[fdf["Intent"]        == intent_f]
    if reply_f   != "All" and "Reply Needed"  in fdf.columns: fdf = fdf[fdf["Reply Needed"]  == reply_f]
    if vendor_f  != "All" and "Vendor Name"   in fdf.columns: fdf = fdf[fdf["Vendor Name"]   == vendor_f]
    if div_f     != "All" and "Division"      in fdf.columns: fdf = fdf[fdf["Division"]      == div_f]
    if sample_f  != "All" and "Sample Status" in fdf.columns: fdf = fdf[fdf["Sample Status"] == sample_f]

    st.caption(f"Showing **{len(fdf)}** of **{len(df)}** threads")
    st.divider()

    # ── Thread cards ─────────────────────────────────────────
    for idx, row in fdf.iterrows():
        vendor  = row.get("Vendor Name", "Unknown Vendor") or "Unknown Vendor"
        subject = row.get("Subject",     "No Subject")     or "No Subject"
        sent    = row.get("Sent Date",   "")               or ""
        intent  = row.get("Intent",      "")               or ""

        label = f"{'⚠️ ' if str(row.get('Sample Reminder','')).startswith('⚠️') else ''}{sent[:10]}  |  {vendor}  |  {subject[:60]}"

        with st.expander(label):
            # ── Row 1: metadata badges ──
            badge_html = _intent_badge(intent) if intent else ""
            rn = row.get("Reply Needed", "No")
            rn_html = ('<span class="badge badge-chase">⚠️ Reply Needed</span>'
                       if rn == "Yes" else
                       '<span class="badge badge-sample">✅ No Reply</span>')
            st.markdown(f"{badge_html} &nbsp; {rn_html}", unsafe_allow_html=True)
            st.markdown("")

            # ── Main 3-column layout ──
            left, mid, right = st.columns([3, 2, 2])

            with left:
                st.markdown("**AI Overview**")
                overview = row.get("AI Overview", "") or ""
                for line in overview.split("\n"):
                    if line.strip():
                        st.markdown(line)

                reminder = row.get("Sample Reminder", "") or ""
                if reminder.startswith("⚠️"):
                    st.warning(reminder)

            with mid:
                st.markdown("**Thread Details**")
                details = {
                    "Division":    row.get("Division", ""),
                    "Style No":    row.get("Style No", ""),
                    "Colour":      row.get("Colour", ""),
                    "PO Number":   row.get("PO Number", ""),
                    "Sender":      row.get("Sender", ""),
                    "CC":          row.get("CC", ""),
                    "Messages":    row.get("Thread Messages", ""),
                    "Sent Date":   sent,
                    "Last Updated":row.get("Last Updated", ""),
                }
                for k, v in details.items():
                    if v:
                        st.text(f"{k}: {v}")

                st.markdown("**Vendor**")
                st.text(f"Name:  {row.get('Vendor Name', '')}")
                st.text(f"Class: {row.get('Partner Classification', '')}")

            with right:
                st.markdown("**Shipment**")
                awb      = row.get("AWB No", "")           or ""
                carrier  = row.get("Shipment Company", "") or ""
                ship_dt  = row.get("Shipment Date", "")    or ""
                if awb or carrier:
                    st.success(f"📦 {carrier}  {awb}")
                    if ship_dt:
                        st.caption(f"Shipped: {ship_dt}")
                else:
                    st.caption("No shipment info")

                st.markdown("**Sample**")
                ss = row.get("Sample Status", "None") or "None"
                ss_color = {
                    "Dispatched": "🟡", "Received": "🟢", "Approved": "✅",
                    "Rejected": "🔴", "Pending": "🟠", "None": "⚪"
                }.get(ss, "⚪")
                st.markdown(f"{ss_color} {ss}")

                attachments = row.get("Attachments", "") or ""
                links       = row.get("Shared Links", "") or ""
                if attachments:
                    st.markdown("**Attachments**")
                    st.caption(attachments)
                if links:
                    st.markdown("**Shared Links**")
                    st.caption(links)

            # ── Reply Draft — full width below columns ───────
            reply_draft = row.get("Reply Draft", "") or ""
            if reply_draft:
                st.markdown("---")
                st.markdown("**✉️ Reply Draft**")
                st.markdown(
                    f'<div class="reply-box">{reply_draft}</div>',
                    unsafe_allow_html=True
                )
                st.download_button(
                    label="📋 Copy / Download reply",
                    data=reply_draft,
                    file_name=f"reply_{subject[:30].replace(' ','_')}.txt",
                    mime="text/plain",
                    key=f"dl_{idx}",
                )
            else:
                st.markdown("---")
                st.caption("No reply draft — thread may not require a reply.")