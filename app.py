import os
import streamlit as st
import pandas as pd
from datetime import datetime
from googleapiclient.discovery import build

# Import building blocks from the core script
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
    fetch_all_metadata,
    _ensure_tab,
    LOGS_HEADERS,
    ERROR_TAB,
    ERROR_HEADERS,
    record_error,
    cache_stats,
    write_audit_log
)
import gmail_reader

# Configuration & Page Setup
st.set_page_config(
    page_title="Merchandising AI Agent",
    page_icon="🤖",
    layout="wide",
)

st.title("🤖 Merchandising Email AI Agent")

# Sidebar navigation
st.sidebar.header("Navigation")
page = st.sidebar.radio("Go to", ["Dashboard", "Sync Gmail", "Thread Viewer"])

# Initialize session state for cached data
if 'df_logs' not in st.session_state:
    st.session_state.df_logs = None

def get_google_services():
    creds = authenticate()
    gmail_svc = build("gmail", "v1", credentials=creds)
    sheets_svc = build("sheets", "v4", credentials=creds)
    return gmail_svc, sheets_svc

def load_data_from_sheets():
    _, sheets_svc = get_google_services()
    try:
        result = sheets_svc.spreadsheets().values().get(
            spreadsheetId=SHEET_ID,
            range=f"'{SHEET_TAB}'!A:R"
        ).execute()
        rows = result.get("values", [])
        if len(rows) > 1:
            headers = rows[0]
            data = rows[1:]
            df = pd.DataFrame(data, columns=headers)
            st.session_state.df_logs = df
            return df
        return pd.DataFrame()
    except Exception as e:
        st.error(f"Failed to load data from Google Sheets: {e}")
        return pd.DataFrame()

# Main Routing logic
if page == "Dashboard":
    st.header("📊 Overview Dashboard")
    
    df = st.session_state.df_logs if st.session_state.df_logs is not None else load_data_from_sheets()
    
    if df.empty:
        st.info("No data found. Go to 'Sync Gmail' to pull in some threads.")
    else:
        # Layout top-level metrics
        col1, col2, col3, col4 = st.columns(4)
        col1.metric("Total Threads Processed", len(df))
        
        requires_reply_count = len(df[df.get("Reply Needed", "") == "Yes"])
        col2.metric("Requires Reply", requires_reply_count)
        
        # Safe metric calculation for Intent
        if "Intent" in df.columns:
            top_intent = df['Intent'].value_counts().idxmax() if not df['Intent'].empty else "N/A"
            col3.metric("Top Intent", top_intent)
            
        if "Vendor Name" in df.columns:
            top_vendor = df[df['Vendor Name'] != '']['Vendor Name'].value_counts().idxmax() if not df[df['Vendor Name'] != ''].empty else "N/A"
            col4.metric("Top Vendor", top_vendor)
        
        st.divider()
        
        st.subheader("Recent Activity Summary")
        if "Sent Date" in df.columns:
            df_recent = df.sort_values(by="Sent Date", ascending=False).head(5)
            st.dataframe(df_recent[["Sent Date", "Vendor Name", "Subject", "Intent", "Reply Needed"]], use_container_width=True)
            
        st.divider()
        col_chart1, col_chart2 = st.columns(2)
        
        with col_chart1:
            st.subheader("Threads by Intent")
            if "Intent" in df.columns:
                intent_counts = df['Intent'].value_counts().reset_index()
                intent_counts.columns = ['Intent', 'Count']
                st.bar_chart(intent_counts.set_index('Intent'))
                
        with col_chart2:
            st.subheader("Threads by Division")
            if "Division" in df.columns:
                div_counts = df['Division'].value_counts().reset_index()
                div_counts.columns = ['Division', 'Count']
                st.bar_chart(div_counts.set_index('Division'))

elif page == "Sync Gmail":
    st.header("🔄 Sync Gmail Inbox")
    st.write("Click below to fetch the latest threads from your inbox, analyze them using Ollama, and write the output to your Google Sheet.")
    
    col_config1, col_config2 = st.columns(2)
    with col_config1:
        max_threads = st.slider("Max threads to fetch", min_value=1, max_value=100, value=20)
    with col_config2:
        st.write("") # Spacing
        st.write("") # Spacing
        dry_run = st.checkbox("Dry Run Mode (Don't write to Sheets)")
        
    if st.button("Start Sync"):
        if True: # Bypass OpenAI key check for local Ollama
            with st.spinner(f"Connecting and Fetching up to {max_threads} threads... this may take a minute."):
                # 1. Setup Phase
                gmail_reader.DRY_RUN = dry_run
                gmail_svc, sheets_svc = get_google_services()
                vendor_db = load_vendor_db(sheets_svc)
                if not dry_run:
                    _ensure_tab(sheets_svc, SHEET_TAB, LOGS_HEADERS)
                    _ensure_tab(sheets_svc, ERROR_TAB, ERROR_HEADERS)
                thread_map, subject_map = load_existing_rows(sheets_svc)
                
                # Fetch thread list
                result = gmail_svc.users().threads().list(userId="me", maxResults=max_threads, q="in:inbox").execute()
                gmail_threads = result.get("threads", [])
                
                if not gmail_threads:
                    st.info("No threads found in inbox.")
                else:
                    st.success(f"Found {len(gmail_threads)} threads.")
                    
                    # Fetch metadata for fast skip-checking
                    all_tids = [t["id"] for t in gmail_threads]
                    meta_map = fetch_all_metadata(gmail_svc, all_tids)
                    
                    # Layout containers for live logs
                    progress_text = "Processing Threads..."
                    my_bar = st.progress(0, text=progress_text)
                    log_container = st.empty()
                    
                    # Counters
                    new_rows = []
                    updated = 0
                    backfilled = 0
                    added = 0
                    skipped = 0
                    errors = 0
                    total = len(gmail_threads)
                    now_str = datetime.now().strftime("%Y-%m-%d %H:%M")
                    
                    for i, thread in enumerate(gmail_threads, 1):
                        tid = thread["id"]
                        meta = meta_map.get(tid, {})
                        curr_count = meta.get("count", 0)
                        meta_subj = meta.get("subject", "")
                        subj_key = meta_subj.lower()
                        status_msg = ""
                        
                        my_bar.progress(int(i/total * 100), text=f"Processing {i}/{total}: {meta_subj[:40]}")
                        
                        try:
                            if tid in thread_map:
                                existing = thread_map[tid]
                                old_count = existing["message_count"]
                                if curr_count <= old_count:
                                    skipped += 1
                                    status_msg = f"Skipped (No change): {meta_subj[:30]}"
                                else:
                                    row_data = process_thread(gmail_svc, tid, vendor_db)
                                    if row_data:
                                        if not dry_run:
                                            update_existing_row(sheets_svc, existing["sheet_row"], row_data)
                                        thread_map[tid]["message_count"] = row_data["Thread Messages"]
                                        updated += 1
                                        status_msg = f"Updated: {row_data['Subject'][:30]}"
                            elif subj_key and subj_key in subject_map:
                                existing = subject_map[subj_key]
                                old_count = existing["message_count"]
                                if curr_count <= old_count:
                                    # Just write tid silently
                                    if not dry_run:
                                        sheets_svc.spreadsheets().values().batchUpdate(
                                            spreadsheetId=SHEET_ID,
                                            body={"valueInputOption": "RAW", "data": [
                                                {"range": f"'{SHEET_TAB}'!O{existing['sheet_row']}", "values": [[tid]]},
                                                {"range": f"'{SHEET_TAB}'!N{existing['sheet_row']}", "values": [[str(curr_count)]]},
                                            ]}
                                        ).execute()
                                    skipped += 1
                                    backfilled += 1
                                    status_msg = f"Skipped (TID Backfilled): {meta_subj[:30]}"
                                else:
                                    row_data = process_thread(gmail_svc, tid, vendor_db)
                                    if row_data:
                                        if not dry_run:
                                            update_existing_row(sheets_svc, existing["sheet_row"], row_data, backfill_tid=True)
                                        updated += 1
                                        backfilled += 1
                                        status_msg = f"Updated + TID Backfilled: {row_data['Subject'][:30]}"
                            else:
                                if curr_count < 2:
                                    skipped += 1
                                    status_msg = f"Skipped (Single Msg): {meta_subj[:30]}"
                                else:
                                    row_data = process_thread(gmail_svc, tid, vendor_db)
                                    if row_data is None:
                                        skipped += 1
                                        status_msg = f"Skipped (Unreadable): {meta_subj[:30]}"
                                    else:
                                        new_rows.append([
                                            row_data["Subject"], row_data["Sender"], row_data["CC"],
                                            row_data["Division"], row_data["Style No"], row_data["Colour"],
                                            row_data["Vendor Name"], row_data["Partner Class"], row_data["Shipment Company"],
                                            row_data["AWB No"], row_data["Shipment Date"], row_data["Sent Date"],
                                            row_data["AI Overview"], row_data["Thread Messages"], row_data["Thread ID"],
                                            now_str, row_data["Intent"], row_data["Reply Needed"],
                                        ])
                                        subject_map[row_data["Subject"].lower()] = {
                                            "sheet_row": None, "message_count": curr_count,
                                            "thread_id": tid, "subject": row_data["Subject"],
                                        }
                                        added += 1
                                        status_msg = f"Added: {row_data['Subject'][:30]}"
                        except Exception as exc:
                            errors += 1
                            record_error(tid, meta_subj, "process_thread_streamlit", exc)
                            status_msg = f"Error: {meta_subj[:30]}"
                            
                        # Refresh log container
                        if i % 5 == 0 or i == total:
                            log_container.text(f"Last Action: {status_msg}\nAdded: {added} | Updated: {updated} | Skipped: {skipped} | Errors: {errors}")

                    # Final Writes + Audit Log
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
                    
                    if not dry_run:
                        if new_rows:
                            append_new_rows(sheets_svc, new_rows)
                        flush_error_log(sheets_svc)
                    
                    my_bar.empty()
                    st.success("Sync Complete!")
                    st.balloons()
                    
                    if dry_run:
                        st.info("DRY RUN MODE: No data was written to Google Sheets.")
                    
                    col1, col2, col3, col4, col5 = st.columns(5)
                    col1.metric("New Rows Added", added)
                    col2.metric("Rows Updated", updated)
                    col3.metric("Skipped", skipped + backfilled)
                    col4.metric("Errors", errors)
                    col5.metric("Cache Hits/Entries", cs.get("cached_entries", 0))
                    
                    # Force a dashboard refresh on next load
                    st.session_state.df_logs = None

elif page == "Thread Viewer":
    st.header("🗂️ Thread Viewer")
    
    df = st.session_state.df_logs if st.session_state.df_logs is not None else load_data_from_sheets()
    
    if df.empty:
        st.info("No data found. Please run a sync first.")
    else:
        # Filtering Options
        st.subheader("Filter Threads")
        col1, col2, col3 = st.columns(3)
        with col1:
            intent_filter = st.selectbox("Filter by Intent", ["All"] + list(df['Intent'].unique()))
        with col2:
            reply_filter = st.selectbox("Reply Needed", ["All", "Yes", "No"])
        with col3:
            vendor_filter = st.selectbox("Vendor Name", ["All"] + list(df[df['Vendor Name'] != '']['Vendor Name'].unique()))
            
        filtered_df = df.copy()
        if intent_filter != "All":
            filtered_df = filtered_df[filtered_df["Intent"] == intent_filter]
        if reply_filter != "All":
            filtered_df = filtered_df[filtered_df["Reply Needed"] == reply_filter]
        if vendor_filter != "All":
            filtered_df = filtered_df[filtered_df["Vendor Name"] == vendor_filter]
            
        st.write(f"Showing {len(filtered_df)} of {len(df)} threads")
        
        # Display Results
        for idx, row in filtered_df.iterrows():
            with st.expander(f"{row.get('Sent Date', '')} | {row.get('Vendor Name', 'Unknown Vendor')} | {row.get('Subject', 'No Subject')}"):
                col_left, col_right = st.columns([2, 1])
                
                with col_left:
                    st.markdown("**AI Overview:**")
                    st.markdown(row.get("AI Overview", "N/A"))
                    
                    st.markdown("**Core Data:**")
                    st.text(f"Style: {row.get('Style No', 'N/A')} | Color: {row.get('Colour', 'N/A')} | Division: {row.get('Division', 'N/A')}")
                    
                with col_right:
                    st.info(f"**Intent:** {row.get('Intent', 'N/A')}")
                    
                    reply_needed = row.get("Reply Needed", "No")
                    if reply_needed == "Yes":
                        st.warning("⚠️ Reply Needed")
                    else:
                        st.success("✅ No Reply Needed")
                        
                    if row.get("AWB No", ""):
                        st.success(f"📦 Shipment: {row.get('Shipment Company', '')} {row.get('AWB No', '')}")
                        
                # Just a placeholder for the future
                # The index is used to guarantee uniqueness even if Thread ID is None
                st.button("Draft AI Reply", key=f"draft_{idx}_{row.get('Thread ID', '')}", disabled=True, help="Feature coming soon!")

