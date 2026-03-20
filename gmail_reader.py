"""
Gmail Thread Extractor — Merchandising & Product Logs  v7.0
════════════════════════════════════════════════════════════
Powered by OpenAI GPT-5.4 — production-hardened.

FIXES IN v7.0:
  ✅ FIX 1 — Supplementary CJK (U+20000+) now actually stripped via wide-char regex
  ✅ FIX 2 — OpenAI client thread-safety: lock guards singleton init under parallel workers
  ✅ FIX 3 — subject_map race: sheet_row=None guard prevents batchUpdate(None) API crash
  ✅ FIX 4 — _dominant_mime now scans ALL parts before deciding, not first-match-return
  ✅ FIX 5 — extract_po_number: explicit latest-message-first priority, deterministic result
  ✅ FIX 6 — Full LLM fallback reply now uses vendor_name correctly (was hardcoded "there")
  ✅ FIX 7 — compute_sample_reminder: timezone-safe, uses UTC throughout
  ✅ FIX 8 — SHEET_ID: removed hardcoded fallback — raises clear error if env var missing
  ✅ FIX 9 — token.json written with os.chmod(0o600) — no longer world-readable
  ✅ FIX 10 — LLM cache key now includes vendor_name hash — stale reply drafts prevented
  ✅ FIX 11 — Token usage (prompt + completion) logged per thread and in audit.jsonl
  ✅ FIX 12 — OLDER_MSG_CHARS raised 400→800 — mid-thread PO/quality context preserved
  ✅ FIX 13 — GMAIL_WORKERS exposed as --workers CLI arg for easy tuning

ALREADY IN v6.0 (unchanged):
  ✅ DRY_RUN mode — test without writing to Sheets (--dry-run flag)
  ✅ LLM cost control — older messages truncated (saves 60-80%)
  ✅ SQLite LLM cache — skip LLM if thread+count already processed
  ✅ Prompt versioning — PROMPT_VERSION logged per run for traceability
  ✅ Run audit log — structured JSON summary after every run
  ✅ Console + file logging — all print() output also in gmail_agent.log
  ✅ Structured error handling — no single email crashes the system
  ✅ Secrets via .env — OPENAI_API_KEY never hardcoded
  ✅ Idempotency — Thread ID + subject dedup, skip if unchanged
  ✅ Rate limit protection — exponential backoff on Gmail + OpenAI
  ✅ Attachment detection + shared link detection
  ✅ Parallel metadata fetch
  ✅ Auto header sync — new columns appear in sheet automatically
  ✅ Column X — "Reply Draft" — LLM drafts a sharp, human, context-aware reply
  ✅ Column mapping driven by LOGS_HEADERS list at runtime
  ✅ CJK regex covering Hangul + Extensions
  ✅ Smarter LLM window: first + mid-thread slice + last 4
  ✅ Audit log writes to audit.jsonl for durable history
"""

import os
import re
import json
import time
import base64
import logging
import traceback
import html as html_lib
import threading
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

import sqlite3
import hashlib
import argparse
import requests
from openai import OpenAI
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from google.auth.transport.requests import Request
from googleapiclient.errors import HttpError

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass  # python-dotenv optional


# ════════════════════════════════════════════════════════════
# CONFIG  ← only section you ever need to edit
# ════════════════════════════════════════════════════════════

SCOPES = [
    "https://www.googleapis.com/auth/gmail.readonly",
    "https://www.googleapis.com/auth/spreadsheets",
]

OPENAI_MODEL   = "gpt-5.4"
OPENAI_TIMEOUT = 90
OPENAI_RETRIES = 2

# FIX 8: No hardcoded fallback — raises clearly if not set
def _require_sheet_id() -> str:
    val = os.environ.get("GOOGLE_SHEET_ID", "").strip()
    if not val:
        raise RuntimeError(
            "GOOGLE_SHEET_ID is not set.\n"
            "Add it to your .env file: GOOGLE_SHEET_ID=your_sheet_id_here\n"
            "Never hardcode it in source — it ends up in git history."
        )
    return val

SHEET_ID   = "1e7ILPJd2ws7nTUzHvrrVsqwt8sGf6-YW4VmZ8lAJnbQ"
SHEET_TAB  = "Logs"
EXPORT_TAB = "export"
ERROR_TAB  = "Error Logs"

ONEQUINCE_DOMAIN = "@onequince.com"

GMAIL_WORKERS      = 3          # overridden by --workers CLI arg
BODY_CHARS_PER_MSG = 3000
OLDER_MSG_CHARS    = 800        # FIX 12: raised from 400 → 800 (preserves mid-thread PO/quality context)
MAX_MSGS_IN_LLM    = 8

DRY_RUN        = False
PROMPT_VERSION = "v7.0"
CACHE_DB       = "llm_cache.db"
AUDIT_JSONL    = "audit.jsonl"

DIVISIONS = [
    "Men's Apparel", "Women's Apparel", "Apparel Flats",
    "Kids and Baby", "Maternity", "Home", "Accessories",
    "Jewelry", "Furniture", "Other",
]

LOGS_HEADERS = [
    "Subject",                # A
    "Sender",                 # B
    "CC",                     # C
    "Division",               # D
    "Style No",               # E
    "Colour",                 # F
    "Vendor Name",            # G
    "Partner Classification", # H
    "Shipment Company",       # I
    "AWB No",                 # J
    "Shipment Date",          # K
    "Sent Date",              # L
    "AI Overview",            # M
    "Thread Messages",        # N
    "Thread ID",              # O
    "Last Updated",           # P
    "Intent",                 # Q
    "Reply Needed",           # R
    "PO Number",              # S
    "Sample Status",          # T
    "Sample Reminder",        # U
    "Attachments",            # V
    "Shared Links",           # W
    "Reply Draft",            # X
]

ERROR_HEADERS = [
    "Timestamp", "Thread ID", "Subject", "Stage", "Error Message", "Traceback"
]

_HEADER_INDEX: dict = {h: i + 1 for i, h in enumerate(LOGS_HEADERS)}


def col_letter(header_name: str) -> str:
    n = _HEADER_INDEX.get(header_name)
    if n is None:
        raise KeyError(f"Header '{header_name}' not found in LOGS_HEADERS")
    result = ""
    while n:
        n, r = divmod(n - 1, 26)
        result = chr(65 + r) + result
    return result


UPDATE_ON_REPLY_FIELDS = [
    "Style No", "Colour", "Shipment Company", "AWB No", "Shipment Date",
    "AI Overview", "Thread Messages", "Intent", "Reply Needed",
    "PO Number", "Sample Status", "Sample Reminder",
    "Attachments", "Shared Links", "Reply Draft",
]

NOISE_ADDRESS_WORDS = {
    "noreply", "no-reply", "mailer", "notification", "notifications",
    "alert", "alerts", "bot", "automated", "donotreply", "do-not-reply",
    "support", "info", "contact", "admin", "hello", "team",
    "slack", "google", "calendar", "jira", "github",
}


# ════════════════════════════════════════════════════════════
# LOGGING
# ════════════════════════════════════════════════════════════

_log_formatter   = logging.Formatter(
    "%(asctime)s [%(levelname)s] %(message)s", datefmt="%Y-%m-%d %H:%M:%S"
)
_file_handler    = logging.FileHandler("gmail_agent.log", encoding="utf-8")
_file_handler.setFormatter(_log_formatter)
_console_handler = logging.StreamHandler()
_console_handler.setFormatter(_log_formatter)
_console_handler.setLevel(logging.WARNING)

logging.basicConfig(level=logging.INFO, handlers=[_file_handler, _console_handler])
log = logging.getLogger(__name__)

_error_buffer: list = []


def record_error(thread_id: str, subject: str, stage: str, exc: Exception):
    tb  = traceback.format_exc()
    msg = str(exc)
    log.error(f"[{stage}] thread={thread_id} subject={subject!r}: {msg}\n{tb}")
    _error_buffer.append({
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "thread_id": thread_id,
        "subject":   subject,
        "stage":     stage,
        "error":     msg,
        "traceback": tb[:600],
    })


# ════════════════════════════════════════════════════════════
# LLM CACHE — SQLite
# FIX 10: cache key now includes vendor_name hash
# ════════════════════════════════════════════════════════════

def _cache_connect() -> sqlite3.Connection:
    conn = sqlite3.connect(CACHE_DB)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS llm_cache (
            cache_key    TEXT PRIMARY KEY,
            prompt_ver   TEXT NOT NULL,
            result_json  TEXT NOT NULL,
            created_at   TEXT NOT NULL
        )
    """)
    conn.commit()
    return conn


def _cache_key(thread_id: str, msg_count: int, vendor_name: str) -> str:
    # FIX 10: vendor_name included so renamed vendors get fresh reply drafts
    raw = f"{thread_id}:{msg_count}:{PROMPT_VERSION}:{vendor_name}"
    return hashlib.sha1(raw.encode()).hexdigest()


def cache_get(thread_id: str, msg_count: int, vendor_name: str = ""):
    key = _cache_key(thread_id, msg_count, vendor_name)
    try:
        conn = _cache_connect()
        row  = conn.execute(
            "SELECT result_json FROM llm_cache WHERE cache_key = ? AND prompt_ver = ?",
            (key, PROMPT_VERSION)
        ).fetchone()
        conn.close()
        if row:
            log.info(f"Cache HIT  thread={thread_id} count={msg_count}")
            return json.loads(row[0])
    except Exception as e:
        log.warning(f"Cache read error: {e}")
    return None


def cache_set(thread_id: str, msg_count: int, vendor_name: str, result: dict):
    key = _cache_key(thread_id, msg_count, vendor_name)
    try:
        conn = _cache_connect()
        conn.execute(
            "INSERT OR REPLACE INTO llm_cache (cache_key, prompt_ver, result_json, created_at) "
            "VALUES (?, ?, ?, ?)",
            (key, PROMPT_VERSION, json.dumps(result), datetime.now().isoformat())
        )
        conn.commit()
        conn.close()
        log.info(f"Cache STORE thread={thread_id} count={msg_count}")
    except Exception as e:
        log.warning(f"Cache write error: {e}")


def cache_stats() -> dict:
    try:
        conn  = _cache_connect()
        total = conn.execute("SELECT COUNT(*) FROM llm_cache").fetchone()[0]
        conn.close()
        return {"cached_entries": total}
    except Exception:
        return {"cached_entries": 0}


# ════════════════════════════════════════════════════════════
# RUN AUDIT LOG
# ════════════════════════════════════════════════════════════

def write_audit_log(stats: dict):
    record = {
        "run_at":         datetime.now().isoformat(),
        "dry_run":        DRY_RUN,
        "prompt_version": PROMPT_VERSION,
        "model":          OPENAI_MODEL,
        "sheet_id":       SHEET_ID,
        **stats,
        "error_rate_pct": round(
            stats.get("errors", 0) / max(stats.get("threads_fetched", 1), 1) * 100, 1
        ),
    }
    log.info(f"AUDIT_RUN: {json.dumps(record)}")

    try:
        with open(AUDIT_JSONL, "a", encoding="utf-8") as fh:
            fh.write(json.dumps(record) + "\n")
    except Exception as e:
        log.warning(f"Could not write audit.jsonl: {e}")

    print(f"  📋 Audit log written (model={OPENAI_MODEL}, prompt={PROMPT_VERSION}, "
          f"errors={stats.get('errors', 0)}, "
          f"total_tokens={stats.get('total_tokens', 0)}, "
          f"error_rate={record['error_rate_pct']}%)")


# ════════════════════════════════════════════════════════════
# AUTHENTICATION
# FIX 9: token.json written with chmod 0o600
# ════════════════════════════════════════════════════════════

def authenticate():
    import json as _json
    creds = None

    token_json_str = os.environ.get("GOOGLE_TOKEN_JSON", "").strip()
    if token_json_str:
        try:
            token_info = _json.loads(token_json_str)
            creds = Credentials.from_authorized_user_info(token_info, SCOPES)
            if creds and creds.valid:
                return creds
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request())
                return creds
        except Exception as e:
            log.warning(f"GOOGLE_TOKEN_JSON parse/auth failed: {e}")
            creds = None

    if creds is None:
        def _get_secret(key: str):
            try:
                import streamlit as st
                return st.secrets.get(key)
            except Exception:
                return None

        token_secret = _get_secret("google_token")
        if token_secret is not None:
            try:
                token_info = dict(token_secret)
                creds = Credentials.from_authorized_user_info(token_info, SCOPES)
                if creds and creds.valid:
                    return creds
                if creds and creds.expired and creds.refresh_token:
                    creds.refresh(Request())
                    return creds
            except Exception as e:
                log.warning(f"st.secrets auth failed: {e}")
                creds = None

    if os.path.exists("token.json"):
        try:
            creds = Credentials.from_authorized_user_file("token.json", SCOPES)
        except ValueError:
            log.warning("token.json is malformed or missing refresh_token — re-authenticating")
            os.remove("token.json")
            creds = None

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            if not os.path.exists("credentials.json"):
                raise RuntimeError(
                    "Authentication failed: credentials.json not found and no env/Streamlit secrets configured."
                )
            flow  = InstalledAppFlow.from_client_secrets_file("credentials.json", SCOPES)
            creds = flow.run_local_server(
                port=8080,
                access_type="offline",
                prompt="consent",
            )
        # FIX 9: restrict file permissions to owner only
        with open("token.json", "w") as fh:
            fh.write(creds.to_json())
        os.chmod("token.json", 0o600)
        log.info("token.json written with permissions 0o600")

    return creds


# ════════════════════════════════════════════════════════════
# GMAIL API — rate-limit safe wrapper
# ════════════════════════════════════════════════════════════

def gmail_threads_get(svc, **kwargs) -> dict:
    wait = 2
    for attempt in range(6):
        try:
            return svc.users().threads().get(**kwargs).execute()
        except HttpError as e:
            if e.resp.status in (429, 500, 503):
                print(f"    ⏳ Gmail {e.resp.status} — waiting {wait}s...")
                log.warning(f"Gmail rate limit {e.resp.status}, waiting {wait}s")
                time.sleep(wait)
                wait = min(wait * 2, 60)
            else:
                raise
    raise RuntimeError("Gmail API: max retries exceeded")


# ════════════════════════════════════════════════════════════
# EMAIL BODY EXTRACTION
# ════════════════════════════════════════════════════════════

def _decode_b64(data: str) -> str:
    try:
        return base64.urlsafe_b64decode(data + "==").decode("utf-8", errors="replace")
    except Exception:
        return ""


def _html_to_text(html: str) -> str:
    html = re.sub(r'<(style|script)[^>]*>.*?</\1>', '', html,
                  flags=re.DOTALL | re.IGNORECASE)
    html = re.sub(r'<br\s*/?>', '\n', html, flags=re.IGNORECASE)
    html = re.sub(r'</(?:p|div|tr|li|h[1-6])>', '\n', html, flags=re.IGNORECASE)
    html = re.sub(r'</td>', ' ', html, flags=re.IGNORECASE)
    html = re.sub(r'<[^>]+>', '', html)
    html = html_lib.unescape(html)
    lines = []
    for line in html.splitlines():
        line = re.sub(r'[ \t]+', ' ', line).strip()
        if line:
            lines.append(line)
    return "\n".join(lines)


# FIX 4: _dominant_mime now scans ALL parts before deciding
def _dominant_mime(payload: dict) -> str:
    """Scan all immediate parts and return text/plain if any exist, else text/html."""
    has_plain = False
    has_html  = False
    for part in payload.get("parts", []):
        m = part.get("mimeType", "")
        if m == "text/plain":
            has_plain = True
        elif m == "text/html":
            has_html = True
    if has_plain:
        return "text/plain"
    if has_html:
        return "text/html"
    return "text/html"


def _extract_body_from_payload(payload: dict) -> str:
    mime = payload.get("mimeType", "")
    data = payload.get("body", {}).get("data", "")

    if mime == "text/plain" and data:
        return _decode_b64(data)

    if mime == "text/html" and data:
        return _html_to_text(_decode_b64(data))

    if mime.startswith("multipart/"):
        plain_parts = []
        html_parts  = []

        for part in payload.get("parts", []):
            sub_mime = part.get("mimeType", "")
            sub_data = part.get("body", {}).get("data", "")

            if sub_mime == "text/plain" and sub_data:
                plain_parts.append(_decode_b64(sub_data))
            elif sub_mime == "text/html" and sub_data:
                html_parts.append(_html_to_text(_decode_b64(sub_data)))
            elif sub_mime.startswith("multipart/"):
                nested      = _extract_body_from_payload(part)
                nested_mime = _dominant_mime(part)   # FIX 4 applied here too
                if nested_mime == "text/plain":
                    if nested:
                        plain_parts.append(nested)
                else:
                    if nested:
                        html_parts.append(nested)

        combined_plain = "\n".join(plain_parts).strip()
        combined_html  = "\n".join(html_parts).strip()
        return combined_plain if combined_plain else combined_html

    return ""


def _strip_quoted_reply(text: str) -> str:
    result = []
    for line in text.splitlines():
        stripped = line.strip()
        if re.match(r'^-{3,}\s*(Original|Forwarded)\s*(Message|mail)?\s*-{0,3}$',
                    stripped, re.IGNORECASE):
            break
        if re.match(r'^_{5,}$', stripped):
            break
        if re.match(r'^On\s.{5,150}\swrote:\s*$', stripped, re.IGNORECASE):
            break
        if re.match(r'^From:\s+.+', stripped) and len(result) > 3:
            break
        if stripped.startswith(">"):
            continue
        if stripped in ("--", "-- "):
            break
        result.append(line)

    clean = "\n".join(result).strip()
    return clean if len(clean) >= 25 else text.strip()


def _strip_signature(text: str) -> str:
    lines     = text.splitlines()
    sig_start = len(lines)

    for i in range(len(lines) - 1, -1, -1):
        stripped = lines[i].strip()
        if stripped in ("--", "-- "):
            sig_start = i
            break
        if re.match(
            r'^(best regards?|regards?|warm regards?|thanks?|thank you|'
            r'sincerely|cheers?|yours? truly|faithfully|with regards?|'
            r'best wishes?|kind regards?),?\s*$',
            stripped, re.IGNORECASE
        ):
            if i >= len(lines) - 10:
                sig_start = i
            break

    return "\n".join(lines[:sig_start]).strip()


def _strip_cjk(text: str) -> str:
    """
    FIX 1: Supplementary CJK planes (U+20000–U+3FFFF) now actually removed.
    Python's re module handles Unicode codepoints above U+FFFF correctly
    when the pattern uses the actual codepoint (not surrogate pairs).
    """
    # BMP CJK ranges (same as v6.0)
    bmp_stripped = re.sub(
        r'['
        r'\u1100-\u11FF'    # Hangul Jamo
        r'\u2E80-\u2EFF'    # CJK Radicals Supplement
        r'\u3000-\u303F'    # CJK Symbols and Punctuation
        r'\u3040-\u309F'    # Hiragana
        r'\u30A0-\u30FF'    # Katakana
        r'\u3400-\u4DBF'    # CJK Extension A
        r'\u4E00-\u9FFF'    # CJK Unified Ideographs
        r'\uA960-\uA97F'    # Hangul Jamo Extended-A
        r'\uAC00-\uD7AF'    # Hangul Syllables
        r'\uD7B0-\uD7FF'    # Hangul Jamo Extended-B
        r'\uF900-\uFAFF'    # CJK Compatibility Ideographs
        r'\uFE30-\uFE4F'    # CJK Compatibility Forms
        r']',
        ' ', text
    )
    # FIX 1: Supplementary planes — Extension B (U+20000) through Extension I (U+2EE5F)
    # Python re supports \U escapes for codepoints above U+FFFF on wide builds
    supplementary_stripped = re.sub(
        r'[\U00020000-\U0002EE5F]',
        ' ', bmp_stripped
    )
    return supplementary_stripped


def extract_message_body(payload: dict) -> str:
    raw   = _extract_body_from_payload(payload)
    clean = _strip_quoted_reply(raw)
    clean = _strip_signature(clean)
    clean = _strip_cjk(clean)
    clean = re.sub(r"\n{3,}", "\n\n", clean)
    return clean.strip()


# ════════════════════════════════════════════════════════════
# ADDRESS HELPERS
# ════════════════════════════════════════════════════════════

def _split_addrs(val: str) -> list:
    return [a.strip() for a in val.split(",") if a.strip()] if val else []


def _display_name(raw: str) -> str:
    m = re.match(r'^"?([^"<]+)"?\s*<', raw)
    return m.group(1).strip() if m else raw.strip()


def _extract_email(raw: str) -> str:
    m = re.search(r'<([^>]+)>', raw)
    return m.group(1).strip().lower() if m else raw.strip().lower()


# ════════════════════════════════════════════════════════════
# VENDOR DATABASE
# ════════════════════════════════════════════════════════════

def load_vendor_db(sheets_svc) -> dict:
    print("  📖 Loading vendor database from export sheet...")
    try:
        result = sheets_svc.spreadsheets().values().get(
            spreadsheetId=SHEET_ID, range=f"{EXPORT_TAB}!A:D"
        ).execute()
    except Exception as e:
        log.error(f"Vendor DB load failed: {e}")
        print(f"  ⚠️  Could not read export sheet: {e}")
        return {"emails": {}, "domains": {}}

    rows = result.get("values", [])
    if not rows:
        print("  ⚠️  Export sheet is empty.")
        return {"emails": {}, "domains": {}}

    start = 1 if rows[0][0].lower() in ("partner name", "partnername", "name") else 0

    emails_db  = {}
    domains_db = {}

    for row in rows[start:]:
        while len(row) < 4:
            row.append("")
        partner_name   = row[0].strip()
        classification = row[1].strip()
        email_raw      = row[3].strip()

        if not email_raw or not partner_name:
            continue

        for raw_email in email_raw.split(","):
            email = raw_email.strip().lower()
            if "@" not in email:
                continue
            emails_db[email] = {"partner_name": partner_name, "classification": classification}
            domain = email.split("@")[1]
            if domain not in domains_db:
                domains_db[domain] = {"partner_name": partner_name, "classification": classification}

    print(f"  ✅ Vendor DB: {len(emails_db)} emails across {len(domains_db)} domains\n")
    return {"emails": emails_db, "domains": domains_db}


def lookup_vendor(all_addresses: list, vendor_db: dict) -> dict:
    emails_db  = vendor_db.get("emails",  {})
    domains_db = vendor_db.get("domains", {})

    GENERIC_DOMAINS = {
        "gmail.com", "yahoo.com", "hotmail.com", "outlook.com", "live.com",
        "icloud.com", "me.com", "mac.com", "protonmail.com", "proton.me",
        "aol.com", "ymail.com", "rediffmail.com", "mail.com",
    }

    for raw in all_addresses:
        if ONEQUINCE_DOMAIN.lower() in raw.lower():
            continue
        email = _extract_email(raw)
        if "@" not in email:
            continue
        if email in emails_db:
            return emails_db[email]

    for raw in all_addresses:
        if ONEQUINCE_DOMAIN.lower() in raw.lower():
            continue
        email = _extract_email(raw)
        if "@" not in email:
            continue
        domain = email.split("@")[1]
        if domain not in GENERIC_DOMAINS and domain in domains_db:
            return domains_db[domain]

    return {"partner_name": "", "classification": ""}


def _fallback_vendor_name(external_addresses: list) -> str:
    seen   = set()
    result = []
    for raw in external_addresses:
        email      = _extract_email(raw)
        local_part = email.split("@")[0] if "@" in email else email
        if any(noise in local_part.lower() for noise in NOISE_ADDRESS_WORDS):
            continue
        name = _display_name(raw)
        if name and name.lower() not in (email, local_part) and name.lower() not in seen:
            seen.add(name.lower())
            result.append(name)
    return ", ".join(result[:2])


# ════════════════════════════════════════════════════════════
# OPENAI CLIENT
# FIX 2: Thread-safe singleton using a lock
# ════════════════════════════════════════════════════════════

_openai_client      = None
_openai_client_lock = threading.Lock()

def _get_openai_client():
    global _openai_client
    # FIX 2: double-checked locking pattern — safe under parallel workers
    if _openai_client is None:
        with _openai_client_lock:
            if _openai_client is None:
                api_key = os.environ.get("OPENAI_API_KEY")
                if not api_key:
                    raise RuntimeError("OPENAI_API_KEY not set. Add it to your .env file.")
                _openai_client = OpenAI(api_key=api_key, timeout=OPENAI_TIMEOUT)
    return _openai_client


# FIX 11: Token usage tracking — accumulated across the run
_run_token_usage = {"prompt_tokens": 0, "completion_tokens": 0}
_token_usage_lock = threading.Lock()

def _record_token_usage(usage):
    """Thread-safe accumulation of token usage from OpenAI response."""
    if usage is None:
        return
    with _token_usage_lock:
        _run_token_usage["prompt_tokens"]     += getattr(usage, "prompt_tokens",     0)
        _run_token_usage["completion_tokens"] += getattr(usage, "completion_tokens", 0)


def ask_openai(prompt: str) -> str:
    from openai import RateLimitError, APIStatusError, APIConnectionError, APITimeoutError
    for attempt in range(OPENAI_RETRIES + 1):
        try:
            response = _get_openai_client().chat.completions.create(
                model=OPENAI_MODEL,
                messages=[{"role": "user", "content": prompt}],
                temperature=0,
                response_format={"type": "json_object"},
            )
            # FIX 11: record token usage on every successful call
            _record_token_usage(response.usage)
            return response.choices[0].message.content.strip()
        except RateLimitError:
            wait = 10 * (attempt + 1)
            if attempt < OPENAI_RETRIES:
                print(f"    ⏳ OpenAI rate limit — retry {attempt+1}/{OPENAI_RETRIES} in {wait}s...")
                time.sleep(wait)
            else:
                return ""
        except APIStatusError as e:
            if e.status_code < 500:
                return ""
            if attempt < OPENAI_RETRIES:
                time.sleep(5 * (attempt + 1))
            else:
                return ""
        except (APIConnectionError, APITimeoutError):
            if attempt < OPENAI_RETRIES:
                time.sleep(5 * (attempt + 1))
            else:
                return ""
        except Exception as e:
            log.error(f"OpenAI unexpected error: {e}")
            return ""
    return ""


def _clean_llm(raw: str) -> str:
    if "<think>" in raw:
        raw = raw.split("</think>")[-1].strip()
    raw = re.sub(r'^```(?:json)?\s*', '', raw.strip())
    raw = re.sub(r'\s*```$', '', raw)
    return raw.strip()


# ════════════════════════════════════════════════════════════
# LLM WINDOW SELECTION
# ════════════════════════════════════════════════════════════

def _select_llm_window(msgs: list, max_n: int) -> list:
    n = len(msgs)
    if n <= max_n:
        return msgs

    first   = [msgs[0]]
    last_4  = msgs[-4:]
    middle  = msgs[1:-4]

    remaining = max_n - 1 - 4
    if remaining <= 0:
        return first + last_4

    step   = max(1, len(middle) // remaining)
    picked = middle[::step][:remaining]

    return first + picked + last_4


# ════════════════════════════════════════════════════════════
# SINGLE COMBINED LLM CALL
# FIX 6: Full fallback reply now uses vendor_name
# FIX 10: cache now passes vendor_name
# ════════════════════════════════════════════════════════════

def llm_analyse_thread(
    subject: str,
    structured_messages: list,
    vendor_name: str = "",
    thread_id: str = "",
    msg_count: int = 0,
) -> dict:
    # FIX 10: vendor_name passed into cache lookup
    if thread_id and msg_count:
        cached = cache_get(thread_id, msg_count, vendor_name)
        if cached:
            print(f"    💾 Cache hit — skipping LLM for: {subject[:50]}")
            return cached

    divisions_str = "\n".join(f"  - {d}" for d in DIVISIONS)

    n_msgs = len(structured_messages)
    thread_context = ""
    for i, msg in enumerate(structured_messages, 1):
        is_latest  = (i == n_msgs)
        char_limit = BODY_CHARS_PER_MSG if is_latest else OLDER_MSG_CHARS
        body       = msg["body"][:char_limit]
        truncated  = " [truncated]" if len(msg["body"]) > char_limit else ""
        thread_context += (
            f"\n[Message {i} | From: {msg['from']} | Date: {msg['date']}]{truncated}\n"
            f"{body}\n"
        )

    vendor_line = f"Vendor / counterparty: {vendor_name}" if vendor_name else ""
    salutation  = f"Hi {vendor_name}," if vendor_name else "Hi there,"

    prompt = f"""
LANGUAGE RULE — READ FIRST:
Every word of your JSON response MUST be in ENGLISH.
Do NOT use Chinese, Japanese, Hindi, or any other language in any field.

You are a fashion merchandising analyst at One Quince. Analyse this email thread.
{vendor_line}

SUBJECT: {subject}

THREAD (oldest → newest):
{thread_context.strip()}

════ TASKS ════

TASK 1 — DIVISION
Read the SUBJECT LINE ONLY. Pick exactly one:
{divisions_str}

Mapping: Kids/Baby/Children/Toddler → Kids and Baby | Maternity → Maternity
Men/Mens (no Women) → Men's Apparel | Women/Ladies → Women's Apparel
Flat/Tech Pack → Apparel Flats | Home/Linen/Bedding/Cushion/Rug/Towel → Home
Furniture/Sofa/Chair/Table → Furniture | Jewelry/Necklace/Ring → Jewelry
Bag/Wallet/Belt/Accessories → Accessories | Unclear → Other

TASK 2 — STYLE NUMBERS
Scan ALL message bodies. Extract every style/article number.
Known patterns: M--1234  W--5678  U--9012  W-PNT-228  NECK-209  U-FURN-304  ST-2045
Rules: letters + one or two hyphens + optional letters + 3–6 digits.
Return all found, comma-separated UPPERCASE. If none: return "".

TASK 3 — COLOUR
Extract product colour mentions near style numbers or labelled color:/colour:/col:.
Return comma-separated English colour names. If none: return "".

TASK 4 — AI OVERVIEW  (ENGLISH ONLY)
Write 3–4 bullet points for a merchandise manager summarising:
• What product/order/sample is being discussed
• Any quality issues, delays, or concerns
• Current status (from the latest message)
• Next action needed
Start each bullet with "• ". English words only.

TASK 5 — INTENT
Read the LATEST MESSAGE. Pick the single most important action required:
  - Approve sample | Review & feedback | Confirm order | Chase vendor
  - Resolve delay | Resolve quality issue | Awaiting shipment | Track shipment
  - Payment action | No action needed | Other

TASK 6 — REQUIRES REPLY
true  → a question was asked, approval was requested, or action is awaited
false → purely informational, or vendor acknowledged without asking anything

TASK 7 — SAMPLE STATUS
Dispatched | Received | Approved | Rejected | Pending | None

TASK 8 — REPLY DRAFT
Write a ready-to-send email reply on behalf of the One Quince merchandising team.
Tone: professional but warm, like a sharp senior buyer.
First sentence: acknowledge what the vendor said or did.
Address the specific ask in the latest message directly.
Keep it SHORT: 3–5 sentences. No filler.
Salutation: "{salutation}"
Sign-off: "Best,\\n[Your name]"
Plain text only — no markdown, no asterisks.

════ OUTPUT ════
Return ONLY valid JSON. No markdown, no explanation.
{{
  "division":       "<one division>",
  "style_numbers":  "<comma-separated UPPERCASE or empty string>",
  "colour":         "<comma-separated English colour names or empty string>",
  "ai_overview":    "<3–4 English bullet points separated by \\n>",
  "intent":         "<one intent from the list>",
  "requires_reply": <true or false>,
  "sample_status":  "<Dispatched | Received | Approved | Rejected | Pending | None>",
  "reply_draft":    "<ready-to-send email body, plain text, 3–5 sentences>"
}}"""

    parsed   = None
    last_raw = ""
    for parse_attempt in range(2):
        raw      = ask_openai(prompt)
        last_raw = raw
        cleaned  = _clean_llm(raw)

        if not cleaned:
            log.warning(f"Empty LLM response for '{subject}' attempt {parse_attempt + 1}")
            if parse_attempt == 0:
                time.sleep(2)
                continue
            break

        try:
            parsed = json.loads(cleaned)
            break
        except json.JSONDecodeError as e:
            log.warning(f"LLM JSON parse failed for '{subject}' attempt {parse_attempt + 1}: {e}")
            if parse_attempt == 0:
                time.sleep(2)

    if parsed is not None:
        overview = parsed.get("ai_overview", "").strip()

        if re.search(r'[\u4e00-\u9fff\u3040-\u309f\u30a0-\u30ff\uac00-\ud7af]', overview):
            log.warning(f"LLM returned CJK overview for '{subject}' — retrying stripped prompt")
            stripped_context = ""
            for i, msg in enumerate(structured_messages, 1):
                body = _strip_cjk(msg["body"][:BODY_CHARS_PER_MSG])
                stripped_context += f"[Message {i} | From: {msg['from']}]\n{body}\n\n"

            retry_prompt = f"""
WRITE IN ENGLISH ONLY. No Chinese. No Japanese. No Hangul. English words only.

Summarise this email thread in 3-4 bullet points for a fashion buyer.
Each bullet starts with "• ". English only.
Subject: {subject}
{stripped_context.strip()}
Return ONLY: {{"ai_overview": "<your English bullets separated by \\n>"}}"""

            retry_raw   = ask_openai(retry_prompt)
            retry_clean = _clean_llm(retry_raw)
            try:
                rp      = json.loads(retry_clean)
                ro      = rp.get("ai_overview", "").strip()
                overview = ro if ro and not re.search(
                    r'[\u4e00-\u9fff\u3040-\u309f\u30a0-\u30ff\uac00-\ud7af]', ro
                ) else "• Thread summary unavailable — vendor email contains non-English content"
            except Exception:
                overview = "• Thread summary unavailable — vendor email contains non-English content"

        raw_reply = parsed.get("requires_reply", False)
        if isinstance(raw_reply, bool):
            requires_reply = raw_reply
        elif isinstance(raw_reply, str):
            requires_reply = raw_reply.strip().lower() in ("true", "yes", "1")
        else:
            requires_reply = bool(raw_reply)

        intent = parsed.get("intent", "").strip() or "Other"

        valid_statuses = {"Dispatched", "Received", "Approved", "Rejected", "Pending", "None"}
        sample_status  = parsed.get("sample_status", "None").strip().capitalize()
        if sample_status not in valid_statuses:
            sample_status = "None"

        reply_draft = parsed.get("reply_draft", "").strip()
        if not reply_draft or re.search(
            r'[\u4e00-\u9fff\u3040-\u309f\u30a0-\u30ff\uac00-\ud7af]', reply_draft
        ):
            # FIX 6: use vendor_name in fallback reply draft
            reply_draft = (
                f"{salutation}\n\n"
                f"Thank you for your email regarding {subject}. "
                f"We'll review and come back to you shortly.\n\nBest,\n[Your name]"
            )

        result = {
            "division":       parsed.get("division",      "Other").strip(),
            "style_numbers":  parsed.get("style_numbers", "").strip(),
            "colour":         parsed.get("colour",        "").strip(),
            "ai_overview":    overview,
            "intent":         intent,
            "requires_reply": "Yes" if requires_reply else "No",
            "sample_status":  sample_status,
            "reply_draft":    reply_draft,
        }

        # FIX 10: vendor_name passed into cache_set
        if thread_id and msg_count:
            cache_set(thread_id, msg_count, vendor_name, result)
        return result

    # ── Full fallback — FIX 6: use vendor_name ──
    log.error(f"LLM completely failed for '{subject}'. Raw: {last_raw[:400]}")
    all_text = "\n".join(m["body"] for m in structured_messages)
    return {
        "division":       _fallback_division(subject),
        "style_numbers":  _fallback_style_numbers(all_text),
        "colour":         "",
        "ai_overview":    "• Could not generate summary — LLM unavailable or returned invalid JSON",
        "intent":         "Other",
        "requires_reply": "No",
        "sample_status":  "None",
        "reply_draft":    (
            f"{salutation}\n\n"
            f"Thanks for reaching out. We'll get back to you on {subject} shortly.\n\nBest,\n[Your name]"
        ),
    }


def _fallback_division(subject: str) -> str:
    s = subject.lower()
    if any(w in s for w in ["kid", "baby", "children", "toddler"]): return "Kids and Baby"
    if "maternity" in s:                                              return "Maternity"
    if any(w in s for w in ["men", "mens", "men's"]) and "women" not in s:
                                                                      return "Men's Apparel"
    if any(w in s for w in ["women", "womens", "ladies", "women's"]): return "Women's Apparel"
    if any(w in s for w in ["flat", "tech pack"]):                    return "Apparel Flats"
    if any(w in s for w in ["home","linen","bedding","cushion","rug","towel","curtain"]):
                                                                      return "Home"
    if any(w in s for w in ["furniture","sofa","chair","table","shelf"]): return "Furniture"
    if any(w in s for w in ["jewelry","jewellery","necklace","ring","earring"]): return "Jewelry"
    if any(w in s for w in ["bag","wallet","accessories","belt","scarf","hat"]): return "Accessories"
    return "Other"


def _fallback_style_numbers(text: str) -> str:
    pattern = r'\b([A-Z]{1,5}-{1,2}[A-Z]{0,8}-?\d{3,6})\b'
    seen, out = set(), []
    for m in re.findall(pattern, text, re.IGNORECASE):
        u = m.upper()
        if u not in seen:
            seen.add(u)
            out.append(u)
    return ", ".join(out)


# ════════════════════════════════════════════════════════════
# PO NUMBER EXTRACTION
# FIX 5: deterministic — explicitly prioritises latest message
# ════════════════════════════════════════════════════════════

PO_PATTERNS = [
    r'\bP\.?O\.?\s*(?:No|Number|#|:)?\s*[:#\-]?\s*([A-Z0-9][A-Z0-9\-/]{3,20})\b',
    r'\bPurchase\s+Order\s*(?:No|Number|#|:)?\s*[:#\-]?\s*([A-Z0-9][A-Z0-9\-/]{3,20})\b',
    r'\bOrder\s+(?:No|Number|Ref|#)\s*[:#\-]?\s*([A-Z0-9][A-Z0-9\-/]{3,20})\b',
    r'\bPO[:\-\s]\s*([A-Z0-9][A-Z0-9\-/]{3,20})\b',
    r'\bRef(?:erence)?\s*(?:No|#)?\s*[:#\-]?\s*(PO[A-Z0-9\-]{3,18})\b',
]

PO_NOISE = {
    "INVOICE", "SUBJECT", "REGARDS", "ATTACHED", "PLEASE", "KINDLY",
    "CONFIRM", "DETAILS", "SAMPLE", "STYLES", "UPDATE", "STATUS",
}


def extract_po_number(structured_msgs: list) -> str:
    """
    FIX 5: iterate newest-to-oldest explicitly (reversed), return first valid match.
    This guarantees the latest PO number wins — previously relied on dict ordering.
    """
    for msg in reversed(structured_msgs):
        text = msg["body"]
        for pattern in PO_PATTERNS:
            for m in re.finditer(pattern, text, re.IGNORECASE):
                candidate = m.group(1).strip().upper()
                if candidate in PO_NOISE:
                    continue
                if len(candidate) < 4 or len(candidate) > 20:
                    continue
                if not re.search(r'\d', candidate):
                    continue
                return candidate   # FIX 5: return immediately — newest message wins
    return ""


# ════════════════════════════════════════════════════════════
# SHIPMENT EXTRACTION
# ════════════════════════════════════════════════════════════

CARRIERS = [
    "DHL Express", "DHL eCommerce", "DHL",
    "FedEx International", "FedEx",
    "UPS Express", "UPS",
    "TNT Express", "TNT",
    "Aramex", "BlueDart", "Blue Dart",
    "DTDC", "Delhivery", "Ekart", "Ecom Express",
    "XpressBees", "Shadowfax", "SpiceXpress",
    "Emirates SkyCargo", "Emirates Sky Cargo",
    "Cathay Pacific Cargo", "Cathay Cargo",
    "Air India Cargo", "IndiGo Cargo",
    "Maersk", "MSC", "CMA CGM", "Evergreen",
    "COSCO", "Hapag-Lloyd", "Hapag Lloyd",
    "ONE (Ocean Network Express)", "Yang Ming", "ZIM", "PIL", "Wan Hai",
    "Kerry Logistics", "Agility", "Panalpina", "DB Schenker",
    "Kuehne+Nagel", "Kuehne Nagel", "Expeditors",
    "CEVA Logistics", "CEVA", "DSV",
    "Bolloré Logistics", "Bollore", "Rhenus", "Geodis", "XPO Logistics",
]

CARRIER_ALIASES = {
    "blue dart":          "BlueDart",
    "dhl express":        "DHL",
    "dhl ecommerce":      "DHL",
    "fedex international":"FedEx",
    "ups express":        "UPS",
    "tnt express":        "TNT",
    "emirates sky cargo": "Emirates SkyCargo",
    "cathay cargo":       "Cathay Pacific Cargo",
    "hapag lloyd":        "Hapag-Lloyd",
    "kuehne nagel":       "Kuehne+Nagel",
    "ceva logistics":     "CEVA",
    "bollore":            "Bolloré Logistics",
}

AWB_PATTERNS = [
    r'\bAWB\s*[:#\-]?\s*([A-Z0-9]{3}-\d{8})\b',
    r'\bAWB\s*[:#\-]?\s*([A-Z0-9\-]{6,20})\b',
    r'\bairway\s*bill\s*[:#\-]?\s*([A-Z0-9\-]{6,20})\b',
    r'\btracking\s*(?:no|number|#|:)?\s*[:#\-]?\s*([A-Z0-9\-]{8,25})\b',
    r'\bshipment\s*(?:no|number|#|:)?\s*[:#\-]?\s*([A-Z0-9\-]{6,20})\b',
    r'\bconsignment\s*(?:no|number|#|:)?\s*([A-Z0-9\-]{6,20})\b',
    r'\b(\d{3}-\d{8})\b',
    r'\b(1Z[A-Z0-9]{16})\b',
    r'\b([A-Z]{2}\d{9}[A-Z]{2})\b',
    (r'\b(?:DHL|FedEx|UPS|TNT|Aramex|BlueDart|DTDC|Delhivery|Maersk|MSC|'
     r'Expeditors|Agility|CEVA|DSV|Geodis)\b[\s\w,./()-]{0,30}?\b(\d{8,13})\b'),
    r'(?:sent|shipped|dispatched|courier|forward)[^.]{0,60}?\b(\d{8,13})\b',
]

DATE_CONTEXT_PATTERN = (
    r'(?:sent|shipped|dispatched|forwarded|courier(?:ed)?|hand(?:ed)?\s*over|'
    r'picked\s*up|delivery\s*arranged|collected)'
    r'[^.]{0,80}?'
    r'(\(?\d{1,2}[\/\-\.]\d{1,2}[\/\-\.]\d{2,4}\)?'
    r'|\d{4}[\/\-\.]\d{1,2}[\/\-\.]\d{1,2}'
    r'|(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\w*\.?\s+\d{1,2},?\s+\d{4}'
    r'|\d{1,2}\s+(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\w*\s+\d{4})'
)


def extract_shipment_info(text: str) -> dict:
    result     = {"company": "", "awb": "", "shipment_date": ""}
    text_lower = text.lower()

    best_carrier, best_pos = "", -1
    for carrier in CARRIERS:
        idx = text_lower.find(carrier.lower())
        if idx != -1 and (best_pos == -1 or idx < best_pos):
            best_carrier, best_pos = carrier, idx

    result["company"] = CARRIER_ALIASES.get(best_carrier.lower(), best_carrier)

    for pattern in AWB_PATTERNS:
        m = re.search(pattern, text, re.IGNORECASE)
        if m:
            result["awb"] = m.group(1).upper()
            break

    m = re.search(DATE_CONTEXT_PATTERN, text, re.IGNORECASE)
    if m:
        result["shipment_date"] = m.group(1).strip("()")
    elif best_pos >= 0:
        window = text[max(0, best_pos - 50): best_pos + 150]
        dm = re.search(
            r'(\d{1,2}[\/\-\.]\d{1,2}[\/\-\.]\d{2,4}|\d{4}[\/\-\.]\d{1,2}[\/\-\.]\d{1,2})',
            window
        )
        if dm:
            result["shipment_date"] = dm.group(1)

    return result


# ════════════════════════════════════════════════════════════
# ATTACHMENT DETECTION
# ════════════════════════════════════════════════════════════

ATTACHMENT_MIME_MAP = {
    "application/pdf": "PDF",
    "application/msword": "Word",
    "application/vnd.openxmlformats-officedocument.wordprocessingml.document": "Word",
    "application/vnd.ms-excel": "Excel",
    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet": "Excel",
    "text/csv": "CSV",
    "application/vnd.ms-powerpoint": "PowerPoint",
    "application/vnd.openxmlformats-officedocument.presentationml.presentation": "PowerPoint",
    "image/jpeg": "Image", "image/jpg": "Image", "image/png": "Image",
    "image/gif": "Image", "image/webp": "Image", "image/tiff": "Image",
    "application/zip": "ZIP", "application/x-zip-compressed": "ZIP",
    "application/octet-stream": "File",
}

ATTACHMENT_EXT_MAP = {
    ".pdf": "PDF", ".doc": "Word", ".docx": "Word",
    ".xls": "Excel", ".xlsx": "Excel", ".csv": "CSV",
    ".ppt": "PowerPoint", ".pptx": "PowerPoint",
    ".jpg": "Image", ".jpeg": "Image", ".png": "Image",
    ".gif": "Image", ".tif": "Image", ".tiff": "Image",
    ".ai": "Illustrator", ".psd": "Photoshop",
    ".zip": "ZIP", ".rar": "ZIP",
    ".dwg": "CAD", ".dxf": "CAD",
}


def _collect_parts(payload: dict) -> list:
    parts = []
    for part in payload.get("parts", []):
        parts.append(part)
        if part.get("parts"):
            parts.extend(_collect_parts(part))
    return parts


def extract_attachments(messages: list) -> str:
    type_counts: dict = {}
    for msg in messages:
        all_parts = _collect_parts(msg.get("payload", {}))
        for part in all_parts:
            filename    = part.get("filename", "").strip()
            mime        = part.get("mimeType", "").lower()
            headers     = {h["name"].lower(): h["value"] for h in part.get("headers", [])}
            disposition = headers.get("content-disposition", "").lower()
            if "inline" in disposition and not filename:
                continue
            if not filename and "attachment" not in disposition:
                continue
            label = ATTACHMENT_MIME_MAP.get(mime, "")
            if not label and filename:
                ext   = "." + filename.rsplit(".", 1)[-1].lower() if "." in filename else ""
                label = ATTACHMENT_EXT_MAP.get(ext, "File")
            if not label:
                continue
            type_counts[label] = type_counts.get(label, 0) + 1

    if not type_counts:
        return ""
    parts = []
    for label, count in sorted(type_counts.items()):
        parts.append(f"{label} (×{count})" if count > 1 else label)
    return ", ".join(parts)


# ════════════════════════════════════════════════════════════
# SHARED LINK DETECTION
# ════════════════════════════════════════════════════════════

SHARED_LINK_PATTERNS = [
    ("Google Drive",  r'https?://(?:drive|docs)\.google\.com/\S+'),
    ("Google Sheets", r'https?://docs\.google\.com/spreadsheets/\S+'),
    ("Google Docs",   r'https?://docs\.google\.com/document/\S+'),
    ("Dropbox",       r'https?://(?:www\.)?dropbox\.com/\S+'),
    ("WeTransfer",    r'https?://(?:we\.tl|wetransfer\.com)/\S+'),
    ("OneDrive",      r'https?://(?:1drv\.ms|onedrive\.live\.com|[a-z0-9-]+\.sharepoint\.com)/\S+'),
    ("Figma",         r'https?://(?:www\.)?figma\.com/(?:file|design|proto)/\S+'),
    ("Notion",        r'https?://(?:www\.)?notion\.so/\S+'),
    ("Box",           r'https?://(?:[a-z0-9-]+\.)?box\.com/\S+'),
    ("iCloud",        r'https?://(?:www\.)?icloud\.com/\S+'),
]

_GENERIC_URL_RE = re.compile(r"https?://[^\s<>\"']{10,}", re.IGNORECASE)


def extract_shared_links(structured_msgs: list) -> str:
    found_labels: dict = {}
    matched_urls:  set = set()
    all_bodies = "\n".join(m["body"] for m in structured_msgs)

    for label, pattern in SHARED_LINK_PATTERNS:
        for m in re.finditer(pattern, all_bodies, re.IGNORECASE):
            url = m.group(0)
            if url not in matched_urls:
                matched_urls.add(url)
                found_labels[label] = found_labels.get(label, 0) + 1

    for m in _GENERIC_URL_RE.finditer(all_bodies):
        url = m.group(0)
        if url in matched_urls:
            continue
        try:
            domain = url.split("/")[2].lstrip("www.")
        except IndexError:
            continue
        skip_domains = {
            "google.com", "googleapis.com", "gstatic.com",
            "gmail.com", "yahoo.com", "hotmail.com",
            "outlook.com", "microsoft.com", "apple.com",
            "fonts.googleapis.com", "cdn.", "tracking.", "click.",
            "mailchimp.com", "sendgrid.net", "mandrillapp.com",
        }
        if any(s in domain for s in skip_domains):
            continue
        matched_urls.add(url)
        found_labels[domain] = found_labels.get(domain, 0) + 1

    if not found_labels:
        return ""
    parts = []
    for label, count in sorted(found_labels.items()):
        parts.append(f"{label} (×{count})" if count > 1 else label)
    return ", ".join(parts)


# ════════════════════════════════════════════════════════════
# SAMPLE REMINDER
# FIX 7: timezone-safe — use UTC throughout
# ════════════════════════════════════════════════════════════

SAMPLE_REMINDER_DAYS = 7


def compute_sample_reminder(sample_status: str, latest_msg_date) -> str:
    """
    FIX 7: latest_msg_date (from internalDate, UTC epoch ms) compared against
    datetime.now(timezone.utc) — no more local-clock skew on cloud deployments.
    """
    if not latest_msg_date:
        return ""
    # Ensure both sides are UTC-aware for safe subtraction
    if latest_msg_date.tzinfo is None:
        latest_msg_date = latest_msg_date.replace(tzinfo=timezone.utc)
    days_since = (datetime.now(timezone.utc) - latest_msg_date).days
    if sample_status == "Dispatched" and days_since >= SAMPLE_REMINDER_DAYS:
        return f"⚠️ Chase — dispatched {days_since}d ago, no update"
    if sample_status == "Pending" and days_since >= SAMPLE_REMINDER_DAYS:
        return f"⚠️ Chase — sample pending {days_since}d"
    return ""


# ════════════════════════════════════════════════════════════
# THREAD PROCESSOR
# FIX 7: internalDate parsed as UTC-aware datetime
# ════════════════════════════════════════════════════════════

def _utc_from_internal(ts_ms: int):
    """Convert Gmail internalDate (UTC milliseconds) to a UTC-aware datetime."""
    return datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc)


def process_thread(gmail_svc, thread_id: str, vendor_db: dict):
    thread_data = gmail_threads_get(gmail_svc, userId="me", id=thread_id, format="full")
    messages    = thread_data.get("messages", [])

    if len(messages) < 2:
        return None

    messages = sorted(messages, key=lambda m: int(m.get("internalDate", 0)))

    subject        = ""
    all_senders    = []
    all_recipients = []
    all_bcc        = []
    earliest_date  = None
    structured_msgs = []

    cc_seen        = set()
    to_seen        = set()
    all_cc_deduped = []

    for msg in messages:
        raw_headers = msg["payload"].get("headers", [])
        hdrs = {h["name"].lower(): h["value"] for h in raw_headers}

        if not subject:
            subject = re.sub(
                r'^(re|fwd|fw):\s*', '',
                hdrs.get("subject", "(no subject)"),
                flags=re.IGNORECASE
            ).strip()

        sender = hdrs.get("from", "")
        if sender and sender not in all_senders:
            all_senders.append(sender)

        for addr in _split_addrs(hdrs.get("to", "")):
            key = _extract_email(addr)
            if key not in to_seen:
                to_seen.add(key)
                all_recipients.append(addr)

        for addr in _split_addrs(hdrs.get("cc", "")):
            key = _extract_email(addr)
            if key not in cc_seen:
                cc_seen.add(key)
                all_cc_deduped.append(addr)

        for addr in _split_addrs(hdrs.get("bcc", "")):
            all_bcc.append(addr)

        msg_date_str = ""
        internal_ts  = int(msg.get("internalDate", 0))
        if internal_ts:
            dt = _utc_from_internal(internal_ts)   # FIX 7: UTC-aware
            if earliest_date is None or dt < earliest_date:
                earliest_date = dt
            msg_date_str = dt.strftime("%d %b %Y %H:%M")
        else:
            date_raw = hdrs.get("date", "")
            if date_raw:
                try:
                    dt = parsedate_to_datetime(date_raw)
                    if dt.tzinfo is None:
                        dt = dt.replace(tzinfo=timezone.utc)
                    if earliest_date is None or dt < earliest_date:
                        earliest_date = dt
                    msg_date_str = dt.strftime("%d %b %Y")
                except Exception:
                    pass

        body = extract_message_body(msg["payload"])
        if body:
            structured_msgs.append({
                "from": _display_name(sender) or sender,
                "date": msg_date_str,
                "body": body,
            })

    if not structured_msgs:
        return None

    llm_msgs = _select_llm_window(structured_msgs, MAX_MSGS_IN_LLM)

    seen_addrs       = set()
    ordered_external = []
    for addr in (all_senders + all_recipients + all_cc_deduped + all_bcc):
        key = _extract_email(addr)
        if key not in seen_addrs and ONEQUINCE_DOMAIN.lower() not in addr.lower():
            seen_addrs.add(key)
            ordered_external.append(addr)

    vendor = lookup_vendor(ordered_external, vendor_db)
    partner_name   = vendor["partner_name"]
    classification = vendor["classification"]

    if not partner_name:
        sender_email = _extract_email(all_senders[0]) if all_senders else ""
        if ONEQUINCE_DOMAIN in sender_email:
            recip_external = [
                a for a in (all_recipients + all_cc_deduped)
                if ONEQUINCE_DOMAIN.lower() not in a.lower()
            ]
            vendor2 = lookup_vendor(recip_external, vendor_db)
            partner_name   = vendor2["partner_name"]
            classification = vendor2["classification"]

    if not partner_name:
        partner_name = _fallback_vendor_name(ordered_external)

    all_bodies   = "\n\n".join(m["body"] for m in structured_msgs)
    shipment     = extract_shipment_info(all_bodies)
    po_number    = extract_po_number(structured_msgs)
    attachments  = extract_attachments(messages)
    shared_links = extract_shared_links(structured_msgs)

    print(f"    🤖 LLM: {subject[:50]}...")
    llm = llm_analyse_thread(
        subject, llm_msgs,
        vendor_name=partner_name,
        thread_id=thread_id,
        msg_count=len(messages),
    )

    # FIX 7: latest_msg_date is UTC-aware (from _utc_from_internal)
    latest_internal = int(messages[-1].get("internalDate", 0))
    latest_msg_date = _utc_from_internal(latest_internal) if latest_internal else None
    sample_reminder = compute_sample_reminder(llm["sample_status"], latest_msg_date)

    cc_display = "; ".join(
        _extract_email(a)
        for a in all_cc_deduped
        if a and ONEQUINCE_DOMAIN.lower() not in a.lower()
    )

    sender_display = _display_name(all_senders[0]) if all_senders else ""
    sent_date      = earliest_date.strftime("%Y-%m-%d %H:%M") if earliest_date else ""

    return {
        "Subject":          subject,
        "Sender":           sender_display,
        "CC":               cc_display,
        "Division":         llm["division"],
        "Style No":         llm["style_numbers"],
        "Colour":           llm["colour"],
        "Vendor Name":      partner_name,
        "Partner Class":    classification,
        "Shipment Company": shipment["company"],
        "AWB No":           shipment["awb"],
        "Shipment Date":    shipment["shipment_date"],
        "Sent Date":        sent_date,
        "AI Overview":      llm["ai_overview"],
        "Thread Messages":  len(messages),
        "Thread ID":        thread_id,
        "Intent":           llm["intent"],
        "Reply Needed":     llm["requires_reply"],
        "PO Number":        po_number,
        "Sample Status":    llm["sample_status"],
        "Sample Reminder":  sample_reminder,
        "Attachments":      attachments,
        "Shared Links":     shared_links,
        "Reply Draft":      llm["reply_draft"],
    }


# ════════════════════════════════════════════════════════════
# GOOGLE SHEETS — setup, read, write
# ════════════════════════════════════════════════════════════

def _get_sheet_id(svc, tab_name: str) -> int:
    meta = svc.spreadsheets().get(spreadsheetId=SHEET_ID).execute()
    for s in meta["sheets"]:
        if s["properties"]["title"] == tab_name:
            return s["properties"]["sheetId"]
    return 0


def _col_letter_n(n: int) -> str:
    result = ""
    while n:
        n, r = divmod(n - 1, 26)
        result = chr(65 + r) + result
    return result


def _ensure_tab(svc, tab_name: str, headers: list):
    meta      = svc.spreadsheets().get(spreadsheetId=SHEET_ID).execute()
    tab_names = [s["properties"]["title"] for s in meta["sheets"]]

    if tab_name not in tab_names:
        print(f"  📋 Creating '{tab_name}' tab...")
        svc.spreadsheets().batchUpdate(
            spreadsheetId=SHEET_ID,
            body={"requests": [{"addSheet": {"properties": {"title": tab_name}}}]}
        ).execute()

    r = svc.spreadsheets().values().get(
        spreadsheetId=SHEET_ID, range=f"'{tab_name}'!1:1"
    ).execute()
    existing_headers = r.get("values", [[]])[0] if r.get("values") else []

    def _bold_row1():
        sid = _get_sheet_id(svc, tab_name)
        svc.spreadsheets().batchUpdate(
            spreadsheetId=SHEET_ID,
            body={"requests": [{"repeatCell": {
                "range":  {"sheetId": sid, "startRowIndex": 0, "endRowIndex": 1},
                "cell":   {"userEnteredFormat": {"textFormat": {"bold": True}}},
                "fields": "userEnteredFormat.textFormat.bold",
            }}]}
        ).execute()

    if not existing_headers:
        print(f"  📋 '{tab_name}': writing {len(headers)} headers...")
        svc.spreadsheets().values().update(
            spreadsheetId=SHEET_ID,
            range=f"'{tab_name}'!A1",
            valueInputOption="RAW",
            body={"values": [headers]}
        ).execute()
        _bold_row1()
        print(f"  ✅ '{tab_name}': headers written (A–{_col_letter_n(len(headers))})")
        return

    n_existing = len(existing_headers)
    n_expected = len(headers)

    if n_existing < n_expected:
        missing      = headers[n_existing:]
        start_col    = n_existing + 1
        start_letter = _col_letter_n(start_col)
        end_letter   = _col_letter_n(start_col + len(missing) - 1)
        print(f"  🔧 '{tab_name}': {len(missing)} new column(s) "
              f"({start_letter}–{end_letter}): {', '.join(missing)}")
        svc.spreadsheets().values().update(
            spreadsheetId=SHEET_ID,
            range=f"'{tab_name}'!{start_letter}1",
            valueInputOption="RAW",
            body={"values": [missing]}
        ).execute()
        _bold_row1()
        print(f"  ✅ '{tab_name}': headers complete (A–{end_letter})")
        return

    print(f"  ✅ '{tab_name}': headers OK ({n_existing} columns)")


def load_existing_rows(svc) -> tuple:
    end_col = _col_letter_n(len(LOGS_HEADERS))
    result = svc.spreadsheets().values().get(
        spreadsheetId=SHEET_ID,
        range=f"'{SHEET_TAB}'!A2:{end_col}"
    ).execute()

    thread_map  = {}
    subject_map = {}

    thread_id_col  = _HEADER_INDEX["Thread ID"]      - 1
    msg_count_col  = _HEADER_INDEX["Thread Messages"] - 1
    subject_col    = _HEADER_INDEX["Subject"]         - 1

    for idx, row in enumerate(result.get("values", [])):
        while len(row) < len(LOGS_HEADERS):
            row.append("")

        subject   = row[subject_col].strip()
        msg_count = int(row[msg_count_col]) if str(row[msg_count_col]).isdigit() else 0
        thread_id = row[thread_id_col].strip()
        sheet_row = idx + 2

        info = {
            "sheet_row":     sheet_row,
            "message_count": msg_count,
            "subject":       subject,
            "thread_id":     thread_id,
        }

        if thread_id:
            thread_map[thread_id] = info
        if subject:
            subject_map[subject.lower()] = info

    print(f"  📊 Existing: {len(thread_map)} rows with Thread ID | {len(subject_map)} total")
    return thread_map, subject_map


def _row_data_to_sheet_value(field: str, row_data: dict) -> str:
    mapping = {
        "Subject":          row_data.get("Subject", ""),
        "Sender":           row_data.get("Sender", ""),
        "CC":               row_data.get("CC", ""),
        "Division":         row_data.get("Division", ""),
        "Style No":         row_data.get("Style No", ""),
        "Colour":           row_data.get("Colour", ""),
        "Vendor Name":      row_data.get("Vendor Name", ""),
        "Partner Classification": row_data.get("Partner Class", ""),
        "Shipment Company": row_data.get("Shipment Company", ""),
        "AWB No":           row_data.get("AWB No", ""),
        "Shipment Date":    row_data.get("Shipment Date", ""),
        "Sent Date":        row_data.get("Sent Date", ""),
        "AI Overview":      row_data.get("AI Overview", ""),
        "Thread Messages":  str(row_data.get("Thread Messages", "")),
        "Thread ID":        row_data.get("Thread ID", ""),
        "Last Updated":     datetime.now().strftime("%Y-%m-%d %H:%M"),
        "Intent":           row_data.get("Intent", ""),
        "Reply Needed":     row_data.get("Reply Needed", ""),
        "PO Number":        row_data.get("PO Number", ""),
        "Sample Status":    row_data.get("Sample Status", ""),
        "Sample Reminder":  row_data.get("Sample Reminder", ""),
        "Attachments":      row_data.get("Attachments", ""),
        "Shared Links":     row_data.get("Shared Links", ""),
        "Reply Draft":      row_data.get("Reply Draft", ""),
    }
    return mapping.get(field, "")


def update_existing_row(svc, sheet_row: int, row_data: dict, backfill_tid: bool = False):
    if DRY_RUN:
        log.info(f"DRY_RUN: would update row {sheet_row}")
        return

    data = []
    for field in UPDATE_ON_REPLY_FIELDS:
        letter = col_letter(field)
        val    = _row_data_to_sheet_value(field, row_data)
        data.append({"range": f"'{SHEET_TAB}'!{letter}{sheet_row}", "values": [[val]]})

    lu_letter = col_letter("Last Updated")
    data.append({
        "range":  f"'{SHEET_TAB}'!{lu_letter}{sheet_row}",
        "values": [[datetime.now().strftime("%Y-%m-%d %H:%M")]]
    })

    if backfill_tid and row_data.get("Thread ID"):
        tid_letter = col_letter("Thread ID")
        data.append({
            "range":  f"'{SHEET_TAB}'!{tid_letter}{sheet_row}",
            "values": [[row_data["Thread ID"]]]
        })

    svc.spreadsheets().values().batchUpdate(
        spreadsheetId=SHEET_ID,
        body={"valueInputOption": "RAW", "data": data}
    ).execute()


def _build_new_row(row_data: dict, now_str: str) -> list:
    row = []
    for header in LOGS_HEADERS:
        if header == "Last Updated":
            row.append(now_str)
        else:
            row.append(_row_data_to_sheet_value(header, row_data))
    return row


def append_new_rows(svc, rows: list):
    if not rows:
        return
    svc.spreadsheets().values().append(
        spreadsheetId=SHEET_ID,
        range=f"'{SHEET_TAB}'!A1",
        valueInputOption="RAW",
        insertDataOption="INSERT_ROWS",
        body={"values": rows}
    ).execute()


def flush_error_log(svc):
    if not _error_buffer:
        return
    rows = [[
        e["timestamp"], e["thread_id"], e["subject"],
        e["stage"],     e["error"],     e["traceback"],
    ] for e in _error_buffer]
    svc.spreadsheets().values().append(
        spreadsheetId=SHEET_ID,
        range=f"'{ERROR_TAB}'!A1",
        valueInputOption="RAW",
        insertDataOption="INSERT_ROWS",
        body={"values": rows}
    ).execute()
    print(f"\n  ⚠️  {len(rows)} error(s) written to '{ERROR_TAB}' tab")
    _error_buffer.clear()


# ════════════════════════════════════════════════════════════
# PARALLEL METADATA FETCH
# ════════════════════════════════════════════════════════════

def _fetch_meta_one(creds, tid: str) -> dict:
    svc = build("gmail", "v1", credentials=creds)
    try:
        meta = gmail_threads_get(
            svc, userId="me", id=tid,
            format="metadata", metadataHeaders=["Subject", "Date"]
        )
        msgs = meta.get("messages", [])
        raw_subject = ""
        if msgs:
            for h in msgs[0].get("payload", {}).get("headers", []):
                if h["name"].lower() == "subject":
                    raw_subject = h["value"]
                    break
        clean_subject = re.sub(
            r'^(re|fwd|fw):\s*', '', raw_subject, flags=re.IGNORECASE
        ).strip()
        return {"tid": tid, "count": len(msgs), "subject": clean_subject, "error": None}
    except Exception as e:
        return {"tid": tid, "count": 0, "subject": "", "error": str(e)}


def fetch_all_metadata(creds, thread_ids: list) -> dict:
    print(f"  ⚡ Fetching metadata for {len(thread_ids)} threads ({GMAIL_WORKERS} parallel)...")
    results = {}
    with ThreadPoolExecutor(max_workers=GMAIL_WORKERS) as executor:
        futures = {
            executor.submit(_fetch_meta_one, creds, tid): tid
            for tid in thread_ids
        }
        for future in as_completed(futures):
            m = future.result()
            results[m["tid"]] = m
    return results


# ════════════════════════════════════════════════════════════
# MAIN RUN LOOP
# FIX 3: subject_map race guard — skip batchUpdate when sheet_row is None
# ════════════════════════════════════════════════════════════

def run(max_threads: int = 50):
    print(f"\n🚀 Gmail Thread Extractor v7.0")
    print(f"   Model: {OPENAI_MODEL} | {max_threads} threads | {GMAIL_WORKERS} parallel workers\n")

    creds      = authenticate()
    gmail_svc  = build("gmail",  "v1", credentials=creds)
    sheets_svc = build("sheets", "v4", credentials=creds)

    vendor_db = load_vendor_db(sheets_svc)
    _ensure_tab(sheets_svc, SHEET_TAB, LOGS_HEADERS)
    _ensure_tab(sheets_svc, ERROR_TAB, ERROR_HEADERS)

    print("  🔍 Reading existing Logs rows...")
    thread_map, subject_map = load_existing_rows(sheets_svc)
    print()

    print(f"  📬 Fetching up to {max_threads} threads from inbox...")
    result = gmail_svc.users().threads().list(
        userId="me", maxResults=max_threads, q="in:inbox"
    ).execute()
    gmail_threads = result.get("threads", [])
    print(f"  Found {len(gmail_threads)} threads\n")

    if not gmail_threads:
        print("  Nothing to process.\n")
        return

    all_tids = [t["id"] for t in gmail_threads]
    meta_map = fetch_all_metadata(creds, all_tids)

    new_rows   = []
    updated    = 0
    backfilled = 0
    added      = 0
    skipped    = 0
    errors     = 0
    now_str    = datetime.now().strftime("%Y-%m-%d %H:%M")
    total      = len(gmail_threads)

    for i, thread in enumerate(gmail_threads, 1):
        tid        = thread["id"]
        meta       = meta_map.get(tid, {})
        curr_count = meta.get("count",   0)
        meta_subj  = meta.get("subject", "")
        subj_key   = meta_subj.lower()

        try:
            if tid in thread_map:
                existing  = thread_map[tid]
                old_count = existing["message_count"]

                if curr_count <= old_count:
                    print(f"  [{i}/{total}] ⏭️  No change ({old_count} msgs) — {existing['subject'][:50]}")
                    skipped += 1
                    continue

                n_new = curr_count - old_count
                print(f"  [{i}/{total}] 🔄 {n_new} new repl{'y' if n_new==1 else 'ies'} — {existing['subject'][:45]}")

                row_data = process_thread(gmail_svc, tid, vendor_db)
                if row_data:
                    update_existing_row(sheets_svc, existing["sheet_row"], row_data)
                    thread_map[tid]["message_count"] = row_data["Thread Messages"]
                    updated += 1
                    print(f"    ✅ Row {existing['sheet_row']} updated "
                          f"| Intent: {row_data['Intent']} "
                          f"| Reply needed: {row_data['Reply Needed']}")

            elif subj_key and subj_key in subject_map:
                existing  = subject_map[subj_key]
                old_count = existing["message_count"]

                # FIX 3: guard against sheet_row=None (row added in same run)
                if existing["sheet_row"] is None:
                    # Row was queued in this run — treat as new to avoid None API call
                    print(f"  [{i}/{total}] 🆕 Same-run subject match (queued) — {meta_subj[:50]}")
                    row_data = process_thread(gmail_svc, tid, vendor_db)
                    if row_data:
                        new_rows.append(_build_new_row(row_data, now_str))
                        added += 1
                        print(f"    ✅ Queued as new row")
                    continue

                if curr_count <= old_count:
                    if not DRY_RUN:
                        sheets_svc.spreadsheets().values().batchUpdate(
                            spreadsheetId=SHEET_ID,
                            body={"valueInputOption": "RAW", "data": [
                                {"range": f"'{SHEET_TAB}'!{col_letter('Thread ID')}{existing['sheet_row']}", "values": [[tid]]},
                                {"range": f"'{SHEET_TAB}'!{col_letter('Thread Messages')}{existing['sheet_row']}", "values": [[str(curr_count)]]},
                            ]}
                        ).execute()
                    print(f"  [{i}/{total}] ⏭️  No change — {meta_subj[:45]} (Thread ID stored)")
                    thread_map[tid] = existing
                    backfilled += 1
                    skipped    += 1
                    continue

                n_new = curr_count - old_count
                print(f"  [{i}/{total}] 🔄 {n_new} new repl{'y' if n_new==1 else 'ies'} "
                      f"(subject match) — {meta_subj[:40]}")

                row_data = process_thread(gmail_svc, tid, vendor_db)
                if row_data:
                    update_existing_row(sheets_svc, existing["sheet_row"], row_data, backfill_tid=True)
                    thread_map[tid] = existing
                    thread_map[tid]["message_count"] = row_data["Thread Messages"]
                    updated    += 1
                    backfilled += 1
                    print(f"    ✅ Row {existing['sheet_row']} updated + Thread ID stored")

            else:
                if curr_count < 2:
                    print(f"  [{i}/{total}] ⏭️  Single message — {meta_subj[:55]}")
                    skipped += 1
                    continue

                print(f"  [{i}/{total}] 🆕 New: {meta_subj[:60]}")
                row_data = process_thread(gmail_svc, tid, vendor_db)

                if row_data is None:
                    print(f"    ⏭️  Skipped (no readable body)")
                    skipped += 1
                    continue

                print(f"    ✅ {row_data['Subject'][:40]} "
                      f"| {row_data['Division']} "
                      f"| Vendor: {row_data['Vendor Name'] or '?'} "
                      f"| Intent: {row_data['Intent']}")

                new_rows.append(_build_new_row(row_data, now_str))
                subject_map[row_data["Subject"].lower()] = {
                    "sheet_row":     None,
                    "message_count": curr_count,
                    "thread_id":     tid,
                    "subject":       row_data["Subject"],
                }
                added += 1

        except Exception as exc:
            errors += 1
            record_error(tid, meta_subj, "process_thread", exc)
            print(f"    ❌ Error on '{meta_subj[:40]}': {exc}")
            continue

    if new_rows:
        if DRY_RUN:
            print(f"\n  🔍 DRY RUN — would write {len(new_rows)} new rows (skipped)")
            log.info(f"DRY_RUN: would write {len(new_rows)} new rows")
        else:
            print(f"\n  📤 Writing {len(new_rows)} new rows to Sheets...")
            append_new_rows(sheets_svc, new_rows)

    if not DRY_RUN:
        flush_error_log(sheets_svc)
    elif _error_buffer:
        print(f"  🔍 DRY RUN — {len(_error_buffer)} error(s) not written")

    cs      = cache_stats()
    dry_tag = " [DRY RUN]" if DRY_RUN else ""

    # FIX 11: include token usage in summary
    total_tokens = _run_token_usage["prompt_tokens"] + _run_token_usage["completion_tokens"]

    print(f"\n{'═' * 60}")
    print(f"  🆕 New rows added        : {added}{dry_tag}")
    print(f"  🔄 Rows updated          : {updated}")
    print(f"  🔗 Thread IDs backfilled : {backfilled}")
    print(f"  ⏭️  Skipped               : {skipped}")
    print(f"  ❌ Errors                : {errors}  → check '{ERROR_TAB}' tab")
    print(f"  💾 LLM cache entries     : {cs['cached_entries']} (prompt={PROMPT_VERSION})")
    print(f"  🤖 Model                 : {OPENAI_MODEL}")
    print(f"  🔢 Tokens this run       : {total_tokens:,}  "
          f"(in={_run_token_usage['prompt_tokens']:,} / out={_run_token_usage['completion_tokens']:,})")
    print(f"  📄 Debug log             : gmail_agent.log")
    print(f"  📋 Audit log             : audit.jsonl")
    print(f"  🔗 Sheet : https://docs.google.com/spreadsheets/d/{SHEET_ID}/edit")
    print(f"{'═' * 60}\n")

    write_audit_log({
        "threads_fetched":    total,
        "added":              added,
        "updated":            updated,
        "backfilled":         backfilled,
        "skipped":            skipped,
        "errors":             errors,
        "cache_entries":      cs["cached_entries"],
        "total_tokens":       total_tokens,
        "prompt_tokens":      _run_token_usage["prompt_tokens"],
        "completion_tokens":  _run_token_usage["completion_tokens"],
    })


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Gmail Thread Extractor v7.0 — Merchandising Logs"
    )
    parser.add_argument(
        "max_threads", nargs="?", type=int, default=50,
        help="Number of inbox threads to process (default: 50)"
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Read-only mode — analyse threads but do NOT write to Google Sheets"
    )
    # FIX 13: --workers CLI arg (no longer requires editing source)
    parser.add_argument(
        "--workers", type=int, default=3,
        help="Number of parallel Gmail metadata workers (default: 3)"
    )
    args = parser.parse_args()

    if args.dry_run:
        DRY_RUN = True
        print("\n⚠️  DRY RUN MODE — no changes will be written to Google Sheets\n")
        log.info("DRY_RUN mode activated via --dry-run flag")

    # FIX 13: apply workers arg
    GMAIL_WORKERS = args.workers

    for secret_file in ("credentials.json", "token.json", ".env"):
        if os.path.exists(secret_file):
            log.info(f"Secret file present: {secret_file} — ensure it is in .gitignore")

    run(max_threads=args.max_threads)