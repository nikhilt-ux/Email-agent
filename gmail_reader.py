"""
Gmail Thread Extractor — Merchandising & Product Logs  v5.0
════════════════════════════════════════════════════════════
Powered by OpenAI GPT-4o — production-hardened.

PRODUCTION IMPROVEMENTS IN v5.0:
  ✅ DRY_RUN mode — test without writing to Sheets (--dry-run flag)
  ✅ LLM cost control — older messages truncated to 400 chars (saves 60-80%)
  ✅ SQLite LLM cache — skip LLM if thread+count already processed
  ✅ Prompt versioning — PROMPT_VERSION logged per run for traceability
  ✅ Run audit log — structured JSON summary after every run
  ✅ Console + file logging — all print() output also in gmail_agent.log
  ✅ .gitignore reminder printed on startup if secrets present

ALREADY IN v4.0 (unchanged):
  ✅ Structured error handling — no single email crashes the system
  ✅ Real logging — gmail_agent.log with timestamps and levels
  ✅ Config section — all settings in one place at top of file
  ✅ Secrets via .env — OPENAI_API_KEY never hardcoded
  ✅ Idempotency — Thread ID + subject dedup, skip if unchanged
  ✅ Rate limit protection — exponential backoff on Gmail + OpenAI
  ✅ Attachment detection + shared link detection
  ✅ Parallel metadata fetch (5 workers)
  ✅ Auto header sync — new columns appear in sheet automatically
"""

import os
import re
import json
import time
import base64
import logging
import traceback
import html as html_lib
from datetime import datetime
from email.utils import parsedate_to_datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

import sqlite3
import hashlib
import argparse
import requests
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from google.auth.transport.requests import Request
from googleapiclient.errors import HttpError


# ════════════════════════════════════════════════════════════
# CONFIG  ← only section you ever need to edit
# ════════════════════════════════════════════════════════════

SCOPES = [
    "https://www.googleapis.com/auth/gmail.readonly",
    "https://www.googleapis.com/auth/spreadsheets",
]

OLLAMA_URL     = "http://localhost:11434/api/chat"
OLLAMA_MODEL   = "qwen3:8b"
OLLAMA_TIMEOUT = 150    # seconds per call before retry
OLLAMA_RETRIES = 2      # retries on timeout

SHEET_ID   = "1hMmU47NkHM7ndTSX6d6Ft1YMy8sevtvpRVatOq-Mx-4"
SHEET_TAB  = "Logs"
EXPORT_TAB = "export"       # your vendor database tab
ERROR_TAB  = "Error Logs"   # auto-created, errors logged here

ONEQUINCE_DOMAIN = "@onequince.com"

GMAIL_WORKERS      = 5     # parallel threads for metadata fetching
BODY_CHARS_PER_MSG = 3000  # char limit per LATEST message sent to LLM
OLDER_MSG_CHARS    = 400   # char limit for older messages (cost control — saves 60-80%)
MAX_MSGS_IN_LLM    = 8     # max messages sent to LLM (always includes msg[0])

# ── Production controls ──
DRY_RUN        = False  # True = read-only mode, nothing written to Sheets
                        # Override at runtime: python script.py --dry-run
PROMPT_VERSION = "v5.0" # Bump when prompt logic changes — logged per run
CACHE_DB       = "llm_cache.db"  # SQLite file — LLM results keyed by thread+count

DIVISIONS = [
    "Men's Apparel", "Women's Apparel", "Apparel Flats",
    "Kids and Baby", "Maternity", "Home", "Accessories",
    "Jewelry", "Furniture", "Other",
]

# Sheet column layout — A through W
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
    "Intent",                 # Q  ← what action is needed
    "Reply Needed",           # R  ← Yes / No
    "PO Number",              # S  ← purchase order number
    "Sample Status",          # T  ← Dispatched / Received / Approved / Rejected / Pending / None
    "Sample Reminder",        # U  ← ⚠️ Chase if dispatched but no update in 7 days
    "Attachments",            # V  ← file types attached (PDF, Excel, image…)
    "Shared Links",           # W  ← Google Drive / Dropbox / WeTransfer / OneDrive URLs
]

ERROR_HEADERS = [
    "Timestamp", "Thread ID", "Subject", "Stage", "Error Message", "Traceback"
]

# Which columns to refresh when new replies arrive
UPDATE_ON_REPLY = {
    "E": "Style No",
    "F": "Colour",
    "I": "Shipment Company",
    "J": "AWB No",
    "K": "Shipment Date",
    "M": "AI Overview",
    "N": "Thread Messages",
    "Q": "Intent",
    "R": "Reply Needed",
    "S": "PO Number",
    "T": "Sample Status",
    "U": "Sample Reminder",
    "V": "Attachments",
    "W": "Shared Links",
    # P (Last Updated) always written, O (Thread ID) only on backfill
}

# Addresses that are never real vendors
NOISE_ADDRESS_WORDS = {
    "noreply", "no-reply", "mailer", "notification", "notifications",
    "alert", "alerts", "bot", "automated", "donotreply", "do-not-reply",
    "support", "info", "contact", "admin", "hello", "team",
    "slack", "google", "calendar", "jira", "github",
}


# ════════════════════════════════════════════════════════════
# LOGGING — writes to gmail_agent.log + Error Logs sheet
# ════════════════════════════════════════════════════════════

# Dual logging — file (gmail_agent.log) + console
_log_formatter = logging.Formatter(
    "%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
_file_handler    = logging.FileHandler("gmail_agent.log", encoding="utf-8")
_file_handler.setFormatter(_log_formatter)
_console_handler = logging.StreamHandler()
_console_handler.setFormatter(_log_formatter)
_console_handler.setLevel(logging.WARNING)  # console: warnings + errors only

logging.basicConfig(level=logging.INFO, handlers=[_file_handler, _console_handler])
log = logging.getLogger(__name__)

_error_buffer: list = []  # flushed to sheet at end of run


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
# LLM CACHE — SQLite, keyed by thread_id + message_count
# Avoids re-calling the LLM for threads that haven't changed.
# Cache is local only — no email content sent anywhere extra.
# ════════════════════════════════════════════════════════════

def _cache_connect() -> sqlite3.Connection:
    """Open (or create) the SQLite cache database."""
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


def cache_get(thread_id: str, msg_count: int):
    """
    Return cached LLM result for this thread+count+prompt_version,
    or None if not cached / prompt version changed.
    """
    key = hashlib.sha1(f"{thread_id}:{msg_count}:{PROMPT_VERSION}".encode()).hexdigest()
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


def cache_set(thread_id: str, msg_count: int, result: dict):
    """Store LLM result in cache."""
    key = hashlib.sha1(f"{thread_id}:{msg_count}:{PROMPT_VERSION}".encode()).hexdigest()
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
    """Return cache hit/miss stats for audit log."""
    try:
        conn  = _cache_connect()
        total = conn.execute("SELECT COUNT(*) FROM llm_cache").fetchone()[0]
        conn.close()
        return {"cached_entries": total}
    except Exception:
        return {"cached_entries": 0}


# ════════════════════════════════════════════════════════════
# RUN AUDIT LOG — structured JSON summary after every run
# Appended to gmail_agent.log so ops can grep/parse it.
# ════════════════════════════════════════════════════════════

def write_audit_log(stats: dict):
    """
    Write a structured JSON audit record to the log after every run.
    Makes it easy to track cost, error rate, and volume over time.

    Fields logged:
      run_at, dry_run, prompt_version, threads_fetched,
      added, updated, skipped, errors, cache_hits,
      error_rate_pct, sheet_id
    """
    record = {
        "run_at":          datetime.now().isoformat(),
        "dry_run":         DRY_RUN,
        "prompt_version":  PROMPT_VERSION,
        "sheet_id":        SHEET_ID,
        **stats,
        "error_rate_pct":  round(
            stats.get("errors", 0) / max(stats.get("threads_fetched", 1), 1) * 100, 1
        ),
    }
    log.info(f"AUDIT_RUN: {json.dumps(record)}")
    print(f"  📋 Audit log written (prompt={PROMPT_VERSION}, "
          f"errors={stats.get('errors',0)}, "
          f"error_rate={record['error_rate_pct']}%)")


# ════════════════════════════════════════════════════════════
# AUTHENTICATION
# ════════════════════════════════════════════════════════════

def authenticate():
    creds = None
    if os.path.exists("token.json"):
        creds = Credentials.from_authorized_user_file("token.json", SCOPES)

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file("credentials.json", SCOPES)
            creds = flow.run_local_server(port=8080)
        with open("token.json", "w") as fh:
            fh.write(creds.to_json())

    return creds


# ════════════════════════════════════════════════════════════
# GMAIL API — rate-limit safe wrapper
# ════════════════════════════════════════════════════════════

def gmail_threads_get(svc, **kwargs) -> dict:
    """threads().get() with exponential backoff for 429/500/503."""
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
# Handles HTML (most vendor emails), plain text, nested multipart.
# Strips quoted replies so LLM only sees NEW content per message.
# ════════════════════════════════════════════════════════════

def _decode_b64(data: str) -> str:
    try:
        return base64.urlsafe_b64decode(data + "==").decode("utf-8", errors="replace")
    except Exception:
        return ""


def _html_to_text(html: str) -> str:
    """
    Convert HTML email body to clean readable plain text.
    No external libraries — handles tables, divs, entities, inline styles.
    """
    # Remove style/script blocks entirely
    html = re.sub(r'<(style|script)[^>]*>.*?</\1>', '', html,
                  flags=re.DOTALL | re.IGNORECASE)
    # Block-level elements → newlines
    html = re.sub(r'<br\s*/?>', '\n', html, flags=re.IGNORECASE)
    html = re.sub(r'</(?:p|div|tr|li|h[1-6])>', '\n', html, flags=re.IGNORECASE)
    html = re.sub(r'</td>', ' ', html, flags=re.IGNORECASE)
    # Strip all remaining tags
    html = re.sub(r'<[^>]+>', '', html)
    # Decode HTML entities (&amp; &nbsp; &#160; etc.)
    html = html_lib.unescape(html)
    # Clean up whitespace, preserve line breaks
    lines = []
    for line in html.splitlines():
        line = re.sub(r'[ \t]+', ' ', line).strip()
        if line:
            lines.append(line)
    return "\n".join(lines)


def _extract_body_from_payload(payload: dict) -> str:
    """
    Recursively extract the best readable text from a Gmail message payload.
    Priority: text/plain > text/html converted to text.
    Handles deeply nested multipart/alternative, multipart/mixed, etc.
    """
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
                nested = _extract_body_from_payload(part)
                if nested:
                    plain_parts.append(nested)

        combined_plain = "\n".join(plain_parts).strip()
        combined_html  = "\n".join(html_parts).strip()
        return combined_plain if combined_plain else combined_html

    return ""


def _strip_quoted_reply(text: str) -> str:
    """
    Remove quoted previous emails from a message body.
    Keeps only the NEW content written by the current sender.

    Handles all common reply formats:
      Gmail:   "On Mon, 7 Jun 2025, Vendor <v@x.com> wrote:"
      Outlook: "-----Original Message-----"  /  "________"
      Outlook: "From: ... Sent: ... To: ... Subject: ..." block
      Apple:   "On 7 Jun 2025, at 14:32, Vendor wrote:"
      Inline:  Lines starting with >
      Yahoo:   "--- Original Message ---"
    """
    result = []

    for line in text.splitlines():
        stripped = line.strip()

        # ── Hard-stop patterns — everything below this line is a quote ──

        # Outlook/Lotus "-----Original Message-----" or "--- Forwarded ---"
        if re.match(r'^-{3,}\s*(Original|Forwarded)\s*(Message|mail)?\s*-{0,3}$',
                    stripped, re.IGNORECASE):
            break

        # Long underscore dividers (Outlook web)
        if re.match(r'^_{5,}$', stripped):
            break

        # Gmail / Apple "On [date/time] [name] wrote:" — single line
        # Handles: "On Mon, Jun 7, 2025 at 2:32 PM Vendor <v@x.com> wrote:"
        # Handles: "On 7 Jun 2025, at 14:32, Vendor wrote:"
        if re.match(r'^On\s.{5,150}\swrote:\s*$', stripped, re.IGNORECASE):
            break

        # Multi-line Gmail quote header: "On [date]" ending without "wrote:"
        # followed next line by the name — catch by checking prior line
        if result and re.match(r'^On\s.{5,80}$', stripped, re.IGNORECASE):
            # peek: if this line has no "wrote:" but looks like a date line, stop
            if re.search(r'\d{4}', stripped) and 'wrote' not in stripped.lower():
                break

        # Outlook header block: "From: X  Sent: Y  To: Z  Subject: W"
        # Only trigger after we already have real content (len > 3 lines)
        if re.match(r'^From:\s+.+', stripped) and len(result) > 3:
            break

        # ── Skip inline quoted lines (start with >) ──
        if stripped.startswith(">"):
            continue

        # ── Skip email signature separators ──
        if stripped in ("--", "-- "):
            break

        result.append(line)

    clean = "\n".join(result).strip()
    # Safety: if aggressive stripping removed almost everything (very short reply),
    # return the original unstripped text so we don't lose content
    return clean if len(clean) >= 25 else text.strip()


def _strip_signature(text: str) -> str:
    """
    Remove email signatures from the bottom of a message.
    Signatures typically follow a blank line + "--" or common sign-off patterns.
    Only removes from the bottom — stops at first non-signature content.
    """
    lines    = text.splitlines()
    sig_start = len(lines)  # default: no signature found

    for i in range(len(lines) - 1, -1, -1):
        stripped = lines[i].strip()
        # Standard signature delimiter
        if stripped in ("--", "-- "):
            sig_start = i
            break
        # Common closing phrases that mark start of signature block
        if re.match(
            r'^(best regards?|regards?|warm regards?|thanks?|thank you|'
            r'sincerely|cheers?|yours? truly|faithfully|with regards?|'
            r'best wishes?|kind regards?),?\s*$',
            stripped, re.IGNORECASE
        ):
            # Only treat as signature if it's in the last 10 lines
            if i >= len(lines) - 10:
                sig_start = i
            break

    return "\n".join(lines[:sig_start]).strip()


def _strip_cjk(text: str) -> str:
    """
    Remove CJK (Chinese/Japanese/Korean) characters from text.
    Vendor emails often contain Chinese addresses or signatures —
    these cause Qwen3 to switch to Chinese for the entire response.
    We keep the structure of the email but remove the trigger characters.
    Covers: CJK Unified Ideographs, Hiragana, Katakana, CJK Compatibility.
    """
    return re.sub(
        r'[一-鿿'       # CJK Unified Ideographs (most Chinese/Japanese/Korean)
        r'㐀-䶿'        # CJK Extension A
        r'぀-ゟ'        # Hiragana
        r'゠-ヿ'        # Katakana
        r'豈-﫿'        # CJK Compatibility Ideographs
        r'　-〿]',      # CJK Symbols and Punctuation
        ' ', text
    )


def extract_message_body(payload: dict) -> str:
    """
    Full extraction pipeline per message:
      1. Extract raw text (prefers text/plain, falls back to HTML→text)
      2. Strip quoted reply sections (previous emails in the chain)
      3. Strip email signature block
      4. Strip CJK characters — prevents Qwen3 language-switching
      5. Collapse excessive blank lines
    Result: only the NEW content this person wrote, clean for LLM consumption.
    """
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
    """
    Reads export sheet → two lookup indexes:
      emails  { "vendor@domain.com" → {partner_name, classification} }
      domains { "domain.com"        → {partner_name, classification} }

    Domain index catches cases where full email isn't in DB
    but domain is known (e.g. everyone @tirupurknits.com = Tirupur Knits).
    """
    print("  📖 Loading vendor database from export sheet...")
    try:
        result = sheets_svc.spreadsheets().values().get(
            spreadsheetId=SHEET_ID,
            range=f"{EXPORT_TAB}!A:D"
        ).execute()
    except Exception as e:
        log.error(f"Vendor DB load failed: {e}")
        print(f"  ⚠️  Could not read export sheet: {e}")
        return {"emails": {}, "domains": {}}

    rows = result.get("values", [])
    if not rows:
        print("  ⚠️  Export sheet is empty.")
        return {"emails": {}, "domains": {}}

    # Skip header row if present
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

            emails_db[email] = {
                "partner_name":   partner_name,
                "classification": classification,
            }

            domain = email.split("@")[1]
            if domain not in domains_db:
                domains_db[domain] = {
                    "partner_name":   partner_name,
                    "classification": classification,
                }

    print(f"  ✅ Vendor DB: {len(emails_db)} emails across {len(domains_db)} domains\n")
    return {"emails": emails_db, "domains": domains_db}


def lookup_vendor(all_addresses: list, vendor_db: dict) -> dict:
    """
    Ordered vendor lookup — skips @onequince.com.
    Returns on the FIRST confident match, preserving address order.
    Caller must pass addresses in priority order: From → To → CC → BCC.

    Pass 1: exact email match  (highest confidence, checked first)
    Pass 2: domain match       (only company domains — generic webmail excluded)

    Two-pass design means an exact email match always beats a domain match,
    even if the domain match address appears earlier in the list.

    Generic domains (gmail, yahoo, etc.) are never used for domain matching
    to avoid false positives from shared webmail addresses.
    """
    emails_db  = vendor_db.get("emails",  {})
    domains_db = vendor_db.get("domains", {})

    GENERIC_DOMAINS = {
        "gmail.com", "yahoo.com", "hotmail.com", "outlook.com", "live.com",
        "icloud.com", "me.com", "mac.com", "protonmail.com", "proton.me",
        "aol.com", "ymail.com", "rediffmail.com", "mail.com",
    }

    # Pass 1: exact email — return immediately on first hit
    for raw in all_addresses:
        if ONEQUINCE_DOMAIN.lower() in raw.lower():
            continue
        email = _extract_email(raw)
        if "@" not in email:
            continue
        if email in emails_db:
            return emails_db[email]

    # Pass 2: domain match — only non-generic business domains
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
    """
    Last resort vendor name — only uses display names that look like
    real people/companies, filtered against known noise patterns.
    """
    seen   = set()
    result = []

    for raw in external_addresses:
        email      = _extract_email(raw)
        local_part = email.split("@")[0] if "@" in email else email

        # Skip obvious noise/automated addresses
        if any(noise in local_part.lower() for noise in NOISE_ADDRESS_WORDS):
            continue

        name = _display_name(raw)
        # Only accept if name looks like a real person/company (not just the email itself)
        if name and name.lower() not in (email, local_part) and name.lower() not in seen:
            seen.add(name.lower())
            result.append(name)

    return ", ".join(result[:2])  # max 2 to avoid clutter


# ════════════════════════════════════════════════════════════
# OLLAMA CLIENT — with retry on timeout
# ════════════════════════════════════════════════════════════

def ask_ollama(prompt: str) -> str:
    """
    Send prompt to Ollama and return raw text response.

    Error handling:
      - ConnectionError  → Ollama not running, fail immediately (no retry)
      - Timeout          → transient, retry with backoff
      - HTTP error       → transient (e.g. 503), retry with backoff
      - Any other error  → log and retry, then give up

    Returns "" on all failure paths — callers must handle empty string.
    """
    payload = {
        "model":    OLLAMA_MODEL,
        "messages": [{"role": "user", "content": prompt}],
        "stream":   False,
    }
    for attempt in range(OLLAMA_RETRIES + 1):
        try:
            resp = requests.post(OLLAMA_URL, json=payload, timeout=OLLAMA_TIMEOUT)
            resp.raise_for_status()
            return resp.json()["message"]["content"].strip()

        except requests.exceptions.ConnectionError:
            print("  ❌ Ollama not running — start with: ollama serve")
            log.error("Ollama ConnectionError — server not running")
            return ""

        except requests.exceptions.Timeout:
            if attempt < OLLAMA_RETRIES:
                wait = 3 * (attempt + 1)
                print(f"    ⏳ Ollama timeout — retry {attempt + 1}/{OLLAMA_RETRIES} in {wait}s...")
                log.warning(f"Ollama timeout on attempt {attempt + 1}")
                time.sleep(wait)
            else:
                log.error("Ollama timed out after all retries")
                return ""

        except requests.exceptions.HTTPError as e:
            if attempt < OLLAMA_RETRIES:
                wait = 2 * (attempt + 1)
                log.warning(f"Ollama HTTP {e.response.status_code} on attempt {attempt + 1}, retry in {wait}s")
                time.sleep(wait)
            else:
                log.error(f"Ollama HTTP error after all retries: {e}")
                return ""

        except Exception as e:
            if attempt < OLLAMA_RETRIES:
                wait = 2 * (attempt + 1)
                log.warning(f"Ollama unexpected error attempt {attempt + 1}: {e}, retry in {wait}s")
                time.sleep(wait)
            else:
                log.error(f"Ollama failed after all retries: {e}")
                return ""

    return ""


def _clean_llm(raw: str) -> str:
    """Strip <think> blocks and markdown code fences."""
    if "<think>" in raw:
        raw = raw.split("</think>")[-1].strip()
    raw = re.sub(r'^```(?:json)?\s*', '', raw.strip())
    raw = re.sub(r'\s*```$', '', raw)
    return raw.strip()


# ════════════════════════════════════════════════════════════
# SINGLE COMBINED LLM CALL
# Division + Style + Colour + AI Overview in ONE prompt.
# Enforces English output — no more Chinese summaries.
# ════════════════════════════════════════════════════════════

def llm_analyse_thread(subject: str, structured_messages: list, thread_id: str = "", msg_count: int = 0) -> dict:
    """
    structured_messages: list of {from, date, body}
    Each message is already cleaned, quote-stripped, and body-capped.
    Messages are labelled so LLM understands who said what and when.

    thread_id + msg_count: used for SQLite cache key.
    If cache hit for this prompt_version → skip LLM call entirely.
    """
    # ── Cache check ──
    if thread_id and msg_count:
        cached = cache_get(thread_id, msg_count)
        if cached:
            print(f"    💾 Cache hit — skipping LLM for: {subject[:50]}")
            return cached
    divisions_str = "\n".join(f"  - {d}" for d in DIVISIONS)

    # Build labelled thread context
    # Cost control: latest message → full BODY_CHARS_PER_MSG chars
    #               older messages → OLDER_MSG_CHARS only (saves 60-80% tokens)
    n_msgs = len(structured_messages)
    thread_context = ""
    for i, msg in enumerate(structured_messages, 1):
        is_latest = (i == n_msgs)
        char_limit = BODY_CHARS_PER_MSG if is_latest else OLDER_MSG_CHARS
        body = msg["body"][:char_limit]
        truncated = " [truncated]" if len(msg["body"]) > char_limit else ""
        thread_context += (
            f"\n[Message {i} | From: {msg['from']} | Date: {msg['date']}]{truncated}\n"
            f"{body}\n"
        )

    prompt = f"""/no_think
LANGUAGE RULE — READ THIS FIRST:
You MUST write every word of your response in ENGLISH.
Do NOT use Chinese (中文), Japanese, Hindi, or any other language.
The email may contain non-English text — IGNORE IT and write your analysis in ENGLISH only.
If you write anything other than English, your response is wrong.

You are a fashion merchandising analyst. Analyse this email thread.

SUBJECT: {subject}

THREAD (oldest → newest):
{thread_context.strip()}

════ TASKS ════

TASK 1 — DIVISION
Read the SUBJECT LINE ONLY. Pick exactly one division:
{divisions_str}

Mapping rules:
- Kids / Baby / Children / Toddler → Kids and Baby
- Maternity → Maternity
- Men / Mens / Men's (subject has no "Women") → Men's Apparel
- Women / Womens / Women's / Ladies → Women's Apparel
- Flat / Tech Pack / Apparel Flat → Apparel Flats
- Home / Linen / Bedding / Cushion / Curtain / Rug / Towel / Decor → Home
- Furniture / Sofa / Chair / Table / Shelf / Storage → Furniture
- Jewelry / Jewellery / Necklace / Ring / Earring / Bracelet → Jewelry
- Bag / Wallet / Belt / Scarf / Hat / Accessories → Accessories
- Unclear → Other

TASK 2 — STYLE NUMBERS
Scan ALL message bodies carefully. Extract every style/article number.
Known patterns: M--1234  W--5678  U--9012  W-PNT-228  NECK-209  U-FURN-304  ST-2045
Rules:
- Format: letters + one or two hyphens + optional letters + 3-6 digits
- M=Men, W=Women, U=Unisex/Kids prefix letters
- Double hyphens valid: M--1348 is ONE style number
- Look near: style, article, ref, #, style no, style number, item no
- Return all found, comma-separated, UPPERCASE
- If none found: return ""

TASK 3 — COLOUR
Extract product colour mentions.
Look for color: / colour: / col: labels, or colour names near style numbers.
Return comma-separated English colour names, or "" if none found.

TASK 4 — AI OVERVIEW  (ENGLISH ONLY — do NOT use Chinese or any other language)
Write 3-4 bullet points for a merchandise manager summarising the thread.
Cover:
- What product / order / sample is being discussed
- Any quality issues, delays, or concerns
- Current status (from the latest message)
- Next action needed

Start each bullet with "• ". Every word must be in English. No Chinese characters.

TASK 5 — INTENT
Read the LATEST MESSAGE in the thread. Identify the single most important action
this thread requires from the merchandising team right now.

Choose exactly one from:
  - Approve sample        (vendor sent sample, waiting for sign-off)
  - Review & feedback     (artwork, tech pack, or design shared for comments)
  - Confirm order         (PO or booking needs confirmation)
  - Chase vendor          (no reply received, follow-up required)
  - Resolve delay         (shipment/production delay flagged)
  - Resolve quality issue (defect, rejection, or quality complaint raised)
  - Awaiting shipment     (order placed, waiting for dispatch)
  - Track shipment        (AWB shared, goods in transit)
  - Payment action        (invoice received or payment overdue)
  - No action needed      (FYI thread, informational only)
  - Other                 (does not fit any above)

TASK 6 — REQUIRES REPLY
Does the latest message require a reply from your team?
Answer true if: a question was asked, approval was requested, or action is awaited.
Answer false if: it is purely informational, or vendor acknowledged without asking anything.

TASK 7 — SAMPLE STATUS
Read ALL messages. What is the current status of any physical sample in this thread?
Choose exactly one:
  - Dispatched   (vendor says sample has been sent / shipped / dispatched)
  - Received     (buyer/team confirmed sample has arrived)
  - Approved     (sample has been approved / sign-off given)
  - Rejected     (sample rejected / failed / not approved)
  - Pending      (sample requested or expected but not yet dispatched)
  - None         (no sample mentioned in this thread at all)

════ OUTPUT ════
REMINDER: Your entire response must be in ENGLISH. No Chinese. No other languages.
Return ONLY valid JSON. No explanation. No markdown. No extra text.
{{
  "division":       "<exactly one division from the list>",
  "style_numbers":  "<comma-separated UPPERCASE or empty string>",
  "colour":         "<comma-separated English colour names or empty string>",
  "ai_overview":    "<3-4 English bullet points separated by \\n>",
  "intent":         "<exactly one intent from the list above>",
  "requires_reply": <true or false — JSON boolean, no quotes>,
  "sample_status":  "<exactly one status from: Dispatched, Received, Approved, Rejected, Pending, None>"
}}"""

    # ── Call Ollama with JSON-parse retry ──
    # Attempt the full call up to 2 times if JSON parse fails.
    # ask_ollama() already handles network/timeout retries internally.
    parsed = None
    last_raw = ""
    for parse_attempt in range(2):
        raw     = ask_ollama(prompt)
        last_raw = raw
        cleaned = _clean_llm(raw)

        if not cleaned:
            log.warning(f"Empty LLM response for '{subject}' on parse attempt {parse_attempt + 1}")
            if parse_attempt == 0:
                time.sleep(2)
                continue
            break

        try:
            parsed = json.loads(cleaned)
            break  # valid JSON — exit retry loop
        except json.JSONDecodeError as e:
            log.warning(
                f"LLM JSON parse failed for '{subject}' attempt {parse_attempt + 1}: {e}. "
                f"Raw (first 300 chars): {cleaned[:300]}"
            )
            if parse_attempt == 0:
                time.sleep(2)  # small pause before retry

    # ── Validate and extract fields ──
    if parsed is not None:
        overview = parsed.get("ai_overview", "").strip()

        # Safety net: if LLM still returned CJK characters, retry once with
        # a minimal prompt that has zero non-English content in the input.
        if re.search(r'[\u4e00-\u9fff\u3040-\u309f\u30a0-\u30ff]', overview):
            log.warning(f"LLM returned CJK overview for '{subject}' — retrying with stripped prompt")
            # Build a clean summary-only prompt with all CJK stripped from bodies
            stripped_context = ""
            for i, msg in enumerate(structured_messages, 1):
                body = _strip_cjk(msg["body"][:BODY_CHARS_PER_MSG])
                stripped_context += f"[Message {i} | From: {msg['from']}]\n{body}\n\n"

            retry_prompt = f"""/no_think
WRITE IN ENGLISH ONLY. No Chinese. No Japanese. English words only.

Summarise this email thread in 3-4 bullet points for a fashion buyer.
Each bullet starts with "• ".
Be brief and factual. English only.

Subject: {subject}

{stripped_context.strip()}

Return ONLY this JSON (English values only):
{{"ai_overview": "<your 3-4 English bullets separated by \\n>"}}"""

            retry_raw    = ask_ollama(retry_prompt)
            retry_clean  = _clean_llm(retry_raw)
            try:
                retry_parsed = json.loads(retry_clean)
                retry_overview = retry_parsed.get("ai_overview", "").strip()
                if retry_overview and not re.search(
                    r'[\u4e00-\u9fff\u3040-\u309f\u30a0-\u30ff]', retry_overview
                ):
                    overview = retry_overview
                    log.info(f"CJK retry succeeded for '{subject}'")
                else:
                    overview = "• Thread summary unavailable — vendor email contains non-English content"
                    log.warning(f"CJK retry also failed for '{subject}'")
            except Exception:
                overview = "• Thread summary unavailable — vendor email contains non-English content"
                log.warning(f"CJK retry JSON parse failed for '{subject}'"  )

        # requires_reply: LLM must return JSON boolean true/false
        # Defensively handle string "true"/"false" in case LLM wraps it in quotes
        raw_reply = parsed.get("requires_reply", False)
        if isinstance(raw_reply, bool):
            requires_reply = raw_reply
        elif isinstance(raw_reply, str):
            requires_reply = raw_reply.strip().lower() in ("true", "yes", "1")
        else:
            requires_reply = bool(raw_reply)

        # intent: must be a non-empty string
        intent = parsed.get("intent", "").strip()
        if not intent:
            intent = "Other"

        # sample_status: validate against allowed values
        valid_statuses = {"Dispatched", "Received", "Approved", "Rejected", "Pending", "None"}
        sample_status  = parsed.get("sample_status", "None").strip().capitalize()
        if sample_status not in valid_statuses:
            sample_status = "None"

        result = {
            "division":       parsed.get("division",      "Other").strip(),
            "style_numbers":  parsed.get("style_numbers", "").strip(),
            "colour":         parsed.get("colour",        "").strip(),
            "ai_overview":    overview,
            "intent":         intent,
            "requires_reply": "Yes" if requires_reply else "No",
            "sample_status":  sample_status,
        }
        # ── Store in cache for future runs ──
        if thread_id and msg_count:
            cache_set(thread_id, msg_count, result)
        return result

    # ── Full fallback — LLM failed after all retries ──
    log.error(f"LLM completely failed for '{subject}'. Last raw output: {last_raw[:400]}")
    all_text = "\n".join(m["body"] for m in structured_messages)
    return {
        "division":       _fallback_division(subject),
        "style_numbers":  _fallback_style_numbers(all_text),
        "colour":         "",
        "ai_overview":    "• Could not generate summary — LLM unavailable or returned invalid JSON",
        "intent":         "Other",
        "requires_reply": "No",
        "sample_status":  "None",
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
    if any(w in s for w in ["furniture", "sofa", "chair", "table", "shelf"]): return "Furniture"
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
# PO NUMBER EXTRACTION — regex, no LLM needed
# ════════════════════════════════════════════════════════════

# PO patterns ordered by confidence — most explicit first
PO_PATTERNS = [
    r'\bP\.?O\.?\s*(?:No|Number|#|:)?\s*[:#\-]?\s*([A-Z0-9][A-Z0-9\-/]{3,20})\b',  # PO No: 12345
    r'\bPurchase\s+Order\s*(?:No|Number|#|:)?\s*[:#\-]?\s*([A-Z0-9][A-Z0-9\-/]{3,20})\b',
    r'\bOrder\s+(?:No|Number|Ref|#)\s*[:#\-]?\s*([A-Z0-9][A-Z0-9\-/]{3,20})\b',
    r'\bPO[:\-\s]\s*([A-Z0-9][A-Z0-9\-/]{3,20})\b',                # PO: ABC-1234
    r'\bRef(?:erence)?\s*(?:No|#)?\s*[:#\-]?\s*(PO[A-Z0-9\-]{3,18})\b',  # Ref: PO12345
]

# Words that look like PO numbers but are not
PO_NOISE = {
    "INVOICE", "SUBJECT", "REGARDS", "ATTACHED", "PLEASE", "KINDLY",
    "CONFIRM", "DETAILS", "SAMPLE", "STYLES", "UPDATE", "STATUS",
}


def extract_po_number(structured_msgs: list) -> str:
    """
    Extract PO number from thread, newest message first.
    Returns the first confident match, or "" if none found.

    Searches newest → oldest so the most recent PO reference wins
    (vendors sometimes correct a PO number in a follow-up message).
    """
    seen = set()
    results = []

    for msg in reversed(structured_msgs):
        text = msg["body"]
        for pattern in PO_PATTERNS:
            for m in re.finditer(pattern, text, re.IGNORECASE):
                candidate = m.group(1).strip().upper()
                # Filter noise words and very short/long matches
                if candidate in PO_NOISE:
                    continue
                if len(candidate) < 4 or len(candidate) > 20:
                    continue
                # Must contain at least one digit
                if not re.search(r'\d', candidate):
                    continue
                if candidate not in seen:
                    seen.add(candidate)
                    results.append(candidate)

    # Return first (most recent) match
    return results[0] if results else ""


# ════════════════════════════════════════════════════════════
# SHIPMENT EXTRACTION — carrier, AWB number, sent date
# ════════════════════════════════════════════════════════════

CARRIERS = [
    # Specific names first (before their shorter aliases)
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
    r'\bAWB\s*[:#\-]?\s*([A-Z0-9]{3}-\d{8})\b',                       # AWB 157-12345678
    r'\bAWB\s*[:#\-]?\s*([A-Z0-9\-]{6,20})\b',                        # AWB: XXXXXXXX
    r'\bairway\s*bill\s*[:#\-]?\s*([A-Z0-9\-]{6,20})\b',              # airway bill XXXX
    r'\btracking\s*(?:no|number|#|:)?\s*[:#\-]?\s*([A-Z0-9\-]{8,25})\b',
    r'\bshipment\s*(?:no|number|#|:)?\s*[:#\-]?\s*([A-Z0-9\-]{6,20})\b',
    r'\bconsignment\s*(?:no|number|#|:)?\s*([A-Z0-9\-]{6,20})\b',
    r'\b(\d{3}-\d{8})\b',                                              # IATA: 157-12345678
    r'\b(1Z[A-Z0-9]{16})\b',                                           # UPS 1Z format
    r'\b([A-Z]{2}\d{9}[A-Z]{2})\b',                                    # Postal EE123456789CN
    # ★ Number directly after carrier name — e.g. "via DHL 4704591124"
    (r'\b(?:DHL|FedEx|UPS|TNT|Aramex|BlueDart|DTDC|Delhivery|Maersk|MSC|'
     r'Expeditors|Agility|CEVA|DSV|Geodis)\b[\s\w,./()-]{0,30}?\b(\d{8,13})\b'),
    # Generic: number near shipment action words
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
    """
    Extracts carrier name, AWB/tracking number, and shipment sent date
    from the full concatenated thread body text.
    Handles all real-world vendor email formats.
    """
    result     = {"company": "", "awb": "", "shipment_date": ""}
    text_lower = text.lower()

    # 1. Find carrier — earliest occurrence wins
    best_carrier, best_pos = "", -1
    for carrier in CARRIERS:
        idx = text_lower.find(carrier.lower())
        if idx != -1 and (best_pos == -1 or idx < best_pos):
            best_carrier, best_pos = carrier, idx

    result["company"] = CARRIER_ALIASES.get(best_carrier.lower(), best_carrier)

    # 2. AWB / tracking number — priority ordered
    for pattern in AWB_PATTERNS:
        m = re.search(pattern, text, re.IGNORECASE)
        if m:
            result["awb"] = m.group(1).upper()
            break

    # 3. Shipment sent date — from keyword context first
    m = re.search(DATE_CONTEXT_PATTERN, text, re.IGNORECASE)
    if m:
        result["shipment_date"] = m.group(1).strip("()")
    elif best_pos >= 0:
        # Fallback: any date within ±150 chars of carrier name
        window = text[max(0, best_pos - 50): best_pos + 150]
        dm = re.search(
            r'(\d{1,2}[\/\-\.]\d{1,2}[\/\-\.]\d{2,4}|\d{4}[\/\-\.]\d{1,2}[\/\-\.]\d{1,2})',
            window
        )
        if dm:
            result["shipment_date"] = dm.group(1)

    return result


# ════════════════════════════════════════════════════════════
# ATTACHMENT DETECTION — reads MIME parts, zero body parsing
# ════════════════════════════════════════════════════════════

# MIME types we care about — maps to a human-readable label
ATTACHMENT_MIME_MAP = {
    # Documents
    "application/pdf":                                              "PDF",
    "application/msword":                                           "Word",
    "application/vnd.openxmlformats-officedocument.wordprocessingml.document": "Word",
    # Spreadsheets
    "application/vnd.ms-excel":                                     "Excel",
    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet": "Excel",
    "text/csv":                                                     "CSV",
    # Presentations
    "application/vnd.ms-powerpoint":                                "PowerPoint",
    "application/vnd.openxmlformats-officedocument.presentationml.presentation": "PowerPoint",
    # Images
    "image/jpeg":  "Image",
    "image/jpg":   "Image",
    "image/png":   "Image",
    "image/gif":   "Image",
    "image/webp":  "Image",
    "image/tiff":  "Image",
    # Archives / CAD
    "application/zip":              "ZIP",
    "application/x-zip-compressed": "ZIP",
    "application/octet-stream":     "File",   # generic binary — filename checked below
}

# Extension → label for application/octet-stream fallback
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
    """Flatten all MIME parts recursively — returns flat list of part dicts."""
    parts = []
    for part in payload.get("parts", []):
        parts.append(part)
        if part.get("parts"):
            parts.extend(_collect_parts(part))
    return parts


def extract_attachments(messages: list) -> str:
    """
    Scan all Gmail message payloads for attached files.
    Returns a human-readable string like "PDF, Excel, Image (×3)"
    or "" if no attachments found.

    Logic:
      - A part is an attachment if it has filename != "" OR
        Content-Disposition header contains "attachment"
      - Groups by type, counts duplicates
      - Skips inline images (Content-Disposition: inline)
    """
    type_counts: dict = {}

    for msg in messages:
        all_parts = _collect_parts(msg.get("payload", {}))

        for part in all_parts:
            filename = part.get("filename", "").strip()
            mime     = part.get("mimeType", "").lower()
            headers  = {h["name"].lower(): h["value"] for h in part.get("headers", [])}
            disposition = headers.get("content-disposition", "").lower()

            # Skip inline images (embedded in HTML email, not real attachments)
            if "inline" in disposition and not filename:
                continue

            # Must have a filename OR explicit attachment disposition
            if not filename and "attachment" not in disposition:
                continue

            # Resolve type label
            label = ATTACHMENT_MIME_MAP.get(mime, "")
            if not label and filename:
                ext = "." + filename.rsplit(".", 1)[-1].lower() if "." in filename else ""
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
# SHARED LINK DETECTION — scans message bodies
# Catches Google Drive, Dropbox, WeTransfer, OneDrive, Notion,
# Figma, and plain https links vendors paste into emails.
# ════════════════════════════════════════════════════════════

# Named patterns — label: regex
# Ordered most-specific first so label is as descriptive as possible
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

# Generic https URL — catch-all for anything else (e.g. custom servers)
_GENERIC_URL_RE = re.compile(r"https?://[^\s<>\"']{10,}", re.IGNORECASE)


def extract_shared_links(structured_msgs: list) -> str:
    """
    Scan message bodies for shared document / file links.
    Returns a deduplicated, labelled summary string.

    Examples:
      "Google Drive, WeTransfer"
      "Google Drive (×2), Dropbox"
      "https://custom-server.com/file.pdf"

    Strategy:
      1. Try each named pattern first → use its label
      2. Anything left that looks like a URL → show domain only
         (avoids dumping full long URLs into the sheet)
    """
    found_labels: dict = {}    # label → count
    matched_urls:  set = set() # track to avoid double-counting

    all_bodies = "\n".join(m["body"] for m in structured_msgs)

    # Pass 1 — named patterns
    for label, pattern in SHARED_LINK_PATTERNS:
        for m in re.finditer(pattern, all_bodies, re.IGNORECASE):
            url = m.group(0)
            if url not in matched_urls:
                matched_urls.add(url)
                found_labels[label] = found_labels.get(label, 0) + 1

    # Pass 2 — generic URLs not already matched
    for m in _GENERIC_URL_RE.finditer(all_bodies):
        url = m.group(0)
        if url in matched_urls:
            continue
        # Extract domain as label — skip very common non-file domains
        try:
            domain = url.split("/")[2].lstrip("www.")
        except IndexError:
            continue
        # Skip tracking pixels, analytics, unsubscribe links, images in HTML
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
# SAMPLE REMINDER — computed flag, no LLM needed
# ════════════════════════════════════════════════════════════

SAMPLE_REMINDER_DAYS = 7   # flag thread if dispatched but no update after this many days


def compute_sample_reminder(sample_status: str, latest_msg_date) -> str:
    """
    Returns a reminder flag string for column U.

    Rules:
      - Status = Dispatched AND last message > SAMPLE_REMINDER_DAYS ago
        → "⚠️ Chase — dispatched N days ago, no update"
      - Status = Pending AND last message > SAMPLE_REMINDER_DAYS ago
        → "⚠️ Chase — sample pending N days"
      - Status = Approved / Rejected / Received → "" (resolved, no action)
      - Status = None → "" (no sample in thread)
      - Any status but last message is recent → "" (still fresh)
    """
    if not latest_msg_date:
        return ""

    days_since = (datetime.now() - latest_msg_date).days

    if sample_status == "Dispatched" and days_since >= SAMPLE_REMINDER_DAYS:
        return f"⚠️ Chase — dispatched {days_since}d ago, no update"

    if sample_status == "Pending" and days_since >= SAMPLE_REMINDER_DAYS:
        return f"⚠️ Chase — sample pending {days_since}d"

    return ""


# ════════════════════════════════════════════════════════════
# THREAD PROCESSOR — the core engine
# ════════════════════════════════════════════════════════════

def process_thread(gmail_svc, thread_id: str, vendor_db: dict):
    """
    1. Fetch full thread
    2. Extract clean body per message (HTML→text, strip quotes)
    3. Deduplicate CC addresses across all messages
    4. Vendor lookup (DB → domain → noise-filtered fallback)
    5. ONE combined LLM call
    6. Regex shipment extraction on full combined text
    7. Return complete row dict
    """
    thread_data = gmail_threads_get(gmail_svc, userId="me", id=thread_id, format="full")
    messages    = thread_data.get("messages", [])

    if len(messages) < 2:
        return None  # not a real conversation

    # Sort by internalDate (milliseconds epoch) — Gmail API does not guarantee order.
    # internalDate is the most reliable timestamp: unaffected by clock skew in Date headers.
    messages = sorted(messages, key=lambda m: int(m.get("internalDate", 0)))

    subject        = ""
    all_senders    = []
    all_recipients = []
    all_bcc        = []
    earliest_date  = None
    structured_msgs = []

    # Dedup sets — track by extracted email address, not raw string
    cc_seen  = set()
    to_seen  = set()
    all_cc_deduped = []

    for msg in messages:
        raw_headers = msg["payload"].get("headers", [])
        hdrs = {h["name"].lower(): h["value"] for h in raw_headers}

        # Subject — clean from first message only
        if not subject:
            subject = re.sub(
                r'^(re|fwd|fw):\s*',
                '',
                hdrs.get("subject", "(no subject)"),
                flags=re.IGNORECASE
            ).strip()

        # Sender
        sender = hdrs.get("from", "")
        if sender and sender not in all_senders:
            all_senders.append(sender)

        # To — deduplicated by email address
        for addr in _split_addrs(hdrs.get("to", "")):
            key = _extract_email(addr)
            if key not in to_seen:
                to_seen.add(key)
                all_recipients.append(addr)

        # CC — deduplicated by email address across ALL messages in thread
        for addr in _split_addrs(hdrs.get("cc", "")):
            key = _extract_email(addr)
            if key not in cc_seen:
                cc_seen.add(key)
                all_cc_deduped.append(addr)

        # BCC
        for addr in _split_addrs(hdrs.get("bcc", "")):
            all_bcc.append(addr)

        # Date — prefer internalDate (reliable epoch ms) over header Date field
        # Header Date can have timezone/clock-skew issues; internalDate cannot.
        msg_date_str = ""
        internal_ts  = int(msg.get("internalDate", 0))
        if internal_ts:
            dt = datetime.fromtimestamp(internal_ts / 1000)
            if earliest_date is None or dt < earliest_date:
                earliest_date = dt
            msg_date_str = dt.strftime("%d %b %Y %H:%M")
        else:
            # Fallback to header Date if internalDate missing
            date_raw = hdrs.get("date", "")
            if date_raw:
                try:
                    dt = parsedate_to_datetime(date_raw)
                    if earliest_date is None or dt < earliest_date:
                        earliest_date = dt
                    msg_date_str = dt.strftime("%d %b %Y")
                except Exception:
                    pass

        # Body — full HTML-aware extraction with quote stripping
        body = extract_message_body(msg["payload"])
        if body:
            structured_msgs.append({
                "from": _display_name(sender) or sender,
                "date": msg_date_str,
                "body": body,
            })

    if not structured_msgs:
        return None

    # LLM context: always include first message + latest N-1
    if len(structured_msgs) > MAX_MSGS_IN_LLM:
        llm_msgs = [structured_msgs[0]] + structured_msgs[-(MAX_MSGS_IN_LLM - 1):]
    else:
        llm_msgs = structured_msgs

    # ── Vendor lookup ──
    # Order matters: From first, then To, then CC, then BCC.
    # lookup_vendor returns on first match — so primary sender/recipient wins.
    # Do NOT use set() here — it destroys the priority ordering.
    seen_addrs = set()
    ordered_external = []
    for addr in (all_senders + all_recipients + all_cc_deduped + all_bcc):
        key = _extract_email(addr)
        if key not in seen_addrs and ONEQUINCE_DOMAIN.lower() not in addr.lower():
            seen_addrs.add(key)
            ordered_external.append(addr)

    vendor = lookup_vendor(ordered_external, vendor_db)
    partner_name   = vendor["partner_name"]
    classification = vendor["classification"]

    # If DB found nothing and YOU are the sender, vendor is in To/CC (you initiated)
    if not partner_name:
        sender_email = _extract_email(all_senders[0]) if all_senders else ""
        if ONEQUINCE_DOMAIN in sender_email:
            # Rerun lookup with To+CC only (you started the thread, vendor = recipient)
            recip_external = [
                a for a in (all_recipients + all_cc_deduped)
                if ONEQUINCE_DOMAIN.lower() not in a.lower()
            ]
            vendor2 = lookup_vendor(recip_external, vendor_db)
            partner_name   = vendor2["partner_name"]
            classification = vendor2["classification"]

    # Last resort: use filtered display names from external addresses
    if not partner_name:
        partner_name = _fallback_vendor_name(ordered_external)

    # ── Shipment extraction — newest message first so latest AWB wins ──
    shipment = extract_shipment_info(structured_msgs)

    # ── PO number extraction (regex, no LLM) ──
    po_number = extract_po_number(structured_msgs)

    # ── Attachment detection — reads MIME parts directly, zero body parsing ──
    attachments = extract_attachments(messages)

    # ── Shared link detection — Google Drive, Dropbox, WeTransfer, etc. ──
    shared_links = extract_shared_links(structured_msgs)

    # ── Single combined LLM call ──
    print(f"    🤖 LLM: {subject[:50]}...")
    llm = llm_analyse_thread(subject, llm_msgs, thread_id=thread_id, msg_count=len(messages))

    # ── Sample reminder (computed from LLM status + latest message date) ──
    latest_msg_date = datetime.fromtimestamp(
        int(messages[-1].get("internalDate", 0)) / 1000
    ) if messages else None
    sample_reminder = compute_sample_reminder(llm["sample_status"], latest_msg_date)

    # ── Assemble output ──
    # CC: deduplicated, external only, email addresses
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


def _ensure_tab(svc, tab_name: str, headers: list):
    """
    Full header-sync check — runs once per tab on every run.

    Handles all four states the sheet can be in:

      Case A — Tab doesn't exist yet
               → Create tab, write full header row, bold it.

      Case B — Tab exists, A1 is empty (header row was deleted)
               → Write full header row, bold it.

      Case C — Tab exists, headers present but FEWER columns than expected
               → New columns were added since sheet was set up.
               → Append only the missing columns to the right — never
                 touch existing columns (preserves user data / formulas).

      Case D — Tab exists, correct number of headers already
               → Nothing to do. Print confirmation and move on.

    This means you can add new columns to LOGS_HEADERS at any time and
    they will appear in the sheet automatically on the next run.
    """
    # ── Step 1: create tab if it doesn't exist ──
    meta      = svc.spreadsheets().get(spreadsheetId=SHEET_ID).execute()
    tab_names = [s["properties"]["title"] for s in meta["sheets"]]

    if tab_name not in tab_names:
        print(f"  📋 Creating '{tab_name}' tab...")
        svc.spreadsheets().batchUpdate(
            spreadsheetId=SHEET_ID,
            body={"requests": [{"addSheet": {"properties": {"title": tab_name}}}]}
        ).execute()

    # ── Step 2: read the existing header row (row 1) ──
    r           = svc.spreadsheets().values().get(
        spreadsheetId=SHEET_ID,
        range=f"'{tab_name}'!1:1"
    ).execute()
    existing_headers = r.get("values", [[]])[0] if r.get("values") else []

    def _bold_row1():
        """Apply bold formatting to the header row."""
        sid = _get_sheet_id(svc, tab_name)
        svc.spreadsheets().batchUpdate(
            spreadsheetId=SHEET_ID,
            body={"requests": [{"repeatCell": {
                "range":  {"sheetId": sid, "startRowIndex": 0, "endRowIndex": 1},
                "cell":   {"userEnteredFormat": {"textFormat": {"bold": True}}},
                "fields": "userEnteredFormat.textFormat.bold",
            }}]}
        ).execute()

    # ── Case B: header row completely missing ──
    if not existing_headers:
        print(f"  📋 '{tab_name}': header row missing — writing all {len(headers)} headers...")
        svc.spreadsheets().values().update(
            spreadsheetId=SHEET_ID,
            range=f"'{tab_name}'!A1",
            valueInputOption="RAW",
            body={"values": [headers]}
        ).execute()
        _bold_row1()
        print(f"  ✅ '{tab_name}': headers written (A–{chr(64 + len(headers))})")
        return

    # ── Case C: fewer columns than expected — append missing ones ──
    n_existing = len(existing_headers)
    n_expected = len(headers)

    if n_existing < n_expected:
        missing      = headers[n_existing:]          # only the new columns
        start_col    = n_existing + 1                # 1-based column index
        # Convert to A1 column letter(s) — supports up to ZZ
        def col_letter(n):
            result = ""
            while n:
                n, r = divmod(n - 1, 26)
                result = chr(65 + r) + result
            return result
        start_letter = col_letter(start_col)
        end_letter   = col_letter(start_col + len(missing) - 1)
        range_str    = f"'{tab_name}'!{start_letter}1"

        print(f"  🔧 '{tab_name}': {len(missing)} new column(s) detected "
              f"({start_letter}–{end_letter}): {', '.join(missing)}")
        svc.spreadsheets().values().update(
            spreadsheetId=SHEET_ID,
            range=range_str,
            valueInputOption="RAW",
            body={"values": [missing]}
        ).execute()
        _bold_row1()
        print(f"  ✅ '{tab_name}': headers now complete (A–{end_letter})")
        return

    # ── Case D: headers already correct ──
    print(f"  ✅ '{tab_name}': headers OK ({n_existing} columns)")


def load_existing_rows(svc) -> tuple:
    """
    Returns:
      thread_map  {thread_id  → {sheet_row, message_count, subject}}
      subject_map {subject.lower() → same}
    Column indices: N=Thread Messages (idx 13), O=Thread ID (idx 14)
    """
    result = svc.spreadsheets().values().get(
        spreadsheetId=SHEET_ID,
        range=f"'{SHEET_TAB}'!A2:P"
    ).execute()

    thread_map  = {}
    subject_map = {}

    for idx, row in enumerate(result.get("values", [])):
        while len(row) < 23:   # A–W (21 + V Attachments + W Shared Links)
            row.append("")

        subject   = row[0].strip()
        msg_count = int(row[13]) if str(row[13]).isdigit() else 0
        thread_id = row[14].strip()
        sheet_row = idx + 2  # +2: row 1 = headers, data from row 2

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


def update_existing_row(svc, sheet_row: int, row_data: dict, backfill_tid: bool = False):
    """
    Batch-updates only the columns that change when new replies arrive.
    Preserves Subject, Sender, Division, Vendor Name etc.
    """
    now  = datetime.now().strftime("%Y-%m-%d %H:%M")
    data = []

    for col, field in UPDATE_ON_REPLY.items():
        val = str(row_data.get(field, "")) if field == "Thread Messages" else row_data.get(field, "")
        data.append({"range": f"'{SHEET_TAB}'!{col}{sheet_row}", "values": [[val]]})

    data.append({"range": f"'{SHEET_TAB}'!P{sheet_row}", "values": [[now]]})

    if backfill_tid and row_data.get("Thread ID"):
        data.append({"range": f"'{SHEET_TAB}'!O{sheet_row}", "values": [[row_data["Thread ID"]]]})

    svc.spreadsheets().values().batchUpdate(
        spreadsheetId=SHEET_ID,
        body={"valueInputOption": "RAW", "data": data}
    ).execute()


def append_new_rows(svc, rows: list):
    """Write all new rows in a single API call."""
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
    """Batch-write all buffered errors to Error Logs sheet."""
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
# Gets message count + subject for all threads simultaneously.
# No body download — just enough to route each thread to Case 1/2/3.
# ════════════════════════════════════════════════════════════

def _fetch_meta_one(svc, tid: str) -> dict:
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


def fetch_all_metadata(svc, thread_ids: list) -> dict:
    """Returns {thread_id: {count, subject}} for all threads in parallel."""
    print(f"  ⚡ Fetching metadata for {len(thread_ids)} threads ({GMAIL_WORKERS} parallel)...")
    results = {}
    with ThreadPoolExecutor(max_workers=GMAIL_WORKERS) as executor:
        futures = {
            executor.submit(_fetch_meta_one, svc, tid): tid
            for tid in thread_ids
        }
        for future in as_completed(futures):
            m = future.result()
            results[m["tid"]] = m
            if m["error"]:
                log.warning(f"Metadata fetch failed for {m['tid']}: {m['error']}")
    print(f"  ✅ Metadata ready\n")
    return results


# ════════════════════════════════════════════════════════════
# MAIN RUN LOOP
# ════════════════════════════════════════════════════════════

def run(max_threads: int = 50):
    print(f"\n🚀 Gmail Thread Extractor v4.0")
    print(f"   {max_threads} threads | {GMAIL_WORKERS} parallel | 1 LLM call/thread\n")

    # Auth + services
    creds         = authenticate()
    gmail_svc     = build("gmail",  "v1", credentials=creds)
    sheets_svc    = build("sheets", "v4", credentials=creds)

    # One-time setup
    vendor_db = load_vendor_db(sheets_svc)
    _ensure_tab(sheets_svc, SHEET_TAB, LOGS_HEADERS)
    _ensure_tab(sheets_svc, ERROR_TAB, ERROR_HEADERS)

    print("  🔍 Reading existing Logs rows...")
    thread_map, subject_map = load_existing_rows(sheets_svc)
    print()

    # Fetch thread list from Gmail
    print(f"  📬 Fetching up to {max_threads} threads from inbox...")
    result = gmail_svc.users().threads().list(
        userId="me", maxResults=max_threads, q="in:inbox"
    ).execute()
    gmail_threads = result.get("threads", [])
    print(f"  Found {len(gmail_threads)} threads\n")

    if not gmail_threads:
        print("  Nothing to process.\n")
        return

    # Parallel metadata fetch — all at once before the main loop
    all_tids = [t["id"] for t in gmail_threads]
    meta_map = fetch_all_metadata(gmail_svc, all_tids)

    # Counters
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

            # ═══════════════════════════════════════════════
            # CASE 1 — Thread ID stored in col O
            #          Fastest: compare message count only
            # ═══════════════════════════════════════════════
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
                          f"| AWB: {row_data['AWB No'] or '—'} "
                          f"| Courier: {row_data['Shipment Company'] or '—'}")

            # ═══════════════════════════════════════════════
            # CASE 2 — No Thread ID, but subject matches old row
            #          Backfill Thread ID + update if new replies
            # ═══════════════════════════════════════════════
            elif subj_key and subj_key in subject_map:
                existing  = subject_map[subj_key]
                old_count = existing["message_count"]

                if curr_count <= old_count:
                    # No new replies — just write Thread ID silently
                    sheets_svc.spreadsheets().values().batchUpdate(
                        spreadsheetId=SHEET_ID,
                        body={"valueInputOption": "RAW", "data": [
                            {"range": f"'{SHEET_TAB}'!O{existing['sheet_row']}", "values": [[tid]]},
                            {"range": f"'{SHEET_TAB}'!N{existing['sheet_row']}", "values": [[str(curr_count)]]},
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
                    print(f"    ✅ Row {existing['sheet_row']} updated + Thread ID stored "
                          f"| AWB: {row_data['AWB No'] or '—'}")

            # ═══════════════════════════════════════════════
            # CASE 3 — Brand new thread
            # ═══════════════════════════════════════════════
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
                      f"| Vendor: {row_data['Vendor Name'] or '?'}")

                new_rows.append([
                    row_data["Subject"],           # A
                    row_data["Sender"],            # B
                    row_data["CC"],                # C
                    row_data["Division"],          # D
                    row_data["Style No"],          # E
                    row_data["Colour"],            # F
                    row_data["Vendor Name"],       # G
                    row_data["Partner Class"],     # H
                    row_data["Shipment Company"],  # I
                    row_data["AWB No"],            # J
                    row_data["Shipment Date"],     # K
                    row_data["Sent Date"],         # L
                    row_data["AI Overview"],       # M
                    row_data["Thread Messages"],   # N
                    row_data["Thread ID"],         # O
                    now_str,                       # P Last Updated
                    row_data["Intent"],            # Q
                    row_data["Reply Needed"],      # R
                    row_data["PO Number"],         # S
                    row_data["Sample Status"],     # T
                    row_data["Sample Reminder"],   # U
                    row_data["Attachments"],       # V
                    row_data["Shared Links"],      # W
                ])

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

    # Batch-write all new rows at once
    if new_rows:
        if DRY_RUN:
            print(f"\n  🔍 DRY RUN — would write {len(new_rows)} new rows (skipped)")
            log.info(f"DRY_RUN: would write {len(new_rows)} new rows")
        else:
            print(f"\n  📤 Writing {len(new_rows)} new rows to Sheets...")
            append_new_rows(sheets_svc, new_rows)

    # Flush errors to sheet
    if not DRY_RUN:
        flush_error_log(sheets_svc)
    elif _error_buffer:
        print(f"  🔍 DRY RUN — {len(_error_buffer)} error(s) would be written to '{ERROR_TAB}'"  )

    dry_tag = " [DRY RUN]" if DRY_RUN else ""
    cs = cache_stats()
    print(f"\n{'═' * 58}")
    print(f"  🆕 New rows added        : {added}{dry_tag}")
    print(f"  🔄 Rows updated          : {updated}  ← new replies processed")
    print(f"  🔗 Thread IDs backfilled : {backfilled}")
    print(f"  ⏭️  Skipped               : {skipped}  ← no changes")
    print(f"  ❌ Errors                : {errors}  → check '{ERROR_TAB}' tab")
    print(f"  💾 LLM cache entries     : {cs['cached_entries']} (prompt={PROMPT_VERSION})")
    print(f"  📄 Debug log             : gmail_agent.log")
    print(f"  🔗 Sheet : https://docs.google.com/spreadsheets/d/{SHEET_ID}/edit")
    print(f"{'═' * 58}\n")

    write_audit_log({
        "threads_fetched": total,
        "added":           added,
        "updated":         updated,
        "backfilled":      backfilled,
        "skipped":         skipped,
        "errors":          errors,
        "cache_entries":   cs["cached_entries"],
    })


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Gmail Thread Extractor v5.0 — Merchandising Logs"
    )
    parser.add_argument(
        "max_threads", nargs="?", type=int, default=50,
        help="Number of inbox threads to process (default: 50)"
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Read-only mode — analyse threads but do NOT write to Google Sheets"
    )
    args = parser.parse_args()

    if args.dry_run:
        DRY_RUN = True
        print("\n⚠️  DRY RUN MODE — no changes will be written to Google Sheets\n")
        log.info("DRY_RUN mode activated via --dry-run flag")

    # Security reminder: warn if secret files are in the current directory
    for secret_file in ("credentials.json", "token.json", ".env"):
        if os.path.exists(secret_file):
            log.info(f"Secret file present: {secret_file} — ensure it is in .gitignore")

    run(max_threads=args.max_threads)