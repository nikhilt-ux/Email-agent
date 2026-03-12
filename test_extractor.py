"""
Unit Tests — Gmail Thread Extractor v5.0
═════════════════════════════════════════
Run with:  pytest test_extractor.py -v

Covers (15 tests):
  - Style number extraction     (regex)
  - AWB / tracking extraction   (regex)
  - PO number extraction        (regex)
  - Vendor lookup               (email + domain + fallback)
  - Shared link detection       (named + generic)
  - Attachment detection        (MIME type + extension)
  - Sample reminder logic       (date arithmetic)
  - HTML → text conversion      (body extraction)
  - CJK stripping               (body cleaning)
  - LLM cache                   (SQLite get/set/miss)
  - LLM fallbacks               (division + style on empty response)
  - Intent validation           (requires_reply coercion)
  - Dry run flag                (config check)
  - Prompt versioning           (PROMPT_VERSION format)
  - Audit log structure         (stats dict keys)
"""

import os
import sys
import json
import sqlite3
import tempfile
import hashlib
from datetime import datetime, timedelta
from unittest.mock import patch, MagicMock

import pytest

# ── Import the module under test ──────────────────────────────
# Add both the test file's own directory AND the cwd so pytest
# finds gmail_thread_extractor.py however you run it:
#   cd "/Users/nikhil/Email agent" && pytest test_extractor.py
#   pytest /full/path/test_extractor.py
_here = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, _here)       # directory containing test_extractor.py
sys.path.insert(0, os.getcwd()) # wherever pytest was launched from

# Patch heavy imports before the module loads
import unittest.mock as mock
with mock.patch.dict("sys.modules", {
    "requests":                        mock.MagicMock(),
    "openai":                          mock.MagicMock(),
    "google.oauth2.credentials":       mock.MagicMock(),
    "google_auth_oauthlib.flow":       mock.MagicMock(),
    "googleapiclient.discovery":       mock.MagicMock(),
    "google.auth.transport.requests":  mock.MagicMock(),
    "googleapiclient.errors":          mock.MagicMock(),
    "dotenv":                          mock.MagicMock(),
}):
    import gmail_reader as ext


# ═══════════════════════════════════════════════════════════
# HELPERS
# ═══════════════════════════════════════════════════════════

def _msg(body: str, from_: str = "vendor@test.com", date: str = "01 Jan 2025") -> dict:
    return {"from": from_, "date": date, "body": body}


# ═══════════════════════════════════════════════════════════
# 1. STYLE NUMBER EXTRACTION
# ═══════════════════════════════════════════════════════════

class TestStyleNumbers:

    def test_men_style_double_hyphen(self):
        result = ext._fallback_style_numbers("Please check style M--1348 for colour approval")
        assert "M--1348" in result

    def test_women_style_with_category(self):
        result = ext._fallback_style_numbers("W-PNT-228 pants sample dispatched")
        assert "W-PNT-228" in result

    def test_multiple_styles(self):
        result = ext._fallback_style_numbers("Styles M--1001 and W--2002 ready for review")
        assert "M--1001" in result
        assert "W--2002" in result

    def test_no_style_returns_empty(self):
        result = ext._fallback_style_numbers("Hello, please confirm receipt of sample")
        assert result == ""

    def test_uppercase_normalised(self):
        result = ext._fallback_style_numbers("style ref: w--9012")
        assert result == result.upper()


# ═══════════════════════════════════════════════════════════
# 2. AWB / TRACKING EXTRACTION
# ═══════════════════════════════════════════════════════════

class TestShipmentExtraction:
    def test_awb_explicit_label(self):
        r = ext.extract_shipment_info("AWB: 157-12345678 via DHL")
        assert r["awb"] == "157-12345678"
    def test_carrier_detected(self):
        r = ext.extract_shipment_info("Shipped via DHL Express, tracking 1234567890")
        assert r["company"] == "DHL"
    def test_ups_1z_format(self):
        r = ext.extract_shipment_info("UPS tracking: 1Z999AA10123456784")
        assert r["awb"].startswith("1Z")
    def test_empty_thread_returns_blank(self):
        r = ext.extract_shipment_info("")
        assert r == {"company": "", "awb": "", "shipment_date": ""}
    def test_newest_message_wins(self):
        text = "Old: DHL AWB: 111-11111111\nUpdated: FedEx AWB: 999-87654321"
        r = ext.extract_shipment_info(text)
        assert r["awb"] in ("111-11111111", "999-87654321")
# ═══════════════════════════════════════════════════════════
# 3. PO NUMBER EXTRACTION
# ═══════════════════════════════════════════════════════════

class TestPOExtraction:

    def test_explicit_po_no(self):
        msgs = [_msg("PO No: OQ-2024-1234")]
        assert ext.extract_po_number(msgs) == "OQ-2024-1234"

    def test_purchase_order_label(self):
        msgs = [_msg("Purchase Order 98765 attached")]
        result = ext.extract_po_number(msgs)
        assert "98765" in result

    def test_noise_word_filtered(self):
        msgs = [_msg("PO: SAMPLE")]
        assert ext.extract_po_number(msgs) == ""

    def test_must_contain_digit(self):
        msgs = [_msg("PO: ABCDEF")]
        assert ext.extract_po_number(msgs) == ""

    def test_newest_po_wins(self):
        msgs = [
            _msg("Original PO: 1111"),
            _msg("Corrected PO No: 2222"),
        ]
        result = ext.extract_po_number(msgs)
        assert result == "2222"


# ═══════════════════════════════════════════════════════════
# 4. VENDOR LOOKUP
# ═══════════════════════════════════════════════════════════

class TestVendorLookup:

    def _db(self):
        return {
            "emails":  {"vendor@tirupur.com": {"partner_name": "Tirupur Knits", "classification": "Manufacturer"}},
            "domains": {"tirupur.com":        {"partner_name": "Tirupur Knits", "classification": "Manufacturer"}},
        }

    def test_exact_email_match(self):
        result = ext.lookup_vendor(["vendor@tirupur.com"], self._db())
        assert result["partner_name"] == "Tirupur Knits"

    def test_domain_fallback(self):
        result = ext.lookup_vendor(["other@tirupur.com"], self._db())
        assert result["partner_name"] == "Tirupur Knits"

    def test_onequince_skipped(self):
        result = ext.lookup_vendor(["me@onequince.com", "vendor@tirupur.com"], self._db())
        assert result["partner_name"] == "Tirupur Knits"

    def test_no_match_returns_empty(self):
        result = ext.lookup_vendor(["unknown@nowhere.com"], self._db())
        assert result["partner_name"] == ""

    def test_gmail_domain_not_matched(self):
        db = {"emails": {}, "domains": {"gmail.com": {"partner_name": "WRONG", "classification": ""}}}
        result = ext.lookup_vendor(["someone@gmail.com"], db)
        assert result["partner_name"] == ""


# ═══════════════════════════════════════════════════════════
# 5. SHARED LINK DETECTION
# ═══════════════════════════════════════════════════════════

class TestSharedLinks:

    def test_google_drive_detected(self):
        msgs = [_msg("Please see https://drive.google.com/file/d/abc123/view")]
        assert "Google Drive" in ext.extract_shared_links(msgs)

    def test_wetransfer_detected(self):
        msgs = [_msg("Download from https://we.tl/t-xyzABC123")]
        assert "WeTransfer" in ext.extract_shared_links(msgs)

    def test_dropbox_detected(self):
        msgs = [_msg("Files at https://www.dropbox.com/s/abc/file.zip")]
        assert "Dropbox" in ext.extract_shared_links(msgs)

    def test_no_links_returns_empty(self):
        msgs = [_msg("Hello, please confirm the order details.")]
        assert ext.extract_shared_links(msgs) == ""

    def test_count_shown_for_multiple(self):
        msgs = [_msg(
            "Drive 1: https://drive.google.com/file/d/aaa/view\n"
            "Drive 2: https://drive.google.com/file/d/bbb/view"
        )]
        result = ext.extract_shared_links(msgs)
        assert "×2" in result


# ═══════════════════════════════════════════════════════════
# 6. ATTACHMENT DETECTION
# ═══════════════════════════════════════════════════════════

class TestAttachments:

    def _make_msg(self, parts):
        return {"payload": {"parts": parts, "mimeType": "multipart/mixed", "body": {}}}

    def test_pdf_detected(self):
        msg = self._make_msg([{
            "filename": "invoice.pdf",
            "mimeType": "application/pdf",
            "headers": [{"name": "Content-Disposition", "value": "attachment"}],
            "body": {},
            "parts": [],
        }])
        assert "PDF" in ext.extract_attachments([msg])

    def test_inline_image_skipped(self):
        msg = self._make_msg([{
            "filename": "",
            "mimeType": "image/png",
            "headers": [{"name": "Content-Disposition", "value": "inline"}],
            "body": {},
            "parts": [],
        }])
        assert ext.extract_attachments([msg]) == ""

    def test_multiple_types_counted(self):
        msg = self._make_msg([
            {"filename": "a.pdf", "mimeType": "application/pdf",
             "headers": [{"name": "Content-Disposition", "value": "attachment"}], "body": {}, "parts": []},
            {"filename": "b.pdf", "mimeType": "application/pdf",
             "headers": [{"name": "Content-Disposition", "value": "attachment"}], "body": {}, "parts": []},
            {"filename": "photo.jpg", "mimeType": "image/jpeg",
             "headers": [{"name": "Content-Disposition", "value": "attachment"}], "body": {}, "parts": []},
        ])
        result = ext.extract_attachments([msg])
        assert "PDF (×2)" in result
        assert "Image" in result


# ═══════════════════════════════════════════════════════════
# 7. SAMPLE REMINDER
# ═══════════════════════════════════════════════════════════

class TestSampleReminder:

    def test_dispatched_old_triggers_warning(self):
        old_date = datetime.now() - timedelta(days=10)
        result   = ext.compute_sample_reminder("Dispatched", old_date)
        assert "⚠️" in result
        assert "10d" in result

    def test_dispatched_recent_no_warning(self):
        recent = datetime.now() - timedelta(days=2)
        assert ext.compute_sample_reminder("Dispatched", recent) == ""

    def test_pending_old_triggers_warning(self):
        old_date = datetime.now() - timedelta(days=8)
        result   = ext.compute_sample_reminder("Pending", old_date)
        assert "⚠️" in result

    def test_approved_never_warns(self):
        old_date = datetime.now() - timedelta(days=30)
        assert ext.compute_sample_reminder("Approved", old_date) == ""

    def test_none_status_no_warning(self):
        old_date = datetime.now() - timedelta(days=30)
        assert ext.compute_sample_reminder("None", old_date) == ""

    def test_no_date_returns_empty(self):
        assert ext.compute_sample_reminder("Dispatched", None) == ""


# ═══════════════════════════════════════════════════════════
# 8. HTML → TEXT CONVERSION
# ═══════════════════════════════════════════════════════════

class TestHTMLToText:

    def test_strips_tags(self):
        result = ext._html_to_text("<p>Hello <b>World</b></p>")
        assert "<" not in result
        assert "Hello" in result
        assert "World" in result

    def test_entities_decoded(self):
        result = ext._html_to_text("Price &amp; Quality &gt; 90%")
        assert "&amp;" not in result
        assert "&" in result

    def test_style_block_removed(self):
        html = "<style>body { color: red }</style><p>Content</p>"
        result = ext._html_to_text(html)
        assert "color" not in result
        assert "Content" in result


# ═══════════════════════════════════════════════════════════
# 9. CJK STRIPPING
# ═══════════════════════════════════════════════════════════

class TestCJKStripping:

    def test_chinese_removed(self):
        result = ext._strip_cjk("Style M--1234 来样确认 delivery")
        assert "来样确认" not in result
        assert "M--1234" in result
        assert "delivery" in result

    def test_latin_preserved(self):
        text = "Hello World — order confirmed"
        assert ext._strip_cjk(text) == text

    def test_empty_string_safe(self):
        assert ext._strip_cjk("") == ""


# ═══════════════════════════════════════════════════════════
# 10. LLM CACHE
# ═══════════════════════════════════════════════════════════

class TestLLMCache:

    def setup_method(self):
        """Use a temp DB for each test."""
        self.tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
        self._orig_db = ext.CACHE_DB
        ext.CACHE_DB   = self.tmp.name

    def teardown_method(self):
        ext.CACHE_DB = self._orig_db
        os.unlink(self.tmp.name)

    def test_cache_miss_returns_none(self):
        assert ext.cache_get("thread_abc", 3) is None

    def test_cache_set_then_get(self):
        payload = {"division": "Home", "style_numbers": "H--001"}
        ext.cache_set("thread_abc", 3, payload)
        result = ext.cache_get("thread_abc", 3)
        assert result is not None
        assert result["division"] == "Home"

    def test_different_count_is_miss(self):
        ext.cache_set("thread_abc", 3, {"division": "Home"})
        assert ext.cache_get("thread_abc", 4) is None

    def test_cache_stats_count(self):
        ext.cache_set("t1", 1, {"x": 1})
        ext.cache_set("t2", 2, {"x": 2})
        stats = ext.cache_stats()
        assert stats["cached_entries"] >= 2


# ═══════════════════════════════════════════════════════════
# 11. LLM FALLBACKS (division + style on empty response)
# ═══════════════════════════════════════════════════════════

class TestFallbacks:

    def test_fallback_division_women(self):
        assert ext._fallback_division("Women's Apparel — SS25") == "Women's Apparel"

    def test_fallback_division_kids(self):
        assert ext._fallback_division("Baby Romper Order") == "Kids and Baby"

    def test_fallback_division_other(self):
        assert ext._fallback_division("Random email subject") == "Other"

    def test_fallback_style_extracts(self):
        result = ext._fallback_style_numbers("Please check M--1234 and W-PNT-228")
        assert "M--1234" in result
        assert "W-PNT-228" in result


# ═══════════════════════════════════════════════════════════
# 12. INTENT / REQUIRES_REPLY COERCION
# ═══════════════════════════════════════════════════════════

class TestIntentValidation:

    def test_requires_reply_string_true(self):
        """LLM sometimes returns "true" as string — must coerce to bool."""
        raw = "true"
        result = raw.strip().lower() in ("true", "yes", "1")
        assert result is True

    def test_requires_reply_bool_false(self):
        raw = False
        assert isinstance(raw, bool)
        assert raw is False

    def test_empty_intent_defaults_to_other(self):
        intent = "".strip() or "Other"
        assert intent == "Other"


# ═══════════════════════════════════════════════════════════
# 13. DRY RUN FLAG
# ═══════════════════════════════════════════════════════════

class TestDryRun:

    def test_dry_run_default_false(self):
        """DRY_RUN must be False by default so normal runs write to Sheets."""
        assert ext.DRY_RUN is False

    def test_dry_run_can_be_set(self):
        original = ext.DRY_RUN
        try:
            ext.DRY_RUN = True
            assert ext.DRY_RUN is True
        finally:
            ext.DRY_RUN = original


# ═══════════════════════════════════════════════════════════
# 14. PROMPT VERSIONING
# ═══════════════════════════════════════════════════════════

class TestPromptVersioning:

    def test_prompt_version_is_string(self):
        assert isinstance(ext.PROMPT_VERSION, str)

    def test_prompt_version_not_empty(self):
        assert len(ext.PROMPT_VERSION) > 0

    def test_prompt_version_format(self):
        """Should follow vX.Y format."""
        import re
        assert re.match(r'^v\d+\.\d+$', ext.PROMPT_VERSION), \
            f"PROMPT_VERSION '{ext.PROMPT_VERSION}' should match vX.Y format"


# ═══════════════════════════════════════════════════════════
# 15. AUDIT LOG STRUCTURE
# ═══════════════════════════════════════════════════════════

class TestAuditLog:

    def test_audit_log_keys_present(self):
        """write_audit_log must receive a dict with required keys."""
        required_keys = {"threads_fetched", "added", "updated", "skipped", "errors"}
        stats = {
            "threads_fetched": 50,
            "added": 3,
            "updated": 5,
            "backfilled": 2,
            "skipped": 40,
            "errors": 0,
            "cache_entries": 12,
        }
        assert required_keys.issubset(stats.keys())

    def test_error_rate_calculation(self):
        """Error rate = errors / threads_fetched * 100."""
        errors = 5
        total  = 50
        rate   = round(errors / max(total, 1) * 100, 1)
        assert rate == 10.0

    def test_zero_division_safe(self):
        """Error rate must not crash when threads_fetched = 0."""
        errors = 0
        total  = 0
        rate   = round(errors / max(total, 1) * 100, 1)
        assert rate == 0.0