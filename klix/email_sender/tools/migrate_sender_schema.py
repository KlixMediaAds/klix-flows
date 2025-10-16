# tools/migrate_sender_schema.py
"""
One-time migration script for Email Sender schema.
Adds required columns to Emails sheet and ensures History tab exists.
This version is debug-heavy so you can see exactly what's happening.
"""

import os, re, gspread, yaml, json
from datetime import datetime, timezone
from dotenv import load_dotenv
from gspread.exceptions import WorksheetNotFound, SpreadsheetNotFound

load_dotenv()

REQUIRED_EMAIL_COLS = [
    "lead_id", "email", "name", "company",
    "status", "attempts", "last_sent_at",
    "follow_up_stage", "next_followup_at",
    "template_id", "reply_flag", "last_response_at",
    "bounce_flag", "bounce_code", "bounce_reason",
    "notes",
]

HISTORY_HEADERS = [
    "timestamp", "lead_id", "action",
    "stage_before", "stage_after",
    "status_before", "status_after",
    "message_id", "notes"
]

def _now_iso(): return datetime.now(timezone.utc).isoformat()

def _load_cfg(path="config/sheet.yaml"):
    def _expand_env(obj):
        if isinstance(obj, dict):
            return {k: _expand_env(v) for k, v in obj.items()}
        if isinstance(obj, list):
            return [_expand_env(v) for v in obj]
        if isinstance(obj, str):
            return os.path.expandvars(obj)
        return obj
    with open(path, "r", encoding="utf-8") as f:
        raw = yaml.safe_load(f)
    return _expand_env(raw)

def _svc_and_email():
    cred_path = os.getenv("GOOGLE_SHEETS_CRED", "").strip()
    if not cred_path or not os.path.isfile(cred_path):
        raise FileNotFoundError(f"Service account JSON not found: {cred_path}")
    with open(cred_path, "r", encoding="utf-8") as f:
        sa = json.load(f)
    email = sa.get("client_email", "(unknown)")
    gc = gspread.service_account(filename=cred_path)
    return gc, email

KEY_RX = re.compile(r"/d/([a-zA-Z0-9-_]+)")
def _extract_key(maybe_url_or_key: str) -> str:
    m = KEY_RX.search(maybe_url_or_key)
    return m.group(1) if m else maybe_url_or_key.strip()

def _headers(ws): return [h.strip() for h in (ws.row_values(1) or [])]

def _ensure_columns(ws, want_cols):
    have = _headers(ws)
    to_add = [c for c in want_cols if c not in have]
    if not have:
        ws.update([want_cols]); print(f"[MIGRATE] Initialized headers with {len(want_cols)} cols")
    elif to_add:
        ws.update([have + to_add]); print(f"[MIGRATE] Added columns: {to_add}")
    else:
        print("[MIGRATE] Emails sheet already has required columns")

def _ensure_history(ss, title="History"):
    try:
        hist = ss.worksheet(title)
        if _headers(hist) != HISTORY_HEADERS:
            hist.clear(); hist.update([HISTORY_HEADERS])
            print("[MIGRATE] Reset History headers")
        else:
            print("[MIGRATE] History tab present")
        return hist
    except WorksheetNotFound:
        hist = ss.add_worksheet(title=title, rows=1000, cols=len(HISTORY_HEADERS))
        hist.update([HISTORY_HEADERS])
        print("[MIGRATE] Created History tab")
        return hist

def main():
    cfg = _load_cfg()
    raw = str(cfg.get("spreadsheet_id", "")).strip()
    key = _extract_key(raw)

    print("=== DEBUG INFO ===")
    print(f"Raw spreadsheet_id from YAML: {raw!r}")
    print(f"Expanded key (after regex): {key!r}")
    print(f"Env SHEET_ID: {os.getenv('SHEET_ID')!r}")
    print(f"Env GOOGLE_SHEETS_CRED: {os.getenv('GOOGLE_SHEETS_CRED')!r}")
    print("==================")

    gc, sa_email = _svc_and_email()
    print(f"[INFO] Using service account: {sa_email}")

    try:
        ss = gc.open_by_key(key)
    except SpreadsheetNotFound as ex:
        print("\n[ERROR] Spreadsheet not found (404).")
        print("Most common fixes:")
        print("  1) Share the sheet with this service account as Editor:")
        print(f"     {sa_email}")
        print("  2) Ensure the spreadsheet_id is correct (copy from URL after /d/).")
        print("  3) If you put ${SHEET_ID} in YAML, confirm .env has SHEET_ID set.")
        raise

    emails_tab_name = cfg["tabs"]["emails"]
    emails_ws = ss.worksheet(emails_tab_name)
    _ensure_columns(emails_ws, REQUIRED_EMAIL_COLS)
    _ensure_history(ss, "History")

    print("[DONE] Migration complete at", _now_iso())

if __name__ == "__main__":
    main()
