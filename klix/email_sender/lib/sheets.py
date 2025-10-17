# lib/sheets.py — Sheets I/O with follow-up, history, and SendsSummary support
import os, yaml, gspread, time
from datetime import datetime
from dotenv import load_dotenv
from gspread.exceptions import WorksheetNotFound, APIError
from zoneinfo import ZoneInfo

load_dotenv()

_ws_cache = {}  # cache worksheets by (book.id, title)

# --------------------------- time helpers ---------------------------

_SENDER_TZ = os.getenv("SENDER_TZ", "America/Toronto")
_TZ = ZoneInfo(_SENDER_TZ)

def _now_iso():
    # Keep timestamps aligned with business-time day boundaries (same as limits.py)
    return datetime.now(_TZ).isoformat()

# --------------------------- cfg helpers ----------------------------

def _expand_env(obj):
    if isinstance(obj, dict):
        return {k: _expand_env(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_expand_env(v) for v in obj]
    if isinstance(obj, str):
        return os.path.expandvars(obj)
    return obj

def connect(spreadsheet_id: str):
    cred_path = os.getenv("GOOGLE_SHEETS_CRED", "").strip()
    if not cred_path or not os.path.isfile(cred_path):
        raise FileNotFoundError(f"Service account JSON not found: {cred_path}")
    print(f"[DEBUG] Using service account: {cred_path}")
    print(f"[DEBUG] Opening spreadsheet: {spreadsheet_id}")
    gc = gspread.service_account(filename=cred_path)
    return gc.open_by_key(spreadsheet_id)

def load_sheet_config(path="config/sheet.yaml"):
    with open(path, "r", encoding="utf-8") as f:
        return _expand_env(yaml.safe_load(f))

def list_worksheets(book):
    titles = [ws.title for ws in book.worksheets()]
    print(f"[DEBUG] Worksheets visible: {titles}")
    return titles

# --------------------------- ws helpers -----------------------------

def _norm(s: str): return (s or "").strip().lower()
def _headers(ws):  return [h.strip() for h in (ws.row_values(1) or [])]

def _open_ws(book, preferred_title: str, required_headers=None):
    """
    Open a worksheet by exact title, case-insensitive title,
    or (if required_headers provided) by matching header set.
    """
    global _ws_cache
    key = (book.id, preferred_title)
    if key in _ws_cache:
        return _ws_cache[key]
    try:
        ws = book.worksheet(preferred_title)
        print(f"[DEBUG] Found worksheet by exact name: '{ws.title}'")
        _ws_cache[key] = ws
        return ws
    except WorksheetNotFound:
        pass

    for ws in book.worksheets():  # case-insensitive
        if _norm(ws.title) == _norm(preferred_title):
            _ws_cache[key] = ws
            return ws

    if required_headers:
        need = set(map(_norm, required_headers))
        for ws in book.worksheets():
            have = set(map(_norm, _headers(ws)))
            if need.issubset(have):
                _ws_cache[key] = ws
                return ws

    raise WorksheetNotFound(
        f"Worksheet '{preferred_title}' not found. "
        f"Available: {[ws.title for ws in book.worksheets()]}"
    )

def _find_row_index(ws, header, col_name, value):
    """
    Find the 1-based row index (including header row) for a row where column `col_name`
    equals `value`. Returns None if not found. Assumes header row is row 1.
    """
    if col_name not in header:
        return None
    col_idx = header.index(col_name) + 1
    # Iterate the column values (skip header) to find the value
    for i, v in enumerate(ws.col_values(col_idx)[1:], start=2):
        if str(v).strip() == str(value).strip():
            return i
    return None

# ------------------------ resilient writes --------------------------

def _with_backoff(fn, *, retries=5, base=1.5, label="op"):
    last_err = None
    for attempt in range(retries):
        try:
            return fn()
        except APIError as e:
            last_err = e
            wait = base ** attempt
            print(f"[WARN] Sheets {label} failed ({attempt+1}/{retries}): {e}. Retrying in {wait:.1f}s")
            time.sleep(wait)
    raise RuntimeError(f"Sheets {label} failed after {retries} retries: {last_err}")

def _safe_batch_update(ws, updates):
    """
    updates: list of tuples (A1_range, value)
    Retries with backoff on transient API errors.
    """
    if not updates:
        return
    def _do():
        ws.batch_update([{"range": r, "values": [[val]]} for r, val in updates])
    _with_backoff(_do, label="batch_update")

def _safe_update(ws, data, *, value_input_option="RAW"):
    def _do():
        ws.update(data, value_input_option=value_input_option)
    _with_backoff(_do, label="update")

def _safe_append_row(ws, row, *, value_input_option="RAW"):
    def _do():
        ws.append_row(row, value_input_option=value_input_option)
    _with_backoff(_do, label="append_row")

def _safe_clear(ws):
    def _do():
        ws.clear()
    _with_backoff(_do, label="clear")

# --------------------- Emails tab read/write ------------------------

def read_emails(ws):
    """Return all rows from Emails tab as list of dicts (header-mapped)."""
    rows = ws.get_all_records()
    print(f"[DEBUG] Emails rows loaded: {len(rows)}")
    return rows

def update_followup_state(ws, cfg, email_id, changes: dict):
    """
    Apply follow-up / state changes to a single email row.
    Supports mapped headers from YAML and literal header names.
    """
    e = cfg["columns"]["emails"]
    header = ws.row_values(1)
    # find row by mapped id column
    row_idx = _find_row_index(ws, header, e["id"], email_id)
    if not row_idx:
        print(f"[WARN] EmailID '{email_id}' not found for follow-up update.")
        return

    updates = []
    for field, val in changes.items():
        # prefer YAML mapping if present, else use the field name directly
        col_name = e.get(field, field)
        if col_name in header:
            col = header.index(col_name) + 1
            a1 = f"{_col_letter(col)}{row_idx}"
            updates.append((a1, str(val)))

    if updates:
        _safe_batch_update(ws, updates)
        print(f"[DEBUG] Follow-up state updated for EmailID={email_id}: {changes}")

def write_email_send_result(ws, cfg, email_id, message_id):
    """
    Mark a row as 'sent' and append to Notes with timestamp + msg id.
    """
    e = cfg["columns"]["emails"]
    header = ws.row_values(1)
    row_idx = _find_row_index(ws, header, e["id"], email_id)
    if not row_idx:
        print(f"[WARN] EmailID '{email_id}' not found.")
        return

    updates = []
    if e["status"] in header:
        col = header.index(e["status"]) + 1
        updates.append((f"{_col_letter(col)}{row_idx}", "sent"))

    if e["notes"] in header:
        col = header.index(e["notes"]) + 1
        existing = ws.cell(row_idx, col).value or ""
        sep = "\n" if existing.strip() else ""
        line = f"SENT {_now_iso()} msg={message_id}"
        updates.append((f"{_col_letter(col)}{row_idx}", existing + sep + line))
        print(f"[DEBUG] Notes appended for EmailID={email_id}: {line}")

    # Optionally set MessageID if the column exists in your mapping
    msg_col_name = e.get("message_id") or "MessageID"
    if msg_col_name in header:
        col = header.index(msg_col_name) + 1
        updates.append((f"{_col_letter(col)}{row_idx}", message_id))

    _safe_batch_update(ws, updates)

def write_email_note(ws, cfg, email_id, note_line: str):
    e = cfg["columns"]["emails"]
    header = ws.row_values(1)
    row_idx = _find_row_index(ws, header, e["id"], email_id)
    if not row_idx:
        print(f"[WARN] EmailID '{email_id}' not found when writing note.")
        return
    if e["notes"] not in header:
        print(f"[WARN] Notes column '{e['notes']}' not found; skipping note.")
        return
    col = header.index(e["notes"]) + 1
    existing = ws.cell(row_idx, col).value or ""
    sep = "\n" if existing.strip() else ""
    new_val = existing + sep + note_line
    _safe_batch_update(ws, [(f"{_col_letter(col)}{row_idx}", new_val)])
    print(f"[DEBUG] Note appended for EmailID={email_id}: {note_line}")

# --------------------- Prospects tab helpers ------------------------

def flip_prospect_status(book, cfg, prospect_id, new_status="SENT"):
    p = cfg["columns"]["prospects"]
    ws = _open_ws(book, cfg["tabs"]["prospects"], [p["id"], p["status"], p["last_touched_at"]])
    header = ws.row_values(1)
    row_idx = _find_row_index(ws, header, p["id"], prospect_id)
    if not row_idx:
        print(f"[WARN] ProspectID '{prospect_id}' not found.")
        return
    updates = []
    status_col = header.index(p["status"]) + 1
    lta_col = header.index(p["last_touched_at"]) + 1
    updates.append((f"{_col_letter(status_col)}{row_idx}", new_status))
    updates.append((f"{_col_letter(lta_col)}{row_idx}", _now_iso()))
    _safe_batch_update(ws, updates)
    print(f"[DEBUG] Prospect '{prospect_id}' flipped to {new_status}")

# ------------------------ History logging ---------------------------

def log_history(book, lead_id, action, stage_before, stage_after,
                status_before, status_after, message_id, notes=""):
    """
    Append a row to History tab (creates with headers if missing).
    """
    headers = ["timestamp","lead_id","action","stage_before","stage_after",
               "status_before","status_after","message_id","notes"]
    try:
        hist_ws = _open_ws(book, "History", [])
        if _headers(hist_ws) != headers:
            # if user altered headers, reset to keep consistency
            _safe_clear(hist_ws)
            _safe_update(hist_ws, [headers])
            print("[MIGRATE] Reset History headers to expected format")
    except WorksheetNotFound:
        hist_ws = book.add_worksheet(title="History", rows=1000, cols=len(headers))
        _safe_update(hist_ws, [headers])
        print("[MIGRATE] Created History tab")

    row = [
        _now_iso(), lead_id, action,
        stage_before, stage_after,
        status_before, status_after,
        message_id, notes
    ]
    _safe_append_row(hist_ws, row, value_input_option="RAW")
    print(f"[DEBUG] History logged: {row}")

# ------------------------ SendsSummary tab --------------------------

def upsert_sends_summary(book, summary_rows):
    """
    Create/refresh the SendsSummary tab with:
      date, domain, sender_id, count, rolling_7, rolling_30
    Strategy: rewrite the whole sheet on each run (simple & robust for our scale).
    """
    title = "SendsSummary"
    headers = ["date","domain","sender_id","count","rolling_7","rolling_30"]

    try:
        ws = _open_ws(book, title, [])
    except WorksheetNotFound:
        ws = book.add_worksheet(title=title, rows=1000, cols=len(headers))
        _safe_update(ws, [headers])
        print("[MIGRATE] Created SendsSummary tab")

    # normalize input (avoid KeyError on missing keys)
    def _row(r):
        return [
            str(r.get("date","")),
            str(r.get("domain","")),
            str(r.get("sender_id","")),
            int(r.get("count", 0)),
            int(r.get("rolling_7", 0)),
            int(r.get("rolling_30", 0)),
        ]

    _safe_clear(ws)
    data = [headers] + [_row(r) for r in (summary_rows or [])]
    _safe_update(ws, data, value_input_option="RAW")
    print(f"[DEBUG] SendsSummary updated: {max(0, len(data)-1)} rows")

# --------------------------- internals --------------------------------

def _col_letter(n: int) -> str:
    """Convert 1-based column index to A1 letter(s)."""
    s = ""
    while n:
        n, r = divmod(n-1, 26)
        s = chr(65+r) + s
    return s
