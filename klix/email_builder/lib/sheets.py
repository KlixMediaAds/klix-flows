import os, time
from typing import List, Dict, Any
import gspread
from google.oauth2.service_account import Credentials
from datetime import datetime, timezone

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

# Minimum columns the system expects. We will ADD these if missing.
PROSPECTS_REQUIRED = [
    "ID","BusinessName","Website","Email","AltEmail","Instagram",
    "City","Niche","Notes","Status","CreatedAt","LastTouchedAt","DedupeKey"
]
EMAILS_REQUIRED = [
    "EmailID","ProspectID","BusinessName","ToEmail","PersonaTarget","Angle",
    "Subject","BodyMD","DraftedAt","Status","Model","Notes"
]

def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")

def _client():
    cred_path = os.getenv("GOOGLE_SHEETS_CRED", "").strip()
    creds = Credentials.from_service_account_file(cred_path, scopes=SCOPES)
    return gspread.authorize(creds)

class Sheet:
    def __init__(self, spreadsheet_id: str):
        self.gc = _client()
        self.ss = self.gc.open_by_key(spreadsheet_id)

    # --- NEW: helpers -------------------------------------------------
    def _titles(self):
        return [ws.title for ws in self.ss.worksheets()]

    def _latest_tab_with_prefix(self, prefix: str) -> str | None:
        """
        Finds the newest tab whose title starts with `prefix`
        and ends with an ISO date (YYYY-MM-DD), e.g. 'Leads 2025-10-01'.
        """
        import re
        from datetime import datetime
        patt = re.compile(rf"^{re.escape(prefix)}\s*(\d{{4}}-\d{{2}}-\d{{2}})$")
        best = None
        best_dt = None
        for title in self._titles():
            m = patt.match(title.strip())
            if not m:
                continue
            try:
                dt = datetime.strptime(m.group(1), "%Y-%m-%d")
            except ValueError:
                continue
            if best_dt is None or dt > best_dt:
                best, best_dt = title, dt
        return best

    def _resolve_name(self, name: str) -> tuple[str, str]:
        """
        Supports selector syntax:
          - 'LATEST:Leads '  -> newest tab starting with 'Leads ' and ending in YYYY-MM-DD
        Returns (resolved_title, reason_string)
        """
        key = name.strip()
        if key.upper().startswith("LATEST:"):
            prefix = key.split(":", 1)[1]
            latest = self._latest_tab_with_prefix(prefix)
            if latest:
                return latest, f"resolved LATEST:'{prefix}' -> '{latest}'"
            # Fallback: first worksheet if none match
            first = self.ss.worksheets()[0].title
            return first, f"no tab matched LATEST:'{prefix}', falling back to first: '{first}'"
        return key, f"explicit tab '{key}'"

    # ------------------------------------------------------------------

    def ws(self, name: str):
        resolved, why = self._resolve_name(name)
        try:
            print(f"[SHEETS] Using tab: {resolved} ({why})")
            return self.ss.worksheet(resolved)
        except gspread.WorksheetNotFound:
            # Only auto-create when caller gave an explicit name
            if resolved == name:
                print(f"[SHEETS] Creating missing tab: {resolved}")
                return self.ss.add_worksheet(title=resolved, rows=2000, cols=50)
            # If using LATEST: and nothing exists, fall back to index 0
            print("[SHEETS] LATEST: could not find any tab; using first worksheet.")
            return self.ss.get_worksheet(0)

    def _ensure_headers(self, ws, required: List[str]):
        header = ws.row_values(1)
        if not header:
            ws.append_row(required)
            time.sleep(0.3)
            return required
        missing = [h for h in required if h not in header]
        if missing:
            new_header = header + missing
            ws.delete_rows(1)
            ws.insert_row(new_header, 1)
            time.sleep(0.3)
            return new_header
        return header

    def ensure_tab(self, name: str, required: List[str]) -> List[str]:
        # NOTE: do not pass LATEST: here; use explicit names for “schema” tabs
        ws = self.ws(name)
        return self._ensure_headers(ws, required)

    def header(self, name: str) -> List[str]:
        ws = self.ws(name)
        return ws.row_values(1)

    def read(self, name: str) -> List[Dict[str, Any]]:
        ws = self.ws(name)
        return ws.get_all_records()

    def append_row_dict(self, name: str, row: Dict[str, Any]):
        ws = self.ws(name)
        header = ws.row_values(1)
        vals = [row.get(h, "") for h in header]
        ws.append_row(vals)

    def upsert_by_id(self, name: str, id_col: str, id_val: str, updates: Dict[str, Any]) -> bool:
        ws = self.ws(name)
        rows = ws.get_all_values()
        if not rows:
            header = list(updates.keys())
            if id_col not in header: header = [id_col] + header
            ws.append_row(header)
            time.sleep(0.2)
            ws.append_row([updates.get(h, "") if h != id_col else id_val for h in header])
            return True

        header = rows[0]
        col_index = {h: i+1 for i, h in enumerate(header)}
        new_cols = [k for k in updates.keys() if k not in col_index and k != id_col]
        if new_cols:
            new_header = header + new_cols
            ws.delete_rows(1)
            ws.insert_row(new_header, 1)
            time.sleep(0.2)
            header = new_header
            col_index = {h: i+1 for i, h in enumerate(header)}

        if id_col not in col_index:
            header.append(id_col)
            ws.delete_rows(1)
            ws.insert_row(header, 1)
            time.sleep(0.2)
            col_index = {h: i+1 for i, h in enumerate(header)}

        target_idx = None
        for i in range(1, len(rows)):
            if col_index.get(id_col) and len(rows[i]) >= col_index[id_col]:
                if rows[i][col_index[id_col]-1] == str(id_val):
                    target_idx = i+1
                    break

        if target_idx is None:
            vals = []
            for h in header:
                vals.append(id_val if h == id_col else str(updates.get(h, "")))
            ws.append_row(vals)
            return True

        cells = []
        for k, v in updates.items():
            if k in col_index:
                cells.append(gspread.Cell(target_idx, col_index[k], str(v)))
        if cells:
            ws.update_cells(cells)
        return True

# Convenience helpers for this project
def ensure_base_tabs(sheet: Sheet, prospects_tab: str, emails_tab: str):
    sheet.ensure_tab(prospects_tab, PROSPECTS_REQUIRED)
    sheet.ensure_tab(emails_tab, EMAILS_REQUIRED)
