# lib/limits.py — SQLite state, counters, suppression, bounce stats, per-recipient locks
import os, sqlite3, time, json
from datetime import datetime
from contextlib import contextmanager
from zoneinfo import ZoneInfo

DB_PATH = os.getenv("SEND_STATE_DB", "send_state.db")

# Timezone used for "today" (daily caps/counters). Default aligns to business tz.
TZ_NAME = os.getenv("SENDER_TZ", "America/Toronto")
TZ = ZoneInfo(TZ_NAME)

@contextmanager
def _db():
    con = sqlite3.connect(DB_PATH)
    try:
        yield con
        con.commit()
    finally:
        con.close()

def init_db():
    """
    Migration-safe DB bootstrap:
    - Ensures all tables exist.
    - Adds missing columns to sends.
    - Creates indexes.
    - Migrates legacy 'donotsend' once (if present).
    - Ensures lock table exists.
    """
    with _db() as con:
        # helpers
        def table_exists(name: str) -> bool:
            cur = con.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (name,))
            return cur.fetchone() is not None

        def columns(name: str) -> set:
            return {row[1] for row in con.execute(f"PRAGMA table_info('{name}')")}

        # Pragmas
        con.execute("PRAGMA journal_mode=WAL;")
        con.execute("PRAGMA synchronous=NORMAL;")

        # ----------------------------- sends -----------------------------
        if not table_exists("sends"):
            con.execute("""
                CREATE TABLE sends (
                  id            INTEGER PRIMARY KEY,
                  ts            INTEGER NOT NULL,
                  date          TEXT NOT NULL,
                  sender_id     TEXT NOT NULL,
                  to_email      TEXT NOT NULL,
                  prospect_id   TEXT,
                  message_id    TEXT,
                  status        TEXT CHECK(status IN (
                                    'sent','friendly','soft_bounce','bounce','skipped','reply','ooo','error'
                                  )) NOT NULL,
                  reason        TEXT DEFAULT '',
                  meta_json     TEXT DEFAULT ''
                );
            """)
        else:
            cols = columns("sends")
            if "date" not in cols:
                con.execute("ALTER TABLE sends ADD COLUMN date TEXT;")
                con.execute("UPDATE sends SET date = date(ts,'unixepoch','localtime') WHERE date IS NULL;")
                con.execute("UPDATE sends SET date = COALESCE(date, date('now','localtime'));")
            if "meta_json" not in cols:
                con.execute("ALTER TABLE sends ADD COLUMN meta_json TEXT DEFAULT '';")
            if "reason" not in cols:
                con.execute("ALTER TABLE sends ADD COLUMN reason TEXT DEFAULT '';")

        con.execute("CREATE INDEX IF NOT EXISTS idx_sends_email ON sends(to_email);")
        con.execute("CREATE INDEX IF NOT EXISTS idx_sends_sender_ts ON sends(sender_id, ts);")
        con.execute("CREATE INDEX IF NOT EXISTS idx_sends_date_sender ON sends(date, sender_id);")

        # ------------------------ counters_today ------------------------
        if not table_exists("counters_today"):
            con.execute("""
                CREATE TABLE counters_today (
                  sender_id   TEXT NOT NULL,
                  ymd         TEXT NOT NULL,
                  count       INTEGER NOT NULL,
                  PRIMARY KEY (sender_id, ymd)
                );
            """)

        # --------------------------- suppression ------------------------
        if not table_exists("suppression"):
            con.execute("""
                CREATE TABLE suppression (
                  email       TEXT PRIMARY KEY,
                  until_ts    INTEGER,
                  reason      TEXT DEFAULT ''
                );
            """)

        # migrate legacy donotsend -> suppression
        if table_exists("donotsend"):
            con.execute("""
                INSERT OR IGNORE INTO suppression(email, until_ts, reason)
                SELECT email, until_ts, 'migrated'
                FROM donotsend;
            """)

        # ------------------------ contact_history -----------------------
        if not table_exists("contact_history"):
            con.execute("""
                CREATE TABLE contact_history (
                  email         TEXT PRIMARY KEY,
                  last_sent_ts  INTEGER NOT NULL
                );
            """)

        # Backfill contact_history once
        cur = con.execute("SELECT COUNT(*) FROM contact_history;")
        if (cur.fetchone() or [0])[0] == 0:
            con.execute("""
                INSERT OR IGNORE INTO contact_history(email, last_sent_ts)
                SELECT to_email, MAX(ts) AS last_ts
                FROM sends
                GROUP BY to_email;
            """)

        # --------------------------- send_locks -------------------------
        if not table_exists("send_locks"):
            con.execute("""
                CREATE TABLE send_locks (
                  email     TEXT PRIMARY KEY,
                  until_ts  INTEGER NOT NULL,
                  holder    TEXT
                );
            """)
            con.execute("CREATE INDEX IF NOT EXISTS idx_send_locks_until ON send_locks(until_ts);")

# ---------------- Counters (per-sender per-day) ----------------

def _today() -> str:
    """Calendar day string in the configured timezone (SENDER_TZ)."""
    return datetime.now(TZ).date().isoformat()

def get_today_count(sender_id: str) -> int:
    with _db() as con:
        cur = con.execute(
            "SELECT count FROM counters_today WHERE sender_id=? AND ymd=?",
            (str(sender_id), _today())
        )
        row = cur.fetchone()
        return int(row[0]) if row else 0

def inc_today_count(sender_id: str, inc: int = 1):
    ymd = _today()
    with _db() as con:
        cur = con.execute("SELECT count FROM counters_today WHERE sender_id=? AND ymd=?", (str(sender_id), ymd))
        row = cur.fetchone()
        if row:
            con.execute(
                "UPDATE counters_today SET count=? WHERE sender_id=? AND ymd=?",
                (int(row[0]) + int(inc), str(sender_id), ymd)
            )
        else:
            con.execute(
                "INSERT INTO counters_today(sender_id, ymd, count) VALUES (?,?,?)",
                (str(sender_id), ymd, int(inc))
            )

# ---------------- Suppression / Dedupe ----------------

def can_send_to(email: str) -> bool:
    email = (email or "").strip().lower()
    if not email or "@" not in email:
        return False
    with _db() as con:
        cur = con.execute("SELECT until_ts FROM suppression WHERE email=?", (email,))
        row = cur.fetchone()
        if not row:
            return True
        until_ts = row[0]
        if until_ts is None:  # permanent block
            return False
        return int(time.time()) > int(until_ts)

def mark_do_not_send(email: str, days: int = 0, reason: str = "hard_bounce"):
    email = (email or "").strip().lower()
    until_ts = None if days <= 0 else int(time.time() + days * 86400)
    with _db() as con:
        con.execute(
            "INSERT INTO suppression(email, until_ts, reason) VALUES (?,?,?) "
            "ON CONFLICT(email) DO UPDATE SET until_ts=excluded.until_ts, reason=excluded.reason",
            (email, until_ts, reason)
        )

def already_contacted(email: str, dedupe_days: int = 30) -> bool:
    email = (email or "").strip().lower()
    cutoff = int(time.time() - dedupe_days * 86400)
    with _db() as con:
        cur = con.execute("SELECT 1 FROM sends WHERE to_email=? AND ts>=? LIMIT 1", (email, cutoff))
        return cur.fetchone() is not None

# ---------------- Per-recipient locks (prevent double sends) ----------------

def reserve_lock(email: str, holder: str, ttl_sec: int = 600) -> bool:
    """
    Try to reserve a short-lived lock for this recipient.
    Returns True if acquired; False if someone else holds a non-expired lock.
    """
    email = (email or "").strip().lower()
    now = int(time.time())
    with _db() as con:
        cur = con.execute("SELECT until_ts FROM send_locks WHERE email=?", (email,))
        row = cur.fetchone()
        if row:
            if int(row[0] or 0) > now:
                return False  # locked by another process
        # create or refresh
        expires = now + int(ttl_sec)
        con.execute(
            "INSERT INTO send_locks(email, until_ts, holder) VALUES (?,?,?) "
            "ON CONFLICT(email) DO UPDATE SET until_ts=?, holder=?",
            (email, expires, holder, expires, holder)
        )
        return True

def release_lock(email: str, holder: str):
    """Release a lock if we still own it."""
    email = (email or "").strip().lower()
    with _db() as con:
        con.execute("DELETE FROM send_locks WHERE email=? AND holder=?", (email, holder))

def prune_expired_locks():
    """Housekeeping: remove expired locks (safe to call occasionally)."""
    now = int(time.time())
    with _db() as con:
        con.execute("DELETE FROM send_locks WHERE until_ts < ?", (now,))

# ---------------- Event logging & bounce stats ----------------

def record_send(sender_id: str, to_email: str, prospect_id: str | None, message_id: str | None,
                status: str = "sent", reason: str = "", meta: dict | None = None):
    """
    Log any event (send, friendly, bounce, soft_bounce, reply, error).
    Also refreshes contact_history for quick dedupe checks.
    - 'date' is computed in SENDER_TZ for daily cap alignment.
    """
    now = int(time.time())
    ymd = _today()
    to_email = (to_email or "").strip().lower()
    with _db() as con:
        con.execute(
            "INSERT INTO sends(ts,date,sender_id,to_email,prospect_id,message_id,status,reason,meta_json) "
            "VALUES (?,?,?,?,?,?,?,?,?)",
            (now, ymd, str(sender_id), to_email, prospect_id, message_id or "", status, reason or "", json.dumps(meta or {}))
        )
        con.execute(
            "INSERT INTO contact_history(email,last_sent_ts) VALUES (?,?) "
            "ON CONFLICT(email) DO UPDATE SET last_sent_ts=excluded.last_sent_ts",
            (to_email, now)
        )

def recent_bounce_rate(sender_id: str, lookback_n: int = 50) -> float:
    with _db() as con:
        cur = con.execute(
            "SELECT status FROM sends WHERE sender_id=? ORDER BY ts DESC LIMIT ?",
            (str(sender_id), int(lookback_n))
        )
        rows = [r[0] for r in cur.fetchall()]
    if not rows:
        return 0.0
    b = sum(1 for s in rows if s in ("bounce", "soft_bounce"))
    return b / float(len(rows))
