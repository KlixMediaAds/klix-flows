# app/sender_engine/pacing.py
import time, random
from datetime import datetime, timedelta
from .types import Context

class Pacer:
    def __init__(self, ctx: Context):
        self.ctx = ctx
        self.last_send_at_global = None
        self.last_send_at_by_sender = {}
        self.rng = random.Random()

    def wait_before_send(self, sender_id: str):
        now = datetime.now(self.ctx.tz)
        waits = []
        if self.last_send_at_global is not None:
            due = self.last_send_at_global + timedelta(seconds=self.ctx.GLOBAL_GAP_SEC)
            if due > now:
                waits.append((due - now).total_seconds())
        last_sender_dt = self.last_send_at_by_sender.get(sender_id)
        if last_sender_dt is not None:
            target_gap = self.rng.uniform(self.ctx.PER_SENDER_MIN_GAP_MIN_SEC, self.ctx.PER_SENDER_MIN_GAP_MAX_SEC)
            due = last_sender_dt + timedelta(seconds=target_gap)
            if due > now:
                waits.append((due - now).total_seconds())
        if waits:
            sleep_s = max(0.0, max(waits))
            if sleep_s > 0.5:
                print(f"[PACER] waiting {sleep_s:.0f}s (global={self.ctx.GLOBAL_GAP_SEC}s, sender_min≈{self.ctx.PER_SENDER_MIN_GAP_MIN_SEC}-{self.ctx.PER_SENDER_MIN_GAP_MAX_SEC}s) for {sender_id}")
                time.sleep(sleep_s)

    def mark_sent(self, sender_id: str):
        now = datetime.now(self.ctx.tz)
        self.last_send_at_global = now
        self.last_send_at_by_sender[sender_id] = now

# per-hour throttle lookup
import sqlite3
def sent_last_hour(sender_id: str) -> int:
    try:
        with sqlite3.connect("send_state.db") as con:
            row = con.execute("""
                SELECT COUNT(*) FROM sends
                WHERE sender_id=? 
                  AND ts >= strftime('%s','now','-1 hour')
                  AND status IN ('sent','friendly')
            """, (sender_id,)).fetchone()
        return int(row[0]) if row and row[0] is not None else 0
    except Exception:
        return 0
