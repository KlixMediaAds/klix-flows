# app/sender_engine/audit.py
from datetime import datetime
from .types import Context, SenderState
from lib.sheets import upsert_sends_summary
import sqlite3, time
from collections import defaultdict

def flush_summary(ctx: Context, active_senders):
    def _rollup_rows():
        ts_now = int(time.time())
        ts_7d  = ts_now - 7*86400
        ts_30d = ts_now - 30*86400
        rows = []
        with sqlite3.connect("send_state.db") as con:
            cur = con.execute("""
                SELECT date(ts,'unixepoch','localtime') as d, sender_id, COUNT(*) as c
                FROM sends
                WHERE status IN ('sent','friendly')
                GROUP BY d, sender_id
                ORDER BY d DESC
            """)
            daily = cur.fetchall()
            sid2dom = {asnd.cfg["id"]: (asnd.cfg.get("domain") or asnd.cfg["from_email"].split("@")[-1])
                       for asnd in active_senders}
            cur = con.execute("""
                SELECT ts, sender_id FROM sends
                WHERE status IN ('sent','friendly') AND ts >= ?
            """, (ts_30d,))
            last = cur.fetchall()
            rolling7 = defaultdict(int); rolling30 = defaultdict(int)
            for ts, sid in last:
                if ts >= ts_7d: rolling7[sid]  += 1
                rolling30[sid] += 1
            today_str = datetime.now(ctx.tz).date().isoformat()
            per_today = [d for d in daily if d[0]==today_str]
            for asnd in active_senders:
                sid = asnd.cfg["id"]
                dom = sid2dom.get(sid, (asnd.cfg.get("domain") or asnd.cfg["from_email"].split("@")[-1]))
                today_count = 0
                for d,sid2,c in per_today:
                    if sid2==sid: today_count = c
                rows.append({
                    "date": today_str,
                    "domain": dom,
                    "sender_id": sid,
                    "count": today_count,
                    "rolling_7": rolling7.get(sid, 0),
                    "rolling_30": rolling30.get(sid, 0),
                })
        return rows
    upsert_sends_summary(ctx.book, _rollup_rows())
