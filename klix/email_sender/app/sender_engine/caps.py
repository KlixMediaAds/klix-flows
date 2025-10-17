# app/sender_engine/caps.py
import sqlite3
from datetime import datetime, timedelta, date
from typing import List
from zoneinfo import ZoneInfo
from .types import Context, SenderState
from lib.limits import recent_bounce_rate, get_today_count
from lib.mailer import SMTPMailer

def _warmup_index_for_sender(s: dict, tz: ZoneInfo) -> int:
    sid = s.get("id")
    start = s.get("warmup_start")
    # Prefer history from DB (unique active weekdays with sends)
    try:
        if sid:
            with sqlite3.connect("send_state.db") as con:
                rows = con.execute("""
                    SELECT DISTINCT date(ts,'unixepoch','localtime') 
                    FROM sends
                    WHERE sender_id=? AND status IN ('sent','friendly')
                """, (sid,)).fetchall()
            days = 0
            for (ds,) in rows:
                try:
                    d = datetime.fromisoformat(ds).date()
                    if d.weekday() < 5:
                        days += 1
                except Exception:
                    pass
            return max(0, days - 1)
    except Exception:
        pass
    # Fallback: walk calendar skipping weekends
    try:
        d0 = datetime.fromisoformat(str(start)).date() if start else None
    except Exception:
        d0 = None
    if not d0:
        return 0
    today = datetime.now(tz).date()
    days = 0; cur = d0
    while cur <= today:
        if cur.weekday() < 5: days += 1
        cur += timedelta(days=1)
    return max(0, days - 1)

def _smart_daily_cap(sender_cfg, warmup_map, day_index, recent_bounce_rate_value, *,
                     base_daily_cap_key="daily_cap", friendly_bias=0.40, min_cap=2):
    ramp = (warmup_map or {}).get(sender_cfg.get("from_email"))
    if ramp and 0 <= day_index < len(ramp):
        total = int(ramp[day_index])
    else:
        total = int(sender_cfg.get(base_daily_cap_key, 25))
    br = recent_bounce_rate_value or 0.0
    if br >= 0.10:
        total = max(min_cap, int(total * 0.25))
    elif br >= 0.06:
        total = max(min_cap, int(total * 0.50))
    elif br >= 0.04:
        total = max(min_cap, int(total * 0.75))
    friendly_cap = max(0, int(round(total * friendly_bias)))
    cold_cap = max(0, total - friendly_cap)
    return total, friendly_cap, cold_cap

def resolve_active_senders(ctx: Context) -> List[SenderState]:
    senders_cfg = [s for s in (ctx.mail_cfg.get("senders") or []) if s.get("enabled", True)]
    active = []
    for s in senders_cfg:
        rbr = recent_bounce_rate(s["id"], lookback_n=ctx.LOOKBACK_N)
        if rbr >= ctx.BOUNCE_THRESHOLD:
            print(f"[PAUSE] sender={s['id']} bounce_rate={rbr:.1%} >= {ctx.BOUNCE_THRESHOLD:.0%} → auto-paused")
            continue
        day_index = _warmup_index_for_sender(s, ctx.tz)
        total_cap, friendly_cap, cold_cap = _smart_daily_cap(
            s, ctx.warmup_map, day_index, rbr, friendly_bias=ctx.FRIENDLY_FRACTION
        )
        today_count = get_today_count(s["id"])
        remaining_raw = total_cap - today_count
        if remaining_raw <= 0:
            if remaining_raw < 0:
                print(f"[CAPS] {s['id']} day={day_index} br={rbr:.2%} total={total_cap} friendly={friendly_cap} cold={cold_cap} remaining=0 (OVER by {abs(remaining_raw)})")
            else:
                print(f"[SKIP] sender={s['id']} at cap (today={today_count}/{total_cap})")
            continue
        remaining = remaining_raw
        print(f"[CAPS] {s.get('id')} day={day_index} br={rbr:.2%} total={total_cap} friendly={friendly_cap} cold={cold_cap} remaining={remaining}")
        active.append(SenderState(
            cfg=s,
            mailer=SMTPMailer(s, dry_run=ctx.DRY_RUN),
            today=today_count,
            daily_cap=total_cap,
            remaining=remaining,
            _cap_total=total_cap,
            _cap_friendly=friendly_cap,
            _cap_cold=cold_cap,
            _used_friendly=0,
            _used_cold=0,
            _bounce_rate=rbr,
            _day_index=day_index,
        ))
    return active
