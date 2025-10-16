# app/sender_engine/pools.py
import time, sqlite3, math, os
from datetime import datetime
from typing import Dict, List, Optional
from zoneinfo import ZoneInfo
from .types import Context, Pools, SenderState
from .util import gv, gi, gs, _parse_iso, _is_blocked, _fmt_time
from lib.friendly import build_friendly_plan as _build_friendly_plan, HistoryItem as _HistoryItem  # optional
from lib.friendly import plan_friendly_pairs as _plan_friendly_pairs                              # optional

def _max_attempts():
    import os
    return int(os.getenv("MAX_ATTEMPTS", "3"))

def _due_followup(ctx: Context, r: dict) -> bool:
    if _is_blocked(gs(ctx.COLMAP, r, "status")): return False
    attempts = gi(ctx.COLMAP, r, "attempts", 0)
    if attempts >= _max_attempts(): return False
    stage = gi(ctx.COLMAP, r, "follow_up_stage", 0)
    if stage < 1: return False
    next_at = _parse_iso(gv(ctx.COLMAP, r, "next_followup_at"))
    return bool(next_at and next_at <= datetime.now(ctx.tz))

def _not_yet_due_followup_min_dt(ctx: Context, rows_iter) -> Optional[datetime]:
    now_t = datetime.now(ctx.tz)
    cand = None
    for r in rows_iter:
        if _is_blocked(gs(ctx.COLMAP, r, "status")): continue
        attempts = gi(ctx.COLMAP, r, "attempts", 0)
        if attempts >= _max_attempts(): continue
        stage = gi(ctx.COLMAP, r, "follow_up_stage", 0)
        if stage < 1: continue
        nxt = _parse_iso(gv(ctx.COLMAP, r, "next_followup_at"))
        if nxt and nxt > now_t:
            if cand is None or nxt < cand:
                cand = nxt
    return cand

def _recompute_fu_map(ctx: Context, rows, by_sender_fu: Dict[str, List[dict]]):
    by_sender_fu.clear()
    for r in rows:
        if not _due_followup(ctx, r): continue
        sid = (gv(ctx.COLMAP, r, "SenderID") or gv(ctx.COLMAP, r, "senderid") or "").strip()
        if not sid: continue
        by_sender_fu.setdefault(sid, []).append(r)
    for sid in by_sender_fu:
        by_sender_fu[sid].sort(key=lambda rr: (gi(ctx.COLMAP, rr, "follow_up_stage",0), _parse_iso(gv(ctx.COLMAP, rr, "next_followup_at")) or datetime.max))

def build_pools(ctx: Context, active: List[SenderState]) -> Pools:
    # follow-ups due now by sender
    by_sender_fu: Dict[str, List[dict]] = {}
    _recompute_fu_map(ctx, ctx.rows, by_sender_fu)

    # new_pool (fresh prospects)
    new_pool = []
    for r in ctx.rows:
        status = gs(ctx.COLMAP, r, "status")
        attempts = gi(ctx.COLMAP, r, "attempts", 0)
        if _is_blocked(status): continue
        if attempts > 0 or status not in {"new","drafted","approved"}: continue
        to_email_raw = (gv(ctx.COLMAP, r, "to_email") or gv(ctx.COLMAP, r, "email") or "").strip()
        if not to_email_raw: continue
        if not re_email_ok(to_email_raw): continue
        new_pool.append(r)

    fu_due_count = sum(len(v) for v in by_sender_fu.values())
    fu_next_dt = _not_yet_due_followup_min_dt(ctx, ctx.rows)

    print("[PLAN-INPUTS]",
          f"new_pool={len(new_pool)}",
          f"followups_due_now={fu_due_count}",
          f"global_remaining={ctx.GLOBAL_DAILY_CAP if ctx.args.limit is None else min(ctx.GLOBAL_DAILY_CAP, ctx.args.limit)}",
          f"allow_wait_secs={ctx.ALLOW_WAIT_SECS}")

    # early cold fallback decision if no work right now
    early_cold_fallback = False
    if fu_due_count == 0 and len(new_pool) == 0:
        if ctx.ALLOW_WAIT_SECS and fu_next_dt:
            wait_s = (fu_next_dt - datetime.now(ctx.tz)).total_seconds()
            if 0 < wait_s <= ctx.ALLOW_WAIT_SECS:
                print(f"[WAIT-FOR-DUE] first_followup_due={_fmt_time(fu_next_dt)} in {wait_s:.0f}s (<= {ctx.ALLOW_WAIT_SECS}s)")
                time.sleep(wait_s)
                _recompute_fu_map(ctx, ctx.rows, by_sender_fu)
                fu_due_count = sum(len(v) for v in by_sender_fu.values())
                print("[PLAN-RESULT]", f"followups_due_now_after_wait={fu_due_count}")
                if fu_due_count == 0 and len(new_pool) == 0:
                    print("[FALLBACK] Mixed idle window after wait: attempting cold fallback.")
                    early_cold_fallback = True
            else:
                print(f"[FALLBACK] No follow-ups due within {ctx.ALLOW_WAIT_SECS/60:.1f}m (next at {_fmt_time(fu_next_dt)}); attempting cold fallback.")
                early_cold_fallback = True
        else:
            print("[FALLBACK] No new & no followups due now; attempting cold fallback.")
            early_cold_fallback = True

    ctx.early_cold_fallback = early_cold_fallback
    return Pools(by_sender_fu=by_sender_fu, new_pool=new_pool, fu_due_count=fu_due_count, fu_next_dt=fu_next_dt)

# helpers
import re as _re
def re_email_ok(s: str) -> bool:
    return bool(_re.match(r"^[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}$", s))
