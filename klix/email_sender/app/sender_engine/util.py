# app/sender_engine/util.py
import os, re, yaml, random
from datetime import datetime, timedelta
from typing import Any, Dict
from zoneinfo import ZoneInfo

EMAIL_RX = re.compile(r"^[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}$")

def clean_email(addr: str):
    if not addr: return None, None
    original = addr.strip()
    a = original.lower()
    while ".." in a: a = a.replace("..", ".")
    a = a.rstrip(".")
    if EMAIL_RX.match(a):
        return (a, f"CLEANED {original} → {a}") if a != original.lower() else (a, None)
    return None, None

def _iso(dt):  return dt.astimezone().isoformat() if isinstance(dt, datetime) else ""
def _parse_iso(s):
    try: return datetime.fromisoformat(str(s).replace("Z",""))
    except: return None
def _is_blocked(status): return str(status or "").strip().lower() in {"replied","bounced","unsubscribed","do_not_contact","closed"}

def _in_business_hours(now_local, start_hhmm: str, end_hhmm: str, weekdays: list[int]):
    wd = now_local.isoweekday()  # Mon=1..Sun=7
    if weekdays and wd not in set(weekdays): return False
    sh, sm = map(int, start_hhmm.split(":"))
    eh, em = map(int, end_hhmm.split(":"))
    start = now_local.replace(hour=sh, minute=sm, second=0, microsecond=0)
    end   = now_local.replace(hour=eh, minute=em, second=0, microsecond=0)
    return start <= now_local <= end

def next_state_after_send(status, stage, attempts, now):
    FU1_HOURS = int(os.getenv("FU1_HOURS", "72"))
    FU2_DAYS  = int(os.getenv("FU2_DAYS", "6"))
    FU3_DAYS  = int(os.getenv("FU3_DAYS", "12"))
    MAX_ATTEMPTS = int(os.getenv("MAX_ATTEMPTS", "3"))
    status = (status or "").strip().lower()
    stage  = int(stage or 0); attempts = int(attempts or 0)
    if status in {"new","approved","drafted"}:
        return {"status":"sent","attempts":attempts+1,"follow_up_stage":1,"last_sent_at":_iso(now),"next_followup_at":_iso(now+timedelta(hours=FU1_HOURS))}
    if status=="sent":
        if stage==1:
            return {"status":"sent","attempts":attempts+1,"follow_up_stage":2,"last_sent_at":_iso(now),"next_followup_at":_iso(now+timedelta(days=FU2_DAYS))}
        if stage==2:
            if MAX_ATTEMPTS>=4:
                return {"status":"sent","attempts":attempts+1,"follow_up_stage":3,"last_sent_at":_iso(now),"next_followup_at":_iso(now+timedelta(days=FU3_DAYS))}
            return {"status":"closed","attempts":attempts+1,"follow_up_stage":2,"last_sent_at":_iso(now),"next_followup_at":""}
        if stage>=3:
            return {"status":"closed","attempts":attempts+1,"follow_up_stage":stage,"last_sent_at":_iso(now),"next_followup_at":""}
    if attempts+1 >= MAX_ATTEMPTS:
        return {"status":"closed","attempts":attempts+1,"follow_up_stage":stage,"last_sent_at":_iso(now),"next_followup_at":""}
    return {"status":"sent","attempts":attempts+1,"follow_up_stage":max(1,stage),"last_sent_at":_iso(now)}

def load_mail_config(path="config/mail.yaml"):
    def _expand_env(obj):
        if isinstance(obj, dict): return {k: _expand_env(v) for k, v in obj.items()}
        if isinstance(obj, list): return [_expand_env(v) for v in obj]
        if isinstance(obj, str):  return os.path.expandvars(obj)
        return obj
    with open(path, "r", encoding="utf-8") as f:
        raw = yaml.safe_load(f)
    return _expand_env(raw)

def _fmt_time(dt):
    try:
        return dt.astimezone().strftime("%H:%M:%S")
    except Exception:
        return str(dt)

# sheet helpers (respect column map)
def gv(COLMAP: Dict[str,str], r: Dict[str,Any], key: str):
    col = COLMAP.get(key) or key
    return r.get(col)

def gi(COLMAP: Dict[str,str], r: Dict[str,Any], key: str, default=0):
    try:
        v = gv(COLMAP, r, key)
        return int(str(v).strip()) if str(v).strip() != "" else default
    except Exception:
        return default

def gs(COLMAP: Dict[str,str], r: Dict[str,Any], key: str):
    v = gv(COLMAP, r, key)
    return str(v or "").strip().lower()
