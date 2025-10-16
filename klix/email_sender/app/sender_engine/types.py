# app/sender_engine/types.py
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List, Optional
from zoneinfo import ZoneInfo

@dataclass
class Context:
    args: Any
    sheet_cfg: Dict[str, Any]
    mail_cfg: Dict[str, Any]
    tz: ZoneInfo
    now_local: datetime

    # business hours
    BH_START: str
    BH_END: str
    BH_DAYS: List[int]

    # knobs
    GLOBAL_DAILY_CAP: int
    FRIENDLY_FRACTION: float
    DEDUPE_DAYS: int
    MIN_DELAY: float
    MAX_DELAY: float
    DRY_RUN: bool
    BOUNCE_THRESHOLD: float
    LOOKBACK_N: int
    warmup_map: Dict[str, List[int]]
    ALLOW_WAIT_SECS: int
    signature_enabled: bool
    verbose: bool

    # pacing
    GLOBAL_GAP_SEC: int
    PER_SENDER_MIN_GAP_MIN_SEC: int
    PER_SENDER_MIN_GAP_MAX_SEC: int
    MAX_PER_HOUR: int

    # runtime objects (populated by init_datastores)
    book: Any = None
    emails_ws: Any = None
    rows: List[Dict[str, Any]] = None
    COLMAP: Dict[str, str] = None
    pacer: Any = None

    # advisory
    early_cold_fallback: bool = False

@dataclass
class SenderState:
    cfg: Dict[str, Any]
    mailer: Any
    today: int
    daily_cap: int
    remaining: int
    _cap_total: int
    _cap_friendly: int
    _cap_cold: int
    _used_friendly: int
    _used_cold: int
    _bounce_rate: float
    _day_index: int

@dataclass
class Pools:
    by_sender_fu: Dict[str, List[Dict[str, Any]]]
    new_pool: List[Dict[str, Any]]
    fu_due_count: int
    fu_next_dt: Optional[datetime]
