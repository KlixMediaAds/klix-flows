# app/sender_engine/config.py
import os
from datetime import datetime
from zoneinfo import ZoneInfo
from dotenv import load_dotenv
from .types import Context
from .util import load_mail_config, _in_business_hours
from lib.sheets import load_sheet_config, connect, list_worksheets, read_emails
from lib.limits import init_db

load_dotenv()

def load_context(args) -> Context:
    sheet_cfg = load_sheet_config()
    mail_cfg  = load_mail_config()
    tz_name = mail_cfg.get("timezone", "America/Toronto")
    tz = ZoneInfo(tz_name)

    bh = (mail_cfg.get("business_hours") or {}) if isinstance(mail_cfg.get("business_hours"), dict) else {}
    BH_START = bh.get("start", "09:00")
    BH_END   = bh.get("end", "17:30")
    BH_DAYS  = bh.get("weekdays", [1,2,3,4,5])

    sending = (mail_cfg.get("sending") or {}) if isinstance(mail_cfg.get("sending"), dict) else {}
    GLOBAL_DAILY_CAP  = int(sending.get("GLOBAL_DAILY_CAP", 100))
    FRIENDLY_FRACTION = float(sending.get("FRIENDLY_FRACTION", 0.40))
    DEDUPE_DAYS = int(sending.get("DEDUPE_DAYS", 30))
    MIN_DELAY   = float(sending.get("MIN_DELAY_SEC", 3))
    MAX_DELAY   = float(sending.get("MAX_DELAY_SEC", 9))
    DRY_RUN     = bool(args.dry_run or sending.get("DRY_RUN", False))
    BOUNCE_THRESHOLD = float(sending.get("BOUNCE_THRESHOLD", 0.12))
    LOOKBACK_N  = int(sending.get("BOUNCE_LOOKBACK", 50))
    warmup_map  = mail_cfg.get("warmup", {}) or {}
    ALLOW_WAIT_SECS = args.allow_wait_secs if args.allow_wait_secs is not None else int(os.getenv("ALLOW_WAIT_SECS", "300"))
    signature_enabled = bool(sending.get("SIGNATURE_ENABLED", True))
    verbose = bool(args.verbose)

    GLOBAL_GAP_SEC = int(os.getenv("GLOBAL_GAP_SEC", "30"))
    PER_SENDER_MIN_GAP_MIN_SEC = int(os.getenv("PER_SENDER_MIN_GAP_MIN_SEC", "720"))
    PER_SENDER_MIN_GAP_MAX_SEC = int(os.getenv("PER_SENDER_MIN_GAP_MAX_SEC", "1500"))
    MAX_PER_HOUR = int(os.getenv("MAX_PER_HOUR", "4"))

    if args.min_delay is not None: MIN_DELAY = float(args.min_delay)
    if args.max_delay is not None: MAX_DELAY = max(float(args.max_delay), MIN_DELAY)

    now_local = datetime.now(tz=tz)

    return Context(
        args=args, sheet_cfg=sheet_cfg, mail_cfg=mail_cfg, tz=tz, now_local=now_local,
        BH_START=BH_START, BH_END=BH_END, BH_DAYS=BH_DAYS,
        GLOBAL_DAILY_CAP=GLOBAL_DAILY_CAP, FRIENDLY_FRACTION=FRIENDLY_FRACTION,
        DEDUPE_DAYS=DEDUPE_DAYS, MIN_DELAY=MIN_DELAY, MAX_DELAY=MAX_DELAY, DRY_RUN=DRY_RUN,
        BOUNCE_THRESHOLD=BOUNCE_THRESHOLD, LOOKBACK_N=LOOKBACK_N, warmup_map=warmup_map,
        ALLOW_WAIT_SECS=ALLOW_WAIT_SECS, signature_enabled=signature_enabled, verbose=verbose,
        GLOBAL_GAP_SEC=GLOBAL_GAP_SEC, PER_SENDER_MIN_GAP_MIN_SEC=PER_SENDER_MIN_GAP_MIN_SEC,
        PER_SENDER_MIN_GAP_MAX_SEC=PER_SENDER_MIN_GAP_MAX_SEC, MAX_PER_HOUR=MAX_PER_HOUR,
    )

def ensure_business_hours(ctx: Context) -> bool:
    return _in_business_hours(ctx.now_local, ctx.BH_START, ctx.BH_END, ctx.BH_DAYS)

def init_datastores(ctx: Context):
    # DB init
    init_db()
    # Sheets connect + cache rows/colmap
    book = connect(ctx.sheet_cfg["spreadsheet_id"])
    list_worksheets(book)
    emails_ws = book.worksheet(ctx.sheet_cfg["tabs"]["emails"])
    rows = read_emails(emails_ws)
    ctx.book = book
    ctx.emails_ws = emails_ws
    ctx.rows = rows
    ctx.COLMAP = ctx.sheet_cfg["columns"]["emails"]
