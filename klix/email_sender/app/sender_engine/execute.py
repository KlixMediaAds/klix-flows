# app/sender_engine/execute.py
import math, os, time, hashlib
from datetime import datetime
from typing import List

from .types import Context, SenderState, Pools
from .util import clean_email, gv, gi, gs, _parse_iso, _iso, _fmt_time, next_state_after_send
from .pacing import Pacer, sent_last_hour
from .mix import build_balanced_labels, pick_next_sender_for
from .subject import Subjector  # subject diversification
from lib.render import render_email
from lib.limits import (
    inc_today_count, already_contacted, can_send_to, record_send, mark_do_not_send,
    reserve_lock, release_lock
)
from lib.bounce import classify_bounce
from lib.sheets import (
    update_followup_state, write_email_note, write_email_send_result,
    flip_prospect_status, log_history, _now_iso
)

LOCK_TTL_SEC = 900  # 15 minutes to prevent near-duplicate sends


# -------- helpers --------
def _sleep_between(ctx: Context, scfg: dict):
    s_min = float(scfg.get("min_delay", ctx.MIN_DELAY))
    s_max = float(scfg.get("max_delay", ctx.MAX_DELAY))
    if s_max < s_min:
        s_max = s_min
    delay = max(1.0, __import__("random").uniform(s_min, s_max))
    print(f"[SLEEP:{scfg['id']}] {delay:.1f}s")
    time.sleep(delay)


def _deterministic_msgid(scfg: dict, *, kind: str, to_email: str, email_id: str | None, extra: str = "") -> str:
    """
    Stable Message-ID so retries/parallel runners don't create dupes.
    Uses sender id + recipient + email_id + kind (+ optional extra).
    """
    dom = (scfg.get("domain") or scfg.get("from_email", "klix.local").split("@")[-1]).strip() or "klix.local"
    basis = f"{scfg.get('id','?')}|{(to_email or '').lower()}|{email_id or ''}|{(kind or '').lower()}|{extra}"
    h = hashlib.blake2b(basis.encode("utf-8"), digest_size=10).hexdigest()  # 20 hex chars
    return f"<{kind.lower()}.{scfg.get('id','x')}.{h}@{dom}>"


def _subject_make(ctx: Context, *, kind: str, stage: int, base_subject: str, body_md: str,
                  to_email: str, sender_cfg: dict, row: dict | None, meta: dict | None) -> str:
    """
    Backward-compatible adapter around Subjector.make() so execute.py works
    with either the 'new' or 'prior' subjector signature.
    """
    try:
        # Newer style (named args)
        return ctx.subjector.make(
            kind=kind, stage=stage, base_subject=base_subject,
            body_md=body_md, to_email=to_email, meta=meta or {}
        )
    except TypeError:
        # Legacy style (base/row/sender)
        return ctx.subjector.make(
            kind=kind, base=base_subject, row=row, sender=sender_cfg,
            stage=stage, to_email=to_email
        )


def _send_one_mail(
    ctx: Context, pacer: Pacer, item_idx: int, total_len: int, kind: str,
    s: SenderState, to_email: str, subject: str, body_md: str, *,
    email_id: str | None, message_id: str
):
    scfg = s.cfg
    mailer = s.mailer
    subj, plain, html = render_email(
        subject, body_md, from_name=scfg["from_name"], signature_enabled=ctx.signature_enabled
    )

    if ctx.DRY_RUN:
        msg_id = message_id or f"<dryrun-{kind.lower()}-{item_idx}@klix>"
        print(f"[DRY_RUN] {item_idx}/{total_len} {kind} to={to_email} via={scfg['id']}  msg={msg_id}")
        pacer.mark_sent(scfg["id"])
        _sleep_between(ctx, scfg)
        return True, msg_id

    try:
        if sent_last_hour(scfg["id"]) >= ctx.MAX_PER_HOUR:
            print(f"[THROTTLE] {scfg['id']} hit {ctx.MAX_PER_HOUR}/hr; pausing 60s")
            time.sleep(60)
        pacer.wait_before_send(scfg["id"])
        msg_id = mailer.send(
            to_email, subj, plain, html,
            in_reply_to=None, references=None, message_id=message_id
        )
        print(f"[SENT] {item_idx}/{total_len} {kind} to={to_email} via={scfg['id']}  msg={msg_id}")
        inc_today_count(scfg["id"], 1)
        pacer.mark_sent(scfg["id"])
        return True, msg_id
    except Exception as err:
        reason = f"{type(err).__name__}: {err}"
        print(f"[ERROR] {kind} {to_email} via={scfg['id']} → {reason}")
        btype = classify_bounce(str(err))
        status = "bounce" if btype == "hard" else "soft_bounce"
        record_send(scfg["id"], to_email, None, "", status=status, reason=reason, meta={"kind": kind.lower()})
        if btype == "hard":
            mark_do_not_send(to_email, days=90)
        _sleep_between(ctx, scfg)
        return False, ""


# ===== Friendlies =====
def _run_friendlies_only(ctx: Context, active: List[SenderState], limit_attempts: int):
    pacer = ctx.pacer
    if os.getenv("SEND_FRIENDLIES_NOW", "0") == "1":
        if ctx.MIN_DELAY < 60:
            ctx.MIN_DELAY = 60
        if ctx.MAX_DELAY < 120:
            ctx.MAX_DELAY = 120

    owned = os.getenv("OWNED_ADDRESSES", "").split(",")
    owned = [a.strip() for a in owned if "@" in a]
    if len(owned) < 2:
        owned = [s.cfg["from_email"] for s in active]

    # pull any previously hard-bounced recipients from DB
    def _addresses_with_hard_bounce():
        import sqlite3
        bad = set()
        try:
            with sqlite3.connect("send_state.db") as con:
                for (rcpt,) in con.execute("SELECT DISTINCT to_email FROM sends WHERE status='bounce'"):
                    if rcpt and '@' in rcpt:
                        bad.add(rcpt.lower())
        except Exception:
            pass
        return bad

    bad_rcpts = _addresses_with_hard_bounce()

    items = []
    # preferred planner
    try:
        from lib.friendly import build_friendly_plan
        from lib.friendly import HistoryItem  # noqa: F401
        recent_hist = _get_recent_history_for_friendlies(ctx)
        tzlabel = ctx.tz.key
        target_threads = max(1, math.ceil(limit_attempts / 3))
        plan = build_friendly_plan(
            owned_addresses=owned,
            target_threads=target_threads,
            min_thread_len=2, max_thread_len=4,
            tz=tzlabel, biz_hours=(9, 17),
            max_per_mailbox_today=8,
            pair_cooldown_hours=24, mailbox_cooldown_hours=2,
            recent_history=recent_hist,
        )
        now = datetime.now(ctx.tz)
        force_now = os.getenv("SEND_FRIENDLIES_NOW", "0") == "1"
        planned = len(plan)
        due_now = later = 0
        for p in plan:
            if len(items) >= limit_attempts:
                break
            if force_now or p.when <= now:
                if (p.to_email or "").lower() in bad_rcpts:
                    continue
                items.append({
                    "from": p.from_email, "to": p.to_email,
                    "subject": p.subject, "body_md": p.body, "is_reply": p.is_reply
                })
                due_now += 1
            else:
                later += 1
        print(f"[FRIENDLY] planned={planned} due_now={due_now} later={later} force_now={force_now}")
    except Exception:
        # legacy fallback
        try:
            from lib.friendly import plan_friendly_pairs, generate_friendly_email
            pairs = plan_friendly_pairs(owned, limit_attempts)
            for frm, to in pairs:
                if (to or "").lower() in bad_rcpts:
                    continue
                subj, body = generate_friendly_email(frm, to)
                items.append({"from": frm, "to": to, "subject": subj, "body_md": body, "is_reply": False})
        except Exception:
            print("[EXIT] No friendly planner available.")
            return

    sentN = errN = 0
    idx = 0
    from_map = {s.cfg["from_email"].lower(): s for s in active}

    for it in items:
        idx += 1
        s = from_map.get(it["from"].lower())
        if not s:
            dom = it["from"].split("@")[-1].lower()
            s = next((x for x in active if x.cfg["from_email"].split("@")[-1].lower() == dom and x.remaining > 0), None)
            if not s:
                s = next((x for x in active if x.remaining > 0), None)
        if not s or s.remaining <= 0:
            print(f"[SKIP] no capacity on sender for friendly from={it['from']}")
            continue
        if s._used_friendly >= s._cap_friendly:
            print(f"[SKIP] friendly cap reached for {s.cfg['id']} ({s._used_friendly}/{s._cap_friendly})")
            continue

        # Subject diversification for friendlies (respects config)
        subj_out = _subject_make(
            ctx, kind="FRIENDLY", stage=0, base_subject=it["subject"], body_md=it["body_md"],
            to_email=it["to"], sender_cfg=s.cfg, row=None, meta={"domain": s.cfg.get("domain")}
        )

        # Per-recipient lock
        holder = f"{s.cfg['id']}:friendly:{it['to'].lower()}"
        if not reserve_lock(it["to"], holder, ttl_sec=LOCK_TTL_SEC):
            print(f"[LOCK] FRIENDLY locked, skipping to={it['to']}")
            record_send(s.cfg["id"], it["to"], None, None, status="skipped", reason="locked", meta={"kind": "friendly"})
            continue

        msg_id_hint = _deterministic_msgid(s.cfg, kind="FRIENDLY", to_email=it["to"], email_id=None, extra=it["from"])
        try:
            ok, msg_id = _send_one_mail(ctx, pacer, idx, len(items), "FRIENDLY", s, it["to"], subj_out, it["body_md"],
                                        email_id=None, message_id=msg_id_hint)
            if ok:
                record_send(s.cfg["id"], it["to"], None, msg_id, status="friendly", reason="",
                            meta={"subject": subj_out, "kind": "friendly"})
                sentN += 1
                s.remaining = max(0, s.remaining - 1)
                s._used_friendly += 1
            else:
                errN += 1
        finally:
            release_lock(it["to"], holder)

    print(f"[DONE:FRIENDLIES] sent={sentN} errors={errN} dry_run={ctx.DRY_RUN}")


def _get_recent_history_for_friendlies(ctx: Context, max_days=7):
    try:
        import sqlite3, time
        with sqlite3.connect("send_state.db") as con:
            con.row_factory = sqlite3.Row
            cols = {row["name"] for row in con.execute("PRAGMA table_info('sends')")}
            recip_col = "to_email" if "to_email" in cols else ("recipient" if "recipient" in cols else None)
            if recip_col is None or "sender_id" not in cols or "ts" not in cols:
                print("[WARN] sends table missing expected columns; returning empty friendly history")
                return []
            cutoff_ts = int(time.time() - max_days * 86400)
            rows = list(con.execute(
                f"""SELECT ts, sender_id, {recip_col} AS rcpt, status
                    FROM sends
                    WHERE ts >= ? AND status IN ('sent','friendly','soft_bounce','bounce')""",
                (cutoff_ts,)
            ))
        id_to_from = {(s.get("id") or "").strip(): (s.get("from_email") or "").strip().lower()
                      for s in (ctx.mail_cfg.get("senders") or [])}
        tzinfo = ctx.tz
        hist = []
        for r in rows:
            frm = id_to_from.get((r["sender_id"] or "").strip())
            to = (r["rcpt"] or "").strip().lower()
            if not frm or "@" not in frm or "@" not in to:
                continue
            when = datetime.fromtimestamp(int(r["ts"]), tzinfo)
            from lib.friendly import HistoryItem
            hist.append(HistoryItem(when=when, from_email=frm, to_email=to,
                                    thread_key=f"{frm}->{to}", is_reply=False))
        print(f"[FRIENDLY:HIST] loaded {len(hist)} items from last {max_days}d")
        return hist
    except Exception as e:
        print(f"[WARN] history lookup failed: {e}")
        return []


# ===== Cold only =====
def _run_cold_only(ctx: Context, active: List[SenderState], new_pool: list, limit_attempts: int):
    pacer = ctx.pacer
    queue = []
    changed = True
    global_rem = min(limit_attempts, sum(s.remaining for s in active))
    pool = list(new_pool)

    while global_rem > 0 and pool and changed:
        changed = False
        for s in active:
            if global_rem <= 0 or not pool:
                break
            if s.remaining <= 0:
                continue
            if s._used_cold >= s._cap_cold:
                continue
            r = pool.pop(0)
            queue.append({"sender": s, "row": r})
            global_rem -= 1
            changed = True

    if not queue:
        print("[EXIT] No cold emails available or capacity is zero.")
        return

    print(f"[PLAN-RESULT] cold_enqueued={len(queue)} (from new_pool={len(new_pool)})")

    sentN = skipN = errN = 0
    for idx, item in enumerate(queue, 1):
        s = item["sender"]
        scfg = s.cfg
        r = item["row"]

        email_id = str(gv(ctx.COLMAP, r, "id") or "").strip()
        to_raw = (gv(ctx.COLMAP, r, "to_email") or gv(ctx.COLMAP, r, "email") or "").strip()
        subject_base = (gv(ctx.COLMAP, r, "subject") or "").strip()
        body_md = (gv(ctx.COLMAP, r, "body_md") or "")
        status_before = (gv(ctx.COLMAP, r, "status") or "")
        stage_before = gi(ctx.COLMAP, r, "follow_up_stage", 0)

        meta = {
            "first_name": gv(ctx.COLMAP, r, "first_name"),
            "company": gv(ctx.COLMAP, r, "company"),
            "domain": scfg.get("domain"),
        }
        subject = _subject_make(
            ctx, kind="COLD", stage=0, base_subject=subject_base,
            body_md=body_md, to_email=to_raw, sender_cfg=scfg, row=r, meta=meta
        )

        if not (email_id and to_raw and subject and body_md):
            print(f"[SKIP] Missing fields EmailID={email_id} -> to={to_raw}")
            if not ctx.DRY_RUN:
                write_email_note(ctx.emails_ws, ctx.sheet_cfg, email_id, f"SKIP {_now_iso()} missing fields")
            skipN += 1
            continue

        last_sent_at = gv(ctx.COLMAP, r, "last_sent_at")
        if last_sent_at:
            dt = _parse_iso(last_sent_at)
            if dt and (datetime.now(ctx.tz) - dt).total_seconds() < 60:
                print(f"[SKIP] Idempotency guard EmailID={email_id}")
                skipN += 1
                continue

        to_email, note = clean_email(to_raw)
        if note and not ctx.DRY_RUN:
            write_email_note(ctx.emails_ws, ctx.sheet_cfg, email_id, f"{_now_iso()} {note}")
        if not to_email:
            print(f"[ERROR] EmailID={email_id} invalid recipient: {to_raw}")
            if not ctx.DRY_RUN:
                write_email_note(ctx.emails_ws, ctx.sheet_cfg, email_id, f"ERROR {_now_iso()} invalid recipient: {to_raw}")
            skipN += 1
            continue

        if not can_send_to(to_email) or already_contacted(to_email, ctx.DEDUPE_DAYS):
            print(f"[SKIP] do-not-send or dedupe EmailID={email_id} {to_email}")
            if not ctx.DRY_RUN:
                write_email_note(ctx.emails_ws, ctx.sheet_cfg, email_id, f"SKIP {_now_iso()} rate/dupe window")
            skipN += 1
            continue

        holder = f"{scfg['id']}:{email_id or to_email}"
        if not reserve_lock(to_email, holder, ttl_sec=LOCK_TTL_SEC):
            print(f"[LOCK] COLD locked, skipping EmailID={email_id} to={to_email}")
            record_send(scfg["id"], to_email, (gv(ctx.COLMAP, r, "prospect_id") or gv(ctx.COLMAP, r, "lead_id") or None),
                        None, status="skipped", reason="locked", meta={"kind": "cold"})
            skipN += 1
            continue

        msg_id_hint = _deterministic_msgid(scfg, kind="COLD", to_email=to_email, email_id=email_id)
        try:
            ok, msg_id = _send_one_mail(
                ctx, pacer, idx, len(queue), "COLD", s, to_email, subject, body_md,
                email_id=email_id, message_id=msg_id_hint
            )
            if ok:
                write_email_send_result(ctx.emails_ws, ctx.sheet_cfg, email_id, msg_id)
                update_followup_state(
                    ctx.emails_ws, ctx.sheet_cfg, email_id,
                    {"SenderID": scfg["id"], "FirstSentAt": _iso(datetime.now(ctx.tz))}
                )
                flip_prospect_status(
                    ctx.book, ctx.sheet_cfg,
                    (gv(ctx.COLMAP, r, "prospect_id") or gv(ctx.COLMAP, r, "lead_id") or email_id),
                    "SENT"
                )
                attempts = gi(ctx.COLMAP, r, "attempts", 0)
                next_changes = next_state_after_send(str(status_before), int(stage_before), int(attempts), datetime.now(ctx.tz))
                update_followup_state(ctx.emails_ws, ctx.sheet_cfg, email_id, next_changes)
                log_history(
                    ctx.book, (gv(ctx.COLMAP, r, "lead_id") or email_id), "sent_initial",
                    stage_before, next_changes.get("follow_up_stage", stage_before),
                    status_before, next_changes.get("status", status_before),
                    msg_id, f"cold via {scfg['id']}"
                )
                record_send(
                    scfg["id"], to_email,
                    (gv(ctx.COLMAP, r, "prospect_id") or gv(ctx.COLMAP, r, "lead_id") or None),
                    msg_id, status="sent", reason="", meta={"subject": subject, "kind": "cold"}
                )
                s._used_cold += 1
                s.remaining = max(0, s.remaining - 1)
                sentN += 1
            else:
                errN += 1
        finally:
            release_lock(to_email, holder)

        _sleep_between(ctx, scfg)

    print(f"[DONE:COLD] sent={sentN} skipped={skipN} errors={errN} dry_run={ctx.DRY_RUN}")


# ===== Mixed (true 60:40 — cold bucket = followups+new) =====
def _run_mixed(ctx: Context, active: List[SenderState], pools: Pools, ratio_spec: str):
    pacer = ctx.pacer
    try:
        cold_pct, friendly_pct = [int(x) for x in ratio_spec.split(":")]
    except Exception:
        cold_pct, friendly_pct = 60, 40

    cold_share = max(0.0, cold_pct / max(1, (cold_pct + friendly_pct)))
    friendly_share = 1.0 - cold_share

    total_cap = min(
        min(ctx.args.limit, ctx.GLOBAL_DAILY_CAP) if ctx.args.limit is not None else ctx.GLOBAL_DAILY_CAP,
        sum(s.remaining for s in active)
    )
    if total_cap <= 0:
        print("[EXIT] No capacity (sum remaining <= 0).")
        return

    cold_slots = max(0, round(total_cap * cold_share))
    friendly_slots = max(0, total_cap - cold_slots)

    # 1) Followups first (consume cold)
    fu_queue = []
    fu_pool = {sid: lst[:] for sid, lst in pools.by_sender_fu.items()}
    changed = True
    while cold_slots > 0 and changed:
        changed = False
        for s in active:
            if cold_slots <= 0:
                break
            if s.remaining <= 0:
                continue
            sid = s.cfg["id"]
            lst = fu_pool.get(sid, [])
            if lst:
                r = lst.pop(0)
                fu_queue.append({"sender": s, "row": r})
                cold_slots -= 1
                changed = True

    # 2) New helps fill remaining cold slots (respect per-sender cold caps)
    new_queue = []
    changed = True
    pool = list(pools.new_pool)
    while cold_slots > 0 and pool and changed:
        changed = False
        for s in active:
            if cold_slots <= 0 or not pool:
                break
            if s.remaining <= 0:
                continue
            if s._used_cold >= s._cap_cold:
                continue
            r = pool.pop(0)
            new_queue.append({"sender": s, "row": r})
            cold_slots -= 1
            changed = True

    # 3) Friendlies
    friendly_items = []
    if friendly_slots > 0:
        owned = [s.cfg["from_email"] for s in active]
        bad_rcpts = _addresses_with_hard_bounce()
        if os.getenv("SEND_FRIENDLIES_NOW", "0") == "1":
            if ctx.MIN_DELAY < 60:
                ctx.MIN_DELAY = 60
            if ctx.MAX_DELAY < 120:
                ctx.MAX_DELAY = 120
        try:
            from lib.friendly import build_friendly_plan
            recent_hist = _get_recent_history_for_friendlies(ctx)
            tzlabel = ctx.tz.key
            target_threads = max(1, math.ceil(friendly_slots / 3))
            plan = build_friendly_plan(
                owned_addresses=owned,
                target_threads=target_threads,
                min_thread_len=2, max_thread_len=4,
                tz=tzlabel, biz_hours=(9, 17),
                max_per_mailbox_today=8,
                pair_cooldown_hours=24, mailbox_cooldown_hours=2,
                recent_history=recent_hist,
            )
            now = datetime.now(ctx.tz)
            force_now = os.getenv("SEND_FRIENDLIES_NOW", "0") == "1"
            for p in plan:
                if len(friendly_items) >= friendly_slots:
                    break
                if force_now or p.when <= now:
                    if (p.to_email or "").lower() in bad_rcpts:
                        continue
                    friendly_items.append({
                        "from": p.from_email, "to": p.to_email,
                        "subject": p.subject, "body_md": p.body, "is_reply": p.is_reply
                    })
            print(f"[FRIENDLY:TAIL] planned={len(plan)} enqueued={len(friendly_items)} force_now={force_now}")
        except Exception:
            try:
                from lib.friendly import plan_friendly_pairs, generate_friendly_email
                pairs = plan_friendly_pairs(owned, friendly_slots)
                for frm, to in pairs:
                    if (to or "").lower() in bad_rcpts:
                        continue
                    subj, body = generate_friendly_email(frm, to)
                    friendly_items.append({"from": frm, "to": to, "subject": subj, "body_md": body, "is_reply": False})
            except Exception:
                pass

    # Interleave labels C/F across cold_items and friendly_items
    cold_items = fu_queue + new_queue
    total_len = len(cold_items) + len(friendly_items)
    labels = build_balanced_labels(total_len, len(cold_items), len(friendly_items))

    # cursors + state
    ci = fi = 0
    from_map = {s.cfg["from_email"].lower(): s for s in active}
    last_sender_id = None
    sent_cold = sent_friendly = skipped_total = errN = 0
    idx = 0

    for lab in labels:
        idx += 1
        if lab == 'C' and ci >= len(cold_items):
            lab = 'F'
        elif lab == 'F' and fi >= len(friendly_items):
            lab = 'C'
        if lab == 'C' and ci >= len(cold_items):
            continue
        if lab == 'F' and fi >= len(friendly_items):
            continue

        if lab == 'C':
            item = cold_items[ci]
            ci += 1
            s = item["sender"]
            scfg = s.cfg
            r = item["row"]
            kind = "FOLLOWUP" if gi(ctx.COLMAP, r, "follow_up_stage", 0) >= 1 else "COLD"
            email_id = str((gv(ctx.COLMAP, r, "id") or "")).strip()
            to_raw = (gv(ctx.COLMAP, r, "to_email") or gv(ctx.COLMAP, r, "email") or "").strip()
            subject_base = (gv(ctx.COLMAP, r, "subject") or "").strip()
            body_md = (gv(ctx.COLMAP, r, "body_md") or "")
            status_before = (gv(ctx.COLMAP, r, "status") or "")
            stage_before = gi(ctx.COLMAP, r, "follow_up_stage", 0)
            assigned_sid = (gv(ctx.COLMAP, r, "SenderID") or gv(ctx.COLMAP, r, "senderid") or "").strip()

            meta = {
                "first_name": gv(ctx.COLMAP, r, "first_name"),
                "company": gv(ctx.COLMAP, r, "company"),
                "domain": scfg.get("domain"),
            }
            subject = _subject_make(
                ctx, kind=kind, stage=int(stage_before or 0), base_subject=subject_base,
                body_md=body_md, to_email=to_raw, sender_cfg=scfg, row=r, meta=meta
            )

            s2 = pick_next_sender_for('C', active, last_sender_id) or s
            s = s2
            scfg = s.cfg

            if not (email_id and to_raw and subject and body_md):
                print(f"[SKIP] Missing fields EmailID={email_id} -> to={to_raw}")
                if not ctx.DRY_RUN:
                    write_email_note(ctx.emails_ws, ctx.sheet_cfg, email_id, f"SKIP {_now_iso()} missing fields")
                skipped_total += 1
                continue

            last_sent_at = gv(ctx.COLMAP, r, "last_sent_at")
            if last_sent_at:
                dt = _parse_iso(last_sent_at)
                if dt and (datetime.now(ctx.tz) - dt).total_seconds() < 60:
                    print(f"[SKIP] Idempotency guard EmailID={email_id}")
                    skipped_total += 1
                    continue

            to_email, note = clean_email(to_raw)
            if note and not ctx.DRY_RUN:
                write_email_note(ctx.emails_ws, ctx.sheet_cfg, email_id, f"{_now_iso()} {note}")
            if not to_email:
                print(f"[ERROR] EmailID={email_id} invalid recipient: {to_raw}")
                if not ctx.DRY_RUN:
                    write_email_note(ctx.emails_ws, ctx.sheet_cfg, email_id, f"ERROR {_now_iso()} invalid recipient: {to_raw}")
                skipped_total += 1
                continue

            if not can_send_to(to_email) or already_contacted(to_email, ctx.DEDUPE_DAYS):
                print(f"[SKIP] do-not-send or dedupe EmailID={email_id} {to_email}")
                if not ctx.DRY_RUN:
                    write_email_note(ctx.emails_ws, ctx.sheet_cfg, email_id, f"SKIP {_now_iso()} rate/dupe window")
                skipped_total += 1
                continue

            if kind == "FOLLOWUP" and assigned_sid and assigned_sid != scfg["id"]:
                alt = next((x for x in active if x.cfg["id"] == assigned_sid and x.remaining > 0), None)
                if alt:
                    s = alt
                    scfg = alt.cfg
                else:
                    print(f"[SKIP] FU needs sender={assigned_sid} but unavailable/capped")
                    skipped_total += 1
                    continue

            if kind == "COLD" and s._used_cold >= s._cap_cold:
                print(f"[SKIP] cold cap reached for {scfg['id']} ({s._used_cold}/{s._cap_cold})")
                skipped_total += 1
                continue

            holder = f"{scfg['id']}:{email_id or to_email}"
            if not reserve_lock(to_email, holder, ttl_sec=LOCK_TTL_SEC):
                print(f"[LOCK] {kind} locked, skipping EmailID={email_id} to={to_email}")
                record_send(
                    scfg["id"], to_email, (gv(ctx.COLMAP, r, "prospect_id") or gv(ctx.COLMAP, r, "lead_id") or None),
                    None, status="skipped", reason="locked", meta={"kind": kind.lower()}
                )
                skipped_total += 1
                continue

            msg_id_hint = _deterministic_msgid(scfg, kind=kind, to_email=to_email, email_id=email_id)
            try:
                ok, msg_id = _send_one_mail(
                    ctx, pacer, idx, total_len, kind, s, to_email, subject, body_md,
                    email_id=email_id, message_id=msg_id_hint
                )
                if ok:
                    write_email_send_result(ctx.emails_ws, ctx.sheet_cfg, email_id, msg_id)
                    if kind == "COLD" and not assigned_sid:
                        update_followup_state(
                            ctx.emails_ws, ctx.sheet_cfg, email_id,
                            {"SenderID": scfg["id"], "FirstSentAt": _iso(datetime.now(ctx.tz))}
                        )
                    flip_prospect_status(
                        ctx.book, ctx.sheet_cfg,
                        (gv(ctx.COLMAP, r, "prospect_id") or gv(ctx.COLMAP, r, "lead_id") or email_id),
                        "SENT"
                    )
                    attempts = gi(ctx.COLMAP, r, "attempts", 0)
                    next_changes = next_state_after_send(
                        str(status_before), int(stage_before), int(attempts), datetime.now(ctx.tz)
                    )
                    update_followup_state(ctx.emails_ws, ctx.sheet_cfg, email_id, next_changes)
                    action = "sent_initial" if kind == "COLD" else f"sent_fu{stage_before}"
                    log_history(
                        ctx.book, (gv(ctx.COLMAP, r, "lead_id") or email_id), action,
                        stage_before, next_changes.get("follow_up_stage", stage_before),
                        status_before, next_changes.get("status", status_before),
                        msg_id, f"{kind.lower()} via {scfg['id']}"
                    )
                    record_send(
                        scfg["id"], to_email,
                        (gv(ctx.COLMAP, r, "prospect_id") or gv(ctx.COLMAP, r, "lead_id") or None),
                        msg_id, status="sent", reason="", meta={"subject": subject, "kind": kind.lower()}
                    )
                    if kind == "COLD":
                        s._used_cold += 1
                    s.remaining = max(0, s.remaining - 1)
                    last_sender_id = scfg["id"]
                    sent_cold += 1
                else:
                    errN += 1
            finally:
                release_lock(to_email, holder)

            _sleep_between(ctx, scfg)

        else:  # 'F'
            it = friendly_items[fi]
            fi += 1
            frm = it["from"]; to = it["to"]; subj = it["subject"]; body = it["body_md"]
            s = from_map.get(frm.lower())
            if not s:
                dom = frm.split("@")[-1].lower()
                s = next((x for x in active if x.cfg["from_email"].split("@")[-1].lower() == dom and x.remaining > 0), None)
                if not s:
                    s = next((x for x in active if x.remaining > 0), None)
            if not s or s.remaining <= 0:
                print(f"[SKIP] no capacity for friendly from={frm}")
                skipped_total += 1
                continue
            if s._used_friendly >= s._cap_friendly:
                print(f"[SKIP] friendly cap reached for {s.cfg['id']} ({s._used_friendly}/{s._cap_friendly})")
                skipped_total += 1
                continue
            s2 = pick_next_sender_for('F', active, last_sender_id) or s
            s = s2

            subj_out = _subject_make(
                ctx, kind="FRIENDLY", stage=0, base_subject=subj,
                body_md=body, to_email=to, sender_cfg=s.cfg, row=None, meta={"domain": s.cfg.get("domain")}
            )

            holder = f"{s.cfg['id']}:friendly:{to.lower()}"
            if not reserve_lock(to, holder, ttl_sec=LOCK_TTL_SEC):
                print(f"[LOCK] FRIENDLY locked, skipping to={to}")
                record_send(s.cfg["id"], to, None, None, status="skipped", reason="locked", meta={"kind": "friendly"})
                skipped_total += 1
                continue

            msg_id_hint = _deterministic_msgid(s.cfg, kind="FRIENDLY", to_email=to, email_id=None, extra=frm)
            try:
                ok, msg_id = _send_one_mail(ctx, pacer, idx, total_len, "FRIENDLY", s, to, subj_out, body,
                                            email_id=None, message_id=msg_id_hint)
                if ok:
                    record_send(s.cfg["id"], to, None, msg_id, status="friendly", reason="",
                                meta={"subject": subj_out, "kind": "friendly"})
                    s.remaining = max(0, s.remaining - 1)
                    s._used_friendly += 1
                    last_sender_id = s.cfg["id"]
                    sent_friendly += 1
                else:
                    errN += 1
            finally:
                release_lock(to, holder)

            _sleep_between(ctx, s.cfg)

    print(f"[DONE:MIXED] target (cold:friendly)={cold_pct}:{friendly_pct} | achieved C={sent_cold} F={sent_friendly} "
          f"| skipped={skipped_total} errors={errN} dry_run={ctx.DRY_RUN}")


def _addresses_with_hard_bounce():
    import sqlite3
    bad = set()
    try:
        with sqlite3.connect("send_state.db") as con:
            for (rcpt,) in con.execute("SELECT DISTINCT to_email FROM sends WHERE status='bounce'"):
                if rcpt and '@' in rcpt:
                    bad.add(rcpt.lower())
    except Exception:
        pass
    return bad


def dispatch(ctx: Context, active: List[SenderState], pools: Pools):
    # create pacer for this run
    from .pacing import Pacer
    ctx.pacer = Pacer(ctx)

    # instantiate Subjector (robust even if subjector block is missing)
    ctx.subjector = Subjector(ctx)

    global_remaining = ctx.GLOBAL_DAILY_CAP
    if ctx.args.limit is not None:
        global_remaining = min(global_remaining, int(ctx.args.limit))
    if ctx.args.drip_per_run is not None:
        global_remaining = min(global_remaining, max(0, int(ctx.args.drip_per_run)))

    if pools and ctx.early_cold_fallback and ctx.args.mode == "mixed":
        print("[DISPATCH] Early cold fallback engaged under mixed mode.")
        _run_cold_only(ctx, active, pools.new_pool, min(global_remaining, sum(s.remaining for s in active)))
        return

    if ctx.args.mode == "friendlies":
        _run_friendlies_only(ctx, active, min(global_remaining, sum(s.remaining for s in active)))
    elif ctx.args.mode == "cold":
        _run_cold_only(ctx, active, pools.new_pool, min(global_remaining, sum(s.remaining for s in active)))
    else:
        _run_mixed(ctx, active, pools, ctx.args.ratio)
