# lib/friendly.py
# Friendly traffic planner: cross-domain bias, cooldowns, human-ish threads

from __future__ import annotations
import random, math, re
from dataclasses import dataclass
from collections import defaultdict, Counter, deque
from datetime import datetime, timedelta, time as dtime
from zoneinfo import ZoneInfo
from itertools import product
from typing import List, Dict, Tuple, Optional

UTC = ZoneInfo("UTC")

# ---------------------------------------
# Types
# ---------------------------------------
@dataclass(frozen=True)
class Pair:
    from_email: str
    to_email: str
    cross_domain: bool

@dataclass
class HistoryItem:
    when: datetime
    from_email: str
    to_email: str
    thread_key: str
    is_reply: bool = False

@dataclass
class PlannedEmail:
    when: datetime
    from_email: str
    to_email: str
    subject: str
    body: str
    thread_key: str
    is_reply: bool
    step_index: int  # 0 = opener, 1..N = replies


# ---------------------------------------
# Helpers
# ---------------------------------------
EMAIL_RX = re.compile(r"^[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}$")

def _domain(addr: str) -> str:
    return (addr or "").split("@")[-1].lower().strip()

def _now_tz(tz: str) -> datetime:
    return datetime.now(ZoneInfo(tz))

def _weekday(dt: datetime) -> int:
    return dt.weekday()  # Mon=0..Sun=6

def _is_weekend(dt: datetime) -> bool:
    return _weekday(dt) >= 5

def _in_biz_hours(dt: datetime, biz_hours: Tuple[int, int]) -> bool:
    start, end = biz_hours
    return start <= dt.hour < end and not _is_weekend(dt)

def _jitter_minutes(n: int) -> timedelta:
    if n <= 0: return timedelta(0)
    return timedelta(minutes=random.randint(-n, n))

def _cap(n: int, lo: int, hi: int) -> int:
    return max(lo, min(hi, n))

def _normalize_addrs(addrs: List[str]) -> List[str]:
    out, seen = [], set()
    for a in addrs or []:
        if not a or "@" not in a: continue
        a = a.strip().lower()
        if EMAIL_RX.match(a) and a not in seen:
            out.append(a); seen.add(a)
    return out

def _aware_utc(dt: datetime) -> datetime:
    if isinstance(dt, datetime):
        return dt if dt.tzinfo is not None else dt.replace(tzinfo=UTC)
    return datetime.now(UTC)

def _snap_to_biz(dt: datetime, tz: str, biz_hours: Tuple[int, int]) -> datetime:
    """
    If dt is outside business hours or weekend, push to next valid business start (with slight jitter).
    """
    tzinfo = ZoneInfo(tz)
    local = dt.astimezone(tzinfo)
    start_h, end_h = biz_hours
    if _is_weekend(local) or not _in_biz_hours(local, biz_hours):
        # next weekday morning
        days = 0
        if _is_weekend(local):
            # move to Monday
            days = (7 - _weekday(local)) % 7 or 1
        elif local.hour >= end_h:
            days = 1
        next_day = (local + timedelta(days=days)).replace(hour=start_h, minute=0, second=0, microsecond=0)
        local = next_day + _jitter_minutes(17)
    return local.astimezone(UTC)

def _schedule_after(
    start_after: datetime,
    tz: str,
    biz_hours: Tuple[int,int],
    min_delay_min: int,
    max_delay_min: int
) -> datetime:
    base = start_after + timedelta(minutes=random.randint(min_delay_min, max_delay_min))
    return _snap_to_biz(base, tz, biz_hours)

# ---------------------------------------
# Pair selection with cooldowns
# ---------------------------------------
def plan_friendly_pairs(
    owned_addresses: List[str],
    target_count: int,
    *,
    recent_history: List[HistoryItem] | None = None,
    pair_cooldown_hours: int = 24,
    mailbox_cooldown_hours: int = 2,
    domain_pair_cooldown_min: int = 20,
    exclude_addresses: Optional[set[str]] = None,
) -> List[Pair]:
    """
    Picks unique (from,to) pairs favoring cross-domain.
    Cooldowns:
      - pair_cooldown_hours: block reusing identical (from,to) too soon
      - mailbox_cooldown_hours: spacing for each 'from' mailbox
      - domain_pair_cooldown_min: spacing for (from_domain -> to_domain)
    """
    addrs = [a for a in _normalize_addrs(owned_addresses) if not (exclude_addresses and a in exclude_addresses)]
    if len(addrs) < 2 or target_count <= 0:
        return []

    now = datetime.now(UTC)
    recent_history = recent_history or []

    # Cooldown maps
    last_pair_send: Dict[Tuple[str, str], datetime] = {}
    last_from_send: Dict[str, datetime] = {}
    last_dom_pair: Dict[Tuple[str, str], datetime] = {}

    for h in recent_history:
        h_when = _aware_utc(h.when)
        f = (h.from_email or "").lower()
        t = (h.to_email or "").lower()
        key = (f, t)
        last_pair_send[key] = max(last_pair_send.get(key, datetime.min.replace(tzinfo=UTC)), h_when)
        last_from_send[f] = max(last_from_send.get(f, datetime.min.replace(tzinfo=UTC)), h_when)
        last_dom_pair[(_domain(f), _domain(t))] = max(last_dom_pair.get((_domain(f), _domain(t)), datetime.min.replace(tzinfo=UTC)), h_when)

    # Bucket by domain
    by_dom: Dict[str, List[str]] = defaultdict(list)
    for a in addrs:
        by_dom[_domain(a)].append(a)
    doms = list(by_dom.keys())

    # Cross-domain candidates first
    candidates: List[Pair] = []
    for i, j in product(range(len(doms)), range(len(doms))):
        if i == j: continue
        for a in by_dom[doms[i]]:
            for b in by_dom[doms[j]]:
                if a == b: continue
                candidates.append(Pair(a, b, cross_domain=True))

    # Fallback to within-domain if cross-domain empty
    if not candidates:
        for d, lst in by_dom.items():
            if len(lst) >= 2:
                for i in range(len(lst)):
                    candidates.append(Pair(lst[i], lst[(i + 1) % len(lst)], cross_domain=False))

    random.shuffle(candidates)

    out: List[Pair] = []
    seen_pairs = set()

    for p in candidates:
        if len(out) >= target_count: break
        key = (p.from_email, p.to_email)
        if key in seen_pairs: continue

        # Cooldown checks
        lp = last_pair_send.get(key)
        if lp and (now - lp).total_seconds() < pair_cooldown_hours * 3600:
            continue
        lf = last_from_send.get(p.from_email.lower())
        if lf and (now - lf).total_seconds() < mailbox_cooldown_hours * 3600:
            continue
        ldp = last_dom_pair.get((_domain(p.from_email), _domain(p.to_email)))
        if ldp and (now - ldp).total_seconds() < domain_pair_cooldown_min * 60:
            continue

        out.append(p)
        seen_pairs.add(key)
        # update short-term memory to avoid immediate repeats during selection
        last_from_send[p.from_email.lower()] = now
        last_dom_pair[(_domain(p.from_email), _domain(p.to_email))] = now

    # If still short, relax cooldowns but keep uniqueness
    if len(out) < target_count:
        for p in candidates:
            if len(out) >= target_count: break
            key = (p.from_email, p.to_email)
            if key in seen_pairs: continue
            out.append(p); seen_pairs.add(key)

    return out[:target_count]

# ---------------------------------------
# Content generator (short, varied, no links)
# ---------------------------------------
_SUBJECT_BITS = [
    "quick q", "tiny thing", "random thought", "one sec", "super quick", "ping",
    "real quick", "tiny favor", "btw", "note", "follow-up", "checking in",
]
_OPENERS = [
    "Hey {name},",
    "Hi {name},",
    "Yo {name},",
    "Hello {name},",
    "{name},",
]
_LINES = [
    "have a sec later today?",
    "can you sanity-check something small for me?",
    "random: how do you usually file receipts?",
    "also — did you see the update about labels?",
    "btw, remind me of that shortcut you mentioned?",
    "tiny thing — is 3pm okay to sync?",
    "quick gut check on a note I’m drafting.",
    "unrelated, but the new keyboard feels… odd.",
]
_CLOSERS = [
    "thx!",
    "appreciate it.",
    "cool if you reply when you can.",
    "no rush at all.",
    "thanks again.",
    "cheers.",
]
_SIGNATURES = [
    "- j",
    "- me",
    "- thanks",
    "- appreciate it",
    "- J",
]

def _name_from_email(addr: str) -> str:
    left = (addr or "").split("@")[0]
    bits = left.replace("_", ".").split(".")
    if bits and len(bits[0]) >= 2:
        return bits[0].capitalize()
    return left[:1].upper()

def _vary_case(s: str) -> str:
    return s.capitalize() if random.random() < 0.2 else s

def generate_message(
    *,
    from_email: str,
    to_email: str,
    is_reply: bool,
    thread_subject_seed: Optional[str] = None,
) -> Tuple[str, str]:
    """
    Returns (subject, body). Keeps subjects short; bodies 2–4 lines; no links.
    """
    to_name = _name_from_email(to_email)
    opener = random.choice(_OPENERS).format(name=to_name)

    base_subject = thread_subject_seed or random.choice(_SUBJECT_BITS)
    subject = base_subject if (is_reply and base_subject.lower().startswith("re:")) else (f"Re: {base_subject}" if is_reply else base_subject)

    n_lines = random.randint(2, 4)
    lines = random.sample(_LINES, k=n_lines)

    if random.random() < 0.15:
        lines.insert(0, "(on phone, sorry for brevity)")

    if is_reply and random.random() < 0.6:
        lines.append("got your note—makes sense.")

    closer = random.choice(_CLOSERS)
    sig = random.choice(_SIGNATURES)

    body = "\n".join([opener, "", *[_vary_case(l) for l in lines], "", closer, sig])
    return (subject, body)

# ---------------------------------------
# Thread scheduling
# ---------------------------------------
def build_friendly_plan(
    owned_addresses: List[str],
    *,
    target_threads: int,
    min_thread_len: int = 2,
    max_thread_len: int = 4,
    tz: str = "America/Toronto",
    biz_hours: Tuple[int,int] = (9, 17),
    max_per_mailbox_today: int = 8,
    max_per_mailbox_hour: int = 3,
    min_gap_between_sends_min: int = 20,
    pair_cooldown_hours: int = 24,
    mailbox_cooldown_hours: int = 2,
    domain_pair_cooldown_min: int = 20,
    recent_history: List[HistoryItem] | None = None,
    exclude_addresses: Optional[set[str]] = None,
    seed: Optional[int] = None,
) -> List[PlannedEmail]:
    """
    Builds a flattened list of PlannedEmail objects (openers + replies) in chronological order.
    Respects:
      - cross-domain bias with domain pair cooldown
      - per-mailbox caps (per-day & per-hour) and min-gap spacing
      - business hours; skips weekends
    """
    if seed is not None:
        random.seed(seed)

    addrs = [a for a in _normalize_addrs(owned_addresses) if not (exclude_addresses and a in exclude_addresses)]
    if len(addrs) < 2 or target_threads <= 0:
        return []

    recent_history = recent_history or []
    tzinfo = ZoneInfo(tz)
    now_local = _now_tz(tz)
    today_start = now_local.replace(hour=0, minute=0, second=0, microsecond=0)

    # Per-day counts (local)
    sent_today = Counter(
        h.from_email.lower()
        for h in recent_history
        if _aware_utc(h.when).astimezone(tzinfo) >= today_start
    )

    # Last-send timestamps per mailbox (UTC) for spacing
    last_send_per_mailbox: Dict[str, datetime] = {}
    per_hour_buckets: Dict[str, Counter] = defaultdict(Counter)  # mailbox -> {YYYYmmddHH: count}

    for h in recent_history:
        when_utc = _aware_utc(h.when)
        last_send_per_mailbox[h.from_email.lower()] = max(last_send_per_mailbox.get(h.from_email.lower(), datetime.min.replace(tzinfo=UTC)), when_utc)
        hh = when_utc.astimezone(tzinfo).strftime("%Y%m%d%H")
        per_hour_buckets[h.from_email.lower()][hh] += 1

    # Choose who starts threads
    pairs = plan_friendly_pairs(
        addrs,
        target_threads,
        recent_history=recent_history,
        pair_cooldown_hours=pair_cooldown_hours,
        mailbox_cooldown_hours=mailbox_cooldown_hours,
        domain_pair_cooldown_min=domain_pair_cooldown_min,
        exclude_addresses=exclude_addresses,
    )
    if not pairs:
        return []

    plan: List[PlannedEmail] = []
    mailbox_slots_left = {a: max(0, max_per_mailbox_today - sent_today.get(a, 0)) for a in addrs}

    def _can_send_from(mailbox: str, when_utc: datetime) -> bool:
        if mailbox_slots_left.get(mailbox, 0) <= 0:
            return False
        # min-gap
        last = last_send_per_mailbox.get(mailbox)
        if last and (when_utc - last).total_seconds() < (min_gap_between_sends_min * 60):
            return False
        # per-hour cap (in local tz for natural cadence)
        hh = when_utc.astimezone(tzinfo).strftime("%Y%m%d%H")
        if per_hour_buckets[mailbox].get(hh, 0) >= max_per_mailbox_hour:
            return False
        return True

    def _record_send(mailbox: str, when_utc: datetime):
        mailbox_slots_left[mailbox] = max(0, mailbox_slots_left.get(mailbox, 0) - 1)
        last_send_per_mailbox[mailbox] = when_utc
        hh = when_utc.astimezone(tzinfo).strftime("%Y%m%d%H")
        per_hour_buckets[mailbox][hh] += 1

    for p in pairs:
        starter = p.from_email.lower()
        if mailbox_slots_left.get(starter, 0) <= 0:
            continue

        # Thread length
        tlen = random.randint(min_thread_len, max_thread_len)

        # Stable-enough thread key
        thread_key = f"{p.from_email}->{p.to_email}:{int(now_local.timestamp())}:{random.randint(1000,9999)}"

        # Opener scheduling
        subject_seed = random.choice(_SUBJECT_BITS)
        first_at = _schedule_after(
            start_after=now_local + _jitter_minutes(8),
            tz=tz,
            biz_hours=biz_hours,
            min_delay_min=5,
            max_delay_min=45,
        ).astimezone(UTC)

        # Respect spacing & per-hour cap; if not allowed now, push forward until allowed
        safety_hops = 0
        while not _can_send_from(starter, first_at) and safety_hops < 6:
            first_at = _schedule_after(first_at, tz, biz_hours, 10, 30).astimezone(UTC)
            safety_hops += 1
        if not _can_send_from(starter, first_at):
            continue  # give up on this thread starter

        subj, body = generate_message(from_email=p.from_email, to_email=p.to_email, is_reply=False, thread_subject_seed=subject_seed)
        plan.append(PlannedEmail(
            when=first_at, from_email=p.from_email, to_email=p.to_email,
            subject=subj, body=body, thread_key=thread_key, is_reply=False, step_index=0
        ))
        _record_send(starter, first_at)

        # Replies (alternate direction where possible)
        last_time = first_at
        dir_from = p.to_email.lower()
        dir_to = p.from_email.lower()

        for step in range(1, tlen):
            # Pick a natural reply window
            delay_min = random.choice([
                (25, 120),     # same day later
                (180, 480),    # few hours
                (720, 1440),   # next day
            ])
            send_at = _schedule_after(
                start_after=last_time,
                tz=tz,
                biz_hours=biz_hours,
                min_delay_min=delay_min[0],
                max_delay_min=delay_min[1],
            ).astimezone(UTC)

            # If current direction can't send, try swapping once; else skip this step
            candidate_from = dir_from
            candidate_to = dir_to
            if not _can_send_from(candidate_from, send_at):
                alt_from, alt_to = dir_to, dir_from
                if _can_send_from(alt_from, send_at):
                    candidate_from, candidate_to = alt_from, alt_to
                else:
                    # push forward a bit and retry up to 3 hops
                    hops = 0
                    while not _can_send_from(candidate_from, send_at) and hops < 3:
                        send_at = _schedule_after(send_at, tz, biz_hours, 10, 30).astimezone(UTC)
                        hops += 1
                    if not _can_send_from(candidate_from, send_at):
                        break

            subj, body = generate_message(
                from_email=candidate_from, to_email=candidate_to, is_reply=True, thread_subject_seed=subject_seed
            )
            plan.append(PlannedEmail(
                when=send_at, from_email=candidate_from, to_email=candidate_to,
                subject=subj, body=body, thread_key=thread_key, is_reply=True, step_index=step
            ))
            _record_send(candidate_from, send_at)
            last_time = send_at
            # Alternate for next step
            dir_from, dir_to = candidate_to, candidate_from

    # Sort and dedupe
    plan.sort(key=lambda x: x.when)
    dedup, seen = [], set()
    for item in plan:
        key = (item.when, item.from_email, item.to_email, item.step_index, item.thread_key)
        if key in seen: continue
        seen.add(key); dedup.append(item)
    return dedup
