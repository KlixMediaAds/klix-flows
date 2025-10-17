# app/sender_engine/subject.py
# Subject diversification engine:
# - weighted strategies (template / spin / interrogative / recycle_excerpt)
# - style controls (case mix, length clamp, optional emoji)
# - similarity guard (Jaccard n-gram vs recent subjects in send_state.db)
# - safe: works when passed a Context or a raw subjector cfg dict

from __future__ import annotations
import re, random, sqlite3, json, time
from typing import Optional, Dict, Any, Iterable

def _norm(s: str) -> str:
    return " ".join((s or "").strip().split())

def _title_case(s: str) -> str:
    # light-weight title case, avoids screaming ALL CAPS
    s = s.strip()
    if not s: return s
    minor = {"and","or","the","a","an","to","of","for","on","in","at","by","with","vs","via"}
    words = re.split(r"(\s+|-|/|:)", s.lower())
    out = []
    cap_next = True
    for w in words:
        if re.match(r"\s+|-|/|:", w):
            out.append(w)
            cap_next = True
            continue
        if not w:
            out.append(w); continue
        if cap_next or w not in minor:
            out.append(w[0:1].upper() + w[1:])
        else:
            out.append(w)
        cap_next = False
    return "".join(out)

def _sentence_case(s: str) -> str:
    s = _norm(s)
    if not s: return s
    return s[0:1].upper() + s[1:]

def _clip(s: str, nmin: int, nmax: int) -> str:
    s = _norm(s)
    if len(s) <= nmax: return s
    # try to cut at word boundary
    cut = s[:nmax]
    space = cut.rfind(" ")
    if space >= max(nmin, nmax-20):
        cut = cut[:space]
    return cut

def _emojify_once(rng: random.Random, s: str, allowed: bool) -> str:
    if not allowed: return s
    # ~12% chance
    if rng.random() > 0.12: return s
    pick = rng.choice(["🙂","🚀","💡","🔍","📈","⏱","✨"])
    # don’t add emoji if it already ends with punctuation
    return s if re.search(r"[.!?…]$", s) else f"{s} {pick}"

def _ngrams(text: str, n: int) -> set:
    t = re.sub(r"[^a-z0-9 ]+", " ", text.lower())
    t = re.sub(r"\s+", " ", t).strip()
    if not t: return set()
    toks = t.split(" ")
    return set(tuple(toks[i:i+n]) for i in range(max(0, len(toks)-n+1)))

def _jaccard(a: Iterable, b: Iterable) -> float:
    A, B = set(a), set(b)
    if not A and not B: return 1.0
    if not A or not B:  return 0.0
    return len(A & B) / float(len(A | B))

class Subjector:
    def __init__(self, ctx_or_cfg, *, ctx=None):
        """
        Accept either:
          - a full Context (with .mail_cfg), or
          - a raw subjector cfg dict, plus optional ctx
        """
        if hasattr(ctx_or_cfg, "mail_cfg"):
            self.ctx = ctx_or_cfg
            self.cfg = (getattr(self.ctx, "mail_cfg", {}) or {}).get("subjector", {}) or {}
        else:
            self.ctx = ctx
            self.cfg = dict(ctx_or_cfg or {})

        self.enabled = bool(self.cfg.get("enabled", True))
        self.apply_to = {
            "cold":       bool((self.cfg.get("apply_to") or {}).get("cold", True)),
            "followups":  bool((self.cfg.get("apply_to") or {}).get("followups", True)),
            "friendlies": bool((self.cfg.get("apply_to") or {}).get("friendlies", True)),
        }
        self.strategies = list(self.cfg.get("strategies", [
            {"name":"template","weight":0.55},
            {"name":"spin","weight":0.35},
            {"name":"recycle_excerpt","weight":0.10},
        ]))
        self.followups = dict(self.cfg.get("followups", {}))
        self.min_len = int(self.cfg.get("min_len", 18))
        self.max_len = int(self.cfg.get("max_len", 74))
        self.title_case_ratio    = float(self.cfg.get("title_case_ratio", 0.35))
        self.sentence_case_ratio = float(self.cfg.get("sentence_case_ratio", 0.40))
        self.lowercase_ratio     = float(self.cfg.get("lowercase_ratio", 0.25))
        self.allow_punctuation   = bool(self.cfg.get("allow_punctuation", True))
        self.allow_emojis        = bool(self.cfg.get("allow_emojis", False))

        self.db_path = str(self.cfg.get("db_path", "send_state.db"))
        self.jaccard_ngram    = int(self.cfg.get("jaccard_ngram", 3))
        self.jaccard_min_diff = float(self.cfg.get("jaccard_min_diff", 0.22))
        self.dedupe_window_days = int(self.cfg.get("dedupe_window_days", 21))

        pools = self.cfg.get("pools", {}) or {}
        self.pools = {
            "channels":    list(pools.get("channels", [])),
            "benefits":    list(pools.get("benefits", [])),
            "topics":      list(pools.get("topics", [])),
            "nudges":      list(pools.get("nudges", [])),
            "pain_points": list(pools.get("pain_points", [])),  # can be empty
        }
        self.banned = set(map(str.lower, self.cfg.get("banned", [])))

    # ------------------------- public API -------------------------
    def make(self, *, kind: str, stage: int, base_subject: str | None,
             body_md: str | None, to_email: str | None = None,
             meta: Optional[Dict[str, Any]] = None) -> str:
        """
        kind: 'COLD' | 'FOLLOWUP' | 'FRIENDLY'
        stage: 0,1,2,3... (for followups)
        base_subject/body_md: row-provided values
        to_email: used only to stabilize RNG seed
        meta: optional dict with keys like first_name, company, domain
        """
        base_subject = (base_subject or "").strip()
        if not self.enabled:
            return base_subject or self._fallback_from_body(body_md)

        apply_flag = {
            "COLD":      self.apply_to["cold"],
            "FOLLOWUP":  self.apply_to["followups"],
            "FRIENDLY":  self.apply_to["friendlies"],
        }.get(kind.upper(), True)

        if not apply_flag:
            return base_subject or self._fallback_from_body(body_md)

        # Stable-ish RNG per recipient/day so multiple runs don't thrash
        seed_str = f"{to_email or ''}|{time.strftime('%Y-%j')}|{base_subject}"
        rng = random.Random(seed_str)

        # Strategy selection by weights
        chosen = self._pick_strategy(rng)

        # Generate a candidate (with re-rolls for similarity/banned)
        first_name = (meta or {}).get("first_name") or ""
        company    = (meta or {}).get("company") or ""
        domain     = (meta or {}).get("domain") or ""
        channel    = self._pick(rng, self.pools["channels"]) or "paid social"
        benefit    = self._pick(rng, self.pools["benefits"]) or "cut CAC"
        topic      = self._pick(rng, self.pools["topics"]) or "creative testing"
        nudge      = self._pick(rng, self.pools["nudges"]) or "quick idea"

        # Try up to 6 times to get a novel, compliant subject
        for attempt in range(6):
            if chosen == "template":
                s = self._templated(rng, first_name, company, domain, channel, benefit, topic, nudge)
            elif chosen == "spin":
                s = self._spun(rng, first_name, company, domain, channel, benefit, topic)
            elif chosen == "interrogative":
                s = self._interrogative(rng, first_name, company, domain, channel, benefit, topic, nudge, kind, stage)
            else:  # recycle_excerpt
                s = self._recycle_excerpt(body_md) or base_subject or self._templated(rng, first_name, company, domain, channel, benefit, topic, nudge)

            # followup flavor on top
            if kind.upper() == "FOLLOWUP":
                s = self._apply_followup_prefix(rng, s, stage)

            s = self._style(rng, s)
            s = _clip(s, self.min_len, self.max_len)

            if self._contains_banned(s):
                chosen = self._fallback_strategy(chosen)  # pivot strategy
                continue
            if not self._novel_enough(s):
                chosen = self._fallback_strategy(chosen)
                continue

            return s

        # as final fallback
        s = base_subject or self._fallback_from_body(body_md) or "quick idea"
        return self._style(rng, s)

    # ----------------------- strategies ---------------------------
    def _templated(self, rng, first, company, domain, channel, benefit, topic, nudge) -> str:
        T = [
            "{company} & {topic}",
            "{first} — {nudge} on {topic}",
            "idea to {benefit}",
            "{channel}: {nudge}",
            "{company} × Klix — {topic}",
            "re: {topic} at {company}",
            "{company} and {pain}",
            "idea to fix {pain}",
            "{first} / {topic}",
            "Klix + {company}",
        ]
        pain_pool = self.pools.get("pain_points") or ["wasted ad spend","lead quality"]
        pain = rng.choice(pain_pool)
        tpl = rng.choice(T)
        s = tpl.format(
            company=company or (domain or "your team"),
            topic=topic, nudge=nudge, channel=channel, benefit=benefit,
            first=(first or "quick thought"), pain=pain
        )
        return _norm(s)

    def _spun(self, rng, first, company, domain, channel, benefit, topic) -> str:
        a = rng.choice(["quick","tiny","fast","short","1-min","two-line"])
        v = rng.choice(["idea","thought","note","ping"])
        s = f"{a} {v}: {topic} @ {company or domain or 'your team'}"
        return _norm(s)

    def _interrogative(self, rng, first_name, company, domain, channel, benefit, topic, nudge, kind, stage) -> str:
        pain_pool = self.pools.get("pain_points") or ["ad spend","lead quality"]
        pain = rng.choice(pain_pool)
        Q = [
            "question about {topic}",
            "{first}, are you handling {channel}?",
            "handling {pain} at {company}?",
            "who owns {topic} at {company}?",
            "different angle on {pain}?",
        ]
        q_template = rng.choice(Q)
        return _norm(q_template.format(
            first=first_name or "Quick one",
            company=company or (domain or "your team"),
            topic=topic, channel=channel, pain=pain
        ))

    def _recycle_excerpt(self, body_md: Optional[str]) -> str:
        if not body_md:
            return ""
        # first non-empty line of body
        for line in (body_md or "").splitlines():
            line = _norm(re.sub(r"[*_`>#-]", "", line))
            if line:
                return line
        return ""

    # ----------------------- followups ----------------------------
    def _apply_followup_prefix(self, rng, s: str, stage: int) -> str:
        fcfg = self.followups
        reply_prefix_chance = float(fcfg.get("reply_prefix_chance", 0.35))
        keep_thread_chance  = float(fcfg.get("keep_thread_chance", 0.50))
        # textual prefix (not actual threading)
        bank = {
            1: list(fcfg.get("f1_prefixes", [])) or ["quick nudge","circling back","quick check","gentle ping"],
            2: list(fcfg.get("f2_prefixes", [])) or ["still up for this?","worth a look?","2nd nudge","looping back"],
            3: list(fcfg.get("f3_prefixes", [])) or ["last ping","closing the loop","parking this?","final check"],
        }
        if rng.random() < reply_prefix_chance:
            pre = (bank.get(max(1, min(3, int(stage))), ["quick nudge"]))[0]
            s = f"{pre}: {s}"
        # optionally tack on a "re:" text token to simulate thread continuity (purely cosmetic)
        if rng.random() < keep_thread_chance and not s.lower().startswith("re:"):
            s = f"Re: {s}"
        return s

    # ----------------------- style/filters -------------------------
    def _style(self, rng, s: str) -> str:
        s = _norm(s)
        r = rng.random()
        if r < self.title_case_ratio:
            s = _title_case(s)
        elif r < self.title_case_ratio + self.sentence_case_ratio:
            s = _sentence_case(s)
        else:
            s = s.lower()
        if not self.allow_punctuation:
            s = re.sub(r"[?!.,;:]+", "", s)
        s = _emojify_once(rng, s, self.allow_emojis)
        return s

    def _contains_banned(self, s: str) -> bool:
        s_l = s.lower()
        return any(b in s_l for b in self.banned)

    def _fallback_from_body(self, body_md: Optional[str]) -> str:
        return self._recycle_excerpt(body_md) or "quick idea"

    def _pick_strategy(self, rng) -> str:
        # allow names + weights in cfg
        items = self.strategies or [{"name":"template","weight":1.0}]
        names, weights = [], []
        for it in items:
            names.append(str(it.get("name","template")))
            weights.append(float(it.get("weight", 1.0)))
        tot = sum(weights) or 1.0
        x = rng.random() * tot
        acc = 0.0
        for n, w in zip(names, weights):
            acc += w
            if x <= acc:
                return n
        return names[-1]

    def _fallback_strategy(self, cur: str) -> str:
        # small pivot cycle to avoid getting stuck
        order = ["template","spin","interrogative","recycle_excerpt"]
        try:
            i = order.index(cur)
            return order[(i+1) % len(order)]
        except ValueError:
            return "template"

    def _pick(self, rng, pool: list) -> Optional[str]:
        return rng.choice(pool) if pool else None

    # ----------------- novelty / history checks --------------------
    def _recent_subjects(self) -> list[str]:
        # Pull recent subjects from meta_json in sends table
        try:
            cutoff = int(time.time() - self.dedupe_window_days * 86400)
            out = []
            con = sqlite3.connect(self.db_path)
            cur = con.execute("SELECT meta_json FROM sends WHERE ts >= ?", (cutoff,))
            for (mj,) in cur.fetchall():
                if not mj: continue
                try:
                    meta = json.loads(mj)
                    subj = (meta or {}).get("subject")
                    if subj:
                        out.append(str(subj))
                except Exception:
                    continue
            con.close()
            return out
        except Exception:
            return []

    def _novel_enough(self, candidate: str) -> bool:
        try:
            prev = self._recent_subjects()
            if not prev: return True
            A = _ngrams(candidate, max(1, self.jaccard_ngram))
            # If any prior is too similar, reject
            for p in prev[:300]:  # cap for speed
                B = _ngrams(p, max(1, self.jaccard_ngram))
                sim = _jaccard(A, B)
                # require at least jaccard_min_diff difference -> similarity must be <= 1 - min_diff
                if sim > (1.0 - float(self.jaccard_min_diff)):
                    return False
            return True
        except Exception:
            # fail open (don't block send if DB hiccups)
            return True
