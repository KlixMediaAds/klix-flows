from __future__ import annotations
from typing import Tuple, Dict, Optional
import os, re, json, yaml

# use your real libs
from klix.email_builder.lib import prompt as pr
from klix.email_builder.lib.enrich import fetch_site_title_tagline, enrich_snippets

FRIENDLY_DOMAINS = {"klixads.org","gmail.com"}

def _domain(email: str) -> str:
    m = re.search(r"@([^>@\\s]+)$", email or "")
    return (m.group(1).lower() if m else "").strip()

def _company(lead: Dict) -> str:
    return (lead.get("company") or "your brand").strip()

def _site(lead: Dict) -> str:
    return (lead.get("website") or "").strip()

def _load_angles() -> dict:
    base = os.path.join(os.path.dirname(__file__), "config", "angles.yaml")
    try:
        with open(base, "r", encoding="utf-8") as f:
            return yaml.safe_load(f) or {}
    except Exception:
        return {"angles":[{"id":"site-copy-hook","weight":1,"requires": []}]}

def _has_path(path: str, data: dict) -> bool:
    if "|" in path:
        return any(_has_path(p.strip(), data) for p in path.split("|"))
    cur = data
    for part in path.split("."):
        if not isinstance(cur, dict) or part not in cur:
            return False
        cur = cur[part]
    return bool((cur or "").strip() if isinstance(cur, str) else cur)

def _choose_angle(angles_cfg: dict, data: dict) -> tuple[str, str]:
    items = (angles_cfg.get("angles") or [])
    pool = []
    for it in items:
        reqs = (it.get("requires") or [])
        if all(_has_path(r, data) for r in reqs):
            pool.extend([it] * max(1, int(it.get("weight",1))))
    if not pool:
        return ("site-copy-hook","")
    import random
    pick = random.choice(pool)
    return (pick.get("id","site-copy-hook"), pick.get("template_hint",""))

def _md_to_html(md: str) -> str:
    parts = [p.strip() for p in (md or "").split("\n\n") if p.strip()]
    return "".join(f"<p>{p.replace('\n','<br>')}</p>" for p in parts)

def build_email_for_lead(lead: Dict) -> Tuple[str, str, str, Optional[str]]:
    """
    Used by flows/email_builder_flow.py
    Returns: (subject, body_text, body_html, send_type_hint)
    """
    email = (lead.get("email") or "").strip()
    dom   = _domain(email)
    company = _company(lead)
    website = _site(lead)

    # light enrichment (safe if site is missing)
    title, desc = fetch_site_title_tagline(website)
    snips = enrich_snippets(website)
    about = snips.get("AboutLine") or desc or title or company

    # build the payload shape your prompt expects
    biz = {
        "BusinessName": company,
        "Website": website,
        "Email": email,
        "City": lead.get("city",""),
        "Niche": lead.get("niche",""),
        "Instagram": lead.get("instagram",""),
        "SiteTitle": title or "",
        "Tagline":   desc  or "",
        "AboutLine": about,
        "Products":  snips.get("Products",[]),
        "Founder":   snips.get("Founder",""),
        "LastPostDate": "",
        "Notes": "",
    }

    # angle gating
    angles_cfg = _load_angles()
    angle_id, _ = _choose_angle(angles_cfg, {"AboutLine": about, "Signals": {}, **biz})

    # model name from env or optional config file
    model = os.getenv("MODEL_NAME","gpt-4o-mini")
    cfgp = os.path.join(os.path.dirname(__file__), "config", "prompt_config.json")
    try:
        if os.path.exists(cfgp):
            with open(cfgp,"r",encoding="utf-8") as f:
                pc = json.load(f) or {}
                model = pc.get("model", model)
    except Exception:
        pass

    draft = pr.draft_email(biz, angle_id, model_name=model) or {}
    subj = (draft.get("subject") or "").strip()[:120]
    body_md = (draft.get("body_md") or "").strip()
    if not subj or not body_md:
        raise RuntimeError("AI draft returned empty subject/body")

    body_text = body_md
    body_html = _md_to_html(body_md)
    send_type = "friendly" if dom in FRIENDLY_DOMAINS else None
    return subj, body_text, body_html, send_type
