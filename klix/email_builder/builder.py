import os, argparse, yaml, random, time, re
import gspread
from dotenv import load_dotenv
from google.oauth2.service_account import Credentials

from lib.sheets import Sheet, utc_now_iso, ensure_base_tabs
from lib.dedupe import dedupe_hit_for_prospect, history_hash_set, is_body_duplicate, compute_body_hash
from lib.enrich import fetch_site_title_tagline, enrich_snippets
from lib import prompt as pr

# ---------------- helpers ----------------

def load_yaml(path):
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)

def rid(prefix: str, n: int = 10) -> str:
    alphabet = "abcdefghijklmnopqrstuvwxyz0123456789"
    return prefix + "_" + "".join(random.choice(alphabet) for _ in range(n))

def make_dedupe_key(business: str, website: str, email: str) -> str:
    business = (business or "").strip().lower()
    website = (website or "").strip().lower()
    email = (email or "").strip().lower()
    if email:   return f"{business}|{email}"
    if website: return f"{business}|{website}"
    return business

def persona_target_fallback(prospect: dict) -> tuple[str, str]:
    """
    Local fallback for when lib.prompt.persona_target is missing.
    Prefers Email, then AltEmail, else constructs team@<brand>.com
    """
    email = (prospect.get("Email","") or "").strip()
    if email:
        return "Owner", email
    alt = (prospect.get("AltEmail","") or "").strip()
    if alt:
        return "Owner", alt
    name = (prospect.get("BusinessName","") or "").strip()
    domain_guess = re.sub(r"[^a-z0-9]+", "", name.lower()) or "brand"
    return "Team", f"team@{domain_guess}.com"

def draft_fallback(biz: dict, angle_id: str) -> dict:
    """
    Local emergency draft if model call fails or times out.
    Keeps tone simple and safe; <= ~120 words; ends with a low-friction question.
    """
    name = biz.get("BusinessName","your brand")
    about = (
        biz.get("AboutLine")
        or biz.get("Tagline")
        or biz.get("SiteTitle")
        or name
    )
    subject = f"Small idea for {name}"[:80]
    body = (
        f"I noticed “{about}” and liked the clarity.\n\n"
        f"Small thought: a simple AI UGC-style moment—close-ups, quiet ambient sound, "
        f"on-screen text that matches your tone. No faces, just product and feel.\n\n"
        f"Open to a 15-sec peek?"
    )
    return {"subject": subject, "body_md": body}

# ------------- import from Lead Finder (auto-detect tab) -------------

def top_up_from_lead_finder(sheet: Sheet, prospects_tab: str, need: int):
    """
    Import up to `need` rows from Lead Finder into Email Builder Prospects as Status=NEW.
    - If LEAD_FINDER_SOURCE_TAB is "LATEST:<prefix>", picks newest tab by date, e.g. LATEST:Leads
    - Else tries named tab, else first data-bearing tab.
    - Appends in 100-row batches (minimal API calls).
    """
    if need <= 0:
        print("[IMPORT] Need=0, skipping Lead Finder import.")
        return 0

    lf_id      = os.getenv("LEAD_FINDER_SHEET_ID", "").strip()
    src_name   = os.getenv("LEAD_FINDER_SOURCE_TAB", "").strip()  # can be "LATEST:Leads "
    cred_path  = os.getenv("GOOGLE_SHEETS_CRED", "").strip()
    if not (lf_id and cred_path and os.path.exists(cred_path)):
        print("[IMPORT] Lead Finder creds or sheet id missing; skipping.")
        return 0

    creds = Credentials.from_service_account_file(cred_path, scopes=["https://www.googleapis.com/auth/spreadsheets"])
    gc = gspread.authorize(creds)
    lf_ss = gc.open_by_key(lf_id)

    # --- helper: newest "Leads YYYY-MM-DD" style tab by prefix ---
    def _latest_tab_with_prefix(prefix: str):
        from datetime import datetime as _dt
        patt = re.compile(rf"^{re.escape(prefix)}\s*(\d{{4}}-\d{{2}}-\d{{2}})$")
        best_title, best_dt = None, None
        for w in lf_ss.worksheets():
            m = patt.match(w.title.strip())
            if not m:
                continue
            try:
                dt = _dt.strptime(m.group(1), "%Y-%m-%d")
            except ValueError:
                continue
            if best_dt is None or dt > best_dt:
                best_title, best_dt = w.title, dt
        return best_title

    # --- resolve source worksheet ---
    src_ws = None
    why = ""

    if src_name.upper().startswith("LATEST:"):
        prefix = src_name.split(":", 1)[1]
        latest_title = _latest_tab_with_prefix(prefix)
        if latest_title:
            src_ws = lf_ss.worksheet(latest_title)
            why = f"resolved LATEST:'{prefix}' -> '{latest_title}'"
        else:
            why = f"no match for LATEST:'{prefix}', falling back to first data-bearing tab"
    elif src_name:
        try:
            src_ws = lf_ss.worksheet(src_name)
            why = f"explicit tab '{src_name}'"
        except gspread.WorksheetNotFound:
            why = f"explicit tab '{src_name}' not found; falling back to first data-bearing tab"

    if src_ws is None:
        for w in lf_ss.worksheets():
            vals = w.get_all_values()
            if len(vals) >= 2 and any(c.strip() for c in vals[1]):
                src_ws = w
                if not why:
                    why = "auto-picked first data-bearing tab"
                break

    if src_ws is None:
        print("[IMPORT] Lead Finder has no data-bearing tabs.")
        return 0

    print(f"[IMPORT] Lead Finder tab -> {src_ws.title}  ({why})")

    # Destination Prospects header (ensure exists)
    dest_ws = sheet.ws(prospects_tab)
    dest_header = dest_ws.row_values(1)
    if not dest_header:
        dest_header = ["ID","BusinessName","Website","Email","AltEmail","Instagram","City","Niche",
                       "Notes","Status","CreatedAt","LastTouchedAt","DedupeKey","SiteTitle","Tagline"]
        dest_ws.append_row(dest_header)

    # Existing dedupe keys (one read)
    dest_vals = dest_ws.get_all_values()
    existing_keys = set()
    if len(dest_vals) > 1:
        for r in dest_vals[1:]:
            m = {dest_header[i]: (r[i] if i < len(dest_header) else "") for i in range(len(dest_header))}
            dk = (m.get("DedupeKey","") or "").strip().lower()
            if dk: existing_keys.add(dk)

    # Read source (one read)
    sv = src_ws.get_all_values()
    print(f"[IMPORT] Source rows (incl header): {len(sv)}")
    if not sv or len(sv) < 2:
        print("[IMPORT] Source tab has header only; nothing to import.")
        return 0
    sh = sv[0]

    def get(m, k): return (m.get(k) or "").strip()

    to_append, now = [], utc_now_iso()
    for row in sv[1:]:
        if len(to_append) >= need:
            break
        s = {sh[i]: (row[i] if i < len(sh) else "") for i in range(len(sh))}
        business = get(s, "BusinessName") or get(s, "Business") or get(s, "Name") or get(s, "name")
        website  = get(s, "Website")     or get(s, "URL")      or get(s, "Site") or get(s, "website")
        email    = get(s, "Email")       or get(s, "PrimaryEmail") or get(s, "ContactEmail") or get(s, "email")
        alt      = get(s, "AltEmail")    or get(s, "SecondaryEmail")
        insta    = get(s, "Instagram")   or get(s, "IG") or get(s, "InstagramHandle") or get(s, "instagram")
        city     = get(s, "City")        or get(s, "Location") or get(s, "city")
        niche    = get(s, "Niche")       or get(s, "Category") or get(s, "Industry") or get(s, "niche_search")
        site_t   = get(s, "SiteTitle")   or get(s, "site_title")
        tagl     = get(s, "Tagline")     or get(s, "site_desc")
        notes    = get(s, "Notes")       or get(s, "Remarks") or get(s, "icebreaker")

        if not (business or website or email):
            continue

        dkey = make_dedupe_key(business, website, email)
        if dkey in existing_keys:
            continue

        out = {
            "ID": rid("pros"),
            "BusinessName": business,
            "Website": website,
            "Email": email,
            "AltEmail": alt,
            "Instagram": insta,
            "City": city,
            "Niche": niche,
            "Notes": notes,
            "Status": "NEW",
            "CreatedAt": now,
            "LastTouchedAt": "",
            "DedupeKey": dkey,
            "SiteTitle": site_t,
            "Tagline": tagl,
        }
        to_append.append([out.get(h,"") for h in dest_header])
        existing_keys.add(dkey)

    total = 0
    for i in range(0, len(to_append), 100):
        sheet.ws(prospects_tab).append_rows(to_append[i:i+100], value_input_option="RAW")
        total += len(to_append[i:i+100])
        time.sleep(0.3)

    print(f"[IMPORT] Appended NEW prospects: {total} (need was {need})")
    return total

# ---------------- angle gating + selection ----------------

def _has_path(path: str, data: dict) -> bool:
    """
    Supports dotted paths like 'Signals.Seasonal' and OR with 'Notes|AboutLine'.
    Returns True if the path resolves to a truthy value.
    """
    if "|" in path:
        return any(_has_path(p.strip(), data) for p in path.split("|"))

    cur = data
    for part in path.split("."):
        if not isinstance(cur, dict):
            return False
        if part not in cur:
            return False
        cur = cur[part]
    if isinstance(cur, str):
        return bool(cur.strip())
    return bool(cur)

def _choose_angle_validated(angles_cfg: dict, data: dict) -> tuple[str, str]:
    """
    Weight-sampled angle where all 'requires' are satisfied by `data`.
    Returns (angle_id, template_hint)
    """
    items = angles_cfg.get("angles", []) or []
    pool = []
    for it in items:
        reqs = it.get("requires", []) or []
        if all(_has_path(r, data) for r in reqs):
            w = int(it.get("weight", 1))
            pool.extend([it] * max(1, w))
    if not pool:
        # graceful fallback to a safe default
        return ("site-copy-hook", "")

    pick = random.choice(pool)
    return (pick.get("id","site-copy-hook"), pick.get("template_hint",""))

# ---------------- main (batch drafting) ----------------

def main():
    load_dotenv()
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=int(os.getenv("N_PER_RUN", "20")))
    parser.add_argument("--history-dedupe", type=int, default=int(os.getenv("HISTORY_DEDUPE_N", "250")),
                        help="Compare body 3-gram hashes against last N emails; 0 to disable.")
    args = parser.parse_args()

    cfg = load_yaml("config/sheet.yaml")
    angles_cfg = load_yaml("config/angles.yaml")

    # Resolve Email Builder sheet (destination)
    raw_id = (cfg.get("spreadsheet_id", "") or "").strip()
    spreadsheet_id = os.path.expandvars(raw_id).strip() or os.getenv("EMAIL_BUILDER_SHEET_ID", "").strip() or os.getenv("SHEET_ID", "").strip()
    if not spreadsheet_id:
        raise RuntimeError("No spreadsheet_id resolved. Set EMAIL_BUILDER_SHEET_ID or SHEET_ID in .env, or put ${EMAIL_BUILDER_SHEET_ID} in config/sheet.yaml.")

    tabs = cfg["tabs"]
    prospects_tab = tabs["prospects"]
    emails_tab = tabs["emails"]

    # Ensure tabs once
    sheet = Sheet(spreadsheet_id)
    ensure_base_tabs(sheet, prospects_tab, emails_tab)

    # TOP-UP from Lead Finder to reach at least --limit NEW
    current = sheet.read(prospects_tab)                # 1 read
    current_new = [r for r in current if str(r.get("Status","")).upper()=="NEW"]
    need = max(0, args.limit - len(current_new))
    imported = top_up_from_lead_finder(sheet, prospects_tab, need)

    # Refresh after import
    prospects = sheet.read(prospects_tab)              # 2nd read
    emails = sheet.read(emails_tab)                    # 3rd read

    # Build optional cross-run dedupe set from Emails history (last N)
    history_hashes = set()
    if args.history_dedupe > 0:
        history_hashes = history_hash_set(emails, body_key="BodyMD", limit=args.history_dedupe)

    # Pick NEW up to limit
    to_process = [p for p in prospects if (str(p.get("Status","")).upper()=="NEW")][:args.limit]
    if not to_process:
        print("No NEW prospects to draft.")
        return

    # Cache worksheets + headers (avoid repeated metadata calls)
    ws_pros = sheet.ws(prospects_tab)
    ws_em   = sheet.ws(emails_tab)
    pros_header = ws_pros.row_values(1)
    em_header   = ws_em.row_values(1)
    pros_col = {h: i+1 for i, h in enumerate(pros_header)}
    em_col   = {h: i+1 for i, h in enumerate(em_header)}

    # Build row index for Prospects by ID (single scan)
    pros_values = ws_pros.get_all_values()             # 4th read
    id_to_row = {}
    if len(pros_values) > 1 and "ID" in pros_col:
        for rix, r in enumerate(pros_values[1:], start=2):
            if len(r) >= pros_col["ID"]:
                pid = r[pros_col["ID"]-1]
                if pid:
                    id_to_row[pid] = rix

    # Prepare batch appends/updates
    emails_to_append = []       # list of lists in em_header order
    prospect_cell_updates = []  # list of gspread.Cell

    drafted_count = 0
    skipped_count = 0

    for p in to_process:
        # Ensure DedupeKey
        dkey = (p.get("DedupeKey") or "").strip()
        if not dkey:
            dkey = make_dedupe_key(p.get("BusinessName",""), p.get("Website",""), p.get("Email",""))

        # Against prospect-level dedupe (already drafted/approved/sent)
        if dedupe_hit_for_prospect({**p, "DedupeKey": dkey}, emails, prospects):
            skipped_count += 1
            continue

        # Enrichment (site title/desc + deeper snippets for human tone)
        site_title, tagline = fetch_site_title_tagline(p.get("Website",""))
        biz = dict(p)
        biz["SiteTitle"] = site_title or p.get("SiteTitle","")
        biz["Tagline"]   = tagline   or p.get("Tagline","")
        biz["LastPostDate"] = ""

        snips = enrich_snippets(p.get("Website",""))
        biz["AboutLine"] = snips.get("AboutLine","")
        biz["Products"]  = snips.get("Products",[])
        biz["Founder"]   = snips.get("Founder","")

        # Compose a data view for angle requires (merge p + enriched + Signals bucket)
        data = dict(biz)
        data.setdefault("Signals", {})
        if "review" in (p.get("Notes","") or "").lower():
            data["Signals"]["ReviewLight"] = True

        # Angle selection with gating. Try up to 3 angles (avoid collisions).
        attempt = 0
        chosen_angle_id = None
        chosen_hint = ""
        subject = ""
        body_md = ""
        body_hash = ""
        human_score = ""

        # Determine persona + target email with fallback
        if hasattr(pr, "persona_target"):
            persona, to_email = pr.persona_target(p)
        else:
            persona, to_email = persona_target_fallback(p)

        # Pre-scan whether Emails has optional fields we can fill
        has_angle_hint  = "AngleHint" in em_col
        has_body_hash   = "BodyHash" in em_col
        has_human_score = "HumannessScore" in em_col

        # Prepare a rolling set of fingerprints to avoid in-run dupes
        inrun_hashes = set()

        while attempt < 3:
            angle_id, angle_hint = _choose_angle_validated(angles_cfg, data)

            # Model call with safety net
            model_name = os.getenv("MODEL_NAME","gpt-5")
            try:
                draft = pr.draft_email(biz, angle_id, model_name=model_name)
            except Exception:
                # API hang/timeout or unexpected failure -> local fallback
                draft = draft_fallback(biz, angle_id)

            # QA + clamps
            subject = (draft.get("subject","") or "").strip()
            if len(subject.split()) > 7:
                subject = f"Quick idea for {p.get('BusinessName','you')}"
            subject = subject[:120]

            body_md = (draft.get("body_md","") or "").strip()
            if len(body_md.split()) > 140:
                body_md = " ".join(body_md.split()[:140])

            body_hash = compute_body_hash(body_md)

            # cross-run/history dedupe
            collision = is_body_duplicate(body_md, history_hashes) or (body_hash in inrun_hashes)
            if not collision:
                chosen_angle_id = angle_id
                chosen_hint = angle_hint or ""
                inrun_hashes.add(body_hash)
                break

            attempt += 1

        # Optional humanness scoring if available
        try:
            human_score = f"{pr.score_humanness({'subject':subject,'body_md':body_md}, biz):.3f}"
        except Exception:
            human_score = ""

        # Build Emails row in header order
        em_row_dict = {
            "EmailID": rid("eml"),
            "ProspectID": p.get("ID",""),
            "BusinessName": p.get("BusinessName",""),
            "ToEmail": to_email,
            "PersonaTarget": persona,
            "Angle": chosen_angle_id or angle_id,
            "Subject": subject,
            "BodyMD": body_md,
            "DraftedAt": utc_now_iso(),
            "Status": "DRAFTED",
            "Model": os.getenv("MODEL_NAME","gpt-5"),
            "Notes": ""
        }
        if has_angle_hint:
            em_row_dict["AngleHint"] = chosen_hint
        if has_body_hash:
            em_row_dict["BodyHash"] = body_hash
        if has_human_score and human_score:
            em_row_dict["HumannessScore"] = human_score

        emails_to_append.append([em_row_dict.get(h,"") for h in em_header])

        # Prospect -> DRAFTED (batch cell updates)
        prow = id_to_row.get(p.get("ID",""))
        if prow:
            if "Status" in pros_col:
                prospect_cell_updates.append(gspread.Cell(prow, pros_col["Status"], "DRAFTED"))
            if "LastTouchedAt" in pros_col:
                prospect_cell_updates.append(gspread.Cell(prow, pros_col["LastTouchedAt"], utc_now_iso()))
            if "CreatedAt" in pros_col and not (p.get("CreatedAt") or "").strip():
                prospect_cell_updates.append(gspread.Cell(prow, pros_col["CreatedAt"], utc_now_iso()))
            if "DedupeKey" in pros_col:
                prospect_cell_updates.append(gspread.Cell(prow, pros_col["DedupeKey"], dkey))

        drafted_count += 1

    # Do the batch writes (2 API calls total)
    if emails_to_append:
        ws_em.append_rows(emails_to_append, value_input_option="RAW")
        time.sleep(0.4)  # gentle pacing
    if prospect_cell_updates:
        ws_pros.update_cells(prospect_cell_updates)

    print(f"Imported from Lead Finder: {imported} | Drafted: {drafted_count} | Skipped(dedupe/status): {skipped_count}")

if __name__ == "__main__":
    main()
