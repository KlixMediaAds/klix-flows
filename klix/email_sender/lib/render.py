# lib/render.py — single signature, remove any inline sign-off, normalize dashes
from datetime import datetime
import html, re

# Common sign-off starters to detect inline signatures from the model
SIGNOFF_START = re.compile(
    r"(?im)^\s*(best|thanks|thank you|cheers|warmly|sincerely|regards|kind regards)\s*[,\-–—:]?\s*$"
)

def normalize_for_render(s: str) -> str:
    if not s: return s
    # Replace em/en dashes with a hyphen
    s = s.replace("—", "-").replace("–", "-")
    # Normalize spaces around hyphens
    s = re.sub(r"\s*-\s*", " - ", s)
    # Collapse extra blank lines
    s = re.sub(r"\n{3,}", "\n\n", s)
    return s.strip()

def strip_inline_signature(body: str, from_name: str) -> str:
    """
    If the model already added a sign-off block, remove it.
    We look for a sign-off starter in the last ~6 lines and trim from there.
    Also remove any trailing lines that equal our from_name or contain 'Klix Media'.
    """
    if not body: return body
    lines = body.rstrip().splitlines()

    # trim trailing empties
    while lines and not lines[-1].strip():
        lines.pop()

    # scan last 6 lines for a signoff starter (e.g., 'Best,')
    start_idx = None
    scan_from = max(0, len(lines) - 6)
    for i in range(scan_from, len(lines)):
        if SIGNOFF_START.match(lines[i]):
            start_idx = i
    if start_idx is not None:
        lines = lines[:start_idx]  # drop everything from the signoff down

    # also drop trailing brand/name lines if present
    def looks_like_name(line: str) -> bool:
        l = line.strip().lower()
        return (from_name.strip().lower() in l) or ("klix media" in l)

    while lines and looks_like_name(lines[-1]):
        lines.pop()

    # de-dupe any consecutive duplicate trailing lines just in case
    while len(lines) >= 2 and lines[-1].strip() == lines[-2].strip():
        lines.pop()

    return "\n".join(lines).rstrip()

def md_to_html(md_text: str) -> str:
    txt = normalize_for_render(md_text or "")
    safe = html.escape(txt).replace("\n", "<br>")
    return safe

def render_email(subject: str, body_md: str, from_name: str, signature_enabled: bool):
    # Normalize and strip any inline signature added by the model
    body_md = normalize_for_render(body_md or "")
    body_md = strip_inline_signature(body_md, from_name)

    # Final safety: if the last non-empty lines already look like our signature, don't add it again
    tail = "\n".join([ln for ln in body_md.splitlines() if ln.strip()])[-200:].lower()
    already_signed = (from_name.strip().lower() in tail) or ("klix media" in tail)

    body_html = md_to_html(body_md)
    body_plain = body_md

    if signature_enabled and not already_signed:
        SIG_HTML = f"<br><br>Best,<br>{html.escape(from_name)}"
        SIG_PLAIN = f"\n\nBest,\n{from_name}"
        body_html += SIG_HTML
        body_plain += SIG_PLAIN

    # compliance footer
    footer_html = '<br><br><span style="color:#888;font-size:12px;">If this isn’t relevant, reply “STOP”.</span>'
    footer_plain = "\n\nIf this isn’t relevant, reply “STOP”."
    return subject or "", body_plain + footer_plain, body_html + footer_html
