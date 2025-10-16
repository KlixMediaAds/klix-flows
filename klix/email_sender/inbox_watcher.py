# inbox_watcher.py — IMAP poller for replies/bounces → logs + suppression
import os, imaplib, email, re, time, json
from email.header import decode_header
from lib.limits import init_db, record_send, mark_do_not_send

"""
ENV per mailbox (comma-separated):
INBOX_ACCOUNTS=klix1@domain.com,klix2@domain.com
IMAP_HOST=imap.gmail.com
IMAP_PORT=993
IMAP_PASS_klix1@domain.com=xxxx xxxx xxxx xxxx   # app password
IMAP_PASS_klix2@domain.com=yyyy yyyy yyyy yyyy
"""

RE_BOUNCE_SUBJ = re.compile(r"(mail delivery|delivery status|undeliverable|failure notice|returned mail)", re.I)
RE_FROM_MAILER = re.compile(r"(mailer-daemon|postmaster)", re.I)

def _hval(v):
    parts = decode_header(v)
    frag = "".join([(t.decode(enc or "utf-8") if isinstance(t, bytes) else t) for t, enc in parts])
    return frag

def _connected(host, port, user, pw):
    M = imaplib.IMAP4_SSL(host, port)
    M.login(user, pw)
    return M

def _parse_to_address(msg):
    # pull original 'To' from headers inside DSN or from message headers if it's a reply
    for hdr in ("X-Failed-Recipients", "Final-Recipient", "Original-Recipient", "To"):
        val = msg.get(hdr)
        if val:
            # Final-Recipient: rfc822; user@example.com
            m = re.search(r"([A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,})", val)
            if m:
                return m.group(1).lower()
    # sometimes inside body
    try:
        payload = msg.get_payload(decode=True) or b""
        text = payload.decode(errors="ignore")
        m = re.search(r"[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}", text)
        if m: return m.group(0).lower()
    except Exception:
        pass
    return None

def _is_bounce(msg, subj_text, from_text):
    if RE_FROM_MAILER.search(from_text or ""): return True
    if RE_BOUNCE_SUBJ.search(subj_text or ""): return True
    # DSN content-type
    ctype = (msg.get_content_type() or "").lower()
    return "delivery-status" in ctype

def _find_in_reply_to(msg):
    # Try standard headers for threading
    return msg.get("In-Reply-To") or msg.get("References") or ""

def poll_once():
    init_db()
    host = os.getenv("IMAP_HOST", "imap.gmail.com")
    port = int(os.getenv("IMAP_PORT", "993"))
    accounts = [a.strip() for a in os.getenv("INBOX_ACCOUNTS", "").split(",") if "@" in a]
    if not accounts:
        print("[INBOX] No accounts configured (INBOX_ACCOUNTS).")
        return

    for acct in accounts:
        pw = os.getenv(f"IMAP_PASS_{acct}", "")
        if not pw:
            print(f"[INBOX] Missing app password for {acct} (IMAP_PASS_{acct}). Skipping.")
            continue

        try:
            M = _connected(host, port, acct, pw)
            M.select("INBOX")
            # Only unseen since last run is okay; or search all and rely on idempotency
            typ, data = M.search(None, '(UNSEEN)')
            if typ != "OK":
                print(f"[INBOX] Search failed for {acct}.")
                M.logout()
                continue

            ids = data[0].split()
            print(f"[INBOX] {acct} unseen messages: {len(ids)}")
            for i in ids:
                typ, msg_data = M.fetch(i, "(RFC822)")
                if typ != "OK": 
                    continue
                raw = msg_data[0][1]
                msg = email.message_from_bytes(raw)

                subj = _hval(msg.get("Subject") or "")
                from_h = _hval(msg.get("From") or "")
                # Bounce?
                if _is_bounce(msg, subj, from_h):
                    failed = _parse_to_address(msg)  # the address we tried to send to
                    if failed:
                        mark_do_not_send(failed, days=90, reason="hard_bounce")
                        record_send(sender_id=acct, to_email=failed, prospect_id=None, message_id="",
                                    status="bounce", reason=f"DSN:{subj}", meta={"from": from_h})
                        print(f"[BOUNCE] {acct} -> {failed} ({subj})")
                    # mark read either way
                    M.store(i, '+FLAGS', '\\Seen')
                    continue

                # Human reply (simplified): message to us that is not a DSN
                in_reply_to = _find_in_reply_to(msg)
                # attempt to find the address we emailed originally (the From is the prospect)
                m = re.search(r"<([^>]+@[^>]+)>", from_h) or re.search(r"([A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,})", from_h)
                prospect = (m.group(1) if m else "").lower()
                if prospect:
                    record_send(sender_id=acct, to_email=prospect, prospect_id=None, message_id=in_reply_to,
                                status="reply", reason="human_reply", meta={"subject": subj})
                    print(f"[REPLY] {acct} <- {prospect} | {subj}")

                M.store(i, '+FLAGS', '\\Seen')

            M.logout()
        except Exception as e:
            print(f"[INBOX] Error for {acct}: {e}")

if __name__ == "__main__":
    poll_once()
