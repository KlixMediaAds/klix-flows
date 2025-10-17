# lib/mailer.py — resilient SMTP with UTF-8, retries, DSN, optional DKIM, deterministic Message-ID
import smtplib, ssl, email.utils, socket, time, random, idna
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.header import Header
from email.utils import formataddr
from typing import Optional, Tuple, Dict, Any

# Optional DKIM; auto-no-op if not installed or config missing
try:
    import dkim
except Exception:  # pragma: no cover
    dkim = None


# ----------------------- config normalization -----------------------
def _normalize_sender_cfg(sender_cfg: dict) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    """
    Accepts either:
      { "smtp": {host, port, username, app_password, local_hostname?, timeout?}, ... }
    or flat: { host, port, username, app_password, ... }
    Returns (sender, smtp) normalized dicts.
    """
    if "smtp" in sender_cfg and isinstance(sender_cfg["smtp"], dict):
        smtp = dict(sender_cfg["smtp"])
    else:
        smtp = {
            "host": sender_cfg.get("host"),
            "port": int(sender_cfg.get("port", 587)),
            "username": sender_cfg.get("username"),
            "app_password": sender_cfg.get("app_password"),
            "local_hostname": sender_cfg.get("local_hostname"),
            "timeout": int(sender_cfg.get("timeout", 30)),
        }

    from_email = sender_cfg.get("from_email") or smtp.get("username") or ""
    domain = sender_cfg.get("domain") or (from_email.split("@")[-1] if "@" in from_email else "localhost")

    sender = {
        "from_name": sender_cfg.get("from_name") or "",
        "from_email": from_email,
        "domain": domain,
        "reply_to": sender_cfg.get("reply_to") or from_email,
        # Optional DKIM block (all or nothing)
        "dkim_selector": sender_cfg.get("dkim_selector"),
        "dkim_domain": sender_cfg.get("dkim_domain") or domain,
        "dkim_private_key": sender_cfg.get("dkim_private_key"),  # path or PEM text
        # Optional campaign/thread metadata
        "x_campaign": sender_cfg.get("x_campaign"),
        "x_sender_id": sender_cfg.get("id"),
    }
    return sender, smtp


# ----------------------- mailer class -----------------------
class SMTPMailer:
    RETRY_SMTP_CODES = {421, 450, 451, 452, 454, 471}
    MAX_TRIES = 4

    def __init__(self, sender_cfg: dict, dry_run: bool = False):
        self.sender_raw = dict(sender_cfg)
        self.sender, self.smtp = _normalize_sender_cfg(sender_cfg)
        missing = [k for k in ("host", "port", "username", "app_password") if not self.smtp.get(k)]
        if missing:
            raise ValueError(f"SMTP config missing keys: {missing}. Got: {self.smtp}")
        self.dry_run = dry_run

    # ---------- helpers ----------
    def _make_msgid(self) -> str:
        dom = (self.sender.get("from_email") or "klix.local").split("@")[-1]
        return email.utils.make_msgid(domain=dom)

    @staticmethod
    def _extract_msgid_from_raw(raw: bytes) -> Optional[str]:
        try:
            start = raw.find(b"Message-ID:")
            if start == -1:
                return None
            end = raw.find(b"\n", start)
            line = raw[start:end].decode(errors="ignore")
            return line.split(":", 1)[1].strip()
        except Exception:
            return None

    @staticmethod
    def _idna_addr(addr: str) -> str:
        """
        Convert an email address to IDNA (punycode) domain if needed for SMTP envelope.
        Header values remain UTF-8 encoded with SMTPUTF8.
        """
        if not addr or "@" not in addr:
            return addr
        local, dom = addr.rsplit("@", 1)
        try:
            dom_ascii = idna.encode(dom).decode("ascii")
            return f"{local}@{dom_ascii}"
        except Exception:
            return addr

    def _build_message(
        self,
        to_email: str,
        subject: str,
        body_plain: str,
        body_html: str,
        in_reply_to: Optional[str],
        references: Optional[list],
        message_id: Optional[str] = None,
    ) -> bytes:
        msg = MIMEMultipart("alternative")
        # RFC 2047 encode Subject and From display name
        msg["Subject"] = str(Header(subject or "", "utf-8"))
        from_disp = formataddr((str(Header(self.sender["from_name"], "utf-8")), self.sender["from_email"]))
        msg["From"] = from_disp
        msg["To"] = to_email
        msg["Reply-To"] = self.sender["reply_to"]
        msg["Date"] = email.utils.formatdate(localtime=True)
        msg["Message-ID"] = message_id or self._make_msgid()
        if self.sender.get("x_campaign"):
            msg["X-Campaign"] = str(self.sender["x_campaign"])
        if self.sender.get("x_sender_id"):
            msg["X-Sender-ID"] = str(self.sender["x_sender_id"])

        if in_reply_to:
            msg["In-Reply-To"] = in_reply_to
        if references:
            msg["References"] = " ".join(references)

        # Body parts
        msg.attach(MIMEText(body_plain or "", "plain", "utf-8"))
        msg.attach(MIMEText(body_html or "", "html", "utf-8"))

        raw = msg.as_bytes()

        # Optional DKIM signing
        if dkim and self.sender.get("dkim_selector") and self.sender.get("dkim_domain") and self.sender.get("dkim_private_key"):
            try:
                key = self.sender["dkim_private_key"]
                if "\n" not in str(key) and not str(key).startswith("-----BEGIN"):
                    # treat as file path
                    with open(str(key), "rb") as fh:
                        key_bytes = fh.read()
                else:
                    key_bytes = key.encode("utf-8") if isinstance(key, str) else key
                d = self.sender["dkim_domain"].encode("utf-8")
                s = self.sender["dkim_selector"].encode("utf-8")
                headers = [b"from", b"to", b"subject", b"date", b"message-id", b"reply-to"]
                sig = dkim.sign(raw, selector=s, domain=d, privkey=key_bytes, include_headers=headers)
                raw = sig + raw
            except Exception as e:  # don't fail sending if DKIM fails
                print(f"[WARN] DKIM signing skipped: {type(e).__name__}: {e}")

        return raw

    # ---------- main API ----------
    def send(
        self,
        to_email: str,
        subject: str,
        body_plain: str,
        body_html: str,
        in_reply_to: Optional[str] = None,
        references: Optional[list] = None,
        message_id: Optional[str] = None,  # <— NEW: deterministic Message-ID in/out
    ) -> str:
        # Build raw message once (with provided or generated Message-ID)
        raw = self._build_message(to_email, subject, body_plain, body_html, in_reply_to, references, message_id=message_id)

        if self.dry_run:
            return message_id or (self._extract_msgid_from_raw(raw) or self._make_msgid())

        # Envelope addresses
        envelope_from = self._idna_addr(self.sender["from_email"])
        rcpt = self._idna_addr(to_email)

        # Hardened TLS
        ctx = ssl.create_default_context()
        ctx.check_hostname = True
        try:
            ctx.minimum_version = ssl.TLSVersion.TLSv1_2  # Python 3.7+; safe to ignore if not present
        except Exception:
            pass

        # Retry with exponential backoff on transient codes
        last_exc = None
        for attempt in range(1, self.MAX_TRIES + 1):
            try:
                # local_hostname helps craft HELO; default to system fqdn
                local_hn = self.smtp.get("local_hostname") or socket.getfqdn()
                timeout = int(self.smtp.get("timeout", 30))

                with smtplib.SMTP(self.smtp["host"], int(self.smtp["port"]), local_hostname=local_hn, timeout=timeout) as server:
                    server.ehlo()
                    server.starttls(context=ctx)
                    server.ehlo()

                    # Gmail app passwords sometimes have spaces—strip defensively
                    app_pw = (self.smtp["app_password"] or "").replace(" ", "")
                    server.login(self.smtp["username"], app_pw)

                    # SMTPUTF8 and DSN options
                    mail_opts = []
                    try:
                        if "smtputf8" in (server.esmtp_features or {}):
                            mail_opts.append("SMTPUTF8")
                    except Exception:
                        pass
                    rcpt_opts = ["NOTIFY=FAILURE,DELAY"]

                    # Send
                    try:
                        server.sendmail(envelope_from, [rcpt], raw, mail_options=mail_opts, rcpt_options=rcpt_opts)
                    except TypeError:
                        # Older Python (<3.10) may not support rcpt_options on sendmail; retry without
                        server.sendmail(envelope_from, [rcpt], raw, mail_options=mail_opts)

                # On success, prefer the header value we used/built
                return message_id or (self._extract_msgid_from_raw(raw) or self._make_msgid())

            except smtplib.SMTPResponseException as e:
                code = int(getattr(e, "smtp_code", 0) or 0)
                last_exc = e
                if code in self.RETRY_SMTP_CODES and attempt < self.MAX_TRIES:
                    sleep_s = (1.7 ** attempt) + random.uniform(0.15, 0.85)
                    print(f"[RETRY] SMTP {code} {getattr(e,'smtp_error',b'').decode(errors='ignore')} attempt={attempt}/{self.MAX_TRIES} sleep={sleep_s:.1f}s")
                    time.sleep(sleep_s)
                    continue
                raise
            except (smtplib.SMTPServerDisconnected, smtplib.SMTPConnectError, TimeoutError, socket.timeout) as e:
                last_exc = e
                if attempt < self.MAX_TRIES:
                    sleep_s = (1.7 ** attempt) + random.uniform(0.15, 0.85)
                    print(f"[RETRY] {type(e).__name__} attempt={attempt}/{self.MAX_TRIES} sleep={sleep_s:.1f}s")
                    time.sleep(sleep_s)
                    continue
                raise
            except Exception as e:
                last_exc = e
                raise

        if last_exc:
            raise last_exc
