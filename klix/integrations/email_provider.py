from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Dict, Optional
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart


class EmailProvider(ABC):
    """
    Abstract email provider interface for KlixOS.

    All email backends must implement this interface.
    """

    @abstractmethod
    def send_email(
        self,
        *,
        from_addr: str,
        to_addr: str,
        subject: str,
        html_body: str,
        text_body: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> None:
        """
        Send an email.

        Implementations may ignore `metadata` or use it for provider-specific
        options (headers, tags, tracking IDs, etc.).
        """
        raise NotImplementedError


class SMTPEmailProvider(EmailProvider):
    """
    Simple SMTP-based EmailProvider implementation.

    Does not read environment variables; callers must pass host/port/credentials.
    """

    def __init__(
        self,
        host: str,
        port: int,
        username: Optional[str] = None,
        password: Optional[str] = None,
        use_tls: bool = True,
        timeout: Optional[float] = 30.0,
    ) -> None:
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.use_tls = use_tls
        self.timeout = timeout

    def send_email(
        self,
        *,
        from_addr: str,
        to_addr: str,
        subject: str,
        html_body: str,
        text_body: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> None:
        """
        Send an email via SMTP.

        Raises exceptions on failure so callers can detect and react.
        """
        if text_body is None:
            text_body = "This message contains HTML content. Please view in an HTML-capable client."

        msg = MIMEMultipart("alternative")
        msg["From"] = from_addr
        msg["To"] = to_addr
        msg["Subject"] = subject

        msg.attach(MIMEText(text_body, "plain", "utf-8"))
        msg.attach(MIMEText(html_body, "html", "utf-8"))

        # Optional: attach custom headers from metadata
        if metadata:
            headers = metadata.get("headers")
            if isinstance(headers, dict):
                for key, value in headers.items():
                    if key not in msg:
                        msg[key] = str(value)

        smtp = smtplib.SMTP(self.host, self.port, timeout=self.timeout)
        try:
            if self.use_tls:
                smtp.starttls()

            if self.username:
                smtp.login(self.username, self.password or "")

            smtp.sendmail(from_addr, [to_addr], msg.as_string())
        finally:
            try:
                smtp.quit()
            except Exception:
                # If close fails, sending has already succeeded or raised.
                pass
