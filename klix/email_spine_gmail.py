from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from datetime import datetime, date, timezone
from pathlib import Path
from typing import Dict, Iterable, List, Optional

from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request

logger = logging.getLogger(__name__)

# Read-only Gmail scope
SCOPES = ["https://www.googleapis.com/auth/gmail.readonly"]

# Canonical token directory on the VPS (per latest landmark)
TOKEN_DIR = Path(os.environ.get("GMAIL_TOKEN_DIR", "/opt/klix/klix-flows/gmail_tokens"))

# Logical account identifiers used in DB + state tables
ACCOUNT_EMAILS: Dict[str, str] = {
    # PERSONAL / CORE INBOXES
    "kolasajosh": "kolasajosh@gmail.com",
    "favourdesirous": "favourdesirous@gmail.com",
    "ads.klix": "ads.klix@gmail.com",

    # SPINE INBOX FOR KLIXADS.CA COLD REPLIES
    "spine_klixads": "spine@klixads.ca",

    # COLD SENDER INBOXES (money inboxes)
    "jess": "jess@klixads.ca",
    "erica": "erica@klixmedia.ca",
    "alex": "alex@klixads.org",
    "team": "team@klixmedia.org",
}


@dataclass
class RawEmail:
    """
    Normalized representation of a Gmail message.

    This is what flows/email_spine_poller.py will classify and persist into email_events.
    """

    account: str          # 'kolasajosh' | 'favourdesirous' | 'ads.klix' | 'jess' | 'erica' | 'alex' | 'team'
    provider: str         # 'gmail'
    message_id: str
    thread_id: str
    received_at: datetime
    from_address: str
    to_address: str
    subject: str
    snippet: str
    labels: List[str]
    payload: dict         # minimal but full Gmail message resource for v1


def _load_credentials(account: str) -> Credentials:
    """
    Load OAuth credentials for a given logical account from TOKEN_DIR.

    v1 is intentionally non-interactive:
    - If the token file is missing, we raise a clear error.
    - If the token is expired but refreshable, we refresh and write it back.
    """
    token_path = TOKEN_DIR / f"{account}.json"

    if not token_path.exists():
        raise RuntimeError(
            f"Gmail token for account '{account}' not found at {token_path}. "
            "Per landmark, generate tokens via bootstrap_gmail_tokens.py and copy them here."
        )

    creds = Credentials.from_authorized_user_file(str(token_path), SCOPES)

    if creds and creds.expired and creds.refresh_token:
        logger.info("Refreshing Gmail token for account %s", account)
        creds.refresh(Request())
        token_path.write_text(creds.to_json())

    if not creds or not creds.valid:
        raise RuntimeError(f"Invalid Gmail credentials for account '{account}' at {token_path}")

    return creds


def _build_gmail_service(account: str):
    """
    Construct a Gmail API client for the given logical account.
    """
    if account not in ACCOUNT_EMAILS:
        raise ValueError(f"Unknown Gmail account key: {account!r}")

    creds = _load_credentials(account)
    service = build("gmail", "v1", credentials=creds, cache_discovery=False)
    return service


def _to_after_query(since: Optional[datetime]) -> Optional[str]:
    """
    Build a Gmail 'after:' date query from a precise timestamp.

    Gmail only supports 'after:YYYY/MM/DD', so:
    - we use the UTC date portion of last_received_at
    - and then do precise filtering client-side via internalDate
    """
    if since is None:
        return None

    if since.tzinfo is None:
        since = since.replace(tzinfo=timezone.utc)
    else:
        since = since.astimezone(timezone.utc)

    d: date = since.date()
    return f"after:{d.year}/{d.month:02d}/{d.day:02d}"


def _parse_header(headers: Iterable[dict], name: str) -> str:
    for h in headers:
        if h.get("name", "").lower() == name.lower():
            return h.get("value", "")
    return ""


def _normalize_message(account: str, msg: dict) -> RawEmail:
    """
    Convert a Gmail message resource into our RawEmail dataclass.
    """
    msg_id = msg.get("id", "")
    thread_id = msg.get("threadId", "")
    internal_date_ms = int(msg.get("internalDate", "0") or 0)
    received_at = datetime.fromtimestamp(internal_date_ms / 1000.0, tz=timezone.utc)

    snippet = msg.get("snippet", "") or ""
    label_ids = msg.get("labelIds", []) or []

    payload = msg.get("payload", {}) or {}
    headers = payload.get("headers", []) or []

    from_address = _parse_header(headers, "From")
    to_address = _parse_header(headers, "To")
    subject = _parse_header(headers, "Subject")

    return RawEmail(
        account=account,
        provider="gmail",
        message_id=msg_id,
        thread_id=thread_id,
        received_at=received_at,
        from_address=from_address,
        to_address=to_address,
        subject=subject,
        snippet=snippet,
        labels=list(label_ids),
        payload=msg,
    )


def fetch_messages_since(account: str, last_received_at: Optional[datetime]) -> List[RawEmail]:
    """
    Fetch all Gmail messages for `account` received strictly after `last_received_at`.

    - Uses a coarse 'after:YYYY/MM/DD' Gmail search to reduce load.
    - Applies exact filtering via internalDate (ms since epoch) on the client.
    - Restricts to INBOX and excludes spam/trash.
    - Returns messages sorted by received_at ascending so the caller can:
        - insert into email_events in order
        - update email_poll_state.last_received_at to the newest timestamp
    """
    service = _build_gmail_service(account)
    query = _to_after_query(last_received_at)

    # Compute internalDate threshold in ms for precise client-side filtering
    after_internal_ms: Optional[int] = None
    if last_received_at is not None:
        if last_received_at.tzinfo is None:
            last_received_at = last_received_at.replace(tzinfo=timezone.utc)
        else:
            last_received_at = last_received_at.astimezone(timezone.utc)
        after_internal_ms = int(last_received_at.timestamp() * 1000)

    logger.info("Email spine: polling Gmail for account=%s query=%r", account, query)

    messages: List[RawEmail] = []
    page_token: Optional[str] = None

    try:
        while True:
            list_kwargs = {
                "userId": "me",
                "labelIds": ["INBOX"],
                "includeSpamTrash": False,
                "maxResults": 500,
            }
            if query:
                list_kwargs["q"] = query
            if page_token:
                list_kwargs["pageToken"] = page_token

            resp = service.users().messages().list(**list_kwargs).execute()
            msg_refs = resp.get("messages", []) or []

            if not msg_refs:
                break

            for ref in msg_refs:
                msg_id = ref.get("id")
                if not msg_id:
                    continue

                msg = (
                    service.users()
                    .messages()
                    .get(userId="me", id=msg_id, format="full")
                    .execute()
                )

                internal_ms = int(msg.get("internalDate", "0") or 0)
                if after_internal_ms is not None and internal_ms <= after_internal_ms:
                    # Older than or equal to our cursor; skip
                    continue

                messages.append(_normalize_message(account, msg))

            page_token = resp.get("nextPageToken")
            if not page_token:
                break

    except HttpError as e:
        logger.exception("Email spine: Gmail API error while polling account=%s: %s", account, e)
        # v1: bubble to caller so the flow can record an incident
        raise

    # Sort ascending to make cursor updates trivial
    messages.sort(key=lambda m: m.received_at)
    logger.info(
        "Email spine: poll complete for account=%s, fetched %d new messages",
        account,
        len(messages),
    )

    return messages


__all__ = ["RawEmail", "fetch_messages_since"]
