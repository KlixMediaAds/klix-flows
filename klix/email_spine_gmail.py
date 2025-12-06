from __future__ import annotations

import os
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

logger = logging.getLogger(__name__)

GMAIL_SCOPES = ["https://www.googleapis.com/auth/gmail.readonly"]
GMAIL_TOKENS_DIR = os.getenv("GMAIL_TOKENS_DIR", "/etc/klix/gmail_tokens")


@dataclass
class RawEmail:
    account: str
    provider: str  # 'gmail'
    message_id: str
    thread_id: str
    received_at: datetime
    from_address: str
    to_address: str
    subject: str
    snippet: str
    labels: List[str]
    payload: Dict[str, Any]


def _load_credentials_for_account(account: str) -> Optional[Credentials]:
    """
    Load OAuth credentials for the given logical account from a token JSON file.

    v1 strategy:
    - We expect a pre-generated OAuth token file at:
        $GMAIL_TOKENS_DIR/{account}.json
      created via a one-time local OAuth flow.
    - If the file is missing, we *log and skip* the account instead of crashing
      the entire poller.
    """
    token_path = os.path.join(GMAIL_TOKENS_DIR, f"{account}.json")

    if not os.path.exists(token_path):
        logger.warning(
            "Email spine: Gmail token file not found for account '%s': %s — skipping this account",
            account,
            token_path,
        )
        return None

    try:
        creds = Credentials.from_authorized_user_file(token_path, scopes=GMAIL_SCOPES)
        return creds
    except Exception as e:  # pragma: no cover
        logger.error(
            "Email spine: Failed to load credentials for account '%s' from %s: %s",
            account,
            token_path,
            e,
        )
        return None


def _build_gmail_service(creds: Credentials):
    return build("gmail", "v1", credentials=creds, cache_discovery=False)


def _parse_internal_date_ms(internal_date_ms: str) -> datetime:
    """
    Gmail internalDate is ms since epoch as a string.
    """
    ms_int = int(internal_date_ms)
    return datetime.fromtimestamp(ms_int / 1000.0, tz=timezone.utc)


def _get_message_metadata(message: Dict[str, Any]) -> Tuple[datetime, str, str, str]:
    """
    Extract received_at, from, to, subject from a Gmail message resource.
    """
    internal_date_ms = message.get("internalDate") or "0"
    received_at = _parse_internal_date_ms(internal_date_ms)

    headers = message.get("payload", {}).get("headers", []) or []
    hdr_map = {h.get("name", "").lower(): h.get("value", "") for h in headers}

    from_addr = hdr_map.get("from", "")
    to_addr = hdr_map.get("to", "")
    subject = hdr_map.get("subject", "")

    return received_at, from_addr, to_addr, subject


def _normalize_labels(msg: Dict[str, Any]) -> List[str]:
    return msg.get("labelIds") or []


def _minimal_payload(msg: Dict[str, Any]) -> Dict[str, Any]:
    """
    Keep only a minimal subset of Gmail fields we might want later.
    Avoid storing full bodies in v1.
    """
    keys = [
        "id",
        "threadId",
        "labelIds",
        "snippet",
        "internalDate",
        "payload",
        "sizeEstimate",
        "historyId",
    ]
    return {k: msg.get(k) for k in keys if k in msg}


def fetch_new_emails(
    account: str,
    since_internal_date_ms: Optional[int],
    max_messages: int = 200,
) -> Tuple[List[RawEmail], Optional[int]]:
    """
    Fetch new Gmail messages for a given logical account.

    Args:
        account: logical account name ('kolasajosh', 'favourdesirous', 'ads.klix')
        since_internal_date_ms: last processed Gmail internalDate (ms) or None
        max_messages: per-run cap to avoid backlogs

    Returns:
        (emails, new_last_internal_date_ms)

        If the account is missing credentials or an error occurs, we log and
        return ([], since_internal_date_ms) so the poller can proceed with
        other accounts.
    """
    creds = _load_credentials_for_account(account)
    if not creds:
        # Skip this account gracefully
        return [], since_internal_date_ms

    try:
        service = _build_gmail_service(creds)

        user_id = "me"
        query_parts: List[str] = []
        # Optional: narrow to INBOX only for v1
        query_parts.append("in:inbox")

        # For simplicity, we rely on internalDate filtering via 'q'
        # Gmail does not support an exact "internalDate > X" in 'q', but we can
        # approximate with 'newer_than' if needed. For v1, we just fetch the
        # latest N and rely on our DB uniqueness.
        query = " ".join(query_parts).strip()

        results = (
            service.users()
            .messages()
            .list(userId=user_id, q=query, maxResults=max_messages)
            .execute()
        )

        messages_meta = results.get("messages", []) or []
        if not messages_meta:
            return [], since_internal_date_ms

        emails: List[RawEmail] = []
        max_internal_ms_seen = since_internal_date_ms or 0

        for meta in messages_meta:
            msg_id = meta.get("id")
            if not msg_id:
                continue

            msg = (
                service.users()
                .messages()
                .get(userId=user_id, id=msg_id, format="metadata")
                .execute()
            )

            internal_date_str = msg.get("internalDate") or "0"
            internal_ms = int(internal_date_str)
            # Skip messages we've already processed if we have a cursor
            if since_internal_date_ms is not None and internal_ms <= since_internal_date_ms:
                continue

            received_at, from_addr, to_addr, subject = _get_message_metadata(msg)
            labels = _normalize_labels(msg)
            snippet = msg.get("snippet", "")

            payload = _minimal_payload(msg)

            raw = RawEmail(
                account=account,
                provider="gmail",
                message_id=msg.get("id", ""),
                thread_id=msg.get("threadId", ""),
                received_at=received_at,
                from_address=from_addr,
                to_address=to_addr,
                subject=subject,
                snippet=snippet,
                labels=labels,
                payload=payload,
            )
            emails.append(raw)

            if internal_ms > max_internal_ms_seen:
                max_internal_ms_seen = internal_ms

        # If we didn't see anything newer, keep the old cursor
        if max_internal_ms_seen <= (since_internal_date_ms or 0):
            return emails, since_internal_date_ms

        return emails, max_internal_ms_seen

    except HttpError as e:  # pragma: no cover
        logger.error(
            "Email spine: Gmail API error for account '%s': %s", account, e, exc_info=True
        )
    except Exception as e:  # pragma: no cover
        logger.error(
            "Email spine: unexpected error while fetching emails for '%s': %s",
            account,
            e,
            exc_info=True,
        )

    # On any error, do not advance cursor to avoid losing messages.
    return [], since_internal_date_ms
