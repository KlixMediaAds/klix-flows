from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import logging

from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

logger = logging.getLogger(__name__)


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


def _load_credentials_for_account(account: str) -> Credentials:
    """
    Load Gmail OAuth credentials for the given logical account.

    Expected env vars:
        GMAIL_TOKENS_DIR: base directory with token JSON files (default: /etc/klix/gmail_tokens)

    Token file convention (v1):
        {account}.json   e.g. kolasajosh.json, favourdesirous.json, ads.klix.json
    """
    tokens_dir = os.getenv("GMAIL_TOKENS_DIR", "/etc/klix/gmail_tokens")
    token_path = os.path.join(tokens_dir, f"{account}.json")

    if not os.path.exists(token_path):
        raise FileNotFoundError(
            f"Gmail token file not found for account '{account}': {token_path}"
        )

    creds = Credentials.from_authorized_user_file(token_path, scopes=[
        "https://www.googleapis.com/auth/gmail.readonly",
    ])
    return creds


def _build_gmail_service(creds: Credentials):
    return build("gmail", "v1", credentials=creds, cache_discovery=False)


def _parse_internal_date(ms: str) -> datetime:
    # Gmail internalDate is milliseconds since epoch as string
    try:
        ts_ms = int(ms)
    except (TypeError, ValueError):
        return datetime.now(timezone.utc)
    return datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc)


def _get_header(headers: List[Dict[str, str]], name: str) -> str:
    for h in headers:
        if h.get("name", "").lower() == name.lower():
            return h.get("value", "")
    return ""


def _normalize_account(email_address: str) -> str:
    """
    Map raw Gmail email address to our logical account key.

    For now we just strip the domain and dots for known accounts.
    You can tighten this later if needed.
    """
    local_part = email_address.split("@", 1)[0].lower()

    # Explicit mappings for v1
    if "kolasajosh" in local_part:
        return "kolasajosh"
    if "favourdesirous" in local_part:
        return "favourdesirous"
    if "ads" in local_part or "klix" in local_part:
        return "ads.klix"

    # Fallback to raw local part
    return local_part


def _message_to_raw_email(
    account: str,
    msg: Dict[str, Any],
) -> RawEmail:
    payload = msg.get("payload", {}) or {}
    headers = payload.get("headers", []) or []

    from_addr = _get_header(headers, "From")
    to_addr = _get_header(headers, "To")
    subject = _get_header(headers, "Subject")

    internal_date = msg.get("internalDate")  # ms since epoch as string
    received_at = _parse_internal_date(internal_date)

    labels = msg.get("labelIds", []) or []

    snippet = msg.get("snippet", "") or ""

    # We keep the full minimal Gmail message in raw_payload;
    # bodies can be reconstructed later if needed.
    raw_payload = {
        "id": msg.get("id"),
        "threadId": msg.get("threadId"),
        "labelIds": labels,
        "payload": payload,
        "internalDate": internal_date,
        "historyId": msg.get("historyId"),
        "sizeEstimate": msg.get("sizeEstimate"),
    }

    return RawEmail(
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
        payload=raw_payload,
    )


def fetch_new_emails(
    account: str,
    logical_email_address: Optional[str] = None,
    since_internal_date_ms: Optional[int] = None,
    max_messages: int = 200,
) -> Tuple[List[RawEmail], Optional[int]]:
    """
    Fetch new Gmail messages for a given logical account.

    Args:
        account:
            Logical account key, e.g. 'kolasajosh', 'favourdesirous', 'ads.klix'.
            Used for token file name and stored on RawEmail.account.
        logical_email_address:
            Optional actual Gmail address (e.g. 'kolasajosh@gmail.com').
            If not provided, Gmail API will use the authenticated user ('me').
        since_internal_date_ms:
            If provided, only fetch messages with internalDate > this value.
        max_messages:
            Hard cap per run to avoid backlogs.

    Returns:
        (emails, last_internal_date_ms)
    """
    creds = _load_credentials_for_account(account)
    service = _build_gmail_service(creds)

    user_id = logical_email_address or "me"

    query_parts: List[str] = []
    if since_internal_date_ms is not None:
        # Gmail supports newer_than through "after:" (date), but for higher precision
        # we filter client-side based on internalDate > since_internal_date_ms.
        # Here we just pull recent results and post-filter.
        pass

    query = " ".join(query_parts).strip() or None

    emails: List[RawEmail] = []
    last_internal_date_ms: Optional[int] = since_internal_date_ms

    try:
        next_page_token: Optional[str] = None

        while True:
            list_kwargs: Dict[str, Any] = {
                "userId": user_id,
                "maxResults": min(max_messages, 500),
                "q": query,
            }
            if next_page_token:
                list_kwargs["pageToken"] = next_page_token

            resp = service.users().messages().list(**list_kwargs).execute()
            messages = resp.get("messages", []) or []

            if not messages:
                break

            for msg_meta in messages:
                if len(emails) >= max_messages:
                    break

                msg_id = msg_meta.get("id")
                if not msg_id:
                    continue

                msg = (
                    service.users()
                    .messages()
                    .get(
                        userId=user_id,
                        id=msg_id,
                        format="metadata",
                        metadataHeaders=[
                            "From",
                            "To",
                            "Subject",
                            "Date",
                            "Reply-To",
                        ],
                    )
                    .execute()
                )

                internal_date_str = msg.get("internalDate")
                try:
                    internal_date_ms_int = int(internal_date_str) if internal_date_str else None
                except (TypeError, ValueError):
                    internal_date_ms_int = None

                # Client-side filter for "newer than"
                if (
                    since_internal_date_ms is not None
                    and internal_date_ms_int is not None
                    and internal_date_ms_int <= since_internal_date_ms
                ):
                    continue

                raw_email = _message_to_raw_email(account=account, msg=msg)
                emails.append(raw_email)

                if internal_date_ms_int is not None:
                    if last_internal_date_ms is None or internal_date_ms_int > last_internal_date_ms:
                        last_internal_date_ms = internal_date_ms_int

            if len(emails) >= max_messages:
                break

            next_page_token = resp.get("nextPageToken")
            if not next_page_token:
                break

    except HttpError as e:
        logger.exception("Error fetching Gmail messages for account %s: %s", account, e)
        # In v1 we fail soft: return whatever we have and keep the old cursor.
        return emails, since_internal_date_ms

    return emails, last_internal_date_ms
