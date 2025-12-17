from __future__ import annotations

import os
import json
from datetime import datetime, timezone
from typing import Dict, List, Optional

from prefect import flow, task, get_run_logger
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from klix.email_spine_gmail import RawEmail, fetch_messages_since
from klix.email_spine_classifier import (
    ClassifiedEmail,
    FinancialEvent,
    classify_email,
)

# Optional integrations: governance + Discord
try:  # pragma: no cover
    from klix.governance import record_incident  # type: ignore[attr-defined]
except Exception:  # pragma: no cover
    record_incident = None  # type: ignore[assignment]

try:  # pragma: no cover
    from klix.discord import send_discord_message  # type: ignore[attr-defined]
except Exception:  # pragma: no cover
    send_discord_message = None  # type: ignore[assignment]


ACCOUNTS: List[str] = [
    # PERSONAL / CORE INBOXES
    "kolasajosh",
    "favourdesirous",
    "ads.klix",

    # SPINE INBOX FOR KLIXADS.CA
    "spine_klixads",
]


# --- DB helpers --------------------------------------------------------------


def _get_engine() -> Engine:
    db_url = os.environ["DATABASE_URL"]
    return create_engine(db_url, future=True)


def _fetch_last_received_at(engine: Engine, account: str) -> Optional[datetime]:
    """
    Read the last_received_at cursor for this account from email_poll_state.
    """
    with engine.connect() as conn:
        row = conn.execute(
            text(
                """
                SELECT last_received_at
                FROM email_poll_state
                WHERE account = :account
                """
            ),
            {"account": account},
        ).one_or_none()

    if not row:
        return None
    value = row[0]
    # Ensure tz-aware in UTC
    if value is not None and value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value


def _upsert_last_received_at(engine: Engine, account: str, last_received_at: datetime) -> None:
    """
    Upsert the cursor for this account.
    """
    if last_received_at.tzinfo is None:
        last_received_at = last_received_at.replace(tzinfo=timezone.utc)

    with engine.begin() as conn:
        conn.execute(
            text(
                """
                INSERT INTO email_poll_state (account, last_received_at)
                VALUES (:account, :last_received_at)
                ON CONFLICT (account) DO UPDATE
                SET last_received_at = EXCLUDED.last_received_at
                """
            ),
            {"account": account, "last_received_at": last_received_at},
        )


def _insert_email_event(
    engine: Engine,
    raw: RawEmail,
    classified: ClassifiedEmail,
) -> int:
    """
    Insert a row into email_events and return its id.
    """
    is_unread = "UNREAD" in (raw.labels or [])

    # Serialize dicts to JSON strings so Postgres jsonb can consume them
    metadata_json = json.dumps({})
    raw_payload_json = json.dumps(raw.payload or {})

    with engine.begin() as conn:
        res = conn.execute(
            text(
                """
                INSERT INTO email_events (
                    received_at,
                    account,
                    provider,
                    gmail_message_id,
                    gmail_thread_id,
                    from_address,
                    to_address,
                    subject,
                    snippet,
                    raw_labels,
                    signal_type,
                    importance_score,
                    requires_host_attention,
                    is_unread_at_poll,
                    metadata,
                    raw_payload
                )
                VALUES (
                    :received_at,
                    :account,
                    :provider,
                    :gmail_message_id,
                    :gmail_thread_id,
                    :from_address,
                    :to_address,
                    :subject,
                    :snippet,
                    :raw_labels,
                    :signal_type,
                    :importance_score,
                    :requires_host_attention,
                    :is_unread_at_poll,
                    :metadata,
                    :raw_payload
                )
                RETURNING id
                """
            ),
            {
                "received_at": raw.received_at,
                "account": raw.account,
                "provider": raw.provider,
                "gmail_message_id": raw.message_id,
                "gmail_thread_id": raw.thread_id,
                "from_address": raw.from_address,
                "to_address": raw.to_address,
                "subject": raw.subject,
                "snippet": raw.snippet,
                "raw_labels": raw.labels,
                "signal_type": classified.signal_type,
                "importance_score": classified.importance_score,
                "requires_host_attention": classified.requires_host_attention,
                "is_unread_at_poll": is_unread,
                "metadata": metadata_json,
                "raw_payload": raw_payload_json,
            },
        )
        email_event_id = res.scalar_one()
    return int(email_event_id)


def _insert_financial_event(
    engine: Engine,
    email_event_id: int,
    financial: FinancialEvent,
) -> None:
    # Keep raw_payload minimal for now; still serialize to JSON
    financial_raw_payload_json = json.dumps({})

    with engine.begin() as conn:
        conn.execute(
            text(
                """
                INSERT INTO financial_email_events (
                    email_event_id,
                    provider,
                    event_type,
                    amount,
                    currency,
                    billing_period,
                    notes,
                    raw_payload
                )
                VALUES (
                    :email_event_id,
                    :provider,
                    :event_type,
                    :amount,
                    :currency,
                    :billing_period,
                    :notes,
                    :raw_payload
                )
                """
            ),
            {
                "email_event_id": email_event_id,
                "provider": financial.provider,
                "event_type": financial.event_type,
                "amount": financial.amount,
                "currency": financial.currency,
                "billing_period": financial.billing_period,
                "notes": financial.notes,
                "raw_payload": financial_raw_payload_json,
            },
        )


def _maybe_record_incident_for_email(
    raw: RawEmail,
    classified: ClassifiedEmail,
    email_event_id: int,
) -> None:
    """
    For critical email events (failed payments, security alerts),
    optionally record a governance incident if the helper is available.
    """
    if record_incident is None:  # type: ignore[truthy-function]
        return

    if classified.signal_type not in {"finance_failed_payment", "security"}:
        return

    # Governance schema is defined elsewhere; here we just forward a structured payload.
    try:  # pragma: no cover
        record_incident(
            source="email_spine_poller",
            category=classified.signal_type,
            severity="high",
            message=f"Critical email event {classified.signal_type} for account {raw.account}",
            metadata={
                "email_event_id": email_event_id,
                "subject": raw.subject,
                "from_address": raw.from_address,
                "importance_score": classified.importance_score,
            },
        )
    except Exception:
        # Never let governance instrumentation break the poller
        logger = get_run_logger()
        logger.exception("Failed to record governance incident for email_event_id=%s", email_event_id)


def _maybe_send_discord_alert(
    raw: RawEmail,
    classified: ClassifiedEmail,
    email_event_id: int,
) -> None:
    """
    For v1, send Discord alerts only for clearly critical events, if the helper is available.
    """
    if send_discord_message is None:  # type: ignore[truthy-function]
        return

    if classified.signal_type not in {"finance_failed_payment", "security", "lead"}:
        return

    try:  # pragma: no cover
        title_map: Dict[str, str] = {
            "finance_failed_payment": "⚠️ Failed Payment Detected",
            "security": "🔐 Security Alert Email",
            "lead": "📩 Lead Reply Detected",
        }
        title = title_map.get(classified.signal_type, "📬 Email Event")

        content = (
            f"{title}\n"
            f"- Account: `{raw.account}`\n"
            f"- From: `{raw.from_address}`\n"
            f"- Subject: `{raw.subject}`\n"
            f"- Importance: `{classified.importance_score}`\n"
            f"- Email Event ID: `{email_event_id}`"
        )

        send_discord_message("email_spine", content)
    except Exception:
        logger = get_run_logger()
        logger.exception("Failed to send Discord alert for email_event_id=%s", email_event_id)


# --- Prefect tasks -----------------------------------------------------------


@task
def process_account(account: str) -> int:
    """
    Poll Gmail for a single logical account, classify, and persist all new messages.

    Returns number of new email_events inserted.
    """
    logger = get_run_logger()
    engine = _get_engine()

    last_received_at = _fetch_last_received_at(engine, account)
    logger.info("Email spine: last_received_at for %s is %s", account, last_received_at)

    raw_messages: List[RawEmail] = fetch_messages_since(account, last_received_at)

    if not raw_messages:
        logger.info("Email spine: no new messages for account=%s", account)
        return 0

    inserted = 0
    newest_seen: Optional[datetime] = last_received_at

    for raw in raw_messages:
        classified: ClassifiedEmail = classify_email(raw)

        email_event_id = _insert_email_event(engine, raw, classified)
        inserted += 1

        if classified.financial_event is not None:
            _insert_financial_event(engine, email_event_id, classified.financial_event)

        _maybe_record_incident_for_email(raw, classified, email_event_id)
        _maybe_send_discord_alert(raw, classified, email_event_id)

        # Track newest received_at for cursor update
        if newest_seen is None or raw.received_at > newest_seen:
            newest_seen = raw.received_at

    if newest_seen is not None:
        _upsert_last_received_at(engine, account, newest_seen)

    logger.info(
        "Email spine: processed %d messages for account=%s, new cursor=%s",
        inserted,
        account,
        newest_seen,
    )
    return inserted


# --- Prefect flow ------------------------------------------------------------


@flow(name="email-spine-poller")
def email_spine_poller_flow() -> Dict[str, int]:
    """
    Unified Email Spine v1 — multi-inbox poller.

    - Polls all configured accounts.
    - Normalizes & classifies into email_events (+ financial_email_events).
    - Emits governance incidents + Discord alerts for critical events.
    """
    logger = get_run_logger()
    summary: Dict[str, int] = {}

    for account in ACCOUNTS:
        try:
            count = process_account.submit(account).result()
            summary[account] = count
        except Exception as exc:  # pragma: no cover
            logger.exception("Email spine: error while processing account=%s: %s", account, exc)
            summary[account] = -1

    logger.info("Email spine: poller summary: %s", summary)
    return summary


if __name__ == "__main__":  # pragma: no cover
    # Allow ad-hoc CLI execution:
    email_spine_poller_flow()
