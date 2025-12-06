import os
from typing import Any, Dict, List, Optional, Tuple

from sqlalchemy import text
from prefect import flow, get_run_logger

from klix.db import SessionLocal
from klix.email_spine_gmail import fetch_new_emails, RawEmail
from klix.email_spine_classifier import classify_email, ClassifiedEmail

# ---------------------------------------------------------------------------
# Optional Discord helper
# ---------------------------------------------------------------------------
try:
    from libs.discord import post_discord
except Exception:  # pragma: no cover
    def post_discord(_msg: str) -> None:  # type: ignore
        # no-op if helper not available; avoid crashing the flow
        pass


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

# Logical accounts we poll; token files expected at:
#   $GMAIL_TOKENS_DIR/{account}.json
EMAIL_SPINE_ACCOUNTS: List[str] = [
    "kolasajosh",
    "favourdesirous",
    "ads.klix",
]

# Max messages per account per run (safety cap)
MAX_MESSAGES_PER_ACCOUNT_DEFAULT = int(
    os.getenv("EMAIL_SPINE_MAX_MESSAGES_PER_ACCOUNT", "200")
)


# ---------------------------------------------------------------------------
# DB Helpers
# ---------------------------------------------------------------------------


def _get_last_internal_date_ms(session, account: str) -> Optional[int]:
    row = session.execute(
        text(
            "SELECT last_internal_date_ms "
            "FROM email_poll_state "
            "WHERE account = :account"
        ),
        {"account": account},
    ).fetchone()
    return None if row is None else row[0]


def _set_last_internal_date_ms(session, account: str, last_ms: int) -> None:
    session.execute(
        text(
            """
            INSERT INTO email_poll_state (account, last_internal_date_ms, updated_at)
            VALUES (:account, :last_internal_date_ms, now())
            ON CONFLICT (account)
            DO UPDATE SET
                last_internal_date_ms = EXCLUDED.last_internal_date_ms,
                updated_at = now()
            """
        ),
        {"account": account, "last_internal_date_ms": last_ms},
    )


def _upsert_email_event(
    session,
    raw: RawEmail,
    classified: ClassifiedEmail,
) -> int:
    """
    Insert/update a row in email_events and return its id.

    Idempotent per (account, gmail_message_id) via unique constraint.
    """
    is_unread = "UNREAD" in (raw.labels or [])

    row = session.execute(
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
            ON CONFLICT (account, gmail_message_id)
            DO UPDATE SET
                signal_type = EXCLUDED.signal_type,
                importance_score = EXCLUDED.importance_score,
                requires_host_attention = EXCLUDED.requires_host_attention,
                raw_labels = EXCLUDED.raw_labels,
                metadata = EXCLUDED.metadata,
                raw_payload = EXCLUDED.raw_payload
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
            "metadata": classified.metadata,
            "raw_payload": raw.payload,
        },
    ).fetchone()

    return int(row[0])


def _insert_financial_email_event(
    session,
    email_event_id: int,
    financial_event: Dict[str, Any],
) -> None:
    session.execute(
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
            "provider": financial_event.get("provider"),
            "event_type": financial_event.get("event_type"),
            "amount": financial_event.get("amount"),
            "currency": financial_event.get("currency") or "USD",
            "billing_period": financial_event.get("billing_period"),
            "notes": financial_event.get("notes"),
            "raw_payload": financial_event.get("raw_payload"),
        },
    )


def _post_critical_alert(
    account: str, raw: RawEmail, classified: ClassifiedEmail
) -> None:
    """
    Immediate Discord alert for critical email events (failed payment, security).
    """
    try:
        msg = (
            "🚨 *Email Spine Critical Event*\n"
            f"- Account: `{account}`\n"
            f"- Type: `{classified.signal_type}` (score={classified.importance_score})\n"
            f"- Subject: {raw.subject or '(no subject)'}\n"
        )
        post_discord(msg)
    except Exception:
        # keep the flow alive; failures here shouldn't kill polling
        pass


def _post_digest(digest_items: List[Tuple[str, RawEmail, ClassifiedEmail]]) -> None:
    """
    Digest message summarizing all requires_host_attention events in this run.
    """
    if not digest_items:
        return

    try:
        lines: List[str] = []
        for account, raw, classified in digest_items[:20]:
            subj = (raw.subject or "").strip()
            if len(subj) > 120:
                subj = subj[:117] + "..."
            lines.append(
                f"- `{account}` · `{classified.signal_type}` ({classified.importance_score}) · {subj}"
            )

        msg = "📬 *Email Spine Digest (latest poller run)*\n" + "\n".join(lines)
        post_discord(msg)
    except Exception:
        # don't break the flow if Discord is down
        pass


# ---------------------------------------------------------------------------
# Flow
# ---------------------------------------------------------------------------


@flow(name="email-spine-poller")
def email_spine_poller_flow(
    max_messages_per_account: int = MAX_MESSAGES_PER_ACCOUNT_DEFAULT,
) -> None:
    """
    Unified Email Spine v1 — poll Gmail for multiple accounts, classify events,
    write into email_events / financial_email_events, and emit Discord alerts.

    v1 is read-only, alert-only. No actions in Gmail itself.
    """
    logger = get_run_logger()
    digest_items: List[Tuple[str, RawEmail, ClassifiedEmail]] = []

    with SessionLocal() as session:
        for account in EMAIL_SPINE_ACCOUNTS:
            logger.info("Polling Gmail for account=%s", account)

            last_ms = _get_last_internal_date_ms(session, account)
            emails, new_last_ms = fetch_new_emails(
                account=account,
                since_internal_date_ms=last_ms,
                max_messages=max_messages_per_account,
            )

            logger.info(
                "Account=%s: fetched %d messages (since internalDate=%s)",
                account,
                len(emails),
                last_ms,
            )

            for raw in emails:
                classified = classify_email(raw)
                email_event_id = _upsert_email_event(session, raw, classified)

                if classified.financial_event:
                    _insert_financial_email_event(
                        session, email_event_id, classified.financial_event
                    )

                if classified.requires_host_attention:
                    digest_items.append((account, raw, classified))

                if classified.signal_type in (
                    "finance_failed_payment",
                    "security",
                ):
                    _post_critical_alert(account, raw, classified)

            if new_last_ms is not None and new_last_ms != last_ms:
                _set_last_internal_date_ms(session, account, new_last_ms)

        session.commit()

    # Digest after DB commit
    _post_digest(digest_items)


if __name__ == "__main__":
    email_spine_poller_flow()
