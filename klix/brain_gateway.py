import os
import time
import logging
from contextlib import contextmanager
from typing import Optional

from sqlalchemy import text
from openai import OpenAI, OpenAIError

# Reuse the normalized engine / SessionLocal from klix.db
from klix.db import SessionLocal

logger = logging.getLogger(__name__)

"""
BrainGateway — Era 1.9.5 Telemetry Spine

This module is the canonical way KlixOS talks to LLMs and logs telemetry
into `llm_calls`. It is part of the "Intelligence-Ready Spine v1".

Key guarantees:

1. All LLM calls are logged to `llm_calls`.
2. `context_type` follows a SMALL, DOCUMENTED enum so Era 2 agents
   can filter calls without regex archaeology.
3. DB access is centralized via `klix.db.SessionLocal` (no custom engines).

Current `context_type` enum (v1):

  - "builder_cold_email"      → cold email generation
  - "builder_followup"        → follow-up email generation
  - "reply_classification"    → reply classifier / hygiene
  - "internal_diagnostic"     → internal tests / diagnostics
  - "telemetry_smoke_test"    → one-off or scheduled telemetry checks
  - "ops_healthcheck"         → model/infra health probes

Anything outside this set is considered "non-standard" and will be logged
with a WARNING, but still written to the DB (no hard failures).
"""

# --- context_type enum -------------------------------------------------

ALLOWED_CONTEXT_TYPES = {
    "builder_cold_email",
    "builder_followup",
    "reply_classification",
    "internal_diagnostic",
    "telemetry_smoke_test",
    "ops_healthcheck",
}


def _normalize_context_type(raw: Optional[str]) -> Optional[str]:
    """
    Normalize and validate context_type.

    Rules:
      - If None → default to "internal_diagnostic".
      - If in ALLOWED_CONTEXT_TYPES → return as-is.
      - If unknown → log a warning, but *do not* mutate it
        (so we keep full fidelity in the DB while nudging
         callers to align with the enum).
    """
    if raw is None:
        return "internal_diagnostic"

    if raw in ALLOWED_CONTEXT_TYPES:
        return raw

    logger.warning(
        "BrainGateway called with non-standard context_type=%r. "
        "Consider updating it to one of: %s",
        raw,
        sorted(ALLOWED_CONTEXT_TYPES),
    )
    return raw


# --- DB session helper (wraps SessionLocal) ---------------------------

@contextmanager
def db_session():
    """
    Context manager for DB sessions using the shared SessionLocal
    configured in klix.db (which already normalizes DATABASE_URL).
    """
    session = SessionLocal()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


# --- OpenAI client (lazy init) ---------------------------------------

def _build_client() -> OpenAI:
    """
    Lazily construct the OpenAI client.

    Avoids crashing at import-time when OPENAI_API_KEY is missing.
    """
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        raise OpenAIError(
            "OPENAI_API_KEY is not set. "
            "Set it in the environment before calling BrainGateway.generate()."
        )
    return OpenAI(api_key=api_key)


class BrainGateway:
    """
    Thin wrapper around OpenAI that:
      1) Calls the model
      2) Logs to llm_calls
      3) Returns output_text

    Usage example:

        brain_gateway.generate(
            prompt=raw_email_body,
            system="classify reply",
            context_type="reply_classification",
            email_send_id=123,
            lead_id=456,
        )
    """

    def __init__(self, client_instance: Optional[OpenAI] = None):
        self._client: Optional[OpenAI] = client_instance

    @property
    def client(self) -> OpenAI:
        if self._client is None:
            self._client = _build_client()
        return self._client

    def generate(
        self,
        prompt: str,
        system: str,
        model: str = "gpt-4.1-mini",
        context_type: Optional[str] = None,
        email_send_id: Optional[int] = None,
        lead_id: Optional[int] = None,
        **params,
    ) -> str:
        """
        Simple text-in → text-out wrapper.

        Telemetry contract (v1):

          - context_type: see ALLOWED_CONTEXT_TYPES above.
          - email_send_id: set for any call directly tied to a specific send.
          - lead_id: optional, used when the call is lead-centric.

        The function will:
          - Call OpenAI Chat Completions.
          - Log a row in `llm_calls` with input/output, timing, and linkage.
          - Raise RuntimeError if the underlying OpenAI call fails.
        """
        norm_context_type = _normalize_context_type(context_type)

        input_text = f"[system]\n{system}\n\n[user]\n{prompt}"

        start = time.perf_counter()
        success = True
        output_text = ""

        try:
            # New-style OpenAI SDK (1.x+)
            response = self.client.chat.completions.create(
                model=model,
                messages=[
                    {"role": "system", "content": system},
                    {"role": "user", "content": prompt},
                ],
                **params,
            )

            output_text = response.choices[0].message.content

        except Exception as e:
            success = False
            output_text = f"[ERROR] {type(e).__name__}: {e}"
            logger.exception("BrainGateway.generate failed")

        latency_ms = int((time.perf_counter() - start) * 1000)

        # --- Log to llm_calls -----------------------------------------
        try:
            with db_session() as session:
                session.execute(
                    text(
                        """
                        INSERT INTO llm_calls (
                            context_type,
                            model_name,
                            input_text,
                            output_text,
                            email_send_id,
                            lead_id,
                            success,
                            latency_ms
                        )
                        VALUES (
                            :context_type,
                            :model_name,
                            :input_text,
                            :output_text,
                            :email_send_id,
                            :lead_id,
                            :success,
                            :latency_ms
                        )
                        """
                    ),
                    {
                        "context_type": norm_context_type,
                        "model_name": model,
                        "input_text": input_text,
                        "output_text": output_text,
                        "email_send_id": email_send_id,
                        "lead_id": lead_id,
                        "success": success,
                        "latency_ms": latency_ms,
                    },
                )
        except Exception:
            logger.exception("Failed to log llm_call")

        if not success:
            raise RuntimeError("BrainGateway.generate failed, see logs / llm_calls")

        return output_text


# Shared singleton you can import in flows / scripts:
brain_gateway = BrainGateway()
