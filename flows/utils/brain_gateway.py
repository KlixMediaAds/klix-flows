import os
import time
from typing import Optional

import psycopg2


def _build_dsn() -> str:
    """
    Build a plain Postgres DSN from DATABASE_URL.

    Example:
      postgresql+psycopg://user:pass@host/db -> postgresql://user:pass@host/db
    """
    url = os.environ["DATABASE_URL"]
    return url.replace("+psycopg", "")


DB_DSN = _build_dsn()


class BrainGateway:
    """
    Single entry point for all LLM calls.

    For this blueprint:
      - Returns a stubbed response.
      - Logs into llm_calls for telemetry.

    Later:
      - Swap stub for real OpenAI / local model.
      - Add advanced routing, retry, etc.
    """

    def __init__(self) -> None:
        # Keep it light; no persistent connection here.
        self.dsn = DB_DSN

    def generate(
        self,
        prompt: str,
        *,
        context_type: str,
        system: str = "",
        model: str = "gpt-4.1-mini",
        email_send_id: Optional[int] = None,
        lead_id: Optional[int] = None,
    ) -> str:
        t0 = time.time()
        success = False
        output = ""

        try:
            # TODO: later -> real model call
            prefix = f"[STUB:{context_type}] "
            output = prefix + (prompt or "")[:200]
            success = True
            return output
        finally:
            latency_ms = int((time.time() - t0) * 1000)

            # Fire-and-forget logging; failures here should not break caller.
            try:
                conn = psycopg2.connect(self.dsn)
                try:
                    with conn:
                        with conn.cursor() as cur:
                            cur.execute(
                                """
                                INSERT INTO llm_calls
                                (context_type, model_name, input_text, output_text,
                                 email_send_id, lead_id, success, latency_ms)
                                VALUES
                                (%s, %s, %s, %s,
                                 %s, %s, %s, %s)
                                """,
                                (
                                    context_type,
                                    model,
                                    prompt,
                                    output,
                                    email_send_id,
                                    lead_id,
                                    success,
                                    latency_ms,
                                ),
                            )
                finally:
                    conn.close()
            except Exception as e:
                # Minimal stderr logging; upgrade to proper logger later.
                print(f"[BrainGateway] failed to log llm_call via psycopg2: {e}")
