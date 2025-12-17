"""
DEPRECATED (Hard Fail)

This flow was an early prototype that inserts directly into email_sends.
It is intentionally disabled to prevent bypassing verification + hygiene gates.

Use:
- flows/email_verifier_flow.py
- flows/email_builder_flow.py
- flows/send_queue_v2.py
"""
raise RuntimeError(
    "flows/email_queue_flow.py is DEPRECATED and disabled. "
    "Use email_verifier_flow → email_builder_flow → send_queue_v2."
)
