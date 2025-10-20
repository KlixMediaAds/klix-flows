def build_email_for_lead(lead: dict):
    """
    Must return a 4-tuple: (subject, body_text, body_html, send_type)
    The flow will insert body_text into the DB 'body' column if that's what exists.
    """
    email = (lead.get("email") or "").strip()
    company = (lead.get("company") or "").strip()
    local = (email.split("@", 1)[0] or "there").replace(".", " ").title()
    domain = (email.split("@", 1)[1] or "").lower() if "@" in email else ""

    # simple subject
    subject = f"{company or local}: quick idea to boost inquiries"

    # plain-text body
    body_text = (
        f"Hi {local},\n\n"
        "This message comes from the minimal Klix builder. "
        "We're validating the builder → queue → sender pipeline.\n\n"
        "If you received this, the system works end-to-end.\n\n"
        "— Klix"
    )

    # lightweight html version (ok if your flow ignores it)
    body_html = (
        f"<p>Hi {local},</p>"
        "<p>This message comes from the <b>minimal Klix builder</b>. "
        "We're validating the builder → queue → sender pipeline.</p>"
        "<p>If you received this, the system works end-to-end.</p>"
        "<p>— Klix</p>"
    )

    # infer send_type (friendly for your test domains)
    friendly_domains = {"klixads.org", "gmail.com"}
    send_type = "friendly" if domain in friendly_domains else "cold"

    return subject, body_text, body_html, send_type
