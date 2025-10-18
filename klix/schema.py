from sqlalchemy import (
    Column, BigInteger, String, Text, TIMESTAMP, ForeignKey,
    func, Index, UniqueConstraint, text
)
from sqlalchemy.orm import declarative_base, relationship
from sqlalchemy.dialects.postgresql import JSONB

Base = declarative_base()

class Lead(Base):
    __tablename__ = "leads"

    id = Column(BigInteger, primary_key=True)
    email = Column(String(320), index=True)
    company = Column(String(256))
    first_name = Column(String(128))
    last_name = Column(String(128))
    source = Column(String(64))

    discovered_at = Column(TIMESTAMP(timezone=True), server_default=func.now(), nullable=False)
    meta = Column(JSONB, nullable=False, server_default=text("'{}'::jsonb"))
    status = Column(String(32), nullable=False, server_default="NEW")

    # deterministic dedupe key: sha1(email + "\0" + company)
    dedupe_key = Column(String(64), index=True)

    __table_args__ = (
        Index("ix_leads_status_discovered_at", "status", "discovered_at"),
        UniqueConstraint("dedupe_key", name="uq_leads_dedupe_key"),
    )


class EmailSend(Base):
    __tablename__ = "email_sends"

    id = Column(BigInteger, primary_key=True)
    lead_id = Column(BigInteger, ForeignKey("leads.id", ondelete="CASCADE"), nullable=False)
    send_type = Column(String(32), nullable=False)  # friendly | cold
    subject = Column(Text)
    body = Column(Text)
    status = Column(String(32), nullable=False, server_default="queued")  # queued|sent|failed|skipped
    provider_message_id = Column(Text)
    error = Column(Text)
    created_at = Column(TIMESTAMP(timezone=True), server_default=func.now(), nullable=False)
    sent_at = Column(TIMESTAMP(timezone=True))

    lead = relationship("Lead")

    __table_args__ = (
        Index("ix_email_sends_status_created_at", "status", "created_at"),
        UniqueConstraint("lead_id", "send_type", name="uq_email_sends_lead_sendtype"),
    )
