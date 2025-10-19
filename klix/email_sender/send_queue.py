import os, time
from datetime import datetime, time as dtime
from sqlalchemy import create_engine, text
from .main import send_email

ENG = create_engine(os.environ["DATABASE_URL"], pool_pre_ping=True, future=True)
DAILY_CAP = int(os.getenv("SEND_DAILY_CAP","50"))
LIVE = os.getenv("SEND_LIVE","0") == "1"

def within_window():
    # Server is UTC; set SEND_WINDOW accordingly (e.g., 13:00-21:00 for 09:00-17:00 Toronto)
    win = os.getenv("SEND_WINDOW","13:00-21:00")
    s,e = [tuple(map(int,x.split(":"))) for x in win.split("-")]
    now = datetime.now().time()
    return dtime(*s) <= now <= dtime(*e)

def send_queue(batch_size=25, allow_weekend=False):
    if not allow_weekend and datetime.utcnow().weekday()>=5:
        return 0
    if not within_window():
        return 0

    sent_today = 0
    with ENG.begin() as cx:
        rows = cx.execute(text("""
          select s.id as send_id, s.lead_id, s.subject, s.body, l.email
            from email_sends s
            join leads l on l.id = s.lead_id
           where s.status='queued'
           order by s.id asc
           limit :lim
        """), {"lim": batch_size}).mappings().all()

    for r in rows:
        if sent_today >= DAILY_CAP:
            break
        try:
            mid = "dry-run"
            if LIVE:
                mid = send_email(r["email"], r["subject"], r["body"]) or "unknown-id"
            with ENG.begin() as cx:
                cx.execute(text("""
                  update email_sends
                     set status='sent', provider_message_id=:mid, sent_at=now()
                   where id=:id
                """), {"mid": mid, "id": r["send_id"]})
            sent_today += 1
            time.sleep(1.2)  # light throttle
        except Exception as e:
            with ENG.begin() as cx:
                cx.execute(text("""
                  update email_sends
                     set status='failed', error=:err
                   where id=:id
                """), {"err": str(e)[:300], "id": r["send_id"]})
    return sent_today
