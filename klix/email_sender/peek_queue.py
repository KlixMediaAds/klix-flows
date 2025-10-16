import sqlite3, os
db = os.path.join("data","sends.db")
con = sqlite3.connect(db); c = con.cursor()

print("\n=== DUE NOW ===")
for s,r,t in c.execute("SELECT sender,recipient,send_time FROM send_queue WHERE datetime(send_time)<=datetime('now','localtime') ORDER BY send_time;"):
    print(f"{t}  {s} -> {r}")

print("\n=== UPCOMING (next 20) ===")
for s,r,t in c.execute("SELECT sender,recipient,send_time FROM send_queue WHERE datetime(send_time)>datetime('now','localtime') ORDER BY send_time LIMIT 20;"):
    print(f"{t}  {s} -> {r}")

print("\n=== CAPS ===")
for sender,cold,friendly,day,last in c.execute("SELECT sender,sent_cold_today,sent_friendly_today,day_index,last_sent_at FROM send_caps ORDER BY sender;"):
    print(f"{sender}: cold={cold} friendly={friendly} day={day} last={last}")
