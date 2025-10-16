import os, gspread
from dotenv import load_dotenv

load_dotenv()

gc = gspread.service_account(filename=os.getenv("GOOGLE_SHEETS_CRED"))
sh = gc.open_by_key(os.getenv("SHEET_ID"))

try:
    ws = sh.worksheet("Leads")
except:
    ws = sh.add_worksheet(title="Leads", rows="1000", cols="20")
    ws.append_row([
        "name","niche_search","city","website","email","phone","address",
        "instagram","tiktok","site_title","site_desc","icebreaker","score",
        "source","first_seen","last_seen"
    ])

ws.append_row(["test","ok","","","","","","","","","","",0,"manual","",""])
print("✅ Sheets write OK")
