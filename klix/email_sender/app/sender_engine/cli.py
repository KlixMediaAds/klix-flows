# app/sender_engine/cli.py
import argparse

def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--limit", type=int, default=None, help="Max attempts this run")
    p.add_argument("--drip-per-run", type=int, default=None,
                   help="Cap total COLD (new+followups) this run; friendlies unaffected by this flag")
    p.add_argument("--mode", choices=["mixed","friendlies","cold"], default="mixed",
                   help="mixed = cold:friendlies (e.g., 60:40), friendlies = only friendly emails, cold = only new")
    p.add_argument("--ratio", default="60:40", help="COLD:FRIENDLIES ratio for mixed mode (default 60:40)")
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--verbose", action="store_true")
    p.add_argument("--min-delay", type=float, default=None)
    p.add_argument("--max-delay", type=float, default=None)
    p.add_argument("--allow-wait-secs", type=int, default=None,
                   help="If nothing is due yet, wait up to N seconds for the first due follow-up, then send")
    return p.parse_args()
