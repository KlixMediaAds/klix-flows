#!/usr/bin/env bash
set -euo pipefail

# 1) Activate venv
cd /opt/klix
source venv/bin/activate
cd klix-flows

# 2) Load your existing .env (SMTP_USER etc.)
if [ -f /opt/klix/.env ]; then
  echo "[run_send_queue_v2] Loading /opt/klix/.env"
  set -a
  source /opt/klix/.env
  set +a
else
  echo "[run_send_queue_v2] WARNING: /opt/klix/.env not found; env may be incomplete." >&2
fi

# 3) Send logs to local Prefect UI (optional but nice)
export PREFECT_API_URL="http://127.0.0.1:4200/api"

# 4) Run the flow directly, but monkeypatch _check_window to always allow
python - << 'PY'
import os, sys, pathlib
from datetime import datetime

ROOT = pathlib.Path("/opt/klix/klix-flows").resolve()
FLOW_DIR = ROOT / "flows"

if str(FLOW_DIR) not in sys.path:
    sys.path.insert(0, str(FLOW_DIR))

import send_queue_v2  # this is flows/send_queue_v2.py, due to sys.path tweak

print("[run_send_queue_v2] NOW:", datetime.now().isoformat())
print("[run_send_queue_v2] ENV SMTP_USER:", os.environ.get("SMTP_USER"))
print("[run_send_queue_v2] MODULE FROM_ADDR:", getattr(send_queue_v2, "FROM_ADDR", None))
print("[run_send_queue_v2] MODULE SMTP_USER:", getattr(send_queue_v2, "SMTP_USER", None))

# 🔓 BYPASS TIME WINDOW JUST FOR THIS MANUAL RUN
send_queue_v2._check_window = lambda now: True

result = send_queue_v2.send_queue_v2_flow(
    batch_size=1,
    allow_weekend=True,  # irrelevant to window, but safe
    dry_run=False,       # real send; KLIX_TEST_MODE can still short-circuit
)

print(f"[send_queue_v2 manual] run completed, sent={result}")
PY
