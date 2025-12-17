#!/usr/bin/env bash
set -euo pipefail

# How many emails to build into the queue (default = 5)
LIMIT="${1:-5}"

cd /opt/klix/klix-flows
source .venv/bin/activate
set -o allexport; source /etc/klix/secret.env; set +o allexport

echo "👉 Building up to $LIMIT emails into email_sends (status='queued')..."

python - << PY
from flows.email_builder_flow import email_builder

if __name__ == "__main__":
    # LIMIT is expanded by bash before this heredoc is sent to Python
    email_builder(limit=$LIMIT)
PY

echo "✅ Finished building emails into email_sends."
