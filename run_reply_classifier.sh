#!/usr/bin/env bash
set -euo pipefail

LIMIT="${1:-50}"

cd /opt/klix/klix-flows

# Activate venv
source .venv/bin/activate

# Load secrets
set -o allexport; source /etc/klix/secret.env; set +o allexport

echo "👉 Classifying up to $LIMIT replies..."
python scripts/classify_replies_once.py "$LIMIT"
