#!/usr/bin/env bash
set -euo pipefail

cd /opt/klix/klix-flows
source .venv/bin/activate
set -o allexport; source /etc/klix/secret.env; set +o allexport

python scripts/daily_health_report.py
