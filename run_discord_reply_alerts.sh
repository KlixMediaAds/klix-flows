#!/usr/bin/env bash
cd /opt/klix/klix-flows
source .venv/bin/activate
set -a
source /etc/klix/secret.env
set +a
python discord_reply_alerts.py >> /var/log/discord_reply_alerts.log 2>&1
