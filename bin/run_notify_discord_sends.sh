#!/usr/bin/env bash
cd /opt/klix/klix-flows
source .venv/bin/activate
set -o allexport; source /etc/klix/secret.env; set +o allexport
python bin/notify_discord_sends.py
