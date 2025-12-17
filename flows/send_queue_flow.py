"""
Compatibility wrapper so the existing 'send-queue' deployment entrypoint
(flows/send_queue_flow.py:send_queue_flow) now uses the v2 engine.
"""

from flows.send_queue_v2 import send_queue_v2_flow as send_queue_flow
