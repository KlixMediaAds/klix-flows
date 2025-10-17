# app/sender_engine/__init__.py
from .cli import parse_args
from .config import load_context, ensure_business_hours, init_datastores
from .caps import resolve_active_senders
from .pools import build_pools
from .execute import dispatch
from .audit import flush_summary

SENDER_ENGINE_VERSION = "1.0.0"

def run_main():
    args = parse_args()
    ctx = load_context(args)
    print(f"[BOOT] sender_engine v{SENDER_ENGINE_VERSION} tz={ctx.tz.key}")
    if not ensure_business_hours(ctx):
        print(f"[EXIT] Outside business hours {ctx.BH_START}-{ctx.BH_END} {ctx.tz.key}; now={ctx.now_local.isoformat()}")
        return
    init_datastores(ctx)                # init db + connect sheets
    active = resolve_active_senders(ctx)
    if not active:
        print("[EXIT] No active senders available (all disabled, paused, or capped).")
        return
    pools = build_pools(ctx, active)    # followups + new_pool (+ maybe early cold fallback signal)
    try:
        dispatch(ctx, active, pools)
    except KeyboardInterrupt:
        print("\n[INTERRUPT] flushing summary…")
    finally:
        try:
            flush_summary(ctx, active)
        except Exception as e:
            print(f"[WARN] summary flush failed: {e}")

# optional: importable API for Prefect or other callers
def run_with_params(*, mode="mixed", ratio="60:40", limit=None, drip_per_run=None, dry_run=False):
    from types import SimpleNamespace
    args = SimpleNamespace(mode=mode, ratio=ratio, limit=limit, drip_per_run=drip_per_run,
                           dry_run=dry_run, verbose=False, min_delay=None, max_delay=None,
                           allow_wait_secs=None)
    ctx = load_context(args)
    if not ensure_business_hours(ctx):
        return
    init_datastores(ctx)
    active = resolve_active_senders(ctx)
    if not active:
        return
    pools = build_pools(ctx, active)
    try:
        dispatch(ctx, active, pools)
    finally:
        try:
            flush_summary(ctx, active)
        except Exception:
            pass
