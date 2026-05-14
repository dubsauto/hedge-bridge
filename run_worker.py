# run_worker.py
#
# Standalone worker process — runs listener_manager and positions_tracker
# concurrently.  Start with:
#
#   python run_worker.py
#
# Both coroutines loop forever.  If either crashes it is restarted after a
# short delay.  SIGINT / SIGTERM triggers a clean shutdown.

import asyncio
import os
import signal
import sys

from dotenv import load_dotenv
load_dotenv()

from app.init_db import init_database
from app.model import Base
from app.database import engine
from hedgebridge.rpc_pool import rpc_pool
from hedgebridge.listener_manager import listener_manager
from hedgebridge.positions_tracker import positions_tracker

RESTART_DELAY = 5   # seconds to wait before restarting a crashed component


async def _run_with_restart(name: str, coro_factory):
    """Run a coroutine, restarting it on unexpected exit."""
    while True:
        print(f"[Worker] Starting {name}...")
        try:
            await coro_factory()
        except asyncio.CancelledError:
            print(f"[Worker] {name} cancelled — shutting down")
            return
        except Exception as e:
            print(f"[Worker] {name} crashed: {e} — restarting in {RESTART_DELAY}s")
            await asyncio.sleep(RESTART_DELAY)


async def main():
    # ── 1. Database ────────────────────────────────────────────────────────
    print("[Worker] Initialising database...")
    Base.metadata.create_all(bind=engine)   # ensure tracked_positions etc. exist
    await init_database()

    # ── 2. RPC pool watchdog ───────────────────────────────────────────────
    rpc_pool.start_watchdog()

    # ── 3. Graceful shutdown via SIGINT / SIGTERM ──────────────────────────
    loop = asyncio.get_running_loop()
    stop_event = asyncio.Event()

    def _handle_signal():
        if not stop_event.is_set():
            print("\n[Worker] Shutdown signal received")
            stop_event.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _handle_signal)
        except NotImplementedError:
            # Windows — use KeyboardInterrupt fallback
            pass

    # ── 4. Launch both components concurrently ─────────────────────────────
    listener_task = asyncio.create_task(
        _run_with_restart("listener_manager", listener_manager.start)
    )
    tracker_task = asyncio.create_task(
        _run_with_restart("positions_tracker", positions_tracker.run)
    )

    print("[Worker] Both components running. Press Ctrl+C to stop.")

    # Wait until a stop signal is received
    await stop_event.wait()

    # ── 5. Clean shutdown ──────────────────────────────────────────────────
    print("[Worker] Stopping components...")
    listener_task.cancel()
    tracker_task.cancel()
    positions_tracker.stop()

    await asyncio.gather(listener_task, tracker_task, return_exceptions=True)
    print("[Worker] Shutdown complete.")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
