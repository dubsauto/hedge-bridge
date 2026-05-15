# run_worker.py
# Runs listener_manager and positions_tracker on separate threads,
# each with its own event loop. A crash in one does not affect the other.

import asyncio
import threading
import time
from dotenv import load_dotenv
load_dotenv()

from hedgebridge.listener_manager import listener_manager
from hedgebridge.positions_tracker import positions_tracker

RESTART_DELAY = 5  # seconds before restarting a crashed component


def _run_forever(name: str, coro_fn):
    """Run an async main-loop in this thread, restarting on any crash."""
    while True:
        try:
            print(f"[Worker] Starting {name}")
            asyncio.run(coro_fn())
        except Exception as e:
            print(f"[Worker] {name} crashed: {e} — restarting in {RESTART_DELAY}s")
        time.sleep(RESTART_DELAY)


if __name__ == "__main__":
    threads = [
        threading.Thread(
            target=_run_forever,
            args=("listener_manager", listener_manager.start),
            name="listener",
            daemon=True,
        ),
        threading.Thread(
            target=_run_forever,
            args=("positions_tracker", positions_tracker.run),
            name="tracker",
            daemon=True,
        ),
    ]

    for t in threads:
        t.start()

    print("[Worker] Both threads started — listener + tracker running independently")

    for t in threads:
        t.join()
