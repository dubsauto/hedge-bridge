# hedgebridge/positions_tracker.py
#
# Standalone safety-net process.
# Polls master accounts and verifies that every open master position has been
# replicated to all slave accounts within REPLICATION_WINDOW seconds.
# If replication is incomplete after that window, closes the master trade AND
# every slave that did receive the copy.
#
# Uses the streaming connections already maintained by listener_manager
# (connection_store) — no separate RPC connections needed.

import asyncio
import os
import sys
from datetime import datetime
from typing import List, Dict

from sqlalchemy.orm import Session

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from dotenv import load_dotenv
load_dotenv()

from app.database import SessionLocal
from app.model import (
    TradingAccount,
    CopyRelationship,
    CopyTradeLink,
    TrackedPosition,
)
from hedgebridge.connection_store import get_connection

# ─── Tuning ────────────────────────────────────────────────────────────────
REPLICATION_WINDOW: int = int(os.getenv("TRACKER_REPLICATION_WINDOW", "10"))
POLL_INTERVAL: int      = int(os.getenv("TRACKER_POLL_INTERVAL", "5"))
CLOSE_TIMEOUT: int      = 15
GET_TIMEOUT: int        = 8
# ───────────────────────────────────────────────────────────────────────────


class PositionsTracker:
    def __init__(self):
        self._running = False
        self._intervening: set = set()

    # ─── Streaming-connection helpers ──────────────────────────────────────

    async def _get_positions(self, metaapi_account_id: str) -> List[dict]:
        """Fetch positions via the live streaming connection."""
        try:
            conn = get_connection(metaapi_account_id)
            if conn is None:
                return []
            positions = await asyncio.wait_for(
                conn.get_positions(), timeout=GET_TIMEOUT
            )
            return positions or []
        except Exception as e:
            print(f"[Tracker] get_positions failed {metaapi_account_id}: {e}")
            return []

    async def _close_position(self, metaapi_account_id: str, ticket: str) -> bool:
        """Close a position via the live streaming connection."""
        try:
            conn = get_connection(metaapi_account_id)
            if conn is None:
                print(f"[Tracker] No connection for {metaapi_account_id} — cannot close {ticket}")
                return False
            await asyncio.wait_for(
                conn.close_position(ticket), timeout=CLOSE_TIMEOUT
            )
            return True
        except Exception as e:
            print(f"[Tracker] close_position failed {metaapi_account_id}/{ticket}: {e}")
            return False

    # ─── Emergency close ───────────────────────────────────────────────────

    async def _emergency_close(
        self,
        master_acc: TradingAccount,
        master_ticket: str,
        slave_links: List[CopyTradeLink],
    ):
        key = f"{master_acc.id}:{master_ticket}"
        if key in self._intervening:
            return
        self._intervening.add(key)

        try:
            print(
                f"🚨 [Tracker] EMERGENCY CLOSE — master={master_acc.id} "
                f"ticket={master_ticket} slaves={[l.slave_account_id for l in slave_links]}"
            )

            slave_ids = [l.slave_account_id for l in slave_links if l.slave_ticket]
            db = SessionLocal()
            try:
                slave_accs: Dict[int, TradingAccount] = {
                    acc.id: acc
                    for acc in db.query(TradingAccount)
                    .filter(TradingAccount.id.in_(slave_ids))
                    .all()
                }
            finally:
                db.close()

            tasks = []
            meta = []

            tasks.append(
                self._close_position(master_acc.metaapi_account_id, master_ticket)
            )
            meta.append(("master", master_acc.id, master_ticket))

            for link in slave_links:
                if not link.slave_ticket:
                    continue
                slave_acc = slave_accs.get(link.slave_account_id)
                if not slave_acc or not slave_acc.metaapi_account_id:
                    continue
                tasks.append(
                    self._close_position(
                        slave_acc.metaapi_account_id, link.slave_ticket
                    )
                )
                meta.append(("slave", link.slave_account_id, link.slave_ticket))

            results = await asyncio.gather(*tasks, return_exceptions=True)

            for (role, acc_id, ticket), result in zip(meta, results):
                ok = result is True
                print(
                    f"[Tracker] Close {role} acc={acc_id} ticket={ticket}: "
                    f"{'OK' if ok else f'FAILED ({result})'}"
                )

            write_db = SessionLocal()
            try:
                links_to_close = (
                    write_db.query(CopyTradeLink)
                    .filter(
                        CopyTradeLink.master_account_id == master_acc.id,
                        CopyTradeLink.master_ticket == master_ticket,
                        CopyTradeLink.status == "open",
                    )
                    .all()
                )
                now = datetime.utcnow()
                for l in links_to_close:
                    l.status = "closed"
                    l.closed_at = now

                tracked = (
                    write_db.query(TrackedPosition)
                    .filter_by(
                        master_account_id=master_acc.id,
                        master_ticket=master_ticket,
                    )
                    .first()
                )
                if tracked:
                    tracked.closed_by_tracker = True
                    tracked.intervention_at = now

                write_db.commit()
                print(f"[Tracker] DB updated for ticket={master_ticket}")
            except Exception as e:
                write_db.rollback()
                print(f"[Tracker] DB update failed: {e}")
            finally:
                write_db.close()

        finally:
            self._intervening.discard(key)

    # ─── Per-master check ──────────────────────────────────────────────────

    async def _check_master(
        self,
        master_acc: TradingAccount,
        expected_slave_ids: set,
    ):
        positions = await self._get_positions(master_acc.metaapi_account_id)
        if not positions:
            return

        now = datetime.utcnow()
        db = SessionLocal()
        try:
            for pos in positions:
                ticket = str(pos.get("id") or pos.get("ticket"))

                tracked = (
                    db.query(TrackedPosition)
                    .filter_by(
                        master_account_id=master_acc.id,
                        master_ticket=ticket,
                    )
                    .first()
                )

                if not tracked:
                    try:
                        tracked = TrackedPosition(
                            master_account_id=master_acc.id,
                            master_ticket=ticket,
                            first_seen_at=now,
                        )
                        db.add(tracked)
                        db.commit()
                    except Exception:
                        db.rollback()
                    continue

                if tracked.closed_by_tracker:
                    continue

                age = (now - tracked.first_seen_at).total_seconds()
                if age < REPLICATION_WINDOW:
                    continue

                links = (
                    db.query(CopyTradeLink)
                    .filter(
                        CopyTradeLink.master_account_id == master_acc.id,
                        CopyTradeLink.master_ticket == ticket,
                        CopyTradeLink.status == "open",
                    )
                    .all()
                )

                replicated = {l.slave_account_id for l in links if l.slave_ticket}
                missing = {s for s in expected_slave_ids if s is not None} - replicated

                if not missing:
                    continue

                print(
                    f"⚠️ [Tracker] ticket={ticket} master={master_acc.id} "
                    f"age={age:.1f}s — missing slaves {missing} → scheduling close"
                )
                asyncio.create_task(
                    self._emergency_close(master_acc, ticket, links)
                )

        except Exception as e:
            print(f"[Tracker] check_master error acc={master_acc.id}: {e}")
        finally:
            db.close()

    # ─── Main poll loop ────────────────────────────────────────────────────

    async def _check_once(self):
        db = SessionLocal()
        try:
            rel_rows = (
                db.query(
                    CopyRelationship.master_account_id,
                    CopyRelationship.slave_account_id,
                )
                .filter(CopyRelationship.is_active == True)
                .all()
            )

            master_slaves: Dict[int, set] = {}
            for master_id, slave_id in rel_rows:
                if slave_id is not None:
                    master_slaves.setdefault(master_id, set()).add(slave_id)

            if not master_slaves:
                return

            masters = (
                db.query(TradingAccount)
                .filter(
                    TradingAccount.id.in_(master_slaves.keys()),
                    TradingAccount.state == "deployed",
                    TradingAccount.listener_active == True,
                    TradingAccount.metaapi_account_id.isnot(None),
                )
                .all()
            )
        finally:
            db.close()

        await asyncio.gather(
            *[
                self._check_master(acc, master_slaves[acc.id])
                for acc in masters
            ],
            return_exceptions=True,
        )

    async def run(self):
        self._running = True
        print(
            f"[Tracker] Starting — REPLICATION_WINDOW={REPLICATION_WINDOW}s "
            f"POLL_INTERVAL={POLL_INTERVAL}s"
        )

        while self._running:
            try:
                await self._check_once()
            except Exception as e:
                print(f"[Tracker] Poll loop error: {e}")
            await asyncio.sleep(POLL_INTERVAL)

    def stop(self):
        self._running = False


positions_tracker = PositionsTracker()


if __name__ == "__main__":
    from app.model import Base
    from app.database import engine

    Base.metadata.create_all(bind=engine)
    asyncio.run(positions_tracker.run())
