# hedgebridge/positions_tracker.py
#
# Standalone safety-net process — fully independent of listener_manager.
#
# Design:
#   - One MetaApi instance per user (isolated, same pattern as dashboard_session).
#   - RPC connections only — no dependency on connection_store / streaming.
#   - Runs per-user in parallel every POLL_INTERVAL seconds.
#   - For each user: check all their master accounts via RPC get_positions().
#   - If a master position is not replicated to every slave within
#     REPLICATION_WINDOW seconds → emergency-close master + any slaves that
#     did receive the copy.
#   - On any RPC failure/timeout → destroy that user's session immediately;
#     next poll creates a fresh MetaApi + connections.

import asyncio
import os
import sys
from datetime import datetime
from typing import Dict, List, Set

from metaapi_cloud_sdk import MetaApi

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from dotenv import load_dotenv
load_dotenv()

from app.database import SessionLocal
from app.model import TradingAccount, CopyRelationship, CopyTradeLink, TrackedPosition

# ─── Tuning ────────────────────────────────────────────────────────────────
REPLICATION_WINDOW: int = int(os.getenv("TRACKER_REPLICATION_WINDOW", "10"))
POLL_INTERVAL: int      = int(os.getenv("TRACKER_POLL_INTERVAL", "3"))
CONNECT_TIMEOUT: int    = 20
RPC_TIMEOUT: int        = 8
CLOSE_TIMEOUT: int      = 15
# ───────────────────────────────────────────────────────────────────────────


def _new_api() -> MetaApi:
    token = os.getenv("ACCESS_TOKEN")
    if not token:
        raise ValueError("ACCESS_TOKEN is not set")
    return MetaApi(token)


# ─── Per-user RPC session ──────────────────────────────────────────────────

class _TrackerUserSession:
    def __init__(self, user_id: int):
        self.user_id = user_id
        self._api = _new_api()
        print(f"[Tracker] Fresh MetaApi → user={user_id}")
        self._connections: Dict[str, object] = {}
        self._lock = asyncio.Lock()
        self._account_locks: Dict[str, asyncio.Lock] = {}

    async def _get_account_lock(self, account_id: str) -> asyncio.Lock:
        async with self._lock:
            if account_id not in self._account_locks:
                self._account_locks[account_id] = asyncio.Lock()
            return self._account_locks[account_id]

    async def get_connection(self, account_id: str):
        async with self._lock:
            conn = self._connections.get(account_id)
            if conn is not None:
                return conn

        acc_lock = await self._get_account_lock(account_id)
        async with acc_lock:
            async with self._lock:
                conn = self._connections.get(account_id)
                if conn is not None:
                    return conn

            print(f"[Tracker] Building RPC → user={self.user_id} account={account_id}")
            account = await self._api.metatrader_account_api.get_account(account_id)
            conn = account.get_rpc_connection()
            await asyncio.wait_for(conn.connect(), timeout=CONNECT_TIMEOUT)

            async with self._lock:
                self._connections[account_id] = conn

            print(f"[Tracker] RPC ready → user={self.user_id} account={account_id}")
            return conn

    async def destroy(self):
        print(f"[Tracker] Destroying session → user={self.user_id}")
        async with self._lock:
            conns = list(self._connections.values())
            self._connections.clear()

        for conn in conns:
            try:
                await asyncio.wait_for(conn.close(), timeout=3)
            except Exception:
                pass

        try:
            if hasattr(self._api, "close"):
                result = self._api.close()
                if asyncio.iscoroutine(result):
                    await asyncio.wait_for(result, timeout=5)
        except Exception:
            pass
        print(f"[Tracker] Session destroyed → user={self.user_id}")


class _TrackerSessionManager:
    def __init__(self):
        self._sessions: Dict[int, _TrackerUserSession] = {}
        self._lock = asyncio.Lock()

    async def get_connection(self, user_id: int, account_id: str):
        async with self._lock:
            if user_id not in self._sessions:
                self._sessions[user_id] = _TrackerUserSession(user_id)
        return await self._sessions[user_id].get_connection(account_id)

    async def destroy_session(self, user_id: int):
        async with self._lock:
            session = self._sessions.pop(user_id, None)
        if session:
            await session.destroy()


_sessions = _TrackerSessionManager()


# ─── Tracker ──────────────────────────────────────────────────────────────

class PositionsTracker:
    def __init__(self):
        self._running = False
        self._intervening: set = set()

    # ── RPC helpers ───────────────────────────────────────────────────────

    async def _get_positions(self, user_id: int, meta_id: str) -> List[dict]:
        try:
            conn = await asyncio.wait_for(
                _sessions.get_connection(user_id, meta_id),
                timeout=CONNECT_TIMEOUT + 2,
            )
            return await asyncio.wait_for(conn.get_positions(), timeout=RPC_TIMEOUT)
        except BaseException as e:
            print(f"[Tracker] get_positions failed user={user_id} account={meta_id}: {type(e).__name__}: {e}")
            await _sessions.destroy_session(user_id)
            return []

    async def _close_position(self, user_id: int, meta_id: str, ticket: str) -> bool:
        try:
            conn = await asyncio.wait_for(
                _sessions.get_connection(user_id, meta_id),
                timeout=CONNECT_TIMEOUT + 2,
            )
            await asyncio.wait_for(conn.close_position(ticket), timeout=CLOSE_TIMEOUT)
            return True
        except BaseException as e:
            print(f"[Tracker] close_position failed user={user_id} account={meta_id} ticket={ticket}: {type(e).__name__}: {e}")
            await _sessions.destroy_session(user_id)
            return False

    # ── Emergency close ───────────────────────────────────────────────────

    async def _emergency_close(
        self,
        user_id: int,
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

            tasks.append(self._close_position(user_id, master_acc.metaapi_account_id, master_ticket))
            meta.append(("master", master_acc.id, master_ticket))

            for link in slave_links:
                if not link.slave_ticket:
                    continue
                slave_acc = slave_accs.get(link.slave_account_id)
                if not slave_acc or not slave_acc.metaapi_account_id:
                    continue
                tasks.append(self._close_position(user_id, slave_acc.metaapi_account_id, link.slave_ticket))
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

    # ── Per-master check ──────────────────────────────────────────────────

    async def _check_master(
        self,
        user_id: int,
        master_acc: TradingAccount,
        expected_slave_ids: Set,
    ):
        positions = await self._get_positions(user_id, master_acc.metaapi_account_id)
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
                    self._emergency_close(user_id, master_acc, ticket, links)
                )

        except Exception as e:
            print(f"[Tracker] check_master error acc={master_acc.id}: {e}")
        finally:
            db.close()

    # ── Per-user check (masters run in parallel) ──────────────────────────

    async def _check_user(
        self,
        user_id: int,
        master_accs: List[TradingAccount],
        master_slaves: Dict[int, Set],
    ):
        await asyncio.gather(
            *[
                self._check_master(user_id, acc, master_slaves[acc.id])
                for acc in master_accs
            ],
            return_exceptions=True,
        )

    # ── Full poll cycle ───────────────────────────────────────────────────

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

            master_slaves: Dict[int, Set] = {}
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
                    TradingAccount.metaapi_account_id.isnot(None),
                )
                .all()
            )

            # Group by owner so each user runs in parallel with its own session
            user_masters: Dict[int, List[TradingAccount]] = {}
            for acc in masters:
                user_masters.setdefault(acc.owner_user_id, []).append(acc)

        finally:
            db.close()

        await asyncio.gather(
            *[
                self._check_user(user_id, accs, master_slaves)
                for user_id, accs in user_masters.items()
            ],
            return_exceptions=True,
        )

    # ── Main loop ─────────────────────────────────────────────────────────

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
