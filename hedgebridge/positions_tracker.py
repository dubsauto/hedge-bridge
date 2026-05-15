# hedgebridge/positions_tracker.py
#
# Standalone safety-net process — fully independent of listener_manager.
#
# Design:
#   - One MetaApi instance per user (isolated, same pattern as dashboard_session).
#   - RPC connections only — no dependency on connection_store / streaming.
#   - Runs per-user in parallel every POLL_INTERVAL seconds.
#
# Two safety checks per poll cycle (run in parallel):
#
#   OPEN CHECK — _check_master():
#     If a new master position is not replicated to every slave within
#     REPLICATION_WINDOW seconds → close master + any slaves that did copy.
#
#   CLOSE CHECK — _check_closes_for_user():
#     If a position is closed on the master OR any slave (trade closed in MT5
#     but listener missed it), and after REPLICATION_WINDOW the close has not
#     propagated to all sides → close all remaining open positions.
#
#   On any RPC failure/timeout → destroy that user's session immediately;
#   next poll creates a fresh MetaApi + connections.

import asyncio
import os
import sys
from datetime import datetime
from typing import Dict, List, Optional, Set, Tuple

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

# Link tuple shape stored in memory to avoid holding SQLAlchemy objects across
# session boundaries: (id, master_account_id, master_ticket, slave_account_id, slave_ticket)
_LinkTuple = Tuple[int, int, str, Optional[int], Optional[str]]


class PositionsTracker:
    def __init__(self):
        self._running = False
        self._intervening: set = set()
        # In-memory close-detection timestamps.
        # Key: "close:{master_db_id}:{master_ticket}"
        # Value: datetime when close was first detected on any side.
        self._close_detected: Dict[str, datetime] = {}

    # ── RPC helpers ───────────────────────────────────────────────────────

    async def _get_positions(self, user_id: int, meta_id: str) -> Optional[List[dict]]:
        """
        Returns list of live positions, or None on failure.
        None means "unknown" — callers must not treat it as "no positions".
        """
        try:
            conn = await asyncio.wait_for(
                _sessions.get_connection(user_id, meta_id),
                timeout=CONNECT_TIMEOUT + 2,
            )
            return await asyncio.wait_for(conn.get_positions(), timeout=RPC_TIMEOUT)
        except BaseException as e:
            print(f"[Tracker] get_positions failed user={user_id} account={meta_id}: {type(e).__name__}: {e}")
            await _sessions.destroy_session(user_id)
            return None

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

    # ── OPEN CHECK — emergency close (failed replication on open) ─────────

    async def _emergency_close_open(
        self,
        user_id: int,
        master_acc: TradingAccount,
        master_ticket: str,
        slave_links: List[CopyTradeLink],
    ):
        key = f"open:{master_acc.id}:{master_ticket}"
        if key in self._intervening:
            return
        self._intervening.add(key)

        try:
            print(
                f"🚨 [Tracker] OPEN — EMERGENCY CLOSE master={master_acc.id} "
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
                print(f"[Tracker] Open-close {role} acc={acc_id} ticket={ticket}: {'OK' if ok else f'FAILED ({result})'}")

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
                    .filter_by(master_account_id=master_acc.id, master_ticket=master_ticket)
                    .first()
                )
                if tracked:
                    tracked.closed_by_tracker = True
                    tracked.intervention_at = now

                write_db.commit()
                print(f"[Tracker] Open-close DB updated ticket={master_ticket}")
            except Exception as e:
                write_db.rollback()
                print(f"[Tracker] Open-close DB update failed: {e}")
            finally:
                write_db.close()

        finally:
            self._intervening.discard(key)

    # ── CLOSE CHECK — emergency close (close not propagated) ─────────────

    async def _emergency_close_sync(
        self,
        user_id: int,
        master_acc: TradingAccount,
        master_ticket: str,
        link_tuples: List[_LinkTuple],
        live_tickets: Dict[int, Optional[Set[str]]],
        slave_meta_map: Dict[int, TradingAccount],
    ):
        key = f"close:{master_acc.id}:{master_ticket}"
        if key in self._intervening:
            return
        self._intervening.add(key)

        try:
            print(f"🚨 [Tracker] CLOSE SYNC master={master_acc.id} ticket={master_ticket}")

            tasks = []
            meta = []

            # Close master if still open (or unknown — err on side of action)
            master_live = live_tickets.get(master_acc.id)
            if master_live is None or master_ticket in master_live:
                tasks.append(self._close_position(user_id, master_acc.metaapi_account_id, master_ticket))
                meta.append(("master", master_acc.id, master_ticket))

            for (_, _, _, slave_db_id, slave_ticket) in link_tuples:
                if not slave_ticket or not slave_db_id:
                    continue
                slave_acc = slave_meta_map.get(slave_db_id)
                if not slave_acc or not slave_acc.metaapi_account_id:
                    continue
                slave_live = live_tickets.get(slave_db_id)
                if slave_live is None or slave_ticket in slave_live:
                    tasks.append(self._close_position(user_id, slave_acc.metaapi_account_id, slave_ticket))
                    meta.append(("slave", slave_db_id, slave_ticket))

            if not tasks:
                print(f"[Tracker] Close sync — nothing to close for {master_acc.id}:{master_ticket}")
                return

            results = await asyncio.gather(*tasks, return_exceptions=True)
            for (role, acc_id, ticket), result in zip(meta, results):
                ok = result is True
                print(f"[Tracker] Close-sync {role} acc={acc_id} ticket={ticket}: {'OK' if ok else f'FAILED ({result})'}")

            write_db = SessionLocal()
            try:
                now = datetime.utcnow()
                link_ids = [t[0] for t in link_tuples]
                rows = (
                    write_db.query(CopyTradeLink)
                    .filter(
                        CopyTradeLink.id.in_(link_ids),
                        CopyTradeLink.status == "open",
                    )
                    .all()
                )
                for row in rows:
                    row.status = "closed"
                    row.closed_at = now
                write_db.commit()
                print(f"[Tracker] Close-sync DB updated {len(rows)} links for ticket={master_ticket}")
            except Exception as e:
                write_db.rollback()
                print(f"[Tracker] Close-sync DB update failed: {e}")
            finally:
                write_db.close()

        finally:
            self._intervening.discard(key)

    # ── Per-master open check ─────────────────────────────────────────────

    async def _check_master(
        self,
        user_id: int,
        master_acc: TradingAccount,
        expected_slave_ids: Set,
    ):
        positions = await self._get_positions(user_id, master_acc.metaapi_account_id)
        if positions is None:
            return  # RPC failed — session already destroyed

        now = datetime.utcnow()
        db = SessionLocal()
        try:
            for pos in positions:
                ticket = str(pos.get("id") or pos.get("ticket"))

                tracked = (
                    db.query(TrackedPosition)
                    .filter_by(master_account_id=master_acc.id, master_ticket=ticket)
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
                    f"⚠️ [Tracker] OPEN not replicated ticket={ticket} master={master_acc.id} "
                    f"age={age:.1f}s missing={missing}"
                )
                asyncio.create_task(
                    self._emergency_close_open(user_id, master_acc, ticket, links)
                )

        except Exception as e:
            print(f"[Tracker] check_master error acc={master_acc.id}: {e}")
        finally:
            db.close()

    # ── Per-user close check ──────────────────────────────────────────────

    async def _check_closes_for_user(
        self,
        user_id: int,
        master_accs: List[TradingAccount],
    ):
        master_db_ids = [acc.id for acc in master_accs]

        # Load open links and slave accounts
        db = SessionLocal()
        try:
            open_links = (
                db.query(CopyTradeLink)
                .filter(
                    CopyTradeLink.master_account_id.in_(master_db_ids),
                    CopyTradeLink.status == "open",
                    CopyTradeLink.slave_ticket.isnot(None),
                )
                .all()
            )
            if not open_links:
                return

            slave_db_ids = list({l.slave_account_id for l in open_links if l.slave_account_id})
            slave_accs_list = (
                db.query(TradingAccount)
                .filter(
                    TradingAccount.id.in_(slave_db_ids),
                    TradingAccount.metaapi_account_id.isnot(None),
                )
                .all()
            )

            # Detach data before closing session
            link_tuples: List[_LinkTuple] = [
                (l.id, l.master_account_id, l.master_ticket, l.slave_account_id, l.slave_ticket)
                for l in open_links
            ]
        finally:
            db.close()

        master_meta_map: Dict[int, TradingAccount] = {acc.id: acc for acc in master_accs}
        slave_meta_map: Dict[int, TradingAccount] = {acc.id: acc for acc in slave_accs_list}

        # Deduplicated set of (db_id, metaapi_id) to poll — master + slaves
        accounts_to_poll: Dict[int, str] = {}
        for acc in master_accs:
            accounts_to_poll[acc.id] = acc.metaapi_account_id
        for acc in slave_accs_list:
            accounts_to_poll[acc.id] = acc.metaapi_account_id

        # Fetch live positions for all accounts in parallel
        db_ids = list(accounts_to_poll.keys())
        results = await asyncio.gather(
            *[self._get_positions(user_id, accounts_to_poll[db_id]) for db_id in db_ids],
            return_exceptions=True,
        )

        # None = RPC failed (unknown), treat as "don't trigger false positive"
        live_tickets: Dict[int, Optional[Set[str]]] = {}
        for db_id, result in zip(db_ids, results):
            if isinstance(result, list):
                live_tickets[db_id] = {str(p.get("id") or p.get("ticket")) for p in result}
            else:
                live_tickets[db_id] = None

        # Group links by (master_account_id, master_ticket)
        groups: Dict[Tuple[int, str], List[_LinkTuple]] = {}
        for lt in link_tuples:
            key = (lt[1], lt[2])
            groups.setdefault(key, []).append(lt)

        now = datetime.utcnow()

        for (master_db_id, master_ticket), lts in groups.items():
            detection_key = f"close:{master_db_id}:{master_ticket}"

            master_live = live_tickets.get(master_db_id)
            # Only treat as closed if we have a definitive answer (not None)
            master_closed = master_live is not None and master_ticket not in master_live

            any_slave_closed = False
            for (_, _, _, slave_db_id, slave_ticket) in lts:
                if not slave_ticket or slave_db_id not in live_tickets:
                    continue
                slave_live = live_tickets[slave_db_id]
                if slave_live is not None and slave_ticket not in slave_live:
                    any_slave_closed = True
                    break

            if not master_closed and not any_slave_closed:
                # Everything still open — clear any stale detection
                self._close_detected.pop(detection_key, None)
                continue

            # At least one side closed — start or continue tracking
            if detection_key not in self._close_detected:
                who = "master" if master_closed else "slave"
                print(f"[Tracker] Close detected on {who} → {master_db_id}:{master_ticket} — watching")
                self._close_detected[detection_key] = now
                continue

            elapsed = (now - self._close_detected[detection_key]).total_seconds()
            if elapsed < REPLICATION_WINDOW:
                continue

            # Window expired — close all remaining open sides
            print(
                f"⚠️ [Tracker] CLOSE not propagated after {elapsed:.1f}s "
                f"→ {master_db_id}:{master_ticket} — syncing close"
            )
            self._close_detected.pop(detection_key, None)

            master_acc = master_meta_map.get(master_db_id)
            if not master_acc:
                continue

            asyncio.create_task(
                self._emergency_close_sync(
                    user_id, master_acc, master_ticket, lts, live_tickets, slave_meta_map
                )
            )

    # ── Per-user entry point (both checks run in parallel) ────────────────

    async def _check_user(
        self,
        user_id: int,
        master_accs: List[TradingAccount],
        master_slaves: Dict[int, Set],
    ):
        await asyncio.gather(
            asyncio.gather(
                *[self._check_master(user_id, acc, master_slaves[acc.id]) for acc in master_accs],
                return_exceptions=True,
            ),
            self._check_closes_for_user(user_id, master_accs),
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
