# hedgebridge/listener_manager.py
#
# Per-account MetaApi isolation.
#
# Root cause of the old memory leak:
#   A single shared MetaApi SDK was used for all accounts.  When any account
#   triggered a nuclear reset, _reset_sdk_safely() replaced the global instance.
#   All other accounts' streaming connections were still pointing at the old
#   (now-closed) SDK — they became orphaned, holding WebSocket threads,
#   asyncio tasks and subscription listeners in memory forever.
#
# Fix:
#   self._apis[account_id] → one MetaApi instance per account.
#   Teardown destroys the connection AND that account's MetaApi completely.
#   Reattach always creates a fresh MetaApi for that account.
#   A failure in one account cannot pollute or reset another account's SDK.

import asyncio
import os
import time
from typing import Dict, Optional

from metaapi_cloud_sdk import MetaApi
from sqlalchemy.orm import Session

from app.database import SessionLocal
from app.model import TradingAccount, CopyRelationship
from hedgebridge.listener import MetaApiTradeListener
from hedgebridge.api_client import get_metaapi_client        # REST admin calls only
from hedgebridge.connection_store import (
    set_connection, get_connection, remove_connection, get_all_connections
)


GRACE_PERIOD              = 60
KEEPALIVE_INTERVAL        = 45
EVENT_SILENCE_THRESHOLD   = 120
SYNC_TIMEOUT              = 15
MAX_SYNC_ATTEMPTS         = 2
DEPLOY_WAIT               = 8
CONNECT_TIMEOUT           = 15
CLOSE_TIMEOUT             = 10
RELOAD_TIMEOUT            = 5
GLOBAL_OUTAGE_THRESHOLD   = 0.6
GLOBAL_OUTAGE_WINDOW      = 10.0
GLOBAL_OUTAGE_COOLDOWN    = 45


def _make_api() -> MetaApi:
    token = os.getenv("ACCESS_TOKEN")
    if not token:
        raise ValueError("ACCESS_TOKEN is not set")
    return MetaApi(token)


class ListenerManager:
    def __init__(self):
        self._lock              = asyncio.Lock()
        self._running           = False

        # ── Per-account state ────────────────────────────────────────────
        self._apis: Dict[str, MetaApi] = {}   # metaapi_account_id → MetaApi
        self._listeners: Dict[str, MetaApiTradeListener] = {}
        self._sync_tasks: Dict[str, asyncio.Task] = {}
        self._connected_at: Dict[str, float] = {}
        self._attaching: set = set()

        # ── Reconnect queue ──────────────────────────────────────────────
        self._reconnect_queue    = asyncio.Queue()
        self._reconnect_attempts: Dict[str, int] = {}
        self._reconnect_limit    = 3

        # ── Global outage detection ──────────────────────────────────────
        self._disconnect_times: Dict[str, float] = {}
        self._global_outage      = False
        self._outage_recovery_task: Optional[asyncio.Task] = None

    # =========================================================================
    # PER-ACCOUNT MetaApi LIFECYCLE
    # =========================================================================

    def _create_api_for(self, account_id: str) -> MetaApi:
        """Create a fresh MetaApi instance for this account and store it."""
        api = _make_api()
        self._apis[account_id] = api
        print(f"[LM] Fresh MetaApi → {account_id}")
        return api

    async def _destroy_api_for(self, account_id: str):
        """Fully close and discard the MetaApi instance for this account."""
        api = self._apis.pop(account_id, None)
        if api is None:
            return
        try:
            if hasattr(api, "close"):
                result = api.close()
                if asyncio.iscoroutine(result):
                    await asyncio.wait_for(result, timeout=10)
            print(f"[LM] MetaApi closed → {account_id}")
        except Exception as e:
            print(f"[LM] MetaApi close error → {account_id}: {e}")

    # =========================================================================
    # SHARED TEARDOWN HELPER
    # =========================================================================

    async def _teardown_account(self, account_id: str, mark_attaching: bool = False):
        """
        Fully tear down one account: remove from all stores, cancel sync task,
        close the streaming connection, destroy its private MetaApi instance.

        mark_attaching=True — hold the attaching slot after teardown (nuclear reset).
        mark_attaching=False — release the slot (remove / mark_disconnected).
        """
        async with self._lock:
            connection = get_connection(account_id)
            listener   = self._listeners.pop(account_id, None)
            if mark_attaching:
                self._attaching.add(account_id)
            else:
                self._attaching.discard(account_id)
            self._connected_at.pop(account_id, None)

        self._cancel_sync_task(account_id)
        self._purge_account_state(account_id)

        # Detach listener before closing connection
        if listener and connection:
            try:
                connection.remove_synchronization_listener(listener)
            except Exception:
                pass

        # Clear listener caches
        if listener:
            try:
                listener._known_positions.clear()
                listener._position_cache.clear()
                listener._disconnected = False
            except Exception:
                pass

        # Close streaming connection
        if connection:
            await self._close_stream_safely(connection, account_id)
            remove_connection(account_id)

        # Destroy private MetaApi — this is what was leaking
        await self._destroy_api_for(account_id)

        self._set_listener_active(account_id, False)

    # =========================================================================
    # PER-ACCOUNT STATE CLEANUP
    # =========================================================================

    def _purge_account_state(self, account_id: str):
        self._reconnect_attempts.pop(account_id, None)
        self._disconnect_times.pop(account_id, None)
        self._connected_at.pop(account_id, None)
        self._attaching.discard(account_id)

    # =========================================================================
    # SAFE STREAM CLOSE
    # =========================================================================

    async def _close_stream_safely(self, connection, account_id: str):
        try:
            await asyncio.wait_for(connection.close(), timeout=CLOSE_TIMEOUT)
            print(f"[LM] Closed stream → {account_id}")
        except asyncio.TimeoutError:
            print(f"[LM] Stream close timed out → {account_id}")
        except Exception as e:
            print(f"[LM] Stream close error → {account_id}: {e}")

    # =========================================================================
    # DB HELPERS
    # =========================================================================

    def _set_listener_active(self, account_id_or_metaapi_id, active: bool):
        db: Session = SessionLocal()
        try:
            if isinstance(account_id_or_metaapi_id, int):
                db.query(TradingAccount).filter(
                    TradingAccount.id == account_id_or_metaapi_id
                ).update({"listener_active": active})
            else:
                db.query(TradingAccount).filter(
                    TradingAccount.metaapi_account_id == account_id_or_metaapi_id
                ).update({"listener_active": active})
            db.commit()
        except Exception as e:
            print(f"⚠️ Failed to set listener_active={active} for {account_id_or_metaapi_id}: {e}")
            db.rollback()
        finally:
            db.close()

    # =========================================================================
    # GLOBAL OUTAGE DETECTION
    # =========================================================================

    def _record_disconnect(self, account_id: str):
        now = time.monotonic()
        self._disconnect_times[account_id] = now

        recent = [t for t in self._disconnect_times.values() if now - t <= GLOBAL_OUTAGE_WINDOW]
        total_known = max(len(get_all_connections()) + len(recent), 1)
        ratio = len(recent) / total_known

        if ratio >= GLOBAL_OUTAGE_THRESHOLD and not self._global_outage:
            print(f"🌐 Global outage detected — {len(recent)}/{total_known} accounts dropped simultaneously")
            self._global_outage = True

            if self._outage_recovery_task and not self._outage_recovery_task.done():
                self._outage_recovery_task.cancel()

            self._outage_recovery_task = asyncio.create_task(
                self._recover_from_global_outage()
            )

    async def _recover_from_global_outage(self):
        print(f"⏸️ Pausing reconnects for {GLOBAL_OUTAGE_COOLDOWN}s while MetaApi socket recovers...")
        await asyncio.sleep(GLOBAL_OUTAGE_COOLDOWN)

        # Drain stale queue entries
        while not self._reconnect_queue.empty():
            try:
                self._reconnect_queue.get_nowait()
                self._reconnect_queue.task_done()
            except Exception:
                break

        # Reset all counters — outage was not per-account
        self._reconnect_attempts.clear()
        self._disconnect_times.clear()

        # With per-account MetaApi, no global SDK reset is needed.
        # Each account gets a fresh MetaApi when it reconnects.

        self._global_outage = False
        print("🌐 Global outage cooldown complete — queuing fresh reconnects")

        db: Session = SessionLocal()
        try:
            accounts = db.query(TradingAccount).all()
            for acc in accounts:
                if not acc.state or acc.state.upper() != "DEPLOYED":
                    continue
                is_used = db.query(CopyRelationship).filter(
                    (CopyRelationship.master_account_id == acc.id) |
                    (CopyRelationship.slave_account_id == acc.id)
                ).first()
                if not is_used:
                    continue
                if get_connection(acc.metaapi_account_id) is None:
                    try:
                        self._reconnect_queue.put_nowait(acc.metaapi_account_id)
                        print(f"📋 Queued for reconnect → {acc.metaapi_account_id}")
                    except Exception:
                        pass
        finally:
            db.close()

    # =========================================================================
    # KEEPALIVE
    # =========================================================================

    async def _keep_connections_alive(self):
        while True:
            try:
                await asyncio.sleep(KEEPALIVE_INTERVAL)
                now = time.monotonic()

                for account_id, connection in list(get_all_connections().items()):
                    try:
                        connected_at = self._connected_at.get(account_id, 0)
                        if now - connected_at < GRACE_PERIOD:
                            print(f"🕐 Grace period active → {account_id}, skipping health check")
                            continue

                        sync_task = self._sync_tasks.get(account_id)
                        if sync_task and not sync_task.done():
                            print(f"⏳ Sync in progress → {account_id}, skipping keepalive kill")
                            continue

                        health  = getattr(connection, "health_monitor", None)
                        status  = getattr(health, "health_status", None) if health else None

                        if status is not None and not status.get("connected", False):
                            print(f"💀 Keepalive detected dead connection → {account_id}")
                            await self.mark_disconnected(account_id)
                            continue

                        listener = self._listeners.get(account_id)
                        if listener and listener._last_event_at > 0:
                            silence = now - listener._last_event_at
                            if silence > EVENT_SILENCE_THRESHOLD:
                                print(
                                    f"🔇 Event silence {silence:.0f}s → {account_id} "
                                    f"(subscription manager likely dead), forcing reconnect"
                                )
                                await self.mark_disconnected(account_id)

                    except Exception as e:
                        print(f"⚠️ Keepalive check error for {account_id}: {e}")

            except Exception as e:
                print(f"❌ Keepalive loop error: {e}")
                await asyncio.sleep(10)

    # =========================================================================
    # START
    # =========================================================================

    async def start(self):
        if self._running:
            return

        self._running = True
        print("🚀 Listener Manager started")

        asyncio.create_task(self._keep_connections_alive())
        asyncio.create_task(self._reconnect_worker())

        while True:
            try:
                await self._sync()
                await asyncio.sleep(5)
            except Exception as e:
                print(f"❌ Manager error: {e}")
                await asyncio.sleep(3)

    # =========================================================================
    # RECONNECT WORKER
    # =========================================================================

    async def _reconnect_worker(self):
        while True:
            try:
                account_id = await self._reconnect_queue.get()

                if self._global_outage:
                    print(f"⏸️ Global outage active — requeueing {account_id}")
                    await asyncio.sleep(5)
                    try:
                        self._reconnect_queue.put_nowait(account_id)
                    except Exception:
                        pass
                    self._reconnect_queue.task_done()
                    continue

                db: Session = SessionLocal()
                try:
                    acc = db.query(TradingAccount).filter(
                        TradingAccount.metaapi_account_id == account_id
                    ).first()
                finally:
                    db.close()

                if not acc:
                    print(f"⚠️ Account not found in DB → {account_id}, skipping reconnect")
                    self._reconnect_attempts.pop(account_id, None)
                    self._reconnect_queue.task_done()
                    continue

                if not acc.state or acc.state.upper() != "DEPLOYED":
                    print(f"🛑 DB state={acc.state!r} for {account_id} — not reconnecting")
                    self._purge_account_state(account_id)
                    self._reconnect_queue.task_done()
                    continue

                attempts = self._reconnect_attempts.get(account_id, 0) + 1
                self._reconnect_attempts[account_id] = attempts

                print(f"🔁 Reconnect attempt {attempts}/{self._reconnect_limit} → {account_id}")

                if attempts >= self._reconnect_limit:
                    print(f"💣 Reconnect limit hit → {account_id}, triggering nuclear reset")
                    self._reconnect_attempts.pop(account_id, None)
                    await self._nuclear_reset(account_id, db_acc=acc)
                    self._reconnect_queue.task_done()
                    continue

                backoff = min(5 * attempts, 60)
                print(f"⏳ Backoff {backoff}s before reconnect → {account_id}")
                await asyncio.sleep(backoff)

                if self._global_outage:
                    print(f"⏸️ Global outage detected during backoff — requeueing {account_id}")
                    self._reconnect_attempts.pop(account_id, None)
                    self._reconnect_queue.task_done()
                    continue

                await self._ensure_listener(acc)
                self._reconnect_queue.task_done()

            except Exception as e:
                print(f"❌ Reconnect worker error: {e}")
                await asyncio.sleep(5)

    # =========================================================================
    # NUCLEAR RESET
    # =========================================================================

    async def _nuclear_reset(self, account_id: str, db_acc: TradingAccount = None):
        """
        Last-resort recovery: tear down the account completely — connection,
        listener, and its private MetaApi instance — then rebuild fresh.

        mark_attaching=True keeps the slot locked throughout so _sync() cannot
        race in and attach a new listener while nuclear is in progress.
        """
        print(f"☢️ Nuclear reset starting → {account_id}")

        # Full teardown: connection + listener + MetaApi; holds _attaching slot.
        await self._teardown_account(account_id, mark_attaching=True)
        print(f"🧹 Nuclear teardown complete → {account_id}")

        # Brief drain — let in-flight MetaApi callbacks settle before rebuild.
        await asyncio.sleep(2)

        # DB is source of truth: abort if no longer deployed.
        if db_acc is None:
            db: Session = SessionLocal()
            try:
                db_acc = db.query(TradingAccount).filter(
                    TradingAccount.metaapi_account_id == account_id
                ).first()
            finally:
                db.close()

        if not db_acc or not db_acc.state or db_acc.state.upper() != "DEPLOYED":
            state_str = db_acc.state if db_acc else "MISSING"
            print(f"🛑 DB state={state_str!r} → skipping nuclear redeploy for {account_id}")
            async with self._lock:
                self._attaching.discard(account_id)
            return

        # Use the shared admin API (REST only, no streaming) to check/redeploy.
        try:
            admin_api = get_metaapi_client()
            account   = await admin_api.metatrader_account_api.get_account(account_id)
            broker_connected = getattr(account, "connection_status", None) == "CONNECTED"

            if account.state.upper() != "DEPLOYED":
                print(f"🚀 Nuclear deploy → {account_id}")
                await account.deploy()
                await asyncio.sleep(DEPLOY_WAIT)
            elif not broker_connected:
                print(f"🔄 Nuclear undeploy → redeploy → {account_id} (broker not connected)")
                await account.undeploy()
                await asyncio.sleep(10)
                await account.deploy()
                await asyncio.sleep(DEPLOY_WAIT)
            else:
                print(f"🔄 Nuclear reattach only → {account_id} (broker already CONNECTED)")

        except Exception as e:
            print(f"⚠️ Nuclear redeploy failed → {account_id}: {e}")

        # Release the slot and queue a fresh connect (fresh MetaApi created in _ensure_listener)
        async with self._lock:
            self._attaching.discard(account_id)

        print(f"🔁 Queuing fresh reconnect after nuclear reset → {account_id}")
        try:
            self._reconnect_queue.put_nowait(account_id)
        except Exception:
            pass

    # =========================================================================
    # DB SYNC + HEALTH CHECK
    # =========================================================================

    async def _sync(self):
        if self._global_outage:
            return

        db: Session = SessionLocal()
        try:
            accounts = db.query(TradingAccount).all()
            for acc in accounts:
                if not acc.state or acc.state.upper() != "DEPLOYED":
                    await self._remove_listener(acc)
                    continue

                is_used = db.query(CopyRelationship).filter(
                    (CopyRelationship.master_account_id == acc.id) |
                    (CopyRelationship.slave_account_id == acc.id)
                ).first()

                if not is_used:
                    await self._remove_listener(acc)
                    continue

                await self._ensure_listener(acc)
        finally:
            db.close()

        now = time.monotonic()
        for account_id, connection in list(get_all_connections().items()):
            try:
                connected_at = self._connected_at.get(account_id, 0)
                if now - connected_at < GRACE_PERIOD:
                    continue

                sync_task = self._sync_tasks.get(account_id)
                if sync_task and not sync_task.done():
                    continue

                health = getattr(connection, "health_monitor", None)
                status = getattr(health, "health_status", None) if health else None

                if status is not None and not status.get("connected", False):
                    print(f"💀 Dead connection detected → {account_id}")
                    await self.mark_disconnected(account_id)
                    continue

                listener = self._listeners.get(account_id)
                if listener and listener._last_event_at > 0:
                    silence = now - listener._last_event_at
                    if silence > EVENT_SILENCE_THRESHOLD:
                        print(
                            f"🔇 Event silence {silence:.0f}s → {account_id} "
                            f"(subscription manager likely dead), forcing reconnect"
                        )
                        await self.mark_disconnected(account_id)

            except Exception:
                pass

    # =========================================================================
    # ENSURE LISTENER EXISTS
    # =========================================================================

    async def _ensure_listener(self, acc: TradingAccount):
        account_id = acc.metaapi_account_id

        async with self._lock:
            if get_connection(account_id) is not None:
                return
            if account_id in self._attaching:
                print(f"⏸️ Already attaching → {account_id}, skipping")
                return
            self._attaching.add(account_id)

        connection = None

        try:
            print(f"🔌 Attaching listener → {account_id}")

            # Fresh MetaApi for this account — destroyed on any failure or disconnect
            api     = self._create_api_for(account_id)
            account = await api.metatrader_account_api.get_account(account_id)

            if account.state.upper() != "DEPLOYED":
                print(
                    f"⚠️ MetaApi reports {account.state!r} for {account_id} "
                    f"but DB says deployed — waiting for MetaApi to sync, skipping attach"
                )
                await self._destroy_api_for(account_id)
                return

            print(f"⏳ Waiting for broker connection → {account_id}")
            connected = False
            for i in range(15):
                try:
                    await asyncio.wait_for(account.reload(), timeout=RELOAD_TIMEOUT)
                except Exception:
                    pass
                status = account.connection_status
                print(f"   [{i+1}/15] connection_status={status}")
                if status == "CONNECTED":
                    connected = True
                    break
                await asyncio.sleep(1)

            if not connected:
                print(f"⚠️ Broker not CONNECTED after 15s → {account_id}, attempting stream anyway")

            await asyncio.sleep(2)

            connection = account.get_streaming_connection()
            print(f"🔗 Connecting stream → {account_id}")
            try:
                await asyncio.wait_for(connection.connect(), timeout=CONNECT_TIMEOUT)
            except asyncio.TimeoutError:
                await self._close_stream_safely(connection, account_id)
                raise Exception(f"[LM] connection.connect() timed out → {account_id}")

            async with self._lock:
                if get_connection(account_id) is not None:
                    print(f"⚠️ Concurrent attach beat us → {account_id}, closing duplicate")
                    await self._close_stream_safely(connection, account_id)
                    return

                listener = MetaApiTradeListener(
                    db_account_id=acc.id,
                    metaapi_account_id=account_id,
                    manager=self
                )
                connection.add_synchronization_listener(listener)
                set_connection(account_id, connection)
                self._listeners[account_id]   = listener
                self._connected_at[account_id] = time.monotonic()

            print(f"👂 Listener attached → {account_id}")
            self._set_listener_active(account_id, False)

            task = asyncio.create_task(
                self._background_sync_wait(account_id, connection)
            )
            async with self._lock:
                self._sync_tasks[account_id] = task

        except Exception as e:
            print(f"❌ Attach failed {acc.id}: {e}")
            if connection:
                await self._close_stream_safely(connection, account_id)
            # Destroy the API we just created — otherwise it leaks
            await self._destroy_api_for(account_id)

        finally:
            async with self._lock:
                self._attaching.discard(account_id)

    # =========================================================================
    # BACKGROUND SYNC WAIT
    # =========================================================================

    async def _background_sync_wait(self, account_id: str, connection):
        try:
            for attempt in range(1, MAX_SYNC_ATTEMPTS + 1):
                try:
                    await asyncio.wait_for(
                        connection.wait_synchronized(),
                        timeout=SYNC_TIMEOUT
                    )
                    print(f"✅ Background sync complete → {account_id}")
                    self._set_listener_active(account_id, True)
                    self._reconnect_attempts.pop(account_id, None)
                    self._disconnect_times.pop(account_id, None)
                    listener = self._listeners.get(account_id)
                    if listener and listener._last_event_at == 0.0:
                        listener._last_event_at = time.monotonic()
                    return

                except asyncio.CancelledError:
                    print(f"🛑 Background sync cancelled → {account_id}")
                    return

                except asyncio.TimeoutError:
                    print(f"⏳ Background sync timeout (attempt {attempt}/{MAX_SYNC_ATTEMPTS}) → {account_id}")

                except Exception as e:
                    print(f"⚠️ Background sync error (attempt {attempt}/{MAX_SYNC_ATTEMPTS}) → {account_id}: {e}")
                    if "connection has been closed" in str(e).lower():
                        print(f"🛑 Connection closed, stopping background sync → {account_id}")
                        await self.mark_disconnected(account_id)
                        return

                await asyncio.sleep(5)

            print(
                f"⚠️ Sync never completed after {MAX_SYNC_ATTEMPTS} attempts → "
                f"{account_id}, triggering reconnect"
            )
            await self.mark_disconnected(account_id, sync_failed=True)

        finally:
            self._sync_tasks.pop(account_id, None)

    # =========================================================================
    # CANCEL SYNC TASK
    # =========================================================================

    def _cancel_sync_task(self, account_id: str):
        task = self._sync_tasks.pop(account_id, None)
        if task and not task.done():
            task.cancel()

    # =========================================================================
    # REMOVE LISTENER
    # =========================================================================

    async def _remove_listener(self, acc: TradingAccount):
        """
        Cleanly remove a listener when an account is undeployed or no longer
        part of any copy relationship.  Full teardown: connection + MetaApi.
        """
        account_id = acc.metaapi_account_id
        if get_connection(account_id) is None and account_id not in self._listeners:
            return

        print(f"🛑 Removing listener → {account_id}")
        await self._teardown_account(account_id, mark_attaching=False)
        print(f"🗑️ Listener removed → {account_id}")

    # =========================================================================
    # MARK DISCONNECTED
    # =========================================================================

    async def mark_disconnected(self, account_id: str, sync_failed: bool = False):
        self._record_disconnect(account_id)

        # Guard: if already torn down, nothing to do
        async with self._lock:
            has_listener = account_id in self._listeners
            has_conn     = get_connection(account_id) is not None
        if not has_listener and not has_conn:
            return

        await self._teardown_account(account_id, mark_attaching=False)
        print(f"♻️ Marked for reconnection → {account_id}")

        if not self._global_outage:
            if sync_failed:
                self._reconnect_attempts.pop(account_id, None)
            self._reconnect_queue.put_nowait(account_id)


# =========================================================================
# SINGLETON
# =========================================================================
listener_manager = ListenerManager()
