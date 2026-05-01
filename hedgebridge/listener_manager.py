# hedgebridge/listener_manager.py

import asyncio
from sqlalchemy.orm import Session
from app.database import SessionLocal
from app.model import TradingAccount, CopyRelationship
from hedgebridge.listener import MetaApiTradeListener
from hedgebridge.api_client import get_metaapi_client
from hedgebridge.connection_store import set_connection, get_connection, remove_connection, get_all_connections
import time


GRACE_PERIOD = 60
KEEPALIVE_INTERVAL = 45
SYNC_TIMEOUT = 180
DEPLOY_WAIT = 8


class ListenerManager:
    def __init__(self):
        self._lock = asyncio.Lock()
        self._running = False
        self._listeners = {}
        self._sync_tasks = {}
        self._connected_at = {}
        self._attaching = set()
        self._api = None
        self._reconnect_queue = asyncio.Queue()
        self._reconnect_attempts = {}
        self._reconnect_limit = 5

    # =====================================
    # GET METAAPI SINGLETON
    # =====================================
    async def _get_api(self):
        if self._api is None:
            self._api = get_metaapi_client()
        return self._api

    # =====================================
    # SET LISTENER ACTIVE FLAG IN DB
    # =====================================
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

    # =====================================
    # KEEPALIVE
    # =====================================
    async def _keep_connections_alive(self):
        while True:
            try:
                await asyncio.sleep(KEEPALIVE_INTERVAL)
                now = time.monotonic()

                for account_id, connection in list(get_all_connections().items()):
                    try:
                        connected_at = self._connected_at.get(account_id, 0)
                        elapsed = now - connected_at

                        if elapsed < GRACE_PERIOD:
                            print(f"🕐 Grace period active → {account_id}, skipping health check")
                            continue

                        # Don't kill while sync is still running
                        sync_task = self._sync_tasks.get(account_id)
                        if sync_task and not sync_task.done():
                            print(f"⏳ Sync in progress → {account_id}, skipping keepalive kill")
                            continue

                        health = getattr(connection, 'health_monitor', None)
                        status = getattr(health, 'health_status', None) if health else None

                        if status is not None and not status.get("connected", False):
                            print(f"💀 Keepalive detected dead connection → {account_id}")
                            await self.mark_disconnected(account_id)

                    except Exception as e:
                        print(f"⚠️ Keepalive check error for {account_id}: {e}")

            except Exception as e:
                print(f"❌ Keepalive loop error: {e}")
                await asyncio.sleep(10)

    # =====================================
    # START MANAGER
    # =====================================
    async def start(self):
        if self._running:
            return

        self._api = get_metaapi_client()
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

    # =====================================
    # RECONNECT WORKER
    # =====================================
    async def _reconnect_worker(self):
        while True:
            try:
                account_id = await self._reconnect_queue.get()

                attempts = self._reconnect_attempts.get(account_id, 0) + 1
                self._reconnect_attempts[account_id] = attempts

                print(f"🔁 Reconnect attempt {attempts}/{self._reconnect_limit} → {account_id}")

                if attempts >= self._reconnect_limit:
                    print(f"💣 Reconnect limit hit → {account_id}, triggering nuclear reset")
                    self._reconnect_attempts.pop(account_id, None)
                    await self._nuclear_reset(account_id)
                    self._reconnect_queue.task_done()
                    continue

                # Backoff scales with attempts: 5s, 10s, 15s, 20s, capped at 60s
                backoff = min(5 * attempts, 60)
                print(f"⏳ Backoff {backoff}s before reconnect → {account_id}")
                await asyncio.sleep(backoff)

                db: Session = SessionLocal()
                try:
                    acc = db.query(TradingAccount).filter(
                        TradingAccount.metaapi_account_id == account_id
                    ).first()
                finally:
                    db.close()

                if acc:
                    await self._ensure_listener(acc)
                else:
                    print(f"⚠️ Account not found in DB → {account_id}, skipping reconnect")

                self._reconnect_queue.task_done()

            except Exception as e:
                print(f"❌ Reconnect worker error: {e}")
                await asyncio.sleep(5)

    # =====================================
    # NUCLEAR RESET
    # =====================================
    async def _nuclear_reset(self, account_id: str):
        print(f"☢️ Nuclear reset starting → {account_id}")

        # Step 1: Full teardown
        async with self._lock:
            connection = get_connection(account_id)
            listener = self._listeners.pop(account_id, None)
            self._attaching.discard(account_id)
            self._connected_at.pop(account_id, None)

        self._cancel_sync_task(account_id)

        if listener:
            try:
                listener._known_positions.clear()
                listener._position_cache.clear()
                listener._disconnected = False
            except Exception:
                pass

        if connection:
            try:
                if listener:
                    connection.remove_synchronization_listener(listener)
            except Exception:
                pass
            try:
                await connection.close()
            except Exception:
                pass
            try:
                remove_connection(account_id)
            except Exception:
                pass

        self._set_listener_active(account_id, False)
        print(f"🧹 Nuclear teardown complete → {account_id}")

        # Step 2: Wait before cold restart
        await asyncio.sleep(15)

        # Step 3: Undeploy → redeploy to force fresh socket on MetaApi side
        try:
            api = await self._get_api()
            account = await api.metatrader_account_api.get_account(account_id)

            if account.state != "DEPLOYED":
                print(f"🚀 Nuclear deploy → {account_id}")
                await account.deploy()
                await asyncio.sleep(DEPLOY_WAIT)
            else:
                print(f"🔄 Nuclear undeploy → redeploy → {account_id}")
                await account.undeploy()
                await asyncio.sleep(10)
                await account.deploy()
                await asyncio.sleep(DEPLOY_WAIT)

        except Exception as e:
            print(f"⚠️ Nuclear redeploy failed → {account_id}: {e}")

        # Step 4: Queue fresh reconnect with clean counter
        print(f"🔁 Queuing fresh reconnect after nuclear reset → {account_id}")
        try:
            self._reconnect_queue.put_nowait(account_id)
        except Exception:
            pass

    # =====================================
    # DB SYNC + HEALTH CHECK
    # =====================================
    async def _sync(self):
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

        # Health check — respects grace period and active sync tasks
        now = time.monotonic()
        for account_id, connection in list(get_all_connections().items()):
            try:
                connected_at = self._connected_at.get(account_id, 0)
                if now - connected_at < GRACE_PERIOD:
                    continue

                sync_task = self._sync_tasks.get(account_id)
                if sync_task and not sync_task.done():
                    continue

                health = getattr(connection, 'health_monitor', None)
                status = getattr(health, 'health_status', None) if health else None

                if status is not None and not status.get("connected", False):
                    print(f"💀 Dead connection detected → {account_id}")
                    await self.mark_disconnected(account_id)

            except Exception:
                pass

    # =====================================
    # ENSURE LISTENER EXISTS
    # =====================================
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

            api = await self._get_api()
            account = await api.metatrader_account_api.get_account(account_id)

            if account.state != "DEPLOYED":
                print(f"🚀 Deploying → {account_id}")
                await account.deploy()
                await asyncio.sleep(DEPLOY_WAIT)

            print(f"⏳ Waiting for broker connection → {account_id}")
            timeout = 60
            connected = False

            for i in range(timeout):
                try:
                    await account.reload()
                except Exception:
                    pass

                status = account.connection_status
                print(f"   [{i+1}/{timeout}] connection_status={status}")

                if status == "CONNECTED":
                    connected = True
                    break

                await asyncio.sleep(1)

            if not connected:
                print(f"❌ Broker not connected after {timeout}s → {account_id}")
                return

            await asyncio.sleep(2)

            connection = account.get_streaming_connection()
            print(f"🔗 Connecting stream → {account_id}")
            await connection.connect()

            async with self._lock:
                if get_connection(account_id) is not None:
                    print(f"⚠️ Concurrent attach beat us → {account_id}, closing duplicate")
                    try:
                        await connection.close()
                    except Exception:
                        pass
                    return

                listener = MetaApiTradeListener(acc.id, manager=self)
                connection.add_synchronization_listener(listener)
                set_connection(account_id, connection)
                self._listeners[account_id] = listener
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
                try:
                    await connection.close()
                except Exception:
                    pass

        finally:
            async with self._lock:
                self._attaching.discard(account_id)

    # =====================================
    # BACKGROUND SYNC WAIT
    # =====================================
    async def _background_sync_wait(self, account_id: str, connection):
        for attempt in range(1, 4):
            try:
                await asyncio.wait_for(
                    connection.wait_synchronized(),
                    timeout=SYNC_TIMEOUT
                )
                print(f"✅ Background sync complete → {account_id}")
                self._set_listener_active(account_id, True)
                # Reset reconnect counter on successful sync
                self._reconnect_attempts.pop(account_id, None)
                return

            except asyncio.CancelledError:
                print(f"🛑 Background sync cancelled → {account_id}")
                return

            except asyncio.TimeoutError:
                print(f"⏳ Background sync timeout (attempt {attempt}/3) → {account_id}")

            except Exception as e:
                print(f"⚠️ Background sync error (attempt {attempt}/3) → {account_id}: {e}")
                if "connection has been closed" in str(e).lower():
                    print(f"🛑 Connection closed, stopping background sync → {account_id}")
                    return

            await asyncio.sleep(5)

        print(f"⚠️ Sync never completed after 3 attempts → {account_id}")

    # =====================================
    # CANCEL BACKGROUND SYNC TASK
    # =====================================
    def _cancel_sync_task(self, account_id: str):
        task = self._sync_tasks.pop(account_id, None)
        if task and not task.done():
            task.cancel()

    # =====================================
    # REMOVE LISTENER (clean removal)
    # =====================================
    async def _remove_listener(self, acc: TradingAccount):
        account_id = acc.metaapi_account_id

        async with self._lock:
            connection = get_connection(account_id)
            listener = self._listeners.pop(account_id, None)
            self._attaching.discard(account_id)
            self._connected_at.pop(account_id, None)

        self._cancel_sync_task(account_id)

        if not connection:
            return

        try:
            print(f"🛑 Removing listener → {account_id}")

            if listener:
                try:
                    connection.remove_synchronization_listener(listener)
                except Exception:
                    pass

            await connection.close()
            remove_connection(account_id)

            if listener:
                try:
                    listener._known_positions.clear()
                    listener._position_cache.clear()
                except Exception:
                    pass

            self._set_listener_active(account_id, False)
            print(f"🗑️ Listener removed → {account_id}")

        except Exception as e:
            print(f"❌ Remove failed {account_id}: {e}")

    # =====================================
    # MARK DISCONNECTED
    # =====================================
    async def mark_disconnected(self, account_id: str):
        async with self._lock:
            # Prevent double-processing if already cleaned up
            if account_id not in self._listeners and get_connection(account_id) is None:
                return

            connection = get_connection(account_id)
            listener = self._listeners.pop(account_id, None)
            self._attaching.discard(account_id)
            self._connected_at.pop(account_id, None)

        self._cancel_sync_task(account_id)

        if connection:
            try:
                if listener:
                    try:
                        connection.remove_synchronization_listener(listener)
                    except Exception:
                        pass

                await connection.close()
                remove_connection(account_id)

            except Exception:
                pass

        if listener:
            try:
                listener._known_positions.clear()
                listener._position_cache.clear()
            except Exception:
                pass

        self._set_listener_active(account_id, False)
        print(f"♻️ Marked for reconnection → {account_id}")

        # Queue reconnect — worker handles backoff and nuclear reset
        try:
            self._reconnect_queue.put_nowait(account_id)
        except asyncio.QueueFull:
            pass


# =====================================
# SINGLETON
# =====================================
listener_manager = ListenerManager()