# hedgebridge/listener_manager.py

import asyncio
from sqlalchemy.orm import Session
from app.database import SessionLocal
from app.model import TradingAccount, CopyRelationship
from hedgebridge.listener import MetaApiTradeListener
from hedgebridge.api_client import get_metaapi_client
from hedgebridge.connection_store import set_connection, get_connection, remove_connection, get_all_connections
import time


class ListenerManager:
    def __init__(self):
        self._lock = asyncio.Lock()
        self._running = False

        # account_id -> listener instance
        self._listeners = {}

        # account_id -> asyncio.Task (background sync task)
        self._sync_tasks = {}

        # account_id -> timestamp when connection was registered
        # used to give new connections a grace period before health checks
        self._connected_at = {}

        # Tracks accounts currently mid-attach — prevents concurrent attempts
        self._attaching = set()

        # singleton MetaApi client cache
        self._api = None

    # =====================================
    # GET METAAPI SINGLETON
    # =====================================
    async def _get_api(self):
        if self._api is None:
            self._api = get_metaapi_client()
        return self._api

    # =====================================
    # KEEP CONNECTIONS ALIVE
    # =====================================
    async def _keep_connections_alive(self):
        while True:
            try:
                now = time.monotonic()
                for account_id, connection in list(get_all_connections().items()):
                    try:
                        # ✅ Grace period: skip health check for first 30s after attach
                        connected_at = self._connected_at.get(account_id, 0)
                        if now - connected_at < 30:
                            print(f"🕐 Grace period active → {account_id}, skipping health check")
                            continue

                        health = getattr(connection, 'health_monitor', None)
                        status = getattr(health, 'health_status', None) if health else None

                        if status is not None and not status.get("connected", False):
                            print(f"💀 Keepalive detected dead connection → {account_id}")
                            await self.mark_disconnected(account_id)
                            continue

                        ts = getattr(connection, 'terminal_state', None)
                        if ts is not None:
                            _ = getattr(ts, 'connected', None)

                        print(f"💓 Connection alive → {account_id}")

                    except Exception as e:
                        print(f"⚠️ Keepalive check error for {account_id}: {e}")

                await asyncio.sleep(30)

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

        while True:
            try:
                await self._sync()
                await asyncio.sleep(5)
            except Exception as e:
                print(f"❌ Manager error: {e}")
                await asyncio.sleep(3)

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

        # ✅ Health check with grace period — don't kill fresh connections
        now = time.monotonic()
        for account_id, connection in list(get_all_connections().items()):
            try:
                connected_at = self._connected_at.get(account_id, 0)
                if now - connected_at < 30:
                    continue  # still in grace period

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
                await asyncio.sleep(5)

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

                # ✅ Record connection time for grace period
                self._connected_at[account_id] = time.monotonic()

            print(f"👂 Listener attached → {account_id}")

            # ✅ Store task reference so we can cancel it if connection is torn down
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
        """
        Wait for sync in background without blocking listener registration.
        Cancelled automatically when connection is torn down.
        """
        for attempt in range(1, 4):
            try:
                await asyncio.wait_for(connection.wait_synchronized(), timeout=120)
                print(f"✅ Background sync complete → {account_id}")
                return
            except asyncio.CancelledError:
                # ✅ Task was cancelled because connection was torn down — exit cleanly
                print(f"🛑 Background sync cancelled → {account_id}")
                return
            except asyncio.TimeoutError:
                print(f"⏳ Background sync timeout (attempt {attempt}/3) → {account_id}")
            except Exception as e:
                print(f"⚠️ Background sync error (attempt {attempt}/3) → {account_id}: {e}")
                # ✅ If the connection was closed under us, stop retrying immediately
                if "connection has been closed" in str(e).lower():
                    print(f"🛑 Connection closed, stopping background sync → {account_id}")
                    return
            await asyncio.sleep(5)

        print(f"⚠️ Sync never completed → {account_id}, listener still active")

    # =====================================
    # CANCEL BACKGROUND SYNC TASK
    # =====================================
    def _cancel_sync_task(self, account_id: str):
        task = self._sync_tasks.pop(account_id, None)
        if task and not task.done():
            task.cancel()

    # =====================================
    # REMOVE LISTENER
    # =====================================
    async def _remove_listener(self, acc: TradingAccount):
        account_id = acc.metaapi_account_id

        async with self._lock:
            connection = get_connection(account_id)
            listener = self._listeners.pop(account_id, None)
            self._attaching.discard(account_id)
            self._connected_at.pop(account_id, None)

        # ✅ Cancel background sync before closing connection
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

            print(f"🗑️ Listener removed → {account_id}")

        except Exception as e:
            print(f"❌ Remove failed {account_id}: {e}")

    # =====================================
    # MARK DISCONNECTED
    # =====================================
    async def mark_disconnected(self, account_id: str):
        async with self._lock:
            connection = get_connection(account_id)
            listener = self._listeners.pop(account_id, None)
            self._attaching.discard(account_id)
            self._connected_at.pop(account_id, None)

        # ✅ Cancel background sync before closing connection
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

        print(f"♻️ Marked for reconnection → {account_id}")


# =====================================
# SINGLETON
# =====================================

listener_manager = ListenerManager()