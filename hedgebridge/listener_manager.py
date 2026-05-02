# hedgebridge/listener_manager.py

import asyncio
import sys
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
GLOBAL_OUTAGE_THRESHOLD = 0.6
GLOBAL_OUTAGE_WINDOW = 10.0
GLOBAL_OUTAGE_COOLDOWN = 45

# SDK call timeouts
SDK_GET_ACCOUNT_TIMEOUT = 30
SDK_DEPLOY_TIMEOUT = 60
SDK_CONNECT_TIMEOUT = 30
SDK_UNDEPLOY_TIMEOUT = 60
SDK_BROKER_WAIT_TIMEOUT = 60

# Stale SDK detection
STALE_SDK_THRESHOLD = 3


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

        # Global outage detection
        self._disconnect_times = {}
        self._global_outage = False
        self._outage_recovery_task = None

        # Stale SDK detection
        self._stale_sdk_count = 0

    # =====================================
    # GET METAAPI CLIENT — singleton, never reset
    # =====================================
    async def _get_api(self):
        if self._api is None:
            self._api = get_metaapi_client()
        return self._api

    # =====================================
    # SDK CALL WRAPPERS WITH TIMEOUTS
    # These prevent the event loop from freezing
    # if the SDK hangs on a network issue
    # =====================================
    async def _sdk_get_account(self, account_id: str):
        """get_account() with hard timeout — never hangs the event loop."""
        api = await self._get_api()
        try:
            return await asyncio.wait_for(
                api.metatrader_account_api.get_account(account_id),
                timeout=SDK_GET_ACCOUNT_TIMEOUT
            )
        except asyncio.TimeoutError:
            self._stale_sdk_count += 1
            print(f"❌ get_account() timed out [{self._stale_sdk_count}/{STALE_SDK_THRESHOLD}] → {account_id}")
            await self._check_stale_sdk(account_id)
            raise

    async def _sdk_deploy(self, account, account_id: str):
        """account.deploy() with hard timeout."""
        try:
            return await asyncio.wait_for(
                account.deploy(),
                timeout=SDK_DEPLOY_TIMEOUT
            )
        except asyncio.TimeoutError:
            self._stale_sdk_count += 1
            print(f"❌ deploy() timed out [{self._stale_sdk_count}/{STALE_SDK_THRESHOLD}] → {account_id}")
            await self._check_stale_sdk(account_id)
            raise

    async def _sdk_undeploy(self, account, account_id: str):
        """account.undeploy() with hard timeout."""
        try:
            return await asyncio.wait_for(
                account.undeploy(),
                timeout=SDK_UNDEPLOY_TIMEOUT
            )
        except asyncio.TimeoutError:
            self._stale_sdk_count += 1
            print(f"❌ undeploy() timed out [{self._stale_sdk_count}/{STALE_SDK_THRESHOLD}] → {account_id}")
            await self._check_stale_sdk(account_id)
            raise

    async def _sdk_connect(self, connection, account_id: str):
        """connection.connect() with hard timeout."""
        try:
            return await asyncio.wait_for(
                connection.connect(),
                timeout=SDK_CONNECT_TIMEOUT
            )
        except asyncio.TimeoutError:
            self._stale_sdk_count += 1
            print(f"❌ connection.connect() timed out [{self._stale_sdk_count}/{STALE_SDK_THRESHOLD}] → {account_id}")
            await self._check_stale_sdk(account_id)
            raise

    async def _check_stale_sdk(self, account_id: str):
        """
        If too many SDK calls time out, the SDK is truly stale.
        Clean exit so supervisor restarts us fresh.
        This is safer than a SDK reset which leaks RAM from old instances.
        """
        if self._stale_sdk_count >= STALE_SDK_THRESHOLD:
            print(
                f"☢️ SDK stale threshold reached ({self._stale_sdk_count} timeouts) "
                f"— clean exit for supervisor restart"
            )
            sys.exit(1)

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
    # GLOBAL OUTAGE DETECTION
    # =====================================
    def _record_disconnect(self, account_id: str):
        now = time.monotonic()
        self._disconnect_times[account_id] = now

        # Prune stale entries older than 2x window
        self._disconnect_times = {
            k: v for k, v in self._disconnect_times.items()
            if now - v <= GLOBAL_OUTAGE_WINDOW * 2
        }

        recent = [
            t for t in self._disconnect_times.values()
            if now - t <= GLOBAL_OUTAGE_WINDOW
        ]

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

        # Reset counters — outage was not per-account
        self._reconnect_attempts.clear()
        self._disconnect_times.clear()
        # Reset stale SDK count — outage timeouts are not SDK staleness
        self._stale_sdk_count = 0

        # NO SDK reset here — SDK is fine, it was a network outage
        self._global_outage = False

        print("🌐 Global outage cooldown complete — queuing fresh reconnects for all accounts")

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

                if self._global_outage:
                    print(f"⏸️ Global outage active — requeueing {account_id}")
                    await asyncio.sleep(5)
                    try:
                        self._reconnect_queue.put_nowait(account_id)
                    except Exception:
                        pass
                    self._reconnect_queue.task_done()
                    continue

                attempts = self._reconnect_attempts.get(account_id, 0) + 1
                self._reconnect_attempts[account_id] = attempts

                print(f"🔁 Reconnect attempt {attempts}/{self._reconnect_limit} → {account_id}")

                if attempts >= self._reconnect_limit:
                    print(f"💣 Reconnect limit hit → {account_id}, triggering nuclear reset")
                    self._reconnect_attempts.pop(account_id, None)
                    await self._nuclear_reset(account_id)
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
    # No SDK reset — only tears down and rebuilds
    # the connection for this specific account
    # =====================================
    async def _nuclear_reset(self, account_id: str):
        print(f"☢️ Nuclear reset starting → {account_id}")

        # ── Teardown ──
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

        await asyncio.sleep(15)

        # ── Redeploy using same SDK singleton ──
        try:
            account = await self._sdk_get_account(account_id)

            if account.state.upper() != "DEPLOYED":
                print(f"🚀 Nuclear deploy → {account_id}")
                await self._sdk_deploy(account, account_id)
                await asyncio.sleep(DEPLOY_WAIT)
            else:
                print(f"🔄 Nuclear undeploy → redeploy → {account_id}")
                await self._sdk_undeploy(account, account_id)
                await asyncio.sleep(10)
                await self._sdk_deploy(account, account_id)
                await asyncio.sleep(DEPLOY_WAIT)

        except asyncio.TimeoutError:
            # _check_stale_sdk already called inside wrapper
            # just requeue and let it retry or exit
            print(f"⚠️ Nuclear redeploy timed out → {account_id}, requeueing")
        except Exception as e:
            print(f"⚠️ Nuclear redeploy failed → {account_id}: {e}")

        print(f"🔁 Queuing fresh reconnect after nuclear reset → {account_id}")
        try:
            self._reconnect_queue.put_nowait(account_id)
        except Exception:
            pass

    # =====================================
    # DB SYNC + HEALTH CHECK
    # =====================================
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

                health = getattr(connection, 'health_monitor', None)
                status = getattr(health, 'health_status', None) if health else None

                if status is not None and not status.get("connected", False):
                    print(f"💀 Dead connection detected → {account_id}")
                    await self.mark_disconnected(account_id)

            except Exception:
                pass

    # =====================================
    # ENSURE LISTENER EXISTS
    # SDK calls are OUTSIDE the lock to prevent
    # the lock from blocking all other coroutines
    # if an SDK call hangs
    # =====================================
    async def _ensure_listener(self, acc: TradingAccount):
        account_id = acc.metaapi_account_id

        # ── Lock: just check and reserve ──
        async with self._lock:
            if get_connection(account_id) is not None:
                return
            if account_id in self._attaching:
                print(f"⏸️ Already attaching → {account_id}, skipping")
                return
            self._attaching.add(account_id)

        # ── ALL SDK calls outside the lock ──
        connection = None

        try:
            print(f"🔌 Attaching listener → {account_id}")

            try:
                account = await self._sdk_get_account(account_id)
            except asyncio.TimeoutError:
                return  # stale SDK check already called, don't proceed

            if account.state.upper() != "DEPLOYED":
                print(f"🚀 Deploying → {account_id}")
                try:
                    await self._sdk_deploy(account, account_id)
                    await asyncio.sleep(DEPLOY_WAIT)
                except asyncio.TimeoutError:
                    return

            print(f"⏳ Waiting for broker connection → {account_id}")
            connected = False

            for i in range(SDK_BROKER_WAIT_TIMEOUT):
                try:
                    await asyncio.wait_for(account.reload(), timeout=5)
                except (asyncio.TimeoutError, Exception):
                    pass

                status = account.connection_status
                print(f"   [{i+1}/{SDK_BROKER_WAIT_TIMEOUT}] connection_status={status}")

                if status == "CONNECTED":
                    connected = True
                    break

                await asyncio.sleep(1)

            if not connected:
                print(f"❌ Broker not connected after {SDK_BROKER_WAIT_TIMEOUT}s → {account_id}")
                return

            await asyncio.sleep(2)

            connection = account.get_streaming_connection()
            print(f"🔗 Connecting stream → {account_id}")

            try:
                await self._sdk_connect(connection, account_id)
            except asyncio.TimeoutError:
                try:
                    await connection.close()
                except Exception:
                    pass
                return

            # ── Lock: just store results ──
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
            # Always release attaching lock
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
                self._reconnect_attempts.pop(account_id, None)
                self._disconnect_times.pop(account_id, None)
                # Reset stale counter on successful sync — SDK is healthy
                self._stale_sdk_count = 0
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
                    await self.mark_disconnected(account_id)
                    return

            await asyncio.sleep(5)

        print(f"⚠️ Sync never completed after 3 attempts → {account_id}, triggering reconnect")
        await self.mark_disconnected(account_id)

    # =====================================
    # CANCEL SYNC TASK + PRUNE COMPLETED
    # =====================================
    def _cancel_sync_task(self, account_id: str):
        task = self._sync_tasks.pop(account_id, None)
        if task and not task.done():
            task.cancel()

        # Prune all other completed tasks while we're here
        completed = [k for k, t in self._sync_tasks.items() if t.done()]
        for k in completed:
            self._sync_tasks.pop(k, None)

    # =====================================
    # REMOVE LISTENER
    # =====================================
    async def _remove_listener(self, acc: TradingAccount):
        account_id = acc.metaapi_account_id

        # Check if we even have anything to remove
        async with self._lock:
            has_connection = get_connection(account_id) is not None
            has_listener = account_id in self._listeners

        if not has_connection and not has_listener:
            return

        try:
            account = await self._sdk_get_account(account_id)

            if account.state.upper() != "UNDEPLOYED":
                print(f"🚀 Undeploying before remove → {account_id}")
                try:
                    await self._sdk_undeploy(account, account_id)
                    await asyncio.sleep(7)
                except asyncio.TimeoutError:
                    print(f"⚠️ Undeploy timed out → {account_id}, continuing removal anyway")

        except asyncio.TimeoutError:
            print(f"⚠️ get_account timed out during remove → {account_id}, continuing anyway")
        except Exception as e:
            print(f"⚠️ get_account failed during remove → {account_id}: {e}")

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
        self._record_disconnect(account_id)

        async with self._lock:
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

        if not self._global_outage:
            try:
                self._reconnect_queue.put_nowait(account_id)
            except asyncio.QueueFull:
                pass


# =====================================
# SINGLETON
# =====================================
listener_manager = ListenerManager()