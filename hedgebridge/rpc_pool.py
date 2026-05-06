# hedgebridge/rpc_pool.py

import asyncio
import time
from typing import Optional, Dict, Any
from hedgebridge.api_client import get_metaapi_client, reset_metaapi_client


class RpcConnectionPool:
    def __init__(self):
        self._api = None

        self._accounts: Dict[str, Any] = {}
        self._connections: Dict[str, Any] = {}
        self._verified_at: Dict[str, float] = {}
        self._failure_count: Dict[str, int] = {}
        self._last_used: Dict[str, float] = {}

        # Per-account build locks — prevents duplicate builds for same account
        self._account_locks: Dict[str, asyncio.Lock] = {}

        # Cooldown: after a hard reset, don't immediately retry
        self._cooldown_until: Dict[str, float] = {}
        self._cooldown_seconds = 60            # longer cooldown after failure (was 30s)

        self._verify_ttl = 10
        self._max_failures = 3
        self._watchdog_interval = 120          # probe every 2 min instead of 1
        self._idle_evict_seconds = 5 * 60     # evict after 5 min idle (was 30)
        self._watchdog_task: Optional[asyncio.Task] = None

        self._hard_reset_times: list = []
        self._sdk_reset_window = 300
        self._sdk_reset_threshold = 5

        # Limit simultaneous connection builds to avoid overloading 0.5 CPU
        self._build_semaphore = asyncio.Semaphore(2)

    def _get_account_lock(self, account_id: str) -> asyncio.Lock:
        if account_id not in self._account_locks:
            self._account_locks[account_id] = asyncio.Lock()
        return self._account_locks[account_id]

    # =====================================
    # WATCHDOG
    # =====================================
    def start_watchdog(self):
        if self._watchdog_task is None or self._watchdog_task.done():
            self._watchdog_task = asyncio.create_task(self._watchdog_loop())
            print("[RpcPool] Watchdog started")

    async def _watchdog_loop(self):
        while True:
            await asyncio.sleep(self._watchdog_interval)
            try:
                await self._health_check_all()
            except Exception as e:
                print(f"[RpcPool] Watchdog error: {e}")

    async def _health_check_all(self):
        account_ids = list(self._connections.keys())
        now = time.monotonic()

        for account_id in account_ids:
            # ─── IDLE EVICTION ───────────────────────────────────────────
            # If no caller has used this connection within the eviction
            # window, close it cleanly and free the memory.  The next
            # poll from the frontend will rebuild it on demand.
            last_used = self._last_used.get(account_id, 0)
            idle_seconds = now - last_used

            if idle_seconds > self._idle_evict_seconds:
                print(
                    f"[RpcPool] Evicting idle connection "
                    f"({idle_seconds / 60:.1f} min idle) → {account_id}"
                )
                async with self._get_account_lock(account_id):
                    await self._hard_reset(account_id, count_toward_sdk_reset=False)
                continue

            # ─── NORMAL HEALTH PROBE ─────────────────────────────────────
            try:
                connection = self._connections.get(account_id)
                if not connection:
                    continue
                await asyncio.wait_for(
                    connection.get_account_information(), timeout=5
                )
                self._failure_count[account_id] = 0
                self._verified_at[account_id] = now
                print(f"[RpcPool] Watchdog OK → {account_id}")

            except Exception as e:
                count = self._failure_count.get(account_id, 0) + 1
                self._failure_count[account_id] = count
                print(
                    f"[RpcPool] Watchdog fail "
                    f"[{count}/{self._max_failures}] → {account_id}: {e}"
                )
                if count >= self._max_failures:
                    print(f"[RpcPool] Max failures → hard reset {account_id}")
                    async with self._get_account_lock(account_id):
                        await self._hard_reset(account_id)

    # =====================================
    # API SINGLETON
    # =====================================
    async def _get_api(self):
        if self._api is None:
            self._api = get_metaapi_client()
        return self._api

    def _maybe_reset_sdk(self):
        now = time.monotonic()
        self._hard_reset_times = [
            t for t in self._hard_reset_times if now - t < self._sdk_reset_window
        ]
        self._hard_reset_times.append(now)
        if len(self._hard_reset_times) >= self._sdk_reset_threshold:
            print(
                f"[RpcPool] {self._sdk_reset_threshold} hard resets "
                f"in {self._sdk_reset_window}s → resetting SDK"
            )
            try:
                self._api = reset_metaapi_client()
                self._hard_reset_times.clear()
                print("[RpcPool] MetaApi SDK reset complete")
            except Exception as e:
                print(f"[RpcPool] SDK reset failed: {e}")
                self._api = None

    # =====================================
    # GET ACCOUNT (CACHED)
    # =====================================
    async def get_account(self, account_id: str):
        if account_id in self._accounts:
            return self._accounts[account_id]
        api = await self._get_api()
        account = await api.metatrader_account_api.get_account(account_id)
        self._accounts[account_id] = account
        return account

    # =====================================
    # GET CONNECTION
    # =====================================
    async def get_connection(self, account_id: str):
        # Check cooldown BEFORE acquiring lock — fast rejection
        cooldown_until = self._cooldown_until.get(account_id, 0)
        if time.monotonic() < cooldown_until:
            remaining = round(cooldown_until - time.monotonic(), 1)
            raise Exception(
                f"[RpcPool] Account {account_id} in cooldown "
                f"for {remaining}s after reset"
            )

        lock = self._get_account_lock(account_id)
        async with lock:
            # Re-check cooldown inside lock (another coroutine may have
            # triggered a reset while we were waiting for the lock)
            cooldown_until = self._cooldown_until.get(account_id, 0)
            if time.monotonic() < cooldown_until:
                remaining = round(cooldown_until - time.monotonic(), 1)
                raise Exception(
                    f"[RpcPool] Account {account_id} in cooldown "
                    f"for {remaining}s after reset"
                )

            connection = self._connections.get(account_id)
            last_verified = self._verified_at.get(account_id, 0)
            now = time.monotonic()

            # Recently verified — stamp last_used and return immediately
            if connection and (now - last_verified) < self._verify_ttl:
                self._last_used[account_id] = now
                return connection

            # Probe existing connection
            if connection:
                try:
                    await asyncio.wait_for(
                        connection.get_account_information(), timeout=3
                    )
                    self._verified_at[account_id] = now
                    self._last_used[account_id] = now
                    self._failure_count[account_id] = 0
                    return connection
                except BaseException:
                    count = self._failure_count.get(account_id, 0) + 1
                    self._failure_count[account_id] = count
                    print(
                        f"[RpcPool] Stale connection "
                        f"[{count}/{self._max_failures}] → {account_id}"
                    )

                    await self._close_connection_safely(connection, account_id)
                    self._connections.pop(account_id, None)
                    self._verified_at.pop(account_id, None)

                    if count >= self._max_failures:
                        await self._hard_reset(account_id)
                        raise Exception(
                            f"[RpcPool] {account_id} hard reset after "
                            f"{count} failures, retry after cooldown"
                        )

            # Build fresh connection
            try:
                connection = await self._build_connection(account_id)
                self._connections[account_id] = connection
                self._verified_at[account_id] = time.monotonic()
                self._last_used[account_id] = time.monotonic()
                self._failure_count[account_id] = 0
                return connection
            except Exception as e:
                print(f"[RpcPool] Build failed → {account_id}: {e}")
                # Apply cooldown so callers stop hammering a broken account
                self._cooldown_until[account_id] = (
                    time.monotonic() + self._cooldown_seconds
                )
                raise

    # =====================================
    # CLOSE SAFELY
    # =====================================
    async def _close_connection_safely(self, connection, account_id: str):
        try:
            await asyncio.wait_for(connection.close(), timeout=10)
            print(f"[RpcPool] Closed connection → {account_id}")
        except asyncio.TimeoutError:
            print(f"[RpcPool] Close timed out → {account_id}")
        except Exception as e:
            print(f"[RpcPool] Close error → {account_id}: {e}")

    # =====================================
    # BUILD CONNECTION
    # =====================================
    async def _build_connection(self, account_id: str):
        async with self._build_semaphore:
            return await self._build_connection_inner(account_id)

    async def _build_connection_inner(self, account_id: str):
        self._accounts.pop(account_id, None)
        account = await self.get_account(account_id)

        if account.state != "DEPLOYED":
            print(f"[RpcPool] Deploying → {account_id}")
            await account.deploy()
            await asyncio.sleep(5)

        # Wait up to 8s for broker-connected status.  Some brokers report
        # DISCONNECTED even when RPC works, so we fall through and let
        # wait_synchronized() be the real gate.  Keeping this short prevents
        # stacked tasks from holding memory when MetaApi is unreachable.
        print(f"[RpcPool] Waiting for broker connection → {account_id}")
        for i in range(8):
            try:
                await account.reload()
            except Exception:
                pass

            state = getattr(account, "connection_status", None)
            if state == "CONNECTED":
                break

            print(
                f"[RpcPool] Waiting broker connection "
                f"[{i+1}/8] → {account_id} (status: {state})"
            )
            await asyncio.sleep(1)
        else:
            print(
                f"[RpcPool] Broker status not CONNECTED after 8s → {account_id}, "
                f"attempting RPC anyway"
            )

        connection = account.get_rpc_connection()
        await connection.connect()

        try:
            await asyncio.wait_for(connection.wait_synchronized(), timeout=30)
        except asyncio.TimeoutError:
            await self._close_connection_safely(connection, account_id)
            raise Exception(
                f"[RpcPool] wait_synchronized timed out → {account_id}. "
                f"Broker may not be reachable."
            )

        print(f"[RpcPool] Connection ready → {account_id}")
        return connection

    # =====================================
    # HARD RESET
    # Must be called while holding the account lock
    # =====================================
    async def _hard_reset(self, account_id: str, count_toward_sdk_reset: bool = True):
        print(f"[RpcPool] Hard reset → {account_id}")

        connection = self._connections.pop(account_id, None)
        self._accounts.pop(account_id, None)
        self._verified_at.pop(account_id, None)
        self._failure_count[account_id] = 0
        self._last_used.pop(account_id, None)
        self._cooldown_until[account_id] = time.monotonic() + self._cooldown_seconds

        if connection:
            await self._close_connection_safely(connection, account_id)

        if count_toward_sdk_reset:
            self._maybe_reset_sdk()

    # =====================================
    # INVALIDATE (explicit, e.g. after undeploy)
    # =====================================
    async def invalidate(self, account_id: str):
        lock = self._get_account_lock(account_id)
        async with lock:
            await self._hard_reset(account_id, count_toward_sdk_reset=False)
            # Clear cooldown on explicit invalidate so a redeploy can
            # connect immediately without waiting 30s
            self._cooldown_until.pop(account_id, None)
        print(f"[RpcPool] Invalidated → {account_id}")

    async def invalidate_all(self):
        account_ids = list(self._connections.keys())
        for account_id in account_ids:
            await self.invalidate(account_id)


rpc_pool = RpcConnectionPool()