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

        # Per-account locks — so two callers for account A don't block account B
        self._account_locks: Dict[str, asyncio.Lock] = {}
        # Separate lock just for mutating the dicts above
        self._dict_lock = asyncio.Lock()

        self._verify_ttl = 10
        self._max_failures = 3
        self._watchdog_interval = 60
        self._watchdog_task: Optional[asyncio.Task] = None

        self._hard_reset_times: list = []
        self._sdk_reset_window = 300
        self._sdk_reset_threshold = 5

    def _get_account_lock(self, account_id: str) -> asyncio.Lock:
        if account_id not in self._account_locks:
            self._account_locks[account_id] = asyncio.Lock()
        return self._account_locks[account_id]

    # =====================================
    # START WATCHDOG
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
        async with self._dict_lock:
            account_ids = list(self._connections.keys())

        for account_id in account_ids:
            try:
                connection = self._connections.get(account_id)
                if not connection:
                    continue
                await asyncio.wait_for(connection.get_account_information(), timeout=5)
                self._failure_count[account_id] = 0
                self._verified_at[account_id] = time.monotonic()
                print(f"[RpcPool] Watchdog OK → {account_id}")
            except Exception as e:
                count = self._failure_count.get(account_id, 0) + 1
                self._failure_count[account_id] = count
                print(f"[RpcPool] Watchdog fail [{count}/{self._max_failures}] → {account_id}: {e}")
                if count >= self._max_failures:
                    print(f"[RpcPool] Max failures reached → full reset for {account_id}")
                    async with self._get_account_lock(account_id):
                        await self._hard_reset(account_id)

    # =====================================
    # GET API SINGLETON
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
            print(f"[RpcPool] {self._sdk_reset_threshold} hard resets in {self._sdk_reset_window}s → resetting SDK")
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
    # GET RPC CONNECTION (SHARED)
    # Per-account lock prevents concurrent builds for the same account
    # without blocking other accounts
    # =====================================
    async def get_connection(self, account_id: str):
        lock = self._get_account_lock(account_id)
        async with lock:
            connection = self._connections.get(account_id)
            last_verified = self._verified_at.get(account_id, 0)
            now = time.monotonic()

            # Recently verified — return immediately
            if connection and (now - last_verified) < self._verify_ttl:
                return connection

            # Probe existing connection — but outside the dict_lock
            # so we're not blocking other accounts
            if connection:
                try:
                    await asyncio.wait_for(
                        connection.get_account_information(), timeout=3
                    )
                    self._verified_at[account_id] = now
                    self._failure_count[account_id] = 0
                    return connection
                except BaseException:
                    count = self._failure_count.get(account_id, 0) + 1
                    self._failure_count[account_id] = count
                    print(f"[RpcPool] Stale connection [{count}/{self._max_failures}] → {account_id}")

                    # Always close the old connection before discarding it
                    await self._close_connection_safely(connection, account_id)
                    self._connections.pop(account_id, None)
                    self._verified_at.pop(account_id, None)

                    if count >= self._max_failures:
                        print(f"[RpcPool] Max failures → hard reset {account_id}")
                        await self._hard_reset(account_id)
                        # _hard_reset cleared failure count, now rebuild below

            # Build fresh connection
            try:
                connection = await self._build_connection(account_id)
                self._connections[account_id] = connection
                self._verified_at[account_id] = time.monotonic()
                self._failure_count[account_id] = 0
                return connection
            except Exception as e:
                print(f"[RpcPool] Build failed → {account_id}: {e}")
                raise

    # =====================================
    # SAFELY CLOSE A CONNECTION
    # Ensures SDK internal tasks are actually stopped
    # =====================================
    async def _close_connection_safely(self, connection, account_id: str):
        try:
            await asyncio.wait_for(connection.close(), timeout=10)
            print(f"[RpcPool] Closed connection → {account_id}")
        except asyncio.TimeoutError:
            print(f"[RpcPool] Close timed out (SDK may have zombie tasks) → {account_id}")
        except Exception as e:
            print(f"[RpcPool] Close error → {account_id}: {e}")

    # =====================================
    # BUILD FRESH CONNECTION
    # =====================================
    async def _build_connection(self, account_id: str):
        self._accounts.pop(account_id, None)
        account = await self.get_account(account_id)

        if account.state != "DEPLOYED":
            print(f"[RpcPool] Deploying → {account_id}")
            await account.deploy()
            await asyncio.sleep(5)

        for i in range(60):
            try:
                await account.reload()
            except Exception:
                pass

            if account.connection_status == "CONNECTED":
                break

            print(f"[RpcPool] Waiting broker connection [{i+1}/60] → {account_id}")
            await asyncio.sleep(1)
        else:
            raise Exception(f"[RpcPool] Broker not connected after 60s → {account_id}")

        connection = account.get_rpc_connection()
        await connection.connect()
        await connection.wait_synchronized()

        print(f"[RpcPool] Connection ready → {account_id}")
        return connection

    # =====================================
    # HARD RESET — always close before clearing
    # Must be called while holding the account lock
    # =====================================
    async def _hard_reset(self, account_id: str, count_toward_sdk_reset: bool = True):
        print(f"[RpcPool] Hard reset → {account_id}")

        connection = self._connections.pop(account_id, None)
        self._accounts.pop(account_id, None)
        self._verified_at.pop(account_id, None)
        self._failure_count[account_id] = 0

        if connection:
            await self._close_connection_safely(connection, account_id)

        if count_toward_sdk_reset:
            self._maybe_reset_sdk()

    async def invalidate(self, account_id: str):
        lock = self._get_account_lock(account_id)
        async with lock:
            await self._hard_reset(account_id, count_toward_sdk_reset=False)
        print(f"[RpcPool] Invalidated → {account_id}")

    async def invalidate_all(self):
        async with self._dict_lock:
            account_ids = list(self._connections.keys())
        for account_id in account_ids:
            await self.invalidate(account_id)


rpc_pool = RpcConnectionPool()