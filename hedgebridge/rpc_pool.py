# hedgebridge/rpc_pool.py

import asyncio
import time
from typing import Optional, Dict, Any
from hedgebridge.api_client import get_metaapi_client, reset_metaapi_client


class RpcConnectionPool:
    def __init__(self):
        self._lock = asyncio.Lock()
        self._api = None

        self._accounts: Dict[str, Any] = {}
        self._connections: Dict[str, Any] = {}
        self._verified_at: Dict[str, float] = {}
        self._failure_count: Dict[str, int] = {}
        self._building: Dict[str, bool] = {}

        self._verify_ttl = 10
        self._max_failures = 3
        self._watchdog_interval = 60
        self._watchdog_task: Optional[asyncio.Task] = None

        # Track total hard resets across all accounts — if too many happen
        # in a short window, reset the MetaApi SDK singleton itself
        self._hard_reset_times: list = []
        self._sdk_reset_window = 300   # seconds (5 min)
        self._sdk_reset_threshold = 5  # N hard resets within window → reset SDK

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
        async with self._lock:
            account_ids = list(self._connections.keys())

        for account_id in account_ids:
            try:
                connection = self._connections.get(account_id)
                if not connection:
                    continue

                await asyncio.wait_for(
                    connection.get_account_information(),
                    timeout=5
                )
                self._failure_count[account_id] = 0
                self._verified_at[account_id] = time.monotonic()
                print(f"[RpcPool] Watchdog OK → {account_id}")

            except Exception as e:
                count = self._failure_count.get(account_id, 0) + 1
                self._failure_count[account_id] = count
                print(f"[RpcPool] Watchdog fail [{count}/{self._max_failures}] → {account_id}: {e}")

                if count >= self._max_failures:
                    print(f"[RpcPool] Max failures reached → full reset for {account_id}")
                    await self._hard_reset(account_id)

    # =====================================
    # GET API SINGLETON
    # =====================================
    async def _get_api(self):
        if self._api is None:
            self._api = get_metaapi_client()
        return self._api

    # =====================================
    # MAYBE RESET SDK
    # Called after every hard reset to decide if the SDK itself needs cycling
    # =====================================
    def _maybe_reset_sdk(self):
        now = time.monotonic()

        # Prune old entries outside the window
        self._hard_reset_times = [
            t for t in self._hard_reset_times
            if now - t < self._sdk_reset_window
        ]

        # Record this reset
        self._hard_reset_times.append(now)

        if len(self._hard_reset_times) >= self._sdk_reset_threshold:
            print(
                f"[RpcPool] {self._sdk_reset_threshold} hard resets in "
                f"{self._sdk_reset_window}s → resetting MetaApi SDK singleton"
            )
            try:
                self._api = reset_metaapi_client()
                self._hard_reset_times.clear()
                print("[RpcPool] MetaApi SDK reset complete")
            except Exception as e:
                print(f"[RpcPool] SDK reset failed: {e}")
                self._api = None  # force lazy reinit on next _get_api()

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
    # =====================================
    async def get_connection(self, account_id: str):
        async with self._lock:
            if self._building.get(account_id):
                pass  # another coroutine is building — fall through

            connection = self._connections.get(account_id)
            last_verified = self._verified_at.get(account_id, 0)
            now = time.monotonic()

            # Recently verified — return immediately
            if connection and (now - last_verified) < self._verify_ttl:
                return connection

            # Probe existing connection
            if connection:
                try:
                    await asyncio.wait_for(
                        connection.get_account_information(),
                        timeout=3
                    )
                    self._verified_at[account_id] = now
                    self._failure_count[account_id] = 0
                    return connection
                except BaseException:   # catches CancelledError too
                    count = self._failure_count.get(account_id, 0) + 1
                    self._failure_count[account_id] = count
                    print(f"[RpcPool] Stale connection [{count}/{self._max_failures}] → {account_id}, reconnecting")

                    if count >= self._max_failures:
                        print(f"[RpcPool] Max failures → hard reset {account_id}")
                        await self._hard_reset(account_id)
                    else:
                        self._connections.pop(account_id, None)
                        self._verified_at.pop(account_id, None)

            self._building[account_id] = True

        try:
            connection = await self._build_connection(account_id)
            async with self._lock:
                self._connections[account_id] = connection
                self._verified_at[account_id] = time.monotonic()
                self._failure_count[account_id] = 0
            return connection
        except Exception as e:
            print(f"[RpcPool] Build failed → {account_id}: {e}")
            raise
        finally:
            async with self._lock:
                self._building.pop(account_id, None)

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

    async def _hard_reset(self, account_id: str, count_toward_sdk_reset: bool = True):
        print(f"[RpcPool] Hard reset → {account_id}")

        connection = self._connections.pop(account_id, None)
        self._accounts.pop(account_id, None)
        self._verified_at.pop(account_id, None)
        self._failure_count[account_id] = 0

        if connection:
            try:
                await asyncio.wait_for(connection.close(), timeout=5)
            except Exception as e:
                print(f"[RpcPool] Close error during reset → {account_id}: {e}")

        if count_toward_sdk_reset:
            self._maybe_reset_sdk()

    async def invalidate(self, account_id: str):
        async with self._lock:
            await self._hard_reset(account_id, count_toward_sdk_reset=False)
        print(f"[RpcPool] Invalidated → {account_id}")

    # =====================================
    # INVALIDATE ALL
    # =====================================
    async def invalidate_all(self):
        async with self._lock:
            account_ids = list(self._connections.keys())

        for account_id in account_ids:
            await self.invalidate(account_id)


# =====================================
# SINGLETON
# =====================================
rpc_pool = RpcConnectionPool()