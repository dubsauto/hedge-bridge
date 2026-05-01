# hedgebridge/rpc_pool.py

import asyncio
import time
from typing import Optional, Dict, Any
from hedgebridge.api_client import get_metaapi_client


class RpcConnectionPool:
    def __init__(self):
        self._lock = asyncio.Lock()
        self._api = None

        self._accounts: Dict[str, Any] = {}
        self._connections: Dict[str, Any] = {}
        self._verified_at: Dict[str, float] = {}
        self._failure_count: Dict[str, int] = {}  # NEW: track consecutive failures
        self._building: Dict[str, bool] = {}       # NEW: prevent concurrent builds

        self._verify_ttl = 10
        self._max_failures = 3        # after this many fails, full reset
        self._watchdog_interval = 60  # seconds between health checks
        self._watchdog_task: Optional[asyncio.Task] = None

    # =====================================
    # START WATCHDOG (call from app startup)
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
                # Reset failure count on success
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
            # If another coroutine is already building, wait and return when done
            if self._building.get(account_id):
                pass  # fall through, will find it after lock released

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
                except Exception:
                    count = self._failure_count.get(account_id, 0) + 1
                    self._failure_count[account_id] = count
                    print(f"[RpcPool] Stale connection [{count}/{self._max_failures}] → {account_id}, reconnecting")

                    if count >= self._max_failures:
                        print(f"[RpcPool] Max failures → hard reset {account_id}")
                        await self._hard_reset(account_id)
                    else:
                        self._connections.pop(account_id, None)
                        self._verified_at.pop(account_id, None)

            # Build a fresh connection
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
        # Evict stale cached account object so we get fresh state
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
    # HARD RESET — nuke everything for account
    # =====================================
    async def _hard_reset(self, account_id: str):
        """
        Closes and discards everything for an account.
        Does NOT redeploy — that happens lazily on next get_connection().
        Called with lock already held OR from watchdog (no lock needed there).
        """
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

    # =====================================
    # INVALIDATE (call on account removal)
    # =====================================
    async def invalidate(self, account_id: str):
        async with self._lock:
            await self._hard_reset(account_id)
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