# hedgebridge/rpc_pool.py

import asyncio
import time
from typing import Optional, Dict, Any
from hedgebridge.api_client import get_metaapi_client


class RpcConnectionPool:
    """
    Shared RPC connection pool for all services.
    One RPC connection per account, reused across account_management and trading.
    Streaming connections are separate — owned by listener_manager.
    """

    def __init__(self):
        self._lock = asyncio.Lock()
        self._api = None

        # account_id -> MetaApi account object (cached)
        self._accounts: Dict[str, Any] = {}

        # account_id -> RPC connection (cached)
        self._connections: Dict[str, Any] = {}

        # account_id -> last verified timestamp
        self._verified_at: Dict[str, float] = {}

        # How long before we re-verify a connection (seconds)
        self._verify_ttl = 10

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
        """
        Returns a live, synchronized RPC connection for the account.
        Reuses existing connection if recently verified.
        Creates a new one if missing or dead.
        """
        async with self._lock:
            connection = self._connections.get(account_id)
            last_verified = self._verified_at.get(account_id, 0)
            now = time.monotonic()

            # ✅ Recently verified — return immediately without probing
            if connection and (now - last_verified) < self._verify_ttl:
                return connection

            # ✅ Probe the existing connection
            if connection:
                try:
                    await asyncio.wait_for(
                        connection.get_account_information(),
                        timeout=3
                    )
                    self._verified_at[account_id] = now
                    return connection
                except Exception:
                    print(f"[RpcPool] Stale connection → {account_id}, reconnecting")
                    self._connections.pop(account_id, None)
                    self._verified_at.pop(account_id, None)

            # ✅ Build a fresh connection
            connection = await self._build_connection(account_id)
            self._connections[account_id] = connection
            self._verified_at[account_id] = time.monotonic()
            return connection

    # =====================================
    # BUILD FRESH CONNECTION
    # =====================================
    async def _build_connection(self, account_id: str):
        account = await self.get_account(account_id)

        if account.state != "DEPLOYED":
            print(f"[RpcPool] Deploying → {account_id}")
            await account.deploy()
            await asyncio.sleep(5)

        # Wait for broker connection
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
    # INVALIDATE (call on account removal)
    # =====================================
    async def invalidate(self, account_id: str):
        async with self._lock:
            connection = self._connections.pop(account_id, None)
            self._accounts.pop(account_id, None)
            self._verified_at.pop(account_id, None)

        if connection:
            try:
                await connection.close()
            except Exception:
                pass

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