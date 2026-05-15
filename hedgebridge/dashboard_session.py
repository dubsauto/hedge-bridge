# hedgebridge/dashboard_session.py
#
# Per-user MetaAPI RPC connections for the dashboard.
#
# Design:
#   - Cache one RPC connection per account per user session.
#   - Per-account lock prevents duplicate concurrent builds for the same account.
#   - On any real failure the caller destroys the whole session; next request
#     gets a fresh API instance and fresh connections.
#   - CancelledError (SDK internally cancels RPC calls during reconnect) is
#     treated as transient by the caller — session is NOT destroyed.
#   - Session destroyed on logout or 30-min idle.

import asyncio
import os
import time
from typing import Dict, Optional

from metaapi_cloud_sdk import MetaApi


def _new_api() -> MetaApi:
    token = os.getenv("ACCESS_TOKEN")
    if not token:
        raise ValueError("ACCESS_TOKEN is not set")
    return MetaApi(token)

CONNECT_TIMEOUT = 20       # conn.connect() timeout (seconds)
IDLE_TIMEOUT    = 30 * 60  # 30 minutes


class _UserSession:
    def __init__(self, user_id):
        self.user_id       = user_id
        self._api          = _new_api()
        print(f"[Session] Fresh MetaApi instance → user={user_id}")
        self._connections: Dict[str, object] = {}
        self._lock         = asyncio.Lock()
        self._account_locks: Dict[str, asyncio.Lock] = {}
        self._last_active  = time.monotonic()

    def touch(self):
        self._last_active = time.monotonic()

    def is_idle(self) -> bool:
        return time.monotonic() - self._last_active > IDLE_TIMEOUT

    async def _get_account_lock(self, account_id: str) -> asyncio.Lock:
        async with self._lock:
            if account_id not in self._account_locks:
                self._account_locks[account_id] = asyncio.Lock()
            return self._account_locks[account_id]

    async def get_connection(self, account_id: str):
        self.touch()

        # Fast path — return cached connection
        async with self._lock:
            conn = self._connections.get(account_id)
            if conn is not None:
                print(f"[Session] Cached → user={self.user_id} account={account_id}")
                return conn

        # Serialize builds for the same account
        acc_lock = await self._get_account_lock(account_id)
        async with acc_lock:
            async with self._lock:
                conn = self._connections.get(account_id)
                if conn is not None:
                    print(f"[Session] Cached → user={self.user_id} account={account_id}")
                    return conn

            print(f"[Session] Building → user={self.user_id} account={account_id}")
            account = await self._api.metatrader_account_api.get_account(account_id)
            conn = account.get_rpc_connection()
            await asyncio.wait_for(conn.connect(), timeout=CONNECT_TIMEOUT)

            async with self._lock:
                self._connections[account_id] = conn

            print(f"[Session] Ready → user={self.user_id} account={account_id}")
            return conn

    async def destroy(self):
        print(f"[Session] Destroying → user={self.user_id}")
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
        print(f"[Session] Destroyed → user={self.user_id}")


class DashboardSessionManager:
    def __init__(self):
        self._sessions: Dict[str, _UserSession] = {}
        self._lock = asyncio.Lock()
        self._cleanup_task: Optional[asyncio.Task] = None

    def start(self):
        if self._cleanup_task is None or self._cleanup_task.done():
            self._cleanup_task = asyncio.create_task(self._cleanup_loop())
            print("[Session] Dashboard session manager started")

    async def _cleanup_loop(self):
        while True:
            await asyncio.sleep(5 * 60)
            try:
                async with self._lock:
                    idle = [uid for uid, s in self._sessions.items() if s.is_idle()]
                for uid in idle:
                    print(f"[Session] Idle timeout → user={uid}")
                    await self._destroy(uid)
            except Exception as e:
                print(f"[Session] Cleanup error: {e}")

    async def get_connection(self, user_id, account_id: str):
        async with self._lock:
            if user_id not in self._sessions:
                print(f"[Session] New session → user={user_id}")
                self._sessions[user_id] = _UserSession(user_id)
        return await self._sessions[user_id].get_connection(account_id)

    async def destroy_session(self, user_id):
        """Destroy all connections + API for this user. Next call gets a fresh start."""
        print(f"[Session] Resetting session → user={user_id}")
        await self._destroy(user_id)

    async def on_logout(self, user_id):
        print(f"[Session] Logout → user={user_id}")
        await self._destroy(user_id)

    async def _destroy(self, user_id):
        async with self._lock:
            session = self._sessions.pop(user_id, None)
        if session:
            await session.destroy()


dashboard_session = DashboardSessionManager()
