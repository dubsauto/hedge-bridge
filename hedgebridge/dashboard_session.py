# hedgebridge/dashboard_session.py
#
# Per-user MetaAPI connections for the dashboard (server 1 / web service only).
#
# Rules:
#   - Fresh SDK instance per user session, created on first request after login
#   - Connection built synchronously on first account touch (no background loops)
#   - Session destroyed immediately on logout OR after IDLE_TIMEOUT of inactivity
#   - No watchdog, no reconnect loops, no cooldowns, no background build tasks

import asyncio
import time
from typing import Dict, Optional

from hedgebridge.api_client import get_metaapi_client

IDLE_TIMEOUT    = 30 * 60   # 30 minutes — destroy session after this much silence
CONNECT_TIMEOUT = 20        # seconds for connection.connect()
SYNC_TIMEOUT    = 20        # seconds for wait_synchronized()


# ─── Per-user session ─────────────────────────────────────────────────────────

class _UserSession:
    def __init__(self, user_id):
        self.user_id = user_id
        self._api = get_metaapi_client()          # fresh SDK for this user
        self._connections: Dict[str, object] = {}
        self._lock = asyncio.Lock()
        self._last_active: float = time.monotonic()

    def touch(self):
        self._last_active = time.monotonic()

    def is_idle(self) -> bool:
        return time.monotonic() - self._last_active > IDLE_TIMEOUT

    async def get_connection(self, metaapi_account_id: str):
        self.touch()

        async with self._lock:
            conn = self._connections.get(metaapi_account_id)
            if conn is not None:
                return conn

            # No cached connection — build a fresh one now.
            # Caller's HTTP request waits here (20 + 20 s max = 40 s total).
            try:
                account = await self._api.metatrader_account_api.get_account(
                    metaapi_account_id
                )
                conn = account.get_rpc_connection()
                await asyncio.wait_for(conn.connect(), timeout=CONNECT_TIMEOUT)
                await asyncio.wait_for(conn.wait_synchronized(), timeout=SYNC_TIMEOUT)
                self._connections[metaapi_account_id] = conn
                print(
                    f"[Session] Ready → user={self.user_id} "
                    f"account={metaapi_account_id}"
                )
                return conn

            except Exception as e:
                raise Exception(
                    f"[Session] Could not connect {metaapi_account_id}: {e}"
                )

    async def destroy(self):
        print(f"[Session] Destroying → user={self.user_id}")
        async with self._lock:
            for account_id, conn in list(self._connections.items()):
                try:
                    await asyncio.wait_for(conn.close(), timeout=5)
                except Exception:
                    pass
            self._connections.clear()

        # Close the SDK instance so its WebSocket threads are freed
        try:
            if hasattr(self._api, "close"):
                result = self._api.close()
                if asyncio.iscoroutine(result):
                    await asyncio.wait_for(result, timeout=10)
        except Exception:
            pass


# ─── Manager (singleton) ──────────────────────────────────────────────────────

class DashboardSessionManager:
    def __init__(self):
        self._sessions: Dict[str, _UserSession] = {}
        self._lock = asyncio.Lock()
        self._cleanup_task: Optional[asyncio.Task] = None

    def start(self):
        """Call once at app startup (inside the async lifespan)."""
        if self._cleanup_task is None or self._cleanup_task.done():
            self._cleanup_task = asyncio.create_task(self._cleanup_loop())
            print("[Session] Dashboard session manager started")

    async def _cleanup_loop(self):
        while True:
            await asyncio.sleep(5 * 60)   # check every 5 minutes
            try:
                async with self._lock:
                    idle = [uid for uid, s in self._sessions.items() if s.is_idle()]

                for user_id in idle:
                    print(f"[Session] Idle timeout — destroying session user={user_id}")
                    await self._destroy(user_id)
            except Exception as e:
                print(f"[Session] Cleanup error: {e}")

    async def get_connection(self, user_id, metaapi_account_id: str):
        """
        Return an RPC connection for this user + account.
        Creates a fresh session/connection the first time.
        Raises if the broker cannot be reached within the timeout.
        """
        async with self._lock:
            if user_id not in self._sessions:
                print(f"[Session] New session → user={user_id}")
                self._sessions[user_id] = _UserSession(user_id)

        return await self._sessions[user_id].get_connection(metaapi_account_id)

    async def on_logout(self, user_id):
        """Call from the logout route to immediately free all connections."""
        print(f"[Session] Logout → user={user_id}")
        await self._destroy(user_id)

    async def _destroy(self, user_id):
        async with self._lock:
            session = self._sessions.pop(user_id, None)
        if session:
            await session.destroy()


# Singleton used by routes
dashboard_session = DashboardSessionManager()
