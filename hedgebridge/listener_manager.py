# hedgebridge/listener_manager.py

import asyncio
from sqlalchemy.orm import Session
from app.database import SessionLocal
from app.model import TradingAccount, CopyRelationship
from hedgebridge.listener import MetaApiTradeListener
from hedgebridge.api_client import get_metaapi_client


class ListenerManager:

    def __init__(self):
        self._lock = asyncio.Lock()
        self._running = False
        self._connections = {}   # account_id -> connection
        self._listeners = {}     # account_id -> listener

    async def start(self):
        if self._running:
            return

        self._running = True
        print("🚀 Listener Manager started")

        while True:
            try:
                async with self._lock:
                    await self._sync()
                await asyncio.sleep(5)
            except Exception as e:
                print(f"❌ Manager error: {e}")
                await asyncio.sleep(3)

    async def _sync(self):
        db: Session = SessionLocal()

        try:
            accounts = db.query(TradingAccount).all()

            for acc in accounts:

                # Only deployed accounts
                if acc.state != "deployed":
                    await self._remove_listener(acc)
                    continue

                # Must be part of copy trading
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

    async def _ensure_listener(self, acc: TradingAccount):
        account_id = acc.metaapi_account_id

        if account_id in self._connections:
            return

        try:
            print(f"🔌 Attaching listener {acc.metaapi_account_id}")

            api = get_metaapi_client()
            account = await api.metatrader_account_api.get_account(account_id)

            # Deploy if needed
            if account.state != 'DEPLOYED':
                print(f"🚀 Deploying {acc.metaapi_account_id}")
                await account.deploy()

            # 🔥 WAIT UNTIL BROKER CONNECTS (IMPORTANT)
            timeout = 60
            for _ in range(timeout):
                account = await api.metatrader_account_api.get_account(account_id)

                if account.connection_status == "CONNECTED":
                    break

                print(f"⏳ Waiting broker connection {acc.metaapi_account_id}...")
                await asyncio.sleep(1)
            else:
                print(f"❌ Broker not connected for {acc.metaapi_account_id} after {timeout} seconds")
                return

            # Now safe to connect streaming
            connection = account.get_streaming_connection()

            await connection.connect()
            await connection.wait_synchronized()

            if account_id in self._connections:
                return

            listener = MetaApiTradeListener(acc.id)

            connection.add_synchronization_listener(listener)
            

            self._connections[account_id] = connection
            self._listeners[account_id] = listener

            print(f"👂 Listener attached → {acc.metaapi_account_id}")

        except Exception as e:
            print(f"❌ Attach failed {acc.id}: {e}")

    async def _remove_listener(self, acc: TradingAccount):
        account_id = acc.metaapi_account_id

        if account_id not in self._connections:
            return

        try:
            print(f"🛑 Removing listener {acc.id}")
            connection = self._connections[account_id]
            listener = self._listeners[account_id]

            connection.remove_synchronization_listener(listener)

            self._connections.pop(account_id, None)
            self._listeners.pop(account_id, None)

            if connection:
                await connection.close()

            print(f"🗑️ Listener removed → {acc.metaapi_account_id}")

        except Exception as e:
            print(f"❌ Remove failed {acc.id}: {e}")


# Singleton
listener_manager = ListenerManager()