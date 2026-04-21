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
                await self._sync()   # ❗ no global lock here
                await asyncio.sleep(2)  # faster recovery
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

        # 🔍 Health check AFTER DB work
        for account_id, connection in list(self._connections.items()):
            try:
                status = getattr(connection.health_monitor, "health_status", None)

                if not status or not status.get("connected", False):
                    print(f"💀 Dead connection detected → {account_id}")
                    await self.mark_disconnected(account_id)

            except Exception:
                await self.mark_disconnected(account_id)

    async def _ensure_listener(self, acc: TradingAccount):
        account_id = acc.metaapi_account_id

        # 🔒 prevent race condition
        async with self._lock:
            if account_id in self._connections:
                return

        try:
            print(f"🔌 Attaching listener {account_id}")

            api = get_metaapi_client()
            account = await api.metatrader_account_api.get_account(account_id)

            # Deploy if needed
            if account.state != 'DEPLOYED':
                print(f"🚀 Deploying {account_id}")
                await account.deploy()

            # ⏳ Wait for broker connection
            timeout = 60
            for _ in range(timeout):
                account = await api.metatrader_account_api.get_account(account_id)

                if account.connection_status == "CONNECTED":
                    break

                print(f"⏳ Waiting broker connection {account_id}...")
                await asyncio.sleep(1)
            else:
                print(f"❌ Broker not connected → {account_id}")
                return

            connection = account.get_streaming_connection()

            # 🔁 Retry connection
            for attempt in range(3):
                try:
                    await connection.connect()
                    await connection.wait_synchronized()
                    break
                except Exception as e:
                    print(f"⚠️ Retry {attempt+1} failed → {account_id}: {e}")
                    await asyncio.sleep(2)
            else:
                print(f"❌ Failed to connect after retries → {account_id}")
                return

            # 🔒 double-check after async ops
            async with self._lock:
                if account_id in self._connections:
                    return

                listener = MetaApiTradeListener(acc.id, manager=self)

                connection.add_synchronization_listener(listener)

                self._connections[account_id] = connection
                self._listeners[account_id] = listener

            print(f"👂 Listener attached → {account_id}")

        except Exception as e:
            print(f"❌ Attach failed {acc.id}: {e}")

    async def _remove_listener(self, acc: TradingAccount):
        account_id = acc.metaapi_account_id

        async with self._lock:
            connection = self._connections.pop(account_id, None)
            listener = self._listeners.pop(account_id, None)

        if not connection:
            return

        try:
            print(f"🛑 Removing listener {account_id}")

            if listener:
                try:
                    connection.remove_synchronization_listener(listener)
                except Exception:
                    pass

            await connection.close()

            print(f"🗑️ Listener removed → {account_id}")

        except Exception as e:
            print(f"❌ Remove failed {account_id}: {e}")

    async def mark_disconnected(self, account_id: str):
        async with self._lock:
            connection = self._connections.pop(account_id, None)
            listener = self._listeners.pop(account_id, None)

        if connection:
            try:
                if listener:
                    try:
                        connection.remove_synchronization_listener(listener)
                    except Exception:
                        pass

                await connection.close()
            except Exception:
                pass

        print(f"♻️ Marked for reconnection → {account_id}")


# Singleton
listener_manager = ListenerManager()



# # hedgebridge/listener_manager.py

# import asyncio
# from sqlalchemy.orm import Session
# from app.database import SessionLocal
# from app.model import TradingAccount, CopyRelationship
# from hedgebridge.listener import MetaApiTradeListener
# from hedgebridge.api_client import get_metaapi_client


# class ListenerManager:

#     def __init__(self):
#         self._lock = asyncio.Lock()
#         self._running = False
#         self._connections = {}   # account_id -> connection
#         self._listeners = {}     # account_id -> listener

#     async def start(self):
#         if self._running:
#             return

#         self._running = True
#         print("🚀 Listener Manager started")

#         while True:
#             try:
#                 async with self._lock:
#                     await self._sync()
#                 await asyncio.sleep(5)
#             except Exception as e:
#                 print(f"❌ Manager error: {e}")
#                 await asyncio.sleep(3)

#     async def _sync(self):
#         db: Session = SessionLocal()

#         try:
#             accounts = db.query(TradingAccount).all()

#             for acc in accounts:

#                 # Only deployed accounts
#                 if acc.state != "deployed":
#                     await self._remove_listener(acc)
#                     continue

#                 # Must be part of copy trading
#                 is_used = db.query(CopyRelationship).filter(
#                     (CopyRelationship.master_account_id == acc.id) |
#                     (CopyRelationship.slave_account_id == acc.id)
#                 ).first()

#                 if not is_used:
#                     await self._remove_listener(acc)
#                     continue

#                 await self._ensure_listener(acc)

#         finally:
#             db.close()

#         # Check existing connections health
#         for account_id, connection in list(self._connections.items()):
#             try:
#                 if not connection.health_monitor.health_status['connected']:
#                     print(f"💀 Dead connection detected → {account_id}")
#                     await self.mark_disconnected(account_id)
#             except Exception:
#                 await self.mark_disconnected(account_id)


#     async def _ensure_listener(self, acc: TradingAccount):
#         account_id = acc.metaapi_account_id

#         if account_id in self._connections:
#             return

#         try:
#             print(f"🔌 Attaching listener {acc.metaapi_account_id}")

#             api = get_metaapi_client()
#             account = await api.metatrader_account_api.get_account(account_id)

#             # Deploy if needed
#             if account.state != 'DEPLOYED':
#                 print(f"🚀 Deploying {acc.metaapi_account_id}")
#                 await account.deploy()

#             # 🔥 WAIT UNTIL BROKER CONNECTS (IMPORTANT)
#             timeout = 60
#             for _ in range(timeout):
#                 account = await api.metatrader_account_api.get_account(account_id)

#                 if account.connection_status == "CONNECTED":
#                     break

#                 print(f"⏳ Waiting broker connection {acc.metaapi_account_id}...")
#                 await asyncio.sleep(1)
#             else:
#                 print(f"❌ Broker not connected for {acc.metaapi_account_id} after {timeout} seconds")
#                 return

#             # Now safe to connect streaming
#             connection = account.get_streaming_connection()

#             await connection.connect()
#             await connection.wait_synchronized()

#             if account_id in self._connections:
#                 return

#             listener = MetaApiTradeListener(acc.id, manager=self)

#             connection.add_synchronization_listener(listener)
            

#             self._connections[account_id] = connection
#             self._listeners[account_id] = listener

#             print(f"👂 Listener attached → {acc.metaapi_account_id}")

#         except Exception as e:
#             print(f"❌ Attach failed {acc.id}: {e}")

#     async def _remove_listener(self, acc: TradingAccount):
#         account_id = acc.metaapi_account_id

#         if account_id not in self._connections:
#             return

#         try:
#             print(f"🛑 Removing listener {acc.id}")
#             connection = self._connections[account_id]
#             listener = self._listeners[account_id]

#             connection.remove_synchronization_listener(listener)

#             self._connections.pop(account_id, None)
#             self._listeners.pop(account_id, None)

#             if connection:
#                 await connection.close()

#             print(f"🗑️ Listener removed → {acc.metaapi_account_id}")

#         except Exception as e:
#             print(f"❌ Remove failed {acc.id}: {e}")

#     async def mark_disconnected(self, account_id: str):
#         async with self._lock:
#             connection = self._connections.pop(account_id, None)
#             listener = self._listeners.pop(account_id, None)

#             if connection:
#                 try:
#                     await connection.close()
#                 except:
#                     pass

#             print(f"♻️ Marked for reconnection → {account_id}")


# # Singleton
# listener_manager = ListenerManager()