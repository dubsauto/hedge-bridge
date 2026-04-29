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

        # account_id -> streaming connection
        self._connections = {}

        # account_id -> listener instance
        self._listeners = {}

        # singleton MetaApi client cache
        self._api = None

    # =====================================
    # GET METAAPI SINGLETON
    # =====================================
    async def _get_api(self):
        if self._api is None:
            self._api = get_metaapi_client()
        return self._api

    # =====================================
    # START MANAGER
    # =====================================
    async def start(self):
        if self._running:
            return

        # initialize MetaApi once at startup
        self._api = get_metaapi_client()

        self._running = True
        print("🚀 Listener Manager started")

        while True:
            try:
                # no global lock here
                await self._sync()

                # small delay for recovery / refresh
                await asyncio.sleep(5)

            except Exception as e:
                print(f"❌ Manager error: {e}")
                await asyncio.sleep(3)

    # =====================================
    # DB SYNC + HEALTH CHECK
    # =====================================
    async def _sync(self):
        db: Session = SessionLocal()

        try:
            accounts = db.query(TradingAccount).all()

            for acc in accounts:
                # skip non-deployed accounts
                if not acc.state or acc.state.upper() != "DEPLOYED":
                    await self._remove_listener(acc)
                    continue

                # only keep listeners for accounts
                # actively used in copy relationships
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

        # =====================================
        # HEALTH CHECK AFTER DB WORK
        # use list() so dict can mutate safely
        # =====================================
        for account_id, connection in list(self._connections.items()):
            try:
                status = getattr(
                    connection.health_monitor,
                    "health_status",
                    None
                )

                if not status or not status.get("connected", False):
                    print(f"💀 Dead connection detected → {account_id}")
                    await self.mark_disconnected(account_id)

            except Exception:
                await self.mark_disconnected(account_id)

    # =====================================
    # ENSURE LISTENER EXISTS
    # =====================================
    async def _ensure_listener(self, acc: TradingAccount):
        account_id = acc.metaapi_account_id

        # prevent race condition
        async with self._lock:
            if account_id in self._connections:
                return

        connection = None

        try:
            print(f"🔌 Attaching listener → {account_id}")

            api = await self._get_api()

            # =====================================
            # FIX 1:
            # Fetch account ONLY ONCE
            # =====================================
            account = await api.metatrader_account_api.get_account(
                account_id
            )

            # deploy if needed
            if account.state != "DEPLOYED":
                print(f"🚀 Deploying → {account_id}")
                await account.deploy()

            # =====================================
            # FIX 2:
            # Reuse account object
            # Don't re-fetch every second
            # =====================================
            timeout = 60

            for _ in range(timeout):
                if account.connection_status == "CONNECTED":
                    break

                print(f"⏳ Waiting broker connection → {account_id}")
                await asyncio.sleep(1)

                try:
                    # lightweight refresh if supported
                    await account.reload()
                except Exception:
                    pass

            else:
                print(f"❌ Broker not connected → {account_id}")
                return

            # create streaming connection
            connection = account.get_streaming_connection()

            # =====================================
            # FIX 3:
            # Only ONE attempt
            # =====================================
            try:
                await connection.connect()
                await connection.wait_synchronized()

            except Exception as e:
                print(f"❌ Failed to connect → {account_id}: {e}")

                # =====================================
                # FIX 4:
                # ALWAYS close failed connections
                # =====================================
                try:
                    await connection.close()
                except Exception:
                    pass

                return

            # double-check after async operations
            async with self._lock:
                if account_id in self._connections:
                    try:
                        await connection.close()
                    except Exception:
                        pass
                    return

                listener = MetaApiTradeListener(
                    acc.id,
                    manager=self
                )

                connection.add_synchronization_listener(listener)

                self._connections[account_id] = connection
                self._listeners[account_id] = listener

            print(f"👂 Listener attached → {account_id}")

        except Exception as e:
            print(f"❌ Attach failed {acc.id}: {e}")

            # extra safety:
            # close connection on unexpected failure
            if connection:
                try:
                    await connection.close()
                except Exception:
                    pass

    # =====================================
    # REMOVE LISTENER
    # =====================================
    async def _remove_listener(self, acc: TradingAccount):
        account_id = acc.metaapi_account_id

        async with self._lock:
            connection = self._connections.pop(account_id, None)
            listener = self._listeners.pop(account_id, None)

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

            # clear listener caches to free memory
            if listener:
                try:
                    listener._known_positions.clear()
                    listener._position_cache.clear()
                except Exception:
                    pass

            print(f"🗑️ Listener removed → {account_id}")

        except Exception as e:
            print(f"❌ Remove failed {account_id}: {e}")

    # =====================================
    # MARK DISCONNECTED
    # =====================================
    async def mark_disconnected(self, account_id: str):
        """
        Remove dead connection and clear caches fully.
        This allows clean re-attachment on next sync.
        """

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

        # clear listener caches to free memory
        if listener:
            try:
                listener._known_positions.clear()
                listener._position_cache.clear()
            except Exception:
                pass

        print(f"♻️ Marked for reconnection → {account_id}")


# =====================================
# SINGLETON
# =====================================

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