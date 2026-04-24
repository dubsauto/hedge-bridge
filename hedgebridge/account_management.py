# hedgebridge/account_management.py

from hedgebridge.api_client import get_metaapi_client
from typing import Optional, Dict, Any
from metaapi_cloud_sdk import MetaApi
import time
import asyncio


class MT5AccountManager:
    def __init__(self):
        self._api: Optional[MetaApi] = None

        # CACHES
        self._accounts_cache: Dict[str, Any] = {}
        self._connections_cache: Dict[str, Any] = {}
        self._metrics_cache: Dict[str, Dict] = {}

        # LIMIT CONCURRENT REQUESTS TO METAAPI
        self._semaphore = asyncio.Semaphore(5)

    # =========================
    # GET API
    # =========================
    async def _get_api(self) -> MetaApi:
        if self._api is None:
            self._api = get_metaapi_client()
        return self._api

    # =========================
    # GET ACCOUNT (CACHED)
    # =========================
    async def _get_account(self, account_id: str):
        if account_id in self._accounts_cache:
            return self._accounts_cache[account_id]

        api = await self._get_api()
        account = await api.metatrader_account_api.get_account(account_id)

        self._accounts_cache[account_id] = account
        return account

    # =========================
    # GET CONNECTION (REUSED 🔥)
    # =========================
    async def _get_connection(self, account):
        acc_id = account.id

        connection = self._connections_cache.get(acc_id)

        try:
            # ✅ If cached connection exists, check if it's usable
            if connection:
                # Try a lightweight call to verify it's alive
                await asyncio.wait_for(connection.get_account_information(), timeout=3)
                return connection

        except Exception:
            # ❌ Connection is bad → drop it
            print(f"[Reconnect] Dropping stale connection {acc_id}")
            self._connections_cache.pop(acc_id, None)
            connection = None

        # ✅ Create fresh connection
        connection = account.get_rpc_connection()

        await connection.connect()
        await connection.wait_synchronized()

        self._connections_cache[acc_id] = connection
        return connection

    # =========================
    # ADD ACCOUNT
    # =========================
    async def add_account(
        self,
        name: str,
        server: str,
        login: str,
        password: str,
        manual_trades: bool = True,
        use_dedicated_ip: bool = True,
        magic: Optional[int] = None
    ) -> Dict:

        api = await self._get_api()

        try:
            accounts = await api.metatrader_account_api.get_accounts_with_infinite_scroll_pagination()

            for acc in accounts:
                if str(acc.login) == str(login) and acc.type.startswith('cloud'):
                    return {"success": True, "account_id": acc.id}

            account_data = {
                'name': name,
                'type': 'cloud',
                'login': login,
                'password': password,
                'server': server,
                'platform': 'mt5',
                'manualTrades': manual_trades,
                'allocateDedicatedIp': 'ipv4' if use_dedicated_ip else None,
                'magic': 0 if manual_trades else (magic or 0)
            }

            new_account = await api.metatrader_account_api.create_account(account_data)

            self._accounts_cache[new_account.id] = new_account

            return {"success": True, "account_id": new_account.id}

        except Exception as e:
            return {"success": False, "message": str(e)}

    # =========================
    # REMOVE ACCOUNT
    # =========================
    async def remove_account(self, account_id: str) -> Dict:
        try:
            account = await self._get_account(account_id)
            await account.remove()

            self._accounts_cache.pop(account_id, None)
            self._connections_cache.pop(account_id, None)
            self._metrics_cache.pop(account_id, None)

            return {"success": True}

        except Exception as e:
            if "not found" in str(e).lower():
                return {"success": True}
            return {"success": False, "message": str(e)}

    # =========================
    # UPDATE ACCOUNT
    # =========================
    async def update_account(self, account_id: str, update_data: Dict) -> Dict:
        try:
            account = await self._get_account(account_id)
            await account.update(update_data)
            return {"success": True}
        except Exception as e:
            return {"success": False, "message": str(e)}

    # =========================
    # DEPLOY (MANUAL ONLY)
    # =========================
    async def deploy(self, account_id: str) -> Dict:
        try:
            account = await self._get_account(account_id)

            if account.state != "DEPLOYED":
                await account.deploy()

            return {"success": True}

        except Exception as e:
            return {"success": False, "message": str(e)}

    # =========================
    # UNDEPLOY
    # =========================
    async def undeploy(self, account_id: str) -> Dict:
        try:
            account = await self._get_account(account_id)

            if account.state != "UNDEPLOYED":
                await account.undeploy()

            return {"success": True}

        except Exception as e:
            return {"success": False, "message": str(e)}

    # =========================
    # METRICS (OPTIMIZED 🔥)
    # =========================
    async def get_account_metrics(self, account_id: str):
        async with self._semaphore:

            now = time.time()

            # =========================
            # CACHE HIT (5s TTL)
            # =========================
            cached = self._metrics_cache.get(account_id)
            if cached and now - cached["ts"] < 5:
                return cached["data"]

            try:
                account = await self._get_account(account_id)

                # 🚫 Skip bad states (NO auto deploy)
                if account.state != "DEPLOYED":
                    return {}

                connection = await self._get_connection(account)

                start = time.perf_counter()

                # =========================
                # PARALLEL FETCH 🔥
                # =========================
                info_task = connection.get_account_information()
                positions_task = connection.get_positions()

                info, positions = await asyncio.gather(
                    asyncio.wait_for(info_task, timeout=5),
                    asyncio.wait_for(positions_task, timeout=5)
                )

                latency_ms = (time.perf_counter() - start) * 1000

                # =========================
                # DEDICATED IP
                # =========================
                dedicated_ip = None
                try:
                    if account.connections:
                        dedicated_ip = account.connections[0].get("ip")
                except Exception:
                    pass

                result = {
                    "balance": info.get("balance"),
                    "equity": info.get("equity"),
                    "latency_ms": round(latency_ms, 2),

                    # NEW
                    "positions_count": len(positions),
                    "dedicated_ip": dedicated_ip
                }

                # =========================
                # CACHE STORE
                # =========================
                self._metrics_cache[account_id] = {
                    "ts": now,
                    "data": result
                }

                return result

            except asyncio.TimeoutError:
                print(f"[Timeout] {account_id}")
                return {}

            except Exception as e:
                print(f"[Error] {account_id}: {e}")
                return {}


# SINGLETON INSTANCE
account_manager = MT5AccountManager()





# # hedgebridge/account_management.py

# from hedgebridge.api_client import get_metaapi_client
# from typing import Optional, Dict
# from metaapi_cloud_sdk import MetaApi
# import time
# import asyncio


# class MT5AccountManager:
#     """Manages MT5 accounts in MetaApi Cloud (Add, Update, Remove, Deploy, Undeploy)"""

#     def __init__(self):
#         self._api: Optional[MetaApi] = None
#         self._accounts_cache: Dict[str, any] = {}  # cache MetaAPI account objects

#     # =========================
#     # GET API (SINGLETON SAFE)
#     # =========================
#     async def _get_api(self) -> MetaApi:
#         if self._api is None:
#             self._api = get_metaapi_client()
#         return self._api

#     # =========================
#     # GET ACCOUNT (FIXED ✅)
#     # =========================
#     async def _get_account(self, account_id: str):
#         if account_id in self._accounts_cache:
#             return self._accounts_cache[account_id]

#         api = await self._get_api()

#         # ✅ CORRECT CALL (no recursion)
#         account = await api.metatrader_account_api.get_account(account_id)

#         self._accounts_cache[account_id] = account
#         return account

#     # =========================
#     # ADD ACCOUNT
#     # =========================
#     async def add_account(
#         self,
#         name: str,
#         server: str,
#         login: str,
#         password: str,
#         manual_trades: bool = True,
#         use_dedicated_ip: bool = True,
#         magic: Optional[int] = None
#     ) -> Dict:

#         api = await self._get_api()

#         try:
#             accounts = await api.metatrader_account_api.get_accounts_with_infinite_scroll_pagination()

#             for acc in accounts:
#                 if str(acc.login) == str(login) and acc.type.startswith('cloud'):
#                     print(f"✅ MT5 account {login} already exists.")
#                     return {
#                         "success": True,
#                         "account_id": acc.id
#                     }

#             print(f"Creating MT5 account: {name} ({login})")

#             account_data = {
#                 'name': name,
#                 'type': 'cloud',
#                 'login': login,
#                 'password': password,
#                 'server': server,
#                 'platform': 'mt5',
#                 'manualTrades': manual_trades,
#                 'allocateDedicatedIp': 'ipv4' if use_dedicated_ip else None,
#                 'magic': 0 if manual_trades else (magic or 0)
#             }

#             new_account = await api.metatrader_account_api.create_account(account_data)

#             # Cache it
#             self._accounts_cache[new_account.id] = new_account

#             # Optional: keep undeployed
#             try:
#                 await new_account.undeploy()
#             except Exception:
#                 pass

#             return {
#                 "success": True,
#                 "account_id": new_account.id
#             }

#         except Exception as e:
#             return {"success": False, "message": str(e)}

#     # =========================
#     # REMOVE ACCOUNT
#     # =========================
#     async def remove_account(self, account_id: str) -> Dict:
#         try:
#             account = await self._get_account(account_id)
#             await account.remove()

#             self._accounts_cache.pop(account_id, None)

#             return {"success": True}

#         except Exception as e:
#             msg = str(e).lower()

#             if "not found" in msg:
#                 return {"success": True}

#             return {"success": False, "message": str(e)}

#     # =========================
#     # UPDATE ACCOUNT
#     # =========================
#     async def update_account(self, account_id: str, update_data: Dict) -> Dict:
#         try:
#             account = await self._get_account(account_id)
#             await account.update(update_data)

#             return {"success": True}

#         except Exception as e:
#             return {"success": False, "message": str(e)}

#     # =========================
#     # DEPLOY
#     # =========================
#     async def deploy(self, account_id: str) -> Dict:
#         try:
#             account = await self._get_account(account_id)

#             if account.state != "DEPLOYED":
#                 await account.deploy()

#             return {"success": True}

#         except Exception as e:
#             return {"success": False, "message": str(e)}

#     # =========================
#     # UNDEPLOY (FIXED 🔥)
#     # =========================
#     async def undeploy(self, account_id: str) -> Dict:
#         try:
#             account = await self._get_account(account_id)

#             if account.state != "UNDEPLOYED":
#                 await account.undeploy()

#             return {"success": True}

#         except Exception as e:
#             return {"success": False, "message": str(e)}

#     async def get_account_metrics(self, account_id: str):
#         print(f"Fetching metrics for account {account_id}")
#         account = await self._get_account(account_id)

#         for attempt in range(3):
#             try:
#                 print(f"account.state: {account.state}")
#                 print(f"account.connection_status: {account.connection_status}")

#                 # ✅ Ensure deployed
#                 if account.state != "DEPLOYED":
#                     print(f"[Deploy] Account {account_id}")
#                     await account.deploy()

#                 # ✅ Wait for connection
#                 if account.connection_status != "CONNECTED":
#                     print(f"[Waiting for connection] Account {account_id} (attempt {attempt+1})")

#                     for _ in range(15):
#                         await account.reload()

#                         if account.connection_status == "CONNECTED":
#                             print(f"[Connected] Account {account_id}")
#                             break

#                         await asyncio.sleep(1)
#                     else:
#                         raise Exception("Connection timeout")

#                 connection = account.get_rpc_connection()

#                 start = time.perf_counter()

#                 # ✅ Always safe to call connect (MetaApi handles internally)
#                 await asyncio.wait_for(connection.connect(), timeout=10)

#                 info = await asyncio.wait_for(
#                     connection.get_account_information(),
#                     timeout=10
#                 )

#                 latency_ms = (time.perf_counter() - start) * 1000
#                 #await connection.close()
#                 return {
#                     "balance": info.get("balance"),
#                     "equity": info.get("equity"),
#                     "latency_ms": round(latency_ms, 2)
#                 }

#             except asyncio.CancelledError:
#                 print(f"[Cancelled] Connection interrupted for {account_id}")
#                 return {}

#             except Exception as e:
#                 print(f"[Retry {attempt+1}] Metrics failed: {e}")
#                 await asyncio.sleep(2)

#         print(f"[FAILED] Could not fetch metrics for {account_id}")
#         return {}
    
    
#     # async def get_account_metrics(self, account_id: str):
#     #     account = await self._get_account(account_id)

#     #     for attempt in range(3):
#     #         try:
#     #             # 🔥 Reconnect only if disconnected
#     #             print(f"account.connection_status: {account.connection_status}")
#     #             if account.connection_status != "CONNECTED":
#     #                 print(f"[Reconnect] Account {account_id} (attempt {attempt+1})")
#     #                 for _ in range(10):
#     #                     await account.reload()
#     #                     if account.connection_status == "CONNECTED":
#     #                         break
#     #                     await asyncio.sleep(1)
#     #             connection = account.get_rpc_connection()

#     #             start = time.perf_counter()

#     #             # ⏱️ Add timeout protection
#     #             await asyncio.wait_for(connection.connect(), timeout=10)

#     #             info = await asyncio.wait_for(
#     #                 connection.get_account_information(),
#     #                 timeout=10
#     #             )

#     #             latency_ms = (time.perf_counter() - start) * 1000

#     #             return {
#     #                 "balance": info.get("balance"),
#     #                 "equity": info.get("equity"),
#     #                 "latency_ms": round(latency_ms, 2)
#     #             }

#     #         except Exception as e:
#     #             print(f"[Retry {attempt+1}] Metrics failed: {e}")
#     #             await asyncio.sleep(2)

#     #     raise Exception(f"Failed to fetch account metrics for {account_id}")
        
#     # async def get_account_metrics(self, account_id: str):
#     #     account = await self._get_account(account_id)

#     #     connection = account.get_rpc_connection()

#     #     # ⏱️ Measure latency (round-trip)
#     #     start = time.perf_counter()
#     #     await connection.connect()
#     #     info = await connection.get_account_information()

#     #     latency_ms = (time.perf_counter() - start) * 1000

#     #     return {
#     #         "balance": info.get("balance"),
#     #         "equity": info.get("equity"),
#     #         "latency_ms": round(latency_ms, 2)
#     #     }


# # Singleton
# account_manager = MT5AccountManager()