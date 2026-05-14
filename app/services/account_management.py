# app/services/account_management.py

import asyncio
import time
from typing import Optional, Dict, Any
from hedgebridge.rpc_pool import rpc_pool


class MT5AccountManager:
    def __init__(self):
        self._metrics_cache: Dict[str, Dict] = {}
        self._semaphore = asyncio.Semaphore(5)

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
        try:
            api = await rpc_pool._get_api()
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

            api = await rpc_pool._get_api()
            new_account = await api.metatrader_account_api.create_account(account_data)

            # Pre-cache the account object in the pool
            rpc_pool._accounts[new_account.id] = new_account

            return {"success": True, "account_id": new_account.id}

        except Exception as e:
            return {"success": False, "message": str(e)}

    # =========================
    # REMOVE ACCOUNT
    # =========================
    async def remove_account(self, account_id: str) -> Dict:
        try:
            account = await rpc_pool.get_account(account_id)
            await account.remove()
            await rpc_pool.invalidate(account_id)
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
            account = await rpc_pool.get_account(account_id)
            await account.update(update_data)
            return {"success": True}
        except Exception as e:
            return {"success": False, "message": str(e)}

    # =========================
    # DEPLOY
    # =========================
    async def deploy(self, account_id: str) -> Dict:
        try:
            account = await rpc_pool.get_account(account_id)
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
            account = await rpc_pool.get_account(account_id)
            if account.state != "UNDEPLOYED":
                await account.undeploy()
            return {"success": True}
        except Exception as e:
            return {"success": False, "message": str(e)}

    # =========================
    # METRICS
    # =========================
    async def get_account_metrics(self, account_id: str):
        async with self._semaphore:
            now = time.time()

            cached = self._metrics_cache.get(account_id)
            if cached and now - cached["ts"] < 30:
                return cached["data"]

            try:
                account = await rpc_pool.get_account(account_id)

                dedicated_ip = None
                try:
                    dedicated_ip = getattr(account, "allocate_dedicated_ip", None)
                except Exception:
                    pass

                if account.state != "DEPLOYED":
                    return {}

                # get_connection() returns immediately (connection ready or error)
                # — the build happens in a background task, never blocking here.
                connection = await rpc_pool.get_connection(account_id)

                start = time.perf_counter()

                info, positions = await asyncio.gather(
                    asyncio.wait_for(connection.get_account_information(), timeout=5),
                    asyncio.wait_for(connection.get_positions(), timeout=5)
                )

                latency_ms = (time.perf_counter() - start) * 1000

                result = {
                    "balance": info.get("balance"),
                    "equity": info.get("equity"),
                    "latency_ms": round(latency_ms, 2),
                    "positions_count": len(positions),
                    "dedicated_ip": dedicated_ip
                }

                self._metrics_cache[account_id] = {"ts": now, "data": result}
                return result

            except asyncio.TimeoutError:
                print(f"[Timeout] {account_id}")
                return {}
            except Exception as e:
                print(f"[Error] {account_id}: {e}")
                return {}


account_manager = MT5AccountManager()