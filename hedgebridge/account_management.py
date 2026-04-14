# hedgebridge/account_management.py

from hedgebridge.api_client import get_metaapi_client
from typing import Optional, Dict
from metaapi_cloud_sdk import MetaApi


class MT5AccountManager:
    """Manages MT5 accounts in MetaApi Cloud (Add, Update, Remove, Deploy, Undeploy)"""

    def __init__(self):
        self._api: Optional[MetaApi] = None
        self._accounts_cache: Dict[str, any] = {}  # cache MetaAPI account objects

    # =========================
    # GET API (SINGLETON SAFE)
    # =========================
    async def _get_api(self) -> MetaApi:
        if self._api is None:
            self._api = get_metaapi_client()
        return self._api

    # =========================
    # GET ACCOUNT (FIXED ✅)
    # =========================
    async def _get_account(self, account_id: str):
        if account_id in self._accounts_cache:
            return self._accounts_cache[account_id]

        api = await self._get_api()

        # ✅ CORRECT CALL (no recursion)
        account = await api.metatrader_account_api.get_account(account_id)

        self._accounts_cache[account_id] = account
        return account

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
                    print(f"✅ MT5 account {login} already exists.")
                    return {
                        "success": True,
                        "account_id": acc.id
                    }

            print(f"Creating MT5 account: {name} ({login})")

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

            # Cache it
            self._accounts_cache[new_account.id] = new_account

            # Optional: keep undeployed
            try:
                await new_account.undeploy()
            except Exception:
                pass

            return {
                "success": True,
                "account_id": new_account.id
            }

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

            return {"success": True}

        except Exception as e:
            msg = str(e).lower()

            if "not found" in msg:
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
    # DEPLOY
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
    # UNDEPLOY (FIXED 🔥)
    # =========================
    async def undeploy(self, account_id: str) -> Dict:
        try:
            account = await self._get_account(account_id)

            if account.state != "UNDEPLOYED":
                await account.undeploy()

            return {"success": True}

        except Exception as e:
            return {"success": False, "message": str(e)}


# Singleton
account_manager = MT5AccountManager()