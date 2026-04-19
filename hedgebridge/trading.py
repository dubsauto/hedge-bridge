#hedgebridge/trading.py
from hedgebridge.api_client import get_metaapi_client
from typing import Optional, Dict, Any
from metaapi_cloud_sdk import MetaApi
import asyncio


class MT5Trader:
    """Handles trade execution (market + pending orders)"""

    def __init__(self):
        self._api: Optional[MetaApi] = None
        self._connections = {}

    async def _get_api(self) -> MetaApi:
        if self._api is None:
            self._api = get_metaapi_client()
        return self._api

    async def _get_connection(self, account_id: str):
        if account_id in self._connections:
            return self._connections[account_id]

        api = await self._get_api()
        account = await api.metatrader_account_api.get_account(account_id)

        # Only deploy if needed
        if account.state != 'DEPLOYED':
            print(f"Deploying account {account_id}...")
            await account.deploy()

        await account.wait_connected()

        connection = account.get_rpc_connection()
        await connection.connect()
        await connection.wait_synchronized()

        # ✅ CACHE IT
        self._connections[account_id] = connection

        return connection

    async def get_price(self, account_id: str, symbol: str) -> Dict[str, float]:
        """
        Get current market price (bid/ask) for a symbol
        """
        try:
            connection = await self._get_connection(account_id)

            price = await connection.get_symbol_price(symbol)

            # MetaApi returns:
            # {
            #   'symbol': 'EURUSD',
            #   'bid': 1.12345,
            #   'ask': 1.12357,
            #   ...
            # }

            if not price:
                raise Exception("No price data returned")

            return {
                "bid": price.get("bid"),
                "ask": price.get("ask")
            }

        except Exception as e:
            raise Exception(f"get_price failed: {str(e)}")
    # ========================
    # MARKET ORDERS
    # ========================

    async def buy(
        self,
        account_id: str,
        symbol: str,
        volume: float,
        sl: Optional[float] = None,
        tp: Optional[float] = None,
        comment: str = "",
        magic: int = 0
    ) -> Dict:

        try:
            connection = await self._get_connection(account_id)

            result = await connection.create_market_buy_order(
                symbol=symbol,
                volume=volume,
                stop_loss=sl,
                take_profit=tp,
                options={
                    "comment": comment,
                    "magic": magic
                }
            )

            return {"success": True, "result": result}

        except Exception as e:
            return {"success": False, "error": str(e)}

    async def sell(
        self,
        account_id: str,
        symbol: str,
        volume: float,
        sl: Optional[float] = None,
        tp: Optional[float] = None,
        comment: str = "",
        magic: int = 0
    ) -> Dict:

        try:
            connection = await self._get_connection(account_id)

            result = await connection.create_market_sell_order(
                symbol=symbol,
                volume=volume,
                stop_loss=sl,
                take_profit=tp,
                options={
                    "comment": comment,
                    "magic": magic
                }
            )

            return {"success": True, "result": result}

        except Exception as e:
            return {"success": False, "error": str(e)}
        
    async def close_position(self, account_id: str, position_id: str):
        try:
            connection = await self._get_connection(account_id)

            result = await connection.close_position(position_id)

            return {"success": True, "result": result}

        except Exception as e:
            return {"success": False, "error": str(e)}
        

    async def modify_position(self, account_id: str, position_id: str, sl: Optional[float] = None, tp: Optional[float] = None):
        try:
            connection = await self._get_connection(account_id)

            result = await connection.modify_position(position_id, stop_loss=sl, take_profit=tp)

            return {"success": True, "result": result}

        except Exception as e:
            return {"success": False, "error": str(e)}


# Singleton
trader = MT5Trader()




# ========================
    # PENDING ORDERS
    # ========================

    # async def buy_limit(
    #     self,
    #     account_id: str,
    #     symbol: str,
    #     volume: float,
    #     price: float,
    #     sl: Optional[float] = None,
    #     tp: Optional[float] = None,
    #     comment: str = "",
    #     magic: int = 0
    # ) -> Dict:

    #     try:
    #         connection = await self._get_connection(account_id)

    #         result = await connection.create_limit_buy_order(
    #             symbol=symbol,
    #             volume=volume,
    #             open_price=price,
    #             stop_loss=sl,
    #             take_profit=tp,
    #             options={
    #                 "comment": comment,
    #                 "magic": magic
    #             }
    #         )

    #         return {"success": True, "result": result}

    #     except Exception as e:
    #         return {"success": False, "error": str(e)}

    # async def sell_limit(
    #     self,
    #     account_id: str,
    #     symbol: str,
    #     volume: float,
    #     price: float,
    #     sl: Optional[float] = None,
    #     tp: Optional[float] = None,
    #     comment: str = "",
    #     magic: int = 0
    # ) -> Dict:

    #     try:
    #         connection = await self._get_connection(account_id)

    #         result = await connection.create_limit_sell_order(
    #             symbol=symbol,
    #             volume=volume,
    #             open_price=price,
    #             stop_loss=sl,
    #             take_profit=tp,
    #             options={
    #                 "comment": comment,
    #                 "magic": magic
    #             }
    #         )

    #         return {"success": True, "result": result}

    #     except Exception as e:
    #         return {"success": False, "error": str(e)}

    # async def buy_stop(
    #     self,
    #     account_id: str,
    #     symbol: str,
    #     volume: float,
    #     price: float,
    #     sl: Optional[float] = None,
    #     tp: Optional[float] = None,
    #     comment: str = "",
    #     magic: int = 0
    # ) -> Dict:

    #     try:
    #         connection = await self._get_connection(account_id)

    #         result = await connection.create_stop_buy_order(
    #             symbol=symbol,
    #             volume=volume,
    #             open_price=price,
    #             stop_loss=sl,
    #             take_profit=tp,
    #             options={
    #                 "comment": comment,
    #                 "magic": magic
    #             }
    #         )

    #         return {"success": True, "result": result}

    #     except Exception as e:
    #         return {"success": False, "error": str(e)}

    # async def sell_stop(
    #     self,
    #     account_id: str,
    #     symbol: str,
    #     volume: float,
    #     price: float,
    #     sl: Optional[float] = None,
    #     tp: Optional[float] = None,
    #     comment: str = "",
    #     magic: int = 0
    # ) -> Dict:

    #     try:
    #         connection = await self._get_connection(account_id)

    #         result = await connection.create_stop_sell_order(
    #             symbol=symbol,
    #             volume=volume,
    #             open_price=price,
    #             stop_loss=sl,
    #             take_profit=tp,
    #             options={
    #                 "comment": comment,
    #                 "magic": magic
    #             }
    #         )

    #         return {"success": True, "result": result}

    #     except Exception as e:
    #         return {"success": False, "error": str(e)}