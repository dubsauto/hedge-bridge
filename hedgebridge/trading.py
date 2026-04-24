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
        api = await self._get_api()
        account = await api.metatrader_account_api.get_account(account_id)

        # Try existing connection first
        connection = self._connections.get(account_id)

        for attempt in range(3):
            try:
                # 🔥 If no connection OR disconnected → recreate
                if connection is None or account.connection_status != "CONNECTED":
                    print(f"[Reconnect] Account {account_id} (attempt {attempt+1})")

                    # Force reconnect via deploy (safe even if already deployed)
                    await account.deploy()

                    # Give time for MetaApi to reconnect
                    await asyncio.sleep(2)

                    connection = account.get_rpc_connection()

                    await connection.connect()
                    await connection.wait_synchronized()

                    # Update cache
                    self._connections[account_id] = connection

                return connection

            except Exception as e:
                print(f"[Retry {attempt+1}] Connection failed: {e}")

                # Reset connection so we recreate it next loop
                connection = None
                self._connections.pop(account_id, None)

                await asyncio.sleep(2)

        raise Exception(f"Failed to establish connection for {account_id}")
    
    async def get_price(self, account_id: str, symbol: str) -> Dict[str, float]:
        """
        Get current market price (bid/ask) for a symbol
        """
        try:
            connection = await self._get_connection(account_id)

            price = await connection.get_symbol_price(symbol)
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
            print(f"Modifying position {position_id} for account {account_id}")
            result = await connection.modify_position(position_id, stop_loss=sl, take_profit=tp)
            print(f"Modify result: {result}")
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