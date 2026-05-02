#hedgebridge/tradingListener.py
from hedgebridge.api_client import get_metaapi_client
from typing import Optional, Dict, Any
from metaapi_cloud_sdk import MetaApi
import asyncio
from hedgebridge.connection_store import get_connection


class MT5TraderListener:
    """Handles trade execution (market + pending orders)"""

    def __init__(self):
        self._api: Optional[MetaApi] = None
        self._connections = {}

    async def _get_api(self) -> MetaApi:
        if self._api is None:
            self._api = get_metaapi_client()
        return self._api

    async def _get_connection(self, account_id: str):
        connection = get_connection(account_id)

        if not connection:
            raise Exception(f"No active streaming connection for {account_id}")

        try:
            status = connection.health_monitor.health_status

            if not status or not status.get("connected", False):
                raise Exception("Connection not healthy")

        except Exception:
            raise Exception(f"Connection unhealthy for {account_id}")

        return connection
    
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
trader_listener = MT5TraderListener()

