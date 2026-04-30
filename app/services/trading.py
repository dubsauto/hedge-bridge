# app/services/trading.py

import asyncio
from typing import Optional, Dict
from hedgebridge.rpc_pool import rpc_pool


class MT5Trader:
    """Handles trade execution — uses shared RPC pool."""

    # =========================
    # GET PRICE
    # =========================
    async def get_price(self, account_id: str, symbol: str) -> Dict:
        try:
            connection = await rpc_pool.get_connection(account_id)
            price = await connection.get_symbol_price(symbol)
            if not price:
                raise Exception("No price data returned")
            return {"bid": price.get("bid"), "ask": price.get("ask")}
        except Exception as e:
            raise Exception(f"get_price failed: {str(e)}")

    # =========================
    # BUY
    # =========================
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
            connection = await rpc_pool.get_connection(account_id)
            result = await connection.create_market_buy_order(
                symbol=symbol,
                volume=volume,
                stop_loss=sl,
                take_profit=tp,
                options={"comment": comment, "magic": magic}
            )
            return {"success": True, "result": result}
        except Exception as e:
            return {"success": False, "error": str(e)}

    # =========================
    # SELL
    # =========================
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
            connection = await rpc_pool.get_connection(account_id)
            result = await connection.create_market_sell_order(
                symbol=symbol,
                volume=volume,
                stop_loss=sl,
                take_profit=tp,
                options={"comment": comment, "magic": magic}
            )
            return {"success": True, "result": result}
        except Exception as e:
            return {"success": False, "error": str(e)}

    # =========================
    # CLOSE POSITION
    # =========================
    async def close_position(self, account_id: str, position_id: str) -> Dict:
        try:
            connection = await rpc_pool.get_connection(account_id)
            result = await connection.close_position(position_id)
            return {"success": True, "result": result}
        except Exception as e:
            return {"success": False, "error": str(e)}

    # =========================
    # MODIFY POSITION
    # =========================
    async def modify_position(
        self,
        account_id: str,
        position_id: str,
        sl: Optional[float] = None,
        tp: Optional[float] = None
    ) -> Dict:
        try:
            connection = await rpc_pool.get_connection(account_id)
            result = await connection.modify_position(position_id, stop_loss=sl, take_profit=tp)
            return {"success": True, "result": result}
        except Exception as e:
            return {"success": False, "error": str(e)}


trader = MT5Trader()