# hedgebridge/copy_engine.py

from sqlalchemy.orm import Session
from app.model import CopyRelationship, CopyTradeLink, TradingAccount
from hedgebridge.trading import trader
from app.services.logger import log

class CopyEngine:

    def __init__(self):
        self._processing = set()  # prevent loops / duplicates

    # =========================
    # NEW TRADE (MASTER)
    # =========================
    async def handle_new_trade(self, db: Session, master_account_id: int, position: dict):
        try:
            master_ticket = str(position.get("id") or position.get("ticket"))
            symbol = position.get("symbol")
            volume = position.get("volume")
            trade_type = position.get("type")
            sl = position.get("stopLoss")
            tp = position.get("takeProfit")

            key = f"open:{master_ticket}"
            if key in self._processing:
                return
            self._processing.add(key)

            # log(db, master_account_id, "TRADE",
            #     f"Master opened {symbol} {trade_type} {volume}",
            #     category="COPY",
            #     raw_json=position)
            log(db=db,
                account_id=master_account_id,
                level="TRADE",
                category="COPY",
                message=f"Master opened {symbol} {trade_type} {volume}",
                raw_json=position)

            # =========================
            # FIND SLAVES
            # =========================
            relationships = db.query(CopyRelationship).filter(
                CopyRelationship.master_account_id == master_account_id,
                CopyRelationship.slave_account_id.isnot(None),
                CopyRelationship.is_active == True
            ).all()

            for rel in relationships:
                slave_id = rel.slave_account_id

                slave_acc = db.query(TradingAccount).filter(
                    TradingAccount.id == slave_id
                ).first()

                if not slave_acc:
                    continue

                # log(db, slave_id, "INFO",
                #     f"Copying trade {symbol} from master {master_ticket}",
                #     category="COPY")
                log(db=db,
                    account_id=slave_id,
                    level="INFO",
                    category="COPY",
                    message=f"Copying trade {symbol} from master {master_ticket}")

                # =========================
                # DIRECTION
                # =========================
                final_type = trade_type
                if rel.copy_direction == "opposite":
                    final_type = "POSITION_TYPE_SELL" if trade_type == "POSITION_TYPE_BUY" else "POSITION_TYPE_BUY"

                # =========================
                # EXECUTE
                # =========================
                if final_type == "POSITION_TYPE_BUY":
                    result = await trader.buy(
                        slave_acc.metaapi_account_id,
                        symbol,
                        volume,
                        sl,
                        tp,
                        comment=f"copy:{master_ticket}",
                        magic=slave_acc.magic
                    )
                else:
                    result = await trader.sell(
                        slave_acc.metaapi_account_id,
                        symbol,
                        volume,
                        sl,
                        tp,
                        comment=f"copy:{master_ticket}",
                        magic=slave_acc.magic
                    )

                # =========================
                # RESULT
                # =========================
                if result.get("success"):
                    slave_ticket = str(result["result"]["orderId"])

                    link = CopyTradeLink(
                        master_account_id=master_account_id,
                        slave_account_id=slave_id,
                        master_ticket=master_ticket,
                        slave_ticket=slave_ticket,
                        symbol=symbol,
                        trade_type=final_type.lower(),
                        volume=volume,
                        status="open"
                    )
                    db.add(link)
                    db.commit()

                    # log(db, slave_id, "TRADE",
                    #     f"Copied trade {symbol} → ticket {slave_ticket}",
                    #     category="COPY",
                    #     raw_json=result.get("result"))
                    log(db=db,
                        account_id=slave_id,
                        level="TRADE",
                        category="COPY",
                        message=f"Copied trade {symbol} → ticket {slave_ticket}",
                        raw_json=result.get("result"))

                else:
                    # log(db, slave_id, "ERROR",
                    #     f"Copy failed: {result.get('error')}",
                    #     category="COPY")
                    log(db=db,
                        account_id=slave_id,
                        level="ERROR",
                        category="COPY",
                        message=f"Copy failed: {result.get('error')}",
                        raw_json=result)

        except Exception as e:
            # log(db, master_account_id, "ERROR",
            #     f"handle_new_trade error: {str(e)}",
            #     category="SYSTEM")
            log(db=db,
                account_id=master_account_id,
                level="ERROR",
                category="SYSTEM",
                message=f"handle_new_trade error: {str(e)}")
        finally:
            self._processing.discard(key)
    # =========================
    # CLOSE TRADE (MASTER)
    # =========================
    async def handle_close_trade(self, db: Session, master_account_id: int, master_ticket: str):
        try:
            key = f"close_master:{master_ticket}"
            if key in self._processing:
                return
            self._processing.add(key)

            log(db=db,
                account_id=master_account_id,
                level="TRADE",
                category="COPY",
                message=f"Master closing trade {master_ticket}")

            links = db.query(CopyTradeLink).filter(
                CopyTradeLink.master_ticket == master_ticket,
                CopyTradeLink.status == "open"
            ).all()

            for link in links:
                slave_acc = db.query(TradingAccount).filter(
                    TradingAccount.id == link.slave_account_id
                ).first()

                if not slave_acc:
                    continue

                try:
                    await trader.close_position(
                        slave_acc.metaapi_account_id,
                        link.slave_ticket
                    )

                    # log(db, link.slave_account_id, "TRADE",
                    #     f"Closed copied trade {link.slave_ticket}",
                    #     category="COPY")
                    
                    log(db=db,
                        account_id=link.slave_account_id,
                        level="TRADE",
                        category="COPY",
                        message=f"Closed copied trade {link.slave_ticket}")

                except Exception as e:
                    # log(db, link.slave_account_id, "ERROR",
                    #     f"Failed closing slave trade {link.slave_ticket}: {str(e)}",
                    #     category="COPY")
                    log(db=db,
                        account_id=link.slave_account_id,
                        level="ERROR",
                        category="COPY",
                        message=f"Failed closing slave trade {link.slave_ticket}: {str(e)}")

                link.status = "closed"

            db.commit()

        except Exception as e:
            # log(db, master_account_id, "ERROR",
            #     f"handle_close_trade error: {str(e)}",
            #     category="SYSTEM")
            log(db=db,
                account_id=master_account_id,
                level="ERROR",
                category="SYSTEM",
                message=f"handle_close_trade error: {str(e)}")
        finally:
            self._processing.discard(key)


    # =========================
    # CLOSE TRADE (SLAVE)
    # =========================
    async def handle_slave_close(self, db: Session, slave_ticket: str):
        try:
            key = f"close_slave:{slave_ticket}"
            if key in self._processing:
                return
            self._processing.add(key)

            link = db.query(CopyTradeLink).filter(
                CopyTradeLink.slave_ticket == slave_ticket,
                CopyTradeLink.status == "open"
            ).first()

            if not link:
                return

            # log(db, link.slave_account_id, "WARNING",
            #     f"Slave manually closed → cascading close",
            #     category="RISK")
            log(db=db,
                account_id=link.slave_account_id,
                level="WARNING",
                category="RISK",
                message=f"Slave manually closed → cascading close")

            master_acc = db.query(TradingAccount).filter(
                TradingAccount.id == link.master_account_id
            ).first()

            try:
                await trader.close_position(
                    master_acc.metaapi_account_id,
                    link.master_ticket
                )
            except Exception as e:
                # log(db, link.master_account_id, "ERROR",
                #     f"Failed closing master: {str(e)}",
                #     category="RISK")
                log(db=db,
                    account_id=link.master_account_id,
                    level="ERROR",
                    category="RISK",
                    message=f"Failed closing master: {str(e)}")

            all_links = db.query(CopyTradeLink).filter(
                CopyTradeLink.master_ticket == link.master_ticket,
                CopyTradeLink.status == "open"
            ).all()

            for l in all_links:
                slave_acc = db.query(TradingAccount).filter(
                    TradingAccount.id == l.slave_account_id
                ).first()

                try:
                    await trader.close_position(
                        slave_acc.metaapi_account_id,
                        l.slave_ticket
                    )
                except:
                    pass

                l.status = "closed"

            db.commit()

        except Exception as e:
            log(db=db,
                account_id=link.slave_account_id,
                level="ERROR",
                category="SYSTEM",
                message=f"handle_slave_close error: {str(e)}")
        finally:
            self._processing.discard(key)

# Singleton
copy_engine = CopyEngine()