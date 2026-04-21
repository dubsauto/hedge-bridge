# hedgebridge/copy_engine.py

from sqlalchemy.orm import Session
from app.model import CopyRelationship, CopyTradeLink, TradingAccount, CopyTradeSettings, AccountLot
from hedgebridge.trading import trader
from app.services.logger import log
from datetime import datetime


class CopyEngine:

    def __init__(self):
        self._processing = set()

    # =========================
    # PIP → PRICE (BROKER ACCURATE)
    # =========================
    async def pips_to_price(self, account_id: str, symbol: str, pips: int) -> float:
        try:
            connection = await trader._get_connection(account_id)
            spec = await connection.get_symbol_specification(symbol)

            point = spec.get("point", 0.0001)
            digits = spec.get("digits", 5)

            # 5-digit / 3-digit brokers → 1 pip = 10 points
            if digits in [3, 5]:
                pip_value = point * 10
            else:
                pip_value = point

            return pips * pip_value

        except Exception:
            # fallback safety
            return pips * 0.0001

    # =========================
    # NEW TRADE (MASTER)
    # =========================
    async def handle_new_trade(self, db: Session, master_account_id: int, position: dict):
        key = None

        try:
            master_ticket = str(position.get("id") or position.get("ticket"))
            symbol = position.get("symbol")
            volume = position.get("volume")
            trade_type = position.get("type")

            master_sl = position.get("stopLoss")
            master_tp = position.get("takeProfit")
            master_entry = position.get("price") or position.get("openPrice")

            key = f"open:{master_ticket}"
            if key in self._processing:
                return
            self._processing.add(key)

            # =========================
            # MASTER ACCOUNT
            # =========================
            master_acc = db.query(TradingAccount).filter(
                TradingAccount.id == master_account_id
            ).first()

            if not master_acc:
                return

            user_id = master_acc.owner_user_id

            # =========================
            # SETTINGS
            # =========================
            settings = db.query(CopyTradeSettings).filter_by(user_id=user_id).first()

            fixed_lot_enabled = settings.fixed_lot_enabled if settings else False
            pips_offset_enabled = settings.pips_offset_enabled if settings else False
            pips_offset = settings.pips_offset if settings else 0

            # =========================
            # LOT MAP
            # =========================
            account_lots_map = {
                row.account_id: row.lot_size
                for row in db.query(AccountLot).all()
            } if fixed_lot_enabled else {}

            # =========================
            # LOG MASTER
            # =========================
            log(
                db=db,
                account_id=master_account_id,
                level="TRADE",
                category="COPY",
                message=f"Master opened {symbol} {trade_type} {volume}",
                raw_json=position
            )

            # =========================
            # FIND SLAVES
            # =========================
            relationships = db.query(CopyRelationship).filter(
                CopyRelationship.master_account_id == master_account_id,
                CopyRelationship.slave_account_id.isnot(None),
                CopyRelationship.is_active == True
            ).all()

            # 🔥 TRACK EXECUTION STATE
            opened_links = []
            failed = False

            for rel in relationships:
                slave_id = rel.slave_account_id

                slave_acc = db.query(TradingAccount).filter(
                    TradingAccount.id == slave_id
                ).first()

                if not slave_acc:
                    continue

                # =========================
                # LOT
                # =========================
                final_volume = (
                    account_lots_map.get(slave_id, volume)
                    if fixed_lot_enabled else volume
                )

                # =========================
                # DIRECTION
                # =========================
                final_type = trade_type
                if rel.copy_direction == "opposite":
                    final_type = (
                        "POSITION_TYPE_SELL"
                        if trade_type == "POSITION_TYPE_BUY"
                        else "POSITION_TYPE_BUY"
                    )

                # =========================
                # SL/TP
                # =========================
                final_sl = None
                final_tp = None
                if master_entry:
                    if rel.copy_direction == "opposite":
                        final_sl = master_tp
                        final_tp = master_sl
                    else:
                        final_sl = master_sl
                        final_tp = master_tp

                # =========================
                # OFFSET
                # =========================
                if pips_offset_enabled and pips_offset > 0:
                    try:
                        offset_value = await self.pips_to_price(
                            slave_acc.metaapi_account_id,
                            symbol,
                            pips_offset
                        )

                        if final_type == "POSITION_TYPE_BUY":
                            if final_sl is not None:
                                final_sl -= offset_value
                            if final_tp is not None:
                                final_tp += offset_value
                        else:
                            if final_sl is not None:
                                final_sl += offset_value
                            if final_tp is not None:
                                final_tp -= offset_value

                    except Exception as offset_err:
                        log(
                            db=db,
                            account_id=slave_id,
                            level="ERROR",
                            category="OFFSET",
                            message=f"Offset error: {str(offset_err)}"
                        )

                # =========================
                # LOG COPY START
                # =========================
                log(
                    db=db,
                    account_id=slave_id,
                    level="INFO",
                    category="COPY",
                    message=f"Copying {symbol} {final_type} from master {master_ticket}"
                )

                # =========================
                # EXECUTE
                # =========================
                try:
                    if final_type == "POSITION_TYPE_BUY":
                        result = await trader.buy(
                            slave_acc.metaapi_account_id,
                            symbol,
                            final_volume,
                            final_sl,
                            final_tp,
                            comment=f"copy:{master_ticket}",
                            magic=slave_acc.magic
                        )
                    else:
                        result = await trader.sell(
                            slave_acc.metaapi_account_id,
                            symbol,
                            final_volume,
                            final_sl,
                            final_tp,
                            comment=f"copy:{master_ticket}",
                            magic=slave_acc.magic
                        )

                except Exception as exec_error:
                    log(
                        db=db,
                        account_id=slave_id,
                        level="ERROR",
                        category="EXECUTION",
                        message=f"Execution error: {str(exec_error)}"
                    )
                    failed = True
                    break

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
                        volume=final_volume,
                        status="open"
                    )

                    db.add(link)
                    db.commit()

                    opened_links.append((slave_acc, slave_ticket))

                    log(
                        db=db,
                        account_id=slave_id,
                        level="TRADE",
                        category="COPY",
                        message=f"Copied {symbol} → ticket {slave_ticket}",
                        raw_json=result.get("result")
                    )

                else:
                    log(
                        db=db,
                        account_id=slave_id,
                        level="ERROR",
                        category="COPY",
                        message=f"Copy failed: {result.get('error')}",
                        raw_json=result
                    )
                    failed = True
                    break

            # =========================
            # 🔥 SAFETY CHECK
            # =========================
            if failed:
                log(
                    db=db,
                    account_id=master_account_id,
                    level="ERROR",
                    category="SAFETY",
                    message="Execution failed → closing master & all slaves"
                )

                # CLOSE MASTER
                try:
                    await trader.close_position(
                        master_acc.metaapi_account_id,
                        master_ticket
                    )
                except:
                    pass

                # CLOSE SUCCESSFUL SLAVES
                for acc, ticket in opened_links:
                    try:
                        await trader.close_position(
                            acc.metaapi_account_id,
                            ticket
                        )
                    except:
                        pass

                return

        except Exception as e:
            log(
                db=db,
                account_id=master_account_id,
                level="ERROR",
                category="SYSTEM",
                message=f"handle_new_trade error: {str(e)}"
            )

        finally:
            if key:
                self._processing.discard(key)

    async def handle_close_trade(
        self,
        db: Session,
        account_id: int,          # 🔥 whoever triggered the close
        closed_ticket: str        # 🔥 ticket that was closed
    ):
        key = f"close:{account_id}:{closed_ticket}"

        try:
            if key in self._processing:
                return
            self._processing.add(key)

            # =========================
            # FIND LINK (MASTER OR SLAVE)
            # =========================
            link = db.query(CopyTradeLink).filter(
                (
                    (CopyTradeLink.master_account_id == account_id) &
                    (CopyTradeLink.master_ticket == closed_ticket)
                ) |
                (
                    (CopyTradeLink.slave_account_id == account_id) &
                    (CopyTradeLink.slave_ticket == closed_ticket)
                ),
                CopyTradeLink.status == "open"
            ).first()

            if not link:
                return

            master_ticket = link.master_ticket

            # =========================
            # GET FULL GROUP
            # =========================
            group_links = db.query(CopyTradeLink).filter(
                CopyTradeLink.master_ticket == master_ticket,
                CopyTradeLink.status == "open"
            ).all()

            # =========================
            # CLOSE MASTER (IF SLAVE TRIGGERED)
            # =========================
            if account_id != link.master_account_id:
                master_acc = db.query(TradingAccount).filter(
                    TradingAccount.id == link.master_account_id
                ).first()

                if master_acc:
                    try:
                        await trader.close_position(
                            master_acc.metaapi_account_id,
                            link.master_ticket
                        )

                        log(
                            db=db,
                            account_id=master_acc.id,
                            level="TRADE",
                            category="COPY",
                            message=f"Closed MASTER trade {link.master_ticket} (triggered by slave)"
                        )

                    except Exception as e:
                        log(
                            db=db,
                            account_id=master_acc.id,
                            level="ERROR",
                            category="COPY",
                            message=f"Failed closing MASTER: {str(e)}"
                        )

            # =========================
            # CLOSE ALL SLAVES
            # =========================
            for l in group_links:

                # skip the one already closed
                if (
                    l.slave_account_id == account_id and
                    l.slave_ticket == closed_ticket
                ):
                    continue

                slave_acc = db.query(TradingAccount).filter(
                    TradingAccount.id == l.slave_account_id
                ).first()

                if not slave_acc:
                    continue

                try:
                    await trader.close_position(
                        slave_acc.metaapi_account_id,
                        l.slave_ticket
                    )

                    log(
                        db=db,
                        account_id=l.slave_account_id,
                        level="TRADE",
                        category="COPY",
                        message=f"Closed slave trade {l.slave_ticket}"
                    )

                except Exception as e:
                    log(
                        db=db,
                        account_id=l.slave_account_id,
                        level="ERROR",
                        category="COPY",
                        message=f"Failed closing slave {l.slave_ticket}: {str(e)}"
                    )

            # =========================
            # MARK ALL CLOSED
            # =========================
            for l in group_links:
                l.status = "closed"
                l.closed_at = datetime.utcnow()

            db.commit()

        except Exception as e:
            log(
                db=db,
                account_id=account_id,
                level="ERROR",
                category="SYSTEM",
                message=f"handle_close_trade error: {str(e)}"
            )

        finally:
            self._processing.discard(key)

    async def handle_modify_trade(self, db: Session, account_id: int, position: dict):
        key = f"modify:{account_id}:{position.get('id')}"

        try:
            if key in self._processing:
                return
            self._processing.add(key)

            ticket = str(position.get("id"))
            new_sl = position.get("stopLoss")
            new_tp = position.get("takeProfit")

            # =========================
            # FIND LINK
            # =========================
            link = db.query(CopyTradeLink).filter(
                (
                    (CopyTradeLink.master_account_id == account_id) &
                    (CopyTradeLink.master_ticket == ticket)
                ) |
                (
                    (CopyTradeLink.slave_account_id == account_id) &
                    (CopyTradeLink.slave_ticket == ticket)
                ),
                CopyTradeLink.status == "open"
            ).first()

            if not link:
                return

            master_ticket = link.master_ticket

            # =========================
            # GET ALL RELATED TRADES
            # =========================
            group_links = db.query(CopyTradeLink).filter(
                CopyTradeLink.master_ticket == master_ticket,
                CopyTradeLink.status == "open"
            ).all()

            # =========================
            # LOOP THROUGH GROUP
            # =========================
            for l in group_links:

                # skip the one that triggered update
                if (
                    (l.master_account_id == account_id and l.master_ticket == ticket) or
                    (l.slave_account_id == account_id and l.slave_ticket == ticket)
                ):
                    continue

                # determine target account + ticket
                if l.master_account_id == account_id:
                    # master changed → update slaves
                    target_account = l.slave_account_id
                    target_ticket = l.slave_ticket
                    direction = l.trade_type
                else:
                    # slave changed → update master + other slaves
                    target_account = (
                        l.master_account_id if l.master_ticket != ticket else l.slave_account_id
                    )
                    target_ticket = (
                        l.master_ticket if l.master_ticket != ticket else l.slave_ticket
                    )
                    direction = l.trade_type

                acc = db.query(TradingAccount).filter(
                    TradingAccount.id == target_account
                ).first()

                if not acc:
                    continue

                final_sl = new_sl
                final_tp = new_tp

                # =========================
                # HANDLE OPPOSITE DIRECTION
                # =========================
                if direction == "position_type_sell" or direction == "position_type_buy":
                    # same direction → keep as is
                    pass
                else:
                    # opposite direction
                    final_sl, final_tp = new_tp, new_sl

                try:
                    await trader.modify_position(
                        acc.metaapi_account_id,
                        target_ticket,
                        final_sl,
                        final_tp
                    )

                    log(
                        db=db,
                        account_id=target_account,
                        level="TRADE",
                        category="MODIFY",
                        message=f"Synced SL/TP for {target_ticket}"
                    )

                except Exception as e:
                    log(
                        db=db,
                        account_id=target_account,
                        level="ERROR",
                        category="MODIFY",
                        message=f"Modify failed: {str(e)}"
                    )

        finally:
            self._processing.discard(key)
            

# Singleton
copy_engine = CopyEngine()






# # =========================
# # GET SLAVE PRICE
# # =========================
# try:
#     price_data = await trader.get_price(
#         slave_acc.metaapi_account_id,
#         symbol
#     )

#     entry_price = (
#         price_data["ask"]
#         if final_type == "POSITION_TYPE_BUY"
#         else price_data["bid"]
#     )

# except Exception as price_err:
#     log(
#         db=db,
#         account_id=slave_id,
#         level="ERROR",
#         category="PRICE",
#         message=f"Price fetch failed: {str(price_err)}"
#     )
#     continue

# # =========================
# # CLOSE TRADE (MASTER)
# # =========================
# async def handle_close_trade(self, db: Session, master_account_id: int, master_ticket: str):
#     try:
#         key = f"close_master:{master_ticket}"
#         if key in self._processing:
#             return
#         self._processing.add(key)

#         log(db=db,
#             account_id=master_account_id,
#             level="TRADE",
#             category="COPY",
#             message=f"Master closing trade {master_ticket}")

#         links = db.query(CopyTradeLink).filter(
#             CopyTradeLink.master_ticket == master_ticket,
#             CopyTradeLink.status == "open"
#         ).all()

#         for link in links:
#             slave_acc = db.query(TradingAccount).filter(
#                 TradingAccount.id == link.slave_account_id
#             ).first()

#             if not slave_acc:
#                 continue

#             try:
#                 await trader.close_position(
#                     slave_acc.metaapi_account_id,
#                     link.slave_ticket
#                 )

#                 # log(db, link.slave_account_id, "TRADE",
#                 #     f"Closed copied trade {link.slave_ticket}",
#                 #     category="COPY")
                
#                 log(db=db,
#                     account_id=link.slave_account_id,
#                     level="TRADE",
#                     category="COPY",
#                     message=f"Closed copied trade {link.slave_ticket}")

#             except Exception as e:
#                 # log(db, link.slave_account_id, "ERROR",
#                 #     f"Failed closing slave trade {link.slave_ticket}: {str(e)}",
#                 #     category="COPY")
#                 log(db=db,
#                     account_id=link.slave_account_id,
#                     level="ERROR",
#                     category="COPY",
#                     message=f"Failed closing slave trade {link.slave_ticket}: {str(e)}")

#             link.status = "closed"

#         db.commit()

#     except Exception as e:
#         # log(db, master_account_id, "ERROR",
#         #     f"handle_close_trade error: {str(e)}",
#         #     category="SYSTEM")
#         log(db=db,
#             account_id=master_account_id,
#             level="ERROR",
#             category="SYSTEM",
#             message=f"handle_close_trade error: {str(e)}")
#     finally:
#         self._processing.discard(key)


# =========================
# CLOSE TRADE (SLAVE)
# =========================
# async def handle_slave_close(self, db: Session, slave_ticket: str):
#     try:
#         key = f"close_slave:{slave_ticket}"
#         if key in self._processing:
#             return
#         self._processing.add(key)

#         link = db.query(CopyTradeLink).filter(
#             CopyTradeLink.slave_ticket == slave_ticket,
#             CopyTradeLink.status == "open"
#         ).first()

#         if not link:
#             return

#         # log(db, link.slave_account_id, "WARNING",
#         #     f"Slave manually closed → cascading close",
#         #     category="RISK")
#         log(db=db,
#             account_id=link.slave_account_id,
#             level="WARNING",
#             category="RISK",
#             message=f"Slave manually closed → cascading close")

#         master_acc = db.query(TradingAccount).filter(
#             TradingAccount.id == link.master_account_id
#         ).first()

#         try:
#             await trader.close_position(
#                 master_acc.metaapi_account_id,
#                 link.master_ticket
#             )
#         except Exception as e:
#             # log(db, link.master_account_id, "ERROR",
#             #     f"Failed closing master: {str(e)}",
#             #     category="RISK")
#             log(db=db,
#                 account_id=link.master_account_id,
#                 level="ERROR",
#                 category="RISK",
#                 message=f"Failed closing master: {str(e)}")

#         all_links = db.query(CopyTradeLink).filter(
#             CopyTradeLink.master_ticket == link.master_ticket,
#             CopyTradeLink.status == "open"
#         ).all()

#         for l in all_links:
#             slave_acc = db.query(TradingAccount).filter(
#                 TradingAccount.id == l.slave_account_id
#             ).first()

#             try:
#                 await trader.close_position(
#                     slave_acc.metaapi_account_id,
#                     l.slave_ticket
#                 )
#             except:
#                 pass

#             l.status = "closed"

#         db.commit()

#     except Exception as e:
#         log(db=db,
#             account_id=link.slave_account_id,
#             level="ERROR",
#             category="SYSTEM",
#             message=f"handle_slave_close error: {str(e)}")
#     finally:
#         self._processing.discard(key)




# sl_distance = abs(master_entry - master_sl) if master_sl else None
# tp_distance = abs(master_entry - master_tp) if master_tp else None

# if final_type == "POSITION_TYPE_BUY":
#     if sl_distance:
#         final_sl = entry_price - sl_distance
#     if tp_distance:
#         final_tp = entry_price + tp_distance

# else:  # SELL
#     if sl_distance:
#         final_sl = entry_price + sl_distance
#     if tp_distance:
#         final_tp = entry_price - tp_distance




# async def handle_new_trade(self, db: Session, master_account_id: int, position: dict):
#     try:
#         master_ticket = str(position.get("id") or position.get("ticket"))
#         symbol = position.get("symbol")
#         volume = position.get("volume")
#         trade_type = position.get("type")
#         sl = position.get("stopLoss")
#         tp = position.get("takeProfit")

#         key = f"open:{master_ticket}"
#         if key in self._processing:
#             return
#         self._processing.add(key)

#         # log(db, master_account_id, "TRADE",
#         #     f"Master opened {symbol} {trade_type} {volume}",
#         #     category="COPY",
#         #     raw_json=position)
#         log(db=db,
#             account_id=master_account_id,
#             level="TRADE",
#             category="COPY",
#             message=f"Master opened {symbol} {trade_type} {volume}",
#             raw_json=position)

#         # =========================
#         # FIND SLAVES
#         # =========================
#         relationships = db.query(CopyRelationship).filter(
#             CopyRelationship.master_account_id == master_account_id,
#             CopyRelationship.slave_account_id.isnot(None),
#             CopyRelationship.is_active == True
#         ).all()

#         settings = db.query(CopyTradeSettings).first()
#         fixed_lot_enabled = settings.fixed_lot_enabled if settings else False

#         for rel in relationships:
#             slave_id = rel.slave_account_id

#             slave_acc = db.query(TradingAccount).filter(
#                 TradingAccount.id == slave_id
#             ).first()

#             if not slave_acc:
#                 continue

#             if fixed_lot_enabled:
#                 lot_row = db.query(AccountLot).filter_by(account_id=slave_id).first()
                
#                 if lot_row:
#                     final_volume = lot_row.lot_size
#                 else:
#                     final_volume = volume  # fallback
#             else:
#                 final_volume = volume

#             # log(db, slave_id, "INFO",
#             #     f"Copying trade {symbol} from master {master_ticket}",
#             #     category="COPY")
#             log(db=db,
#                 account_id=slave_id,
#                 level="INFO",
#                 category="COPY",
#                 message=f"Copying trade {symbol} from master {master_ticket}")

#             # =========================
#             # DIRECTION
#             # =========================
#             final_type = trade_type
#             if rel.copy_direction == "opposite":
#                 final_type = "POSITION_TYPE_SELL" if trade_type == "POSITION_TYPE_BUY" else "POSITION_TYPE_BUY"

#             # =========================
#             # EXECUTE
#             # =========================
#             if final_type == "POSITION_TYPE_BUY":
#                 result = await trader.buy(
#                     slave_acc.metaapi_account_id,
#                     symbol,
#                     final_volume,
#                     sl,
#                     tp,
#                     comment=f"copy:{master_ticket}",
#                     magic=slave_acc.magic
#                 )
#             else:
#                 result = await trader.sell(
#                     slave_acc.metaapi_account_id,
#                     symbol,
#                     volume,
#                     sl,
#                     tp,
#                     comment=f"copy:{master_ticket}",
#                     magic=slave_acc.magic
#                 )

#             # =========================
#             # RESULT
#             # =========================
#             if result.get("success"):
#                 slave_ticket = str(result["result"]["orderId"])

#                 link = CopyTradeLink(
#                     master_account_id=master_account_id,
#                     slave_account_id=slave_id,
#                     master_ticket=master_ticket,
#                     slave_ticket=slave_ticket,
#                     symbol=symbol,
#                     trade_type=final_type.lower(),
#                     volume=volume,
#                     status="open"
#                 )
#                 db.add(link)
#                 db.commit()

#                 # log(db, slave_id, "TRADE",
#                 #     f"Copied trade {symbol} → ticket {slave_ticket}",
#                 #     category="COPY",
#                 #     raw_json=result.get("result"))
#                 log(db=db,
#                     account_id=slave_id,
#                     level="TRADE",
#                     category="COPY",
#                     message=f"Copied trade {symbol} → ticket {slave_ticket}",
#                     raw_json=result.get("result"))

#             else:
#                 # log(db, slave_id, "ERROR",
#                 #     f"Copy failed: {result.get('error')}",
#                 #     category="COPY")
#                 log(db=db,
#                     account_id=slave_id,
#                     level="ERROR",
#                     category="COPY",
#                     message=f"Copy failed: {result.get('error')}",
#                     raw_json=result)

#     except Exception as e:
#         # log(db, master_account_id, "ERROR",
#         #     f"handle_new_trade error: {str(e)}",
#         #     category="SYSTEM")
#         log(db=db,
#             account_id=master_account_id,
#             level="ERROR",
#             category="SYSTEM",
#             message=f"handle_new_trade error: {str(e)}")
#     finally:
#         self._processing.discard(key)




    # =========================
# NEW TRADE (MASTER)
# =========================
# async def handle_new_trade(self, db: Session, master_account_id: int, position: dict):
#     key = None
#     try:
#         master_ticket = str(position.get("id") or position.get("ticket"))
#         symbol = position.get("symbol")
#         volume = position.get("volume")
#         trade_type = position.get("type")
#         sl = position.get("stopLoss")
#         tp = position.get("takeProfit")

#         key = f"open:{master_ticket}"
#         if key in self._processing:
#             return
#         self._processing.add(key)

#         # =========================
#         # GET MASTER ACCOUNT + USER
#         # =========================
#         master_acc = db.query(TradingAccount).filter(
#             TradingAccount.id == master_account_id
#         ).first()

#         if not master_acc:
#             return

#         user_id = master_acc.owner_user_id

#         # =========================
#         # SETTINGS (PER USER)
#         # =========================
#         settings = db.query(CopyTradeSettings).filter_by(user_id=user_id).first()
#         fixed_lot_enabled = settings.fixed_lot_enabled if settings else False

#         # =========================
#         # PRELOAD ACCOUNT LOTS (OPTIMIZED)
#         # =========================
#         account_lots_map = {
#             row.account_id: row.lot_size
#             for row in db.query(AccountLot).all()
#         } if fixed_lot_enabled else {}

#         # =========================
#         # LOG MASTER TRADE
#         # =========================
#         log(
#             db=db,
#             account_id=master_account_id,
#             level="TRADE",
#             category="COPY",
#             message=f"Master opened {symbol} {trade_type} {volume}",
#             raw_json=position
#         )

#         # =========================
#         # FIND SLAVES
#         # =========================
#         relationships = db.query(CopyRelationship).filter(
#             CopyRelationship.master_account_id == master_account_id,
#             CopyRelationship.slave_account_id.isnot(None),
#             CopyRelationship.is_active == True
#         ).all()

#         for rel in relationships:
#             slave_id = rel.slave_account_id

#             slave_acc = db.query(TradingAccount).filter(
#                 TradingAccount.id == slave_id
#             ).first()

#             if not slave_acc:
#                 continue

#             # =========================
#             # LOT LOGIC
#             # =========================
#             if fixed_lot_enabled:
#                 final_volume = account_lots_map.get(slave_id, volume)
#             else:
#                 final_volume = volume

#             # =========================
#             # LOG COPY START
#             # =========================
#             log(
#                 db=db,
#                 account_id=slave_id,
#                 level="INFO",
#                 category="COPY",
#                 message=f"Copying trade {symbol} from master {master_ticket}"
#             )

#             # =========================
#             # DIRECTION
#             # =========================
#             final_type = trade_type
#             if rel.copy_direction == "opposite":
#                 final_type = (
#                     "POSITION_TYPE_SELL"
#                     if trade_type == "POSITION_TYPE_BUY"
#                     else "POSITION_TYPE_BUY"
#                 )

#             # =========================
#             # EXECUTE TRADE
#             # =========================
#             try:
#                 if final_type == "POSITION_TYPE_BUY":
#                     result = await trader.buy(
#                         slave_acc.metaapi_account_id,
#                         symbol,
#                         final_volume,
#                         sl,
#                         tp,
#                         comment=f"copy:{master_ticket}",
#                         magic=slave_acc.magic
#                     )
#                 else:
#                     result = await trader.sell(
#                         slave_acc.metaapi_account_id,
#                         symbol,
#                         final_volume,  # ✅ FIXED
#                         sl,
#                         tp,
#                         comment=f"copy:{master_ticket}",
#                         magic=slave_acc.magic
#                     )

#             except Exception as exec_error:
#                 log(
#                     db=db,
#                     account_id=slave_id,
#                     level="ERROR",
#                     category="EXECUTION",
#                     message=f"Execution error: {str(exec_error)}"
#                 )
#                 continue

#             # =========================
#             # RESULT HANDLING
#             # =========================
#             if result.get("success"):
#                 slave_ticket = str(result["result"]["orderId"])

#                 link = CopyTradeLink(
#                     master_account_id=master_account_id,
#                     slave_account_id=slave_id,
#                     master_ticket=master_ticket,
#                     slave_ticket=slave_ticket,
#                     symbol=symbol,
#                     trade_type=final_type.lower(),
#                     volume=final_volume,  # ✅ store actual used volume
#                     status="open"
#                 )
#                 db.add(link)
#                 db.commit()

#                 log(
#                     db=db,
#                     account_id=slave_id,
#                     level="TRADE",
#                     category="COPY",
#                     message=f"Copied trade {symbol} → ticket {slave_ticket}",
#                     raw_json=result.get("result")
#                 )

#             else:
#                 log(
#                     db=db,
#                     account_id=slave_id,
#                     level="ERROR",
#                     category="COPY",
#                     message=f"Copy failed: {result.get('error')}",
#                     raw_json=result
#                 )

#     except Exception as e:
#         log(
#             db=db,
#             account_id=master_account_id,
#             level="ERROR",
#             category="SYSTEM",
#             message=f"handle_new_trade error: {str(e)}"
#         )
#     finally:
#         if key:
#             self._processing.discard(key)



    # async def handle_new_trade(self, db: Session, master_account_id: int, position: dict):
    #     key = None

    #     try:
    #         master_ticket = str(position.get("id") or position.get("ticket"))
    #         symbol = position.get("symbol")
    #         volume = position.get("volume")
    #         trade_type = position.get("type")

    #         master_sl = position.get("stopLoss")
    #         master_tp = position.get("takeProfit")
    #         master_entry = position.get("price") or position.get("openPrice")

    #         key = f"open:{master_ticket}"
    #         if key in self._processing:
    #             return
    #         self._processing.add(key)

    #         # =========================
    #         # MASTER ACCOUNT
    #         # =========================
    #         master_acc = db.query(TradingAccount).filter(
    #             TradingAccount.id == master_account_id
    #         ).first()

    #         if not master_acc:
    #             return

    #         user_id = master_acc.owner_user_id

    #         # =========================
    #         # SETTINGS
    #         # =========================
    #         settings = db.query(CopyTradeSettings).filter_by(user_id=user_id).first()

    #         fixed_lot_enabled = settings.fixed_lot_enabled if settings else False
    #         pips_offset_enabled = settings.pips_offset_enabled if settings else False
    #         pips_offset = settings.pips_offset if settings else 0

    #         # =========================
    #         # LOT MAP
    #         # =========================
    #         account_lots_map = {
    #             row.account_id: row.lot_size
    #             for row in db.query(AccountLot).all()
    #         } if fixed_lot_enabled else {}

    #         # =========================
    #         # LOG MASTER
    #         # =========================
    #         log(
    #             db=db,
    #             account_id=master_account_id,
    #             level="TRADE",
    #             category="COPY",
    #             message=f"Master opened {symbol} {trade_type} {volume}",
    #             raw_json=position
    #         )

    #         # =========================
    #         # FIND SLAVES
    #         # =========================
    #         relationships = db.query(CopyRelationship).filter(
    #             CopyRelationship.master_account_id == master_account_id,
    #             CopyRelationship.slave_account_id.isnot(None),
    #             CopyRelationship.is_active == True
    #         ).all()

    #         for rel in relationships:
    #             slave_id = rel.slave_account_id

    #             slave_acc = db.query(TradingAccount).filter(
    #                 TradingAccount.id == slave_id
    #             ).first()

    #             if not slave_acc:
    #                 continue

    #             # =========================
    #             # LOT
    #             # =========================
    #             final_volume = (
    #                 account_lots_map.get(slave_id, volume)
    #                 if fixed_lot_enabled else volume
    #             )

    #             # =========================
    #             # DIRECTION
    #             # =========================
    #             final_type = trade_type
    #             if rel.copy_direction == "opposite":
    #                 final_type = (
    #                     "POSITION_TYPE_SELL"
    #                     if trade_type == "POSITION_TYPE_BUY"
    #                     else "POSITION_TYPE_BUY"
    #                 )

    #             # =========================
    #             # DISTANCE-BASED SL/TP
    #             # =========================
    #             final_sl = None
    #             final_tp = None
    #             if master_entry:
    #                 if rel.copy_direction == "opposite":
    #                     final_sl = master_tp
    #                     final_tp = master_sl
    #                 else:
    #                     final_sl = master_sl
    #                     final_tp = master_tp

    #             # =========================
    #             # OFFSET (AFTER SL/TP FIX)
    #             # =========================
    #             if pips_offset_enabled and pips_offset > 0:
    #                 try:
    #                     offset_value = await self.pips_to_price(
    #                         slave_acc.metaapi_account_id,
    #                         symbol,
    #                         pips_offset
    #                     )

    #                     if final_type == "POSITION_TYPE_BUY":
    #                         if final_sl is not None:
    #                             final_sl -= offset_value
    #                         if final_tp is not None:
    #                             final_tp += offset_value
    #                     else:
    #                         if final_sl is not None:
    #                             final_sl += offset_value
    #                         if final_tp is not None:
    #                             final_tp -= offset_value

    #                 except Exception as offset_err:
    #                     log(
    #                         db=db,
    #                         account_id=slave_id,
    #                         level="ERROR",
    #                         category="OFFSET",
    #                         message=f"Offset error: {str(offset_err)}"
    #                     )

    #             # =========================
    #             # LOG COPY START
    #             # =========================
    #             log(
    #                 db=db,
    #                 account_id=slave_id,
    #                 level="INFO",
    #                 category="COPY",
    #                 message=f"Copying {symbol} {final_type} from master {master_ticket}"
    #             )

    #             # =========================
    #             # EXECUTE
    #             # =========================
    #             try:
    #                 if final_type == "POSITION_TYPE_BUY":
    #                     result = await trader.buy(
    #                         slave_acc.metaapi_account_id,
    #                         symbol,
    #                         final_volume,
    #                         final_sl,
    #                         final_tp,
    #                         comment=f"copy:{master_ticket}",
    #                         magic=slave_acc.magic
    #                     )
    #                 else:
    #                     result = await trader.sell(
    #                         slave_acc.metaapi_account_id,
    #                         symbol,
    #                         final_volume,
    #                         final_sl,
    #                         final_tp,
    #                         comment=f"copy:{master_ticket}",
    #                         magic=slave_acc.magic
    #                     )

    #             except Exception as exec_error:
    #                 log(
    #                     db=db,
    #                     account_id=slave_id,
    #                     level="ERROR",
    #                     category="EXECUTION",
    #                     message=f"Execution error: {str(exec_error)}"
    #                 )
    #                 continue

    #             # =========================
    #             # RESULT
    #             # =========================
    #             if result.get("success"):
    #                 slave_ticket = str(result["result"]["orderId"])

    #                 link = CopyTradeLink(
    #                     master_account_id=master_account_id,
    #                     slave_account_id=slave_id,
    #                     master_ticket=master_ticket,
    #                     slave_ticket=slave_ticket,
    #                     symbol=symbol,
    #                     trade_type=final_type.lower(),
    #                     volume=final_volume,
    #                     status="open"
    #                 )

    #                 db.add(link)
    #                 db.commit()

    #                 log(
    #                     db=db,
    #                     account_id=slave_id,
    #                     level="TRADE",
    #                     category="COPY",
    #                     message=f"Copied {symbol} → ticket {slave_ticket}",
    #                     raw_json=result.get("result")
    #                 )

    #             else:
    #                 log(
    #                     db=db,
    #                     account_id=slave_id,
    #                     level="ERROR",
    #                     category="COPY",
    #                     message=f"Copy failed: {result.get('error')}",
    #                     raw_json=result
    #                 )

    #     except Exception as e:
    #         log(
    #             db=db,
    #             account_id=master_account_id,
    #             level="ERROR",
    #             category="SYSTEM",
    #             message=f"handle_new_trade error: {str(e)}"
    #         )

    #     finally:
    #         if key:
    #             self._processing.discard(key)
    