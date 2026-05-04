# hedgebridge/copy_engine.py

from sqlalchemy.orm import Session
from app.model import CopyRelationship, CopyTradeLink, TradingAccount, CopyTradeSettings, AccountLot
from hedgebridge.tradingListener import trader_listener
from app.database import SessionLocal
from app.services.logger import log
from datetime import datetime
import asyncio
from sqlalchemy import func


class CopyEngine:

    def __init__(self):
        self._processing = set()

    # =========================
    # PIP → PRICE (BROKER ACCURATE)
    # =========================
    async def pips_to_price(self, account_id: str, symbol: str, pips: int) -> float:
        try:
            connection = await trader_listener._get_connection(account_id)
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
    async def handle_new_trade(self, master_account_id: int, position: dict):
        db = SessionLocal()
        key = None

        try:
            print(f"\n========== NEW TRADE DETECTED ==========")
            print(f"[START] master_account_id={master_account_id}")
            print(f"[POSITION] raw_position={position}")

            master_ticket = str(position.get("id") or position.get("ticket"))
            symbol = position.get("symbol")
            volume = position.get("volume")
            trade_type = position.get("type")

            master_sl = position.get("stopLoss")
            master_tp = position.get("takeProfit")
            master_entry = position.get("price") or position.get("openPrice")

            print(
                f"[MASTER TRADE] ticket={master_ticket}, symbol={symbol}, "
                f"volume={volume}, type={trade_type}, "
                f"SL={master_sl}, TP={master_tp}, entry={master_entry}"
            )

            key = f"open:{master_ticket}"

            if key in self._processing:
                print(f"[SKIP] Already processing trade {master_ticket}")
                return

            self._processing.add(key)
            print(f"[LOCK] Added processing key={key}")

            # =========================
            # STEP 1: DB READ ONLY
            # =========================
            print("[STEP 1] Loading DB data...")

            master_acc = db.query(TradingAccount).filter(
                TradingAccount.id == master_account_id
            ).first()

            if not master_acc:
                print(f"[ERROR] Master account not found: {master_account_id}")
                return

            print(
                f"[MASTER ACCOUNT] id={master_acc.id}, "
                f"metaapi_account_id={master_acc.metaapi_account_id}"
            )

            user_id = master_acc.owner_user_id

            settings = db.query(CopyTradeSettings).filter_by(
                user_id=user_id
            ).first()

            fixed_lot_enabled = settings.fixed_lot_enabled if settings else False
            pips_offset_enabled = settings.pips_offset_enabled if settings else False
            pips_offset = settings.pips_offset if settings else 0

            print(
                f"[SETTINGS] fixed_lot_enabled={fixed_lot_enabled}, "
                f"pips_offset_enabled={pips_offset_enabled}, "
                f"pips_offset={pips_offset}"
            )

            account_lots_map = {
                row.account_id: row.lot_size
                for row in db.query(AccountLot).all()
            } if fixed_lot_enabled else {}

            if fixed_lot_enabled:
                print(f"[LOT MAP] {account_lots_map}")

            relationships = db.query(CopyRelationship).filter(
                CopyRelationship.master_account_id == master_account_id,
                CopyRelationship.slave_account_id.isnot(None),
                CopyRelationship.is_active == True
            ).all()

            print(f"[RELATIONSHIPS] Found {len(relationships)} active relationships")

            if not relationships:
                print("[SKIP] No active slave relationships found")
                return

            slave_accounts = {
                acc.id: acc
                for acc in db.query(TradingAccount).filter(
                    TradingAccount.id.in_(
                        [r.slave_account_id for r in relationships]
                    ),
                    func.lower(TradingAccount.state) == "deployed",
                    TradingAccount.listener_active == True
                ).all()
            }

            print(f"[SLAVES READY] Found {len(slave_accounts)} deployed + active slaves")

            # close read session early
            db.close()
            print("[DB] Read session closed")

            # =========================
            # STEP 2: PREPARE TASKS
            # =========================
            print("[STEP 2] Preparing execution tasks...")

            execution_tasks = []
            task_meta = []

            for rel in relationships:
                slave_id = rel.slave_account_id
                slave_acc = slave_accounts.get(slave_id)

                if not slave_acc:
                    print(
                        f"[SKIP] Slave {slave_id} not deployed or listener not active"
                    )
                    continue

                print(f"\n[SLAVE] Preparing slave_id={slave_id}")

                # -------------------------
                # LOT SIZE
                # -------------------------
                final_volume = (
                    account_lots_map.get(slave_id, volume)
                    if fixed_lot_enabled
                    else volume
                )

                print(f"[LOT] final_volume={final_volume}")

                # -------------------------
                # DIRECTION
                # -------------------------
                final_type = trade_type

                if rel.copy_direction == "opposite":
                    final_type = (
                        "POSITION_TYPE_SELL"
                        if trade_type == "POSITION_TYPE_BUY"
                        else "POSITION_TYPE_BUY"
                    )

                print(
                    f"[DIRECTION] copy_direction={rel.copy_direction}, "
                    f"final_type={final_type}"
                )

                # -------------------------
                # SL / TP
                # -------------------------
                final_sl = master_sl
                final_tp = master_tp

                if master_entry and rel.copy_direction == "opposite":
                    final_sl, final_tp = master_tp, master_sl

                print(f"[SLTP BEFORE OFFSET] final_sl={final_sl}, final_tp={final_tp}")

                # -------------------------
                # PIPS OFFSET
                # -------------------------
                if pips_offset_enabled and pips_offset > 0:
                    try:
                        print(
                            f"[PIPS OFFSET] Calculating offset for slave={slave_id}"
                        )

                        offset_value = await asyncio.wait_for(
                            self.pips_to_price(
                                slave_acc.metaapi_account_id,
                                symbol,
                                pips_offset
                            ),
                            timeout=5
                        )

                        print(f"[PIPS OFFSET] offset_value={offset_value}")

                        if final_type == "POSITION_TYPE_BUY":
                            if final_sl:
                                final_sl -= offset_value
                            if final_tp:
                                final_tp += offset_value
                        else:
                            if final_sl:
                                final_sl += offset_value
                            if final_tp:
                                final_tp -= offset_value

                        print(
                            f"[SLTP AFTER OFFSET] final_sl={final_sl}, "
                            f"final_tp={final_tp}"
                        )

                    except Exception as e:
                        print(
                            f"[WARNING] Pips offset failed for slave={slave_id}: {str(e)}"
                        )

                # -------------------------
                # BUILD EXECUTION TASK
                # -------------------------
                print(
                    f"[TASK] Building {final_type} task for slave={slave_id}"
                )

                if final_type == "POSITION_TYPE_BUY":
                    task = asyncio.wait_for(
                        trader_listener.buy(
                            slave_acc.metaapi_account_id,
                            symbol,
                            final_volume,
                            final_sl,
                            final_tp,
                            comment=f"copy:{master_ticket}",
                            magic=slave_acc.magic
                        ),
                        timeout=15
                    )
                else:
                    task = asyncio.wait_for(
                        trader_listener.sell(
                            slave_acc.metaapi_account_id,
                            symbol,
                            final_volume,
                            final_sl,
                            final_tp,
                            comment=f"copy:{master_ticket}",
                            magic=slave_acc.magic
                        ),
                        timeout=15
                    )

                execution_tasks.append(task)

                task_meta.append({
                    "slave_id": slave_id,
                    "slave_acc": slave_acc,
                    "final_type": final_type,
                    "final_volume": final_volume
                })

            print(f"\n[STEP 3] Executing {len(execution_tasks)} tasks in parallel...")

            # =========================
            # STEP 3: EXECUTE ALL IN PARALLEL
            # =========================
            results = await asyncio.gather(
                *execution_tasks,
                return_exceptions=True
            )

            opened_links = []
            failed = False

            # =========================
            # STEP 4: SAVE SUCCESSFUL RESULTS
            # =========================
            print("[STEP 4] Processing execution results...")

            for meta, result in zip(task_meta, results):
                slave_id = meta["slave_id"]
                slave_acc = meta["slave_acc"]
                final_type = meta["final_type"]
                final_volume = meta["final_volume"]

                print(f"\n[RESULT] slave_id={slave_id}")

                # task exception
                if isinstance(result, Exception):
                    print(
                        f"[FAILURE] Task exception for slave={slave_id}: {str(result)}"
                    )
                    failed = True
                    continue

                print(f"[BROKER RESPONSE] {result}")

                # broker returned failure
                if not result.get("success"):
                    print(
                        f"[FAILURE] Broker returned unsuccessful response "
                        f"for slave={slave_id}"
                    )
                    failed = True
                    continue

                try:
                    slave_ticket = str(
                        result["result"]["orderId"]
                    )

                    print(
                        f"[SUCCESS] slave_id={slave_id}, "
                        f"slave_ticket={slave_ticket}"
                    )

                    write_db = SessionLocal()

                    try:
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

                        write_db.add(link)
                        write_db.commit()

                        print(
                            f"[DB SAVE] Link saved for slave={slave_id}, "
                            f"ticket={slave_ticket}"
                        )

                        opened_links.append(
                            (slave_acc, slave_ticket)
                        )

                    finally:
                        write_db.close()

                except Exception as e:
                    print(
                        f"[FAILURE] Failed saving link for slave={slave_id}: {str(e)}"
                    )
                    failed = True

            # =========================
            # STEP 5: SAFETY ROLLBACK
            # =========================
            if failed:
                print("\n========== SAFETY ROLLBACK TRIGGERED ==========")
                print(
                    "[ROLLBACK REASON] At least one slave copy failed, "
                    "closing master + all successful slave trades"
                )

                try:
                    print(
                        f"[ROLLBACK] Closing MASTER trade ticket={master_ticket}"
                    )

                    await trader_listener.close_position(
                        master_acc.metaapi_account_id,
                        master_ticket
                    )

                    print("[ROLLBACK] Master trade closed successfully")

                except Exception as e:
                    print(
                        f"[ROLLBACK ERROR] Failed closing master trade: {str(e)}"
                    )

                rollback_tasks = [
                    trader_listener.close_position(
                        acc.metaapi_account_id,
                        ticket
                    )
                    for acc, ticket in opened_links
                ]

                print(
                    f"[ROLLBACK] Closing {len(rollback_tasks)} successful slave trades"
                )

                if rollback_tasks:
                    rollback_results = await asyncio.gather(
                        *rollback_tasks,
                        return_exceptions=True
                    )

                    for i, rollback_result in enumerate(rollback_results, 1):
                        print(
                            f"[ROLLBACK RESULT {i}] {rollback_result}"
                        )

                print("========== ROLLBACK COMPLETE ==========\n")
            else:
                print(
                    "\n[SUCCESS] All slave trades copied successfully. No rollback needed.\n"
                )

        finally:
            try:
                db.close()
            except Exception:
                pass

            if key:
                self._processing.discard(key)
                print(f"[UNLOCK] Removed processing key={key}")


    async def handle_close_trade(
        self,
        account_id: int,
        closed_ticket: str
    ):
        key = f"close:{account_id}:{closed_ticket}"
        db = SessionLocal()

        try:
            if key in self._processing:
                return

            self._processing.add(key)

            # =========================
            # STEP 1: DB READ ONLY
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
            master_account_id = link.master_account_id

            group_links = db.query(CopyTradeLink).filter(
                CopyTradeLink.master_ticket == master_ticket,
                CopyTradeLink.status == "open"
            ).all()

            if not group_links:
                return

            master_acc = db.query(TradingAccount).filter(
                TradingAccount.id == master_account_id
            ).first()

            slave_accounts = {
                acc.id: acc
                for acc in db.query(TradingAccount).filter(
                    TradingAccount.id.in_(
                        [l.slave_account_id for l in group_links if l.slave_account_id]
                    ),
                    func.lower(TradingAccount.state) == "deployed",
                    TradingAccount.listener_active == True
                ).all()
            }

            # close read session early
            db.close()

            # =========================
            # STEP 2: BUILD CLOSE TASKS
            # =========================
            execution_tasks = []
            task_meta = []

            # --------------------------------
            # close master if triggered by slave
            # --------------------------------
            if account_id != master_account_id and master_acc:
                execution_tasks.append(
                    asyncio.wait_for(
                        trader_listener.close_position(
                            master_acc.metaapi_account_id,
                            master_ticket
                        ),
                        timeout=15
                    )
                )

                task_meta.append({
                    "role": "master",
                    "ticket": master_ticket,
                    "account_id": master_account_id
                })

            # --------------------------------
            # close slave positions
            # --------------------------------
            for l in group_links:

                # skip origin slave
                if (
                    l.slave_account_id == account_id and
                    l.slave_ticket == closed_ticket
                ):
                    continue

                slave_acc = slave_accounts.get(l.slave_account_id)

                if not slave_acc:
                    continue

                execution_tasks.append(
                    asyncio.wait_for(
                        trader_listener.close_position(
                            slave_acc.metaapi_account_id,
                            l.slave_ticket
                        ),
                        timeout=15
                    )
                )

                task_meta.append({
                    "role": "slave",
                    "ticket": l.slave_ticket,
                    "account_id": l.slave_account_id
                })

            # =========================
            # STEP 3: EXECUTE ALL CLOSES IN PARALLEL
            # =========================
            results = []

            if execution_tasks:
                results = await asyncio.gather(
                    *execution_tasks,
                    return_exceptions=True
                )

            # =========================
            # STEP 4: LOG RESULTS
            # =========================
            for meta, result in zip(task_meta, results):
                role = meta["role"]
                ticket = meta["ticket"]
                acc_id = meta["account_id"]

                log_db = SessionLocal()

                try:
                    if isinstance(result, Exception):
                        log(
                            db=log_db,
                            account_id=acc_id,
                            level="ERROR",
                            category="COPY",
                            message=f"Failed closing {role} {ticket}: {str(result)}"
                        )

                    elif result.get("success"):
                        log(
                            db=log_db,
                            account_id=acc_id,
                            level="TRADE",
                            category="COPY",
                            message=f"Closed {role.upper()} trade {ticket}"
                        )

                    else:
                        log(
                            db=log_db,
                            account_id=acc_id,
                            level="ERROR",
                            category="COPY",
                            message=f"Failed closing {role} {ticket}: {result.get('error')}"
                        )

                finally:
                    log_db.close()

            # =========================
            # STEP 5: UPDATE DB STATUS
            # =========================
            write_db = SessionLocal()

            try:
                write_links = write_db.query(CopyTradeLink).filter(
                    CopyTradeLink.master_ticket == master_ticket,
                    CopyTradeLink.status == "open"
                ).all()

                for l in write_links:
                    l.status = "closed"
                    l.closed_at = datetime.utcnow()

                write_db.commit()

            except Exception:
                write_db.rollback()
                raise

            finally:
                write_db.close()

        except Exception as e:
            log_db = SessionLocal()

            try:
                log(
                    db=log_db,
                    account_id=account_id,
                    level="ERROR",
                    category="SYSTEM",
                    message=f"handle_close_trade error: {str(e)}"
                )

            finally:
                log_db.close()

        finally:
            try:
                db.close()
            except Exception:
                pass

            self._processing.discard(key)

    async def handle_modify_trade(
        self,
        account_id: int,
        ticket: str,
        new_sl: float,
        new_tp: float
    ):
        key = f"modify:{account_id}:{ticket}"
        db = SessionLocal()

        try:
            if key in self._processing:
                return

            self._processing.add(key)

            # =========================
            # STEP 1: DB READ ONLY
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
            origin_is_master = account_id == link.master_account_id

            group_links = db.query(CopyTradeLink).filter(
                CopyTradeLink.master_ticket == master_ticket,
                CopyTradeLink.status == "open"
            ).all()

            if not group_links:
                return

            # preload accounts
            account_ids = set()

            for l in group_links:
                if l.slave_account_id:
                    account_ids.add(l.slave_account_id)

            account_ids.add(link.master_account_id)

            accounts = {
                acc.id: acc
                for acc in db.query(TradingAccount).filter(
                    TradingAccount.id.in_(account_ids)
                ).all()
            }

            # preload relationships
            relationships = {
                (r.master_account_id, r.slave_account_id): r
                for r in db.query(CopyRelationship).filter(
                    CopyRelationship.master_account_id == link.master_account_id
                ).all()
            }

            # close DB early
            db.close()

            # =========================
            # STEP 2: BUILD MODIFY TASKS
            # =========================
            execution_tasks = []
            task_meta = []

            # --------------------------------
            # MODIFY MASTER (if triggered by slave)
            # --------------------------------
            if not origin_is_master:
                master_acc = accounts.get(link.master_account_id)

                if master_acc:
                    rel = relationships.get(
                        (link.master_account_id, link.slave_account_id)
                    )

                    master_sl = new_sl
                    master_tp = new_tp

                    if rel and rel.copy_direction == "opposite":
                        master_sl = new_tp
                        master_tp = new_sl

                    execution_tasks.append(
                        asyncio.wait_for(
                            trader_listener.modify_position(
                                master_acc.metaapi_account_id,
                                link.master_ticket,
                                master_sl,
                                master_tp
                            ),
                            timeout=15
                        )
                    )

                    task_meta.append({
                        "role": "master",
                        "ticket": link.master_ticket,
                        "account_id": master_acc.id
                    })

            # --------------------------------
            # MODIFY SLAVES
            # --------------------------------
            for l in group_links:

                # skip origin slave
                if not origin_is_master:
                    if (
                        l.slave_account_id == account_id and
                        l.slave_ticket == ticket
                    ):
                        continue

                slave_acc = accounts.get(l.slave_account_id)

                if not slave_acc:
                    continue

                rel = relationships.get(
                    (l.master_account_id, l.slave_account_id)
                )

                if not rel:
                    continue

                final_sl = new_sl
                final_tp = new_tp

                if rel.copy_direction == "opposite":
                    final_sl = new_tp
                    final_tp = new_sl

                execution_tasks.append(
                    asyncio.wait_for(
                        trader_listener.modify_position(
                            slave_acc.metaapi_account_id,
                            l.slave_ticket,
                            final_sl,
                            final_tp
                        ),
                        timeout=15
                    )
                )

                task_meta.append({
                    "role": "slave",
                    "ticket": l.slave_ticket,
                    "account_id": l.slave_account_id
                })

            # =========================
            # STEP 3: EXECUTE ALL MODIFIES IN PARALLEL
            # =========================
            results = []

            if execution_tasks:
                results = await asyncio.gather(
                    *execution_tasks,
                    return_exceptions=True
                )

            # =========================
            # STEP 4: LOG RESULTS
            # =========================
            for meta, result in zip(task_meta, results):
                role = meta["role"]
                ticket_id = meta["ticket"]
                acc_id = meta["account_id"]

                log_db = SessionLocal()

                try:
                    if isinstance(result, Exception):
                        log(
                            db=log_db,
                            account_id=acc_id,
                            level="ERROR",
                            category="MODIFY",
                            message=f"{role.capitalize()} modify failed for {ticket_id}: {str(result)}"
                        )

                    elif result.get("success"):
                        log(
                            db=log_db,
                            account_id=acc_id,
                            level="TRADE",
                            category="MODIFY",
                            message=f"Modified SL/TP for {ticket_id}"
                        )

                    else:
                        log(
                            db=log_db,
                            account_id=acc_id,
                            level="ERROR",
                            category="MODIFY",
                            message=f"{role.capitalize()} modify failed for {ticket_id}: {result.get('error')}"
                        )

                finally:
                    log_db.close()

        except Exception as e:
            log_db = SessionLocal()

            try:
                log(
                    db=log_db,
                    account_id=account_id,
                    level="ERROR",
                    category="SYSTEM",
                    message=f"handle_modify_trade error: {str(e)}"
                )

            finally:
                log_db.close()

        finally:
            try:
                db.close()
            except Exception:
                pass

            self._processing.discard(key)




# Singleton
copy_engine = CopyEngine()
