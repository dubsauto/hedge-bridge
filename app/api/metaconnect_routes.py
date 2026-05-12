# app/api/metaconnect_routes.py
from fastapi import APIRouter, Depends, HTTPException
from fastapi.security import HTTPAuthorizationCredentials
from sqlalchemy.orm import Session
from fastapi.responses import FileResponse
from jose import jwt, JWTError
from datetime import datetime

from app.auth import SECRET_KEY, ALGORITHM, security, get_current_user
from app.database import get_db
from app.model import TradingAccount, User, CopyRelationship, BotLog, CopyTradeLink, AccountLot
from app.services.logger import log
from app.services.account_management import account_manager   
from app.services.trading import trader
from hedgebridge.rpc_pool import rpc_pool

router = APIRouter(prefix="/mt5", tags=["MT5 Accounts"])


@router.get("/accounts")
async def get_mt5_accounts(
    credentials: HTTPAuthorizationCredentials = Depends(security),
    db: Session = Depends(get_db)
):
    try:
        # =========================
        # STEP 1: AUTH
        # =========================
        payload = jwt.decode(credentials.credentials, SECRET_KEY, algorithms=[ALGORITHM])
        user_id = payload.get("user_id")

        # =========================
        # STEP 2: DB WORK ONLY
        # =========================
        accounts = db.query(TradingAccount).filter(
            TradingAccount.owner_user_id == user_id
        ).all()

        account_data = []

        for acc in accounts:

            slave_count = db.query(CopyRelationship).filter(
                CopyRelationship.master_account_id == acc.id
            ).count()

            role = "master" if slave_count > 0 else "none"
            slave_count = slave_count - 1 if slave_count > 0 else 0

            as_slave_rel = db.query(CopyRelationship).filter(
                CopyRelationship.slave_account_id == acc.id
            ).first()

            master_account_id = None
            master_name = None
            copy_direction = "same"
            strict_mode = False

            if as_slave_rel:
                role = "slave"
                master_account_id = as_slave_rel.master_account_id
                copy_direction = as_slave_rel.copy_direction
                strict_mode = as_slave_rel.strict_mode

                master = db.query(TradingAccount).filter(
                    TradingAccount.id == master_account_id
                ).first()

                if master:
                    master_name = master.name

            account_data.append({
                "id": acc.id,
                "db_id": acc.id,
                "name": acc.name,
                "login": acc.login,
                "server": acc.server,
                "state": acc.state,
                "magic": acc.magic,
                "online": acc.connection_status == "connected",
                "listener_active": acc.listener_active or False,   # ✅ NEW
                "metaapi_account_id": acc.metaapi_account_id,
                "copy_role": role,
                "master_account_id": master_account_id,
                "master_name": master_name,
                "slave_count": slave_count,
                "copy_direction": copy_direction,
                "strict_mode": strict_mode
            })

        # =========================
        # STEP 3: CLOSE DB EARLY
        # =========================
        db.close()

        # =========================
        # STEP 4: ASYNC METRICS (CLEAN + FAST)
        # =========================
        import asyncio
        async def fetch_metrics(acc):
            try:
                deployed = (
                    acc["state"].lower() == "deployed"
                    and acc["online"]
                    and acc["metaapi_account_id"]
                )

                if not deployed:
                    return {}

                return await asyncio.wait_for(
                    account_manager.get_account_metrics(acc["metaapi_account_id"]),
                    timeout=6
                )

            except BaseException:   # catches CancelledError
                return {}

        tasks = [fetch_metrics(acc) for acc in account_data]

        results = await asyncio.gather(*tasks, return_exceptions=True)

        # =========================
        # STEP 5: BUILD RESPONSE
        # =========================
        account_list = []

        for acc, metrics in zip(account_data, results):
            metrics = metrics if isinstance(metrics, dict) else {}

            account_list.append({
                **acc,

                # CORE METRICS
                "balance": metrics.get("balance", "N/A"),
                "equity": metrics.get("equity", "N/A"),
                "latency_ms": metrics.get("latency_ms", "N/A"),

                # 🔥 NEW FIELDS
                "positions_count": metrics.get("positions_count", 0),
                "dedicated_ip": metrics.get("dedicated_ip"),
            })

        return {"accounts": account_list}

    except JWTError:
        raise HTTPException(status_code=401, detail="Invalid token")

    except Exception as e:
        print(f"Error fetching accounts: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")
    

# =========================
# DEPLOY MT5 ACCOUNT
# =========================
@router.post("/accounts/{account_id}/deploy")
async def deploy_mt5_account(
    account_id: int,
    payload: dict = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    try:
        user_id = payload.get("user_id")
        if not user_id:
            raise HTTPException(status_code=401, detail="Invalid token payload")

        trading_account = db.query(TradingAccount).filter(
            TradingAccount.id == account_id,
            TradingAccount.owner_user_id == user_id
        ).first()

        if not trading_account:
            raise HTTPException(status_code=404, detail="MT5 account not found or you don't own it")

        if not trading_account.metaapi_account_id:
            raise HTTPException(status_code=400, detail="This account is not linked to MetaAPI yet")

        # =========================
        # LOG INTENT
        # =========================
        log(db=db,
            account_id=account_id,
            level="INFO",
            category="SYSTEM",
            message="Deploy request initiated"
        )

        if trading_account.metaapi_account_id:
            await rpc_pool.invalidate(trading_account.metaapi_account_id)

        result = await account_manager.deploy(trading_account.metaapi_account_id)
        print(result)

        if result.get("success"):
            trading_account.state = "deployed"
            trading_account.connection_status = "connected"
            trading_account.last_connected_at = datetime.utcnow()
            db.commit()

            # ✅ SUCCESS LOG
            log(db=db,
                account_id=account_id,
                level="INFO",
                category="SYSTEM",
                message="Account deployed successfully",
                raw_json=result
            )
        else:
            # ❌ API FAILURE LOG
            log(db=db,
                account_id=account_id,
                level="ERROR",
                category="SYSTEM",
                message=f"Deploy failed: {result.get('error')}",
                raw_json=result
            )

        return result

    except HTTPException as e:
        raise e

    except Exception as e:
        print(f"❌ Deploy error for account {account_id}: {e}")

        # 🔴 CRITICAL ERROR LOG
        log(db=db,
            account_id=account_id,
            level="ERROR",
            category="SYSTEM",
            message=f"Deploy exception: {str(e)}",
        )

        raise HTTPException(status_code=500, detail=f"Failed to deploy: {str(e)}")
    
# =========================
# UNDEPLOY MT5 ACCOUNT
# =========================
@router.post("/accounts/{account_id}/undeploy")
async def undeploy_mt5_account(
    account_id: int,
    payload: dict = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    try:
        user_id = payload.get("user_id")
        if not user_id:
            raise HTTPException(status_code=401, detail="Invalid token payload")

        trading_account = db.query(TradingAccount).filter(
            TradingAccount.id == account_id,
            TradingAccount.owner_user_id == user_id
        ).first()

        if not trading_account:
            raise HTTPException(status_code=404, detail="MT5 account not found or you don't own it")

        if not trading_account.metaapi_account_id:
            raise HTTPException(status_code=400, detail="This account is not linked to MetaAPI yet")

        # =========================
        # LOG INTENT
        # =========================
        log(db=db,
            account_id=account_id,
            level="INFO",
            category="SYSTEM",
            message="Undeploy request initiated",
        )

        result = await account_manager.undeploy(trading_account.metaapi_account_id)

        if result.get("success"):
            trading_account.state = "undeployed"
            trading_account.connection_status = "disconnected"
            db.commit()
            # ✅ Evict the now-dead connection from the pool
            await rpc_pool.invalidate(trading_account.metaapi_account_id)

            # ✅ SUCCESS LOG
            log(db=db,
                account_id=account_id,
                level="INFO",
                category="SYSTEM",
                message="Account undeployed successfully",
                raw_json=result
            )
        else:
            # ❌ API FAILURE LOG
            log(db=db,
                account_id=account_id,
                level="ERROR",
                category="SYSTEM",
                message=f"Undeploy failed: {result.get('error')}",
                raw_json=result
            )

        return result

    except HTTPException as e:
        raise e

    except Exception as e:
        print(f"❌ Undeploy error for account {account_id}: {e}")

        # 🔴 CRITICAL ERROR LOG
        log(db=db,
            account_id=account_id,
            level="ERROR",
            category="SYSTEM",
            message=f"Undeploy exception: {str(e)}",
        )

        raise HTTPException(status_code=500, detail=f"Failed to undeploy: {str(e)}")
    

@router.post("/accounts")
async def create_mt5_account(
    data: dict,
    credentials: HTTPAuthorizationCredentials = Depends(security),
    db: Session = Depends(get_db)
):
    try:
        payload = jwt.decode(credentials.credentials, SECRET_KEY, algorithms=[ALGORITHM])
        user_id = payload.get("user_id")

        # 1. Call MetaApi to add / get account
        result = await account_manager.add_account(
            name=data.get("name"),
            server=data.get("server"),
            login=str(data.get("login")),
            password=data.get("password"),
            manual_trades=data.get("manual_trades", True),
            use_dedicated_ip=data.get("use_dedicated_ip", True),
            magic=data.get("magic", 0)
        )

        if not result.get("success"):
            raise HTTPException(status_code=400, detail=result.get("message"))

        metaapi_account_id = result.get("account_id")

        # 2. Check if this account already exists in our database
        existing = db.query(TradingAccount).filter(
            TradingAccount.login == int(data.get("login")),
            TradingAccount.owner_user_id == user_id
        ).first()

        if existing:
            # Update existing record
            existing.name = data.get("name")
            existing.server = data.get("server")
            existing.password = data.get("password")
            existing.manual_trades = data.get("manual_trades", True)
            existing.use_dedicated_ip = data.get("use_dedicated_ip", True)
            existing.magic = data.get("magic", 0)
            existing.metaapi_account_id = metaapi_account_id
            existing.state = "created"
            db.commit()
            return {"message": "Account updated successfully", "account_id": existing.id}

        # 3. Create new record in database
        new_account = TradingAccount(
            owner_user_id=user_id,
            name=data.get("name"),
            login=int(data.get("login")),
            password=data.get("password"),
            server=data.get("server"),
            magic=data.get("magic", 0),
            manual_trades=data.get("manual_trades", True),
            use_dedicated_ip=data.get("use_dedicated_ip", True),
            metaapi_account_id=metaapi_account_id,
            state="created"
        )

        db.add(new_account)
        db.commit()
        db.refresh(new_account)

        return {
            "message": "MT5 account added successfully",
            "account_id": new_account.id,
            "metaapi_account_id": metaapi_account_id
        }

    except JWTError:
        raise HTTPException(status_code=401, detail="Invalid token")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal error: {str(e)}")


@router.put("/accounts/{account_id}")
async def update_mt5_account(
    account_id: int,
    data: dict,
    credentials: HTTPAuthorizationCredentials = Depends(security),
    db: Session = Depends(get_db)
):
    try:
        payload = jwt.decode(credentials.credentials, SECRET_KEY, algorithms=[ALGORITHM])
        user_id = payload.get("user_id")

        account = db.query(TradingAccount).filter(
            TradingAccount.id == account_id,
            TradingAccount.owner_user_id == user_id
        ).first()

        if not account:
            raise HTTPException(status_code=404, detail="Account not found")

        # =========================
        # 1. UPDATE METAAPI FIRST
        # =========================
        if account.metaapi_account_id:
            update_data = {}

            if data.get("name"):
                update_data["name"] = data.get("name")

            if data.get("server"):
                update_data["server"] = data.get("server")

            if data.get("password"):
                update_data["password"] = data.get("password")

            # Optional fields
            update_data["manualTrades"] = data.get("manual_trades", account.manual_trades)
            update_data["magic"] = data.get("magic", account.magic)

            result = await account_manager.update_account(
                account.metaapi_account_id,
                update_data
            )

            if not result.get("success"):
                raise HTTPException(status_code=400, detail=result.get("message"))

        # =========================
        # 2. UPDATE DATABASE
        # =========================
        account.name = data.get("name", account.name)
        account.server = data.get("server", account.server)

        if data.get("password"):
            account.password = data.get("password")

        account.manual_trades = data.get("manual_trades", account.manual_trades)
        account.use_dedicated_ip = data.get("use_dedicated_ip", account.use_dedicated_ip)
        account.magic = data.get("magic", account.magic)

        db.commit()
        db.refresh(account)

        return {
            "message": "Account updated successfully",
            "account_id": account.id
        }

    except JWTError:
        raise HTTPException(status_code=401, detail="Invalid token")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal error: {str(e)}")


@router.delete("/accounts/{account_id}")
async def delete_mt5_account(
    account_id: int,
    credentials: HTTPAuthorizationCredentials = Depends(security),
    db: Session = Depends(get_db)
):
    try:
        payload = jwt.decode(credentials.credentials, SECRET_KEY, algorithms=[ALGORITHM])
        user_id = payload.get("user_id")

        # =========================
        # 1. FIND ACCOUNT
        # =========================
        account = db.query(TradingAccount).filter(
            TradingAccount.id == account_id,
            TradingAccount.owner_user_id == user_id
        ).first()

        if not account:
            raise HTTPException(status_code=404, detail="Account not found")

        meta_id = account.metaapi_account_id

        print(f"🗑 Deleting account {account.id}")

        # =========================
        # 2. UNDEPLOY + REMOVE METAAPI ACCOUNT
        # =========================
        if meta_id:
            try:
                # force undeploy first (safe cleanup)
                await account_manager.undeploy(meta_id)

                # then remove account
                result = await account_manager.remove_account(meta_id)

                if not result.get("success"):
                    raise HTTPException(
                        status_code=400,
                        detail=result.get("message", "MetaAPI deletion failed")
                    )

            except Exception as e:
                print(f"⚠ MetaAPI cleanup warning: {e}")
                # continue deletion anyway (do not block DB cleanup)

        # =========================
        # 3. DELETE COPY RELATIONSHIPS
        # =========================
        db.query(CopyRelationship).filter(
            (CopyRelationship.master_account_id == account_id) |
            (CopyRelationship.slave_account_id == account_id)
        ).delete(synchronize_session=False)

        # =========================
        # 4. DELETE COPY TRADE LINKS
        # =========================
        db.query(CopyTradeLink).filter(
            (CopyTradeLink.master_account_id == account_id) |
            (CopyTradeLink.slave_account_id == account_id)
        ).delete(synchronize_session=False)

        # =========================
        # 5. DELETE BOT LOGS
        # =========================
        db.query(BotLog).filter(
            BotLog.account_id == account_id
        ).delete(synchronize_session=False)

        # =========================
        # 6. OPTIONAL: USER PERMISSION CLEANUP
        # (only if you tie permissions to accounts in future)
        # =========================
        # db.query(UserPermission).filter(
        #     UserPermission.user_id == account.owner_user_id
        # ).delete(synchronize_session=False)

        # =========================
        # 7. DELETE ACCOUNT ITSELF
        # =========================
        db.delete(account)

        db.commit()

        print(f"✅ Account {account_id} fully deleted (DB + MetaAPI + relations)")

        return {
            "message": "Account and all related data deleted successfully",
            "account_id": account_id
        }

    except JWTError:
        raise HTTPException(status_code=401, detail="Invalid token")

    except Exception as e:
        db.rollback()
        print(f"❌ Delete route error: {e}")
        raise HTTPException(status_code=500, detail=f"Internal error: {str(e)}")
    

# =========================
# SET ACCOUNT ROLE (Master / Slave / None)
# =========================
@router.post("/accounts/{account_id}/role")
async def set_account_role(
    account_id: int,
    data: dict,
    payload: dict = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    try:
        user_id = payload.get("user_id")
        if not user_id:
            raise HTTPException(status_code=401, detail="Invalid token")

        account = db.query(TradingAccount).filter(
            TradingAccount.id == account_id,
            TradingAccount.owner_user_id == user_id
        ).first()

        if not account:
            raise HTTPException(status_code=404, detail="Account not found or not yours")

        role = data.get("role", "none").lower()

        if role not in ["none", "master", "slave"]:
            raise HTTPException(status_code=400, detail="Invalid role. Must be none, master or slave")

        # =========================
        # LOG INTENT
        # =========================
        log(db=db,
            account_id=account_id,
            level="INFO",
            category="ROLE",
            message=f"User setting role → {role}",
            raw_json=data
        )
        # =========================
        # CLEAR EXISTING RELATIONSHIPS
        # =========================
        db.query(CopyRelationship).filter(
            (CopyRelationship.master_account_id == account_id) |
            (CopyRelationship.slave_account_id == account_id)
        ).delete()

        log(db=db,
            account_id=account_id,
            level="INFO",
            category="ROLE",
            message="Cleared existing copy relationships",
        )

        # =========================
        # SLAVE
        # =========================
        if role == "slave":
            master_id = data.get("master_account_id")
            if not master_id:
                raise HTTPException(status_code=400, detail="master_account_id is required when setting slave")

            master = db.query(TradingAccount).filter(
                TradingAccount.id == master_id,
                TradingAccount.owner_user_id == user_id
            ).first()

            if not master:
                raise HTTPException(status_code=404, detail="Master account not found")

            rel = CopyRelationship(
                master_account_id=master_id,
                slave_account_id=account_id,
                copy_direction=data.get("copy_direction", "same"),
                strict_mode=data.get("strict_mode", False),
                is_active=True
            )
            db.add(rel)

            log(db=db,
                account_id=master_id,
                level="INFO",
                category="ROLE",
                message=f"Set as SLAVE → linked to master {master.name}",
            )
            log(db=db,
                account_id=account_id,
                level="INFO",
                category="ROLE",
                message=f"New slave linked → account {account.name} linked to {master.name}",
            )
        # =========================
        # MASTER
        # =========================
        elif role == "master":
            rel = CopyRelationship(
                master_account_id=account_id,
                slave_account_id=None,
                copy_direction="same",
                strict_mode=False,
                is_active=True
            )
            db.add(rel)

            log(db=db,
                account_id=account_id,
                level="INFO",
                category="ROLE",
                message="Set as MASTER"
            )

        # =========================
        # NONE
        # =========================
        else:
            log(db=db,
                account_id=account_id,
                level="INFO",
                category="ROLE",
                message="Role set to NONE (all relationships removed)"
            )

        db.commit()

        # =========================
        # SUCCESS LOG
        # =========================
        log(db=db,
            account_id=account_id,
            level="INFO",
            category="ROLE",
            message=f"Role successfully set to {role}"
        )

        return {"success": True, "message": f"Role successfully set to {role}"}

    except HTTPException as e:
        raise e

    except Exception as e:
        db.rollback()
        print(f"❌ Role update error: {e}")

        # 🔴 CRITICAL ERROR LOG
        log(db=db,
            account_id=account_id,
            level="ERROR",
            category="SYSTEM",
            message=f"Role update error: {str(e)}",
        )

        raise HTTPException(status_code=500, detail=str(e))

# =========================
# UPDATE COPY SETTINGS (Direction + Strict Mode)
# =========================
@router.post("/accounts/{account_id}/copy-settings")
async def update_copy_settings(
    account_id: int,
    data: dict,
    payload: dict = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    try:
        print(f"🔧 Updating copy settings for account {account_id} with data: {data}")
        user_id = payload.get("user_id")

        # =========================
        # OWNERSHIP CHECK
        # =========================
        account = db.query(TradingAccount).filter(
            TradingAccount.id == account_id,
            TradingAccount.owner_user_id == user_id
        ).first()

        if not account:
            raise HTTPException(status_code=403, detail="Not authorized")

        # =========================
        # LOG INTENT
        # =========================
        log(db=db,
            account_id=account_id,
            level="INFO",
            category="COPY_SETTINGS",
            message="User updating copy settings",
            raw_json=data
        )

        # =========================
        # COPY DIRECTION (SLAVE)
        # =========================
        if "copy_direction" in data:
            relationship = db.query(CopyRelationship).filter(
                CopyRelationship.slave_account_id == account_id
            ).first()

            if not relationship:
                log(db=db,
                    account_id=account_id,
                    level="ERROR",
                    category="COPY_SETTINGS",
                    message="No copy relationship found for slave when updating copy_direction",
                    raw_json=data
                )
                raise HTTPException(status_code=404, detail="No active copy relationship found for this slave account")

            direction = str(data["copy_direction"]).lower()
            if direction not in ["same", "opposite"]:
                raise HTTPException(status_code=400, detail="copy_direction must be 'same' or 'opposite'")

            relationship.copy_direction = direction
            log(db=db,
                account_id=relationship.master_account_id,
                level="INFO",
                category="COPY_SETTINGS",
                message=f"Slave {account.name} updated copy direction → {direction}",
                raw_json=data
            )
        # =========================
        # STRICT MODE (MASTER)
        # =========================
        if "strict_mode" in data:
            relationship = db.query(CopyRelationship).filter(
                CopyRelationship.master_account_id == account_id
            ).first()

            if not relationship:
                log(db=db,
                    account_id=account_id,
                    level="ERROR",
                    category="COPY_SETTINGS",
                    message="No copy relationship found for master when updating strict_mode",
                    raw_json=data
                )
                raise HTTPException(status_code=404, detail="No active copy relationship found for this master account")

            relationship.strict_mode = bool(data["strict_mode"])
            log(db=db,
                account_id=relationship.slave_account_id,
                level="INFO",
                category="COPY_SETTINGS",
                message=f"Master {account.name} updated strict mode → {relationship.strict_mode}",
                raw_json=data
            )

        db.commit()

        # =========================
        # SUCCESS LOG
        # =========================
        log(db=db,
            account_id=account_id,
            level="INFO",
            category="COPY_SETTINGS",
            message="Copy settings updated successfully",
        )

        return {"success": True, "message": "Copy settings updated successfully"}

    except HTTPException as e:
        raise e

    except Exception as e:
        db.rollback()
        print(f"❌ Copy settings error: {e}")

        # 🔴 CRITICAL ERROR LOG
        log(db=db,
            account_id=account_id,
            level="ERROR",
            category="SYSTEM",
            message=f"Copy settings error: {str(e)}",
        )

        raise HTTPException(status_code=500, detail=str(e))
    

@router.post("/accounts/{account_id}/trade")
async def quick_trade(
    account_id: int,
    data: dict,
    payload: dict = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    try:
        print(f"⚡ Quick trade request for account {account_id} with data: {data}")
        user_id = payload.get("user_id")

        # =========================
        # VALIDATE ACCOUNT
        # =========================
        account = db.query(TradingAccount).filter(
            TradingAccount.id == account_id,
            TradingAccount.owner_user_id == user_id
        ).first()

        if not account:
            raise HTTPException(status_code=404, detail="Account not found")

        if not account.metaapi_account_id:
            raise HTTPException(status_code=400, detail="Account not connected to MetaAPI")

        if account.state.lower() != "deployed":
            raise HTTPException(status_code=400, detail="Account is not deployed")

        if account.connection_status != "connected":
            raise HTTPException(status_code=400, detail="Account is not connected")

        # =========================
        # INPUTS
        # =========================
        action = data.get("action")
        symbol = data.get("symbol")
        sl_tp_mode = data.get("sl_tp_mode", "price")
        volume = float(data.get("volume", 0))
        fixed_lot_enabled = data.get("fixed_lot_enabled", False)

        if fixed_lot_enabled:
            lot_row = db.query(AccountLot).filter_by(account_id=account_id).first()
            if lot_row:
                volume = lot_row.lot_size

        sl = data.get("sl")
        tp = data.get("tp")

        sl = float(sl) if sl is not None else None
        tp = float(tp) if tp is not None else None

        # =========================
        # BASIC VALIDATION
        # =========================
        if action not in ["buy", "sell"]:
            raise HTTPException(status_code=400, detail="Invalid action (buy/sell only)")

        if not symbol:
            raise HTTPException(status_code=400, detail="Symbol is required")

        if volume <= 0:
            raise HTTPException(status_code=400, detail="Volume must be greater than 0")

        # =========================
        # LOG INTENT
        # =========================
        log(
            db=db,
            account_id=account_id,
            level="INFO",
            category="EXECUTION",
            message=f"Request: {action.upper()} {symbol} {volume}",
            raw_json=data
        )

        # =========================
        # SL/TP PROCESSING
        # =========================
        try:
            # force=True: user-initiated trade — bypass cooldown, never lock them out
            print(f"[Route] rpc_pool id: {id(rpc_pool)}")
            connection = await rpc_pool.get_connection(account.metaapi_account_id, force=True)

            symbol_spec = await connection.get_symbol_specification(symbol)
            symbol_price = await connection.get_symbol_price(symbol)

            if not symbol_spec or not symbol_price:
                raise Exception("Symbol data unavailable")

            point = (
                symbol_spec.get("point")
                or symbol_spec.get("tickSize")
                or symbol_spec.get("pipSize")
            )

            if point is None:
                raise Exception(f"Missing point size in symbol spec: {symbol_spec}")

            point = float(point)
            digits = symbol_spec.get("digits", 5)
            stops_level = symbol_spec.get("stopsLevel", 0)

            bid = symbol_price.get("bid")
            ask = symbol_price.get("ask")

            print("SYMBOL SPEC:", symbol_spec)
            print("POINT:", point)
            print("SL:", sl, "TP:", tp)

            if not bid or not ask:
                raise Exception("Price data unavailable")

            entry_price = ask if action == "buy" else bid

            # -------------------------
            # POINTS MODE
            # -------------------------
            if sl_tp_mode == "points":
                if sl is not None and sl <= 0:
                    raise Exception("SL must be > 0 in points mode")
                if tp is not None and tp <= 0:
                    raise Exception("TP must be > 0 in points mode")
                if sl is not None:
                    sl = entry_price - (sl * point) if action == "buy" else entry_price + (sl * point)
                if tp is not None:
                    tp = entry_price + (tp * point) if action == "buy" else entry_price - (tp * point)

            # -------------------------
            # PRICE MODE
            # -------------------------
            elif sl_tp_mode == "price":
                pass

            else:
                raise Exception("Invalid SL/TP mode")

            # -------------------------
            # ROUND VALUES
            # -------------------------
            if sl is not None:
                sl = round(sl, digits)
            if tp is not None:
                tp = round(tp, digits)

            # -------------------------
            # BROKER STOP LEVEL CHECK
            # -------------------------
            min_distance = stops_level * point

            if sl is not None and abs(entry_price - sl) < min_distance:
                raise Exception(f"SL too close (min distance: {round(min_distance, digits)})")
            if tp is not None and abs(entry_price - tp) < min_distance:
                raise Exception(f"TP too close (min distance: {round(min_distance, digits)})")

            print(f"🎯 Final SL/TP → SL={sl}, TP={tp}, mode={sl_tp_mode}")

        except Exception as e:
            log(
                db=db,
                account_id=account_id,
                level="ERROR",
                category="VALIDATION",
                message=f"SL/TP error: {str(e)}",
                raw_json=data
            )
            raise HTTPException(status_code=400, detail=str(e))

        # =========================
        # MAGIC RULE
        # =========================
        magic = account.magic if not account.manual_trades else 0

        # =========================
        # EXECUTE TRADE
        # =========================
        if action == "buy":
            result = await trader.buy(
                account.metaapi_account_id,
                symbol, volume, sl, tp,
                comment="QuickTrade",
                magic=magic
            )
        else:
            result = await trader.sell(
                account.metaapi_account_id,
                symbol, volume, sl, tp,
                comment="QuickTrade",
                magic=magic
            )

        # =========================
        # HANDLE FAILURE
        # =========================
        if not result.get("success"):
            error_msg = result.get("error", "Unknown execution error")
            log(
                db=db,
                account_id=account_id,
                level="ERROR",
                category="EXECUTION",
                message=f"{action.upper()} failed: {error_msg}",
                raw_json=result
            )
            raise HTTPException(status_code=500, detail=error_msg)

        # =========================
        # SUCCESS
        # =========================
        log(
            db=db,
            account_id=account_id,
            level="TRADE",
            category="EXECUTION",
            message=f"{action.upper()} executed {symbol} {volume}",
            raw_json=result.get("result")
        )

        return {
            "success": True,
            "message": f"{action.upper()} order placed",
            "data": result.get("result")
        }

    except HTTPException as e:
        log(
            db=db,
            account_id=account_id,
            level="ERROR",
            category="VALIDATION",
            message=f"HTTP error: {e.detail}",
            raw_json=data
        )
        raise e

    except Exception as e:
        print(f"❌ Trade error: {e}")
        log(
            db=db,
            account_id=account_id,
            level="ERROR",
            category="EXECUTION",
            message=f"Trade exception: {str(e)}",
            raw_json=data
        )
        raise HTTPException(status_code=500, detail="Internal server error")


# =========================
# CLOSE POSITION
# =========================
@router.post("/accounts/{account_id}/close-position")
async def close_position(
    account_id: int,
    data: dict,
    payload: dict = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    try:
        user_id = payload.get("user_id")
        position_id = data.get("position_id")

        if not position_id:
            raise HTTPException(status_code=400, detail="position_id is required")

        account = db.query(TradingAccount).filter(
            TradingAccount.id == account_id,
            TradingAccount.owner_user_id == user_id
        ).first()

        if not account:
            raise HTTPException(status_code=404, detail="Account not found")

        if not account.metaapi_account_id:
            raise HTTPException(status_code=400, detail="Account not connected")

        result = await trader.close_position(
            account.metaapi_account_id,
            position_id
        )

        if not result.get("success"):
            raise HTTPException(status_code=500, detail=result.get("error"))

        log(
            db=db,
            account_id=account_id,
            level="TRADE",
            message=f"Closed position {position_id}",
            category="EXECUTION",
            raw_json=result.get("result")
        )
        db.commit()

        return {
            "success": True,
            "message": f"Position {position_id} closed",
            "data": result.get("result")
        }

    except HTTPException:
        raise
    except Exception as e:
        print(f"❌ Close position error: {e}")
        log(
            db=db,
            account_id=account_id,
            level="ERROR",
            category="EXECUTION",
            message=f"Failed to close position {position_id}: {str(e)}",
        )
        db.commit()
        raise HTTPException(status_code=500, detail=str(e))
    
@router.get("/accounts/{account_id}/logs")
def get_logs(account_id: int, db: Session = Depends(get_db)):

    # 🔒 CRITICAL: validate ownership
    account = db.query(TradingAccount).filter(
        TradingAccount.id == account_id,
    ).first()

    if not account:
        raise HTTPException(status_code=403, detail="Unauthorized")

    logs = db.query(BotLog)\
        .filter(BotLog.account_id == account_id)\
        .order_by(BotLog.id.desc())\
        .limit(50)\
        .all()

    return logs

@router.get("/accounts/{account_id}/logs")
def get_logs(
    account_id: int,
    db: Session = Depends(get_db)
):
    # 🔒 Validate account ownership
    account = db.query(TradingAccount).filter(
        TradingAccount.id == account_id
    ).first()

    if not account:
        raise HTTPException(status_code=403, detail="Unauthorized")

    logs = (
        db.query(BotLog)
        .filter(BotLog.account_id == account_id)
        .order_by(BotLog.timestamp.asc())      # Most recent first
        .limit(50)
        .all()
    )

    return logs

# =========================
# GET OPEN POSITIONS
# =========================
@router.get("/accounts/{account_id}/positions")
async def get_positions(
    account_id: int,
    payload: dict = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    user_id = payload.get("user_id")

    try:
        # =========================
        # STEP 1: DB ONLY
        # =========================
        account = db.query(TradingAccount).filter(
            TradingAccount.id == account_id,
            TradingAccount.owner_user_id == user_id
        ).first()

        if not account:
            raise HTTPException(status_code=403, detail="Unauthorized")

        deployed = account.state.lower() == "deployed"
        metaapi_account_id = account.metaapi_account_id

        # 🔥 CLOSE DB EARLY
        db.close()

        # =========================
        # STEP 2: EXTERNAL CALLS
        # =========================
        if not deployed or not metaapi_account_id:
            return {"success": True, "positions": []}

        import asyncio

        # Add timeout to prevent hanging forever
        connection = await asyncio.wait_for(
            rpc_pool.get_connection(account.metaapi_account_id),
            timeout=20
        )

        positions = await asyncio.wait_for(
            connection.get_positions(),
            timeout=30
        )
        print(f"✅ Retrieved {len(positions)} positions for account {account_id}")
        return {"success": True, "positions": positions}

    except asyncio.TimeoutError:
        print(f"⏰ Timeout while fetching positions for account {account_id}")
        return {
            "success": False,
            "positions": [],
            "error": "MetaApi not ready (connection timeout)"
        }

    except asyncio.CancelledError:
        print(f"❌ Fetching positions cancelled for account {account_id}")
        return {
            "success": False,
            "positions": [],
            "error": "MetaApi connection was cancelled"
        }
    
    except Exception as e:
        print(f"❌ Error fetching positions for account {account_id}: {e}")
        return {
            "success": False,
            "positions": [],
            "error": str(e)
        }