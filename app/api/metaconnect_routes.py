# app/api/metaconnect_routes.py
from fastapi import APIRouter, Depends, HTTPException
from fastapi.security import HTTPAuthorizationCredentials
from sqlalchemy.orm import Session
from fastapi.responses import FileResponse
from jose import jwt, JWTError
from datetime import datetime

from app.auth import SECRET_KEY, ALGORITHM, security, get_current_user
from app.database import get_db
from app.model import TradingAccount, User, CopyRelationship, BotLog
from app.services.logger import log
from hedgebridge.account_management import account_manager   
from hedgebridge.trading import trader

router = APIRouter(prefix="/mt5", tags=["MT5 Accounts"])



@router.get("/accounts")
async def get_mt5_accounts(
    credentials: HTTPAuthorizationCredentials = Depends(security),
    db: Session = Depends(get_db)
):
    try:
        payload = jwt.decode(credentials.credentials, SECRET_KEY, algorithms=[ALGORITHM])
        user_id = payload.get("user_id")

        # Get all user accounts
        accounts = db.query(TradingAccount).filter(
            TradingAccount.owner_user_id == user_id
        ).all()

        account_list = []

        for acc in accounts:
            # Determine role from CopyRelationship
            # Determine role from CopyRelationship
            as_master = db.query(CopyRelationship).filter(
                CopyRelationship.master_account_id == acc.id,
                CopyRelationship.slave_account_id.is_(None)   # or just check existence
            ).first()

            as_slave_rel = db.query(CopyRelationship).filter(
                CopyRelationship.slave_account_id == acc.id
            ).first()

            role = "none"
            master_account_id = None
            copy_direction = "same"
            strict_mode = False

            if as_master:
                role = "master"
            elif as_slave_rel:
                role = "slave"
                master_account_id = as_slave_rel.master_account_id
                copy_direction = as_slave_rel.copy_direction
                strict_mode = as_slave_rel.strict_mode

            account_list.append({
                "id": acc.id,
                "name": acc.name,
                "login": acc.login,
                "server": acc.server,
                "magic": acc.magic,
                "manual_trades": acc.manual_trades,
                "use_dedicated_ip": acc.use_dedicated_ip,
                "metaapi_account_id": acc.metaapi_account_id,
                "state": acc.state,
                "online": acc.connection_status == "connected",
                "copy_role": role,
                "master_account_id": master_account_id,
                "copy_direction": copy_direction,
                "strict_mode": strict_mode
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

        result = await account_manager.deploy(trading_account.metaapi_account_id)

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

        print(f"🗑 Deleting account {account.id} (MetaApi: {account.metaapi_account_id})")

        # =========================
        # 2. DELETE FROM METAAPI FIRST
        # =========================
        if account.metaapi_account_id:
            result = await account_manager.remove_account(account.metaapi_account_id)

            if not result.get("success"):
                raise HTTPException(
                    status_code=400,
                    detail=result.get("message")
                )

        # =========================
        # 3. DELETE FROM DATABASE
        # =========================
        db.delete(account)
        db.commit()

        print(f"✅ Account {account.id} deleted from DB")

        return {
            "message": "Account deleted successfully",
            "account_id": account.id
        }

    except JWTError:
        raise HTTPException(status_code=401, detail="Invalid token")

    except Exception as e:
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
                message=f"Set as SLAVE → linked to master {master_id}",
            )
            log(db=db,
                account_id=account_id,
                level="INFO",
                category="ROLE",
                message=f"New slave linked → account {account_id}",
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
                message=f"Slave {account_id} updated copy direction → {direction}",
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
                message=f"Master {account_id} updated strict mode → {relationship.strict_mode}",
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
    

# =========================
# QUICK TRADE (BUY / SELL)
# =========================
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

        if account.connection_status != "connected":
            raise HTTPException(status_code=400, detail="Account is not connected")

        # =========================
        # INPUTS
        # =========================
        action = data.get("action")
        symbol = data.get("symbol")
        volume = float(data.get("volume", 0))
        sl = data.get("sl")
        tp = data.get("tp")

        if action not in ["buy", "sell"]:
            raise HTTPException(status_code=400, detail="Invalid action (buy/sell only)")

        if not symbol or volume <= 0:
            raise HTTPException(status_code=400, detail="Invalid symbol or volume")

        # =========================
        # LOG INTENT
        # =========================
        
        log(db=db,
            account_id=account_id,
            level="INFO",
            category="EXECUTION",
            message=f"User requested {action.upper()} {symbol} {volume}",
            raw_json=data
        )

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
                symbol,
                volume,
                sl,
                tp,
                comment="QuickTrade",
                magic=magic
            )
        else:
            result = await trader.sell(
                account.metaapi_account_id,
                symbol,
                volume,
                sl,
                tp,
                comment="QuickTrade",
                magic=magic
            )

        # =========================
        # RESPONSE
        # =========================
        if not result.get("success"):
            log(
                db=db,
                account_id=account_id,
                level="ERROR",
                category="EXECUTION",
                message=f"{action.upper()} failed: {result.get('error')}"
            )

            raise HTTPException(status_code=500, detail=result.get("error"))

        # ✅ SUCCESS LOG
        log(db=db,
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
        raise e

    except Exception as e:
        print(f"❌ Trade error: {e}")

        # 🔴 CRITICAL ERROR LOG
        log(db=db,
            account_id=account_id,
            level="ERROR",
            category="EXECUTION",
            message=f"Trade exception: {str(e)}",
        )

        raise HTTPException(status_code=500, detail=str(e))
    
# =========================
# CLOSE TRADE
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

        # =========================
        # VALIDATE ACCOUNT OWNERSHIP
        # =========================
        account = db.query(TradingAccount).filter(
            TradingAccount.id == account_id,
            TradingAccount.owner_user_id == user_id
        ).first()

        if not account:
            raise HTTPException(status_code=404, detail="Account not found")

        if not account.metaapi_account_id:
            raise HTTPException(status_code=400, detail="Account not connected")

        # =========================
        # EXECUTE CLOSE
        # =========================
        result = await trader.close_position(
            account.metaapi_account_id,
            position_id
        )

        if not result.get("success"):
            raise HTTPException(status_code=500, detail=result.get("error"))

        # =========================
        # LOG (VERY IMPORTANT)
        # =========================
        log(db=db,
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
        #log_error(db, account_id, f"Failed to close position {position_id}: {str(e)}")
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

    account = db.query(TradingAccount).filter(
        TradingAccount.id == account_id,
        TradingAccount.owner_user_id == user_id
    ).first()

    if not account:
        raise HTTPException(status_code=403, detail="Unauthorized")

    try:
        connection = await trader._get_connection(account.metaapi_account_id)
        positions = await connection.get_positions()

        return {"success": True, "positions": positions}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))