# app/api/dashboard_routes.py
from fastapi import APIRouter, Depends, HTTPException
from fastapi.security import HTTPAuthorizationCredentials
from sqlalchemy.orm import Session
from jose import jwt, JWTError
from app.schemas import AccountLotSchema
from app.database import get_db
from app.model import User, UserPermission, TradingAccount, SymbolMappingEntry, SymbolMappingGroup, AccountLot, CopyTradeSettings
from app.auth import SECRET_KEY, ALGORITHM, security 

router = APIRouter(prefix="/dashboard", tags=["Dashboard"])


# =========================
# DASHBOARD ROUTE (Protected)
# =========================
@router.get("/")
async def dashboard(
    credentials: HTTPAuthorizationCredentials = Depends(security), 
    db: Session = Depends(get_db)
):
    try:
        payload = jwt.decode(credentials.credentials, SECRET_KEY, algorithms=[ALGORITHM])
        user_id = payload.get("user_id")

        if not user_id:
            raise HTTPException(status_code=401, detail="Invalid token payload")

        user = db.query(User).filter(User.id == user_id).first()
        if not user:
            raise HTTPException(status_code=404, detail="User not found")

        # Get can_trade from UserPermission (better than user.can_trade)
        permission = db.query(UserPermission).filter(UserPermission.user_id == user_id).first()
        can_trade = permission.can_trade if permission else False

        return {
            "message": "Welcome to Hedge Bridge Dashboard",
            "username": user.username,
            "role": user.role,
            "approval_status": user.approval_status,
            "can_trade": can_trade
        }

    except JWTError:
        raise HTTPException(status_code=401, detail="Invalid token")
    except Exception as e:
        raise HTTPException(status_code=500, detail="Internal server error")
    

@router.get("/symbol-mapping")
def get_mappings(
    credentials: HTTPAuthorizationCredentials = Depends(security), 
    db: Session = Depends(get_db)):

    payload = jwt.decode(credentials.credentials, SECRET_KEY, algorithms=[ALGORITHM])
    user_id = payload.get("user_id")
    if not user_id:
        raise HTTPException(status_code=401, detail="Invalid token payload")

    groups = db.query(SymbolMappingGroup)\
        .filter_by(owner_user_id=user_id)\
        .all()

    result = []

    for g in groups:
        entries = db.query(SymbolMappingEntry)\
            .filter_by(group_id=g.id)\
            .all()

        symbol_map = {}
        for e in entries:
            symbol_map[str(e.account_id)] = e.symbol

        result.append({
            "group_id": g.id,
            "name": g.name or f"Group {g.id}",   # ← THIS WAS MISSING
            "symbols": symbol_map
        })

    return {"mappings": result}

@router.post("/symbol-mapping/save")
def save_mapping(data: dict, 
    credentials: HTTPAuthorizationCredentials = Depends(security), 
    db: Session = Depends(get_db)):
    
    payload = jwt.decode(credentials.credentials, SECRET_KEY, algorithms=[ALGORITHM])
    user_id = payload.get("user_id")
    if not user_id:
        raise HTTPException(status_code=401, detail="Invalid token payload")

    for group_key, group_data in data.items():
        group_name = group_data.get("name", f"Mapping Group {len(data)}")
        entries = group_data.get("entries", [])

        if group_key.startswith("new_") or group_key == "new":
            group = SymbolMappingGroup(
                owner_user_id=user_id, 
                name=group_name
            )
            db.add(group)
            db.flush()
        else:
            group = db.query(SymbolMappingGroup).filter_by(
                id=int(group_key), 
                owner_user_id=user_id
            ).first()
            if not group:
                continue
            group.name = group_name  # Update name if changed
            db.query(SymbolMappingEntry).filter_by(group_id=group.id).delete()

        for e in entries:
            db.add(SymbolMappingEntry(
                group_id=group.id,
                account_id=e["account_id"],
                symbol=e["symbol"]
            ))

    db.commit()
    return {"status": "ok", "message": "Mappings saved"}


@router.delete("/symbol-mapping/{group_id}")
def delete_mapping_group(group_id: int, 
    credentials: HTTPAuthorizationCredentials = Depends(security), 
    db: Session = Depends(get_db)):
    payload = jwt.decode(credentials.credentials, SECRET_KEY, algorithms=[ALGORITHM])
    user_id = payload.get("user_id")
    if not user_id:
        raise HTTPException(status_code=401, detail="Invalid token payload")
    group = db.query(SymbolMappingGroup).filter_by(id=group_id, owner_user_id=user_id).first()
    if not group:
        raise HTTPException(404, "Group not found")
    db.delete(group)
    db.commit()
    return {"status": "deleted"}

@router.post("/copy-settings")
def update_copy_settings(
    data: dict,
    credentials: HTTPAuthorizationCredentials = Depends(security),
    db: Session = Depends(get_db)
):
    payload = jwt.decode(credentials.credentials, SECRET_KEY, algorithms=[ALGORITHM])
    user_id = payload.get("user_id")

    settings = db.query(CopyTradeSettings).filter_by(user_id=user_id).first()

    if not settings:
        settings = CopyTradeSettings(user_id=user_id)

    # =========================
    # FIXED LOT
    # =========================
    settings.fixed_lot_enabled = data.get("fixed_lot_enabled", settings.fixed_lot_enabled)

    # =========================
    # PIPS OFFSET FEATURE (NEW)
    # =========================
    settings.pips_offset_enabled = data.get(
        "pips_offset_enabled",
        settings.pips_offset_enabled or False
    )

    # only update if provided
    if data.get("pips_offset") is not None:
        settings.pips_offset = int(data.get("pips_offset", settings.pips_offset or 0))

    db.add(settings)
    db.commit()
    db.refresh(settings)

    return {
        "success": True,
        "fixed_lot_enabled": settings.fixed_lot_enabled,
        "pips_offset_enabled": settings.pips_offset_enabled,
        "pips_offset": settings.pips_offset
    }



@router.get("/copy-settings")
def get_copy_settings(
    credentials: HTTPAuthorizationCredentials = Depends(security),
    db: Session = Depends(get_db)
):
    payload = jwt.decode(credentials.credentials, SECRET_KEY, algorithms=[ALGORITHM])
    user_id = payload.get("user_id")

    settings = db.query(CopyTradeSettings).filter_by(user_id=user_id).first()

    if not settings:
        return {
            "fixed_lot_enabled": False,
            "pips_offset_enabled": False,
            "pips_offset": 0
        }

    return {
        "fixed_lot_enabled": settings.fixed_lot_enabled,
        "pips_offset_enabled": settings.pips_offset_enabled,
        "pips_offset": settings.pips_offset
    }


@router.post("/account-lots/save")
def save_account_lots(
    data: list[AccountLotSchema],
    credentials: HTTPAuthorizationCredentials = Depends(security),
    db: Session = Depends(get_db)
):
    print(f"here is the save lot data {data}")

    for row in data:
        account_id = row.account_id
        lot_size = row.lot_size

        existing = db.query(AccountLot).filter_by(account_id=account_id).first()

        if not existing:
            existing = AccountLot(account_id=account_id)

        existing.lot_size = lot_size
        db.add(existing)

    db.commit()
    return {"success": True}


@router.get("/account-lots")
def get_account_lots( 
    credentials: HTTPAuthorizationCredentials = Depends(security), 
    db: Session = Depends(get_db)):
    rows = db.query(AccountLot).all()

    return [
        {"account_id": r.account_id, "lot_size": r.lot_size}
        for r in rows
    ]