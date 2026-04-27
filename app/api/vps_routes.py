# app/api/vps_routes.py
from fastapi import APIRouter, Depends, HTTPException
from fastapi.security import HTTPAuthorizationCredentials
from sqlalchemy.orm import Session
from jose import jwt, JWTError
from datetime import datetime
from pydantic import BaseModel
from typing import Optional

from app.auth import SECRET_KEY, ALGORITHM, security, get_current_user
from app.database import get_db
from app.model import VpsAccount, TradingAccount
from app.services.guacamole import get_launch_url, GuacamoleError

router = APIRouter(prefix="/vps", tags=["VPS Accounts"])


# ─────────────────────────────────────────────────────────────
# SCHEMAS
# ─────────────────────────────────────────────────────────────

class VpsCreatePayload(BaseModel):
    host: str
    username: str
    password: str
    associated_mt5_id: Optional[int] = None
    auto_connect: bool = True
    protocol: str = "ssh"   # "ssh" | "rdp"
    port: Optional[int] = None


class VpsUpdatePayload(BaseModel):
    host: Optional[str] = None
    username: Optional[str] = None
    password: Optional[str] = None
    associated_mt5_id: Optional[int] = None
    auto_connect: Optional[bool] = None
    protocol: Optional[str] = None
    port: Optional[int] = None


# ─────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────

def _decode(credentials: HTTPAuthorizationCredentials) -> int:
    try:
        payload = jwt.decode(credentials.credentials, SECRET_KEY, algorithms=[ALGORITHM])
        user_id = payload.get("user_id")
        if not user_id:
            raise HTTPException(status_code=401, detail="Invalid token payload")
        return user_id
    except JWTError:
        raise HTTPException(status_code=401, detail="Invalid token")


def _serialize(vps: VpsAccount) -> dict:
    return {
        "id":                vps.id,
        "host":              vps.host,
        "username":          vps.username,
        "protocol":          vps.protocol,
        "port":              vps.port,
        "associated_mt5_id": vps.associated_mt5_id,
        "mt5_name":          vps.associated_mt5.name  if vps.associated_mt5 else None,
        "mt5_login":         vps.associated_mt5.login if vps.associated_mt5 else None,
        "auto_connect":      vps.auto_connect,
        "is_online":         vps.is_online,
        "last_checked_at":   vps.last_checked_at.isoformat() if vps.last_checked_at else None,
        "created_at":        vps.created_at.isoformat(),
    }


# ─────────────────────────────────────────────────────────────
# LIST
# ─────────────────────────────────────────────────────────────

@router.get("/accounts")
async def list_vps_accounts(
    credentials: HTTPAuthorizationCredentials = Depends(security),
    db: Session = Depends(get_db),
):
    user_id = _decode(credentials)
    vps_list = (
        db.query(VpsAccount)
        .filter(VpsAccount.owner_user_id == user_id)
        .order_by(VpsAccount.id)
        .all()
    )
    return {"accounts": [_serialize(v) for v in vps_list]}


# ─────────────────────────────────────────────────────────────
# AVAILABLE MT5 FOR DROPDOWN
# ─────────────────────────────────────────────────────────────

@router.get("/available-mt5")
async def available_mt5_accounts(
    credentials: HTTPAuthorizationCredentials = Depends(security),
    db: Session = Depends(get_db),
):
    user_id = _decode(credentials)
    all_accounts = (
        db.query(TradingAccount)
        .filter(TradingAccount.owner_user_id == user_id)
        .all()
    )
    taken_ids = {
        row.associated_mt5_id
        for row in db.query(VpsAccount.associated_mt5_id)
        .filter(
            VpsAccount.owner_user_id == user_id,
            VpsAccount.associated_mt5_id.isnot(None),
        )
        .all()
    }
    result = [
        {"id": a.id, "name": a.name, "login": a.login}
        for a in all_accounts
        if a.id not in taken_ids
    ]
    return {"accounts": result}


# ─────────────────────────────────────────────────────────────
# CREATE
# ─────────────────────────────────────────────────────────────

@router.post("/accounts")
async def create_vps_account(
    body: VpsCreatePayload,
    credentials: HTTPAuthorizationCredentials = Depends(security),
    db: Session = Depends(get_db),
):
    user_id = _decode(credentials)

    if body.associated_mt5_id:
        mt5 = db.query(TradingAccount).filter(
            TradingAccount.id == body.associated_mt5_id,
            TradingAccount.owner_user_id == user_id,
        ).first()
        if not mt5:
            raise HTTPException(status_code=404, detail="MT5 account not found")
        clash = db.query(VpsAccount).filter(
            VpsAccount.associated_mt5_id == body.associated_mt5_id
        ).first()
        if clash:
            raise HTTPException(status_code=400, detail="That MT5 account is already linked to another VPS entry")

    vps = VpsAccount(
        owner_user_id=user_id,
        host=body.host.strip(),
        username=body.username.strip(),
        password=body.password,
        associated_mt5_id=body.associated_mt5_id,
        auto_connect=body.auto_connect,
        protocol=body.protocol or "ssh",
        port=body.port,
    )
    db.add(vps)
    db.commit()
    db.refresh(vps)
    return {"message": "VPS added successfully", "vps": _serialize(vps)}


# ─────────────────────────────────────────────────────────────
# UPDATE
# ─────────────────────────────────────────────────────────────

@router.put("/accounts/{vps_id}")
async def update_vps_account(
    vps_id: int,
    body: VpsUpdatePayload,
    credentials: HTTPAuthorizationCredentials = Depends(security),
    db: Session = Depends(get_db),
):
    user_id = _decode(credentials)
    vps = db.query(VpsAccount).filter(
        VpsAccount.id == vps_id,
        VpsAccount.owner_user_id == user_id,
    ).first()
    if not vps:
        raise HTTPException(status_code=404, detail="VPS not found")

    if body.host is not None:         vps.host = body.host.strip()
    if body.username is not None:     vps.username = body.username.strip()
    if body.password:                 vps.password = body.password
    if body.auto_connect is not None: vps.auto_connect = body.auto_connect
    if body.protocol is not None:     vps.protocol = body.protocol
    if body.port is not None:         vps.port = body.port

    if body.associated_mt5_id is not None:
        mt5 = db.query(TradingAccount).filter(
            TradingAccount.id == body.associated_mt5_id,
            TradingAccount.owner_user_id == user_id,
        ).first()
        if not mt5:
            raise HTTPException(status_code=404, detail="MT5 account not found")
        clash = db.query(VpsAccount).filter(
            VpsAccount.associated_mt5_id == body.associated_mt5_id,
            VpsAccount.id != vps_id,
        ).first()
        if clash:
            raise HTTPException(status_code=400, detail="That MT5 account is already linked to another VPS entry")
        vps.associated_mt5_id = body.associated_mt5_id
    elif body.associated_mt5_id == 0:
        vps.associated_mt5_id = None

    vps.updated_at = datetime.utcnow()
    db.commit()
    db.refresh(vps)
    return {"message": "VPS updated successfully", "vps": _serialize(vps)}


# ─────────────────────────────────────────────────────────────
# DELETE
# ─────────────────────────────────────────────────────────────

@router.delete("/accounts/{vps_id}")
async def delete_vps_account(
    vps_id: int,
    credentials: HTTPAuthorizationCredentials = Depends(security),
    db: Session = Depends(get_db),
):
    user_id = _decode(credentials)
    vps = db.query(VpsAccount).filter(
        VpsAccount.id == vps_id,
        VpsAccount.owner_user_id == user_id,
    ).first()
    if not vps:
        raise HTTPException(status_code=404, detail="VPS not found")
    db.delete(vps)
    db.commit()
    return {"message": "VPS deleted successfully"}


# ─────────────────────────────────────────────────────────────
# GET BY MT5 (dashboard terminal button lookup)
# ─────────────────────────────────────────────────────────────

@router.get("/accounts/by-mt5/{mt5_account_id}")
async def get_vps_by_mt5(
    mt5_account_id: int,
    credentials: HTTPAuthorizationCredentials = Depends(security),
    db: Session = Depends(get_db),
):
    user_id = _decode(credentials)
    vps = db.query(VpsAccount).filter(
        VpsAccount.associated_mt5_id == mt5_account_id,
        VpsAccount.owner_user_id == user_id,
    ).first()
    if not vps:
        raise HTTPException(status_code=404, detail="No VPS linked to this MT5 account")
    return {"vps": _serialize(vps)}


# ─────────────────────────────────────────────────────────────
# ★ LAUNCH — create a Guacamole session and return a browser URL
# ─────────────────────────────────────────────────────────────

@router.get("/accounts/{vps_id}/launch")
async def launch_vps_terminal(
    vps_id: int,
    credentials: HTTPAuthorizationCredentials = Depends(security),
    db: Session = Depends(get_db),
):
    """
    Called by the dashboard 🖥️ button.
    Returns a Guacamole client URL that opens the remote desktop
    (SSH / RDP) directly in a new browser tab.
    """
    user_id = _decode(credentials)

    vps = db.query(VpsAccount).filter(
        VpsAccount.id == vps_id,
        VpsAccount.owner_user_id == user_id,
    ).first()

    if not vps:
        raise HTTPException(status_code=404, detail="VPS not found")

    try:
        url = await get_launch_url(
            vps_host=vps.host,
            vps_username=vps.username,
            vps_password=vps.password,
            protocol=vps.protocol or "ssh",
            port=vps.port,
        )
    except GuacamoleError as e:
        raise HTTPException(status_code=502, detail=f"Guacamole error: {str(e)}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to create terminal session: {str(e)}")

    return {"url": url}