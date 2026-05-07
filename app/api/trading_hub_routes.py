# app/api/trading_hub_routes.py

from fastapi import APIRouter, Depends, HTTPException
from fastapi.security import HTTPAuthorizationCredentials
from sqlalchemy.orm import Session
from jose import jwt, JWTError

from app.database import get_db
from app.auth import SECRET_KEY, ALGORITHM, security
from app.model import User, UserPermission, TradingHubState

router = APIRouter(prefix="/trading-hub", tags=["Trading Hub"])


def _get_user_id(credentials: HTTPAuthorizationCredentials) -> int:
    try:
        payload = jwt.decode(credentials.credentials, SECRET_KEY, algorithms=[ALGORITHM])
        user_id = payload.get("user_id")
        if not user_id:
            raise HTTPException(status_code=401, detail="Invalid token payload")
        return user_id
    except JWTError:
        raise HTTPException(status_code=401, detail="Invalid token")


@router.get("/state")
def get_state(
    credentials: HTTPAuthorizationCredentials = Depends(security),
    db: Session = Depends(get_db)
):
    user_id = _get_user_id(credentials)

    perm = db.query(UserPermission).filter_by(user_id=user_id).first()
    if perm and perm.can_use_trading_hub is False:
        raise HTTPException(status_code=403, detail="Trading Hub access disabled")

    row = db.query(TradingHubState).filter_by(user_id=user_id).first()
    if not row:
        return {"blocks": [], "notes": [], "tradare": ""}
    return row.data or {"blocks": [], "notes": [], "tradare": ""}


@router.put("/state")
def save_state(
    payload: dict,
    credentials: HTTPAuthorizationCredentials = Depends(security),
    db: Session = Depends(get_db)
):
    user_id = _get_user_id(credentials)

    perm = db.query(UserPermission).filter_by(user_id=user_id).first()
    if perm and perm.can_use_trading_hub is False:
        raise HTTPException(status_code=403, detail="Trading Hub access disabled")

    row = db.query(TradingHubState).filter_by(user_id=user_id).first()
    if row:
        row.data = payload
    else:
        row = TradingHubState(user_id=user_id, data=payload)
        db.add(row)

    db.commit()
    return {"status": "ok"}
