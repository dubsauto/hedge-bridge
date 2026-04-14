# app/api/dashboard_routes.py
from fastapi import APIRouter, Depends, HTTPException
from fastapi.security import HTTPAuthorizationCredentials
from sqlalchemy.orm import Session
from jose import jwt, JWTError
from app.auth import get_current_user
from hedgebridge.account_management import account_manager 
from app.database import get_db
from app.model import User, UserPermission, TradingAccount
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


