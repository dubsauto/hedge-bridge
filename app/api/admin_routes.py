# app/api/admin_routes.py
from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.security import HTTPAuthorizationCredentials
from sqlalchemy.orm import Session
from jose import jwt, JWTError
from datetime import datetime
from app.database import get_db
import time
from app.auth import SECRET_KEY, ALGORITHM, security
from app.auth import hash_password
from app.model import (
    User,
    TradingAccount,
    UserPermission,
    ActivityLog,
    ActiveUser,
    CopyRelationship,
    CopyTradeLink,
    BotLog
)
from app.services.account_management import account_manager


router = APIRouter(prefix="/admin", tags=["Admin"])


# ========================
# ADMIN - PROFILE MANAGEMENT
# ========================

@router.get("/profiles")
async def get_profiles(
    credentials: HTTPAuthorizationCredentials = Depends(security),
    db: Session = Depends(get_db)
):
    try:
        payload = jwt.decode(credentials.credentials, SECRET_KEY, algorithms=[ALGORITHM])
        current_user_id = payload.get("user_id")
        current_user = db.query(User).filter_by(id=current_user_id).first()
        if current_user.role != "admin":
            raise HTTPException(status_code=403, detail="Admin access only")
        users = db.query(User).all()

        all_users = []
        pending_users = []

        for user in users:
            # ❌ Skip current admin
            if user.id == current_user_id:
                continue

            perm = db.query(UserPermission).filter_by(user_id=user.id).first()
            can_trade = True if not perm else perm.can_trade
            can_use_calculator = True if not perm else perm.can_use_calculator

            user_data = {
                "id": user.id,
                "username": user.username,
                "email": user.email,
                "role": user.role,
                "approval_status": user.approval_status,
                "can_trade": can_trade,
                "can_use_calculator": can_use_calculator
            }

            all_users.append(user_data)

            if user.approval_status == "pending":
                pending_users.append(user_data)

        return {
            "all_users": all_users,
            "pending_users": pending_users,
            "pending_count": len(pending_users)
        }

    except JWTError:
        raise HTTPException(status_code=401, detail="Invalid token")


@router.post("/approve")
async def approve_user(
    data: dict,
    credentials: HTTPAuthorizationCredentials = Depends(security),
    db: Session = Depends(get_db)
):
    try:
        payload = jwt.decode(credentials.credentials, SECRET_KEY, algorithms=[ALGORITHM])
        current_user_id = payload.get("user_id")
        current_user = db.query(User).filter_by(id=current_user_id).first()
        if current_user.role != "admin":
            raise HTTPException(status_code=403, detail="Admin access only")

        user_id = data.get("user_id")
        decision = data.get("decision")   # "approve" or "decline"
        note = data.get("approval_note")

        if not user_id or decision not in ["approve", "decline"]:
            raise HTTPException(status_code=400, detail="Invalid request")

        status = "approved" if decision == "approve" else "declined"

        user = db.query(User).filter(User.id == user_id).first()
        if not user:
            raise HTTPException(status_code=404, detail="User not found")

        user.approval_status = status
        user.approval_note = note
        user.approved_by = str(payload.get("user_id"))   # store as string for safety
        user.approved_at = datetime.utcnow()

        db.commit()

        return {"message": f"User {user.username} has been {status}"}

    except JWTError:
        raise HTTPException(status_code=401, detail="Invalid token")
    

@router.get("/activity")
def get_activity(hours: int = 24, db: Session = Depends(get_db), credentials: HTTPAuthorizationCredentials = Depends(security)):

    # =========================
    # 🔒 ADMIN ONLY
    # =========================
    payload = jwt.decode(credentials.credentials, SECRET_KEY, algorithms=[ALGORITHM])
    current_user_id = payload.get("user_id")
    current_user = db.query(User).filter_by(id=current_user_id).first()
    if current_user.role != "admin":
        raise HTTPException(status_code=403, detail="Admin access only")

    now_ts = int(time.time())
    cutoff_ts = now_ts - (hours * 3600)

    # =========================
    # 🟢 ACTIVE USERS
    # =========================
    ACTIVE_WINDOW = 60  # seconds (last seen within 60s = active)

    active_users = db.query(ActiveUser).filter(
        ActiveUser.last_seen >= (now_ts - ACTIVE_WINDOW),
        ActiveUser.online == True
    ).all()

    active_now = []
    for u in active_users:
        active_now.append({
            "username": u.username,
            "role": u.role,
            "page": u.page,
            "action": u.action
        })

    # =========================
    # 🔴 RECENTLY OFFLINE
    # =========================
    offline_users = db.query(ActiveUser).filter(
        ActiveUser.last_seen < (now_ts - ACTIVE_WINDOW)
    ).order_by(ActiveUser.last_seen.desc()).limit(10).all()

    def time_ago(ts):
        diff = now_ts - ts
        if diff < 60:
            return f"{diff}s ago"
        elif diff < 3600:
            return f"{diff // 60}m ago"
        elif diff < 86400:
            return f"{diff // 3600}h ago"
        else:
            return f"{diff // 86400}d ago"

    offline = []
    for u in offline_users:
        offline.append({
            "username": u.username,
            "role": u.role,
            "page": u.page,
            "last_seen_ago": time_ago(u.last_seen)
        })

    # =========================
    # 📜 HISTORY LOGS
    # =========================
    logs = db.query(ActivityLog).filter(
        ActivityLog.ts >= cutoff_ts
    ).order_by(ActivityLog.ts.desc()).limit(100).all()

    history = []
    for log in logs:
        history.append({
            "username": log.username,
            "action": log.action,
            "page": log.page,
            "time_ago": time_ago(log.ts)
        })

    return {
        "active_now": active_now,
        "offline": offline,
        "history": history,
        "active_count": len(active_now)
    }


@router.post("/activity/heartbeat")
async def heartbeat(
    request: Request,
    db: Session = Depends(get_db),
    credentials: HTTPAuthorizationCredentials = Depends(security)
):
    try:
        payload = jwt.decode(credentials.credentials, SECRET_KEY, algorithms=[ALGORITHM])
    except Exception:
        raise HTTPException(status_code=401, detail="Invalid token")

    user_id = payload.get("user_id")

    if not user_id:
        raise HTTPException(status_code=401, detail="Invalid token payload")

    # ✅ Fetch real user
    user = db.query(User).filter(User.id == user_id).first()

    if not user:
        raise HTTPException(status_code=404, detail="User not found")

    now_ts = int(time.time())
    ip = request.client.host if request.client else None

    body = await request.json()
    page = body.get("page", "dashboard")
    action = body.get("action", "heartbeat")

    # =========================
    # UPSERT ACTIVE USER
    # =========================
    existing = db.query(ActiveUser).filter_by(username=user.username).first()

    if existing:
        existing.page = page
        existing.action = action
        existing.last_seen = now_ts
        existing.online = True
        existing.ip = ip
    else:
        db.add(ActiveUser(
            username=user.username,
            role=user.role,
            page=page,
            action=action,
            ip=ip,
            last_seen=now_ts,
            online=True
        ))

    # =========================
    # LOG HISTORY (OPTIONAL THROTTLE)
    # =========================
    db.add(ActivityLog(
        ts=now_ts,
        username=user.username,
        role=user.role,
        page=page,
        action=action,
        ip=ip
    ))

    db.commit()

    return {"status": "ok"}


@router.post("/update-user")
async def update_user(
    data: dict,
    credentials: HTTPAuthorizationCredentials = Depends(security),
    db: Session = Depends(get_db)
):
    try:
        payload = jwt.decode(credentials.credentials, SECRET_KEY, algorithms=[ALGORITHM])
        current_user_id = payload.get("user_id")
        current_user = db.query(User).filter_by(id=current_user_id).first()
        if current_user.role != "admin":
            raise HTTPException(status_code=403, detail="Admin access only")
        

        user_id = data.get("user_id")
        new_role = data.get("role")
        can_trade = data.get("can_trade")
        can_use_calculator = data.get("can_use_calculator")
        new_password = data.get("password")

        if not user_id:
            raise HTTPException(status_code=400, detail="User ID required")

        user = db.query(User).filter(User.id == user_id).first()
        if not user:
            raise HTTPException(status_code=404, detail="User not found")

        # =========================
        # 🔐 SAFETY: prevent self-demotion (optional but recommended)
        # =========================
        if user.id == payload.get("user_id") and new_role == "user":
            raise HTTPException(status_code=400, detail="You cannot remove your own admin role")

        # =========================
        # 🎭 UPDATE ROLE
        # =========================
        if new_role:
            if new_role not in ["user", "admin"]:
                raise HTTPException(status_code=400, detail="Invalid role")
            user.role = new_role

        # =========================
        # 💼 UPDATE CAN_TRADE
        # =========================
        if can_trade is not None:
            perm = db.query(UserPermission).filter_by(user_id=user_id).first()

            if not perm:
                perm = UserPermission(user_id=user_id)
                db.add(perm)

            perm.can_trade = bool(can_trade)

        if can_use_calculator is not None:
            perm = db.query(UserPermission).filter_by(user_id=user_id).first()

            if not perm:
                perm = UserPermission(user_id=user_id)
                db.add(perm)

            perm.can_use_calculator = bool(can_use_calculator)

        if new_password:
            user.password_hash = hash_password(new_password)

        db.commit()

        return {"message": f"User {user.username} updated successfully"}

    except JWTError:
        raise HTTPException(status_code=401, detail="Invalid token")


@router.post("/reset-password")
async def reset_password(
    data: dict,
    credentials: HTTPAuthorizationCredentials = Depends(security),
    db: Session = Depends(get_db)
):
    try:
        payload = jwt.decode(credentials.credentials, SECRET_KEY, algorithms=[ALGORITHM])
        current_user_id = payload.get("user_id")
        current_user = db.query(User).filter_by(id=current_user_id).first()
        if current_user.role != "admin":
            raise HTTPException(status_code=403, detail="Admin access only")

        user_id = data.get("user_id")
        new_password = data.get("password")

        if not user_id or not new_password:
            raise HTTPException(status_code=400, detail="Missing fields")

        user = db.query(User).filter(User.id == user_id).first()
        if not user:
            raise HTTPException(status_code=404, detail="User not found")

        user.password_hash = hash_password(new_password)

        db.commit()

        return {"message": f"Password updated for {user.username}"}

    except JWTError:
        raise HTTPException(status_code=401, detail="Invalid token")
    
@router.delete("/delete-user/{user_id}")
async def delete_user(
    user_id: int,
    credentials: HTTPAuthorizationCredentials = Depends(security),
    db: Session = Depends(get_db)
):
    try:
        # =========================
        # AUTH CHECK
        # =========================
        payload = jwt.decode(credentials.credentials, SECRET_KEY, algorithms=[ALGORITHM])
        current_user_id = payload.get("user_id")
        current_user = db.query(User).filter_by(id=current_user_id).first()
        if current_user.role != "admin":
            raise HTTPException(status_code=403, detail="Admin access only")

        current_admin_id = payload.get("user_id")

        # Prevent self-delete
        if user_id == current_admin_id:
            raise HTTPException(status_code=400, detail="You cannot delete yourself")

        # =========================
        # GET USER
        # =========================
        user = db.query(User).filter(User.id == user_id).first()
        if not user:
            raise HTTPException(status_code=404, detail="User not found")

        # =========================
        # GET TRADING ACCOUNTS
        # =========================
        accounts = db.query(TradingAccount).filter(
            TradingAccount.owner_user_id == user_id
        ).all()

        account_ids = [a.id for a in accounts]

        # =========================
        # METAAPI CLEANUP
        # =========================
        for acc in accounts:
            if acc.metaapi_account_id:
                try:
                    await account_manager.undeploy(acc.metaapi_account_id)
                    await account_manager.remove_account(acc.metaapi_account_id)
                except Exception as e:
                    print(f"[MetaAPI Delete Error] {acc.metaapi_account_id}: {e}")

        # =========================
        # COPY SYSTEM CLEANUP
        # =========================
        if account_ids:
            db.query(CopyTradeLink).filter(
                (CopyTradeLink.master_account_id.in_(account_ids)) |
                (CopyTradeLink.slave_account_id.in_(account_ids))
            ).delete(synchronize_session=False)

            db.query(CopyRelationship).filter(
                (CopyRelationship.master_account_id.in_(account_ids)) |
                (CopyRelationship.slave_account_id.in_(account_ids))
            ).delete(synchronize_session=False)

            db.query(BotLog).filter(
                BotLog.account_id.in_(account_ids)
            ).delete(synchronize_session=False)

        # =========================
        # DELETE USER-RELATED TABLES
        # =========================
        db.query(TradingAccount).filter(
            TradingAccount.owner_user_id == user_id
        ).delete(synchronize_session=False)

        db.query(UserPermission).filter(
            UserPermission.user_id == user_id
        ).delete(synchronize_session=False)

        db.query(ActivityLog).filter(
            ActivityLog.username == user.username
        ).delete(synchronize_session=False)

        db.query(ActiveUser).filter(
            ActiveUser.username == user.username
        ).delete(synchronize_session=False)

        # =========================
        # DELETE USER
        # =========================
        db.delete(user)

        db.commit()

        return {
            "success": True,
            "message": f"User '{user.username}' and all related data deleted successfully"
        }

    except JWTError:
        raise HTTPException(status_code=401, detail="Invalid token")

    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    



# @router.get("/activity")
# async def get_activity(
#     hours: int = 24,
#     credentials: HTTPAuthorizationCredentials = Depends(security),
#     db: Session = Depends(get_db)
# ):
#     try:
#         payload = jwt.decode(credentials.credentials, SECRET_KEY, algorithms=[ALGORITHM])
#         if payload.get("role") != "admin":
#             raise HTTPException(status_code=403, detail="Admin only")

#         # Calculate cutoff time
#         cutoff = datetime.utcnow() - timedelta(hours=hours)

#         # Recent Activity Log
#         history = db.query(ActivityLog)\
#             .filter(ActivityLog.created_at >= cutoff)\
#             .order_by(desc(ActivityLog.created_at))\
#             .limit(100)\
#             .all()

#         history_list = [{
#             "id": log.id,
#             "username": log.username,
#             "role": log.role,
#             "page": log.page,
#             "action": log.action,
#             "time_ago": "recent",
#             "created_at": log.created_at.isoformat()
#         } for log in history]

#         # TODO: Later integrate real ActiveUser logic
#         active_now = []
#         offline = []

#         return {
#             "active_now": active_now,
#             "offline": offline,
#             "history": history_list,
#             "active_count": len(active_now)
#         }

#     except JWTError:
#         raise HTTPException(status_code=401, detail="Invalid token")