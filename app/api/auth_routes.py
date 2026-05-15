# app/api/auth_routes.py
import os
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from datetime import datetime
from dotenv import load_dotenv

from app.database import get_db
from app.model import User, UserPermission
from app.auth import verify_password, create_access_token, hash_password, get_current_user
from hedgebridge.dashboard_session import dashboard_session

load_dotenv()

router = APIRouter(prefix="", tags=["Authentication"])


# =========================
# AUTH ROUTES
# =========================

@router.post("/login")
async def login(
    data: dict, 
    db: Session = Depends(get_db)
):
    identifier = data.get("identifier")
    password = data.get("password")

    if not identifier or not password:
        raise HTTPException(status_code=400, detail="Missing credentials")

    user = db.query(User).filter(
        (User.username == identifier) | (User.email == identifier)
    ).first()

    if not user or not verify_password(password, user.password_hash):
        raise HTTPException(status_code=401, detail="Invalid credentials")

    if user.approval_status != "approved":
        raise HTTPException(status_code=403, detail="Your account is pending admin approval")

    token = create_access_token({
        "user_id": user.id,
        "role": user.role
    })

    return {
        "access_token": token,
        "role": user.role
    }


@router.post("/logout")
async def logout(payload: dict = Depends(get_current_user)):
    user_id = payload.get("user_id")
    if user_id:
        await dashboard_session.on_logout(user_id)
    return {"message": "Logged out"}


@router.post("/signup")
async def signup(
    data: dict, 
    db: Session = Depends(get_db)
):
    username = data.get("username")
    email = data.get("email")
    password = data.get("password")

    if not username or not email or not password:
        raise HTTPException(status_code=400, detail="Username, email and password are required")

    # Check if username or email already exists
    existing = db.query(User).filter(
        (User.username == username) | (User.email == email)
    ).first()

    if existing:
        raise HTTPException(status_code=400, detail="Username or email already registered")

    # Create new user with pending approval
    new_user = User(
        username=username,
        email=email,
        password_hash=hash_password(password),
        role="user",
        approval_status="pending",
        created_at=datetime.utcnow()
    )

    db.add(new_user)
    db.flush()   # Get the ID for permission

    # Create permission record
    permission = UserPermission(
        user_id=new_user.id,
        can_trade=True
    )
    db.add(permission)
    db.commit()

    return {
        "message": "Account created successfully! Your account is pending admin approval."
    }