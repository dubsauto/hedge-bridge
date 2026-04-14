# app/init_db.py

from sqlalchemy.orm import sessionmaker
from app.model import User, UserPermission, Base
from app.auth import hash_password
from app.database import engine
from datetime import datetime
import asyncio

SessionLocal = sessionmaker(bind=engine)


async def create_default_admins(db):
    admins = [
        {
            "username": "alessandro",
            "password": "alessandro_dashboard_786786"
        },
        {
            "username": "adabs",
            "password": "hedge001"
        }
    ]

    for admin in admins:
        existing = db.query(User).filter(User.username == admin["username"]).first()

        if existing:
            print(f"⚠️ Admin '{admin['username']}' already exists")
            continue

        user = User(
            username=admin["username"],
            email=None,
            password_hash=hash_password(admin["password"]),
            role="admin",
            approval_status="approved",
            approved_at=datetime.utcnow()
        )

        db.add(user)
        db.flush()   # get user.id

        permission = UserPermission(
            user_id=user.id,
            can_trade=True
        )
        db.add(permission)

        print(f"✅ Created admin: {admin['username']}")

    db.commit()


async def init_database():
    print("🚀 Initializing database...")

    # Create tables (this is synchronous, but acceptable during startup)
    Base.metadata.create_all(bind=engine)
    print("✅ All tables created or already exist.")

    db = SessionLocal()
    try:
        await create_default_admins(db)
    finally:
        db.close()

    print("✅ Database ready.")


# For backward compatibility (if called from sync context)
def init_database_sync():
    """Use this only if needed from non-async code"""
    asyncio.run(init_database())


# if __name__ == "__main__":
#     init_database()