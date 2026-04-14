# app/model.py

import os
from datetime import datetime
from sqlalchemy import (
    Column, Integer, String, Float, DateTime, Text,
    Boolean, ForeignKey, UniqueConstraint
)
from sqlalchemy.orm import declarative_base, relationship
from sqlalchemy import create_engine
from sqlalchemy import JSON

Base = declarative_base()

# =========================
# USERS
# =========================
class User(Base):
    __tablename__ = "users"

    id = Column(Integer, primary_key=True)
    username = Column(String(64), unique=True, nullable=False)
    email = Column(String(190), unique=True)
    password_hash = Column(String(255), nullable=False)

    role = Column(String(16), default="user")  # admin/user
    approval_status = Column(String(16), default="approved")

    approval_note = Column(String(255))
    approved_by = Column(String(64))
    approved_at = Column(DateTime)

    created_at = Column(DateTime, default=datetime.utcnow)

    accounts = relationship("TradingAccount", back_populates="owner")


# =========================
# USER PERMISSIONS
# =========================
class UserPermission(Base):
    __tablename__ = "user_permissions"

    user_id = Column(Integer, ForeignKey("users.id", ondelete="CASCADE"), primary_key=True)
    can_trade = Column(Boolean, default=True)

    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

class TradingAccount(Base):
    __tablename__ = "trading_accounts"

    id = Column(Integer, primary_key=True)

    owner_user_id = Column(Integer, ForeignKey("users.id", ondelete="SET NULL"))

    # Basic Info
    name = Column(String(255), nullable=False)           # Friendly name
    login = Column(Integer, nullable=False, unique=True) # MT5 Login
    password = Column(Text, nullable=False)
    server = Column(String(255), nullable=False)

    # MetaAPI Integration
    metaapi_account_id = Column(String(255), unique=True, nullable=True)  # Important
    region = Column(String(50))
    state = Column(String(50), default="created")       # created, deployed, undeployed, error

    # Trading Settings (You asked about these)
    manual_trades = Column(Boolean, default=True)        # Show api trades as manual trade
    use_dedicated_ip = Column(Boolean, default=True)     # allocateDedicatedIp

    # Magic number (very important for copy trading)
    magic = Column(Integer, default=0)

    # Additional useful fields
    connection_status = Column(String(20), default="disconnected")  # connected, disconnected, error
    last_connected_at = Column(DateTime, nullable=True)
    last_error = Column(Text, nullable=True)

    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    owner = relationship("User", back_populates="accounts")
    copy_relationships_as_master = relationship("CopyRelationship", 
                                                foreign_keys="[CopyRelationship.master_account_id]",
                                                backref="master_account")
    
    copy_relationships_as_slave = relationship("CopyRelationship", 
                                               foreign_keys="[CopyRelationship.slave_account_id]",
                                               backref="slave_account")


# =========================
# COPY RELATIONSHIPS (MASTER/SLAVE)
# =========================
class CopyRelationship(Base):
    __tablename__ = "copy_relationships"

    id = Column(Integer, primary_key=True)

    master_account_id = Column(Integer, ForeignKey("trading_accounts.id", ondelete="CASCADE"))
    slave_account_id = Column(Integer, ForeignKey("trading_accounts.id", ondelete="CASCADE"))

    # behavior
    copy_direction = Column(String(16), default="same")  # same/opposite
    strict_mode = Column(Boolean, default=False)

    is_active = Column(Boolean, default=True)

    created_at = Column(DateTime, default=datetime.utcnow)

    __table_args__ = (
        UniqueConstraint('master_account_id', 'slave_account_id', name='uniq_copy_pair'),
    )


# =========================
# COPY TRADE LINKS (LIVE TRADE MAPPING)
# =========================
class CopyTradeLink(Base):
    __tablename__ = "copy_trade_links"

    id = Column(Integer, primary_key=True)

    master_account_id = Column(Integer, ForeignKey("trading_accounts.id", ondelete="CASCADE"))
    slave_account_id = Column(Integer, ForeignKey("trading_accounts.id", ondelete="CASCADE"))

    master_ticket = Column(String(64), nullable=False)
    slave_ticket = Column(String(64))

    symbol = Column(String(32))
    trade_type = Column(String(64))  # buy/sell

    volume = Column(Float, default=0)

    status = Column(String(16), default="open")  # open/closed/error
    last_error = Column(String(255))

    closed_at = Column(DateTime)

    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


# =========================
# GLOBAL COPY SETTINGS
# =========================
class CopyTradeSettings(Base):
    __tablename__ = "copy_trade_settings"

    id = Column(Integer, primary_key=True, default=1)

    fixed_lot_enabled = Column(Boolean, default=False)
    master_lot = Column(Float, default=0.10)
    slave_lot = Column(Float, default=0.10)

    updated_by = Column(String(64))
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


# =========================
# ACTIVITY LOG
# =========================
class ActivityLog(Base):
    __tablename__ = "activity_log"

    id = Column(Integer, primary_key=True)

    ts = Column(Integer, nullable=False)

    username = Column(String(64), nullable=False)
    role = Column(String(16), nullable=False)

    page = Column(String(64), nullable=False)
    action = Column(String(64), nullable=False)

    meta = Column(Text)
    ip = Column(String(64))

    created_at = Column(DateTime, default=datetime.utcnow)


# =========================
# ACTIVE USERS (REAL-TIME TRACKING)
# =========================
class ActiveUser(Base):
    __tablename__ = "active_users"

    username = Column(String(64), primary_key=True)

    role = Column(String(16), nullable=False)
    page = Column(String(64), nullable=False)
    action = Column(String(64), nullable=False)

    meta = Column(Text)

    ip = Column(String(64))
    ua = Column(String(255))

    online = Column(Boolean, default=True)

    last_seen = Column(Integer, nullable=False)

    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


class BotLog(Base):
    __tablename__ = "bot_logs"

    id = Column(Integer, primary_key=True, autoincrement=True)

    account_id = Column(Integer, ForeignKey("trading_accounts.id", ondelete="CASCADE"), nullable=False)

    timestamp = Column(DateTime, default=datetime.utcnow)

    level = Column(String(20))        # INFO, TRADE, ERROR
    category = Column(String(32))     # SYSTEM, COPY, EXECUTION

    message = Column(Text)
    raw_json = Column(JSON)

    created_at = Column(DateTime, default=datetime.utcnow)

# =========================
# CREATE TABLES
# =========================
# def init_db():
#     Base.metadata.create_all(bind=engine)


# =========================
# TRADING ACCOUNTS (REPLACES VPS)
# =========================
# class TradingAccount(Base):
#     __tablename__ = "trading_accounts"

#     id = Column(Integer, primary_key=True)

#     owner_user_id = Column(Integer, ForeignKey("users.id", ondelete="SET NULL"))

#     name = Column(String(255), nullable=False)  # custom name
#     login = Column(Integer, nullable=False)
#     password = Column(Text, nullable=False)
#     server = Column(String(255), nullable=False)

#     # MetaAPI fields (important)
#     metaapi_account_id = Column(String(255))  # returned from MetaAPI
#     region = Column(String(50))
#     status = Column(String(50), default="created")

#     created_at = Column(DateTime, default=datetime.utcnow)
#     updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

#     owner = relationship("User", back_populates="accounts")