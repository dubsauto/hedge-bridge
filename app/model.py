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
    cycle_slots = relationship("CycleSlot", back_populates="owner")


# =========================
# USER PERMISSIONS
# =========================
class UserPermission(Base):
    __tablename__ = "user_permissions"

    user_id = Column(Integer, ForeignKey("users.id", ondelete="CASCADE"), primary_key=True)
    can_trade = Column(Boolean, default=True)
    can_use_calculator = Column(Boolean, default=True)
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

    # ✅ NEW: tracks whether streaming listener is fully synchronized
    listener_active = Column(Boolean, default=False)

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
    user_id = Column(Integer, ForeignKey("users.id", ondelete="CASCADE"))
    fixed_lot_enabled = Column(Boolean, default=False)
    pips_offset_enabled = Column(Boolean, default=False)
    pips_offset = Column(Integer, default=50)
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

class VpsAccount(Base):
    __tablename__ = "vps_accounts"
 
    id = Column(Integer, primary_key=True)
 
    owner_user_id = Column(
        Integer,
        ForeignKey("users.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
 
    # Connection details
    host     = Column(String(255), nullable=False)   # IP or hostname
    username = Column(String(128), nullable=False)   # SSH / RDP username
    password = Column(Text, nullable=False)          # store encrypted at app layer if needed
 
    # Protocol — "ssh" (default, Linux) or "rdp" (Windows)
    protocol = Column(String(8), default="ssh")
    # Port — None means use the protocol default (22 for SSH, 3389 for RDP)
    port     = Column(Integer, nullable=True)
 
    # Optional link to a TradingAccount
    associated_mt5_id = Column(
        Integer,
        ForeignKey("trading_accounts.id", ondelete="SET NULL"),
        nullable=True,
    )
 
    # Behaviour flags
    auto_connect = Column(Boolean, default=True)
 
    # Runtime state (updated by health-check or deploy hooks)
    is_online      = Column(Boolean, default=False)
    last_checked_at = Column(DateTime, nullable=True)
 
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
 
    # Relationships
    owner          = relationship("User")
    associated_mt5 = relationship("TradingAccount", foreign_keys=[associated_mt5_id])

    
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

class SymbolMappingGroup(Base):
    __tablename__ = "symbol_mapping_groups"

    id = Column(Integer, primary_key=True)

    owner_user_id = Column(Integer, ForeignKey("users.id", ondelete="CASCADE"))

    name = Column(String(100))

    created_at = Column(DateTime, default=datetime.utcnow)

class SymbolMappingEntry(Base):
    __tablename__ = "symbol_mapping_entries"

    id = Column(Integer, primary_key=True)

    group_id = Column(Integer, ForeignKey("symbol_mapping_groups.id", ondelete="CASCADE"))
    account_id = Column(Integer, ForeignKey("trading_accounts.id", ondelete="CASCADE"))

    symbol = Column(String(32), nullable=False)

    __table_args__ = (
        UniqueConstraint("group_id", "account_id", name="uniq_group_account"),
    )
    
class AccountLot(Base):
    __tablename__ = "account_lots"

    account_id = Column(
        Integer,
        ForeignKey("trading_accounts.id", ondelete="CASCADE"),
        primary_key=True
    )

    lot_size = Column(Float, default=0.10)

    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

# =========================
# FX CYCLE SLOTS
# =========================
class CycleSlot(Base):
    """
    One named slot per user.  Holds both the calculator inputs (payload)
    and the live TP/SL progress (cycle_state) so the frontend never has
    to touch localStorage again.
    """
    __tablename__ = "cycle_slots"
 
    id          = Column(Integer, primary_key=True)
    user_id     = Column(Integer, ForeignKey("users.id", ondelete="CASCADE"), nullable=False, index=True)
    name        = Column(String(128), nullable=False)
 
    # --- calculator inputs ---------------------------------------------------
    big_balance     = Column(Float, nullable=False, default=3000.0)
    small_balance   = Column(Float, nullable=False, default=1000.0)
    starting_pips   = Column(Integer, nullable=False, default=100)
    num_phases      = Column(Integer, nullable=False, default=3)
    trades_per_phase= Column(Integer, nullable=False, default=4)
 
    # per-phase real losses as a JSON list, e.g. [1000.0, 1000.0, 1000.0]
    losses          = Column(JSON, nullable=False, default=list)
 
    # --- strategy settings (sidebar "Strategia Ciclo") -----------------------
    ea_name             = Column(String(128), default="Cycle_EA_Premium")
    use_name_as_comment = Column(Boolean, default=True)
    signal_tf           = Column(String(16), default="5")       # MT5 ENUM value
    ema_period          = Column(Integer, default=200)
    bb_period           = Column(Integer, default=20)
    bb_deviation        = Column(Float, default=2.0)
    require_closed_candle   = Column(Boolean, default=True)
    require_close_inside_bb = Column(Boolean, default=True)
 
    # --- live cycle state ----------------------------------------------------
    # Stored as JSON so we avoid a full separate table for a small dict.
    # Shape: {
    #   "currentPhase": 0,
    #   "currentTradeIndex": 0,
    #   "consecutiveTPCount": 0,
    #   "slCount": 0,
    #   "cycleWinner": null,          # null | "BIG" | "SMALL"
    #   "outcomes": [[null,null,...], ...]   # phases × trades matrix
    # }
    cycle_state = Column(JSON, nullable=True)
 
    created_at  = Column(DateTime, default=datetime.utcnow)
    updated_at  = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
 
    # relationships
    owner   = relationship("User", back_populates="cycle_slots")
    phases  = relationship("CyclePhase", back_populates="slot",
                           cascade="all, delete-orphan", order_by="CyclePhase.phase_num")
 
 
# =========================
# COMPUTED PHASES  (read-only cache — re-generated on every "Calculate")
# =========================
class CyclePhase(Base):
    """
    One row per phase, written (or re-written) whenever the user hits
    'Calcola Ciclo'.  Lets the backend serve the computed table without
    re-running the arithmetic every request.
    """
    __tablename__ = "cycle_phases"
 
    id       = Column(Integer, primary_key=True)
    slot_id  = Column(Integer, ForeignKey("cycle_slots.id", ondelete="CASCADE"), nullable=False, index=True)
    phase_num= Column(Integer, nullable=False)   # 1-based
 
    # computed fields (mirrors calculateCycle() output)
    recovery        = Column(Float, nullable=False)
    tp_value        = Column(Float, nullable=False)   # TP money per trade
    lot             = Column(Float, nullable=False)
    sl_base_pips    = Column(Integer, nullable=False)
    loss_real       = Column(Float, nullable=False)
    disallineamento = Column(Float, nullable=False)
 
    # trades as JSON list of dicts
    # [{ "num":1, "lot":0.05, "tpPips":100, "slPips":100,
    #    "tpMoney":50.0, "slMoney":50.0, "outcome": null }, ...]
    trades = Column(JSON, nullable=False, default=list)
 
    slot = relationship("CycleSlot", back_populates="phases")
 