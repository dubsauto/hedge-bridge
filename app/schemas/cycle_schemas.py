# app/schemas/cycle_schemas.py

from __future__ import annotations
from typing import List, Optional, Literal
from pydantic import BaseModel, Field, field_validator


# ─── SUB-SCHEMAS ──────────────────────────────────────────────────────────────

class StrategySettings(BaseModel):
    ea_name:                 str   = Field("Cycle_EA_Premium", max_length=128)
    use_name_as_comment:     bool  = True
    signal_tf:               str   = "5"      # MT5 ENUM string
    ema_period:              int   = Field(200, ge=1)
    bb_period:               int   = Field(20,  ge=1)
    bb_deviation:            float = Field(2.0, ge=0.1)
    require_closed_candle:   bool  = True
    require_close_inside_bb: bool  = True


class TradeOut(BaseModel):
    num:      int
    lot:      float
    tp_pips:  int
    sl_pips:  int
    tp_money: float
    sl_money: float
    outcome:  Optional[Literal["TP", "SL"]] = None

    class Config:
        from_attributes = True


class PhaseOut(BaseModel):
    phase_num:       int
    recovery:        float
    tp_value:        float
    lot:             float
    sl_base_pips:    int
    loss_real:       float
    disallineamento: float
    trades:          List[TradeOut]

    class Config:
        from_attributes = True


class CycleStateSchema(BaseModel):
    """Mirrors the JS cycleState object stored in the DB."""
    current_phase:        int   = 0
    current_trade_index:  int   = 0
    consecutive_tp_count: int   = 0
    sl_count:             int   = 0
    cycle_winner:         Optional[Literal["BIG", "SMALL"]] = None
    outcomes:             List[List[Optional[Literal["TP", "SL"]]]] = []


# ─── SLOT REQUEST / RESPONSE ──────────────────────────────────────────────────

class SlotCreate(BaseModel):
    """POST /cycle/slots  — create a named (initially empty) slot."""
    name: str = Field(..., min_length=1, max_length=128)


class SlotPayloadUpdate(BaseModel):
    """
    PUT /cycle/slots/{slot_id}/payload
    Saves the current sidebar inputs into the slot WITHOUT running the
    calculation (mirrors the JS buildSlotPayload()).
    """
    big_balance:      float = Field(..., gt=0)
    small_balance:    float = Field(..., gt=0)
    starting_pips:    int   = Field(..., ge=1)
    num_phases:       int   = Field(..., ge=1, le=10)
    trades_per_phase: int   = Field(..., ge=1, le=10)
    losses:           List[float]          # len must equal num_phases
    strategy:         StrategySettings

    @field_validator("losses")
    @classmethod
    def losses_length(cls, v: list, info) -> list:
        # num_phases comes first in model field order so it's available
        num = info.data.get("num_phases", len(v))
        if len(v) != num:
            raise ValueError(f"losses must have exactly {num} entries (one per phase)")
        return v


class CalculateRequest(BaseModel):
    """
    POST /cycle/slots/{slot_id}/calculate
    Triggers the deterministic cycle calculation on the server.
    All inputs are read from the stored slot; the caller may optionally
    override them inline (useful for "calculate without saving first").
    """
    # Optional overrides — if omitted the server uses what's in the slot
    big_balance:      Optional[float] = None
    small_balance:    Optional[float] = None
    starting_pips:    Optional[int]   = None
    num_phases:       Optional[int]   = None
    trades_per_phase: Optional[int]   = None
    losses:           Optional[List[float]] = None
    strategy:         Optional[StrategySettings] = None


class OutcomeUpdate(BaseModel):
    """
    POST /cycle/slots/{slot_id}/outcome
    Records a single TP/SL button click and advances the state machine.
    """
    phase_idx: int = Field(..., ge=0)
    trade_idx: int = Field(..., ge=0)
    outcome:   Literal["TP", "SL"]


class SlotOut(BaseModel):
    """Full slot response (returned after create / load / calculate)."""
    id:               int
    name:             str
    big_balance:      float
    small_balance:    float
    starting_pips:    int
    num_phases:       int
    trades_per_phase: int
    losses:           List[float]
    strategy:         StrategySettings
    cycle_state:      Optional[CycleStateSchema]
    phases:           List[PhaseOut]
    created_at:       str
    updated_at:       Optional[str]

    class Config:
        from_attributes = True


class SlotSummary(BaseModel):
    """Lightweight row for the slots list sidebar."""
    id:         int
    name:       str
    has_data:   bool      # True when cycle has been calculated at least once
    created_at: str
    updated_at: Optional[str]

    class Config:
        from_attributes = True