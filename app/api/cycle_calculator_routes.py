# app/api/cycle_calculator_routes.py
"""
FX Cycle Calculator — FastAPI router
====================================
Replaces the localStorage slot system with a proper per-user database backend.

Endpoints
---------
POST   /cycle/slots                        Create a new (empty) slot
GET    /cycle/slots                        List all slots for current user
GET    /cycle/slots/{slot_id}              Load a full slot (inputs + phases + state)
PUT    /cycle/slots/{slot_id}/payload      Save sidebar inputs without recalculating
POST   /cycle/slots/{slot_id}/calculate    Run the deterministic cycle calculation
POST   /cycle/slots/{slot_id}/outcome      Record a TP/SL trade outcome
DELETE /cycle/slots/{slot_id}              Delete a slot
"""

from __future__ import annotations

from typing import List

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session
from jose import jwt, JWTError
from fastapi.security import HTTPAuthorizationCredentials

from app.database import get_db
from app.model import User, CycleSlot, CyclePhase
from app.auth import SECRET_KEY, ALGORITHM, security
from app.schemas.cycle_schemas import (
    SlotCreate,
    SlotPayloadUpdate,
    CalculateRequest,
    OutcomeUpdate,
    SlotOut,
    SlotSummary,
    StrategySettings,
    CycleStateSchema,
    PhaseOut,
    TradeOut,
)
from app.services.cycle_engine import (
    calculate_cycle,
    apply_outcome,
    initial_state,
)

router = APIRouter(prefix="/cycle", tags=["Cycle Calculator"])


# ─── AUTH HELPER ──────────────────────────────────────────────────────────────

def get_current_user(
    credentials: HTTPAuthorizationCredentials = Depends(security), 
    db: Session = Depends(get_db),
) -> User:
    """Decode the Bearer JWT and return the matching User row."""
    try:
        payload = jwt.decode(credentials.credentials, SECRET_KEY, algorithms=[ALGORITHM])
        user_id = payload.get("user_id")
    except (JWTError, TypeError, ValueError):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or expired token",
        )
    #print(f"user_id from token: {user_id}")  # Debug log
    user = db.query(User).filter(User.id == user_id).first()
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    return user


# ─── SLOT OWNERSHIP GUARD ─────────────────────────────────────────────────────

def _get_slot_or_404(slot_id: int, user: User, db: Session) -> CycleSlot:
    """Fetch a slot that belongs to the current user or raise 404."""
    slot = (
        db.query(CycleSlot)
        .filter(CycleSlot.id == slot_id, CycleSlot.user_id == user.id)
        .first()
    )
    if not slot:
        raise HTTPException(status_code=404, detail="Slot not found")
    return slot


# ─── SERIALISATION HELPERS ────────────────────────────────────────────────────

def _strategy_from_slot(slot: CycleSlot) -> StrategySettings:
    return StrategySettings(
        ea_name=slot.ea_name or "Cycle_EA_Premium",
        use_name_as_comment=slot.use_name_as_comment,
        signal_tf=slot.signal_tf or "5",
        ema_period=slot.ema_period,
        bb_period=slot.bb_period,
        bb_deviation=slot.bb_deviation,
        require_closed_candle=slot.require_closed_candle,
        require_close_inside_bb=slot.require_close_inside_bb,
    )


def _phases_out(phases: List[CyclePhase]) -> List[PhaseOut]:
    result = []
    for p in phases:
        trades = [
            TradeOut(
                num=t["num"],
                lot=t["lot"],
                tp_pips=t["tp_pips"],
                sl_pips=t["sl_pips"],
                tp_money=t["tp_money"],
                sl_money=t["sl_money"],
                outcome=t.get("outcome"),
            )
            for t in (p.trades or [])
        ]
        result.append(
            PhaseOut(
                phase_num=p.phase_num,
                recovery=p.recovery,
                tp_value=p.tp_value,
                lot=p.lot,
                sl_base_pips=p.sl_base_pips,
                loss_real=p.loss_real,
                disallineamento=p.disallineamento,
                trades=trades,
            )
        )
    return result


def _slot_out(slot: CycleSlot) -> SlotOut:
    cs = slot.cycle_state
    cycle_state_schema = (
        CycleStateSchema(
            current_phase=cs.get("current_phase", 0),
            current_trade_index=cs.get("current_trade_index", 0),
            consecutive_tp_count=cs.get("consecutive_tp_count", 0),
            sl_count=cs.get("sl_count", 0),
            cycle_winner=cs.get("cycle_winner"),
            outcomes=cs.get("outcomes", []),
        )
        if cs
        else None
    )

    return SlotOut(
        id=slot.id,
        name=slot.name,
        big_balance=slot.big_balance,
        small_balance=slot.small_balance,
        starting_pips=slot.starting_pips,
        num_phases=slot.num_phases,
        trades_per_phase=slot.trades_per_phase,
        losses=slot.losses or [],
        strategy=_strategy_from_slot(slot),
        cycle_state=cycle_state_schema,
        phases=_phases_out(slot.phases),
        created_at=slot.created_at.isoformat() if slot.created_at else "",
        updated_at=slot.updated_at.isoformat() if slot.updated_at else None,
    )


def _apply_strategy(slot: CycleSlot, s: StrategySettings) -> None:
    """Write a StrategySettings object into a CycleSlot row in-place."""
    slot.ea_name                = s.ea_name
    slot.use_name_as_comment    = s.use_name_as_comment
    slot.signal_tf              = s.signal_tf
    slot.ema_period             = s.ema_period
    slot.bb_period              = s.bb_period
    slot.bb_deviation           = s.bb_deviation
    slot.require_closed_candle  = s.require_closed_candle
    slot.require_close_inside_bb = s.require_close_inside_bb


# ─── ROUTES ───────────────────────────────────────────────────────────────────

# POST /cycle/slots ─── Create empty slot ──────────────────────────────────────
@router.post(
    "/slots",
    response_model=SlotSummary,
    status_code=status.HTTP_201_CREATED,
    summary="Create a new (empty) named slot",
)
def create_slot(
    body: SlotCreate,
    user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """
    Equivalent to the JS `confirmCreateEmptySlot()`.
    Creates the slot with default values; no cycle calculation is run yet.
    """
    slot = CycleSlot(
        user_id=user.id,
        name=body.name,
        # defaults come from the column definitions
    )
    db.add(slot)
    db.commit()
    db.refresh(slot)

    return SlotSummary(
        id=slot.id,
        name=slot.name,
        has_data=False,
        created_at=slot.created_at.isoformat(),
        updated_at=None,
    )


# GET /cycle/slots ─── List all slots ─────────────────────────────────────────
@router.get(
    "/slots",
    response_model=List[SlotSummary],
    summary="List all slots belonging to the current user",
)
def list_slots(
    user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """
    Lightweight list — just name + has_data flag.
    Mirrors the JS `renderSlots()` sidebar.
    """
    slots = (
        db.query(CycleSlot)
        .filter(CycleSlot.user_id == user.id)
        .order_by(CycleSlot.created_at.desc())
        .all()
    )
    return [
        SlotSummary(
            id=s.id,
            name=s.name,
            has_data=bool(s.phases),        # True once calculate has been called
            created_at=s.created_at.isoformat(),
            updated_at=s.updated_at.isoformat() if s.updated_at else None,
        )
        for s in slots
    ]


# GET /cycle/slots/{slot_id} ─── Load full slot ───────────────────────────────
@router.get(
    "/slots/{slot_id}",
    response_model=SlotOut,
    summary="Load a slot with all computed phases and current cycle state",
)
def get_slot(
    slot_id: int,
    user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """
    Equivalent to the JS `loadSlot()` — returns everything the frontend needs
    to restore sidebar inputs, TP/SL button states, and cycle progress.
    """
    slot = _get_slot_or_404(slot_id, user, db)
    return _slot_out(slot)


# PUT /cycle/slots/{slot_id}/payload ─── Save sidebar inputs ──────────────────
@router.put(
    "/slots/{slot_id}/payload",
    response_model=SlotOut,
    summary="Persist current sidebar inputs into the slot without recalculating",
)
def update_slot_payload(
    slot_id: int,
    body: SlotPayloadUpdate,
    user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """
    Mirrors JS `buildSlotPayload()` + `saveCurrentStateToActiveSlot()`.
    Call this whenever the user edits inputs and wants to persist without
    triggering a new calculation (e.g. auto-save on blur).

    Does NOT clear the existing cycle_state or phases — those survive
    until the next `calculate` call.
    """
    slot = _get_slot_or_404(slot_id, user, db)

    slot.big_balance      = body.big_balance
    slot.small_balance    = body.small_balance
    slot.starting_pips    = body.starting_pips
    slot.num_phases       = body.num_phases
    slot.trades_per_phase = body.trades_per_phase
    slot.losses           = body.losses
    _apply_strategy(slot, body.strategy)

    db.commit()
    db.refresh(slot)
    return _slot_out(slot)


# POST /cycle/slots/{slot_id}/calculate ─── Run calculation ───────────────────
@router.post(
    "/slots/{slot_id}/calculate",
    response_model=SlotOut,
    summary="Run the deterministic cycle calculation and store computed phases",
)
def calculate_slot(
    slot_id: int,
    body: CalculateRequest,
    user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """
    Server-side port of JS `calculateCycle()`.

    - If body fields are provided they OVERRIDE the slot's stored values
      (and the slot is updated accordingly).
    - Wipes all existing CyclePhase rows for this slot and writes fresh ones.
    - Resets cycle_state to zero (fresh simulation).
    """
    slot = _get_slot_or_404(slot_id, user, db)

    # Apply any overrides from the request body
    if body.big_balance      is not None: slot.big_balance      = body.big_balance
    if body.small_balance    is not None: slot.small_balance    = body.small_balance
    if body.starting_pips    is not None: slot.starting_pips    = body.starting_pips
    if body.num_phases       is not None: slot.num_phases       = body.num_phases
    if body.trades_per_phase is not None: slot.trades_per_phase = body.trades_per_phase
    if body.losses           is not None: slot.losses           = body.losses
    if body.strategy         is not None: _apply_strategy(slot, body.strategy)

    # Validate losses length
    losses = slot.losses or []
    theoretical = slot.big_balance / slot.num_phases
    if len(losses) < slot.num_phases:
        # Pad with theoretical value (same as JS placeholder behaviour)
        losses = list(losses) + [round(theoretical, 2)] * (slot.num_phases - len(losses))
        slot.losses = losses

    # Run the engine
    phase_dicts = calculate_cycle(
        big_balance=slot.big_balance,
        small_balance=slot.small_balance,
        starting_pips=slot.starting_pips,
        num_phases=slot.num_phases,
        trades_per_phase=slot.trades_per_phase,
        losses=losses,
    )

    # Replace stored phases (delete-orphan cascade handles old rows)
    for old_phase in slot.phases:
        db.delete(old_phase)
    db.flush()

    for pd in phase_dicts:
        db.add(CyclePhase(
            slot_id=slot.id,
            phase_num=pd["phase_num"],
            recovery=pd["recovery"],
            tp_value=pd["tp_value"],
            lot=pd["lot"],
            sl_base_pips=pd["sl_base_pips"],
            loss_real=pd["loss_real"],
            disallineamento=pd["disallineamento"],
            trades=pd["trades"],
        ))

    # Reset simulation state
    slot.cycle_state = initial_state(slot.num_phases, slot.trades_per_phase)

    db.commit()
    db.refresh(slot)
    return _slot_out(slot)


# POST /cycle/slots/{slot_id}/outcome ─── Record TP/SL ────────────────────────
@router.post(
    "/slots/{slot_id}/outcome",
    response_model=SlotOut,
    summary="Record a TP or SL outcome and advance the cycle state machine",
)
def record_outcome(
    slot_id: int,
    body: OutcomeUpdate,
    user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """
    Mirrors JS `setOutcome()` + `promptOutcomeSave()`.

    1. Validates it's the currently expected trade.
    2. Runs the state machine transition.
    3. Persists the new cycle_state and the updated trade outcome in
       the CyclePhase.trades JSON column.
    4. Returns the full updated slot so the frontend can re-render.
    """
    slot = _get_slot_or_404(slot_id, user, db)

    if not slot.phases:
        raise HTTPException(
            status_code=400,
            detail="No cycle calculated yet. Call /calculate first.",
        )

    if not slot.cycle_state:
        raise HTTPException(
            status_code=400,
            detail="Cycle state missing. Call /calculate to (re-)initialise.",
        )

    # Build mutable phase dicts from the DB rows so the engine can mutate them
    phase_dicts = [
        {
            "phase_num":       p.phase_num,
            "recovery":        p.recovery,
            "tp_value":        p.tp_value,
            "lot":             p.lot,
            "sl_base_pips":    p.sl_base_pips,
            "loss_real":       p.loss_real,
            "disallineamento": p.disallineamento,
            "trades":          [dict(t) for t in p.trades],  # deep-copy
        }
        for p in slot.phases
    ]

    state = dict(slot.cycle_state)

    try:
        updated_state = apply_outcome(
            state=state,
            phases=phase_dicts,
            phase_idx=body.phase_idx,
            trade_idx=body.trade_idx,
            outcome=body.outcome,
            trades_per_phase=slot.trades_per_phase,
            num_phases=slot.num_phases,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))

    # Persist updated trade outcomes back to each CyclePhase row
    phase_map = {p.phase_num: p for p in slot.phases}
    for pd in phase_dicts:
        db_phase = phase_map.get(pd["phase_num"])
        if db_phase:
            db_phase.trades = pd["trades"]

    slot.cycle_state = updated_state

    db.commit()
    db.refresh(slot)
    return _slot_out(slot)


# DELETE /cycle/slots/{slot_id} ─── Delete slot ───────────────────────────────
@router.delete(
    "/slots/{slot_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="Permanently delete a slot and all its computed data",
)
def delete_slot(
    slot_id: int,
    user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """Mirrors JS `deleteSlot()`. Cascade deletes CyclePhase rows too."""
    slot = _get_slot_or_404(slot_id, user, db)
    db.delete(slot)
    db.commit()