# app/services/cycle_engine.py
"""
Deterministic cycle calculator — pure Python port of calculateCycle() in the
FX Cycle Calculator frontend.  No side-effects; takes raw inputs, returns a
list of phase dicts ready to be stored in CyclePhase rows.
"""

from __future__ import annotations
from typing import List, Optional
import math


PIP_VALUE = 10      # $10 per pip per lot  (standard MT5 convention used in UI)
MIN_LOT   = 0.01


# ─── LOW-LEVEL HELPERS ────────────────────────────────────────────────────────

def _round2(val: float) -> float:
    return round(val * 100) / 100


def _round_lot(val: float) -> float:
    rounded = round(val * 100) / 100
    return max(rounded, MIN_LOT)


# ─── MAIN ENGINE ──────────────────────────────────────────────────────────────

def calculate_cycle(
    big_balance:      float,
    small_balance:    float,
    starting_pips:    int,
    num_phases:       int,
    trades_per_phase: int,
    losses:           List[float],          # one real-loss value per phase
) -> List[dict]:
    """
    Returns a list of phase dicts, each shaped like:
    {
        "phase_num":       int,
        "recovery":        float,
        "tp_value":        float,
        "lot":             float,
        "sl_base_pips":    int,
        "loss_real":       float,
        "disallineamento": float,
        "trades": [
            {
                "num":      int,
                "lot":      float,
                "tp_pips":  int,
                "sl_pips":  int,
                "tp_money": float,
                "sl_money": float,
                "outcome":  None,
            }, ...
        ]
    }
    """
    theoretical = _round2(big_balance / num_phases)
    phases = []

    for i in range(num_phases):
        loss_real = losses[i] if i < len(losses) else theoretical
        disallineamento = _round2(loss_real - theoretical)

        # Recovery = SMALL + sum of all preceding real losses
        recovery = small_balance
        for j in range(i):
            recovery = _round2(recovery + losses[j])

        tp_value = _round2(recovery / trades_per_phase)
        lot      = _round_lot(tp_value / (starting_pips * PIP_VALUE))
        tp_pips  = starting_pips
        sl_base  = math.ceil(loss_real / (lot * PIP_VALUE))   # mirrors JS Math.round

        trades = []
        for t in range(trades_per_phase):
            sl_pips  = sl_base + t * starting_pips
            tp_money = _round2(tp_pips * lot * PIP_VALUE)
            sl_money = _round2(sl_pips * lot * PIP_VALUE)
            trades.append({
                "num":      t + 1,
                "lot":      lot,
                "tp_pips":  tp_pips,
                "sl_pips":  sl_pips,
                "tp_money": tp_money,
                "sl_money": sl_money,
                "outcome":  None,
            })

        phases.append({
            "phase_num":       i + 1,
            "recovery":        recovery,
            "tp_value":        tp_value,
            "lot":             lot,
            "sl_base_pips":    sl_base,
            "loss_real":       loss_real,
            "disallineamento": disallineamento,
            "trades":          trades,
        })

    return phases


# ─── STATE MACHINE ────────────────────────────────────────────────────────────

def apply_outcome(
    state: dict,
    phases: List[dict],
    phase_idx: int,
    trade_idx: int,
    outcome: str,           # "TP" | "SL"
    trades_per_phase: int,
    num_phases: int,
) -> dict:
    """
    Advance the state machine exactly as the JS setOutcome() does.
    `state` is the current cycle_state dict (mutated and returned).
    `phases` is the list of phase dicts (trades[...].outcome is mutated).

    Returns the updated state dict.
    """
    # Guard: only allow action on the current active trade
    if state.get("cycle_winner"):
        raise ValueError("Cycle already completed")
    if phase_idx != state["current_phase"] or trade_idx != state["current_trade_index"]:
        raise ValueError(
            f"Expected phase {state['current_phase']} / trade {state['current_trade_index']}, "
            f"got phase {phase_idx} / trade {trade_idx}"
        )

    # Write the outcome into the phase data
    phases[phase_idx]["trades"][trade_idx]["outcome"] = outcome

    if outcome == "TP":
        state["consecutive_tp_count"] += 1
        if state["consecutive_tp_count"] >= trades_per_phase:
            state["cycle_winner"] = "BIG"
        else:
            state["current_trade_index"] += 1
            if state["current_trade_index"] >= trades_per_phase:
                # Move to next phase, reset trade pointer
                state["current_trade_index"] = 0
                state["current_phase"] = min(
                    state["current_phase"] + 1, num_phases - 1
                )
                state["consecutive_tp_count"] = 0

    elif outcome == "SL":
        state["sl_count"] += 1
        state["consecutive_tp_count"] = 0
        if state["sl_count"] >= num_phases:
            state["cycle_winner"] = "SMALL"
        else:
            state["current_phase"] = min(
                state["current_phase"] + 1, num_phases - 1
            )
            state["current_trade_index"] = 0

    # Rebuild the full outcomes matrix from the phase dicts
    state["outcomes"] = [
        [t["outcome"] for t in p["trades"]]
        for p in phases
    ]

    return state


def initial_state(num_phases: int, trades_per_phase: int) -> dict:
    """Fresh state for a brand-new calculated cycle."""
    return {
        "current_phase":        0,
        "current_trade_index":  0,
        "consecutive_tp_count": 0,
        "sl_count":             0,
        "cycle_winner":         None,
        "outcomes": [
            [None] * trades_per_phase
            for _ in range(num_phases)
        ],
    }