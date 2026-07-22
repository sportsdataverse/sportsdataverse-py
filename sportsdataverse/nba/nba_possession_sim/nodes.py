"""Possession event tree — one small node class per event (WS4).

The reference NBA ``RootNode`` pattern at v1 granularity: an
:class:`OutcomeNode` samples the possession outcome from the shelf PMF for
the current gamestate, a :class:`ReboundNode` decides whether a miss
continues the possession, and a :class:`FreeThrowNode` resolves trip makes.
The monolithic 100KB ``Game``-class shape is deliberately rejected — new
events (and-1 chains, player attribution) land as new node classes, not
branches inside a god method.
"""

from __future__ import annotations

import dataclasses
from typing import Dict, List, Optional, Tuple

import numpy as np

from sportsdataverse.nba.nba_possession_sim.factors import FactorAdjustment
from sportsdataverse.nba.nba_possession_sim.shelf import OUTCOMES, Shelf

_POINTS: Dict[str, int] = {
    "rim_make": 2,
    "mid_make": 2,
    "three_make": 3,
}
_MISSES = ("rim_miss", "mid_miss", "three_miss")


@dataclasses.dataclass
class PossessionState:
    """Mutable state carried through one simulated possession.

    Attributes:
        score_diff: Offense-perspective differential entering the possession.
        period: Current period.
        clock_seconds: Seconds remaining in the period.
        points: Points scored so far this possession.
        events: Outcome trail (for conservation checks / debugging).
    """

    score_diff: float
    period: int
    clock_seconds: float
    points: int = 0
    events: List[str] = dataclasses.field(default_factory=list)


class OutcomeNode:
    """Samples the possession outcome from the shelf's gamestate PMF."""

    def sample(
        self,
        shelf: Shelf,
        state: PossessionState,
        rng: np.random.Generator,
        factors: "Optional[FactorAdjustment]" = None,
        key: Optional[str] = None,
    ) -> str:
        """Draw one outcome for the current gamestate.

        Args:
            shelf: The PMF shelf.
            state: Current possession state.
            rng: Numpy generator.
            factors: Optional auditable PMF adjustment applied pre-draw.

        Returns:
            One of :data:`~sportsdataverse.nba.nba_possession_sim.shelf.OUTCOMES`.
        """
        key = key or shelf.key_for(state.score_diff, state.period, state.clock_seconds)
        pmf, _ = shelf.get_pmf(key)
        if factors is not None:
            pmf = factors.adjust(pmf)
        probs = np.array([pmf[o] for o in OUTCOMES], dtype=float)
        total = probs.sum()
        if total <= 0:  # pragma: no cover - a built shelf always has mass
            probs = np.full(len(OUTCOMES), 1.0 / len(OUTCOMES))
        else:
            probs = probs / total
        # cum-sum inverse-CDF draw (the whole cython _choice in one line)
        idx = int(np.searchsorted(np.cumsum(probs), rng.random()))
        return OUTCOMES[min(idx, len(OUTCOMES) - 1)]


class ReboundNode:
    """Decides whether a missed shot is rebounded by the offense."""

    def sample(self, shelf: Shelf, rng: np.random.Generator, key: Optional[str] = None) -> bool:
        """True = offensive rebound (per-key model rate when fitted)."""
        return bool(rng.random() < shelf.get_oreb(key))


class FreeThrowNode:
    """Resolves a free-throw trip into made points."""

    def sample(self, shelf: Shelf, n_attempts: int, rng: np.random.Generator) -> int:
        """Binomial trip makes at the shelf's empirical FT percentage."""
        return int(rng.binomial(n_attempts, shelf.ft_pct))


def simulate_possession(
    shelf: Shelf,
    *,
    score_diff: float,
    period: int,
    clock_seconds: float,
    rng: np.random.Generator,
    factors: Optional[FactorAdjustment] = None,
) -> Tuple[int, List[str]]:
    """Walk the event tree for one possession.

    Args:
        shelf: The PMF shelf.
        score_diff: Offense-perspective differential entering the possession.
        period: Current period.
        clock_seconds: Seconds remaining in the period.
        rng: Numpy generator.

    Returns:
        ``(points, events)`` — offense points scored and the outcome trail.

    Example:
        Quick start::

            import numpy as np
            from sportsdataverse.nba.nba_possession_sim.nodes import simulate_possession
            pts, trail = simulate_possession(
                shelf, score_diff=0, period=1, clock_seconds=600.0,
                rng=np.random.default_rng(7),
            )
    """
    state = PossessionState(score_diff=score_diff, period=period, clock_seconds=clock_seconds)
    outcome_node = OutcomeNode()
    rebound_node = ReboundNode()
    ft_node = FreeThrowNode()

    key = shelf.key_for(state.score_diff, state.period, state.clock_seconds)
    while True:
        outcome = outcome_node.sample(shelf, state, rng, factors, key)
        state.events.append(outcome)
        if outcome in _POINTS:
            state.points += _POINTS[outcome]
            break
        if outcome == "tov":
            break
        if outcome in _MISSES:
            if rebound_node.sample(shelf, rng, key):
                state.events.append("oreb")
                continue
            state.events.append("dreb")
            break
        if outcome.startswith("ft_trip_"):
            n_attempts = int(outcome.rsplit("_", 1)[1])
            made = ft_node.sample(shelf, n_attempts, rng)
            state.points += made
            state.events.append(f"ft_made_{made}")
            break
    return state.points, state.events
