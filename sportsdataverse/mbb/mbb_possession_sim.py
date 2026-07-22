"""MBB possession simulation — the shared basketball engine, MBB rules.

Thin league shim over ``sportsdataverse.nba.nba_possession_sim`` (the reference
one-engine/league-arg pattern): ESPN summary plays classify through the
shared adapter, the shelf builder is unchanged, and every simulation entry
point pins :data:`~sportsdataverse.nba.nba_possession_sim.rules.MBB_RULES`
(2 x 20:00 halves).
"""

from __future__ import annotations

from typing import Any, Dict, Optional, Tuple

import numpy as np

from sportsdataverse.nba.nba_possession_sim import (
    MBB_RULES,
    GameState,
    Shelf,
    build_shelf,
    simulate_ensemble,
    simulate_game_pbp,
)
from sportsdataverse.nba.nba_possession_sim.espn_adapter import espn_summary_to_events

RULES = MBB_RULES

__all__ = ["RULES", "mbb_shelf_from_espn_summary", "mbb_simulate_ensemble", "mbb_simulate_game_pbp"]


def mbb_shelf_from_espn_summary(summary: Dict[str, Any]) -> Shelf:
    """Build a MBB PMF shelf from a real ESPN summary payload.

    Args:
        summary: Site v2 ``summary`` dict with ``plays`` + ``header``.

    Returns:
        The populated shelf.

    Example:
        Quick start::

            import json
            from sportsdataverse.wnba.mbb_possession_sim import mbb_shelf_from_espn_summary
            shelf = mbb_shelf_from_espn_summary(json.load(open("summary.json")))
    """
    return build_shelf(espn_summary_to_events(summary))


def mbb_simulate_ensemble(shelf: Shelf, **kwargs: Any) -> Dict[str, Any]:
    """MBB Monte Carlo ensemble (see the shared engine for kwargs).

    Args:
        shelf: The MBB shelf.
        **kwargs: Forwarded to the shared ``simulate_ensemble``.

    Returns:
        The ensemble dict (score/total/margin vectors, win prob, ...).
    """
    return simulate_ensemble(shelf, rules=RULES, **kwargs)


def mbb_simulate_game_pbp(
    shelf: Shelf,
    rng: np.random.Generator,
    *,
    start: Optional[GameState] = None,
    **kwargs: Any,
) -> Tuple[GameState, "list[dict[str, Any]]"]:
    """Simulate one MBB game with its full play-by-play log.

    Args:
        shelf: The MBB shelf.
        rng: Numpy generator.
        start: Optional resume state.
        **kwargs: Forwarded to the shared ``simulate_game_pbp``.

    Returns:
        ``(final_state, pbp_rows)``.
    """
    return simulate_game_pbp(shelf, rng, rules=RULES, start=start, **kwargs)
