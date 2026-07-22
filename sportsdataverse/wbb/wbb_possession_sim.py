"""WBB possession simulation — the shared basketball engine, WBB rules.

Thin league shim over ``sportsdataverse.nba.nba_possession_sim`` (the reference
one-engine/league-arg pattern): ESPN summary plays classify through the
shared adapter, the shelf builder is unchanged, and every simulation entry
point pins :data:`~sportsdataverse.nba.nba_possession_sim.rules.WBB_RULES`
(4 x 10:00 quarters).
"""

from __future__ import annotations

from typing import Any, Dict, Optional, Tuple

import numpy as np

from sportsdataverse.nba.nba_possession_sim import (
    WBB_RULES,
    GameState,
    Shelf,
    build_shelf,
    simulate_ensemble,
    simulate_game_pbp,
)
from sportsdataverse.nba.nba_possession_sim.espn_adapter import espn_summary_to_events

RULES = WBB_RULES

__all__ = ["RULES", "wbb_shelf_from_espn_summary", "wbb_simulate_ensemble", "wbb_simulate_game_pbp"]


def wbb_shelf_from_espn_summary(summary: Dict[str, Any]) -> Shelf:
    """Build a WBB PMF shelf from a real ESPN summary payload.

    Args:
        summary: Site v2 ``summary`` dict with ``plays`` + ``header``.

    Returns:
        The populated shelf.

    Example:
        Quick start::

            import json
            from sportsdataverse.wnba.wbb_possession_sim import wbb_shelf_from_espn_summary
            shelf = wbb_shelf_from_espn_summary(json.load(open("summary.json")))
    """
    return build_shelf(espn_summary_to_events(summary))


def wbb_simulate_ensemble(shelf: Shelf, **kwargs: Any) -> Dict[str, Any]:
    """WBB Monte Carlo ensemble (see the shared engine for kwargs).

    Args:
        shelf: The WBB shelf.
        **kwargs: Forwarded to the shared ``simulate_ensemble``.

    Returns:
        The ensemble dict (score/total/margin vectors, win prob, ...).
    """
    return simulate_ensemble(shelf, rules=RULES, **kwargs)


def wbb_simulate_game_pbp(
    shelf: Shelf,
    rng: np.random.Generator,
    *,
    start: Optional[GameState] = None,
    **kwargs: Any,
) -> Tuple[GameState, "list[dict[str, Any]]"]:
    """Simulate one WBB game with its full play-by-play log.

    Args:
        shelf: The WBB shelf.
        rng: Numpy generator.
        start: Optional resume state.
        **kwargs: Forwarded to the shared ``simulate_game_pbp``.

    Returns:
        ``(final_state, pbp_rows)``.
    """
    return simulate_game_pbp(shelf, rng, rules=RULES, start=start, **kwargs)
