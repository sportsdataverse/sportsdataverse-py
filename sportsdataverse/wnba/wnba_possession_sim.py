"""WNBA possession simulation — the shared basketball engine, WNBA rules.

Thin league shim over ``sportsdataverse.nba.nba_possession_sim`` (the reference
one-engine/league-arg pattern): ESPN summary plays classify through the
shared adapter, the shelf builder is unchanged, and every simulation entry
point pins :data:`~sportsdataverse.nba.nba_possession_sim.rules.WNBA_RULES`
(4 x 10:00 quarters).
"""

from __future__ import annotations

from typing import Any, Dict, Optional, Tuple

import numpy as np

from sportsdataverse.nba.nba_possession_sim import (
    WNBA_RULES,
    GameState,
    Shelf,
    build_shelf,
    simulate_ensemble,
    simulate_game_pbp,
)
from sportsdataverse.nba.nba_possession_sim.espn_adapter import espn_summary_to_events

RULES = WNBA_RULES

__all__ = ["RULES", "wnba_shelf_from_espn_summary", "wnba_simulate_ensemble", "wnba_simulate_game_pbp"]


def wnba_shelf_from_espn_summary(summary: Dict[str, Any]) -> Shelf:
    """Build a WNBA PMF shelf from a real ESPN summary payload.

    Args:
        summary: Site v2 ``summary`` dict with ``plays`` + ``header``.

    Returns:
        The populated shelf.

    Example:
        Quick start::

            import json
            from sportsdataverse.wnba.wnba_possession_sim import wnba_shelf_from_espn_summary
            shelf = wnba_shelf_from_espn_summary(json.load(open("summary.json")))
    """
    return build_shelf(espn_summary_to_events(summary))


def wnba_simulate_ensemble(shelf: Shelf, **kwargs: Any) -> Dict[str, Any]:
    """WNBA Monte Carlo ensemble (see the shared engine for kwargs).

    Args:
        shelf: The WNBA shelf.
        **kwargs: Forwarded to the shared ``simulate_ensemble``.

    Returns:
        The ensemble dict (score/total/margin vectors, win prob, ...).
    """
    return simulate_ensemble(shelf, rules=RULES, **kwargs)


def wnba_simulate_game_pbp(
    shelf: Shelf,
    rng: np.random.Generator,
    *,
    start: Optional[GameState] = None,
    **kwargs: Any,
) -> Tuple[GameState, "list[dict[str, Any]]"]:
    """Simulate one WNBA game with its full play-by-play log.

    Args:
        shelf: The WNBA shelf.
        rng: Numpy generator.
        start: Optional resume state.
        **kwargs: Forwarded to the shared ``simulate_game_pbp``.

    Returns:
        ``(final_state, pbp_rows)``.
    """
    return simulate_game_pbp(shelf, rng, rules=RULES, start=start, **kwargs)
