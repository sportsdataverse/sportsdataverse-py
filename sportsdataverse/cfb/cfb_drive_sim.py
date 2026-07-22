"""CFB drive/play simulation — the shared football engine, CFB data.

Thin league shim over ``sportsdataverse.nfl.nfl_drive_sim`` (the same
one-engine/league-arg pattern as the basketball family): ESPN CFB
summaries ship the identical ``drives.previous[].plays`` shape, so the
classifier, shelf builder, and engine run unchanged on college data.
The college-specific overtime format (alternating 25-yard possessions)
is a documented seam — v1 resolves ties with the shared engine's single
alternating round.
"""

from __future__ import annotations

from typing import Any, Dict, List, Tuple

import numpy as np
import polars as pl

from sportsdataverse.nfl.nfl_drive_sim import (
    FootballShelf,
    FootballState,
    build_football_shelf,
    plays_from_espn_drives,
    simulate_football_ensemble,
    simulate_football_game_pbp,
)

__all__ = [
    "cfb_plays_from_espn_drives",
    "cfb_shelf_from_espn_summary",
    "cfb_simulate_ensemble",
    "cfb_simulate_game_pbp",
]


def cfb_plays_from_espn_drives(summary: Dict[str, Any]) -> pl.DataFrame:
    """Classify a CFB summary's drive plays (see the shared classifier).

    Args:
        summary: Site v2 CFB ``summary`` with ``drives.previous[]``.

    Returns:
        The classified play frame.
    """
    return plays_from_espn_drives(summary)


def cfb_shelf_from_espn_summary(summary: Dict[str, Any]) -> FootballShelf:
    """Build a CFB football shelf from a real ESPN summary payload.

    Args:
        summary: Site v2 CFB ``summary`` dict.

    Returns:
        The populated shelf.

    Example:
        Quick start::

            import json
            from sportsdataverse.cfb.cfb_drive_sim import cfb_shelf_from_espn_summary
            shelf = cfb_shelf_from_espn_summary(json.load(open("summary_cfb.json")))
    """
    return build_football_shelf(plays_from_espn_drives(summary))


def cfb_simulate_ensemble(shelf: FootballShelf, **kwargs: Any) -> Dict[str, Any]:
    """CFB Monte Carlo ensemble (see the shared engine for kwargs).

    Args:
        shelf: The CFB shelf.
        **kwargs: Forwarded to the shared ``simulate_football_ensemble``.

    Returns:
        The ensemble dict.
    """
    kwargs.setdefault("college_ot", True)
    return simulate_football_ensemble(shelf, **kwargs)


def cfb_simulate_game_pbp(shelf: FootballShelf, rng: np.random.Generator) -> Tuple[FootballState, List[Dict[str, Any]]]:
    """Simulate one CFB game with its full play-by-play log (college OT).

    Args:
        shelf: The CFB shelf.
        rng: Numpy generator.

    Returns:
        ``(final_state, pbp_rows)``.
    """
    return simulate_football_game_pbp(shelf, rng, college_ot=True)
