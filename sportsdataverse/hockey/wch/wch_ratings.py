"""WCH (women's college hockey) opponent-adjusted ratings -- by-reference shim.

Fixes ``league="wch"`` on the league-agnostic
:mod:`sportsdataverse.hockey.college_hockey_ratings` core.

**Coverage caveat:** a full 2024-25-season date sweep of ESPN's WCH
scoreboard (``dev/league_ports/capture_wch_and_scoreboards.py``) found
completed games on only 7 dates, all inside the March 2025 NCAA Tournament
bracket (8 teams). ESPN does not appear to populate WCH regular-season
scoreboards for that season in this environment, so ``wch_ratings`` over a
regular-season date range may return few or no games -- callers should not
assume the same season coverage as :func:`sportsdataverse.hockey.mch.mch_ratings.mch_ratings`.
"""

from __future__ import annotations

import pandas as pd
import polars as pl

from sportsdataverse.hockey.college_hockey_ratings import college_hockey_ratings
from sportsdataverse.hockey.wch.wch_espn_ext import espn_wch_scoreboard

__all__ = ["wch_ratings"]


def wch_ratings(
    dates: list[str],
    *,
    return_as_pandas: bool = False,
) -> pl.DataFrame | pd.DataFrame:
    """WCH opponent-adjusted goal-margin ratings over a set of scoreboard dates.

    See the module docstring's coverage caveat -- ESPN's WCH scoreboard
    coverage observed during this port was tournament-only.

    Args:
        dates: ``YYYYMMDD`` date strings to fetch.
        return_as_pandas: Return pandas instead of polars.

    Returns:
        One row per team: ``team_id, adj_off, adj_def, adj_net, raw_off,
        raw_def, games``.

    Example:
        Quick start::

            from sportsdataverse.hockey.wch import wch_ratings
            ratings = wch_ratings(["20250315", "20250321", "20250322", "20250323"])
            ratings.sort("adj_net", descending=True).head()
    """
    events: list[dict] = []
    for d in dates:
        raw = espn_wch_scoreboard(dates=d, return_parsed=False)
        events.extend(raw.get("events", []))
    return college_hockey_ratings(events, league="wch", return_as_pandas=return_as_pandas)
