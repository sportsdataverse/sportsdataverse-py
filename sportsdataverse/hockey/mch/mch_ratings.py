"""MCH (men's college hockey) opponent-adjusted ratings -- by-reference shim.

Fixes ``league="mch"`` on the league-agnostic
:mod:`sportsdataverse.hockey.college_hockey_ratings` core.
"""

from __future__ import annotations

import pandas as pd
import polars as pl

from sportsdataverse.hockey.college_hockey_ratings import college_hockey_ratings
from sportsdataverse.hockey.mch.mch_espn_ext import espn_mch_scoreboard

__all__ = ["mch_ratings"]


def mch_ratings(
    dates: list[str],
    *,
    return_as_pandas: bool = False,
) -> pl.DataFrame | pd.DataFrame:
    """MCH opponent-adjusted goal-margin ratings over a set of scoreboard dates.

    Fetches ``espn_mch_scoreboard`` for each date in ``dates``, concatenates
    the completed games, and adjusts with
    :func:`sportsdataverse.hockey.college_hockey_ratings.college_hockey_ratings`.

    Args:
        dates: ``YYYYMMDD`` date strings to fetch (ESPN has no single
            "whole season" scoreboard endpoint; the caller supplies the
            date sweep -- see ``dev/league_ports/capture_wch_and_scoreboards.py``
            for the sweep used to build the committed oracle fixture).
        return_as_pandas: Return pandas instead of polars.

    Returns:
        One row per team: ``team_id, adj_off, adj_def, adj_net, raw_off,
        raw_def, games``.

    Example:
        Quick start::

            from sportsdataverse.hockey.mch import mch_ratings
            ratings = mch_ratings(["20250118", "20250201"])
            ratings.sort("adj_net", descending=True).head()
    """
    events: list[dict] = []
    for d in dates:
        raw = espn_mch_scoreboard(dates=d, return_parsed=False)
        events.extend(raw.get("events", []))
    return college_hockey_ratings(events, league="mch", return_as_pandas=return_as_pandas)
