"""NCAA hockey (MCH/WCH) opponent-adjusted goal-margin ratings.

Per the Phase-0 capture-contract finding in
:mod:`sportsdataverse.hockey.college_hockey_constants`, ESPN's college-hockey
``game_plays``/``summary`` feed carries no shot-level data, so this module
does **not** port the NHL xG/RAPM/GSAx suite -- it builds team-game
goals-for/against from the ESPN scoreboard and feeds the league-agnostic
:func:`sportsdataverse._common.ratings.iterative_opponent_adjust` fixed
point (the same MBB/NBA KenPom-style solver), unchanged.

League-agnostic core; :mod:`sportsdataverse.hockey.mch.mch_ratings` /
:mod:`sportsdataverse.hockey.wch.wch_ratings` are by-reference shims that
fix ``league``.
"""

from __future__ import annotations

import pandas as pd
import polars as pl

from sportsdataverse._common.ratings import iterative_opponent_adjust
from sportsdataverse.hockey.college_hockey_constants import get_college_hockey_constants

_GAME_RESULTS_SCHEMA: dict[str, pl.PolarsDataType] = {
    "game_id": pl.Utf8,
    "team_id": pl.Utf8,
    "opp_team_id": pl.Utf8,
    "goals_for": pl.Int64,
    "goals_against": pl.Int64,
    "is_home": pl.Boolean,
    "is_neutral": pl.Boolean,
}


def college_hockey_game_results(events: list[dict], *, league: str) -> pl.DataFrame:
    """Parse raw ESPN scoreboard ``events[]`` into one row per (game, team).

    Args:
        events: Raw ``events`` list from ``espn_{mch,wch}_scoreboard(...,
            return_parsed=False)`` (one or more dates, already concatenated
            by the caller). Only completed games are kept.
        league: ``"mch"`` or ``"wch"`` (validated via
            :func:`~sportsdataverse.hockey.college_hockey_constants.get_college_hockey_constants`).

    Returns:
        A polars frame with columns ``game_id, team_id, opp_team_id,
        goals_for, goals_against, is_home, is_neutral`` -- two rows per
        game (one per team). Empty/malformed input returns a zero-row frame
        with this schema.

    Example:
        Quick start::

            from sportsdataverse.hockey.mch import espn_mch_scoreboard
            from sportsdataverse.hockey.college_hockey_ratings import college_hockey_game_results
            raw = espn_mch_scoreboard(dates="20250118", return_parsed=False)
            games = college_hockey_game_results(raw.get("events", []), league="mch")
    """
    get_college_hockey_constants(league)  # validates the league slug
    rows: list[dict] = []
    for ev in events or []:
        comp = (ev.get("competitions") or [{}])[0]
        status = (comp.get("status") or {}).get("type", {})
        if not status.get("completed"):
            continue
        competitors = comp.get("competitors") or []
        if len(competitors) != 2:
            continue
        is_neutral = bool(comp.get("neutralSite", False))
        game_id = str(ev.get("id", comp.get("id", "")))
        try:
            scores = {c["homeAway"]: int(c["score"]) for c in competitors}
            team_ids = {c["homeAway"]: str(c["team"]["id"]) for c in competitors}
        except (KeyError, TypeError, ValueError):
            continue
        for side, other in (("home", "away"), ("away", "home")):
            rows.append(
                {
                    "game_id": game_id,
                    "team_id": team_ids[side],
                    "opp_team_id": team_ids[other],
                    "goals_for": scores[side],
                    "goals_against": scores[other],
                    "is_home": side == "home",
                    "is_neutral": is_neutral,
                }
            )
    if not rows:
        return pl.DataFrame(schema=_GAME_RESULTS_SCHEMA)
    return pl.DataFrame(rows, schema=_GAME_RESULTS_SCHEMA)


def college_hockey_ratings(
    events: list[dict],
    *,
    league: str,
    hfa: float | None = None,
    return_as_pandas: bool = False,
) -> pl.DataFrame | pd.DataFrame:
    """Opponent-adjusted goal-margin ratings from ESPN scoreboard events.

    Builds team-game goals-for/against via :func:`college_hockey_game_results`
    and adjusts it with the league-agnostic
    :func:`sportsdataverse._common.ratings.iterative_opponent_adjust` fixed
    point (MBB/NBA KenPom-style solver, unchanged).

    Args:
        events: Raw ESPN scoreboard ``events[]``, concatenated across
            however many dates the caller fetched (see
            :func:`sportsdataverse.hockey.mch.mch_ratings.mch_ratings` for
            a season-fetch convenience wrapper).
        league: ``"mch"`` or ``"wch"``.
        hfa: Home-ice edge in goals/game. Defaults to the league's
            :class:`~sportsdataverse.hockey.college_hockey_constants.CollegeHockeyConstants.hfa_goals`.
        return_as_pandas: Return pandas instead of polars.

    Returns:
        One row per team: ``team_id, adj_off, adj_def, adj_net, raw_off,
        raw_def, games``. Empty input returns that schema with zero rows.

    Example:
        Quick start::

            from sportsdataverse.hockey.college_hockey_ratings import college_hockey_ratings
            ratings = college_hockey_ratings(events, league="mch")
            ratings.sort("adj_net", descending=True).head()
    """
    c = get_college_hockey_constants(league)
    games = college_hockey_game_results(events, league=league)
    out = iterative_opponent_adjust(
        games,
        team_col="team_id",
        opp_col="opp_team_id",
        off_col="goals_for",
        def_col="goals_against",
        home_col="is_home",
        neutral_col="is_neutral",
        hfa=hfa if hfa is not None else c.hfa_goals,
    )
    return out.to_pandas() if return_as_pandas else out
