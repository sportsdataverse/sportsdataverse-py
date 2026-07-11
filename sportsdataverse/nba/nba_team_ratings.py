"""Opponent-adjusted team-rating engine (model ①) for the NBA prediction stack.

Computes opponent-adjusted offensive/defensive rating + pace from schedule
results + per-team box scores, as-of-date aware. The pieces:

* :func:`raw_game_efficiency` -- per-team, per-game possessions + raw
  offensive/defensive rating.
* :func:`adjust_efficiency` -- iterative opponent adjustment -> AdjOffRtg /
  AdjDefRtg / AdjNet (Task 1.2). *(T7.2-shared algorithm -- byte-identical
  fixed point to the MBB ``mbb_team_ratings.adjust_efficiency`` /
  CFB ``cfb_ratings`` cores; only the fitted constants differ.)*
* :func:`adjust_pace` -- opponent-adjusted pace (Task 1.3).
* :func:`nba_team_ratings` -- public per-team-season, as-of-date-aware entry
  point (Task 1.4).

Algorithms here are league-agnostic (``league_id``); league-specific
constants (HFA, pace/efficiency baselines) come from
:func:`sportsdataverse.nba.nba_prediction_constants.get_constants`.
"""

from __future__ import annotations

import datetime as dt
from typing import TYPE_CHECKING, Literal, Union, overload

import numpy as np
import polars as pl

from sportsdataverse._common.ratings import iterative_opponent_adjust
from sportsdataverse.nba.nba_loaders import load_nba_schedule, load_nba_team_boxscore
from sportsdataverse.nba.nba_prediction_constants import as_of_ratings_split, get_constants

if TYPE_CHECKING:
    import pandas as pd

__all__ = [
    "adjust_efficiency",
    "adjust_pace",
    "nba_team_ratings",
    "raw_game_efficiency",
]

_EFF_SCHEMA = {
    "game_id": pl.Utf8,
    "season": pl.Int64,
    "date": pl.Date,
    "team_id": pl.Utf8,
    "opp_team_id": pl.Utf8,
    "is_home": pl.Boolean,
    "neutral_site": pl.Boolean,
    "poss": pl.Float64,
    "off_rtg": pl.Float64,
    "def_rtg": pl.Float64,
}


def raw_game_efficiency(schedule: pl.DataFrame, team_box: pl.DataFrame) -> pl.DataFrame:
    """Per-team, per-game possessions + raw offensive/defensive rating.

    Possessions use the standard box estimate averaged over both teams::

        team_poss = FGA - OREB + TO + 0.44 * FTA
        poss      = 0.5 * (team_poss + opp_poss)
        off_rtg   = 100 * pts     / poss
        def_rtg   = 100 * opp_pts / poss

    The opponent is derived by self-joining ``team_box`` on ``game_id`` (each
    game has exactly two team rows); home/neutral context comes from
    ``schedule``.

    Args:
        schedule: Frame with ``game_id, season, date, home_team_id,
            away_team_id, neutral_site`` (ids cast to ``Utf8`` here).
        team_box: Per-team box score with ``game_id, team_id,
            field_goals_attempted, offensive_rebounds, turnovers,
            free_throws_attempted, team_score``.

    Returns:
        One row per (game_id, team_id): ``game_id, season, date, team_id,
        opp_team_id, is_home, neutral_site, poss, off_rtg, def_rtg``. Empty
        input returns that schema with zero rows.

    Example:
        Quick start::

            from sportsdataverse.nba.nba_loaders import load_nba_schedule, load_nba_team_boxscore
            from sportsdataverse.nba.nba_team_ratings import raw_game_efficiency
            eff = raw_game_efficiency(load_nba_schedule([2024]), load_nba_team_boxscore([2024]))
    """
    if schedule.height == 0 or team_box.height == 0:
        return pl.DataFrame(schema=_EFF_SCHEMA)

    box = team_box.select(
        pl.col("game_id").cast(pl.Utf8),
        pl.col("team_id").cast(pl.Utf8),
        pl.col("field_goals_attempted").cast(pl.Float64).alias("fga"),
        pl.col("offensive_rebounds").cast(pl.Float64).alias("orb"),
        pl.col("turnovers").cast(pl.Float64).alias("tov"),
        pl.col("free_throws_attempted").cast(pl.Float64).alias("fta"),
        pl.col("team_score").cast(pl.Float64).alias("pts"),
    ).with_columns((pl.col("fga") - pl.col("orb") + pl.col("tov") + 0.44 * pl.col("fta")).alias("team_poss"))

    opp = box.select(
        "game_id",
        pl.col("team_id").alias("opp_team_id"),
        pl.col("team_poss").alias("opp_poss"),
        pl.col("pts").alias("opp_pts"),
    )
    paired = (
        box.join(opp, on="game_id")
        .filter(pl.col("team_id") != pl.col("opp_team_id"))
        .with_columns((0.5 * (pl.col("team_poss") + pl.col("opp_poss"))).alias("poss"))
        .with_columns(
            (100.0 * pl.col("pts") / pl.col("poss")).alias("off_rtg"),
            (100.0 * pl.col("opp_pts") / pl.col("poss")).alias("def_rtg"),
        )
    )

    sched = schedule.select(
        pl.col("game_id").cast(pl.Utf8),
        pl.col("season").cast(pl.Int64),
        pl.col("date").cast(pl.Date),
        pl.col("home_team_id").cast(pl.Utf8),
        pl.col("away_team_id").cast(pl.Utf8),
        pl.col("neutral_site").cast(pl.Boolean),
    )

    return (
        paired.join(sched, on="game_id", how="inner")
        .with_columns((pl.col("team_id") == pl.col("home_team_id")).alias("is_home"))
        .select(
            "game_id",
            "season",
            "date",
            "team_id",
            "opp_team_id",
            "is_home",
            "neutral_site",
            "poss",
            "off_rtg",
            "def_rtg",
        )
        .sort("game_id", "team_id")
    )


_ADJ_SCHEMA = {
    "season": pl.Int64,
    "team_id": pl.Utf8,
    "adj_off_rtg": pl.Float64,
    "adj_def_rtg": pl.Float64,
    "adj_net_rtg": pl.Float64,
    "raw_off_rtg": pl.Float64,
    "raw_def_rtg": pl.Float64,
    "games": pl.Int64,
}


def _adjust_one_season(
    sub: pl.DataFrame, season: int, hfa: float, avg: float, max_iter: int, tol: float
) -> pl.DataFrame:
    """Fixed-point opponent adjustment for one season's game-efficiency rows.

    Thin wrapper (T7.2): the fixed-point core moved verbatim to
    :func:`sportsdataverse._common.ratings.iterative_opponent_adjust`
    (``baseline=avg`` reproduces this module's fitted-constant average, as
    opposed to MBB's data-derived mean); only the column rename to this
    module's public names stays local.
    """
    core = iterative_opponent_adjust(
        sub,
        team_col="team_id",
        opp_col="opp_team_id",
        off_col="off_rtg",
        def_col="def_rtg",
        home_col="is_home",
        neutral_col="neutral_site",
        hfa=hfa,
        baseline=avg,
        max_iter=max_iter,
        tol=tol,
    )
    return (
        core.rename(
            {
                "adj_off": "adj_off_rtg",
                "adj_def": "adj_def_rtg",
                "adj_net": "adj_net_rtg",
                "raw_off": "raw_off_rtg",
                "raw_def": "raw_def_rtg",
            }
        )
        .with_columns(pl.lit(season, dtype=pl.Int64).alias("season"))
        .select("season", "team_id", "adj_off_rtg", "adj_def_rtg", "adj_net_rtg", "raw_off_rtg", "raw_def_rtg", "games")
    )


def adjust_efficiency(
    game_eff: pl.DataFrame, *, league_id: str = "00", max_iter: int = 100, tol: float = 1e-4
) -> pl.DataFrame:
    """Iterative opponent-adjusted rating -> AdjOffRtg / AdjDefRtg / AdjNet per team-season.

    KenPom-style fixed point: initialize ``adj_off = raw_off`` /
    ``adj_def = raw_def``, then repeatedly recompute each team's rating from
    its games with the opponent's *current* adjusted rating and a
    home-court adjustment removed, until the largest change is below ``tol``.
    Ratings are computed independently per season.

    The per-game offensive update is
    ``off_rtg - (adj_def_opp - avg) - loc_o`` where ``loc_o`` is ``+hfa/2`` at
    home, ``-hfa/2`` away, ``0`` neutral (defense is symmetric with the
    opposite sign); ``avg`` is the league baseline off rating and ``hfa``
    comes from :func:`~sportsdataverse.nba.nba_prediction_constants.get_constants`.

    *(T7.2-shared algorithm)* -- identical fixed point to the MBB
    ``mbb_team_ratings.adjust_efficiency`` / CFB ratings cores.

    Args:
        game_eff: Output of :func:`raw_game_efficiency`.
        league_id: ``"00"`` NBA / ``"10"`` WNBA / ``"20"`` G-League -- selects
            the HFA + baseline off-rating constants.
        max_iter: Maximum fixed-point iterations.
        tol: Convergence tolerance on the largest rating change.

    Returns:
        One row per (season, team_id): ``season, team_id, adj_off_rtg,
        adj_def_rtg, adj_net_rtg, raw_off_rtg, raw_def_rtg, games``. Empty
        input returns that schema with zero rows.

    Example:
        Quick start::

            from sportsdataverse.nba.nba_team_ratings import adjust_efficiency, raw_game_efficiency
            ratings = adjust_efficiency(raw_game_efficiency(sched, box))
    """
    if game_eff.height == 0:
        return pl.DataFrame(schema=_ADJ_SCHEMA)
    c = get_constants(league_id)
    frames = [
        _adjust_one_season(sub, int(sub["season"][0]), c.hfa, c.avg_off_rtg, max_iter, tol)
        for _key, sub in game_eff.group_by("season", maintain_order=True)
    ]
    return pl.concat(frames)


_PACE_SCHEMA = {"season": pl.Int64, "team_id": pl.Utf8, "adj_pace": pl.Float64, "raw_pace": pl.Float64}


def _adjust_pace_one_season(sub: pl.DataFrame, season: int, avg: float, max_iter: int, tol: float) -> pl.DataFrame:
    """Fixed-point opponent-adjusted pace for one season's game rows."""
    teams = sub["team_id"].unique(maintain_order=True).to_list()
    index = {t: i for i, t in enumerate(teams)}
    n = len(teams)
    ti = np.array([index[t] for t in sub["team_id"].to_list()], dtype=np.int64)
    oi = np.array([index[t] for t in sub["opp_team_id"].to_list()], dtype=np.int64)
    poss = sub["poss"].to_numpy().astype(float)

    counts: np.ndarray = np.bincount(ti, minlength=n).astype(float)
    raw = np.bincount(ti, weights=poss, minlength=n) / counts

    adj = raw.copy()
    for _ in range(max_iter):
        contrib = poss - (adj[oi] - avg)
        new = np.bincount(ti, weights=contrib, minlength=n) / counts
        delta = float(np.abs(new - adj).max())
        adj = new
        if delta < tol:
            break

    return pl.DataFrame(
        {"season": [season] * n, "team_id": teams, "adj_pace": adj, "raw_pace": raw}, schema=_PACE_SCHEMA
    )


def adjust_pace(
    game_eff: pl.DataFrame, *, league_id: str = "00", max_iter: int = 100, tol: float = 1e-4
) -> pl.DataFrame:
    """Opponent-adjusted pace (possessions/game) per team-season.

    Same fixed point as :func:`adjust_efficiency`, applied to game
    possessions under the additive model ``poss = pace_i + pace_j - avg``: a
    team's pace is recovered by removing its opponents' current adjusted
    pace. ``avg`` is the league baseline pace from
    :func:`~sportsdataverse.nba.nba_prediction_constants.get_constants`.

    Args:
        game_eff: Output of :func:`raw_game_efficiency`.
        league_id: ``"00"`` / ``"10"`` / ``"20"`` -- selects the pace baseline.
        max_iter: Maximum fixed-point iterations.
        tol: Convergence tolerance on the largest pace change.

    Returns:
        One row per (season, team_id): ``season, team_id, adj_pace,
        raw_pace``. Empty input returns that schema with zero rows.

    Example:
        Quick start::

            from sportsdataverse.nba.nba_team_ratings import adjust_pace, raw_game_efficiency
            pace = adjust_pace(raw_game_efficiency(sched, box))
    """
    if game_eff.height == 0:
        return pl.DataFrame(schema=_PACE_SCHEMA)
    avg = get_constants(league_id).avg_pace
    frames = [
        _adjust_pace_one_season(sub, int(sub["season"][0]), avg, max_iter, tol)
        for _key, sub in game_eff.group_by("season", maintain_order=True)
    ]
    return pl.concat(frames)


_RATINGS_SCHEMA = {
    "season": pl.Int64,
    "team_id": pl.Utf8,
    "adj_off_rtg": pl.Float64,
    "adj_def_rtg": pl.Float64,
    "adj_net_rtg": pl.Float64,
    "adj_pace": pl.Float64,
    "raw_off_rtg": pl.Float64,
    "raw_def_rtg": pl.Float64,
    "raw_pace": pl.Float64,
    "games": pl.Int64,
    "rank": pl.Int64,
    "adj_net_z": pl.Float64,
}


def _league_loaders(league_id: str):  # type: ignore[no-untyped-def]
    """(schedule, team_box) loaders for a ``league_id``.

    ``"00"`` NBA / ``"20"`` G-League use the module-level ``load_nba_*`` (so
    monkeypatching ``mod.load_nba_schedule`` in tests still works, and G-League
    rides the same ESPN NBA loader surface); ``"10"`` WNBA uses the ``wnba``
    loaders. Algorithms stay league-agnostic -- only the constants + data
    source differ by ``league_id``.
    """
    if league_id == "10":
        from sportsdataverse.wnba.wnba_loaders import (  # noqa: PLC0415
            load_wnba_schedule,
            load_wnba_team_boxscore,
        )

        return load_wnba_schedule, load_wnba_team_boxscore
    return load_nba_schedule, load_nba_team_boxscore


def _normalize_schedule(schedule: pl.DataFrame) -> pl.DataFrame:
    """Map ESPN ``load_nba_schedule`` columns to the engine's canonical names.

    ESPN ships ``home_id``/``away_id`` and both a string ``date`` (with time)
    and a proper ``game_date``; the engine wants ``home_team_id``/
    ``away_team_id``/``date`` (a ``Date``). Frames already using the
    canonical names (e.g. the committed fixtures) pass through unchanged.
    """
    out = schedule
    if "home_id" in out.columns and "home_team_id" not in out.columns:
        out = out.rename({"home_id": "home_team_id"})
    if "away_id" in out.columns and "away_team_id" not in out.columns:
        out = out.rename({"away_id": "away_team_id"})
    if "game_date" in out.columns:
        if "date" in out.columns:
            out = out.drop("date")
        out = out.rename({"game_date": "date"})
    return out


@overload
def nba_team_ratings(
    seasons: Union[int, list[int]],
    *,
    league_id: str = "00",
    as_of_date: Union[dt.date, None] = None,
    return_as_pandas: Literal[False] = False,
) -> pl.DataFrame: ...


@overload
def nba_team_ratings(
    seasons: Union[int, list[int]],
    *,
    league_id: str = "00",
    as_of_date: Union[dt.date, None] = None,
    return_as_pandas: Literal[True],
) -> "pd.DataFrame": ...


def nba_team_ratings(
    seasons: Union[int, list[int]],
    *,
    league_id: str = "00",
    as_of_date: Union[dt.date, None] = None,
    return_as_pandas: bool = False,
) -> Union[pl.DataFrame, "pd.DataFrame"]:
    """Opponent-adjusted team ratings (AdjOffRtg/AdjDefRtg/AdjNet/AdjPace), as-of-date aware.

    Loads schedule + team box score for ``seasons``, optionally filters to
    games strictly before ``as_of_date`` (the leakage boundary, via
    :func:`~sportsdataverse.nba.nba_prediction_constants.as_of_ratings_split`),
    computes per-game efficiency, runs the opponent-adjustment fixed points,
    and adds a per-season dense ``rank`` (on ``adj_net_rtg`` descending) and
    ``adj_net_z`` (z-score of ``adj_net_rtg``).

    Args:
        seasons: A season (e.g. ``2024``) or list of seasons.
        league_id: ``"00"`` NBA / ``"10"`` WNBA / ``"20"`` G-League.
        as_of_date: If given, only games with ``date < as_of_date`` are used
            (predictive/backtest usage); ``None`` computes full-season
            descriptive ratings.
        return_as_pandas: Return a pandas frame instead of polars.

    Returns:
        One row per (season, team_id): ``season, team_id, adj_off_rtg,
        adj_def_rtg, adj_net_rtg, adj_pace, raw_off_rtg, raw_def_rtg,
        raw_pace, games, rank, adj_net_z``. Empty input returns that schema
        with zero rows.

    Example:
        Quick start::

            from sportsdataverse.nba.nba_team_ratings import nba_team_ratings
            ratings = nba_team_ratings(2024)
            ratings.sort("rank").head()

        As-of-date (leakage-safe) ratings for a backtest::

            import datetime as dt
            ratings = nba_team_ratings(2024, as_of_date=dt.date(2024, 1, 15))

        WNBA / G-League via ``league_id``::

            wnba_ratings = nba_team_ratings(2024, league_id="10")
    """
    seasons_list = [seasons] if isinstance(seasons, int) else list(seasons)
    load_schedule, load_team_box = _league_loaders(league_id)
    schedule = _normalize_schedule(load_schedule(seasons_list))
    team_box = load_team_box(seasons_list)
    if as_of_date is not None:
        schedule = as_of_ratings_split(schedule, as_of_date)
    eff = raw_game_efficiency(schedule, team_box)

    if eff.height == 0:
        out = pl.DataFrame(schema=_RATINGS_SCHEMA)
    else:
        ratings = adjust_efficiency(eff, league_id=league_id)
        pace = adjust_pace(eff, league_id=league_id)
        out = (
            ratings.join(pace, on=["season", "team_id"], how="left")
            .with_columns(
                pl.col("adj_net_rtg").rank(method="dense", descending=True).over("season").cast(pl.Int64).alias("rank"),
                (
                    (pl.col("adj_net_rtg") - pl.col("adj_net_rtg").mean().over("season"))
                    / pl.col("adj_net_rtg").std().over("season")
                ).alias("adj_net_z"),
            )
            .select(list(_RATINGS_SCHEMA))
            .sort(["season", "rank"])
        )

    return out.to_pandas() if return_as_pandas else out
