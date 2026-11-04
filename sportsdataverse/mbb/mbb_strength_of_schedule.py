"""Strength of schedule, NET-style Quad résumé, and Wins Above Bubble.

Phase 4 of the MBB/WBB prediction & tournament stack. Consumes the
opponent-adjusted ratings (``mbb_team_ratings``) and a season's completed
games; produces the per-team résumé the selection/bracketology phases rank
teams by. League-specific quad thresholds and the bubble AdjEM live in
:func:`sportsdataverse.mbb.mbb_prediction_constants.get_constants`.
"""

from __future__ import annotations

import datetime
from collections.abc import Iterable
from typing import Literal, overload

import pandas as pd
import polars as pl

from sportsdataverse.mbb.mbb_prediction_constants import get_constants

__all__ = [
    "mbb_strength_of_schedule",
    "strength_of_schedule",
]

_SOS_SCHEMA = {
    "season": pl.Int64,
    "team_id": pl.Utf8,
    "sos": pl.Float64,
    "sos_rank": pl.Int64,
    "wab": pl.Float64,
    "quad1_w": pl.Int64,
    "quad1_l": pl.Int64,
    "quad2_w": pl.Int64,
    "quad2_l": pl.Int64,
    "quad3_w": pl.Int64,
    "quad3_l": pl.Int64,
    "quad4_w": pl.Int64,
    "quad4_l": pl.Int64,
    "quality_wins": pl.Int64,
}


def _team_game_long(results: pl.DataFrame) -> pl.DataFrame:
    """Two rows per game: each side's (team, opp, venue, won)."""
    base = results.select(
        "game_id",
        pl.col("season").cast(pl.Int64),
        pl.col("home_team_id"),
        pl.col("away_team_id"),
        (pl.col("home_score") > pl.col("away_score")).alias("home_won"),
        pl.col("neutral_site").cast(pl.Boolean),
    )
    home = base.select(
        "game_id",
        "season",
        pl.col("home_team_id").alias("team_id"),
        pl.col("away_team_id").alias("opp_team_id"),
        pl.col("home_won").alias("won"),
        pl.when(pl.col("neutral_site") == True).then(pl.lit("neutral")).otherwise(pl.lit("home")).alias("venue"),  # noqa: E712
    )
    away = base.select(
        "game_id",
        "season",
        pl.col("away_team_id").alias("team_id"),
        pl.col("home_team_id").alias("opp_team_id"),
        (pl.col("home_won") == False).alias("won"),  # noqa: E712
        pl.when(pl.col("neutral_site") == True).then(pl.lit("neutral")).otherwise(pl.lit("away")).alias("venue"),  # noqa: E712
    )
    return pl.concat([home, away])


def strength_of_schedule(
    results: pl.DataFrame,
    ratings: pl.DataFrame,
    *,
    league: str = "mens",
) -> pl.DataFrame:
    """Per-team SoS + Quad 1-4 record + WAB from completed games and ratings.

    Args:
        results: Completed games with ``game_id, season, home_team_id,
            away_team_id, home_score, away_score, neutral_site``.
        ratings: One row per team with ``season, team_id, adj_em, rank``
            (the :func:`mbb_team_ratings` output). Team-id dtype must match
            ``results``.
        league: ``"mens"`` or ``"womens"`` (quad thresholds, HFA, bubble EM).

    Returns:
        One row per (season, team_id): ``season, team_id, sos, sos_rank, wab,
        quad1_w .. quad4_l, quality_wins``. ``sos`` is the mean opponent
        ``adj_em`` (rank 1 = hardest schedule); quads follow the NET
        venue-adjusted opponent-rank thresholds; ``quality_wins`` is Quad-1 +
        Quad-2 wins; ``wab`` is actual wins minus a bubble-quality team's
        expected wins against the same schedule. Empty input returns the
        schema with zero rows.

    Raises:
        ValueError: If the team-id dtypes of ``results`` and ``ratings``
            disagree.

    Example:
        Quick start::

            from sportsdataverse.mbb.mbb_strength_of_schedule import strength_of_schedule
            resume = strength_of_schedule(results, ratings)
    """
    for key in ("home_team_id", "away_team_id"):
        if results.schema[key] != ratings.schema["team_id"]:
            raise ValueError(
                f"join-key dtype mismatch: results[{key!r}] is {results.schema[key]} "
                f"but ratings['team_id'] is {ratings.schema['team_id']}"
            )
    if results.height == 0:
        return pl.DataFrame(schema=_SOS_SCHEMA)

    c = get_constants(league)
    thr = c.quad_thresholds
    long = _team_game_long(results).join(
        ratings.select(
            pl.col("team_id").alias("opp_team_id"),
            pl.col("adj_em").alias("opp_adj_em"),
            pl.col("rank").alias("opp_rank"),
        ),
        on="opp_team_id",
        how="inner",
    )

    bound = {
        q: pl.col("venue").replace_strict({v: thr[v][q] for v in thr}, return_dtype=pl.Int64)
        for q in ("q1", "q2", "q3")
    }
    long = long.with_columns(
        pl.when(pl.col("opp_rank") <= bound["q1"])
        .then(1)
        .when(pl.col("opp_rank") <= bound["q2"])
        .then(2)
        .when(pl.col("opp_rank") <= bound["q3"])
        .then(3)
        .otherwise(4)
        .alias("quad")
    )

    # bubble expected win prob per game, from the bubble team's perspective
    hfa_adj = pl.when(pl.col("venue") == "home").then(c.hfa).when(pl.col("venue") == "away").then(-c.hfa).otherwise(0.0)
    bubble_margin = c.em_scale * (c.bubble_adj_em - pl.col("opp_adj_em")) + hfa_adj
    long = long.with_columns(
        ((bubble_margin / c.margin_sd).map_batches(_norm_cdf, return_dtype=pl.Float64)).alias("p_bubble")
    )

    agg = [
        pl.col("opp_adj_em").mean().alias("sos"),
        (pl.col("won").cast(pl.Int64).sum() - pl.col("p_bubble").sum()).alias("wab"),
    ]
    for q in (1, 2, 3, 4):
        agg.append(((pl.col("quad") == q) & (pl.col("won") == True)).cast(pl.Int64).sum().alias(f"quad{q}_w"))  # noqa: E712
        agg.append(((pl.col("quad") == q) & (pl.col("won") == False)).cast(pl.Int64).sum().alias(f"quad{q}_l"))  # noqa: E712

    out = (
        long.group_by("season", "team_id")
        .agg(agg)
        .with_columns(
            pl.col("sos").rank(method="min", descending=True).over("season").cast(pl.Int64).alias("sos_rank"),
            (pl.col("quad1_w") + pl.col("quad2_w")).alias("quality_wins"),
        )
        .select(*_SOS_SCHEMA)
        .sort("season", "sos_rank")
    )
    return out


def _norm_cdf(s: pl.Series) -> pl.Series:
    from scipy.stats import norm

    return pl.Series(norm.cdf(s.to_numpy()))


@overload
def mbb_strength_of_schedule(
    seasons: list[int],
    *,
    as_of_date: datetime.date | None = None,
    league: str = "mens",
    ineligible: Iterable[str] = (),
    return_as_pandas: Literal[False] = False,
) -> pl.DataFrame: ...


@overload
def mbb_strength_of_schedule(
    seasons: list[int],
    *,
    as_of_date: datetime.date | None = None,
    league: str = "mens",
    ineligible: Iterable[str] = (),
    return_as_pandas: Literal[True],
) -> pd.DataFrame: ...


def mbb_strength_of_schedule(
    seasons: list[int],
    *,
    as_of_date: datetime.date | None = None,
    league: str = "mens",
    ineligible: Iterable[str] = (),
    return_as_pandas: bool = False,
) -> pl.DataFrame | pd.DataFrame:
    """The daily ``strength_of_schedule`` table: records, SoS, quad résumé, WAB, RPI and a NET estimate.

    Loads the released ESPN schedule and team box per season and builds
    :func:`~sportsdataverse.mbb.mbb_college_season.season_resume` from the games strictly
    before ``as_of_date``: opponent-adjusted ratings, SoS (mean opponent AdjEM), WAB, the
    NCAA Quad 1-4 record (opponents ranked by the NET estimate), the NCAA RPI
    (25% WP / 50% OWP / 25% OOWP, location-weighted WP) and the NET-style estimate
    (``0.8 * z(AdjEM) + 0.2 * z(WAB)``). Division I teams only; RPI, SoS, WAB and the quads
    count Division I games only.

    Args:
        seasons: Seasons to compute (e.g. ``[2024]``).
        as_of_date: Only use games strictly before this date; ``None`` uses every completed
            game. Applies to every season in ``seasons``.
        league: ``"mens"`` or ``"womens"``.
        ineligible: Team ids barred from the NCAA tournament (sets ``eligible``).
        return_as_pandas: Return a pandas DataFrame instead of polars.

    Returns:
        One row per Division I (season, team_id), best ``net_estimate_rank`` first. The
        ``_rank`` / ``_pct`` columns (1 is best, average ties, within the season) and
        ``_n`` (the Division I games behind each number; all games for ``adj_em``) follow
        BB-D2's league-context grammar.

    Example:
        Quick start::

            from sportsdataverse.mbb import mbb_strength_of_schedule
            resume = mbb_strength_of_schedule([2024])

        Pipeline next step (one line)::

            resume.sort("rpi_rank").head(25)

    See Also:
        * `hoopR <https://hoopR.sportsdataverse.org>`_ -- men's basketball (R)
        * `wehoop <https://wehoop.sportsdataverse.org>`_ -- women's basketball (R)
    """
    from sportsdataverse.mbb.mbb_college_season import load_season_inputs

    out = pl.concat(
        [load_season_inputs(s, as_of_date=as_of_date, league=league, ineligible=ineligible).resume for s in seasons]
    )
    return out.to_pandas() if return_as_pandas else out
