"""Rating-based résumé metrics for college football (T2.1 Phase 3).

Complements -- does not replace -- the record-based SOV/SOS in
:mod:`sportsdataverse.cfb.cfb_standings` with rating-based strength of schedule,
quality wins, game control, and (Task 3.2) wins-above-bubble. All metrics are
derived from :func:`cfb_ratings.cfb_ratings` (team strength) joined onto the
played schedule (opponents + margins).

``load_cfb_schedule`` / ``cfb_ratings`` are imported at module scope so tests can
monkeypatch them on this module's namespace (same pattern as ``cfb_ratings``).
"""

from __future__ import annotations

import datetime
from typing import Literal, overload

import pandas as pd
import polars as pl
from scipy.stats import norm

from sportsdataverse.cfb.cfb_loaders import load_cfb_schedule
from sportsdataverse.cfb.cfb_prediction_constants import PredictConfig, get_constants
from sportsdataverse.cfb.cfb_ratings import cfb_ratings

__all__ = ["cfb_resume"]

_RESUME_SCHEMA: dict[str, pl.PolarsDataType] = {
    "season": pl.Int64,
    "team_id": pl.Utf8,
    "sos": pl.Float64,
    "sos_rank": pl.Int64,
    "quality_wins": pl.Int64,
    "game_control": pl.Float64,
}
_RESUME_COLUMNS = list(_RESUME_SCHEMA)

# Real load_cfb_schedule column names -> the normalized names the core consumes.
_SCHEDULE_RENAME = {
    "home_id": "home_team_id",
    "away_id": "away_team_id",
    "home_points": "home_score",
    "away_points": "away_score",
}


def _normalize_schedule(schedule: pl.DataFrame) -> pl.DataFrame:
    """Map the real loader's ``home_id`` / ``home_points`` names to the core's, and
    pin both team-id columns to ``Utf8`` (the ratings join-key dtype) at the boundary."""
    ren = {src: dst for src, dst in _SCHEDULE_RENAME.items() if src in schedule.columns}
    schedule = schedule.rename(ren)
    return schedule.with_columns(
        pl.col("home_team_id").cast(pl.Utf8),
        pl.col("away_team_id").cast(pl.Utf8),
    )


def _team_game_rows(schedule: pl.DataFrame, ratings: pl.DataFrame) -> pl.DataFrame:
    """Explode each completed game into two team-perspective rows + join opponent adj_net."""
    played = schedule.filter(pl.col("home_score").is_not_null() & pl.col("away_score").is_not_null())
    home = played.select(
        pl.col("home_team_id").alias("team_id"),
        pl.col("away_team_id").alias("opp_team_id"),
        (pl.col("home_score") - pl.col("away_score")).cast(pl.Float64).alias("team_margin"),
        pl.when(pl.col("neutral_site") == True).then(0).otherwise(1).alias("hfa_sign"),  # noqa: E712
    )
    away = played.select(
        pl.col("away_team_id").alias("team_id"),
        pl.col("home_team_id").alias("opp_team_id"),
        (pl.col("away_score") - pl.col("home_score")).cast(pl.Float64).alias("team_margin"),
        pl.when(pl.col("neutral_site") == True).then(0).otherwise(-1).alias("hfa_sign"),  # noqa: E712
    )
    rows = pl.concat([home, away]).with_columns(team_won=pl.col("team_margin") > 0)
    opp = ratings.select(pl.col("team_id").alias("opp_team_id"), pl.col("adj_net").alias("opp_adj_net"))
    assert rows.schema["opp_team_id"] == opp.schema["opp_team_id"], (
        f"opponent join-key dtype {rows.schema['opp_team_id']} != ratings team_id {opp.schema['opp_team_id']}"
    )
    return rows.join(opp, on="opp_team_id", how="left")


def _resume_core(
    ratings: pl.DataFrame, schedule: pl.DataFrame, config: PredictConfig, season_value: int | None
) -> pl.DataFrame:
    """Aggregate the per-team résumé metrics from ratings + a normalized schedule."""
    rows = _team_game_rows(schedule, ratings)
    if rows.height == 0:
        return pl.DataFrame(schema=_RESUME_SCHEMA)

    # game_control = mean postgame win-expectancy Phi(team_margin / margin_sd); the same
    # Gaussian as win_prob_from_margin, computed vectorized over the played games.
    control = norm.cdf(rows["team_margin"].to_numpy() / config.margin_sd)
    rows = rows.with_columns(pl.Series("_control", control, dtype=pl.Float64))

    agg = (
        rows.group_by("team_id")
        .agg(
            pl.col("opp_adj_net").mean().alias("sos"),
            ((pl.col("team_won") == True) & (pl.col("opp_adj_net") >= config.quality_win_threshold))  # noqa: E712
            .sum()
            .cast(pl.Int64)
            .alias("quality_wins"),
            pl.col("_control").mean().alias("game_control"),
        )
        .with_columns(
            pl.lit(season_value).cast(pl.Int64).alias("season"),
            pl.col("sos").rank(method="dense", descending=True).cast(pl.Int64).alias("sos_rank"),
        )
        .select(_RESUME_COLUMNS)
        .sort("sos_rank", "team_id")
    )
    return agg


@overload
def cfb_resume(
    seasons: int | list[int],
    *,
    as_of_date: datetime.date | None = ...,
    era: str = ...,
    return_as_pandas: Literal[True],
) -> pd.DataFrame: ...
@overload
def cfb_resume(
    seasons: int | list[int],
    *,
    as_of_date: datetime.date | None = ...,
    era: str = ...,
    return_as_pandas: Literal[False] = ...,
) -> pl.DataFrame: ...
def cfb_resume(
    seasons: int | list[int],
    *,
    as_of_date: datetime.date | None = None,
    era: str = "modern",
    return_as_pandas: bool = False,
) -> pl.DataFrame | pd.DataFrame:
    """Rating-based résumé metrics: strength of schedule, quality wins, game control.

    For each team, joins every played opponent to its :func:`cfb_ratings.cfb_ratings`
    strength and rolls the games up into:

    - ``sos`` -- mean opponent ``adj_net`` over played games (rating-based strength
      of schedule; complements the record-based SOV/SOS in ``cfb_standings``).
    - ``quality_wins`` -- count of wins over opponents with ``adj_net`` at or above
      the era ``quality_win_threshold``.
    - ``game_control`` -- mean postgame win expectancy ``Phi(actual_margin /
      margin_sd)``, i.e. how *dominant* the results were, not just win/loss.

    Args:
        seasons: A single season or list of seasons.
        as_of_date: Leakage boundary forwarded to :func:`cfb_ratings.cfb_ratings`
            (ratings use only games before this date). ``None`` uses the full season.
        era: Era key into :data:`cfb_prediction_constants.CFB_CONSTANTS`.
        return_as_pandas: If True, return a pandas DataFrame; otherwise polars.

    Returns:
        One row per team: ``season``, ``team_id`` (Utf8), ``sos``, ``sos_rank``
        (Int64 dense rank, best = 1), ``quality_wins`` (Int64), ``game_control``
        (Float64). Zero-row (typed) when no games are available.

    Example:
        Quick start::

            from sportsdataverse.cfb.cfb_resume import cfb_resume
            resume = cfb_resume(2023)
            resume.sort("sos_rank").head()

    See Also:
        * `cfbfastR`_ -- the R companion whose résumé/SoS surface this mirrors.

    .. _cfbfastR: https://cfbfastR.sportsdataverse.org
    """
    season_list = [seasons] if isinstance(seasons, int) else list(seasons)
    ratings = cfb_ratings(seasons, as_of_date=as_of_date, era=era)
    schedule = load_cfb_schedule(season_list)
    if ratings.is_empty() or schedule.is_empty():
        empty = pl.DataFrame(schema=_RESUME_SCHEMA)
        return empty.to_pandas() if return_as_pandas else empty

    schedule = _normalize_schedule(schedule)
    season_value = season_list[0] if len(season_list) == 1 else None
    out = _resume_core(ratings, schedule, get_constants(era), season_value)
    return out.to_pandas() if return_as_pandas else out
