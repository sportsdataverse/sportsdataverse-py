"""Garbage-filtered per-play substrate for the CFB advanced-stats models.

Implements the canonical published methodology (Bill Connelly / GameOnPaper /
collegefootballdata): the quarter-indexed garbage-time filter, the 50/70/100
success rule, EPA-threshold explosive flags, and ``build_play_long`` -- the
one long ``(team, opp, value)`` frame every advanced-stats model reads.
Thresholds live in :mod:`sportsdataverse.cfb.cfb_advanced_constants`.
"""

from __future__ import annotations

import datetime
from typing import Optional

import polars as pl

from sportsdataverse.cfb.cfb_advanced_constants import (
    EXPLOSIVE_EPA,
    GARBAGE_TIME_MARGIN,
    SUCCESS_COEF,
)

__all__ = [
    "DEFAULT_PBP_COLS",
    "flag_garbage_time",
    "add_success",
    "add_explosive",
    "build_play_long",
]

#: real released load_cfb_pbp column name -> canonical substrate name.
DEFAULT_PBP_COLS: dict[str, str] = {
    "season": "season",
    "game_id": "game_id",
    "wallclock": "wallclock",
    "period": "period",
    # NB: the released pbp's bare `down`/`distance` are near-all-null
    # artifacts; the live per-play values are start.down / start.distance.
    "start.down": "down",
    "start.distance": "distance",
    "statYardage": "yards_gained",
    "EPA": "epa",
    "pass": "pass",
    "rush": "rush",
    "havoc": "havoc",
    "scrimmage_play": "scrimmage_play",
    "pos_team_score": "pos_team_score",
    "def_pos_team_score": "def_pos_team_score",
    "start.pos_team.id": "pos_team_id",
    "start.def_pos_team.id": "def_pos_team_id",
}

#: canonical output schema of build_play_long.
_LONG_SCHEMA: dict[str, pl.DataType] = {
    "season": pl.Int64(),
    "game_id": pl.Utf8(),
    "date": pl.Date(),
    "team_id": pl.Utf8(),
    "opp_team_id": pl.Utf8(),
    "epa": pl.Float64(),
    "success": pl.Boolean(),
    "explosive": pl.Boolean(),
    "havoc": pl.Boolean(),
    "pass": pl.Boolean(),
    "rush": pl.Boolean(),
}


def flag_garbage_time(
    pbp: pl.DataFrame,
    *,
    period_col: str = "period",
    margin_col: str = "abs_score_diff",
) -> pl.DataFrame:
    """Add the Connelly garbage-time flag to a play-by-play frame.

    A play is garbage time when the absolute score margin strictly exceeds
    the quarter's threshold (``GARBAGE_TIME_MARGIN``: 43/37/27/21; the
    4th-quarter threshold also applies to overtime).

    Args:
        pbp: play-by-play frame.
        period_col: quarter/period column name.
        margin_col: absolute score-margin column; derived from
            ``pos_team_score``/``def_pos_team_score`` when absent.

    Returns:
        The frame with a boolean ``garbage_time`` column added.

    Example:
        Quick start::

            import polars as pl
            from sportsdataverse.cfb.cfb_opponent_adjust import flag_garbage_time
            out = flag_garbage_time(pl.DataFrame({"period": [4], "abs_score_diff": [28]}))
    """
    if margin_col not in pbp.columns and {
        "pos_team_score",
        "def_pos_team_score",
    } <= set(pbp.columns):
        pbp = pbp.with_columns((pl.col("pos_team_score") - pl.col("def_pos_team_score")).abs().alias(margin_col))
    q = pl.col(period_col)
    m = pl.col(margin_col)
    return pbp.with_columns(
        garbage_time=(
            ((q == 1) & (m > GARBAGE_TIME_MARGIN[1]))
            | ((q == 2) & (m > GARBAGE_TIME_MARGIN[2]))
            | ((q == 3) & (m > GARBAGE_TIME_MARGIN[3]))
            | ((q >= 4) & (m > GARBAGE_TIME_MARGIN[4]))
        ).fill_null(False)
    )


def add_success(
    pbp: pl.DataFrame,
    *,
    down_col: str = "down",
    dist_col: str = "distance",
    yards_col: str = "yards_gained",
) -> pl.DataFrame:
    """Add the Connelly/GameOnPaper 50/70/100 success flag.

    Success = ``yards_gained >= SUCCESS_COEF[down] * distance`` (0.5 on 1st,
    0.7 on 2nd, 1.0 on 3rd/4th). Downs outside 1..4 are ``False``.

    Args:
        pbp: play-by-play frame.
        down_col: down column name.
        dist_col: distance-to-go column name.
        yards_col: yards-gained column name.

    Returns:
        The frame with a boolean ``success`` column added.

    Example:
        Quick start::

            import polars as pl
            from sportsdataverse.cfb.cfb_opponent_adjust import add_success
            out = add_success(pl.DataFrame({"down": [1], "distance": [10.0], "yards_gained": [5.0]}))
    """
    d, dist, y = pl.col(down_col), pl.col(dist_col), pl.col(yards_col)
    return pbp.with_columns(
        success=pl.when(d == 1)
        .then(y >= SUCCESS_COEF[1] * dist)
        .when(d == 2)
        .then(y >= SUCCESS_COEF[2] * dist)
        .when((d == 3) | (d == 4))
        .then(y >= SUCCESS_COEF[3] * dist)
        .otherwise(False)
        .fill_null(False)
    )


def add_explosive(
    pbp: pl.DataFrame,
    *,
    epa_col: str = "epa",
    pass_col: str = "pass",
    rush_col: str = "rush",
) -> pl.DataFrame:
    """Add the EPA-threshold explosive-play flag (pass >= 2.4, rush >= 1.8).

    Args:
        pbp: play-by-play frame.
        epa_col: EPA column name.
        pass_col: boolean pass-play column name.
        rush_col: boolean rush-play column name.

    Returns:
        The frame with a boolean ``explosive`` column added.

    Example:
        Quick start::

            import polars as pl
            from sportsdataverse.cfb.cfb_opponent_adjust import add_explosive
            out = add_explosive(pl.DataFrame({"epa": [2.5], "pass": [True], "rush": [False]}))
    """
    epa = pl.col(epa_col)
    return pbp.with_columns(
        explosive=(
            ((pl.col(pass_col) == True) & (epa >= EXPLOSIVE_EPA["pass"]))  # noqa: E712
            | ((pl.col(rush_col) == True) & (epa >= EXPLOSIVE_EPA["rush"]))  # noqa: E712
        ).fill_null(False)
    )


def build_play_long(
    pbp: pl.DataFrame,
    *,
    cols: Optional[dict[str, str]] = None,
    exclude_garbage: bool = True,
    as_of_date: Optional[datetime.date] = None,
) -> pl.DataFrame:
    """Build the garbage-filtered per-play long frame the models consume.

    One row per scrimmage play with canonical columns ``season, game_id,
    date, team_id, opp_team_id, epa, success, explosive, havoc, pass, rush``.
    ``team_id``/``opp_team_id`` are ``Utf8`` (cast from the raw integer id).

    Args:
        pbp: play-by-play frame. Expects the canonical substrate names
            (``pos_team_id``, ``epa``, ``yards_gained``, ...); pass ``cols``
            to rename from the released loader names first.
        cols: optional ``{real_name: canonical_name}`` rename mapping, e.g.
            ``DEFAULT_PBP_COLS`` for raw ``load_cfb_pbp`` output.
        exclude_garbage: drop Connelly garbage-time plays (default True).
        as_of_date: leakage boundary -- keep only plays with
            ``date < as_of_date`` (strict; null dates are dropped when set).

    Returns:
        The canonical long frame; empty/malformed input returns a zero-row
        frame with the documented schema.

    Example:
        Quick start::

            import sportsdataverse.cfb as cfb
            from sportsdataverse.cfb.cfb_opponent_adjust import (
                DEFAULT_PBP_COLS, build_play_long,
            )
            pbp = cfb.load_cfb_pbp([2021])
            long = build_play_long(pbp, cols=DEFAULT_PBP_COLS)
    """
    if cols:
        mapping = {k: v for k, v in cols.items() if k in pbp.columns}
        # drop pre-existing columns the rename would collide with
        # (e.g. the released pbp's null-artifact bare `down`/`distance`)
        clobber = [v for k, v in mapping.items() if v in pbp.columns and v != k]
        pbp = pbp.drop(clobber).rename(mapping)
    required = {
        "season",
        "game_id",
        "pos_team_id",
        "def_pos_team_id",
        "epa",
        "down",
        "distance",
        "yards_gained",
        "pass",
        "rush",
        "period",
    }
    if pbp.height == 0 or not required <= set(pbp.columns):
        return pl.DataFrame(schema=_LONG_SCHEMA)

    if "havoc" not in pbp.columns:
        pbp = pbp.with_columns(havoc=pl.lit(False))
    if "scrimmage_play" not in pbp.columns:
        pbp = pbp.with_columns(scrimmage_play=pl.lit(True))
    if "date" not in pbp.columns:
        date_expr = (
            pl.col("wallclock").str.slice(0, 10).str.strptime(pl.Date, "%Y-%m-%d", strict=False)
            if "wallclock" in pbp.columns
            else pl.lit(None, dtype=pl.Date)
        )
        # per game: a single date (max over plays; wallclock is sparse/null)
        pbp = pbp.with_columns(date_expr.max().over("game_id").alias("date"))

    out = flag_garbage_time(pbp)
    out = add_success(out)
    out = add_explosive(out)
    out = out.filter(pl.col("scrimmage_play").cast(pl.Boolean) == True)  # noqa: E712
    if exclude_garbage:
        out = out.filter(pl.col("garbage_time") == False)  # noqa: E712
    if as_of_date is not None:
        out = out.filter(pl.col("date") < as_of_date)
    return out.select(
        pl.col("season").cast(pl.Int64),
        pl.col("game_id").cast(pl.Int64, strict=False).cast(pl.Utf8),
        pl.col("date").cast(pl.Date),
        pl.col("pos_team_id").cast(pl.Int64).cast(pl.Utf8).alias("team_id"),
        pl.col("def_pos_team_id").cast(pl.Int64).cast(pl.Utf8).alias("opp_team_id"),
        pl.col("epa").cast(pl.Float64),
        pl.col("success").cast(pl.Boolean),
        pl.col("explosive").cast(pl.Boolean),
        pl.col("havoc").cast(pl.Boolean),
        pl.col("pass").cast(pl.Boolean),
        pl.col("rush").cast(pl.Boolean),
    )
