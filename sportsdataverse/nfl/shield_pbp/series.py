"""Series data derivation.

Port of reference doc §7 (``helper_add_series_data.R :: add_series_data``,
lines 13-98): ``series`` (Int32, 1-based, cumulative per game, shared numbering
across both teams -- increments on each new first down or new drive),
``series_success`` (Int32 0/1), ``series_result`` (Utf8 category string).

Must run AFTER :func:`sportsdataverse.nfl.shield_pbp.drives.add_drive_detail` -- ``new_series``'s
"new drive" branch reads ``fixed_drive``, which ``add_drive_detail`` produces
(confirmed dependency in nflfastR's own pipeline: ``top-level_scraper.R`` runs
``add_drive_results() |> add_series_data()``, never the reverse).
"""

from __future__ import annotations

import polars as pl

from sportsdataverse.nfl.shield_pbp.drives import last_or_first_result, swapped_posteam

_HALF_GROUP = ["game_id", "game_half"]
_SERIES_GROUP = ["game_id", "series"]

_REQUIRED_SERIES_COLUMNS = {
    "game_id",
    "game_half",
    "play_seq",
    "fixed_drive",
    "first_down_rush",
    "first_down_pass",
    "first_down_penalty",
    "touchdown",
    "td_team",
    "posteam",
    "defteam",
    "field_goal_result",
    "safety",
    "play_type",
    "punt_attempt",
    "interception",
    "fumble_lost",
    "down",
    "yards_gained",
    "ydstogo",
    "qb_kneel",
    "desc",
    "kickoff_attempt",
}

_OUTPUT_SCHEMA: dict[str, pl.PolarsDataType] = {
    "series": pl.Int32,
    "series_result": pl.Utf8,
    "series_success": pl.Int32,
}


def _series_tmp_result(pos: pl.Expr) -> pl.Expr:
    """§7's per-play ``tmp_result`` case_when -- same as §8's, plus a leading
    ``"First down"`` branch and a ``"QB kneel"`` branch (both absent from the
    drive-level vocabulary; see :mod:`sportsdataverse.nfl.shield_pbp.drives`'s port-contract note).
    """
    return (
        pl.when(
            ((pl.col("first_down_penalty") == 1) | (pl.col("first_down_rush") == 1) | (pl.col("first_down_pass") == 1))
            & (pl.col("touchdown") == 0)
        )
        .then(pl.lit("First down"))
        .when((pl.col("touchdown") == 1) & (pos == pl.col("td_team")))
        .then(pl.lit("Touchdown"))
        .when((pl.col("touchdown") == 1) & (pos != pl.col("td_team")))
        .then(pl.lit("Opp touchdown"))
        .when(pl.col("field_goal_result") == "made")
        .then(pl.lit("Field goal"))
        .when(pl.col("field_goal_result").is_in(["blocked", "missed"]))
        .then(pl.lit("Missed field goal"))
        .when(pl.col("safety") == 1)
        .then(pl.lit("Safety"))
        .when((pl.col("play_type") == "punt") | (pl.col("punt_attempt") == 1))
        .then(pl.lit("Punt"))
        .when((pl.col("interception") == 1) | (pl.col("fumble_lost") == 1))
        .then(pl.lit("Turnover"))
        .when((pl.col("down") == 4) & (pl.col("yards_gained") < pl.col("ydstogo")) & (pl.col("play_type") != "no_play"))
        .then(pl.lit("Turnover on downs"))
        .when(pl.col("qb_kneel") == 1)
        .then(pl.lit("QB kneel"))
        .when(pl.col("desc").fill_null("").str.contains("(END QUARTER 2)|(END QUARTER 4)|(END GAME)"))
        .then(pl.lit("End of half"))
        .otherwise(None)
    )


def add_series_data(df: pl.DataFrame) -> pl.DataFrame:
    """Add ``series``, ``series_result``, ``series_success`` (reference §7).

    Args:
        df: The play frame, sorted (or sortable) by ``play_seq``, already
            carrying ``fixed_drive`` (from :func:`sportsdataverse.nfl.shield_pbp.drives.add_drive_detail`)
            plus every column in :data:`_REQUIRED_SERIES_COLUMNS`.

    Returns:
        The frame with ``series`` (Int32, 1-based, shared numbering across both
        teams within a game), ``series_result`` (Utf8 category -- one of
        ``"First down"``, ``"Touchdown"``, ``"Opp touchdown"``, ``"Field goal"``,
        ``"Missed field goal"``, ``"Safety"``, ``"Punt"``, ``"Turnover"``,
        ``"Turnover on downs"``, ``"QB kneel"``, ``"End of half"``, or ``None``),
        and ``series_success`` (Int32 0/1 -- 1 iff
        ``series_result in {"Touchdown", "First down"}``) added. Empty input or
        missing required columns returns the input frame with the *absent*
        output columns added as typed nulls (never raises); output columns
        already present on the input are left untouched, so a degraded re-run
        cannot null out real values.
    """
    if df.height == 0 or not _REQUIRED_SERIES_COLUMNS <= set(df.columns):
        return df.with_columns(**{c: pl.lit(None, dtype=t) for c, t in _OUTPUT_SCHEMA.items() if c not in df.columns})

    df = df.sort(["game_id", "play_seq"])
    g = _HALF_GROUP

    df = df.with_columns(_row=pl.int_range(0, pl.len()).over(g))
    fd_on_prior_play = (
        (pl.col("first_down_rush").shift(1).over(g) == 1)
        | (pl.col("first_down_pass").shift(1).over(g) == 1)
        | (pl.col("first_down_penalty").shift(1).over(g) == 1)
    ) & (pl.col("touchdown").shift(1).over(g) == 0)
    new_series = (
        (pl.col("fixed_drive") != pl.col("fixed_drive").shift(1).over(g)) | fd_on_prior_play | (pl.col("_row") == 0)
    )
    df = df.with_columns(_new_series=new_series.fill_null(False).cast(pl.Int64))
    df = df.with_columns(series=pl.col("_new_series").cum_sum().over("game_id").cast(pl.Int32))

    df = df.with_columns(_swap=swapped_posteam(df))
    df = df.with_columns(_tmp_result=_series_tmp_result(pl.col("_swap")))
    df = last_or_first_result(df, _SERIES_GROUP, "_tmp_result", "series_result")

    df = df.with_columns(
        series_success=pl.when(pl.col("series_result").is_in(["Touchdown", "First down"]))
        .then(1)
        .otherwise(0)
        .cast(pl.Int32)
    )
    df = df.drop("_row", "_new_series", "_swap", "_tmp_result")
    return df
