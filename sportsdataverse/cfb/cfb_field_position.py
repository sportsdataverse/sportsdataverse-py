"""CFB field-position value model (④): starting field position + drive EP.

Values each drive start with a committed EP-by-yardline curve
(``cfb/models/cfb_field_position_ep.parquet``, isotonic fit on 2018-2021
next-score drive points by ``dev/cfb_advanced/fit_field_position.py``) and
aggregates per team-season: average starting yard line, drive EP (``fp_ep``),
field-position margin, and points per drive.

Data availability: hosted ``load_cfb_pbp`` covers 2002-2021 only (2022+
404s -- cfb-data producer backfill pending).
"""

from __future__ import annotations

from importlib.resources import files
from typing import TYPE_CHECKING, Literal, Union, overload

import numpy as np
import polars as pl
from scipy.optimize import isotonic_regression

from sportsdataverse.cfb.cfb_advanced_constants import FP_ARTIFACT
from sportsdataverse.cfb.cfb_loaders import load_cfb_pbp
from sportsdataverse.cfb.cfb_opponent_adjust import DEFAULT_PBP_COLS, flag_garbage_time

if TYPE_CHECKING:
    import pandas as pd

__all__ = ["cfb_field_position", "fit_field_position_ep", "load_fp_curve"]

#: extra rename entries (real released pbp -> canonical) for drive fields.
FP_PBP_COLS: dict[str, str] = {
    **DEFAULT_PBP_COLS,
    "drive.id": "drive_id",
    "drive.result": "drive_result",
    "drive.start.yardLine": "drive_start_yardline_raw",
    "homeTeamId": "home_team_id",
}

#: offense-signed realized drive points from the ESPN drive result string.
_DRIVE_PTS: pl.Expr = (
    pl.when(pl.col("drive_result") == "TD")
    .then(7.0)
    .when(pl.col("drive_result") == "FG")
    .then(3.0)
    .when(pl.col("drive_result") == "SF")
    .then(-2.0)
    .when(pl.col("drive_result").str.ends_with(" TD"))
    .then(-7.0)  # INT TD / FUMBLE RETURN TD / PUNT RETURN TD / ... = defense scored
    .otherwise(0.0)
)

_OUT_SCHEMA: dict[str, pl.DataType] = {
    "season": pl.Int64(),
    "team_id": pl.Utf8(),
    "drives": pl.Int64(),
    "avg_start_yardline": pl.Float64(),
    "fp_ep": pl.Float64(),
    "fp_margin": pl.Float64(),
    "points_per_drive": pl.Float64(),
}


def _drives_from_pbp(pbp: pl.DataFrame, *, exclude_garbage: bool = True) -> pl.DataFrame:
    """One row per drive: offense/defense ids, start yardline (own goal), points."""
    required = {
        "season",
        "game_id",
        "drive_id",
        "drive_result",
        "drive_start_yardline_raw",
        "home_team_id",
        "period",
        "pos_team_id",
        "def_pos_team_id",
    }
    if pbp.height == 0 or not required <= set(pbp.columns):
        return pl.DataFrame(
            schema={
                "season": pl.Int64(),
                "game_id": pl.Utf8(),
                "drive_id": pl.Utf8(),
                "half": pl.Int64(),
                "team_id": pl.Utf8(),
                "opp_team_id": pl.Utf8(),
                "start_yardline_own": pl.Float64(),
                "drive_pts": pl.Float64(),
                "order": pl.Int64(),
            }
        )
    pbp = flag_garbage_time(pbp)
    if "scrimmage_play" in pbp.columns:
        pbp = pbp.filter(pl.col("scrimmage_play").cast(pl.Boolean) == True)  # noqa: E712
    # Drive start comes from the drive-level ESPN field (constant per drive,
    # so no play ordering is needed -- the released pbp's clock and
    # within-drive indices are unreliable in some games). Coordinate
    # semantics: drive.start.yardLine is the HOME team's own yard line;
    # for the away offense the own yard line is 100 - yardLine.
    d = (
        pbp.filter(pl.col("drive_id").is_not_null())
        .with_columns(game_id_str=pl.col("game_id").cast(pl.Int64, strict=False).cast(pl.Utf8))
        .group_by(["season", "game_id", "drive_id"])
        .agg(
            # drive start period; min is order-free (periods only increase)
            half=pl.when(pl.col("period").min() <= 2)
            .then(1)
            .when(pl.col("period").min() <= 4)
            .then(2)
            .otherwise(3)
            .cast(pl.Int64),
            team_id=pl.col("pos_team_id").first().cast(pl.Int64).cast(pl.Utf8),
            opp_team_id=pl.col("def_pos_team_id").first().cast(pl.Int64).cast(pl.Utf8),
            start_yardline_own=pl.when(pl.col("pos_team_id").first() == pl.col("home_team_id").first())
            .then(pl.col("drive_start_yardline_raw").first().cast(pl.Float64))
            .otherwise(100.0 - pl.col("drive_start_yardline_raw").first().cast(pl.Float64)),
            drive_pts=_DRIVE_PTS.first(),
            # order-free: a drive counts as garbage when EVERY play is garbage
            garbage_start=pl.col("garbage_time").all(),
            game_id_str=pl.col("game_id_str").first(),
        )
        # true drive sequence = drive_id suffix after the game_id prefix
        .with_columns(
            order=pl.col("drive_id")
            .cast(pl.Utf8)
            .str.slice(pl.col("game_id_str").str.len_chars())
            .cast(pl.Int64, strict=False),
        )
        .drop("game_id")
        .rename({"game_id_str": "game_id"})
    )
    if exclude_garbage:
        d = d.filter(pl.col("garbage_start") == False)  # noqa: E712
    return d.drop("garbage_start").filter(pl.col("start_yardline_own").is_between(1, 99))


def fit_field_position_ep(
    drives: pl.DataFrame,
    *,
    start_col: str = "drive_start_yardline",
    pts_col: str = "drive_next_score_pts",
) -> pl.DataFrame:
    """Fit the monotone EP-by-starting-yardline curve from a drives frame.

    Groups drives by starting yard line (from own goal), takes the mean
    next-score points, and applies sample-count-weighted isotonic regression
    (weight = number of drives at each starting yard line, non-decreasing),
    interpolated onto the full 1..99 grid.

    Args:
        drives: one row per drive.
        start_col: starting yard line from own goal (1..99).
        pts_col: net next-score points for the drive's offense.

    Returns:
        ``yardline_own: Int64 (1..99), ep: Float64`` -- monotone
        non-decreasing. Empty input returns a zero-row frame.

    Example:
        Quick start::

            import polars as pl
            from sportsdataverse.cfb.cfb_field_position import fit_field_position_ep
            curve = fit_field_position_ep(drives_frame)
    """
    schema = {"yardline_own": pl.Int64(), "ep": pl.Float64()}
    if drives.height == 0:
        return pl.DataFrame(schema=schema)
    g = (
        drives.select(
            pl.col(start_col).round(0).cast(pl.Int64).alias("yl"),
            pl.col(pts_col).cast(pl.Float64).alias("pts"),
        )
        .filter(pl.col("yl").is_between(1, 99))
        .group_by("yl")
        .agg(mean_pts=pl.col("pts").mean(), n=pl.len())
        .sort("yl")
    )
    x = g["yl"].to_numpy()
    iso = isotonic_regression(g["mean_pts"].to_numpy(), weights=g["n"].to_numpy(), increasing=True).x
    grid = np.arange(1, 100)
    ep = np.interp(grid, x, iso)
    return pl.DataFrame({"yardline_own": grid, "ep": ep}, schema=schema)


def load_fp_curve() -> pl.DataFrame:
    """Load the bundled EP-by-yardline curve (no network, no first-use download).

    Returns:
        ``yardline_own: Int64 (1..99), ep: Float64``.

    Example:
        Quick start::

            from sportsdataverse.cfb.cfb_field_position import load_fp_curve
            curve = load_fp_curve()
    """
    path = files("sportsdataverse.cfb") / "models" / FP_ARTIFACT
    with path.open("rb") as f:
        return pl.read_parquet(f)


@overload
def cfb_field_position(
    seasons: Union[int, list[int]],
    *,
    exclude_garbage: bool = ...,
    return_as_pandas: Literal[False] = ...,
) -> pl.DataFrame: ...


@overload
def cfb_field_position(
    seasons: Union[int, list[int]],
    *,
    exclude_garbage: bool = ...,
    return_as_pandas: Literal[True],
) -> pd.DataFrame: ...


def cfb_field_position(
    seasons: Union[int, list[int]],
    *,
    exclude_garbage: bool = True,
    return_as_pandas: bool = False,
) -> Union[pl.DataFrame, pd.DataFrame]:
    """Team-season field-position value: avg start, drive EP, margin, pts/drive.

    Derives one row per drive from ``load_cfb_pbp``, values each starting
    yard line with the bundled EP curve, and aggregates per (season, team):
    ``avg_start_yardline`` (yards from own goal, higher = better),
    ``fp_ep`` (mean drive-start EP), ``fp_margin`` (own ``fp_ep`` minus the
    mean drive-start EP of opponents' drives faced), and
    ``points_per_drive`` (mean realized offensive points: TD=7, FG=3;
    non-offensive negative results such as safeties and defensive return
    TDs are floored to 0 before averaging).

    Args:
        seasons: season or list of seasons (hosted pbp covers 2002-2021).
        exclude_garbage: drop drives that start in Connelly garbage time.
        return_as_pandas: return a pandas ``DataFrame`` instead of polars.

    Returns:
        One row per (season, team_id); zero-row frame with the documented
        schema on empty input.

    Example:
        Quick start::

            from sportsdataverse.cfb import cfb_field_position
            df = cfb_field_position([2021])
            print(df.shape)

        Pipeline next step (one line)::

            df.sort("fp_margin", descending=True).head()

    See Also:
        * `cfbfastR`_ -- R sister package (CFBD advanced stats wrappers)

    .. _cfbfastR: https://cfbfastR.sportsdataverse.org
    """
    season_list = [seasons] if isinstance(seasons, int) else list(seasons)
    pbp = load_cfb_pbp(season_list)
    if not isinstance(pbp, pl.DataFrame):
        pbp = pl.DataFrame(pbp)
    if pbp.height > 0:
        mapping = {k: v for k, v in FP_PBP_COLS.items() if k in pbp.columns}
        clobber = [v for k, v in mapping.items() if v in pbp.columns and v != k]
        pbp = pbp.drop(clobber).rename(mapping)
    d = _drives_from_pbp(pbp, exclude_garbage=exclude_garbage)
    if d.height == 0:
        out = pl.DataFrame(schema=_OUT_SCHEMA)
        return out.to_pandas() if return_as_pandas else out

    curve = load_fp_curve()
    d = d.with_columns(yardline_own=pl.col("start_yardline_own").round(0).cast(pl.Int64))
    assert d.schema["yardline_own"] == curve.schema["yardline_own"]
    d = d.join(curve, on="yardline_own", how="left")

    off = d.group_by(["season", "team_id"]).agg(
        drives=pl.len().cast(pl.Int64),
        avg_start_yardline=pl.col("start_yardline_own").mean(),
        fp_ep=pl.col("ep").mean(),
        points_per_drive=pl.col("drive_pts").clip(lower_bound=0.0).mean(),
    )
    faced = d.group_by(["season", "opp_team_id"]).agg(opp_fp_ep=pl.col("ep").mean()).rename({"opp_team_id": "team_id"})
    assert off.schema["team_id"] == faced.schema["team_id"]
    out = (
        off.join(faced, on=["season", "team_id"], how="left")
        .with_columns(fp_margin=pl.col("fp_ep") - pl.col("opp_fp_ep"))
        .select(list(_OUT_SCHEMA))
        .sort(["season", "team_id"])
    )
    return out.to_pandas() if return_as_pandas else out
