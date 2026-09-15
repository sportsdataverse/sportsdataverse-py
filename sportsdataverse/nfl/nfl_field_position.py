"""NFL field-position value: the EP-by-starting-yardline curve.

The NFL twin of :mod:`sportsdataverse.cfb.cfb_field_position`. Values a drive
start with a committed EP-by-yardline curve
(``nfl/models/nfl_field_position_ep.parquet``), fit with the same recipe as
the college one so the two leagues' field-position margins are comparable:
sample-count-weighted isotonic regression of offense-signed realized drive
points (TD +7, FG +3, safety -2, defensive return TD -7) on the drive's
starting yard line from its own goal, drive-level starts
(``drive.start.yardLine`` oriented by ``homeTeamId``), garbage-time drives
excluded.

Provenance of the bundled artifact: fit 2026-09-14 by
:func:`fit_nfl_field_position_ep` on the ``espn_nfl_pbp`` seasons 2016-2025
(59,026 drives; nfl-data ``nfl_espn_build`` over nfl-raw's ESPN library).
Observed anchors: own-25 1.82, midfield 2.50, opp-25 3.62, opp-10 4.51.
"""

from __future__ import annotations

from importlib.resources import files

import polars as pl

from sportsdataverse.cfb.cfb_field_position import (
    FP_PBP_COLS,
    _drives_from_pbp,
    fit_field_position_ep,
)

__all__ = ["NFL_FP_ARTIFACT", "fit_nfl_field_position_ep", "load_nfl_fp_curve"]

#: bundled EP-by-yardline curve (``nfl/models/``)
NFL_FP_ARTIFACT = "nfl_field_position_ep.parquet"


def fit_nfl_field_position_ep(pbp: pl.DataFrame, *, exclude_garbage: bool = True) -> pl.DataFrame:
    """Fit the NFL EP-by-starting-yardline curve from released ``espn_nfl_pbp`` plays.

    Extracts one row per drive (starting yard line from the offense's own
    goal, realized drive points) and fits the monotone curve with
    :func:`sportsdataverse.cfb.cfb_field_position.fit_field_position_ep` --
    the same estimator and target the college curve uses. This is how the
    bundled artifact was produced; re-run it on newer seasons to refresh it.

    Args:
        pbp: plays in the released ``espn_nfl_pbp`` shape (any number of
            seasons concatenated). Needs the drive fields (``drive.id``,
            ``drive.result``, ``drive.start.yardLine``), ``homeTeamId``,
            ``period`` and ``start.pos_team.id`` / ``start.def_pos_team.id``.
        exclude_garbage: drop drives that start in garbage time.

    Returns:
        ``yardline_own: Int64 (1..99), ep: Float64`` -- monotone
        non-decreasing. Empty input returns a zero-row frame.

    Example:
        Refresh the curve from local season parquet::

            import polars as pl
            from sportsdataverse.nfl import fit_nfl_field_position_ep
            pbp = pl.concat([pl.read_parquet(f) for f in files], how="diagonal_relaxed")
            curve = fit_nfl_field_position_ep(pbp)
            curve.write_parquet("nfl_field_position_ep.parquet")
    """
    schema = {"yardline_own": pl.Int64(), "ep": pl.Float64()}
    if pbp.height == 0:
        return pl.DataFrame(schema=schema)
    rename = {k: v for k, v in FP_PBP_COLS.items() if k in pbp.columns and k != v}
    drives = _drives_from_pbp(pbp.rename(rename), exclude_garbage=exclude_garbage)
    return fit_field_position_ep(drives, start_col="start_yardline_own", pts_col="drive_pts")


def load_nfl_fp_curve() -> pl.DataFrame:
    """Load the bundled NFL EP-by-yardline curve (no network).

    Returns:
        ``yardline_own: Int64 (1..99), ep: Float64``.

    Example:
        Value an average drive start at the own 30::

            from sportsdataverse.nfl import load_nfl_fp_curve
            curve = load_nfl_fp_curve()
            curve.filter(curve["yardline_own"] == 30)
    """
    path = files("sportsdataverse.nfl") / "models" / NFL_FP_ARTIFACT
    with path.open("rb") as f:
        return pl.read_parquet(f)
