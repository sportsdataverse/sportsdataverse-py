"""Measurement eras of the Baseball Savant pitch feed, shared by the pitch-level models.

Two boundaries in ``sdv-internal-refs rules/mlb.yaml`` change what a Savant column
measures without changing its name:

* **2017** (``mlb-2017-release-speed-statcast``): ``release_speed``, ``release_pos_x/z``,
  ``release_spin_rate`` and ``release_extension`` are TrackMan readings at release;
  2008-16 values are PITCHf/x readings at 50 ft adjusted to an approximate release
  point. Velocities read about 0.5-1 mph higher from 2017, so a pitcher's level must
  never be pooled across that boundary (:data:`STATCAST_RELEASE_SEASON`).
* **2026** (``mlb-2026-savant-plate-location-middle-of-plate`` and
  ``mlb-2026-abs-challenge-system``): ``plate_x`` / ``plate_z`` are reported at the
  middle of the plate instead of its front, and ``sz_top`` / ``sz_bot`` are the batter's
  ABS zone (53.5% / 27% of certified height) instead of the operator-set zone; ball and
  strike calls are challengeable. The re-base is a per-pitch trajectory shift, not a
  constant offset, so a called-strike location fit cannot mix the two frames
  (:data:`ABS_SEASON`, :func:`drop_abs_era_rows`).

The season of a pitch is ``game_year`` when the frame carries it, else the year of
``game_date``; a frame with neither (an umpire oracle capture, a synthetic frame) has
no era and is left alone.
"""

from __future__ import annotations

import warnings
from typing import Iterable, Optional

import polars as pl

from sportsdataverse._temporal import as_date

__all__ = ["ABS_SEASON", "STATCAST_RELEASE_SEASON", "abs_era_expr", "drop_abs_era_rows", "season_expr"]

#: First season of the ABS challenge system and of Savant's middle-of-plate location frame.
ABS_SEASON = 2026
#: First season Savant's release_speed / release_pos_* / spin come from TrackMan at release.
STATCAST_RELEASE_SEASON = 2017
_ABS_ENTRIES = "rules/mlb.yaml#mlb-2026-savant-plate-location-middle-of-plate, #mlb-2026-abs-challenge-system"


def season_expr(columns: Iterable[str]) -> Optional[pl.Expr]:
    """The season of each pitch as ``Int64``: ``game_year``, else the year of ``game_date``.

    Args:
        columns: The frame's column names.

    Returns:
        A polars expression, or ``None`` when the frame has neither column.

    Example:
        Quick start::

            from sportsdataverse.mlb._savant_eras import season_expr
            pitches.with_columns(season=season_expr(pitches.columns))
    """
    cols = set(columns)
    if "game_year" in cols:
        return pl.col("game_year").cast(pl.Int64)
    if "game_date" in cols:
        return as_date(pl.col("game_date")).dt.year().cast(pl.Int64)
    return None


def abs_era_expr(columns: Iterable[str]) -> pl.Expr:
    """``True`` where the pitch is from :data:`ABS_SEASON` on (``False`` with no season column)."""
    season = season_expr(columns)
    if season is None:
        return pl.lit(False)
    return (season >= ABS_SEASON).fill_null(False)


def drop_abs_era_rows(pitches: pl.DataFrame, *, what: str) -> pl.DataFrame:
    """The rows a single-frame plate-location fit may use.

    Pitches from :data:`ABS_SEASON` on are measured at the middle of the plate against an
    ABS zone, so a frame that spans the boundary keeps only its pre-2026 rows for a
    called-strike / framing fit, with a :class:`UserWarning` naming the catalogue entries.
    A frame entirely from 2026 on is one frame and is kept whole (a 2026 season build fits
    its own pitches). Scoring is not affected: callers still score every row and flag it
    ``abs_era``.

    Args:
        pitches: A Savant pitch frame.
        what: The fit being guarded, for the warning.

    Returns:
        ``pitches`` without its 2026+ rows when it also has earlier ones; otherwise unchanged.
    """
    if season_expr(pitches.columns) is None:
        return pitches
    kept = pitches.filter(~abs_era_expr(pitches.columns))
    if kept.height == 0:
        return pitches
    dropped = pitches.height - kept.height
    if dropped:
        warnings.warn(
            f"{what}: {dropped} of {pitches.height} pitches are from {ABS_SEASON}+, where Savant reports "
            "plate_x/plate_z at the middle of the plate against the ABS zone and calls are challengeable; "
            f"they are excluded from the fit ({_ABS_ENTRIES}).",
            UserWarning,
            stacklevel=3,
        )
    return kept
