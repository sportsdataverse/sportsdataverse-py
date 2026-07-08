"""Rookie/sophomore projection ④ -- composes ①②③ + a small fitted residual.

Reuses the draft model (①), aging curve (②), and availability model (③)
verbatim -- no re-derived features -- and applies a small per-``pro_tier``
residual correction (fit in ``dev/nba_draft/fit_rookie_residual.py``) on top
of the composed projection.
"""

from __future__ import annotations

import json
from importlib import resources
from typing import Literal, Optional, overload

import pandas as pd
import polars as pl

from sportsdataverse.nba.nba_aging_curve import nba_aging_curve
from sportsdataverse.nba.nba_availability import nba_availability
from sportsdataverse.nba.nba_draft_constants import get_constants
from sportsdataverse.nba.nba_draft_model import nba_draft_model

__all__ = ["nba_rookie_projection"]

_SCHEMA = {
    "player_id": pl.Utf8,
    "draft_year": pl.Int64,
    "proj_rookie_value": pl.Float64,
    "proj_soph_value": pl.Float64,
    "proj_rookie_min": pl.Float64,
    "proj_avail_pct": pl.Float64,
    "pro_tier": pl.Utf8,
}

_ROOKIE_AGE = 19.0
_EXPECTED_MPG = {"lottery": 28.0, "first_round": 20.0, "second_round": 12.0, "undrafted": 6.0}


def _load_residual_artifact(league: str) -> dict:
    prefix = get_constants(league).artifact_prefix
    path = resources.files("sportsdataverse.nba") / "models" / f"{prefix}_rookie_projection.json"
    return dict(json.loads(path.read_text(encoding="utf-8")))  # type: ignore[attr-defined]


def _rel(curve: pl.DataFrame, age: float) -> float:
    rounded = int(round(age))
    row = curve.filter(pl.col("age") == rounded)
    if row.height == 0:
        return 1.0
    return float(row["rel_value"][0])


@overload
def nba_rookie_projection(
    draft_year: "int | list[int]",
    *,
    league: str = "nba",
    college_prior: "Optional[pl.DataFrame]" = None,
    return_as_pandas: Literal[False] = False,
) -> pl.DataFrame: ...


@overload
def nba_rookie_projection(
    draft_year: "int | list[int]",
    *,
    league: str = "nba",
    college_prior: "Optional[pl.DataFrame]" = None,
    return_as_pandas: Literal[True],
) -> pd.DataFrame: ...


def nba_rookie_projection(
    draft_year: "int | list[int]",
    *,
    league: str = "nba",
    college_prior: "Optional[pl.DataFrame]" = None,
    return_as_pandas: bool = False,
) -> "pl.DataFrame | pd.DataFrame":
    """Project rookie/sophomore value by composing draft x aging x availability.

    Composition (no re-derived features -- each term is the verbatim public
    output of ①②③):

    - ``base = nba_draft_model(...).proj_career_value * rookie_fraction``
      (``rookie_fraction`` from the bundled residual artifact -- the share of
      career value realized in a single rookie season).
    - ``proj_rookie_value = base * rel_value(rookie_age) / rel_value(peak_age)
      + residual[pro_tier]``; ``proj_soph_value`` uses ``rookie_age + 1``.
    - ``proj_avail_pct`` from :func:`sportsdataverse.nba.nba_availability.nba_availability`
      at rookie age -- reported separately, **never** multiplied into the
      value columns (availability is availability, not skill).
    - ``proj_rookie_min = games_full_season * proj_avail_pct * expected_mpg(pro_tier)``.

    Args:
        draft_year: A draft year or list of years.
        league: ``"nba"``, ``"wnba"``, or ``"gleague"``.
        college_prior: Optional college-side prior frame, forwarded verbatim
            to :func:`sportsdataverse.nba.nba_draft_model.nba_draft_model`.
        return_as_pandas: Return a pandas DataFrame instead of polars.

    Returns:
        Frame ``player_id:Utf8, draft_year:Int64, proj_rookie_value:Float64,
        proj_soph_value:Float64, proj_rookie_min:Float64,
        proj_avail_pct:Float64, pro_tier:Utf8``. Empty input -> zero-row schema.

    Example:
        Quick start::

            from sportsdataverse.nba import nba_rookie_projection
            board = nba_rookie_projection(2019)
            print(board.sort("proj_rookie_value", descending=True).head())

    See Also:
        * `hoopR <https://hoopR.sportsdataverse.org>`_ -- men's college basketball (R)
        * `nba_api <https://github.com/swar/nba_api>`_ -- NBA/WNBA (Python)
    """
    draft_board = nba_draft_model(draft_year, league=league, college_prior=college_prior)
    if draft_board.is_empty():
        out = pl.DataFrame(schema=_SCHEMA)
        return out.to_pandas() if return_as_pandas else out

    art = _load_residual_artifact(league)
    rookie_fraction = float(art.get("rookie_fraction", 0.1))
    residual = {str(k): float(v) for k, v in art.get("residual", {}).items()}

    curve = nba_aging_curve(league=league).select("age", "rel_value")
    # the bundled curve is normalized so its peak rel_value == 1.0 (see
    # nba_aging_curve.build_aging_deltas), so the peak-centered denominator
    # is always 1.0 -- no need to re-locate the peak age here.
    rel_rookie = _rel(curve, _ROOKIE_AGE)
    rel_soph = _rel(curve, _ROOKIE_AGE + 1)
    rel_peak = 1.0

    seasons = [int(y) for y in draft_board["draft_year"].unique().to_list()]
    avail = nba_availability(seasons, league=league)
    # avail is keyed by (player_id, season); rookie-season availability uses
    # the player's draft_year as the season key (their debut season).
    avail_map = avail.rename({"season": "draft_year"}) if not avail.is_empty() else None

    out = draft_board.with_columns(
        (pl.col("proj_career_value") * rookie_fraction * (rel_rookie / rel_peak)).alias("_base_rookie"),
        (pl.col("proj_career_value") * rookie_fraction * (rel_soph / rel_peak)).alias("_base_soph"),
    )
    residual_expr = pl.lit(0.0)
    for tier, val in residual.items():
        residual_expr = pl.when(pl.col("pro_tier") == tier).then(pl.lit(val)).otherwise(residual_expr)
    out = out.with_columns(
        (pl.col("_base_rookie") + residual_expr).alias("proj_rookie_value"),
        (pl.col("_base_soph") + residual_expr).alias("proj_soph_value"),
    )

    if avail_map is not None and not avail_map.is_empty():
        assert out.schema["player_id"] == avail_map.schema["player_id"]
        out = out.join(
            avail_map.select("player_id", "draft_year", "avail_pct"), on=["player_id", "draft_year"], how="left"
        )
    else:
        out = out.with_columns(pl.lit(None).cast(pl.Float64).alias("avail_pct"))
    out = out.with_columns(pl.col("avail_pct").fill_null(0.75).alias("proj_avail_pct"))

    expected_mpg_expr = pl.lit(_EXPECTED_MPG["undrafted"])
    for tier, mpg in _EXPECTED_MPG.items():
        expected_mpg_expr = pl.when(pl.col("pro_tier") == tier).then(pl.lit(mpg)).otherwise(expected_mpg_expr)
    games_full_season = float(get_constants(league).games_full_season)
    out = out.with_columns((games_full_season * pl.col("proj_avail_pct") * expected_mpg_expr).alias("proj_rookie_min"))

    out = out.select(list(_SCHEMA))
    return out.to_pandas() if return_as_pandas else out
