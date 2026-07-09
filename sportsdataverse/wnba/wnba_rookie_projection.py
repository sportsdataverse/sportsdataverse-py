"""WNBA rookie/sophomore projection -- composes the WNBA draft/aging/availability shims.

Same composition as :func:`sportsdataverse.nba.nba_rookie_projection.nba_rookie_projection`
(base value from the draft model, aged via the aging curve, availability
reported separately, plus a small per-tier residual), but built from the
WNBA-side pieces (:mod:`wnba_draft_model`, :mod:`wnba_aging_curve`,
:mod:`wnba_availability`) since the NBA draft-model core cannot run without
combine features (see ``wnba_draft_constants.py``'s coverage caveat).
"""

from __future__ import annotations

import json
from importlib import resources
from typing import Literal, overload

import pandas as pd
import polars as pl

from sportsdataverse.nba.nba_draft_constants import get_constants
from sportsdataverse.wnba.wnba_aging_curve import wnba_aging_curve
from sportsdataverse.wnba.wnba_availability import wnba_availability
from sportsdataverse.wnba.wnba_draft_model import wnba_draft_model

__all__ = ["wnba_rookie_projection"]

_SCHEMA = {
    "player_id": pl.Utf8,
    "draft_year": pl.Int64,
    "proj_rookie_value": pl.Float64,
    "proj_soph_value": pl.Float64,
    "proj_rookie_min": pl.Float64,
    "proj_avail_pct": pl.Float64,
    "pro_tier": pl.Utf8,
}

_ROOKIE_AGE = 22.0  # WNBA draftees are typically 4-year college seniors
_EXPECTED_MPG = {"lottery": 26.0, "first_round": 18.0, "second_round": 10.0, "undrafted": 5.0}


def _load_residual_artifact() -> dict:
    prefix = get_constants("wnba").artifact_prefix
    path = resources.files("sportsdataverse.nba") / "models" / f"{prefix}_rookie_projection.json"
    return dict(json.loads(path.read_text(encoding="utf-8")))  # type: ignore[attr-defined]


def _rel(curve: pl.DataFrame, age: float) -> float:
    rounded = int(round(age))
    row = curve.filter(pl.col("age") == rounded)
    if row.height == 0:
        return 1.0
    return float(row["rel_value"][0])


@overload
def wnba_rookie_projection(
    draft_year: "int | list[int]", *, return_as_pandas: Literal[False] = False
) -> pl.DataFrame: ...


@overload
def wnba_rookie_projection(draft_year: "int | list[int]", *, return_as_pandas: Literal[True]) -> pd.DataFrame: ...


def wnba_rookie_projection(
    draft_year: "int | list[int]", *, return_as_pandas: bool = False
) -> "pl.DataFrame | pd.DataFrame":
    """WNBA rookie/sophomore projection -- composes the WNBA draft/aging/availability pieces.

    Args:
        draft_year: A draft year or list of years.
        return_as_pandas: Return a pandas DataFrame instead of polars.

    Returns:
        Frame ``player_id:Utf8, draft_year:Int64, proj_rookie_value:Float64,
        proj_soph_value:Float64, proj_rookie_min:Float64,
        proj_avail_pct:Float64, pro_tier:Utf8``. Empty input -> zero-row schema.

    Example:
        Quick start::

            from sportsdataverse.wnba import wnba_rookie_projection
            board = wnba_rookie_projection(2023)

    See Also:
        * `wehoop <https://wehoop.sportsdataverse.org>`_ -- women's college basketball (R)
    """
    draft_board = wnba_draft_model(draft_year)
    if draft_board.is_empty():
        out = pl.DataFrame(schema=_SCHEMA)
        return out.to_pandas() if return_as_pandas else out

    art = _load_residual_artifact()
    rookie_fraction = float(art.get("rookie_fraction", 0.1))
    residual = {str(k): float(v) for k, v in art.get("residual", {}).items()}

    curve = wnba_aging_curve().select("age", "rel_value")
    rel_rookie = _rel(curve, _ROOKIE_AGE)
    rel_soph = _rel(curve, _ROOKIE_AGE + 1)
    rel_peak = 1.0

    seasons = [int(y) for y in draft_board["draft_year"].unique().to_list()]
    avail = wnba_availability(seasons)
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
        out = out.join(
            avail_map.select("player_id", "draft_year", "avail_pct"), on=["player_id", "draft_year"], how="left"
        )
    else:
        out = out.with_columns(pl.lit(None).cast(pl.Float64).alias("avail_pct"))
    out = out.with_columns(pl.col("avail_pct").fill_null(0.75).alias("proj_avail_pct"))

    expected_mpg_expr = pl.lit(_EXPECTED_MPG["undrafted"])
    for tier, mpg in _EXPECTED_MPG.items():
        expected_mpg_expr = pl.when(pl.col("pro_tier") == tier).then(pl.lit(mpg)).otherwise(expected_mpg_expr)
    games_full_season = float(get_constants("wnba").games_full_season)
    out = out.with_columns((games_full_season * pl.col("proj_avail_pct") * expected_mpg_expr).alias("proj_rookie_min"))

    out = out.select(list(_SCHEMA))
    return out.to_pandas() if return_as_pandas else out
