"""Rookie/sophomore projection ④ -- draft pick -> rookie and second-season value (pick curves).

Applies the bundled ``nba_rookie_projection.json`` pick curves (fit in
``tools/models/nba_draft/fit_pick_curve.py`` on draft classes 2003-2015) to
the picks :func:`sportsdataverse.nba.nba_draft_model.nba_draft_model` returns,
and reports availability from
:func:`sportsdataverse.nba.nba_availability.nba_availability` separately.

Each curve is monotone in the pick, so it ranks a class exactly as the pick
does. It ties the pick baseline by construction. The earlier composition (the
combine draft model x aging curve + a per-tier residual) lost to the pick on
its own holdout (Spearman 0.097 vs 0.400 among drafted players).
"""

from __future__ import annotations

import json
from importlib import resources
from typing import Literal, Optional, overload

import pandas as pd
import polars as pl

from sportsdataverse.nba._nba_draft_features import FEATURE_SCHEMA
from sportsdataverse.nba.nba_availability import nba_availability
from sportsdataverse.nba.nba_draft_model import _apply_pick_curve, _check_league, nba_draft_model

__all__ = ["nba_rookie_projection"]

_SCHEMA = {
    "player_id": pl.Utf8,
    "draft_year": pl.Int64,
    "overall_pick": pl.Int64,
    "player_name": pl.Utf8,
    "proj_rookie_value": pl.Float64,
    "proj_soph_value": pl.Float64,
    "proj_rookie_min": pl.Float64,
    "proj_avail_pct": pl.Float64,
    "pro_tier": pl.Utf8,
}
_DEFAULT_AVAIL = 0.75  # reported when nba_availability has no row for the pick (e.g. a future season)


def _load_rookie_artifact(league: str = "nba") -> dict:
    """Load the bundled rookie pick-curve artifact (``rookie_value`` / ``soph_value`` / ``rookie_min``)."""
    _check_league(league)
    path = resources.files("sportsdataverse.nba") / "models" / "nba_rookie_projection.json"
    return dict(json.loads(path.read_text(encoding="utf-8")))


def _score(board: pl.DataFrame, art: dict) -> pl.DataFrame:
    """Apply the three rookie curves to a frame carrying ``overall_pick`` (no network)."""
    c = art["curves"]
    pick = pl.col("overall_pick")
    return board.with_columns(
        _apply_pick_curve(c["rookie_value"], pick).alias("proj_rookie_value"),
        _apply_pick_curve(c["soph_value"], pick).alias("proj_soph_value"),
        _apply_pick_curve(c["rookie_min"], pick).clip(0.0, None).alias("proj_rookie_min"),
    )


@overload
def nba_rookie_projection(
    draft_year: "int | list[int]",
    *,
    league: str = "nba",
    pre_draft_features: bool = False,
    college_prior: "Optional[pl.DataFrame]" = None,
    return_as_pandas: Literal[False] = False,
) -> pl.DataFrame: ...


@overload
def nba_rookie_projection(
    draft_year: "int | list[int]",
    *,
    league: str = "nba",
    pre_draft_features: bool = False,
    college_prior: "Optional[pl.DataFrame]" = None,
    return_as_pandas: Literal[True],
) -> pd.DataFrame: ...


def nba_rookie_projection(
    draft_year: "int | list[int]",
    *,
    league: str = "nba",
    pre_draft_features: bool = False,
    college_prior: "Optional[pl.DataFrame]" = None,
    return_as_pandas: bool = False,
) -> "pl.DataFrame | pd.DataFrame":
    """Project each pick's rookie and second-season value and rookie minutes from his pick.

    Three pick curves (``tools/models/nba_draft/fit_pick_curve.py``, fit on
    draft classes 2003-2015) score the class
    :func:`sportsdataverse.nba.nba_draft_model.nba_draft_model` returns:

    - ``proj_rookie_value``: box value of the first NBA season the pick
      played (the corpus labels the first and second seasons played, so a
      pick stashed or injured in year one is labelled from his first season
      on the floor; ``proj_avail_pct`` is still for ``draft_year + 1``).
    - ``proj_soph_value``: box value of the second season played.
    - ``proj_rookie_min``: minutes in the first season played.

    Each curve ranks a class exactly as the pick does, so it ties the pick
    baseline by construction. On the 2016-2019 holdout Spearman is 0.477 for
    rookie value and the pick alike. ``proj_avail_pct`` comes from
    :func:`sportsdataverse.nba.nba_availability.nba_availability` at the debut
    season. It is reported on its own and never multiplied into the value
    columns.

    Args:
        draft_year: A draft year or list of years.
        league: ``"nba"`` only; the WNBA has
            :func:`sportsdataverse.wnba.wnba_rookie_projection.wnba_rookie_projection`.
        pre_draft_features: Attach the pre-draft college and EuroLeague
            columns, as :func:`~sportsdataverse.nba.nba_draft_model.nba_draft_model`
            does. They are information only; no curve reads them.
        college_prior: Optional frame keyed on ``player_id:Utf8``, forwarded
            to :func:`~sportsdataverse.nba.nba_draft_model.nba_draft_model` as
            extra columns.
        return_as_pandas: Return a pandas DataFrame instead of polars.

    Returns:
        One row per pick (1-60) of each class.

        |col_name          |type    |
        |:-----------------|:-------|
        |player_id         |String  |
        |draft_year        |Int64   |
        |overall_pick      |Int64   |
        |player_name       |String  |
        |proj_rookie_value |Float64 |
        |proj_soph_value   |Float64 |
        |proj_rookie_min   |Float64 |
        |proj_avail_pct    |Float64 |
        |pro_tier          |String  |

        With ``pre_draft_features=True`` it adds the same college and
        EuroLeague columns as ``nba_draft_model``.

        - ``proj_avail_pct`` falls back to 0.75 when availability has no row
          for the pick.
        - An empty or unpublished class returns the zero-row schema.

    Raises:
        ValueError: ``league`` is not ``"nba"``.
        SeasonNotFoundError: a draft year before 1996 (the draft release starts there).

    Example:
        Quick start::

            from sportsdataverse.nba import nba_rookie_projection
            board = nba_rookie_projection(2019)
            print(board.sort("proj_rookie_value", descending=True).head())

        Pipeline next step (one line)::

            board.filter(pl.col("pro_tier") == "lottery")

    See Also:
        * `hoopR <https://hoopR.sportsdataverse.org>`_ -- men's college basketball (R)
        * `nba_api <https://github.com/swar/nba_api>`_ -- NBA/WNBA (Python)
    """
    _check_league(league)
    schema = dict(_SCHEMA, **(FEATURE_SCHEMA if pre_draft_features else {}))
    board = nba_draft_model(
        draft_year, league=league, pre_draft_features=pre_draft_features, college_prior=college_prior
    )
    if board.is_empty():
        out = pl.DataFrame(schema=schema)
        return out.to_pandas() if return_as_pandas else out

    out = _score(board, _load_rookie_artifact(league))
    # nba_availability takes the season END year: a draft_year (start year) debuts the season ending draft_year + 1.
    draft_years = [int(y) for y in out["draft_year"].unique().to_list()]
    avail = nba_availability([y + 1 for y in draft_years], league=league)
    if avail is not None and not avail.is_empty():
        avail_map = avail.select(
            "player_id", (pl.col("season").cast(pl.Int64) - 1).alias("draft_year"), pl.col("avail_pct").cast(pl.Float64)
        )
        assert out.schema["player_id"] == avail_map.schema["player_id"]
        out = out.join(avail_map, on=["player_id", "draft_year"], how="left")
    else:
        out = out.with_columns(pl.lit(None, dtype=pl.Float64).alias("avail_pct"))
    out = out.with_columns(pl.col("avail_pct").fill_null(_DEFAULT_AVAIL).alias("proj_avail_pct"))
    extra = [
        c
        for c in board.columns
        if c not in _SCHEMA and c not in ("organization", "proj_career_value", "projected_pick")
    ]
    out = out.select(*_SCHEMA, *extra).sort("draft_year", "overall_pick")
    return out.to_pandas() if return_as_pandas else out
