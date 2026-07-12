"""WNBA draft model -- draft-slot -> projected career value (reduced coverage).

**Naming note:** ``sportsdataverse/wnba/wnba_draft.py`` already exists (an
ESPN draft-history scraper). This module is deliberately named
``wnba_draft_model`` (public function ``wnba_draft_model``) to avoid
shadowing it.

Unlike the NBA core (:mod:`sportsdataverse.nba.nba_draft_model`), this is
**not** a by-reference combine-measurement shim -- live capture confirmed
``wnba_stats_draftcombinestats`` returns 0 rows for every WNBA season (see
the coverage caveat in ``wnba_draft_constants.py``), so there is no combine
feature vector to build. This model instead regresses realized career value
(the same all-era box formula, ``league="wnba"``) onto ``wnba_stats_drafthistory``'s
draft slot (``overall_pick``, ``round_number``) -- still a legitimate,
non-leaking, pre-career-value feature -- via the same ridge/logistic
fitters and the same bundled-artifact application pattern as the NBA core.
"""

from __future__ import annotations

import json
from importlib import resources
from typing import Literal, overload

import numpy as np
import pandas as pd
import polars as pl

from sportsdataverse.nba.nba_draft_constants import get_constants
from sportsdataverse.wnba.wnba_stats import wnba_stats_drafthistory

__all__ = ["wnba_draft_model"]

_SCHEMA = {
    "player_id": pl.Utf8,
    "draft_year": pl.Int64,
    "proj_career_value": pl.Float64,
    "draft_prob": pl.Float64,
    "projected_pick": pl.Int64,
    "pro_tier": pl.Utf8,
}

FEATURES = ["overall_pick", "round_number"]


def _load_artifact() -> dict:
    prefix = get_constants("wnba").artifact_prefix
    path = resources.files("sportsdataverse.nba") / "models" / f"{prefix}_draft_value.json"
    return dict(json.loads(path.read_text(encoding="utf-8")))


def _score(feats: pl.DataFrame, art: dict) -> pl.DataFrame:
    """Apply the bundled draft-slot artifact to a pre-built feature frame (no network).

    Factored out of :func:`wnba_draft_model` so offline fit/backtest scripts
    (``dev/wnba_draft/fit_rookie_residual.py``, ``tests/wnba/test_wnba_draft_backtest.py``)
    can score a committed fixture through the exact same math the runtime uses, without
    re-fetching ``wnba_stats_drafthistory`` over the network.

    Args:
        feats: Frame carrying ``player_id``, ``draft_year``, and the artifact's
            ``features`` columns (``overall_pick``, ``round_number``).
        art: Parsed ``wnba_draft_value.json`` artifact dict.

    Returns:
        Frame ``player_id, draft_year, proj_career_value, draft_prob, projected_pick,
        pro_tier`` -- same shape as :func:`wnba_draft_model`.
    """
    cols = art["features"]
    median = art.get("feature_median", {})
    for col in cols:
        if col not in feats.columns:
            feats = feats.with_columns(pl.lit(None).cast(pl.Float64).alias(col))
    X = feats.select([pl.col(c).fill_null(median.get(c, 0.0)) for c in cols]).to_numpy()
    mu = np.asarray(art.get("feature_mean", [0.0] * len(cols)), dtype=float)
    sd = np.asarray(art.get("feature_sd", [1.0] * len(cols)), dtype=float)
    sd = np.where(sd == 0.0, 1.0, sd)
    Z = (X - mu) / sd
    value = float(art["value_intercept"]) + Z @ np.asarray(art["value_coef"], dtype=float)
    logit = float(art["prob_intercept"]) + Z @ np.asarray(art["prob_coef"], dtype=float)
    prob = 1.0 / (1.0 + np.exp(-logit))

    out = feats.select("player_id", "draft_year").with_columns(
        pl.Series("proj_career_value", value, dtype=pl.Float64),
        pl.Series("draft_prob", prob, dtype=pl.Float64),
    )
    out = out.with_columns(
        pl.col("proj_career_value")
        .rank(method="ordinal", descending=True)
        .over("draft_year")
        .cast(pl.Int64)
        .alias("projected_pick")
    )
    return out.with_columns(
        pl.when(pl.col("projected_pick") <= 4)
        .then(pl.lit("lottery"))
        .when(pl.col("projected_pick") <= 12)
        .then(pl.lit("first_round"))
        .when(pl.col("projected_pick") <= 36)
        .then(pl.lit("second_round"))
        .otherwise(pl.lit("undrafted"))
        .alias("pro_tier")
    )


@overload
def wnba_draft_model(draft_year: "int | list[int]", *, return_as_pandas: Literal[False] = False) -> pl.DataFrame: ...


@overload
def wnba_draft_model(draft_year: "int | list[int]", *, return_as_pandas: Literal[True]) -> pd.DataFrame: ...


def wnba_draft_model(draft_year: "int | list[int]", *, return_as_pandas: bool = False) -> "pl.DataFrame | pd.DataFrame":
    """Project WNBA prospect career value + draft probability from draft slot.

    Args:
        draft_year: A draft year (e.g. ``2023``) or list of years.
        return_as_pandas: Return a pandas DataFrame instead of polars.

    Returns:
        Frame ``player_id:Utf8, draft_year:Int64, proj_career_value:Float64,
        draft_prob:Float64, projected_pick:Int64, pro_tier:Utf8``. Empty
        input returns the zero-row schema, never raises.

    Example:
        Quick start::

            from sportsdataverse.wnba import wnba_draft_model
            board = wnba_draft_model(2023)

    See Also:
        * `wehoop <https://wehoop.sportsdataverse.org>`_ -- women's college basketball (R)
        * `nba_api <https://github.com/swar/nba_api>`_ -- NBA/WNBA (Python)
    """
    years = [draft_year] if isinstance(draft_year, int) else list(draft_year)
    if not years:
        out = pl.DataFrame(schema=_SCHEMA)
        return out.to_pandas() if return_as_pandas else out

    history = wnba_stats_drafthistory()
    if history.is_empty():
        out = pl.DataFrame(schema=_SCHEMA)
        return out.to_pandas() if return_as_pandas else out

    feats = history.filter(pl.col("season").is_in(years)).select(
        pl.col("person_id").cast(pl.Int64).cast(pl.Utf8).alias("player_id"),
        pl.col("season").cast(pl.Int64).alias("draft_year"),
        pl.col("overall_pick").cast(pl.Float64),
        pl.col("round_number").cast(pl.Float64),
    )
    if feats.is_empty():
        out = pl.DataFrame(schema=_SCHEMA)
        return out.to_pandas() if return_as_pandas else out

    art = _load_artifact()
    out = _score(feats, art)
    return out.to_pandas() if return_as_pandas else out
