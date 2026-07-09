"""Draft model ① -- combine measurements -> projected NBA career value.

Applies the bundled ``nba_draft_value.json`` artifact (fit offline in
``dev/nba_draft/fit_draft_model.py`` against realized career value from
``nba_stats_playercareerstats``) to any draft class's combine measurements.
Two linear heads share one feature vector: a ridge head for
``proj_career_value`` and a logistic head for ``draft_prob``.
"""

from __future__ import annotations

import json
from importlib import resources
from typing import Literal, Optional, Union, overload

import numpy as np
import pandas as pd
import polars as pl

from sportsdataverse.nba.nba_draft_constants import build_combine_features, get_constants
from sportsdataverse.nba.nba_stats import (
    nba_stats_draftcombinedrillresults,
    nba_stats_draftcombinenonstationaryshooting,
    nba_stats_draftcombineplayeranthro,
    nba_stats_draftcombinespotshooting,
)

__all__ = ["nba_draft_model"]

_SCHEMA = {
    "player_id": pl.Utf8,
    "draft_year": pl.Int64,
    "proj_career_value": pl.Float64,
    "draft_prob": pl.Float64,
    "projected_pick": pl.Int64,
    "pro_tier": pl.Utf8,
}


def _load_artifact(league: str) -> dict:
    """Load the bundled per-league draft-value artifact (JSON).

    Args:
        league: League key resolved via :func:`sportsdataverse.nba.nba_draft_constants.get_constants`.

    Returns:
        The parsed artifact dict (``features``, ``value_coef``,
        ``value_intercept``, ``prob_coef``, ``prob_intercept``,
        ``feature_median``).
    """
    prefix = get_constants(league).artifact_prefix
    path = resources.files("sportsdataverse.nba") / "models" / f"{prefix}_draft_value.json"
    text = path.read_text(encoding="utf-8")
    return dict(json.loads(text))


def _fetch_combine(draft_years: list[int], league: str) -> pl.DataFrame:
    frames = []
    for year in draft_years:
        anthro = nba_stats_draftcombineplayeranthro(season_year=str(year))
        if anthro.is_empty():
            continue
        anthro = anthro.with_columns(
            pl.col("player_id").cast(pl.Int64).cast(pl.Utf8), pl.lit(year).cast(pl.Int64).alias("draft_year")
        )
        drills = nba_stats_draftcombinedrillresults(season_year=str(year))
        spot = nba_stats_draftcombinespotshooting(season_year=str(year))
        nonstat = nba_stats_draftcombinenonstationaryshooting(season_year=str(year))
        drills = (
            drills.with_columns(pl.col("player_id").cast(pl.Int64).cast(pl.Utf8)) if not drills.is_empty() else drills
        )
        spot = spot.with_columns(pl.col("player_id").cast(pl.Int64).cast(pl.Utf8)) if not spot.is_empty() else spot
        nonstat = (
            nonstat.with_columns(pl.col("player_id").cast(pl.Int64).cast(pl.Utf8))
            if not nonstat.is_empty()
            else nonstat
        )
        feats = build_combine_features(anthro, drills, spot, nonstat, league=league)
        frames.append(feats)
    return pl.concat(frames, how="diagonal_relaxed") if frames else pl.DataFrame()


def _score(feats: pl.DataFrame, art: dict) -> pl.DataFrame:
    cols = art["features"]
    for col in cols:
        if col not in feats.columns:
            feats = feats.with_columns(pl.lit(None).cast(pl.Float64).alias(col))
    median = art.get("feature_median", {})
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
        pl.when(pl.col("projected_pick") <= 14)
        .then(pl.lit("lottery"))
        .when(pl.col("projected_pick") <= 30)
        .then(pl.lit("first_round"))
        .when(pl.col("projected_pick") <= 60)
        .then(pl.lit("second_round"))
        .otherwise(pl.lit("undrafted"))
        .alias("pro_tier")
    )


@overload
def nba_draft_model(
    draft_year: "Union[int, list[int]]",
    *,
    league: str = "nba",
    college_prior: "Optional[pl.DataFrame]" = None,
    return_as_pandas: Literal[False] = False,
) -> pl.DataFrame: ...


@overload
def nba_draft_model(
    draft_year: "Union[int, list[int]]",
    *,
    league: str = "nba",
    college_prior: "Optional[pl.DataFrame]" = None,
    return_as_pandas: Literal[True],
) -> pd.DataFrame: ...


def nba_draft_model(
    draft_year: "Union[int, list[int]]",
    *,
    league: str = "nba",
    college_prior: "Optional[pl.DataFrame]" = None,
    return_as_pandas: bool = False,
) -> "Union[pl.DataFrame, pd.DataFrame]":
    """Project prospect career value + draft probability from combine measurements.

    Loads the draft-combine wrappers for ``draft_year`` (or each year in the
    list), builds the shared combine-feature vector
    (:func:`sportsdataverse.nba.nba_draft_constants.build_combine_features`),
    and applies the bundled ridge (``proj_career_value``) / logistic
    (``draft_prob``) heads fit in ``dev/nba_draft/fit_draft_model.py``.

    Args:
        draft_year: A draft year (e.g. ``2019``) or list of years.
        league: ``"nba"``, ``"wnba"``, or ``"gleague"`` -- selects the bundled
            artifact and the combine-wrapper family.
        college_prior: Optional frame keyed on ``player_id:Utf8`` carrying the
            college-side MBB/WBB player-value spine's ``projected_pick`` /
            ``box_bpm`` / ``archetype`` (model ⑤, see design doc §3.5). When
            present and the bundled artifact has matching feature columns, it
            is left-joined as an extra feature block. This function **never**
            imports ``sportsdataverse.mbb`` -- callers pass the frame in.
        return_as_pandas: Return a pandas DataFrame instead of polars.

    Returns:
        Frame ``player_id:Utf8, draft_year:Int64, proj_career_value:Float64,
        draft_prob:Float64, projected_pick:Int64, pro_tier:Utf8`` — one row
        per prospect with combine measurements for that class.
        ``projected_pick`` is a contiguous 1..N rank within each draft year.
        Empty/malformed input returns the zero-row schema, never raises.

    Example:
        Quick start::

            from sportsdataverse.nba import nba_draft_model
            board = nba_draft_model(2019)
            print(board.sort("proj_career_value", descending=True).head())

        With a college-side prior::

            board = nba_draft_model(2019, college_prior=mbb_prior_df)

        Pipeline next step (one line)::

            board.filter(pl.col("pro_tier") == "lottery")

        See Also:
            * `hoopR <https://hoopR.sportsdataverse.org>`_ -- men's college basketball (R)
            * `nba_api <https://github.com/swar/nba_api>`_ -- NBA/WNBA (Python)

    """
    years = [draft_year] if isinstance(draft_year, int) else list(draft_year)
    if not years:
        out = pl.DataFrame(schema=_SCHEMA)
        return out.to_pandas() if return_as_pandas else out

    feats = _fetch_combine(years, league)
    if feats.is_empty():
        out = pl.DataFrame(schema=_SCHEMA)
        return out.to_pandas() if return_as_pandas else out

    if college_prior is not None and not college_prior.is_empty():
        assert feats.schema["player_id"] == college_prior.schema.get("player_id", pl.Utf8)
        feats = feats.join(college_prior, on="player_id", how="left")

    art = _load_artifact(league)
    out = _score(feats, art)
    return out.to_pandas() if return_as_pandas else out
