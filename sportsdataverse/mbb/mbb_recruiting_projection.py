"""Recruiting -> freshman production projection from bundled ridge coefficients.

Model ③ of the MBB/WBB player-value spine. A recruit's expected freshman
``box_bpm`` is scored from pre-arrival recruiting inputs only (composite
grade + log national rank -- the as-of boundary holds by construction), and
``resume_residual`` reports realized-minus-expected once the freshman season
exists. The recruiting API's athlete id is NOT the college boxscore athlete
id, so the realized join is by (normalized name, team, season).
"""

from __future__ import annotations

import re
from typing import Literal, Union, overload

import numpy as np
import pandas as pd
import polars as pl

from sportsdataverse.mbb.mbb_box_bpm import mbb_box_bpm
from sportsdataverse.mbb.mbb_player_value_constants import (
    get_player_value_constants,
    load_artifact,
)

__all__ = ["mbb_recruiting_projection"]

_SCHEMA = {
    "recruit_id": pl.Utf8,
    "player_id": pl.Utf8,
    "player": pl.Utf8,
    "season": pl.Int64,
    "team_id": pl.Utf8,
    "composite": pl.Float64,
    "rank_nat": pl.Int64,
    "exp_box_bpm": pl.Float64,
    "resume_residual": pl.Float64,
}


def _load_recruits(seasons: "list[int]", league: str = "mens") -> pl.DataFrame:
    """Resolve the season's recruiting class to a tidy frame (one HTTP call
    per recruit -- ESPN ships the class as a paginated ``$ref`` collection).

    HS class C arrives college in season C+1; the ``rank`` attribute is only
    trustworthy alongside a non-null composite grade (ungraded players carry
    bogus low ranks), so it is nulled there.
    """
    from sportsdataverse.dl_utils import download  # noqa: PLC0415
    from sportsdataverse.errors import NoESPNDataError  # noqa: PLC0415

    if league == "womens":
        from sportsdataverse.wbb import espn_wbb_season_recruits as season_recruits  # noqa: PLC0415
    else:
        from sportsdataverse.mbb import espn_mbb_season_recruits as season_recruits  # noqa: PLC0415

    rows = []
    for season in seasons:
        refs = season_recruits(season - 1, limit=1000)
        ref_list = refs["$ref"].to_list() if "$ref" in refs.columns else []
        for url in ref_list:
            try:
                payload = download(url).json()
            except NoESPNDataError:  # $ref resolves to no data — skip this recruit
                continue
            ath = payload.get("athlete") or {}
            school_ref = ((payload.get("schools") or [{}])[0].get("team") or {}).get("$ref", "")
            m = re.search(r"/teams/(\d+)", school_ref)
            attrs = {a.get("name"): a.get("value") for a in payload.get("attributes") or []}
            composite = float(payload.get("grade") or 0) or None
            rank = int(attrs["rank"]) if attrs.get("rank") and composite is not None else None
            rows.append(
                {
                    "recruit_id": str(ath.get("id") or ""),
                    "player": ath.get("displayName"),
                    "team_id": m.group(1) if m else None,
                    "season": season,
                    "composite": composite,
                    "rank_nat": rank,
                    "height_in": float(ath.get("height") or 0) or None,
                }
            )
    if not rows:
        return pl.DataFrame()
    return pl.DataFrame(rows).with_columns(
        pl.col("recruit_id").cast(pl.Utf8), pl.col("team_id").cast(pl.Utf8), pl.col("season").cast(pl.Int64)
    )


@overload
def mbb_recruiting_projection(
    seasons: "Union[int, list[int]]",
    *,
    league: str = "mens",
    return_as_pandas: Literal[False] = False,
) -> pl.DataFrame: ...


@overload
def mbb_recruiting_projection(
    seasons: "Union[int, list[int]]",
    *,
    league: str = "mens",
    return_as_pandas: Literal[True],
) -> pd.DataFrame: ...


def mbb_recruiting_projection(
    seasons: "Union[int, list[int]]",
    *,
    league: str = "mens",
    return_as_pandas: bool = False,
) -> "Union[pl.DataFrame, pd.DataFrame]":
    """Expected freshman box-BPM per recruit + over/under-performance residual.

    Scores each recruit of the season's incoming class through the bundled
    recruiting ridge (composite grade + log national rank; missing values
    imputed with the class median / the bubble rank). When the freshman
    season is already observable, ``resume_residual = realized box_bpm -
    exp_box_bpm`` (null otherwise, and ``player_id`` carries the matched
    college athlete id).

    Args:
        seasons: Freshman college season(s) (e.g. ``2025`` = the class
            arriving for 2024-25).
        league: ``"mens"`` or ``"womens"`` (selects the bundled artifact).
        return_as_pandas: Return a pandas DataFrame instead of polars.

    Returns:
        One row per recruit: ``recruit_id:Utf8, player_id:Utf8 (nullable),
        player, season, team_id:Utf8, composite, rank_nat, exp_box_bpm,
        resume_residual``. Empty input returns the schema with zero rows.

    Example:
        Quick start::

            from sportsdataverse.mbb import mbb_recruiting_projection
            proj = mbb_recruiting_projection(2026)

        Pipeline next step (one line)::

            proj.sort("exp_box_bpm", descending=True).head(15)

    See Also:
        * `hoopR <https://hoopR.sportsdataverse.org>`_ -- men's basketball (R)
        * `recruitR <https://github.com/sportsdataverse/recruitR>`_ -- CFB recruiting (R)
    """
    seasons_list = [seasons] if isinstance(seasons, int) else list(seasons)
    recruits = _load_recruits(seasons_list, league=league)
    if recruits.is_empty():
        out = pl.DataFrame(schema=_SCHEMA)
        return out.to_pandas() if return_as_pandas else out

    art = load_artifact(f"{get_player_value_constants(league).bundle_prefix}_recruiting")
    bubble = float(art.get("bubble_rank", 150))
    feats = recruits.with_columns(
        pl.col("composite").fill_null(pl.col("composite").median().over("season")).alias("composite"),
        pl.col("rank_nat").cast(pl.Float64).fill_null(bubble).log().alias("log_rank"),
        pl.col("height_in").fill_null(pl.col("height_in").median().over("season")),
    )
    X = feats.select(art["feature_cols"]).to_numpy()
    coef = np.asarray(art["coef"], dtype=float)
    exp = np.hstack([np.ones((len(X), 1)), X]) @ coef
    scored = recruits.with_columns(
        pl.Series("exp_box_bpm", exp, dtype=pl.Float64),
        pl.col("player")
        .fill_null("")
        .str.to_lowercase()
        .str.replace_all(r"[^a-z ]", "")
        .str.replace_all(r"\s+", " ")
        .str.strip_chars()
        .alias("_pn"),
    )

    realized = mbb_box_bpm(seasons_list, league=league).filter(pl.col("min") >= float(art.get("min_minutes", 150.0)))
    realized = realized.with_columns(
        pl.col("player")
        .fill_null("")
        .str.to_lowercase()
        .str.replace_all(r"[^a-z ]", "")
        .str.replace_all(r"\s+", " ")
        .str.strip_chars()
        .alias("_pn")
    ).select("_pn", "team_id", "season", pl.col("player_id").alias("_matched_id"), pl.col("box_bpm").alias("_realized"))
    assert scored.schema["team_id"] == realized.schema["team_id"] == pl.Utf8
    out = (
        scored.join(realized, on=["_pn", "team_id", "season"], how="left")
        .with_columns(
            (pl.col("_realized") - pl.col("exp_box_bpm")).alias("resume_residual"),
            pl.col("_matched_id").alias("player_id"),
            pl.col("rank_nat").cast(pl.Int64),
        )
        .select(list(_SCHEMA))
        .sort("season", "exp_box_bpm", descending=[False, True])
    )
    return out.to_pandas() if return_as_pandas else out
