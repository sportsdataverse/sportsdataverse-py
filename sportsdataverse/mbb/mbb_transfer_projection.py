"""Transfer-portal projection from bundled ridge coefficients.

Model ④ of the MBB/WBB player-value spine. The transfer cohort is detected
from roster discontinuity (same player, different team in consecutive
seasons); each transfer's post-move ``box_bpm`` is projected from
pre-transfer production only (the as-of boundary: nothing from the
destination season enters the features).
"""

from __future__ import annotations

from typing import Literal, Union, overload

import numpy as np
import pandas as pd
import polars as pl

from sportsdataverse.mbb.mbb_box_bpm import mbb_box_bpm
from sportsdataverse.mbb.mbb_player_value_constants import (
    get_player_value_constants,
    load_artifact,
)

__all__ = ["mbb_transfer_projection", "transfer_cohort"]

_SCHEMA = {
    "player_id": pl.Utf8,
    "player": pl.Utf8,
    "from_team_id": pl.Utf8,
    "to_team_id": pl.Utf8,
    "to_season": pl.Int64,
    "pre_box_bpm": pl.Float64,
    "proj_box_bpm": pl.Float64,
    "proj_delta": pl.Float64,
}


def transfer_cohort(rosters: pl.DataFrame) -> pl.DataFrame:
    """One row per transfer: same ``player_id``, different ``team_id`` in
    consecutive seasons.

    Args:
        rosters: Frame with ``player_id``, ``team_id``, ``season`` (extra
            columns ignored; one row per player-season-team).

    Returns:
        ``player_id:Utf8, from_team_id:Utf8, to_team_id:Utf8,
        from_season:Int64, to_season:Int64`` -- a player transferring twice
        appears twice.

    Example:
        Quick start::

            from sportsdataverse.mbb import mbb_box_bpm, transfer_cohort
            bpm = mbb_box_bpm([2025, 2026]).filter(pl.col("min") >= 150)
            moves = transfer_cohort(bpm.select("player_id", "team_id", "season"))
    """
    r = (
        rosters.select("player_id", "team_id", "season")
        .with_columns(
            pl.col("player_id").cast(pl.Utf8),
            pl.col("team_id").cast(pl.Utf8),
            pl.col("season").cast(pl.Int64),
        )
        .unique()
    )
    cur = r.rename({"team_id": "from_team_id", "season": "from_season"})
    nxt = r.rename({"team_id": "to_team_id", "season": "to_season"})
    return (
        cur.join(nxt, on="player_id", how="inner")
        .filter((pl.col("to_season") == pl.col("from_season") + 1) & (pl.col("from_team_id") != pl.col("to_team_id")))
        .select("player_id", "from_team_id", "to_team_id", "from_season", "to_season")
        .sort("player_id", "from_season")
    )


@overload
def mbb_transfer_projection(
    seasons: "Union[int, list[int]]",
    *,
    league: str = "mens",
    return_as_pandas: Literal[False] = False,
) -> pl.DataFrame: ...


@overload
def mbb_transfer_projection(
    seasons: "Union[int, list[int]]",
    *,
    league: str = "mens",
    return_as_pandas: Literal[True],
) -> pd.DataFrame: ...


def mbb_transfer_projection(
    seasons: "Union[int, list[int]]",
    *,
    league: str = "mens",
    return_as_pandas: bool = False,
) -> "Union[pl.DataFrame, pd.DataFrame]":
    """Projected post-transfer box-BPM for each transfer arriving in ``seasons``.

    Detects the transfer cohort from BOXSCORE discontinuity -- a player who
    logged qualifying minutes for different teams in consecutive seasons
    (the roster release under-reports moves ~70x, so production is the
    cohort source of record; bench-riders pre-move are excluded, which is
    fine because they carry no pre production to project from). Joins each
    player's pre-transfer (from-season) ``box_bpm`` and scores the bundled
    ridge. ``proj_delta = proj_box_bpm - pre_box_bpm`` (the expected
    move-related change; typically shrinks stars toward the mean).

    Args:
        seasons: Destination season(s), e.g. ``2026`` = arrived for 2025-26.
        league: ``"mens"`` or ``"womens"`` (selects the bundled artifact).
        return_as_pandas: Return a pandas DataFrame instead of polars.

    Returns:
        One row per transfer: ``player_id:Utf8, player, from_team_id:Utf8,
        to_team_id:Utf8, to_season:Int64, pre_box_bpm, proj_box_bpm,
        proj_delta``. Transfers without a qualifying pre-season sample are
        dropped. Empty input returns the schema with zero rows.

    Example:
        Quick start::

            from sportsdataverse.mbb import mbb_transfer_projection
            proj = mbb_transfer_projection(2026)

        Pipeline next step (one line)::

            proj.sort("proj_box_bpm", descending=True).head(15)

    See Also:
        * `hoopR <https://hoopR.sportsdataverse.org>`_ -- men's basketball (R)
        * `wehoop <https://wehoop.sportsdataverse.org>`_ -- women's basketball (R)
    """
    seasons_list = [seasons] if isinstance(seasons, int) else list(seasons)
    all_seasons = sorted({s for t in seasons_list for s in (t - 1, t)})
    if not all_seasons:
        out = pl.DataFrame(schema=_SCHEMA)
        return out.to_pandas() if return_as_pandas else out
    art = load_artifact(f"{get_player_value_constants(league).bundle_prefix}_transfer")
    bpm = mbb_box_bpm(all_seasons, league=league)
    if bpm.is_empty():
        out = pl.DataFrame(schema=_SCHEMA)
        return out.to_pandas() if return_as_pandas else out
    bpm = bpm.filter(pl.col("min") >= float(art.get("min_minutes", 150.0)))
    cohort = transfer_cohort(bpm.select("player_id", "team_id", "season")).filter(
        pl.col("to_season").is_in(seasons_list)
    )
    if cohort.is_empty():
        out = pl.DataFrame(schema=_SCHEMA)
        return out.to_pandas() if return_as_pandas else out

    pre = bpm
    assert cohort.schema["player_id"] == pre.schema["player_id"] == pl.Utf8
    j = cohort.join(
        pre.select(
            "player_id",
            "player",
            pl.col("season").alias("from_season"),
            pl.col("team_id").alias("from_team_id"),
            pl.col("box_bpm").alias("pre_box_bpm"),
        ),
        on=["player_id", "from_season", "from_team_id"],
        how="inner",
    )
    if j.is_empty():
        out = pl.DataFrame(schema=_SCHEMA)
        return out.to_pandas() if return_as_pandas else out

    X = j.select(art["feature_cols"]).to_numpy()
    coef = np.asarray(art["coef"], dtype=float)
    proj = np.hstack([np.ones((len(X), 1)), X]) @ coef
    out = (
        j.with_columns(pl.Series("proj_box_bpm", proj, dtype=pl.Float64))
        .with_columns((pl.col("proj_box_bpm") - pl.col("pre_box_bpm")).alias("proj_delta"))
        .select(list(_SCHEMA))
        .sort("to_season", "proj_box_bpm", descending=[False, True])
    )
    return out.to_pandas() if return_as_pandas else out
