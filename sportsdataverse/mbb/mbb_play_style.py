"""College player play-style archetypes (BB-D20): the box-based archetypes plus shot and lineup-role features.

:func:`sportsdataverse.mbb.mbb_archetypes.mbb_archetypes` clusters a player-season on per-100 box
rates and the rim / mid-range / three split of his attempts. This model keeps those box and
shot-zone features and adds:

* **shot features** from the ESPN shots release: the assisted share of his made field goals (an
  assister on the play) and the dunk share of his rim attempts (a tip-in share was left out: ESPN's
  ``TipShot`` labels doubled from 2025 to 2026, a labelling change rather than a style one);
* **lineup-role features** from the box: the share of his games he started and his minutes per
  game as a share of a 40-minute game.

On-court lineup features (the team's style with him on the floor) are left out: the college
lineups (:func:`sportsdataverse.mbb.mbb_lineup_seasons.mbb_lineup_seasons`) carry stats.ncaa.org
player ids, and there is no NCAA-to-ESPN player crosswalk yet (roadmap BB-D16's blocker).

The method is :mod:`sportsdataverse._common.play_style`: features z-scored within the season,
centers fit on 2025 and frozen in ``mbb/models/{mbb,wbb}_play_style_player.json``, 2026 held out
(the box and shots releases start in 2025, so one training and one holdout season exist). A
player qualifies with at least a quarter of the season's most minutes. The artifacts are refit by
``tools/models/play_style/fit_play_style.py``.
"""

from __future__ import annotations

import json
from functools import cache
from importlib.resources import files
from typing import Final

import pandas as pd
import polars as pl

from sportsdataverse._common.play_style import ASSIGN_SCHEMA, assign_play_style
from sportsdataverse.mbb.mbb_player_value_constants import (
    _load_shots,
    aggregate_player_seasons,
    get_player_value_constants,
    player_per100_features,
)

__all__ = [
    "COLLEGE_PLAY_STYLE_FEATURES",
    "COLLEGE_PLAY_STYLE_PHRASES",
    "college_play_style_features",
    "mbb_play_styles",
]

#: Player qualifier: season minutes at least this share of the season's most minutes.
PLAYER_MIN_SHARE: Final[float] = 0.25

#: The clustering features, in artifact order: the box and shot-zone rates of the box-based
#: archetypes, then the shot and lineup-role features this model adds.
COLLEGE_PLAY_STYLE_FEATURES: Final[tuple[str, ...]] = (
    "usage",
    "rim_share",
    "mid_share",
    "three_share",
    "ftr",
    "ast_per100",
    "tov_pct",
    "oreb_pct",
    "reb_per100",
    "blk_pct",
    "stl_pct",
    "assisted_share",
    "dunk_share",
    "starter_share",
    "mpg_share",
)

#: The label phrase of each feature (see :func:`sportsdataverse._common.play_style.label_centers`).
COLLEGE_PLAY_STYLE_PHRASES: Final[dict[str, str]] = {
    "usage": "high-usage scorer",
    "rim_share": "rim finisher",
    "mid_share": "mid-range shooter",
    "three_share": "perimeter shooter",
    "ftr": "foul drawer",
    "ast_per100": "playmaker",
    "tov_pct": "ball handler",
    "oreb_pct": "offensive rebounder",
    "reb_per100": "rebounder",
    "blk_pct": "shot blocker",
    "stl_pct": "ball hawk",
    "assisted_share": "play finisher",
    "dunk_share": "lob threat",
    "starter_share": "starter",
    "mpg_share": "heavy-minutes starter",
}

_RIM = "(?i)layup|dunk|tip"


def _shot_features(shots: pl.DataFrame) -> pl.DataFrame:
    """Per-shooter assisted and dunk shares from the shots release (free throws dropped)."""
    if shots.is_empty() or "athlete_id_2" not in shots.columns:
        return pl.DataFrame(
            schema={
                "player_id": pl.Utf8,
                "season": pl.Int64,
                "assisted_share": pl.Float64,
                "dunk_share": pl.Float64,
            }
        )
    fga = shots.filter(
        pl.col("athlete_id_1").is_not_null()
        & (pl.col("type_text").str.contains(r"(?i)free\s*throw") == False)  # noqa: E712
        & (pl.col("score_value") >= 2)
    )
    t = pl.col("type_text")
    made = pl.col("scoring_play") == True  # noqa: E712
    rim = t.str.contains(_RIM)
    return (
        fga.with_columns(
            pl.col("athlete_id_1").cast(pl.Int64, strict=False).cast(pl.Utf8).alias("player_id"),
            pl.col("season").cast(pl.Int64),
        )
        .group_by("player_id", "season")
        .agg(
            ((made & pl.col("athlete_id_2").is_not_null()).sum() / made.sum().clip(lower_bound=1)).alias(
                "assisted_share"
            ),
            (t.str.contains("(?i)dunk").sum() / rim.sum().clip(lower_bound=1)).alias("dunk_share"),
        )
        .with_columns(pl.col("assisted_share", "dunk_share").cast(pl.Float64))
    )


def college_play_style_features(seasons: int | list[int], *, league: str = "mens") -> pl.DataFrame:
    """The college play-style features, one row per qualifying player-season.

    Args:
        seasons: A season or list of seasons (2025+, the box and shots releases' floor).
        league: ``"mens"`` or ``"womens"``.

    Returns:
        ``player_id`` (Utf8, ESPN), ``season``, ``team_id`` (Utf8), ``player``, ``min`` and the
        raw features of :data:`COLLEGE_PLAY_STYLE_FEATURES`. Empty input returns zero rows.
    """
    seasons_list = [seasons] if isinstance(seasons, int) else list(seasons)
    agg = aggregate_player_seasons(seasons_list, league=league)
    if agg.is_empty():
        return pl.DataFrame()
    key = ["player_id", "season", "team_id"]
    role = agg.select(
        *key,
        "player",
        "starter_share",
        (pl.col("minutes") / pl.col("games") / 40.0).cast(pl.Float64).alias("mpg_share"),
    )
    shots = _shot_features(_load_shots(seasons_list, league))
    out = (
        player_per100_features(agg)
        .join(role, on=key, how="left")
        .join(shots, on=["player_id", "season"], how="left")
        .filter(pl.col("min") >= PLAYER_MIN_SHARE * pl.col("min").max().over("season"))
    )
    return out.select(*key, "player", "min", *COLLEGE_PLAY_STYLE_FEATURES)


@cache
def _artifact(prefix: str) -> dict:
    path = files("sportsdataverse.mbb") / "models" / f"{prefix}_play_style_player.json"
    return dict(json.loads(path.read_text(encoding="utf-8")))


def mbb_play_styles(
    seasons: int | list[int], *, league: str = "mens", return_as_pandas: bool = False
) -> pl.DataFrame | pd.DataFrame:
    """College player play-style archetypes: box, shot and lineup-role features (BB-D20).

    The features and their sources are in :mod:`sportsdataverse.mbb.mbb_play_style`; the method
    (season-relative z-scores, centers fit on 2025 and frozen, applied as-of) in
    :mod:`sportsdataverse._common.play_style`.

    Args:
        seasons: A season or list of seasons (2025+).
        league: ``"mens"`` or ``"womens"`` (selects the bundled artifact).
        return_as_pandas: Return a pandas frame instead of polars.

    Returns:
        One row per qualifying ``(player_id, season, team_id)``: ``player_id`` / ``team_id`` (Utf8,
        ESPN ids), ``season``, ``player``, ``min`` and the ``archetype``, ``cluster``,
        ``dist_to_center``, ``second_archetype`` and ``margin``. Empty input returns that schema.

    Example:
        Quick start::

            from sportsdataverse.mbb.mbb_play_style import mbb_play_styles
            styles = mbb_play_styles(2026)
            styles.group_by("archetype").len()
    """
    ids = ["player_id", "season", "team_id", "player", "min"]
    feats = college_play_style_features(seasons, league=league)
    if feats.is_empty():
        schema = {"player_id": pl.Utf8(), "season": pl.Int64(), "team_id": pl.Utf8(), "player": pl.Utf8()}
        out = pl.DataFrame(schema=schema | {"min": pl.Float64()} | ASSIGN_SCHEMA)
    else:
        art = _artifact(get_player_value_constants(league).bundle_prefix)
        out = assign_play_style(feats, art, ids).sort("season", "cluster", "dist_to_center")
    return out.to_pandas() if return_as_pandas else out
