"""College -> NBA/WNBA draft projection from bundled dual-head coefficients.

Model ⑤ of the MBB/WBB player-value spine. Assembles the season's box-BPM
(model ①) and archetype (model ②) features, standardizes with the fit-time
moments, and scores two bundled heads: an L2-logistic ``draft_prob`` and a
ridge ``projected_pick`` (bucketed into ``pro_tier``). Features are
runtime-assemblable production only -- recruiting inputs stay offline as the
fitter's baseline-to-beat (resolving them live would cost hundreds of HTTP
calls per invocation and skew train/serve).
"""

from __future__ import annotations

from typing import Literal, Union, overload

import numpy as np
import pandas as pd
import polars as pl

from sportsdataverse.errors import SeasonNotFoundError
from sportsdataverse.mbb.mbb_archetypes import mbb_archetypes
from sportsdataverse.mbb.mbb_box_bpm import mbb_box_bpm
from sportsdataverse.mbb.mbb_player_value_constants import (
    get_player_value_constants,
    load_artifact,
)

__all__ = ["mbb_draft_projection"]

_SCHEMA = {
    "player_id": pl.Utf8,
    "player": pl.Utf8,
    "season": pl.Int64,
    "team_id": pl.Utf8,
    "draft_prob": pl.Float64,
    "projected_pick": pl.Float64,
    "pro_tier": pl.Utf8,
}


def _load_roster_class(seasons: "list[int]", league: str = "mens") -> pl.DataFrame:
    """(player_id, season) -> class/height from the rosters release.

    Sufficient for CURRENT-season scoring (active players are on current
    rosters); the fitter additionally backfills class from the recruiting
    class year because the release drops already-departed players.
    """
    if league == "womens":
        from sportsdataverse.wbb.wbb_loaders import load_wbb_rosters as loader  # noqa: PLC0415
    else:
        from sportsdataverse.mbb.mbb_loaders import load_mbb_rosters as loader  # noqa: PLC0415

    frames = []
    for s in seasons:
        try:
            df = loader([s])
        except SeasonNotFoundError:  # season below this league's roster-release floor
            continue
        if df.is_empty():
            continue
        frames.append(
            df.select(
                pl.col("athlete_id").cast(pl.Int64, strict=False).cast(pl.Utf8).alias("player_id"),
                pl.col("season").cast(pl.Int64),
                (
                    pl.col("experience_display_value").alias("class")
                    if "experience_display_value" in df.columns
                    else pl.lit(None, dtype=pl.Utf8).alias("class")
                ),
                (
                    # the release ships display-format heights (``6' 5"``)
                    (
                        pl.col("height").cast(pl.Utf8).str.extract(r"(\d+)'", 1).cast(pl.Float64) * 12
                        + pl.col("height").cast(pl.Utf8).str.extract(r"'\s*(\d+)", 1).cast(pl.Float64)
                    ).alias("height_in")
                    if "height" in df.columns
                    else pl.lit(None, dtype=pl.Float64).alias("height_in")
                ),
            )
        )
    if not frames:
        return pl.DataFrame(
            schema={"player_id": pl.Utf8, "season": pl.Int64, "class": pl.Utf8, "height_in": pl.Float64}
        )
    out = pl.concat(frames, how="diagonal_relaxed").unique(subset=["player_id", "season"], keep="first")
    # fail LOUDLY if the release height format drifts (a silent all-null parse
    # would feed a constant-zero height into the draft heads -> train/serve skew)
    if out.height > 0 and out.get_column("height_in").null_count() == out.height:
        raise ValueError("rosters release height format changed: no heights parsed from the display strings")
    return out


@overload
def mbb_draft_projection(
    seasons: "Union[int, list[int]]",
    *,
    league: str = "mens",
    return_as_pandas: Literal[False] = False,
) -> pl.DataFrame: ...


@overload
def mbb_draft_projection(
    seasons: "Union[int, list[int]]",
    *,
    league: str = "mens",
    return_as_pandas: Literal[True],
) -> pd.DataFrame: ...


def mbb_draft_projection(
    seasons: "Union[int, list[int]]",
    *,
    league: str = "mens",
    return_as_pandas: bool = False,
) -> "Union[pl.DataFrame, pd.DataFrame]":
    """Draft probability, projected pick, and pro tier per player-season.

    ``draft_prob`` is the probability of being selected in the draft
    immediately following the college season; ``projected_pick`` is the
    expected overall pick conditional on being drafted (lower = better);
    ``pro_tier`` buckets the pick through the bundled tier edges.

    Args:
        seasons: A season (e.g. ``2025``, feeding the June 2025 draft) or
            list of seasons.
        league: ``"mens"`` or ``"womens"`` (selects the bundled artifact;
            womens = WNBA draft).
        return_as_pandas: Return a pandas DataFrame instead of polars.

    Returns:
        One row per qualifying player-season: ``player_id:Utf8, player,
        season, team_id:Utf8, draft_prob, projected_pick, pro_tier``. Empty
        input returns the schema with zero rows.

    Example:
        Quick start::

            from sportsdataverse.mbb import mbb_draft_projection
            board = mbb_draft_projection(2025)

        Pipeline next step (one line)::

            board.sort("draft_prob", descending=True).head(30)

    See Also:
        * `hoopR <https://hoopR.sportsdataverse.org>`_ -- men's basketball (R)
        * `nba_api <https://github.com/swar/nba_api>`_ -- NBA/WNBA (Python)
    """
    seasons_list = [seasons] if isinstance(seasons, int) else list(seasons)
    bpm = mbb_box_bpm(seasons_list, league=league) if seasons_list else pl.DataFrame()
    if bpm.is_empty():
        out = pl.DataFrame(schema=_SCHEMA)
        return out.to_pandas() if return_as_pandas else out
    art = load_artifact(f"{get_player_value_constants(league).bundle_prefix}_draft")
    uni = bpm.filter(pl.col("min") >= float(art.get("min_minutes", 150.0)))

    if "team_net" in art["feature_cols"] or "pos_score" in art["feature_cols"]:
        # women's upside proxies: program strength + position (WNBA classes
        # are senior-constant and roster height drops departed players)
        from sportsdataverse.mbb.mbb_player_value_constants import aggregate_player_seasons  # noqa: PLC0415
        from sportsdataverse.mbb.mbb_team_ratings import mbb_team_ratings  # noqa: PLC0415

        rat = mbb_team_ratings(seasons_list, league=league).filter(pl.col("games") >= 10)
        net = rat.with_columns(
            (
                (pl.col("adj_o") - pl.col("adj_o").mean().over("season"))
                - (pl.col("adj_d") - pl.col("adj_d").mean().over("season"))
            ).alias("team_net")
        ).select("season", "team_id", "team_net")
        agg_pos = aggregate_player_seasons(seasons_list, league=league).select(
            "player_id", "season", "team_id", "position"
        )
        uni = (
            uni.join(net, on=["season", "team_id"], how="left")
            .join(agg_pos, on=["player_id", "season", "team_id"], how="left")
            .with_columns(
                pl.col("team_net").fill_null(0.0),
                pl.when(pl.col("position").fill_null("").str.contains("(?i)C"))
                .then(1.0)
                .when(pl.col("position").fill_null("").str.contains("(?i)F"))
                .then(0.5)
                .otherwise(0.0)
                .alias("pos_score"),
            )
        )

    if {"is_fr", "is_so", "height_in"} & set(art["feature_cols"]):
        rc = _load_roster_class(seasons_list, league=league)
        assert uni.schema["player_id"] == rc.schema["player_id"] == pl.Utf8
        uni = uni.join(rc, on=["player_id", "season"], how="left").with_columns(
            (pl.col("class").fill_null("") == "Freshman").cast(pl.Float64).alias("is_fr"),
            (pl.col("class").fill_null("") == "Sophomore").cast(pl.Float64).alias("is_so"),
            pl.col("height_in").fill_null(pl.col("height_in").median()),
        )

    labels = list(art.get("archetype_labels") or [])
    if labels and any(c.startswith("arch_") for c in art["feature_cols"]):
        arch = mbb_archetypes(seasons_list, league=league).select("player_id", "season", "team_id", "archetype")
        assert uni.schema["player_id"] == arch.schema["player_id"] == pl.Utf8
        uni = uni.join(arch, on=["player_id", "season", "team_id"], how="left")
        uni = uni.with_columns(
            [
                (pl.col("archetype").fill_null("") == lab).cast(pl.Float64).alias(f"arch_{i}")
                for i, lab in enumerate(labels)
                if f"arch_{i}" in art["feature_cols"]
            ]
        )

    X = uni.select(art["feature_cols"]).fill_null(0.0).to_numpy()
    mu = np.asarray(art["feature_mean"], dtype=float)
    sd = np.asarray(art["feature_sd"], dtype=float)
    Z = np.hstack([np.ones((len(X), 1)), (X - mu) / sd])
    prob = 1.0 / (1.0 + np.exp(-(Z @ np.asarray(art["prob_coef"], dtype=float))))
    pick = Z @ np.asarray(art["pick_coef"], dtype=float)
    if art.get("pick_log_target"):
        pick = np.exp(pick)
    pick = np.clip(pick, 1.0, None)

    edges = [float(e) for e in art["tier_edges"]]
    tier_labels = list(art["tier_labels"])
    tiers = [tier_labels[int(np.searchsorted(edges, p))] for p in pick]
    out = (
        uni.select("player_id", "player", "season", "team_id")
        .with_columns(
            pl.Series("draft_prob", prob, dtype=pl.Float64),
            pl.Series("projected_pick", pick, dtype=pl.Float64),
            pl.Series("pro_tier", tiers, dtype=pl.Utf8),
        )
        .select(list(_SCHEMA))
        .sort("season", "draft_prob", descending=[False, True])
    )
    return out.to_pandas() if return_as_pandas else out
