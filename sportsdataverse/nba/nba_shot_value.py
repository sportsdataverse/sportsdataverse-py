"""Shot-value spine: per-shot expected points (qSP) + downstream models.

Five league-agnostic models switched by ``league_id`` (``"00"`` NBA, ``"10"``
WNBA, ``"20"`` G-League):

1. ``score_shot_xpoints`` — per-shot expected points from the ``LeagueAverages``
   FG%-by-zone table that ``nba_stats_shotchartdetail`` returns for free.
2. ``make_prob_by_context`` / ``make_prob_joint`` — FG% by defender distance and
   shot clock (aggregate buckets, the only form the public API exposes).
3. ``shooter_talent`` — regressed make%-above-expected per shooter.
4. ``shot_selection_quality`` — player expected value per shot vs the league.
5. ``zone_value_map`` — per-player per-zone points / expected-points maps.

Everything downstream consumes the single scored frame from
``score_shot_xpoints`` — no divergent recomputation.
"""

from __future__ import annotations

from typing import Union

import pandas as pd
import polars as pl

from sportsdataverse.nba.nba_shot_value_constants import ZONE_COLLAPSE, get_shrinkage_k

_ZONE_KEYS = ["shot_zone_basic", "shot_zone_area", "shot_zone_range"]
_BASELINE_SCHEMA = {
    "shot_zone_basic": pl.Utf8,
    "shot_zone_area": pl.Utf8,
    "shot_zone_range": pl.Utf8,
    "base_fg_pct": pl.Float64,
    "is_three": pl.Boolean,
}
_SCORED_EXTRA = {
    "shot_value": pl.Int64,
    "base_fg_pct": pl.Float64,
    "xpoints": pl.Float64,
    "actual_points": pl.Float64,
}


def xpoints_baseline(league_avgs: pl.DataFrame) -> pl.DataFrame:
    """League-average FG% baseline table keyed by the three shot-zone columns.

    Args:
        league_avgs: The ``LeagueAverages`` result set from
            ``nba_stats_shotchartdetail`` (``shot_zone_basic`` /
            ``shot_zone_area`` / ``shot_zone_range`` / ``fga`` / ``fgm`` /
            ``fg_pct``).

    Returns:
        One row per ``(shot_zone_basic, shot_zone_area, shot_zone_range)``:
        ``... base_fg_pct:Float64, is_three:Boolean`` (``is_three`` = the
        basic zone names a three). Empty input returns the zero-row schema.

    Example:
        Quick start::

            from sportsdataverse.nba import nba_stats
            from sportsdataverse.nba.nba_shot_value import xpoints_baseline
            base = xpoints_baseline(league_avgs)
    """
    if league_avgs.is_empty():
        return pl.DataFrame(schema=_BASELINE_SCHEMA)
    return league_avgs.select(
        *_ZONE_KEYS,
        pl.col("fg_pct").cast(pl.Float64).alias("base_fg_pct"),
        pl.col("shot_zone_basic").str.contains("3").alias("is_three"),
    ).unique(subset=_ZONE_KEYS, keep="first")


def score_shot_xpoints(
    shots: pl.DataFrame,
    league_avgs: pl.DataFrame,
    *,
    return_as_pandas: bool = False,
) -> "Union[pl.DataFrame, pd.DataFrame]":
    """Score each shot with expected points from the league-average baseline.

    Joins the per-shot frame to the zone baseline (falling back to the
    within-``shot_zone_range`` mean when a zone triple is unmatched) and adds
    ``shot_value`` (3 for a ``3PT`` shot else 2), ``xpoints = base_fg_pct *
    shot_value``, and ``actual_points = shot_made_flag * shot_value``.

    Args:
        shots: Per-shot ``Shot_Chart_Detail`` frame (needs ``shot_type`` +
            the three zone keys + ``shot_made_flag``).
        league_avgs: The ``LeagueAverages`` frame (see :func:`xpoints_baseline`).
        return_as_pandas: Return a pandas DataFrame instead of polars.

    Returns:
        The input ``shots`` plus ``shot_value:Int64, base_fg_pct:Float64,
        xpoints:Float64, actual_points:Float64``. Empty input returns the
        augmented schema with zero rows.

    Raises:
        ValueError: A zone join key has a different dtype on the two frames.

    Example:
        Quick start::

            from sportsdataverse.nba.nba_shot_value import score_shot_xpoints
            scored = score_shot_xpoints(shots, league_avgs)

        Pipeline next step (one line)::

            scored.group_by("player_id").agg(pl.col("xpoints").sum())

    See Also:
        * `nba_api <https://github.com/swar/nba_api>`_ -- NBA/WNBA (Python)
        * `hoopR <https://hoopR.sportsdataverse.org>`_ -- men's basketball (R)
    """
    if shots.is_empty():
        out = shots.with_columns([pl.lit(None, dtype=t).alias(c) for c, t in _SCORED_EXTRA.items()])
        return out.to_pandas() if return_as_pandas else out

    base = xpoints_baseline(league_avgs)
    for k in _ZONE_KEYS:
        if shots.schema[k] != base.schema[k]:
            raise ValueError(f"join-key dtype mismatch on {k}: {shots.schema[k]} vs {base.schema[k]}")

    scored = shots.with_columns(
        pl.when(pl.col("shot_type").str.starts_with("3")).then(3).otherwise(2).cast(pl.Int64).alias("shot_value")
    ).join(base.select(*_ZONE_KEYS, "base_fg_pct"), on=_ZONE_KEYS, how="left")
    scored = scored.with_columns(
        pl.col("base_fg_pct").fill_null(pl.col("base_fg_pct").mean().over("shot_zone_range")).alias("base_fg_pct")
    ).with_columns(
        (pl.col("base_fg_pct") * pl.col("shot_value")).alias("xpoints"),
        (pl.col("shot_made_flag").cast(pl.Float64) * pl.col("shot_value")).alias("actual_points"),
    )
    return scored.to_pandas() if return_as_pandas else scored


_CONTEXT_SCHEMA = {"bucket": pl.Utf8, "fga": pl.Int64, "fgm": pl.Int64, "fg_pct": pl.Float64}


def make_prob_by_context(
    ptshots: pl.DataFrame,
    *,
    return_as_pandas: bool = False,
) -> "dict[str, Union[pl.DataFrame, pd.DataFrame]]":
    """Marginal FG% tables by defender distance and by shot clock.

    The public API exposes defender-distance and shot-clock only as aggregate
    bucket tables (``playerdashptshots``), not per-shot fields, so this
    aggregates ``Σfgm/Σfga`` across players within each bucket.

    Args:
        ptshots: The stacked ``playerdashptshots`` fixture — one frame with a
            ``result_set`` tag (``ClosestDefenderShooting`` /
            ``ShotClockShooting``) plus ``bucket, fga, fgm``.
        return_as_pandas: Return pandas DataFrames instead of polars.

    Returns:
        ``{"defender": frame, "shot_clock": frame}`` each with rows per
        ``bucket`` (``bucket, fga, fgm, fg_pct``). Missing result sets return
        the zero-row schema.

    Example:
        Quick start::

            from sportsdataverse.nba.nba_shot_value import make_prob_by_context
            tables = make_prob_by_context(ptshots)
            tables["defender"].sort("fg_pct")
    """

    def _agg(result_set: str) -> pl.DataFrame:
        sub = ptshots.filter(pl.col("result_set") == result_set) if "result_set" in ptshots.columns else ptshots.clear()
        if sub.is_empty():
            return pl.DataFrame(schema=_CONTEXT_SCHEMA)
        return (
            sub.group_by("bucket")
            .agg(pl.col("fga").sum().cast(pl.Int64), pl.col("fgm").sum().cast(pl.Int64))
            .with_columns((pl.col("fgm") / pl.col("fga")).alias("fg_pct"))
            .select("bucket", "fga", "fgm", "fg_pct")
            .sort("bucket")
        )

    defender = _agg("ClosestDefenderShooting")
    shot_clock = _agg("ShotClockShooting")
    if return_as_pandas:
        return {"defender": defender.to_pandas(), "shot_clock": shot_clock.to_pandas()}
    return {"defender": defender, "shot_clock": shot_clock}


def make_prob_joint(
    defender: pl.DataFrame,
    shot_clock: pl.DataFrame,
    overall_fg_pct: float,
    *,
    return_as_pandas: bool = False,
) -> "Union[pl.DataFrame, pd.DataFrame]":
    """Independence-combined defender x shot-clock make probability.

    Combines the two marginal FG% tables under a conditional-independence
    assumption via odds multipliers: ``odds(p) = p/(1-p)``;
    ``odds_joint = odds_overall * (odds_def/odds_overall) *
    (odds_clock/odds_overall)``; ``joint = odds_joint/(1+odds_joint)``. This
    assumes defender distance and shot-clock effects are independent given the
    league baseline — a simplification (a late clock correlates with tighter
    defense), documented here so callers weigh it.

    Args:
        defender: The ``"defender"`` marginal table from
            :func:`make_prob_by_context` (``bucket, fg_pct``).
        shot_clock: The ``"shot_clock"`` marginal table (``bucket, fg_pct``).
        overall_fg_pct: The league overall FG% baseline.
        return_as_pandas: Return a pandas DataFrame instead of polars.

    Returns:
        One row per ``(close_def_dist_range, shot_clock_range)``:
        ``close_def_dist_range:Utf8, shot_clock_range:Utf8,
        joint_fg_pct:Float64``. Empty inputs return the zero-row schema.

    Example:
        Quick start::

            from sportsdataverse.nba.nba_shot_value import make_prob_by_context, make_prob_joint
            t = make_prob_by_context(ptshots)
            joint = make_prob_joint(t["defender"], t["shot_clock"], 0.47)
    """
    schema = {"close_def_dist_range": pl.Utf8, "shot_clock_range": pl.Utf8, "joint_fg_pct": pl.Float64}
    if defender.is_empty() or shot_clock.is_empty():
        out = pl.DataFrame(schema=schema)
        return out.to_pandas() if return_as_pandas else out

    o0 = overall_fg_pct / (1.0 - overall_fg_pct)
    d = defender.select(
        pl.col("bucket").alias("close_def_dist_range"),
        (pl.col("fg_pct") / (1.0 - pl.col("fg_pct")) / o0).alias("_def_mult"),
    )
    c = shot_clock.select(
        pl.col("bucket").alias("shot_clock_range"),
        (pl.col("fg_pct") / (1.0 - pl.col("fg_pct")) / o0).alias("_clock_mult"),
    )
    out = (
        d.join(c, how="cross")
        .with_columns((o0 * pl.col("_def_mult") * pl.col("_clock_mult")).alias("_odds"))
        .with_columns((pl.col("_odds") / (1.0 + pl.col("_odds"))).alias("joint_fg_pct"))
        .select("close_def_dist_range", "shot_clock_range", "joint_fg_pct")
        .sort("close_def_dist_range", "shot_clock_range")
    )
    return out.to_pandas() if return_as_pandas else out


_TALENT_SCHEMA = {
    "player_id": pl.Int64,
    "n_att": pl.Int64,
    "actual_makes": pl.Int64,
    "exp_makes": pl.Float64,
    "points_above_expected": pl.Float64,
    "raw_above_pct": pl.Float64,
    "talent_pct": pl.Float64,
}


def shooter_talent(
    scored_shots: pl.DataFrame,
    *,
    league_id: str = "00",
    min_attempts: int = 50,
    return_as_pandas: bool = False,
) -> "Union[pl.DataFrame, pd.DataFrame]":
    """Regressed shooter true-talent: make%-above-expected, shrunk to the mean.

    Aggregates :func:`score_shot_xpoints` output per shooter and regresses the
    raw over-expected rate toward zero by ``n/(n+k)`` (``k =
    get_shrinkage_k(league_id)``, fitted split-half). **As-of leakage
    boundary:** to score a shooter's talent for shots after date *D*, pass
    only that shooter's shots before *D* -- this function does not enforce the
    cut itself.

    Args:
        scored_shots: :func:`score_shot_xpoints` output (needs ``player_id``,
            ``shot_made_flag``, ``base_fg_pct``, ``xpoints``,
            ``actual_points``).
        league_id: ``"00"`` NBA, ``"10"`` WNBA, ``"20"`` G-League.
        min_attempts: Drop shooters with fewer attempts (unstable estimate).
        return_as_pandas: Return a pandas DataFrame instead of polars.

    Returns:
        One row per ``player_id``: ``player_id:Int64, n_att:Int64,
        actual_makes:Int64, exp_makes:Float64, points_above_expected:Float64,
        raw_above_pct:Float64, talent_pct:Float64``. Empty input returns the
        zero-row schema.

    Example:
        Quick start::

            from sportsdataverse.nba.nba_shot_value import score_shot_xpoints, shooter_talent
            talent = shooter_talent(score_shot_xpoints(shots, league_avgs))

        Pipeline next step (one line)::

            talent.sort("talent_pct", descending=True).head(15)

    See Also:
        * `nba_api <https://github.com/swar/nba_api>`_ -- NBA/WNBA (Python)
    """
    if scored_shots.is_empty():
        out = pl.DataFrame(schema=_TALENT_SCHEMA)
        return out.to_pandas() if return_as_pandas else out
    k = get_shrinkage_k(league_id)
    agg = (
        scored_shots.group_by("player_id")
        .agg(
            pl.len().cast(pl.Int64).alias("n_att"),
            pl.col("shot_made_flag").sum().cast(pl.Int64).alias("actual_makes"),
            pl.col("base_fg_pct").sum().alias("exp_makes"),
            (pl.col("actual_points").sum() - pl.col("xpoints").sum()).alias("points_above_expected"),
        )
        .filter(pl.col("n_att") >= min_attempts)
        .with_columns(((pl.col("actual_makes") - pl.col("exp_makes")) / pl.col("n_att")).alias("raw_above_pct"))
        .with_columns((pl.col("raw_above_pct") * pl.col("n_att") / (pl.col("n_att") + k)).alias("talent_pct"))
        .select(list(_TALENT_SCHEMA))
        .sort("talent_pct", descending=True)
    )
    return agg.to_pandas() if return_as_pandas else agg


_SELECTION_SCHEMA = {
    "player_id": pl.Int64,
    "n_att": pl.Int64,
    "xev_per_shot": pl.Float64,
    "league_xev_per_shot": pl.Float64,
    "selection_quality": pl.Float64,
}


def shot_selection_quality(
    scored_shots: pl.DataFrame,
    *,
    min_attempts: int = 50,
    return_as_pandas: bool = False,
) -> "Union[pl.DataFrame, pd.DataFrame]":
    """Player shot-selection quality: mean expected value vs the league mean.

    ``xev_per_shot`` is a player's mean ``xpoints`` (the value of the LOOKS
    they take, independent of makes); ``selection_quality`` is that minus the
    league-wide mean ``xpoints`` over the same frame -- a rim-and-three diet
    scores positive, a mid-range diet negative.

    Args:
        scored_shots: :func:`score_shot_xpoints` output (needs ``player_id``,
            ``xpoints``).
        min_attempts: Drop players with fewer attempts.
        return_as_pandas: Return a pandas DataFrame instead of polars.

    Returns:
        One row per ``player_id``: ``player_id:Int64, n_att:Int64,
        xev_per_shot:Float64, league_xev_per_shot:Float64,
        selection_quality:Float64``. Empty input returns the zero-row schema.

    Example:
        Quick start::

            from sportsdataverse.nba.nba_shot_value import score_shot_xpoints, shot_selection_quality
            sel = shot_selection_quality(score_shot_xpoints(shots, league_avgs))

        Pipeline next step (one line)::

            sel.sort("selection_quality", descending=True).head(15)

    See Also:
        * `nba_api <https://github.com/swar/nba_api>`_ -- NBA/WNBA (Python)
    """
    if scored_shots.is_empty():
        out = pl.DataFrame(schema=_SELECTION_SCHEMA)
        return out.to_pandas() if return_as_pandas else out
    league_xev = float(scored_shots.get_column("xpoints").mean())
    out = (
        scored_shots.group_by("player_id")
        .agg(pl.len().cast(pl.Int64).alias("n_att"), pl.col("xpoints").mean().alias("xev_per_shot"))
        .filter(pl.col("n_att") >= min_attempts)
        .with_columns(
            pl.lit(league_xev).alias("league_xev_per_shot"),
            (pl.col("xev_per_shot") - league_xev).alias("selection_quality"),
        )
        .select(list(_SELECTION_SCHEMA))
        .sort("selection_quality", descending=True)
    )
    return out.to_pandas() if return_as_pandas else out


_ZONE_MAP_SCHEMA = {
    "player_id": pl.Int64,
    "zone": pl.Utf8,
    "att": pl.Int64,
    "makes": pl.Int64,
    "pts": pl.Float64,
    "pps": pl.Float64,
    "xpps": pl.Float64,
    "pps_above_expected": pl.Float64,
}


def zone_value_map(
    scored_shots: pl.DataFrame,
    *,
    return_as_pandas: bool = False,
) -> "Union[pl.DataFrame, pd.DataFrame]":
    """Per-player per-zone value map: points and expected points per shot.

    Collapses ``shot_zone_basic`` to a canonical zone via ``ZONE_COLLAPSE``
    (the two corner-3 zones merge) and aggregates realized vs expected points
    per shot in each zone.

    Args:
        scored_shots: :func:`score_shot_xpoints` output (needs ``player_id``,
            ``shot_zone_basic``, ``shot_made_flag``, ``actual_points``,
            ``xpoints``).
        return_as_pandas: Return a pandas DataFrame instead of polars.

    Returns:
        One row per ``(player_id, zone)``: ``player_id:Int64, zone:Utf8,
        att:Int64, makes:Int64, pts:Float64, pps:Float64, xpps:Float64,
        pps_above_expected:Float64`` (``pps`` = points per shot, ``xpps`` =
        expected). Empty input returns the zero-row schema.

    Example:
        Quick start::

            from sportsdataverse.nba.nba_shot_value import score_shot_xpoints, zone_value_map
            zmap = zone_value_map(score_shot_xpoints(shots, league_avgs))

        Pipeline next step (one line)::

            zmap.filter(pl.col("zone") == "corner_3").sort("pps_above_expected", descending=True)

    See Also:
        * `nba_api <https://github.com/swar/nba_api>`_ -- NBA/WNBA (Python)
    """
    if scored_shots.is_empty():
        out = pl.DataFrame(schema=_ZONE_MAP_SCHEMA)
        return out.to_pandas() if return_as_pandas else out
    out = (
        scored_shots.with_columns(
            pl.col("shot_zone_basic").replace_strict(ZONE_COLLAPSE, default="other").alias("zone")
        )
        .group_by("player_id", "zone")
        .agg(
            pl.len().cast(pl.Int64).alias("att"),
            pl.col("shot_made_flag").sum().cast(pl.Int64).alias("makes"),
            pl.col("actual_points").sum().alias("pts"),
            pl.col("xpoints").sum().alias("_xpts"),
        )
        .with_columns(
            (pl.col("pts") / pl.col("att")).alias("pps"),
            (pl.col("_xpts") / pl.col("att")).alias("xpps"),
        )
        .with_columns((pl.col("pps") - pl.col("xpps")).alias("pps_above_expected"))
        .select(list(_ZONE_MAP_SCHEMA))
        .sort("player_id", "zone")
    )
    return out.to_pandas() if return_as_pandas else out
