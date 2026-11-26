"""Shot-value spine: per-shot expected points (qSP) + downstream models.

Five league-agnostic models switched by ``league_id`` (``"00"`` NBA, ``"10"``
WNBA, ``"20"`` G-League):

1. ``score_shot_xpoints`` — per-shot expected points. The NBA's default is the
   bundled location model on twos (:func:`add_shot_make_prob`: the make chance
   from the shot's court location, value and shot type, the fields the
   play-by-play and the shot chart both carry), with threes on the zone table.
   The WNBA and the G League have no booster and use the ``LeagueAverages``
   FG%-by-zone table that ``nba_stats_shotchartdetail`` returns for free
   (``model="zone"`` picks it in any league).
2. ``make_prob_by_context`` / ``make_prob_joint`` — FG% by defender distance and
   shot clock (aggregate buckets, the only form the public API exposes).
3. ``shooter_talent`` — regressed make%-above-expected per shooter.
4. ``shot_selection_quality`` — player expected value per shot vs the league.
5. ``zone_value_map`` — per-player per-zone points / expected-points maps.

Everything downstream consumes the single scored frame from
``score_shot_xpoints`` — no divergent recomputation.
"""

from __future__ import annotations

import functools
import json
from typing import TYPE_CHECKING, Literal, Union

import numpy as np
import pandas as pd
import polars as pl

from sportsdataverse.nba.nba_shot_value_constants import ZONE_COLLAPSE, get_shrinkage_k

if TYPE_CHECKING:
    import xgboost as xgb

#: The bundled shot-make boosters by ``league_id`` (``tools/models/nba_shot_value/train_shot_make.py``).
#: The WNBA (``"10"``) has none: its booster failed the walk-forward gates on 2016-2020 and 2022
#: (``tools/models/nba_shot_value/shot_make_wnba_holdout.json``). The G League (``"20"``) has no
#: play-by-play release to fit on.
SHOT_MAKE_MODELS: dict[str, str] = {"00": "nba_shot_make.ubj"}
#: The columns the shot-make model reads (play-by-play names; ``x_legacy`` / ``y_legacy`` in tenths of
#: a foot from the hoop).
SHOT_MAKE_INPUTS: list[str] = ["x_legacy", "y_legacy", "shot_value", "sub_type"]
#: The design's numeric columns, in order. ``distance_ft`` is the location's distance from the hoop:
#: the play-by-play's ``shot_distance`` rounds where the shot chart's truncates (they differ on half of
#: all shots) and codes many corner threes at 0 ft, so neither source's column is read. One indicator
#: per level in the booster's ``shot_types`` attribute follows.
SHOT_MAKE_NUMERIC: list[str] = ["distance_ft", "x_legacy", "y_legacy", "shot_value"]

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


@functools.lru_cache(maxsize=4)
def load_shot_make_model(league_id: str = "00") -> xgb.Booster:
    """Load the bundled shot-make booster for ``league_id`` (only ``"00"``, the NBA, has one).

    Its attributes carry the shot-type levels it was fitted with (``shot_types``, a JSON list),
    the train / holdout seasons and the fitting script.

    Args:
        league_id: ``"00"`` NBA.

    Returns:
        The ``xgboost.Booster``.

    Raises:
        ValueError: No booster is bundled for ``league_id``, or the booster's feature names are
            not :data:`SHOT_MAKE_NUMERIC` plus its shot-type levels.

    Example:
        Quick start::

            from sportsdataverse.nba.nba_shot_value import load_shot_make_model
            booster = load_shot_make_model("00")
            booster.attr("train_seasons")

    See Also:
        * `hoopR <https://hoopR.sportsdataverse.org>`_ -- men's basketball (R)
    """
    from importlib.resources import files

    import xgboost as xgb

    if league_id not in SHOT_MAKE_MODELS:
        raise ValueError(f"no shot-make model for league_id {league_id!r}; bundled: {sorted(SHOT_MAKE_MODELS)}")
    booster = xgb.Booster()
    booster.load_model(
        bytearray(files("sportsdataverse.nba.models").joinpath(SHOT_MAKE_MODELS[league_id]).read_bytes())
    )
    if list(booster.feature_names or []) != _shot_make_feature_names(_shot_make_levels(booster)):
        raise ValueError(f"{SHOT_MAKE_MODELS[league_id]}: feature names do not match the shot-make design")
    return booster


def _shot_make_levels(booster: xgb.Booster) -> list[str]:
    """The shot-type levels a shot-make booster was fitted with (its ``shot_types`` attribute)."""
    return json.loads(booster.attr("shot_types") or "[]")


def _shot_make_feature_names(levels: list[str]) -> list[str]:
    """The shot-make design's column names: :data:`SHOT_MAKE_NUMERIC`, then ``sub_type=<level>``."""
    return [*SHOT_MAKE_NUMERIC, *(f"sub_type={t}" for t in levels)]


def _located(c: str) -> pl.Expr:
    return pl.col(c).cast(pl.Float64).fill_nan(None)


def _shot_make_design(shots: pl.DataFrame, levels: list[str]) -> np.ndarray:
    """The shot-make design matrix: the numeric inputs (null as NaN), then one indicator per level.

    A shot type outside ``levels`` (rare, or new to the feed) gets every indicator 0, the trees'
    "any other type" branch. The fitting script builds its training matrix with this same function.
    """
    x, y = _located("x_legacy"), _located("y_legacy")
    num = shots.select(
        ((x**2 + y**2).sqrt() / 10).alias("distance_ft"),
        x,
        y,
        pl.col("shot_value").cast(pl.Float64),
    ).to_numpy()
    sub = shots.get_column("sub_type").cast(pl.Utf8)
    ind = np.column_stack([(sub == t).fill_null(False).to_numpy() for t in levels]) if levels else None
    return num if ind is None else np.hstack([num, ind.astype(np.float64)])


def add_shot_make_prob(
    shots: pl.DataFrame,
    *,
    league_id: str = "00",
    return_as_pandas: bool = False,
) -> pl.DataFrame | pd.DataFrame:
    """Add each field-goal attempt's make chance from the bundled location model.

    The model reads only what the play-by-play records for a shot: its court location
    (``x_legacy`` / ``y_legacy``, and its distance from the hoop computed from them), its value
    and its shot type (``sub_type``, e.g. ``"Driving Layup Shot"``). It was fitted on three
    regular-season play-by-play release seasons and gated against the zone table walk-forward by
    season (``tools/models/nba_shot_value/train_shot_make.py``; the fit report sits beside it).

    The make chance sits at the level of the booster's training seasons (2022-23 to 2024-25); the
    league's make rate moves from season to season, by more than a point in some (see
    :func:`score_shot_xpoints`). The shot type is the scorer's label, written after the play, so a
    make chance that reads it already credits some of what the shooter did (a "Running Dunk Shot"
    goes in about 95% of the time).

    Args:
        shots: Play-by-play rows (``nba_stats_pbp`` release shape) with ``x_legacy``,
            ``y_legacy``, ``shot_value`` and ``sub_type``. Other rows may be present.
        league_id: ``"00"`` NBA (the only league with a booster).
        return_as_pandas: Return a pandas DataFrame instead of polars.

    Returns:
        The input plus ``xfg:Float64``: the make chance of every row with ``shot_value`` 2 or 3
        and a location; null on every other row (free throws, non-shots, an unlocated attempt).
        Empty input returns the input with the null column.

    Raises:
        ValueError: No booster is bundled for ``league_id``, or an input column is missing.

    Example:
        Quick start::

            import polars as pl
            from sportsdataverse.nba import load_nba_stats_pbp
            from sportsdataverse.nba.nba_shot_value import add_shot_make_prob
            pbp = add_shot_make_prob(load_nba_stats_pbp([2025]))
            pbp.filter(pl.col("xfg").is_not_null()).select("sub_type", "x_legacy", "y_legacy", "xfg").head()

        Pipeline next step (one line)::

            pbp.group_by("team_id").agg((pl.col("xfg") * pl.col("shot_value")).sum().alias("xfg_points"))

    See Also:
        * `hoopR <https://hoopR.sportsdataverse.org>`_ -- men's basketball (R)
    """
    missing = [c for c in SHOT_MAKE_INPUTS if c not in shots.columns]
    if missing:
        raise ValueError(f"add_shot_make_prob needs columns {missing}")
    booster = load_shot_make_model(league_id)
    scorable = (
        pl.col("shot_value").cast(pl.Float64).is_in([2.0, 3.0]).fill_null(False)
        & _located("x_legacy").is_not_null()
        & _located("y_legacy").is_not_null()
    )
    row = "__shot_make_row"
    idx = shots.with_row_index(row).filter(scorable)
    xfg = np.full(shots.height, np.nan)
    if idx.height:
        xfg[idx[row].to_numpy()] = booster.inplace_predict(_shot_make_design(idx, _shot_make_levels(booster)))
    out = shots.with_columns(pl.Series("xfg", xfg, dtype=pl.Float64).fill_nan(None))
    return out.to_pandas() if return_as_pandas else out


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
    league_id: str = "00",
    model: Literal["zone", "location"] | None = None,
    return_as_pandas: bool = False,
) -> "Union[pl.DataFrame, pd.DataFrame]":
    """Score each shot with its make chance and expected points.

    ``base_fg_pct`` is the make chance: ``model="location"`` (NBA default) is :func:`add_shot_make_prob`
    on ``loc_x`` / ``loc_y`` / ``action_type`` / ``shot_type`` for twos, the zone value for threes and
    unlocated shots; ``model="zone"`` (WNBA / G League default, no booster) is the ``LeagueAverages``
    zone-triple FG% (unmatched: the ``shot_zone_range`` mean). Adds ``shot_value`` (3 for ``3PT`` else
    2), ``xpoints = base_fg_pct * shot_value``, ``actual_points = shot_made_flag * shot_value``.

    Walk-forward (``tools/models/nba_shot_value/shot_make_nba_holdout.json``) its log loss is 3% to 4%
    under the zone table's every season since 2018-19, nearly all at the rim; its level misses by over
    a point on 2018-19, 2020-21 and 2022-23 (owner-accepted).

    Args:
        shots: Per-shot ``Shot_Chart_Detail`` frame (needs ``shot_type``, the
            three zone keys and ``shot_made_flag``; the location model also
            reads ``loc_x``, ``loc_y`` and ``action_type``).
        league_avgs: The ``LeagueAverages`` frame (see :func:`xpoints_baseline`).
        league_id: ``"00"`` NBA, ``"10"`` WNBA, ``"20"`` G-League (the booster to load).
        model: ``"location"``, ``"zone"`` or ``None`` (location where a booster ships).
        return_as_pandas: Return a pandas DataFrame instead of polars.

    Returns:
        The input ``shots`` plus ``shot_value:Int64, base_fg_pct:Float64,
        xpoints:Float64, actual_points:Float64``. Empty input returns the
        augmented schema with zero rows.

    Raises:
        ValueError: A zone join key's dtype differs between the frames, ``model`` is unknown, or
            ``model="location"`` lacks a booster for ``league_id`` or a location column.

    Example:
        Quick start::

            import polars as pl
            from sportsdataverse.nba.nba_shot_value import score_shot_xpoints
            scored = score_shot_xpoints(shots, league_avgs)
            located = score_shot_xpoints(shots, league_avgs, model="location")

        Pipeline next step (one line)::

            scored.group_by("player_id").agg(pl.col("xpoints").sum())

    See Also:
        * `nba_api <https://github.com/swar/nba_api>`_ -- NBA/WNBA (Python)
        * `hoopR <https://hoopR.sportsdataverse.org>`_ -- men's basketball (R)
    """
    if model is None:
        model = "location" if league_id in SHOT_MAKE_MODELS else "zone"
    if model not in ("location", "zone"):
        raise ValueError(f"model must be 'location' or 'zone', got {model!r}")
    if model == "location" and league_id not in SHOT_MAKE_MODELS:
        raise ValueError(f"no shot-make model for league_id {league_id!r}; use model='zone'")
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
    )
    if model == "location":
        missing = [c for c in ("loc_x", "loc_y", "action_type") if c not in scored.columns]
        if missing:
            raise ValueError(f"model='location' needs the shot chart's {missing}")
        located = add_shot_make_prob(
            scored.select(
                pl.col("loc_x").alias("x_legacy"),
                pl.col("loc_y").alias("y_legacy"),
                "shot_value",
                pl.col("action_type").alias("sub_type"),
            ),
            league_id=league_id,
        )
        scored = scored.with_columns(
            pl.when(pl.col("shot_value") == 3)
            .then(pl.col("base_fg_pct"))
            .otherwise(located.get_column("xfg").fill_null(scored.get_column("base_fg_pct")))
            .alias("base_fg_pct")
        )
    scored = scored.with_columns(
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

            import polars as pl
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


def _fetch_scored(
    player_ids: "list[int]",
    season: str,
    league_id: str,
) -> "tuple[pl.DataFrame, pl.DataFrame]":
    """Fetch + concat per-shot frames and keep one LeagueAverages, scored.

    ``game_id_nullable=None`` is REQUIRED: the wrapper's empty-string default
    zero-pads to ``"0000000000"``, a nonexistent game filter that returns 0
    shots. Returns ``(scored_shots, league_avgs)``.
    """
    from sportsdataverse.nba.nba_stats import nba_stats_shotchartdetail
    from sportsdataverse.nba.nba_stats_parsers import parse_nba_stats_result_sets

    shot_frames, league_avgs = [], None
    for pid in player_ids:
        raw = nba_stats_shotchartdetail(
            player_id=str(pid),
            season_nullable=season,
            league_id=league_id,
            context_measure_simple="FGA",
            game_id_nullable=None,
            return_parsed=False,
        )
        parsed = parse_nba_stats_result_sets(raw)
        if not isinstance(parsed, dict):
            continue
        sc = parsed.get("Shot_Chart_Detail")
        if sc is not None and not sc.is_empty():
            shot_frames.append(_pin_shot_ids(sc))
        if league_avgs is None:
            la = parsed.get("LeagueAverages")
            if la is not None and not la.is_empty():
                league_avgs = la
    if not shot_frames or league_avgs is None:
        return pl.DataFrame(), pl.DataFrame()
    shots = pl.concat(shot_frames, how="vertical_relaxed")
    return score_shot_xpoints(shots, league_avgs, league_id=league_id), league_avgs


def _pin_shot_ids(sc: pl.DataFrame) -> pl.DataFrame:
    out = sc
    if "game_id" in out.columns:
        out = out.with_columns(pl.col("game_id").cast(pl.Utf8))  # zero-padded: no int round-trip
    for c in ("player_id", "team_id"):
        if c in out.columns:
            out = out.with_columns(pl.col(c).cast(pl.Int64, strict=False))
    return out


def nba_shot_value(
    player_ids: "list[int]",
    season: str,
    *,
    league_id: str = "00",
    include_context: bool = False,
    return_as_pandas: bool = False,
) -> "dict[str, Union[pl.DataFrame, pd.DataFrame]]":
    """One-call shot-value spine: fetch, score, and run all five models.

    Fetches each player's ``shotchartdetail``, scores per-shot expected points
    (:func:`score_shot_xpoints`: the location model for the NBA, the
    ``LeagueAverages`` zone table elsewhere), and returns the scored shots
    plus shooter talent, selection quality, and zone-value maps (and the
    defender/shot-clock context tables when ``include_context=True``).

    Args:
        player_ids: Player ids to fetch.
        season: Season string, e.g. ``"2022-23"``.
        league_id: ``"00"`` NBA, ``"10"`` WNBA, ``"20"`` G-League.
        include_context: Also fetch + return the ``playerdashptshots``
            defender/shot-clock context tables, once per player and team his
            fetched shots came from.
        return_as_pandas: Return pandas frames instead of polars.

    Returns:
        ``{"shots", "talent", "selection", "zones"}`` (plus ``"context"`` when
        requested). An empty fetch returns a dict of zero-row frames.

    Example:
        Quick start::

            from sportsdataverse.nba import nba_shot_value
            out = nba_shot_value([201939], "2022-23")
            out["talent"].head()

    See Also:
        * `nba_api <https://github.com/swar/nba_api>`_ -- NBA/WNBA (Python)
        * `hoopR <https://hoopR.sportsdataverse.org>`_ -- men's basketball (R)
    """
    scored, league_avgs = _fetch_scored(player_ids, season, league_id)
    out: "dict[str, pl.DataFrame]" = {
        "shots": scored,
        "talent": shooter_talent(scored, league_id=league_id),
        "selection": shot_selection_quality(scored),
        "zones": zone_value_map(scored),
    }
    if include_context:
        from sportsdataverse.nba.nba_stats import nba_stats_playerdashptshots
        from sportsdataverse.nba.nba_stats_parsers import parse_nba_stats_result_sets

        rows = []
        # playerdashptshots needs the player's own team (TeamID "0" with a season is HTTP 500, and the
        # wrapper's default team belongs to its default player), so ask once per (player, team) the
        # shots came from: a player traded mid-season has one context per team.
        wanted = [int(p) for p in player_ids]
        pairs = (
            scored.select("player_id", "team_id")
            .drop_nulls()
            .filter(pl.col("player_id").is_in(wanted))
            .unique()
            .sort("player_id", "team_id")
            .rows()
            if {"player_id", "team_id"} <= set(scored.columns)
            else []
        )
        for pid, tid in pairs:
            raw = nba_stats_playerdashptshots(
                player_id=str(pid), team_id=str(tid), season=season, league_id=league_id, return_parsed=False
            )
            parsed = parse_nba_stats_result_sets(raw)
            if not isinstance(parsed, dict):
                continue
            for rs, col in (
                ("ClosestDefenderShooting", "close_def_dist_range"),
                ("ShotClockShooting", "shot_clock_range"),
            ):
                f = parsed.get(rs)
                if f is not None and not f.is_empty() and col in f.columns:
                    rows.append(
                        f.select(
                            pl.lit(rs).alias("result_set"),
                            pl.col(col).alias("bucket"),
                            pl.col("fga").cast(pl.Int64, strict=False),
                            pl.col("fgm").cast(pl.Int64, strict=False),
                        )
                    )
        stacked = pl.concat(rows, how="vertical_relaxed") if rows else pl.DataFrame()
        ctx = make_prob_by_context(stacked)
        out["context"] = pl.concat(
            [
                ctx["defender"].with_columns(pl.lit("defender").alias("kind")),
                ctx["shot_clock"].with_columns(pl.lit("shot_clock").alias("kind")),
            ],
            how="vertical_relaxed",
        )
    if return_as_pandas:
        return {k: v.to_pandas() for k, v in out.items()}
    return out


def nba_shot_value_lineups(
    group_id: str,
    season: str,
    *,
    team_id: int,
    league_id: str = "00",
    return_as_pandas: bool = False,
) -> "Union[pl.DataFrame, pd.DataFrame]":
    """Scored per-shot frame for one 5-man lineup (``shotchartlineupdetail``).

    Args:
        group_id: The 5-man lineup group id (dash-joined player ids); kept
            ``Utf8``.
        season: Season string, e.g. ``"2022-23"``.
        team_id: The lineup's team id.
        league_id: ``"00"`` NBA, ``"10"`` WNBA, ``"20"`` G-League.
        return_as_pandas: Return a pandas DataFrame instead of polars.

    Returns:
        The lineup's shots scored by :func:`score_shot_xpoints` (with
        ``xpoints``). Empty fetch returns the augmented zero-row schema.

    Example:
        Quick start::

            from sportsdataverse.nba import nba_shot_value_lineups
            df = nba_shot_value_lineups("201939-202691-...", "2022-23", team_id=1610612744)

    See Also:
        * `nba_api <https://github.com/swar/nba_api>`_ -- NBA/WNBA (Python)
    """
    from sportsdataverse.nba.nba_stats import nba_stats_shotchartlineupdetail
    from sportsdataverse.nba.nba_stats_parsers import parse_nba_stats_result_sets

    raw = nba_stats_shotchartlineupdetail(
        group_id=str(group_id),
        season=season,
        team_id_nullable=str(team_id),
        league_id=league_id,
        context_measure_detailed="FGA",
        return_parsed=False,
    )
    parsed = parse_nba_stats_result_sets(raw)
    shots = parsed.get("ShotChartLineupDetail") if isinstance(parsed, dict) else parsed
    # shotchartlineupdetail names its zone table ShotChartLineupLeagueAverage (shotchartdetail: LeagueAverages)
    league_avgs = parsed.get("ShotChartLineupLeagueAverage") if isinstance(parsed, dict) else pl.DataFrame()
    if shots is None or shots.is_empty():
        empty = pl.DataFrame(schema={"group_id": pl.Utf8, "shot_type": pl.Utf8, **_SCORED_EXTRA})
        return empty.to_pandas() if return_as_pandas else empty
    if "group_id" in shots.columns:
        shots = shots.with_columns(pl.col("group_id").cast(pl.Utf8))
    scored = score_shot_xpoints(shots, league_avgs if league_avgs is not None else pl.DataFrame(), league_id=league_id)
    return scored.to_pandas() if return_as_pandas else scored
