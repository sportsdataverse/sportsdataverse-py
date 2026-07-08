"""NFL player projection: season rates, delta-method aging curves, and the
Marcel-style projection engine (compute-on-demand, no bundled artifacts).

Methodology (cited, no code copied): Tom Tango's "Marcel the Monkey" forecaster
(recency-weighted rates regressed to the positional mean) and the standard
delta-method aging curve (paired same-player consecutive seasons, playing-time
weighted, chained and peak-normalized).
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Dict, List, Literal, Union, overload

import polars as pl

from sportsdataverse.nfl.nfl_loaders import load_nfl_player_stats, load_nfl_rosters
from sportsdataverse.nfl.nfl_projection_constants import (
    POSITION_CONSTANTS,
    SCORING_HALF,
    SCORING_PPR,
    SCORING_STANDARD,
    PositionConstants,
    as_of_season_split,
    get_position_constants,
)

if TYPE_CHECKING:  # pragma: no cover
    import pandas as pd

# Counting stats aggregated to season level (all Float64 in the fixtures).
_COUNTING_STATS: List[str] = [
    "completions",
    "attempts",
    "passing_yards",
    "passing_tds",
    "interceptions",
    "carries",
    "rushing_yards",
    "rushing_tds",
    "receptions",
    "targets",
    "receiving_yards",
    "receiving_tds",
    "receiving_air_yards",
    "fumbles_lost",
]

_RATE_SCHEMA_BASE: dict = {
    "player_id": pl.Utf8,
    "season": pl.Int64,
    "position_group": pl.Utf8,
    "age": pl.Float64,
    "games": pl.Int64,
    "volume": pl.Float64,
    "ppg": pl.Float64,
}


def _col_or_zero(name: str, cols: List[str]) -> pl.Expr:
    return pl.col(name) if name in cols else pl.lit(0.0)


def season_player_rates(weekly: pl.DataFrame, rosters: pl.DataFrame) -> pl.DataFrame:
    """Aggregate weekly player stats to one row per (player_id, season).

    Computes games played, position-specific volume (QB = pass attempts,
    RB = carries + targets, WR/TE and default = targets), PPR points per game,
    and per-game component rates (``<stat>_rate``) for every counting stat
    present in ``weekly``.

    Args:
        weekly (pl.DataFrame): nflverse weekly offense stats (at minimum
            ``player_id, season, week, position_group`` + counting stats +
            ``fantasy_points_ppr``).
        rosters (pl.DataFrame): Season rosters carrying ``player_id, season,
            age``.

    Returns:
        pl.DataFrame: One row per (player_id, season) with ``player_id:Utf8,
        season:Int64, position_group:Utf8, age:Float64, games:Int64,
        volume:Float64, ppg:Float64`` plus ``<stat>_rate`` columns. Empty or
        malformed input returns a zero-row frame with the documented schema.

    Example:
        Quick start::

            import sportsdataverse.nfl as nfl
            from sportsdataverse.nfl.nfl_projection import season_player_rates
            weekly = nfl.load_nfl_player_stats()
            rosters = nfl.load_nfl_rosters([2023])
            rates = season_player_rates(weekly, rosters)

    See Also:
        * `nflverse`_ -- full data ecosystem (R + Python)

    .. _nflverse: https://nflverse.nflverse.com
    """
    stats = [c for c in _COUNTING_STATS if c in weekly.columns]
    schema = dict(_RATE_SCHEMA_BASE)
    for c in stats:
        schema[f"{c}_rate"] = pl.Float64
    if weekly.height == 0 or not {"player_id", "season"}.issubset(weekly.columns):
        return pl.DataFrame(schema=schema)

    wk = weekly.with_columns(
        pl.col("player_id").cast(pl.Utf8),
        pl.col("season").cast(pl.Int64),
    )
    games_expr = pl.col("week").n_unique() if "week" in wk.columns else pl.len()
    # sort_by(week).last() = the most recent week's designation; a bare
    # .first() on an unordered group is nondeterministic under parallel group_by
    pos_expr = (
        pl.col("position_group").sort_by("week").drop_nulls().last()
        if "week" in wk.columns
        else pl.col("position_group").drop_nulls().sort().last()
    )
    agg = wk.group_by("player_id", "season").agg(
        pos_expr.alias("position_group"),
        games_expr.cast(pl.Int64).alias("games"),
        *[pl.col(c).sum().alias(c) for c in stats],
        _col_or_zero("fantasy_points_ppr", wk.columns).sum().alias("_fp_ppr"),
    )
    cols = agg.columns
    volume = (
        pl.when(pl.col("position_group") == "QB")
        .then(_col_or_zero("attempts", cols))
        .when(pl.col("position_group") == "RB")
        .then(_col_or_zero("carries", cols) + _col_or_zero("targets", cols))
        .otherwise(_col_or_zero("targets", cols))
        .cast(pl.Float64)
        .alias("volume")
    )
    agg = agg.with_columns(
        volume,
        (pl.col("_fp_ppr") / pl.col("games")).cast(pl.Float64).alias("ppg"),
        *[(pl.col(c) / pl.col("games")).cast(pl.Float64).alias(f"{c}_rate") for c in stats],
    ).drop("_fp_ppr", *stats)

    if {"player_id", "season", "age"}.issubset(rosters.columns) and rosters.height > 0:
        ros = rosters.select(
            pl.col("player_id").cast(pl.Utf8),
            pl.col("season").cast(pl.Int64),
            pl.col("age").cast(pl.Float64),
        ).unique(subset=["player_id", "season"], keep="first")
        assert agg.schema["player_id"] == ros.schema["player_id"]
        assert agg.schema["season"] == ros.schema["season"]
        agg = agg.join(ros, on=["player_id", "season"], how="left")
    else:
        agg = agg.with_columns(pl.lit(None, dtype=pl.Float64).alias("age"))

    ordered = ["player_id", "season", "position_group", "age", "games", "volume", "ppg"] + [f"{c}_rate" for c in stats]
    return agg.select(ordered)


def aging_curve(season_rates: pl.DataFrame, *, position_group: str, rate_col: str = "ppg") -> pl.DataFrame:
    """Delta-method aging curve for a position group, peak-normalized to 1.0.

    Pairs each player's season with the same player's next season, computes the
    playing-time-weighted mean transition multiplier ``rate_next / rate`` per
    starting age, chains the transitions into a level curve (``cum_prod``), and
    normalizes so the peak age has multiplier 1.0. Only paired consecutive
    seasons contribute (the standard survivorship mitigation).

    Args:
        season_rates (pl.DataFrame): Output of :func:`season_player_rates`
            (needs ``player_id, season, position_group, age, volume`` +
            ``rate_col``).
        position_group (str): Position group to fit (``"QB"``/``"RB"``/...).
        rate_col (str): Rate column the curve is fit on.

    Returns:
        pl.DataFrame: ``age:Float64, aging_mult:Float64`` sorted by age. Empty
        input returns a zero-row frame with that schema.

    Example:
        Quick start::

            from sportsdataverse.nfl.nfl_projection import aging_curve, season_player_rates
            curve = aging_curve(rates, position_group="RB")
    """
    consts = get_position_constants(position_group)
    # Ages are bucketed to integer years: roster ages are continuous
    # (birth-date-derived) floats, and per-unique-float transitions chained by
    # cum_prod compound noise into a degenerate curve.
    df = season_rates.filter(
        (pl.col("position_group") == position_group) & (pl.col("volume") >= consts.min_volume)
    ).select("player_id", "season", pl.col("age").floor().alias("age"), "volume", pl.col(rate_col).alias("rate"))
    # match season s (age a) to the same player's season s+1 (age a+1)
    nxt = df.select(
        "player_id",
        (pl.col("season") - 1).alias("season"),
        pl.col("rate").alias("rate_next"),
        pl.col("volume").alias("volume_next"),
    )
    paired = (
        df.join(nxt, on=["player_id", "season"], how="inner")
        .filter(pl.col("rate") > 0)
        .with_columns(
            (pl.col("rate_next") / pl.col("rate")).alias("delta"),
            pl.min_horizontal("volume", "volume_next").alias("w"),
        )
    )
    trans = (
        paired.group_by("age")
        .agg(((pl.col("delta") * pl.col("w")).sum() / pl.col("w").sum()).alias("mult_step"))
        .sort("age")
    )
    if trans.height == 0:
        return pl.DataFrame(schema={"age": pl.Float64, "aging_mult": pl.Float64})
    # chain transitions into a level curve anchored at the youngest age, then peak-normalize
    curve = trans.with_columns(pl.col("mult_step").cum_prod().alias("level"))
    curve = curve.with_columns((pl.col("age") + 1.0).alias("age")).select("age", "level")
    anchor = pl.DataFrame({"age": [float(trans["age"][0])], "level": [1.0]})
    curve = pl.concat([anchor.with_columns(pl.col("age").cast(pl.Float64)), curve], how="vertical").sort("age")
    peak = curve["level"].max()
    return curve.with_columns((pl.col("level") / peak).cast(pl.Float64).alias("aging_mult")).select(
        pl.col("age").cast(pl.Float64), "aging_mult"
    )


def _marcel_blend(hist: pl.DataFrame, *, value_cols: List[str], consts: PositionConstants) -> pl.DataFrame:
    """Recency-weighted Marcel blend regressed to the volume-weighted position mean.

    Shared engine for the rate projection (①) and the usage-share projection
    (③). One row per player: ``_blend_<col>`` blended values, ``reliability``
    (recency-weighted volume), ``proj_volume``, ``proj_games``, ``last_season``,
    ``last_age``.
    """
    weights = consts.recency_weights
    # dense rank: rows sharing a season (mid-season trades -> one row per team)
    # share the same recency weight; ordinal rank would tie-break on
    # nondeterministic group_by row order
    ranked = hist.with_columns(
        pl.col("season").rank(method="dense", descending=True).over("player_id").cast(pl.Int64).alias("_r")
    ).filter(pl.col("_r") <= len(weights))
    wmap = {i + 1: float(w) for i, w in enumerate(weights)}
    ranked = ranked.with_columns(pl.col("_r").replace_strict(wmap, default=0.0, return_dtype=pl.Float64).alias("_w"))
    total_vol = float(hist["volume"].fill_null(0.0).sum())
    pos_means = {
        c: (float((hist[c].fill_null(0.0) * hist["volume"].fill_null(0.0)).sum()) / total_vol if total_vol > 0 else 0.0)
        for c in value_cols
    }
    has_games = "games" in hist.columns
    agg = ranked.group_by("player_id").agg(
        (pl.col("_w") * pl.col("volume")).sum().alias("reliability"),
        pl.col("_w").sum().alias("_wsum"),
        (pl.col("_w") * pl.col("volume")).sum().alias("_wv"),
        *[(pl.col(c).fill_null(0.0) * pl.col("_w") * pl.col("volume")).sum().alias(f"_num_{c}") for c in value_cols],
        ((pl.col("_w") * pl.col("games")).sum() if has_games else pl.lit(0.0)).alias("_wg"),
        pl.col("season").max().alias("last_season"),
        pl.col("age").filter(pl.col("_r") == 1).first().alias("last_age")
        if "age" in hist.columns
        else pl.lit(None, dtype=pl.Float64).alias("last_age"),
    )
    k = consts.shrinkage_k
    shrink = k / (k + pl.col("reliability"))
    blend_exprs = []
    for c in value_cols:
        wmean = pl.col(f"_num_{c}") / pl.max_horizontal(pl.col("_wv"), pl.lit(1e-9))
        blend_exprs.append(((1.0 - shrink) * wmean + shrink * pl.lit(pos_means[c])).alias(f"_blend_{c}"))
    return agg.with_columns(
        *blend_exprs,
        (pl.col("_wv") / pl.max_horizontal(pl.col("_wsum"), pl.lit(1e-9))).alias("proj_volume"),
        (pl.col("_wg") / pl.max_horizontal(pl.col("_wsum"), pl.lit(1e-9))).alias("proj_games"),
    ).drop([f"_num_{c}" for c in value_cols] + ["_wsum", "_wv", "_wg"])


def _aging_ratio(
    blend: pl.DataFrame,
    curve: pl.DataFrame,
    *,
    base_age: float,
    target_season: int,
    damping: float = 1.0,
) -> pl.DataFrame:
    """Attach ``proj_age`` and the aging multiplier ratio ``aging_mult`` =
    ``curve(proj_age) / curve(last_age)`` (nearest-age lookup; 1.0 fallback).

    The raw ratio is damped by the fitted per-position ``aging_damping``
    (``1 + damping * (ratio - 1)``) and clamped to ``[0.75, 1.15]`` — a
    one-season aging effect outside that band is curve noise (thin transition
    cells chained by ``cum_prod``), not signal.
    """
    blend = blend.with_columns(
        pl.col("last_age").fill_null(base_age).alias("last_age"),
    ).with_columns(
        (pl.col("last_age") + (pl.lit(float(target_season)) - pl.col("last_season").cast(pl.Float64))).alias(
            "proj_age"
        ),
    )
    if curve.height == 0:
        return blend.with_columns(pl.lit(1.0).alias("aging_mult"))
    cur = curve.sort("age")
    blend = (
        blend.sort("last_age")
        .join_asof(cur.rename({"aging_mult": "_m_cur"}), left_on="last_age", right_on="age", strategy="nearest")
        .drop("age")
        .sort("proj_age")
        .join_asof(cur.rename({"aging_mult": "_m_proj"}), left_on="proj_age", right_on="age", strategy="nearest")
        .drop("age")
    )
    raw_ratio = (
        pl.when((pl.col("_m_cur") > 0) & pl.col("_m_proj").is_not_null())
        .then(pl.col("_m_proj") / pl.col("_m_cur"))
        .otherwise(1.0)
    )
    damped = (1.0 + damping * (raw_ratio - 1.0)).clip(0.75, 1.15).alias("aging_mult")
    return blend.with_columns(damped).drop("_m_cur", "_m_proj")


@overload
def nfl_player_projection(
    seasons: List[int], target_season: int, *, return_as_pandas: Literal[False] = ...
) -> pl.DataFrame: ...


@overload
def nfl_player_projection(
    seasons: List[int], target_season: int, *, return_as_pandas: Literal[True]
) -> "pd.DataFrame": ...


def nfl_player_projection(
    seasons: List[int], target_season: int, *, return_as_pandas: bool = False
) -> Union[pl.DataFrame, "pd.DataFrame"]:
    """Marcel-style next-season player projection with delta-method aging.

    Loads weekly player stats + rosters, aggregates to season rates, and for
    every player visible in seasons **strictly before** ``target_season``
    (the as-of-date leakage boundary) produces a recency-weighted rate blend
    regressed toward the volume-weighted position mean by
    ``k / (k + reliability)``, scaled by the position aging-curve ratio
    ``aging_mult(proj_age) / aging_mult(current_age)``. The aging curve is fit
    only on the same pre-target history.

    Args:
        seasons (List[int]): History seasons to load (seasons ``>=
            target_season`` are discarded by the leakage split).
        target_season (int): The season being projected.
        return_as_pandas (bool): If True, returns a pandas dataframe.

    Returns:
        pl.DataFrame: One row per projected player: ``player_id:Utf8,
        target_season:Int64, position_group:Utf8, proj_age:Float64,
        proj_ppg:Float64, proj_volume:Float64, proj_games:Float64,
        aging_mult:Float64, reliability:Float64`` plus ``proj_<stat>_rate``
        component-rate columns. Empty history returns a zero-row frame.

    Example:
        Quick start::

            from sportsdataverse.nfl.nfl_projection import nfl_player_projection
            proj = nfl_player_projection([2021, 2022, 2023], 2024)
            proj.sort("proj_ppg", descending=True).head()

        Pandas round-trip::

            proj_pd = nfl_player_projection([2021, 2022, 2023], 2024, return_as_pandas=True)

    See Also:
        * `nflverse`_ -- full data ecosystem (R + Python)
        * `nflreadpy`_ -- direct nflverse Python bindings

    .. _nflverse: https://nflverse.nflverse.com
    .. _nflreadpy: https://github.com/nflverse/nflreadpy
    """
    weekly = load_nfl_player_stats()
    if "season" in weekly.columns and seasons:
        weekly = weekly.filter(pl.col("season").is_in(list(seasons)))
    rosters = load_nfl_rosters(list(seasons))
    rates = season_player_rates(weekly, rosters)
    hist = as_of_season_split(rates, target_season)
    rate_cols = ["ppg"] + [c for c in rates.columns if c.endswith("_rate")]
    out_schema: dict = {
        "player_id": pl.Utf8,
        "target_season": pl.Int64,
        "position_group": pl.Utf8,
        "proj_age": pl.Float64,
        "proj_ppg": pl.Float64,
        "proj_volume": pl.Float64,
        "proj_games": pl.Float64,
        "aging_mult": pl.Float64,
        "reliability": pl.Float64,
        **{f"proj_{c}": pl.Float64 for c in rate_cols if c != "ppg"},
    }
    frames = []
    for pos in sorted([p for p in hist["position_group"].unique().to_list() if p is not None]):
        consts = get_position_constants(pos)
        sub = hist.filter(pl.col("position_group") == pos)
        if sub.height == 0:
            continue
        blend = _marcel_blend(sub, value_cols=rate_cols, consts=consts)
        curve = aging_curve(sub, position_group=pos)
        blend = _aging_ratio(
            blend,
            curve,
            base_age=consts.aging_base_age,
            target_season=target_season,
            damping=consts.aging_damping,
        )
        proj_exprs = [
            (pl.col(f"_blend_{c}") * pl.col("aging_mult")).cast(pl.Float64).alias(f"proj_{c}") for c in rate_cols
        ]
        frames.append(
            blend.with_columns(*proj_exprs, pl.lit(pos).alias("position_group"))
            .with_columns(pl.lit(target_season, dtype=pl.Int64).alias("target_season"))
            .select(list(out_schema.keys()))
        )
    if not frames:
        result = pl.DataFrame(schema=out_schema)
    else:
        result = pl.concat(frames, how="vertical").sort("proj_ppg", descending=True)
    return result.to_pandas(use_pyarrow_extension_array=True) if return_as_pandas else result


def score_fantasy(stats: pl.DataFrame, scoring: Dict[str, float]) -> pl.Series:
    """Score a frame of counting stats under a scoring-format dict.

    Pure ``sum_k stats[k] * scoring[k]`` over the scoring keys present in
    ``stats`` — reused by the fantasy projection (②) and its oracle.

    Args:
        stats (pl.DataFrame): Frame whose columns include some scoring keys.
        scoring (Dict[str, float]): Points per unit for each stat (e.g.
            ``SCORING_PPR``).

    Returns:
        pl.Series: Float64 fantasy points, one value per row (zeros when no
        scoring column is present).

    Example:
        Quick start::

            import polars as pl
            from sportsdataverse.nfl.nfl_projection import score_fantasy
            from sportsdataverse.nfl.nfl_projection_constants import SCORING_PPR
            pts = score_fantasy(pl.DataFrame({"receiving_yards": [100.0], "receptions": [5.0]}), SCORING_PPR)
    """
    present = [k for k in scoring if k in stats.columns]
    if not present:
        return pl.Series("fantasy_points", [0.0] * stats.height, dtype=pl.Float64)
    expr = pl.sum_horizontal([pl.col(k).fill_null(0.0) * scoring[k] for k in present]).cast(pl.Float64)
    return stats.select(expr.alias("fantasy_points"))["fantasy_points"]


_SCORING_BY_NAME: Dict[str, Dict[str, float]] = {
    "ppr": SCORING_PPR,
    "half": SCORING_HALF,
    "standard": SCORING_STANDARD,
}


@overload
def nfl_fantasy_projection(
    seasons: List[int],
    target_season: int,
    *,
    scoring: Union[Dict[str, float], str] = ...,
    calibrate: bool = ...,
    return_as_pandas: Literal[False] = ...,
) -> pl.DataFrame: ...


@overload
def nfl_fantasy_projection(
    seasons: List[int],
    target_season: int,
    *,
    scoring: Union[Dict[str, float], str] = ...,
    calibrate: bool = ...,
    return_as_pandas: Literal[True],
) -> "pd.DataFrame": ...


def nfl_fantasy_projection(
    seasons: List[int],
    target_season: int,
    *,
    scoring: Union[Dict[str, float], str] = "ppr",
    calibrate: bool = True,
    return_as_pandas: bool = False,
) -> Union[pl.DataFrame, "pd.DataFrame"]:
    """Fantasy-points projection: deterministic scoring of the Marcel component
    stats plus a fitted per-position linear calibration.

    Scores :func:`nfl_player_projection`'s projected component *counting* stats
    (rate x projected games) under the scoring format, then applies the fitted
    ``fp_calibration`` ``(a, b)`` from ``POSITION_CONSTANTS``
    (``calibrated = a + b * raw``). The FantasyPros consensus is used only as a
    concurrent-validity oracle in the tests — never as an input.

    Args:
        seasons (List[int]): History seasons to load.
        target_season (int): The season being projected.
        scoring (Union[Dict[str, float], str]): ``"ppr"`` / ``"half"`` /
            ``"standard"`` or a custom points-per-unit dict.
        calibrate (bool): Apply the fitted per-position calibration.
        return_as_pandas (bool): If True, returns a pandas dataframe.

    Returns:
        pl.DataFrame: ``player_id:Utf8, target_season:Int64,
        position_group:Utf8, proj_fantasy_points:Float64,
        proj_fantasy_points_per_game:Float64, position_rank:Int64``.

    Raises:
        ValueError: If ``scoring`` is a string other than ``"ppr"`` /
            ``"half"`` / ``"standard"``.

    Example:
        Quick start::

            from sportsdataverse.nfl.nfl_projection import nfl_fantasy_projection
            fp = nfl_fantasy_projection([2021, 2022, 2023], 2024)
            fp.filter(pl.col("position_group") == "WR").head()

        Custom scoring::

            fp_std = nfl_fantasy_projection([2021, 2022, 2023], 2024, scoring="standard")

    See Also:
        * `nflverse`_ -- full data ecosystem (R + Python)

    .. _nflverse: https://nflverse.nflverse.com
    """
    if isinstance(scoring, str):
        if scoring not in _SCORING_BY_NAME:
            raise ValueError(f"scoring must be one of {sorted(_SCORING_BY_NAME)} or a dict, got {scoring!r}")
        scoring_map = _SCORING_BY_NAME[scoring]
    else:
        scoring_map = scoring
    proj = nfl_player_projection(seasons, target_season)
    counting = proj.select(
        "player_id",
        "target_season",
        "position_group",
        pl.col("proj_games"),
        *[
            (pl.col(f"proj_{k}_rate") * pl.col("proj_games")).alias(k)
            for k in scoring_map
            if f"proj_{k}_rate" in proj.columns
        ],
    )
    fp = score_fantasy(counting, scoring_map)
    out = counting.select("player_id", "target_season", "position_group", "proj_games").with_columns(
        fp.alias("_raw_fp")
    )
    if calibrate:
        cal = pl.DataFrame(
            {
                "position_group": list(POSITION_CONSTANTS.keys()),
                "_cal_a": [c.fp_calibration[0] for c in POSITION_CONSTANTS.values()],
                "_cal_b": [c.fp_calibration[1] for c in POSITION_CONSTANTS.values()],
            }
        )
        default = POSITION_CONSTANTS["DEFAULT"]
        out = out.join(cal, on="position_group", how="left").with_columns(
            pl.col("_cal_a").fill_null(default.fp_calibration[0]),
            pl.col("_cal_b").fill_null(default.fp_calibration[1]),
        )
        out = out.with_columns((pl.col("_cal_a") + pl.col("_cal_b") * pl.col("_raw_fp")).alias("_fp")).drop(
            "_cal_a", "_cal_b"
        )
    else:
        out = out.with_columns(pl.col("_raw_fp").alias("_fp"))
    result = (
        out.with_columns(
            pl.col("_fp").cast(pl.Float64).alias("proj_fantasy_points"),
            (pl.col("_fp") / pl.max_horizontal(pl.col("proj_games"), pl.lit(1e-9)))
            .cast(pl.Float64)
            .alias("proj_fantasy_points_per_game"),
        )
        .with_columns(
            pl.col("proj_fantasy_points")
            .rank(method="dense", descending=True)
            .over("position_group")
            .cast(pl.Int64)
            .alias("position_rank")
        )
        .select(
            "player_id",
            "target_season",
            "position_group",
            "proj_fantasy_points",
            "proj_fantasy_points_per_game",
            "position_rank",
        )
        .sort("position_group", "position_rank")
    )
    return result.to_pandas(use_pyarrow_extension_array=True) if return_as_pandas else result
