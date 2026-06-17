"""NFL expected points and win probability calculators.

Mirrors nflfastR's ``calculate_expected_points()`` and
``calculate_win_probability()`` R functions.  Works directly on nflverse-format
play-by-play DataFrames (polars, as returned by ``load_nfl_pbp()``).

Required columns per function — same contract as nflfastR:

``calculate_expected_points``:
    ``season``, ``posteam``, ``home_team``, ``roof``,
    ``half_seconds_remaining``, ``yardline_100``, ``down``, ``ydstogo``,
    ``posteam_timeouts_remaining``, ``defteam_timeouts_remaining``

``calculate_win_probability``:
    All EP columns plus ``score_differential``, ``game_seconds_remaining``,
    ``spread_line``, ``receive_2h_ko``

Example:
    Compute EP on a loaded PBP season::

        import polars as pl
        from sportsdataverse.nfl import load_nfl_pbp
        from sportsdataverse.nfl.ep_wp import calculate_expected_points, calculate_win_probability

        pbp = load_nfl_pbp([2023])
        pbp_with_ep = calculate_expected_points(pbp)
        pbp_with_wp = calculate_win_probability(pbp_with_ep)
        print(pbp_with_wp.select("ep", "wp", "vegas_wp").head())

    See Also:
        * `nflfastR`_ -- the R package this API mirrors
        * `nflreadpy`_ -- Python parity wrapper for nflverse loaders

    .. _nflfastR: https://www.nflfastr.com
    .. _nflreadpy: https://github.com/nflverse/nflreadpy
"""

from __future__ import annotations

from functools import lru_cache
from importlib.resources import files as _resource_files
from pathlib import Path
from typing import TYPE_CHECKING, Union

if TYPE_CHECKING:
    import pandas as pd

import numpy as np
import polars as pl

from sportsdataverse.nfl.model_vars import (
    _EP_POINT_VALUES,
    defense_score_vec,
    end_change_vec,
    kickoff_turnovers,
    kickoff_vec,
    offense_score_vec,
)

# ---------------------------------------------------------------------------
# Feature lists — mirror nflfastR's ep_model_select / wp_*_model_select
# ---------------------------------------------------------------------------

EP_FEATURES: list[str] = [
    "half_seconds_remaining",
    "yardline_100",
    "home",
    "retractable",
    "dome",
    "outdoors",
    "ydstogo",
    "era0",
    "era1",
    "era2",
    "era3",
    "era4",
    "down1",
    "down2",
    "down3",
    "down4",
    "posteam_timeouts_remaining",
    "defteam_timeouts_remaining",
]

WP_NAIVE_FEATURES: list[str] = [
    "receive_2h_ko",
    "home",
    "half_seconds_remaining",
    "game_seconds_remaining",
    "Diff_Time_Ratio",
    "score_differential",
    "down",
    "ydstogo",
    "yardline_100",
    "posteam_timeouts_remaining",
    "defteam_timeouts_remaining",
]

WP_SPREAD_FEATURES: list[str] = [
    "receive_2h_ko",
    "spread_time",
    "home",
    "half_seconds_remaining",
    "game_seconds_remaining",
    "Diff_Time_Ratio",
    "score_differential",
    "down",
    "ydstogo",
    "yardline_100",
    "posteam_timeouts_remaining",
    "defteam_timeouts_remaining",
]

# EP class order: TD=0, OppTD=1, FG=2, OppFG=3, Safety=4, OppSafety=5, No_Score=6
# _EP_POINT_VALUES is imported from model_vars at the top of this module.

_EP_CLASS_NAMES: list[str] = [
    "td_prob",
    "opp_td_prob",
    "fg_prob",
    "opp_fg_prob",
    "safety_prob",
    "opp_safety_prob",
    "no_score_prob",
]

# ---------------------------------------------------------------------------
# CP — completion probability feature contract
# ---------------------------------------------------------------------------
# Mirrors fastrmodels cp_model (Zach Feldman) + nflfastR helper_add_cp_cpoe.R.
# Only eras 2-4 are used (no era0/era1 — model was trained on 2006+ data).
# Filter to pass plays (air_yards not null) before scoring.

CP_FEATURES: list[str] = [
    "air_yards",
    "yardline_100",
    "ydstogo",
    "down1",
    "down2",
    "down3",
    "down4",
    "air_is_zero",
    "pass_middle",
    "era2",
    "era3",
    "era4",
    "qb_hit",
    "home",
    "outdoors",
    "retractable",
    "dome",
    "distance_to_sticks",
]

# ---------------------------------------------------------------------------
# XYAC — expected yards after catch feature contract (4 sub-models)
# ---------------------------------------------------------------------------
# Mirrors fastrmodels xyac_* models (mean / median / sd / prob_complete).
# All four share the same feature vector; outputs differ.
# Requires ep + cp to already be computed (EP and CP are features).

XYAC_FEATURES: list[str] = [
    "season",
    "half_seconds_remaining",
    "yardline_100",
    "ydstogo",
    "down",
    "home",
    "qb_hit",
    "air_yards",
    "air_is_zero",
    "pass_middle",
    "era2",
    "era3",
    "era4",
    "cp",
    "ep",
]

# ---------------------------------------------------------------------------
# Model loading (lazy — avoids ImportError when .ubj files are absent)
# ---------------------------------------------------------------------------


def _model_path(name: str) -> Path:
    return Path(str(_resource_files("sportsdataverse").joinpath(f"nfl/models/{name}")))


@lru_cache(maxsize=4)
def _load_model(name: str):
    """Load a named XGBoost Booster from nfl/models/.  Cached per process."""
    from xgboost import Booster

    path = _model_path(name)
    if not path.exists():
        raise FileNotFoundError(
            f"NFL model '{name}' not found at {path}. Run the track6 training pipeline to produce the model files."
        )
    b = Booster({"nthread": 4})
    b.load_model(str(path))
    return b


# ---------------------------------------------------------------------------
# Feature engineering — mirrors nflfastR make_model_mutations()
# ---------------------------------------------------------------------------


def _make_model_mutations(df: pl.DataFrame) -> pl.DataFrame:
    """Add era/roof/down one-hots and the ``home`` indicator.

    Matches the R ``make_model_mutations()`` in nflfastR exactly:
    era bins, retractable/dome/outdoors from ``roof``, down dummies,
    home indicator from ``posteam == home_team``.
    """
    df = df.with_columns(
        # Era flags
        pl.when(pl.col("season") <= 2001).then(1).otherwise(0).alias("era0"),
        pl.when((pl.col("season") > 2001) & (pl.col("season") <= 2005)).then(1).otherwise(0).alias("era1"),
        pl.when((pl.col("season") > 2005) & (pl.col("season") <= 2013)).then(1).otherwise(0).alias("era2"),
        pl.when((pl.col("season") > 2013) & (pl.col("season") <= 2017)).then(1).otherwise(0).alias("era3"),
        pl.when(pl.col("season") > 2017).then(1).otherwise(0).alias("era4"),
        # Down dummies
        pl.when(pl.col("down") == 1).then(1).otherwise(0).alias("down1"),
        pl.when(pl.col("down") == 2).then(1).otherwise(0).alias("down2"),
        pl.when(pl.col("down") == 3).then(1).otherwise(0).alias("down3"),
        pl.when(pl.col("down") == 4).then(1).otherwise(0).alias("down4"),
        # Home indicator
        pl.when(pl.col("posteam") == pl.col("home_team")).then(1).otherwise(0).alias("home"),
    )

    # Roof one-hots: open/closed/null → retractable; dome → dome; outdoors → outdoors
    if "roof" in df.columns:
        df = df.with_columns(
            pl.when(pl.col("roof").is_null() | pl.col("roof").is_in(["open", "closed"]))
            .then(1)
            .otherwise(0)
            .alias("retractable"),
            pl.when(pl.col("roof") == "dome").then(1).otherwise(0).alias("dome"),
            pl.when(pl.col("roof") == "outdoors").then(1).otherwise(0).alias("outdoors"),
        )
    else:
        # Default: treat as retractable when roof is unknown
        df = df.with_columns(
            pl.lit(1).alias("retractable"),
            pl.lit(0).alias("dome"),
            pl.lit(0).alias("outdoors"),
        )

    return df


def _add_wp_aux(df: pl.DataFrame) -> pl.DataFrame:
    """Add WP derived features: elapsed_share, spread_time, Diff_Time_Ratio.

    Requires ``game_seconds_remaining`` and ``score_differential``.
    ``spread_line`` is used for ``spread_time``; if absent or null,
    ``spread_time`` is set to 0 and the WP-naive model should be used instead.
    """
    # Cast spread_line to Float64 so negation works even when all values are null
    # (polars 1.x raises on neg for dtype Null without the cast).
    _spread = pl.col("spread_line").cast(pl.Float64) if "spread_line" in df.columns else pl.lit(0.0)
    df = df.with_columns(
        pl.when(pl.col("posteam") == pl.col("home_team")).then(_spread).otherwise(-_spread).alias("posteam_spread"),
        ((3600 - pl.col("game_seconds_remaining")) / 3600).clip(0.0, 1.0).alias("elapsed_share"),
    )

    df = df.with_columns(
        (pl.col("posteam_spread") * ((-4.0 * pl.col("elapsed_share")).exp())).alias("spread_time"),
        (pl.col("score_differential") / ((-4.0 * pl.col("elapsed_share")).exp())).alias("Diff_Time_Ratio"),
    )

    # When spread_line is null, spread_time = 0 (use wp_naive model)
    if "spread_line" in df.columns:
        df = df.with_columns(
            pl.when(pl.col("spread_line").is_null())
            .then(pl.lit(0.0))
            .otherwise(pl.col("spread_time"))
            .alias("spread_time"),
        )

    return df


# ---------------------------------------------------------------------------
# CP feature engineering — mirrors nflfastR helper_add_cp_cpoe.R
# ---------------------------------------------------------------------------


def _make_cp_mutations(df: pl.DataFrame) -> pl.DataFrame:
    """Add CP-specific derived columns to a nflverse-format DataFrame.

    Computes the three derived features that aren't direct column copies:
    - ``air_is_zero`` — air_yards == 0
    - ``distance_to_sticks`` — ydstogo - air_yards
    - Era flags era2..4 (era0/era1 are intentionally excluded from CP)
    - ``home`` indicator (if not already present)
    - Roof one-hots (identical to :func:`_make_model_mutations`)
    - ``pass_middle`` — ``pass_location == "middle"`` coerced to int
    """
    df = df.with_columns(
        pl.when(pl.col("air_yards") == 0).then(1).otherwise(0).alias("air_is_zero"),
        (pl.col("ydstogo") - pl.col("air_yards")).alias("distance_to_sticks"),
        pl.when((pl.col("season") > 2005) & (pl.col("season") <= 2013)).then(1).otherwise(0).alias("era2"),
        pl.when((pl.col("season") > 2013) & (pl.col("season") <= 2017)).then(1).otherwise(0).alias("era3"),
        pl.when(pl.col("season") > 2017).then(1).otherwise(0).alias("era4"),
    )
    if "home" not in df.columns:
        df = df.with_columns(
            pl.when(pl.col("posteam") == pl.col("home_team")).then(1).otherwise(0).alias("home"),
        )
    if "pass_middle" not in df.columns and "pass_location" in df.columns:
        df = df.with_columns(
            pl.when(pl.col("pass_location") == "middle").then(1).otherwise(0).alias("pass_middle"),
        )
    elif "pass_middle" not in df.columns:
        df = df.with_columns(pl.lit(0).alias("pass_middle"))

    if "qb_hit" not in df.columns:
        df = df.with_columns(pl.lit(0).alias("qb_hit"))
    else:
        df = df.with_columns(pl.col("qb_hit").cast(pl.Int8))

    if "roof" in df.columns:
        df = df.with_columns(
            pl.when(pl.col("roof").is_null() | pl.col("roof").is_in(["open", "closed"]))
            .then(1)
            .otherwise(0)
            .alias("retractable"),
            pl.when(pl.col("roof") == "dome").then(1).otherwise(0).alias("dome"),
            pl.when(pl.col("roof") == "outdoors").then(1).otherwise(0).alias("outdoors"),
        )
    else:
        df = df.with_columns(
            pl.lit(1).alias("retractable"),
            pl.lit(0).alias("dome"),
            pl.lit(0).alias("outdoors"),
        )

    df = df.with_columns(
        pl.when(pl.col("down") == 1).then(1).otherwise(0).alias("down1"),
        pl.when(pl.col("down") == 2).then(1).otherwise(0).alias("down2"),
        pl.when(pl.col("down") == 3).then(1).otherwise(0).alias("down3"),
        pl.when(pl.col("down") == 4).then(1).otherwise(0).alias("down4"),
    )
    return df


# ---------------------------------------------------------------------------
# ESPN-format adapters — identical mutations to the nflverse path
# ---------------------------------------------------------------------------
# Both _espn_ep_features and _espn_wp_features produce the same (N, K)
# float32 arrays that _make_model_mutations / _add_wp_aux produce for
# nflverse-format DataFrames.  The only difference is column naming:
#   nflverse:  "half_seconds_remaining", "yardline_100", "down", ...
#   ESPN:      "start.TimeSecsRem", "start.yardsToEndzone", "start.down", ...
# Both feed the identical model files (ep_model.ubj / wp_spread.ubj /
# wp_naive.ubj), so a fix to the feature engineering in one path must be
# mirrored in the other.
#
# Roof: ESPN play-level data doesn't carry a per-play roof type, so all
# ESPN plays default to retractable=1 / dome=0 / outdoors=0.  This matches
# how nflfastR handles missing roof data in its make_model_mutations().


def _espn_ep_features(
    play_df: pl.DataFrame,
    *,
    half_sec_col: str = "start.TimeSecsRem",
    yardline_col: str = "start.yardsToEndzone",
    home_col: str = "start.is_home",
    ydstogo_col: str = "start.distance",
    down1_col: str = "down_1",
    down2_col: str = "down_2",
    down3_col: str = "down_3",
    down4_col: str = "down_4",
    pos_timeouts_col: str = "start.posTeamTimeouts",
    def_timeouts_col: str = "start.defPosTeamTimeouts",
) -> np.ndarray:
    """Build the 18-feature EP matrix (nflfastR format) from ESPN play data.

    Produces the same ``(N, 18)`` float32 feature array as the nflverse path
    in :func:`calculate_expected_points`, using ESPN ``start.*`` / ``end.*``
    column conventions.  Era bins and down one-hots are computed identically
    to :func:`_make_model_mutations`.

    Pass as-is to ``DMatrix(X, feature_names=EP_FEATURES)`` then
    ``_load_model("ep_model.ubj").predict(dmat)``.

    Args:
        play_df: ESPN-format play DataFrame (from ``NFLPlayProcess``).
        half_sec_col: Column for half seconds remaining.
        yardline_col: Column for yards to end zone.  Use
            ``"start.yardsToEndzone.touchback"`` for the kickoff-touchback
            variant.
        home_col: Boolean column indicating pos team is home.
        ydstogo_col: Column for yards to go.  Use ``"distance"`` (top-level
            kickoff override) for the touchback variant.
        down1_col … down4_col: Boolean down-indicator columns.  Use
            ``"down_1_end"`` … ``"down_4_end"`` for the end-of-play variant.
        pos_timeouts_col: Possessing-team timeouts remaining.
        def_timeouts_col: Defending-team timeouts remaining.

    Returns:
        ``(N, 18)`` float32 ndarray in :data:`EP_FEATURES` column order.
    """
    df = play_df.with_columns(
        pl.when(pl.col("season") <= 2001).then(1).otherwise(0).alias("_era0"),
        pl.when((pl.col("season") > 2001) & (pl.col("season") <= 2005)).then(1).otherwise(0).alias("_era1"),
        pl.when((pl.col("season") > 2005) & (pl.col("season") <= 2013)).then(1).otherwise(0).alias("_era2"),
        pl.when((pl.col("season") > 2013) & (pl.col("season") <= 2017)).then(1).otherwise(0).alias("_era3"),
        pl.when(pl.col("season") > 2017).then(1).otherwise(0).alias("_era4"),
    )
    return (
        df.select(
            pl.col(half_sec_col).alias("half_seconds_remaining"),
            pl.col(yardline_col).alias("yardline_100"),
            pl.col(home_col).cast(pl.Int8).alias("home"),
            pl.lit(1).alias("retractable"),  # ESPN data: default retractable
            pl.lit(0).alias("dome"),
            pl.lit(0).alias("outdoors"),
            pl.col(ydstogo_col).alias("ydstogo"),
            pl.col("_era0").alias("era0"),
            pl.col("_era1").alias("era1"),
            pl.col("_era2").alias("era2"),
            pl.col("_era3").alias("era3"),
            pl.col("_era4").alias("era4"),
            pl.col(down1_col).cast(pl.Int8).alias("down1"),
            pl.col(down2_col).cast(pl.Int8).alias("down2"),
            pl.col(down3_col).cast(pl.Int8).alias("down3"),
            pl.col(down4_col).cast(pl.Int8).alias("down4"),
            pl.col(pos_timeouts_col).alias("posteam_timeouts_remaining"),
            pl.col(def_timeouts_col).alias("defteam_timeouts_remaining"),
        )
        .to_numpy(allow_copy=True)
        .astype(np.float32)
    )


def _espn_wp_features(
    play_df: pl.DataFrame,
    *,
    receive_ko_col: str = "start.pos_team_receives_2H_kickoff",
    spread_time_col: str = "start.spread_time",
    home_col: str = "start.is_home",
    half_sec_col: str = "start.TimeSecsRem",
    game_sec_col: str = "start.adj_TimeSecsRem",
    score_diff_col: str = "pos_score_diff_start",
    down_col: str = "start.down",
    ydstogo_col: str = "start.distance",
    yardline_col: str = "start.yardsToEndzone",
    pos_timeouts_col: str = "start.posTeamTimeouts",
    def_timeouts_col: str = "start.defPosTeamTimeouts",
    include_spread: bool = True,
) -> np.ndarray:
    """Build the WP feature matrix (nflfastR format) from ESPN play data.

    Mirrors :func:`_add_wp_aux` feature engineering:

    - ``elapsed_share = clip((3600 - game_seconds_remaining) / 3600, 0, 1)``
    - ``Diff_Time_Ratio = score_differential / exp(-4 * elapsed_share)``

    ``spread_time`` must already be computed by ``__add_spread_time()``
    (which applies the same ``spread * exp(-4 * elapsed_share)`` formula) and
    referenced via *spread_time_col*.

    Args:
        play_df: ESPN-format play DataFrame.
        receive_ko_col: Boolean receives-2H-kickoff column.
        spread_time_col: Pre-computed spread × time-decay column.
        home_col: Boolean home-team indicator.
        half_sec_col: Half seconds remaining.
        game_sec_col: Full-game (adjusted) seconds remaining.
        score_diff_col: Raw score differential (pos team minus def team).
        down_col: Integer down (1–4).
        ydstogo_col: Yards to go.
        yardline_col: Yards to end zone.  Use
            ``"start.yardsToEndzone.touchback"`` for the kickoff-touchback
            variant.
        pos_timeouts_col / def_timeouts_col: Timeout counts.
        include_spread: ``True`` → ``(N, 12)`` in :data:`WP_SPREAD_FEATURES`
            order; ``False`` → ``(N, 11)`` in :data:`WP_NAIVE_FEATURES` order.

    Returns:
        float32 ndarray in the requested WP feature order.
    """
    df = play_df.with_columns(
        ((3600.0 - pl.col(game_sec_col)) / 3600.0).clip(0.0, 1.0).alias("_elapsed"),
    ).with_columns(
        (pl.col(score_diff_col) / ((-4.0 * pl.col("_elapsed")).exp())).alias("_dtr"),
    )
    exprs = [pl.col(receive_ko_col).cast(pl.Int8).alias("receive_2h_ko")]
    if include_spread:
        exprs.append(pl.col(spread_time_col).alias("spread_time"))
    exprs.extend(
        [
            pl.col(home_col).cast(pl.Int8).alias("home"),
            pl.col(half_sec_col).alias("half_seconds_remaining"),
            pl.col(game_sec_col).alias("game_seconds_remaining"),
            pl.col("_dtr").alias("Diff_Time_Ratio"),
            pl.col(score_diff_col).alias("score_differential"),
            pl.col(down_col).alias("down"),
            pl.col(ydstogo_col).alias("ydstogo"),
            pl.col(yardline_col).alias("yardline_100"),
            pl.col(pos_timeouts_col).alias("posteam_timeouts_remaining"),
            pl.col(def_timeouts_col).alias("defteam_timeouts_remaining"),
        ]
    )
    return df.select(exprs).to_numpy(allow_copy=True).astype(np.float32)


# ---------------------------------------------------------------------------
# ESPN-format adapters — CP and XYAC
# ---------------------------------------------------------------------------
# Both _espn_cp_features and _espn_xyac_features produce the same (N, K)
# float32 arrays as the nflverse path via _make_cp_mutations.  ESPN plays
# default to retractable=1/dome=0/outdoors=0 (no per-play roof column).


def _espn_cp_features(
    play_df: pl.DataFrame,
    *,
    air_yards_col: str = "air_yards",
    yardline_col: str = "start.yardsToEndzone",
    ydstogo_col: str = "start.distance",
    down1_col: str = "down_1",
    down2_col: str = "down_2",
    down3_col: str = "down_3",
    down4_col: str = "down_4",
    pass_middle_col: str | None = None,
    qb_hit_col: str | None = None,
    home_col: str = "start.is_home",
) -> np.ndarray:
    """Build the 18-feature CP matrix (nflfastR format) from ESPN play data.

    Mirrors :func:`_make_cp_mutations` feature engineering using ESPN column
    conventions.  Produces a ``(N, 18)`` float32 array in :data:`CP_FEATURES`
    column order suitable for ``DMatrix(X, feature_names=CP_FEATURES)`` then
    ``_load_model("cp_model.ubj").predict(dmat)``.

    Intended for pass plays only — filter to ``air_yards`` not-null before
    calling this function.

    Args:
        play_df: ESPN-format play DataFrame (pass plays only).
        air_yards_col: Air yards column.
        yardline_col: Yards to end zone.
        ydstogo_col: Yards to go.
        down1_col … down4_col: Boolean down-indicator columns.
        pass_middle_col: Boolean/int column for middle-of-field pass location.
            When ``None`` or not present, defaults to 0.
        qb_hit_col: Boolean/int QB-hit indicator column.  When ``None`` or
            not present, defaults to 0.
        home_col: Boolean home-team indicator.

    Returns:
        ``(N, 18)`` float32 ndarray in :data:`CP_FEATURES` column order.
    """
    df = play_df.with_columns(
        pl.when(pl.col(air_yards_col) == 0).then(1).otherwise(0).alias("_air_is_zero"),
        (pl.col(ydstogo_col) - pl.col(air_yards_col)).alias("_distance_to_sticks"),
        pl.when((pl.col("season") > 2005) & (pl.col("season") <= 2013)).then(1).otherwise(0).alias("_era2"),
        pl.when((pl.col("season") > 2013) & (pl.col("season") <= 2017)).then(1).otherwise(0).alias("_era3"),
        pl.when(pl.col("season") > 2017).then(1).otherwise(0).alias("_era4"),
    )
    pass_mid = (
        pl.col(pass_middle_col).cast(pl.Int8)
        if pass_middle_col is not None and pass_middle_col in play_df.columns
        else pl.lit(0)
    )
    qb_hit = pl.col(qb_hit_col).cast(pl.Int8) if qb_hit_col is not None and qb_hit_col in play_df.columns else pl.lit(0)
    return (
        df.select(
            pl.col(air_yards_col).alias("air_yards"),
            pl.col(yardline_col).alias("yardline_100"),
            pl.col(ydstogo_col).alias("ydstogo"),
            pl.col(down1_col).cast(pl.Int8).alias("down1"),
            pl.col(down2_col).cast(pl.Int8).alias("down2"),
            pl.col(down3_col).cast(pl.Int8).alias("down3"),
            pl.col(down4_col).cast(pl.Int8).alias("down4"),
            pl.col("_air_is_zero").alias("air_is_zero"),
            pass_mid.alias("pass_middle"),
            pl.col("_era2").alias("era2"),
            pl.col("_era3").alias("era3"),
            pl.col("_era4").alias("era4"),
            qb_hit.alias("qb_hit"),
            pl.col(home_col).cast(pl.Int8).alias("home"),
            pl.lit(0).alias("outdoors"),
            pl.lit(1).alias("retractable"),
            pl.lit(0).alias("dome"),
            pl.col("_distance_to_sticks").alias("distance_to_sticks"),
        )
        .to_numpy(allow_copy=True)
        .astype(np.float32)
    )


def _espn_xyac_features(
    play_df: pl.DataFrame,
    *,
    air_yards_col: str = "air_yards",
    yardline_col: str = "start.yardsToEndzone",
    ydstogo_col: str = "start.distance",
    down_col: str = "start.down",
    half_sec_col: str = "start.TimeSecsRem",
    home_col: str = "start.is_home",
    qb_hit_col: str | None = None,
    pass_middle_col: str | None = None,
    cp_col: str = "cp",
    ep_col: str = "ep",
) -> np.ndarray:
    """Build the 15-feature XYAC matrix (nflfastR format) from ESPN play data.

    Mirrors :data:`XYAC_FEATURES` column order.  Requires ``cp`` and ``ep``
    to already be computed and present on *play_df*.

    Args:
        play_df: ESPN-format pass-play DataFrame with ``cp`` and ``ep``.
        air_yards_col: Air yards column.
        yardline_col: Yards to end zone.
        ydstogo_col: Yards to go.
        down_col: Integer down column (1–4, NOT boolean one-hots).
        half_sec_col: Half seconds remaining.
        home_col: Boolean home-team indicator.
        qb_hit_col: QB-hit indicator column.  Defaults to 0 when absent.
        pass_middle_col: Middle-field pass column.  Defaults to 0 when absent.
        cp_col: Pre-computed completion probability column.
        ep_col: Pre-computed expected points column.

    Returns:
        ``(N, 15)`` float32 ndarray in :data:`XYAC_FEATURES` column order.
    """
    df = play_df.with_columns(
        pl.when(pl.col(air_yards_col) == 0).then(1).otherwise(0).alias("_air_is_zero"),
        pl.when((pl.col("season") > 2005) & (pl.col("season") <= 2013)).then(1).otherwise(0).alias("_era2"),
        pl.when((pl.col("season") > 2013) & (pl.col("season") <= 2017)).then(1).otherwise(0).alias("_era3"),
        pl.when(pl.col("season") > 2017).then(1).otherwise(0).alias("_era4"),
    )
    pass_mid = (
        pl.col(pass_middle_col).cast(pl.Int8)
        if pass_middle_col is not None and pass_middle_col in play_df.columns
        else pl.lit(0)
    )
    qb_hit = pl.col(qb_hit_col).cast(pl.Int8) if qb_hit_col is not None and qb_hit_col in play_df.columns else pl.lit(0)
    return (
        df.select(
            pl.col("season"),
            pl.col(half_sec_col).alias("half_seconds_remaining"),
            pl.col(yardline_col).alias("yardline_100"),
            pl.col(ydstogo_col).alias("ydstogo"),
            pl.col(down_col).alias("down"),
            pl.col(home_col).cast(pl.Int8).alias("home"),
            qb_hit.alias("qb_hit"),
            pl.col(air_yards_col).alias("air_yards"),
            pl.col("_air_is_zero").alias("air_is_zero"),
            pass_mid.alias("pass_middle"),
            pl.col("_era2").alias("era2"),
            pl.col("_era3").alias("era3"),
            pl.col("_era4").alias("era4"),
            pl.col(cp_col).alias("cp"),
            pl.col(ep_col).alias("ep"),
        )
        .to_numpy(allow_copy=True)
        .astype(np.float32)
    )


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def calculate_expected_points(
    pbp_data: pl.DataFrame,
    *,
    return_as_pandas: bool = False,
) -> Union[pl.DataFrame, "pd.DataFrame"]:
    """Compute expected points for provided plays.

    Mirrors nflfastR's ``calculate_expected_points()``.  Drops and recomputes
    any existing ``ep`` / ``*_prob`` columns so the output is always fresh.

    Args:
        pbp_data: Play-by-play DataFrame with nflverse columns.  Required:
            ``season``, ``posteam``, ``home_team``, ``roof``,
            ``half_seconds_remaining``, ``yardline_100``, ``down``,
            ``ydstogo``, ``posteam_timeouts_remaining``,
            ``defteam_timeouts_remaining``.
        return_as_pandas: When ``True``, return a ``pandas.DataFrame``.

    Returns:
        DataFrame with the original columns plus:
        ``td_prob``, ``opp_td_prob``, ``fg_prob``, ``opp_fg_prob``,
        ``safety_prob``, ``opp_safety_prob``, ``no_score_prob``, and ``ep``
        (expected points, clipped to [-10, 10]).

    Example:
        Quick start::

            from sportsdataverse.nfl import load_nfl_pbp
            from sportsdataverse.nfl.ep_wp import calculate_expected_points

            pbp = load_nfl_pbp([2023])
            pbp_ep = calculate_expected_points(pbp)
            print(pbp_ep.select("ep").head())
    """
    from xgboost import DMatrix

    # Drop stale columns
    drop = ["ep"] + _EP_CLASS_NAMES
    df = pbp_data.drop([c for c in drop if c in pbp_data.columns])

    df = _make_model_mutations(df)

    X = df.select(EP_FEATURES).to_numpy(allow_copy=True).astype(np.float32)
    dmat = DMatrix(X, feature_names=EP_FEATURES)

    probs = _load_model("ep_model.ubj").predict(dmat)
    if probs.ndim == 1:
        probs = probs.reshape(-1, 7)

    ep = np.clip(probs @ _EP_POINT_VALUES, -10.0, 10.0)

    prob_frame = pl.DataFrame({name: probs[:, i].tolist() for i, name in enumerate(_EP_CLASS_NAMES)}).with_columns(
        ep=pl.Series("ep", ep.tolist())
    )

    result = pl.concat([df, prob_frame], how="horizontal")

    if return_as_pandas:
        return result.to_pandas()
    return result


def calculate_win_probability(
    pbp_data: pl.DataFrame,
    *,
    return_as_pandas: bool = False,
) -> Union[pl.DataFrame, "pd.DataFrame"]:
    """Compute win probability for provided plays.

    Mirrors nflfastR's ``calculate_win_probability()``.  Uses the
    spread-adjusted model (``wp_spread.ubj``) when ``spread_line`` is
    non-null, and falls back to the naive model (``wp_naive.ubj``) for plays
    with a missing spread line.  Drops and recomputes any existing ``wp`` /
    ``vegas_wp`` columns.

    Args:
        pbp_data: Play-by-play DataFrame.  Required: all EP columns plus
            ``score_differential``, ``game_seconds_remaining``,
            ``spread_line``, ``receive_2h_ko``.
        return_as_pandas: When ``True``, return a ``pandas.DataFrame``.

    Returns:
        DataFrame with the original columns plus:
        ``wp`` (naive WP) and ``vegas_wp`` (spread-adjusted WP).

    Example:
        Quick start::

            from sportsdataverse.nfl import load_nfl_pbp
            from sportsdataverse.nfl.ep_wp import calculate_win_probability

            pbp = load_nfl_pbp([2023])
            pbp_wp = calculate_win_probability(pbp)
            print(pbp_wp.select("wp", "vegas_wp").head())
    """
    from xgboost import DMatrix

    df = pbp_data.drop([c for c in ("wp", "vegas_wp") if c in pbp_data.columns])

    if "home" not in df.columns:
        df = _make_model_mutations(df)

    df = _add_wp_aux(df)

    n = len(df)

    # --- WP naive ---
    X_naive = df.select(WP_NAIVE_FEATURES).to_numpy(allow_copy=True).astype(np.float32)
    dmat_naive = DMatrix(X_naive, feature_names=WP_NAIVE_FEATURES)
    wp_naive = _load_model("wp_naive.ubj").predict(dmat_naive)

    # --- WP spread (or naive fallback per-row when spread_line is null) ---
    X_spread = df.select(WP_SPREAD_FEATURES).to_numpy(allow_copy=True).astype(np.float32)
    dmat_spread = DMatrix(X_spread, feature_names=WP_SPREAD_FEATURES)
    wp_spread = _load_model("wp_spread.ubj").predict(dmat_spread)

    if "spread_line" in df.columns:
        # Where spread_line is null, fall back to naive wp for vegas_wp
        has_spread = np.array(df["spread_line"].is_not_null().to_list(), dtype=bool)
        vegas_wp = np.where(has_spread, wp_spread, wp_naive)
    else:
        vegas_wp = wp_naive

    result = df.with_columns(
        wp=pl.Series("wp", wp_naive.tolist()),
        vegas_wp=pl.Series("vegas_wp", vegas_wp.tolist()),
    )

    if return_as_pandas:
        return result.to_pandas()
    return result


# ---------------------------------------------------------------------------
# CP / CPOE public API
# ---------------------------------------------------------------------------


def calculate_completion_probability(
    pbp_data: pl.DataFrame,
    *,
    return_as_pandas: bool = False,
) -> Union[pl.DataFrame, "pd.DataFrame"]:
    """Compute completion probability (CP) and CPOE for pass plays.

    Mirrors nflfastR's ``helper_add_cp_cpoe.R``.  Scores only intended pass
    plays (where ``air_yards`` is not null); non-pass plays receive null in
    the ``cp`` column.  When ``complete_pass`` is present,
    ``cpoe = complete_pass - cp`` is also added.

    Drops and recomputes any existing ``cp`` / ``cpoe`` columns.

    Args:
        pbp_data: nflverse-format play-by-play DataFrame.  Required:
            ``air_yards``, ``season``, ``ydstogo``, ``down``,
            ``posteam``, ``home_team``.  Optional: ``roof``,
            ``pass_location`` (for ``pass_middle``), ``qb_hit``,
            ``complete_pass`` (to derive ``cpoe``).
        return_as_pandas: When ``True``, return a ``pandas.DataFrame``.

    Returns:
        DataFrame with the original columns plus ``cp`` (null for non-pass
        plays) and ``cpoe`` (null when ``complete_pass`` absent).

    Example:
        Quick start::

            from sportsdataverse.nfl import load_nfl_pbp
            from sportsdataverse.nfl.ep_wp import calculate_completion_probability

            pbp = load_nfl_pbp([2023])
            pbp_cp = calculate_completion_probability(pbp)
            print(pbp_cp.select("cp", "cpoe").head())
    """
    from xgboost import DMatrix

    df = pbp_data.drop([c for c in ("cp", "cpoe") if c in pbp_data.columns])
    df = df.with_row_index("_row_idx")

    pass_df = df.filter(pl.col("air_yards").is_not_null())

    if len(pass_df) > 0:
        pass_df = _make_cp_mutations(pass_df)
        X = pass_df.select(CP_FEATURES).to_numpy(allow_copy=True).astype(np.float32)
        cp_preds = _load_model("cp_model.ubj").predict(DMatrix(X, feature_names=CP_FEATURES))
        cp_frame = pass_df.select("_row_idx").with_columns(pl.Series("cp", cp_preds.tolist(), dtype=pl.Float64))
    else:
        cp_frame = pl.DataFrame(
            {
                "_row_idx": pl.Series([], dtype=pl.UInt32),
                "cp": pl.Series([], dtype=pl.Float64),
            }
        )

    result = df.join(cp_frame, on="_row_idx", how="left").drop("_row_idx")

    if "complete_pass" in result.columns:
        result = result.with_columns((pl.col("complete_pass").cast(pl.Float64) - pl.col("cp")).alias("cpoe"))
    else:
        result = result.with_columns(pl.lit(None).cast(pl.Float64).alias("cpoe"))

    if return_as_pandas:
        return result.to_pandas()
    return result


# ---------------------------------------------------------------------------
# XYAC public API
# ---------------------------------------------------------------------------

_XYAC_OUT_COLS: tuple[str, ...] = (
    "xyac_mean_yardage",
    "xyac_median_yardage",
    "xyac_sd_yardage",
    "xyac_prob_complete",
)
_XYAC_MODEL_FILES: tuple[str, ...] = (
    "xyac_mean_yardage.ubj",
    "xyac_median_yardage.ubj",
    "xyac_sd_yardage.ubj",
    "xyac_prob_complete.ubj",
)


def calculate_xyac(
    pbp_data: pl.DataFrame,
    *,
    return_as_pandas: bool = False,
) -> Union[pl.DataFrame, "pd.DataFrame"]:
    """Compute expected yards after catch (XYAC) for intended pass plays.

    Mirrors nflfastR's four XYAC sub-models (mean yardage, median yardage,
    SD of yardage, completion probability).  Requires ``ep`` and ``cp`` to
    already be present — call :func:`calculate_expected_points` and
    :func:`calculate_completion_probability` before this function.

    Scores all intended pass plays (``air_yards`` not null and ``cp`` + ``ep``
    not null); non-pass plays receive null.  Drops and recomputes any existing
    XYAC output columns.

    Args:
        pbp_data: nflverse-format play-by-play DataFrame with ``ep`` and
            ``cp`` columns already computed.  Required: ``air_yards``,
            ``season``, ``half_seconds_remaining``, ``yardline_100``,
            ``ydstogo``, ``down``, ``home`` (or ``posteam`` + ``home_team``),
            ``ep``, ``cp``.  Optional: ``qb_hit``, ``pass_location``.
        return_as_pandas: When ``True``, return a ``pandas.DataFrame``.

    Returns:
        DataFrame with the original columns plus:
        ``xyac_mean_yardage``, ``xyac_median_yardage``, ``xyac_sd_yardage``,
        ``xyac_prob_complete`` (null for non-pass plays).

    Example:
        Quick start::

            from sportsdataverse.nfl import load_nfl_pbp
            from sportsdataverse.nfl.ep_wp import (
                calculate_expected_points,
                calculate_completion_probability,
                calculate_xyac,
            )

            pbp = load_nfl_pbp([2023])
            pbp = calculate_expected_points(pbp)
            pbp = calculate_completion_probability(pbp)
            pbp = calculate_xyac(pbp)
            print(pbp.select("xyac_mean_yardage", "xyac_prob_complete").head())
    """
    from xgboost import DMatrix

    df = pbp_data.drop([c for c in _XYAC_OUT_COLS if c in pbp_data.columns])
    df = df.with_row_index("_row_idx")

    pass_df = df.filter(pl.col("air_yards").is_not_null() & pl.col("cp").is_not_null() & pl.col("ep").is_not_null())

    if len(pass_df) > 0:
        pass_df = _make_cp_mutations(pass_df)
        X = pass_df.select(XYAC_FEATURES).to_numpy(allow_copy=True).astype(np.float32)
        dmat = DMatrix(X, feature_names=XYAC_FEATURES)
        xyac_frame = pass_df.select("_row_idx")
        for col, model_file in zip(_XYAC_OUT_COLS, _XYAC_MODEL_FILES):
            preds = _load_model(model_file).predict(dmat)
            xyac_frame = xyac_frame.with_columns(pl.Series(col, preds.tolist(), dtype=pl.Float64))
    else:
        xyac_frame = pl.DataFrame(
            {
                "_row_idx": pl.Series([], dtype=pl.UInt32),
                **{col: pl.Series([], dtype=pl.Float64) for col in _XYAC_OUT_COLS},
            }
        )

    result = df.join(xyac_frame, on="_row_idx", how="left").drop("_row_idx")

    if return_as_pandas:
        return result.to_pandas()
    return result


# ---------------------------------------------------------------------------
# EPA derivation (lifted from NFLPlayProcess.__process_epa)
# ---------------------------------------------------------------------------

#: Columns the EPA derivation reads.  ``calculate_epa`` validates that these
#: are present and raises a clear ``KeyError`` if the caller hasn't scored the
#: EP point estimates / classified the plays first.
_EPA_REQUIRED_COLUMNS: tuple[str, ...] = (
    "game_id",
    "type.text",
    "text",
    "EP_start",
    "EP_end",
    "EP_start_touchback",
    "change_of_pos_team",
    "downs_turnover",
    "kickoff_onside",
    "scoring_play",
    "end_of_half",
    "penalty_in_text",
)


def calculate_epa(df: pl.DataFrame) -> pl.DataFrame:
    """Derive expected points added (EPA) from pre-scored EP point estimates.

    This is the **derivation half** of ``NFLPlayProcess.__process_epa`` lifted
    into a shared, model-free function so the same nflfastR-faithful EPA logic
    can be reused by the streaming ``enrich_nfl_pbp`` pipeline and by
    ``__process_epa`` itself.  It performs **no** model inference — the caller
    must already have scored the per-play EP point estimates.

    Derivation rules (mirror nflfastR / the original ``__process_epa``):

    * Scoring overlays rewrite ``EP_end`` to the realized point value
      (offense TD ``+7`` / ``+6.92`` / 2pt variants, made FG ``+3``,
      defensive scores, extra points, etc.) using the same ``type.text`` /
      ``text`` classification as ``__process_epa``.
    * Turnovers (``end_change_vec`` / ``downs_turnover``), kickoff turnovers
      and recovered onside kicks flip ``EP_end`` to the opponent's
      perspective (``EP_end * -1``).
    * ``lag_EP_end`` is the previous play's ``EP_end``; ``EP_between`` flips
      its sign on a prior-play possession change.
    * Kickoffs use ``EP_start_touchback`` as ``EP_start``.
    * ``EPA = EP_end - EP_start`` normally; ``-EP_start`` on a non-scoring
      end-of-half play; ``0`` on a timeout; ``EP_end - EP_start + EP_between``
      on a (non-kickoff, non-``Penalty``) penalty-in-text play.

    **Every** ``shift`` is grouped ``.over("game_id")`` so a concatenated
    multi-game frame never leaks EP across game boundaries — this differs from
    ``__process_epa`` (which runs one game per instance and therefore needs no
    grouping).

    Args:
        df: Play-by-play DataFrame that already carries the EP point estimates
            under the ESPN-internal names ``EP_start``, ``EP_end`` and
            ``EP_start_touchback`` (e.g. as produced by the EP-scoring half of
            ``__process_epa``), plus the play-classification / flag columns:
            ``game_id``, ``type.text``, ``text``, ``change_of_pos_team``,
            ``downs_turnover``, ``kickoff_onside``, ``scoring_play``,
            ``end_of_half`` and ``penalty_in_text``.  See
            :data:`_EPA_REQUIRED_COLUMNS`.  This function does **not** score EP
            itself — score it first via the EP feature pipeline (the ``EP_*``
            triple is the ESPN-internal naming, distinct from
            :func:`calculate_expected_points`'s lowercase ``ep``).

    Returns:
        The input frame with the EPA derivation applied.  ``EP_start`` is
        rewritten to ``0.92`` for scoring-attempt play types (``Extra Point
        Good``, ``Extra Point Missed``, ``Two-Point Conversion Good``,
        ``Two-Point Conversion Missed``, ``Two Point Pass``, ``Two Point Rush``,
        ``Blocked PAT``) before any other overlays fire.  ``EP_start`` /
        ``EP_end`` are then rewritten in place (overlays, sign flips,
        touchback), ``EP_between``, ``lag_EP_end`` and
        ``lag_change_of_pos_team`` are added, ``EPA`` is added, and lowercase
        nflverse aliases ``ep`` (``= EP_end``), ``epa`` (``= EPA``),
        ``ep_start`` (``= EP_start``) and ``ep_end`` (``= EP_end``) are added
        for downstream contract parity.

    Raises:
        KeyError: If any column in :data:`_EPA_REQUIRED_COLUMNS` is absent.

    Example:
        Derive EPA from a pre-scored frame::

            import polars as pl
            from sportsdataverse.nfl.ep_wp import calculate_epa

            # `scored` already has EP_start / EP_end / EP_start_touchback
            out = calculate_epa(scored)
            print(out.select("game_id", "ep", "epa").head())
    """
    missing = [c for c in _EPA_REQUIRED_COLUMNS if c not in df.columns]
    if missing:
        raise KeyError(
            "calculate_epa: input frame is missing required EPA-derivation "
            f"columns: {missing}.  Score the EP point estimates "
            "(EP_start / EP_end / EP_start_touchback) and classify the plays "
            "before calling calculate_epa."
        )

    play_df = (
        df.with_columns(
            # --- Scoring-attempt EP_start override (must precede EP_end overlays) ---
            # PAT / 2pt / Blocked-PAT plays compute EPA as ``points_value - EP_start``;
            # the model score for those plays is meaningless so we force EP_start = 0.92
            # (the pre-snap expected value of a scoring attempt) before any EP_end
            # branch fires.  Mirrors nfl_pbp.py lines 3496-3511 verbatim.
            EP_start=pl.when(
                pl.col("type.text").is_in(
                    [
                        "Extra Point Good",
                        "Extra Point Missed",
                        "Two-Point Conversion Good",
                        "Two-Point Conversion Missed",
                        "Two Point Pass",
                        "Two Point Rush",
                        "Blocked PAT",
                    ],
                ),
            )
            .then(0.92)
            .otherwise(pl.col("EP_start")),
        )
        .with_columns(
            # --- Scoring overlays + turnover sign flips on EP_end ---
            EP_end=pl.when(
                (pl.col("type.text").str.to_lowercase().str.contains(r"end of game")).or_(
                    pl.col("type.text").str.to_lowercase().str.contains(r"end of half"),
                ),
            )
            .then(0)
            # Defensive 2pt Conversion
            .when(pl.col("type.text").is_in(["Defensive 2pt Conversion"]))
            .then(-2)
            # Safeties
            .when(
                (pl.col("type.text").is_in(defense_score_vec)).and_(
                    pl.col("text").str.to_lowercase().str.contains(r"(?i)safety"),
                ),
            )
            .then(-2)
            # Defense TD + Successful Two-Point Conversion
            .when(
                (pl.col("type.text").is_in(defense_score_vec))
                .and_(pl.col("text").str.to_lowercase().str.contains(r"(?i)conversion"))
                .and_(pl.col("text").str.to_lowercase().str.contains(r"(?i)failed") == False),
            )
            .then(-8)
            # Defense TD + Failed Two-Point Conversion
            .when(
                (pl.col("type.text").is_in(defense_score_vec))
                .and_(pl.col("text").str.to_lowercase().str.contains(r"(?i)conversion"))
                .and_(pl.col("text").str.to_lowercase().str.contains(r"(?i)failed")),
            )
            .then(-6)
            # Defense TD + Kick/PAT Missed
            .when(
                (pl.col("type.text").is_in(defense_score_vec))
                .and_(pl.col("text").str.to_lowercase().str.contains(r"PAT"))
                .and_(pl.col("text").str.to_lowercase().str.contains(r"(?i)missed")),
            )
            .then(-6)
            # Defense TD + Kick/PAT Good
            .when(
                (pl.col("type.text").is_in(defense_score_vec)).and_(
                    pl.col("text").str.to_lowercase().str.contains(r"kick\)"),
                ),
            )
            .then(-7)
            # Defense TD
            .when(pl.col("type.text").is_in(defense_score_vec))
            .then(-6.92)
            # Offense TD + Failed Two-Point Conversion
            .when(
                (pl.col("type.text").is_in(offense_score_vec))
                .and_(pl.col("text").str.to_lowercase().str.contains(r"(?i)conversion"))
                .and_(pl.col("text").str.to_lowercase().str.contains(r"(?i)failed")),
            )
            .then(6)
            # Offense TD + Successful Two-Point Conversion
            .when(
                (pl.col("type.text").is_in(offense_score_vec))
                .and_(pl.col("text").str.to_lowercase().str.contains(r"(?i)conversion"))
                .and_(pl.col("text").str.to_lowercase().str.contains(r"(?i)failed") == False),
            )
            .then(8)
            # Offense Made FG
            .when(
                (pl.col("type.text").is_in(offense_score_vec))
                .and_(pl.col("type.text").str.to_lowercase().str.contains(r"(?i)field goal"))
                .and_(pl.col("type.text").str.to_lowercase().str.contains(r"(?i)good")),
            )
            .then(3)
            # Offense TD + Kick/PAT Missed
            .when(
                (pl.col("type.text").is_in(offense_score_vec))
                .and_(pl.col("text").str.to_lowercase().str.contains(r"PAT"))
                .and_(pl.col("text").str.to_lowercase().str.contains(r"(?i)missed")),
            )
            .then(6)
            # Offense TD + Kick/PAT Good
            .when(
                (pl.col("type.text").is_in(offense_score_vec)).and_(
                    pl.col("text").str.to_lowercase().str.contains(r"kick\)"),
                ),
            )
            .then(7)
            # Offense TD
            .when(pl.col("type.text").is_in(offense_score_vec))
            .then(6.92)
            # Extra Point Good
            .when(pl.col("type.text").is_in(["Extra Point Good"]))
            .then(1)
            # Extra Point Missed
            .when(pl.col("type.text").is_in(["Extra Point Missed"]))
            .then(0)
            # Two-Point Conversion Good
            .when(pl.col("type.text").is_in(["Two-Point Conversion Good"]))
            .then(2)
            # Two-Point Conversion Missed
            .when(pl.col("type.text").is_in(["Two-Point Conversion Missed"]))
            .then(0)
            # Two Point Pass/Rush Missed (Pre-2014 Data)
            .when(
                (pl.col("type.text").is_in(["Two Point Pass", "Two Point Rush"])).and_(
                    pl.col("text").str.to_lowercase().str.contains(r"(?i)no good"),
                ),
            )
            .then(0)
            # Two Point Pass/Rush Good (Pre-2014 Data)
            .when(
                (pl.col("type.text").is_in(["Two Point Pass", "Two Point Rush"])).and_(
                    pl.col("text").str.to_lowercase().str.contains(r"(?i)no good") == False,
                ),
            )
            .then(2)
            # Blocked PAT
            .when(pl.col("type.text").is_in(["Blocked PAT"]))
            .then(0)
            # Flips for Turnovers that aren't kickoffs
            .when(
                ((pl.col("type.text").is_in(end_change_vec)).or_(pl.col("downs_turnover") == True)).and_(
                    pl.col("type.text").is_in(kickoff_vec) == False,
                ),
            )
            .then(pl.col("EP_end") * -1)
            # Flips for Turnovers that are kickoffs
            .when(pl.col("type.text").is_in(kickoff_turnovers))
            .then(pl.col("EP_end") * -1)
            # Onside kicks
            .when((pl.col("kickoff_onside") == True).and_(pl.col("change_of_pos_team") == True))
            .then(pl.col("EP_end") * -1)
            .otherwise(pl.col("EP_end")),
        )
        .with_columns(
            # Group EVERY shift by game_id so concatenated frames don't leak
            # EP across game boundaries.
            lag_EP_end=pl.col("EP_end").shift(1).over("game_id"),
            lag_change_of_pos_team=pl.col("change_of_pos_team").shift(1).over("game_id"),
        )
        .with_columns(
            lag_change_of_pos_team=pl.when(pl.col("lag_change_of_pos_team").is_null())
            .then(False)
            .otherwise(pl.col("lag_change_of_pos_team")),
        )
        .with_columns(
            EP_between=pl.when(pl.col("lag_change_of_pos_team") == True)
            .then(pl.col("EP_start") + pl.col("lag_EP_end"))
            .otherwise(pl.col("EP_start") - pl.col("lag_EP_end")),
            EP_start=pl.when(
                (pl.col("type.text").is_in(["Timeout", "End Period"])).and_(
                    pl.col("lag_change_of_pos_team") == False,
                ),
            )
            .then(pl.col("lag_EP_end"))
            .otherwise(pl.col("EP_start")),
        )
        .with_columns(
            EP_start=pl.when(pl.col("type.text").is_in(kickoff_vec))
            .then(pl.col("EP_start_touchback"))
            .otherwise(pl.col("EP_start")),
        )
        .with_columns(
            EP_end=pl.when(pl.col("type.text").is_in(["Timeout"])).then(pl.col("EP_start")).otherwise(pl.col("EP_end")),
        )
        .with_columns(
            EPA=pl.when(pl.col("type.text").is_in(["Timeout"]))
            .then(0)
            .when((pl.col("scoring_play") == False).and_(pl.col("end_of_half") == True))
            .then(-1 * pl.col("EP_start"))
            .when((pl.col("type.text").is_in(kickoff_vec)).and_(pl.col("penalty_in_text") == True))
            .then(pl.col("EP_end") - pl.col("EP_start"))
            .when(
                (pl.col("penalty_in_text") == True)
                .and_(pl.col("type.text").is_in(["Penalty"]) == False)
                .and_(pl.col("type.text").is_in(kickoff_vec) == False),
            )
            .then(pl.col("EP_end") - pl.col("EP_start") + pl.col("EP_between"))
            .otherwise(pl.col("EP_end") - pl.col("EP_start")),
        )
    )

    # Lowercase nflverse aliases for downstream contract parity.
    play_df = play_df.with_columns(
        ep=pl.col("EP_end"),
        epa=pl.col("EPA"),
        ep_start=pl.col("EP_start"),
        ep_end=pl.col("EP_end"),
    )

    return play_df


# ---------------------------------------------------------------------------
# WPA derivation (lifted from NFLPlayProcess.__process_wpa)
# ---------------------------------------------------------------------------

_WPA_REQUIRED_COLUMNS: tuple[str, ...] = (
    "game_id",
    "type.text",
    # WP point estimates the orchestrator (or enrich_nfl_pbp) must score first.
    "wp_before",
    "wp_touchback",
    "wp_after",
    # Perspective / flag columns the derivation references.
    "homeTeamId",
    "start.pos_team.id",
    "end.pos_team.id",
    "start.pos_team_receives_2H_kickoff",
    "change_of_pos_team",
    "scoringPlay",
    "kickoff_onside",
    "end_of_half",
    "status_type_completed",
    "pos_score_diff_end",
    "lead_play_type",
    "lead_pos_team",
    "game_play_number",
)


def calculate_wpa(df: pl.DataFrame) -> pl.DataFrame:
    """Derive win probability added (WPA) from pre-scored WP point estimates.

    This is the **derivation half** of ``NFLPlayProcess.__process_wpa`` lifted
    into a shared, model-free function so the same nflfastR-faithful WPA logic
    can be reused by the streaming ``enrich_nfl_pbp`` pipeline and by
    ``__process_wpa`` itself.  It performs **no** model inference — the caller
    must already have scored the per-play WP point estimates
    (``wp_spread.ubj``) for the start / touchback / end feature views and
    attached them as ``wp_before`` / ``wp_touchback`` / ``wp_after``.  This
    mirrors :func:`calculate_epa`, which likewise consumes pre-scored EP point
    estimates and leaves prediction to the orchestrator.

    Derivation rules (mirror the original ``__process_wpa``):

    * **Leading overlay (do not drop):** on a kickoff (``type.text`` in
      ``kickoff_vec``) ``wp_before`` is replaced by ``wp_touchback`` — the
      win-probability scored from the touchback feature view — before any
      other column derives.  This is the WP analogue of the EPA ``0.92``
      scoring-attempt overlay and must fire first.
    * ``def_wp_before = 1 - wp_before``; ``home_wp_before`` / ``away_wp_before``
      are the posteam->home perspective columns (the offense's ``wp_before``
      flows to home when the start possession team is the home team, otherwise
      to the defense ``def_wp_before``).
    * ``wp_after`` is rewritten by the end-of-half / end-of-game / OT two-path:
      timeouts hold ``wp_before``; a completed final play resolves to ``1.0`` /
      ``0.0`` by the winner; end-of-half and ``End Period`` / ``End of Half``
      lead plays take ``lead_wp_before`` (or ``1 - lead_wp_before`` on a
      possession change); a possession change otherwise flips the lead;
      everything else keeps the model ``wp_after``.
    * ``def_wp_after = 1 - wp_after``; ``home_wp_after`` / ``away_wp_after``
      use the **end** possession team for the perspective flip.
    * ``wpa = wp_after - wp_before``.

    **Every** ``shift`` / forward reference is grouped ``.over("game_id")`` so a
    concatenated multi-game frame never leaks WP across game boundaries — the
    ``lead_wp_before`` / ``lead_wp_before2`` shifts and the end-of-game
    ``game_play_number == max()`` lookup are all per-game.  This differs from
    ``__process_wpa`` (which runs one game per instance and therefore needs no
    grouping).

    Args:
        df: Play-by-play DataFrame that already carries the WP point estimates
            ``wp_before`` (start feature view), ``wp_touchback`` (touchback
            feature view) and ``wp_after`` (end feature view), plus the
            play-classification / perspective columns ``game_id``,
            ``type.text``, ``homeTeamId``, ``start.pos_team.id``,
            ``end.pos_team.id``, ``start.pos_team_receives_2H_kickoff``,
            ``change_of_pos_team``, ``scoringPlay``, ``kickoff_onside``,
            ``end_of_half``, ``status_type_completed``, ``pos_score_diff_end``,
            ``lead_play_type``, ``lead_pos_team`` and ``game_play_number``.  See
            :data:`_WPA_REQUIRED_COLUMNS`.  This function does **not** score WP
            itself — score it first via :func:`calculate_win_probability` /
            the ``wp_spread`` feature pipeline.

    Returns:
        The input frame with the WPA derivation applied: ``wp_before`` rewritten
        by the kickoff-touchback overlay; ``def_wp_before``, ``home_wp_before``,
        ``away_wp_before``, ``lead_wp_before``, ``lead_wp_before2``, the
        rewritten ``wp_after``, ``def_wp_after``, ``home_wp_after``,
        ``away_wp_after`` and ``wpa`` added; plus first-class lowercase aliases
        ``wp`` (``= wp_before``), ``def_wp`` (``= def_wp_before``), ``home_wp``
        (``= home_wp_before``) and ``away_wp`` (``= away_wp_before``) for
        downstream contract parity (the per-play offense win probability is the
        pre-snap ``wp_before``, matching nflfastR's ``wp`` semantics).

    Raises:
        KeyError: If any column in :data:`_WPA_REQUIRED_COLUMNS` is absent.

    Example:
        Derive WPA from a pre-scored frame::

            import polars as pl
            from sportsdataverse.nfl.ep_wp import calculate_wpa

            # `scored` already has wp_before / wp_touchback / wp_after
            out = calculate_wpa(scored)
            print(out.select("game_id", "wp", "wpa").head())
    """
    missing = [c for c in _WPA_REQUIRED_COLUMNS if c not in df.columns]
    if missing:
        raise KeyError(
            "calculate_wpa: input frame is missing required WPA-derivation "
            f"columns: {missing}.  Score the WP point estimates "
            "(wp_before / wp_touchback / wp_after via the wp_spread model) and "
            "classify the plays before calling calculate_wpa."
        )

    play_df = (
        df.with_columns(
            # --- Leading overlay: kickoff wp_before uses the touchback view ---
            # Mirrors nfl_pbp.py lines 3964-3967; must fire before the
            # perspective / lead columns derive.
            wp_before=pl.when(pl.col("type.text").is_in(kickoff_vec))
            .then(pl.col("wp_touchback"))
            .otherwise(pl.col("wp_before")),
        )
        .with_columns(
            def_wp_before=1 - pl.col("wp_before"),
        )
        .with_columns(
            home_wp_before=pl.when(pl.col("start.pos_team.id") == pl.col("homeTeamId"))
            .then(pl.col("wp_before"))
            .otherwise(pl.col("def_wp_before")),
            away_wp_before=pl.when(pl.col("start.pos_team.id") != pl.col("homeTeamId"))
            .then(pl.col("wp_before"))
            .otherwise(pl.col("def_wp_before")),
        )
        .with_columns(
            # Group EVERY shift by game_id so concatenated frames don't leak
            # WP across game boundaries.
            lead_wp_before=pl.col("wp_before").shift(-1).over("game_id"),
            lead_wp_before2=pl.col("wp_before").shift(-2).over("game_id"),
        )
        .with_columns(
            wp_after=pl.when(pl.col("type.text").is_in(["Timeout"]))
            .then(pl.col("wp_before"))
            .when(
                (pl.col("status_type_completed") == True)
                .and_(
                    (pl.col("lead_play_type").is_null()).or_(
                        # Per-game max so the end-of-game branch is scoped to
                        # each game's final play in a concatenated frame.
                        pl.col("game_play_number") == pl.col("game_play_number").max().over("game_id"),
                    ),
                )
                .and_(pl.col("pos_score_diff_end") > 0),
            )
            .then(1.0)
            .when(
                (pl.col("status_type_completed") == True)
                .and_(
                    (pl.col("lead_play_type").is_null()).or_(
                        pl.col("game_play_number") == pl.col("game_play_number").max().over("game_id"),
                    ),
                )
                .and_(pl.col("pos_score_diff_end") < 0),
            )
            .then(0.0)
            .when(
                (pl.col("end_of_half") == True)
                .and_(pl.col("start.pos_team.id") == pl.col("lead_pos_team"))
                .and_(pl.col("type.text") != "Timeout"),
            )
            .then(pl.col("lead_wp_before"))
            .when(
                (pl.col("end_of_half") == True)
                .and_(pl.col("start.pos_team.id") != pl.col("end.pos_team.id"))
                .and_(pl.col("type.text") != "Timeout"),
            )
            .then(1 - pl.col("lead_wp_before"))
            .when(
                (pl.col("end_of_half") == True)
                .and_(pl.col("start.pos_team_receives_2H_kickoff") == False)
                .and_(pl.col("type.text") == "Timeout"),
            )
            .then(pl.col("wp_after"))
            .when(
                (pl.col("lead_play_type").is_in(["End Period", "End of Half"])).and_(
                    pl.col("change_of_pos_team") == False,
                ),
            )
            .then(pl.col("lead_wp_before"))
            .when(
                (pl.col("lead_play_type").is_in(["End Period", "End of Half"])).and_(
                    pl.col("change_of_pos_team") == True,
                ),
            )
            .then(1 - pl.col("lead_wp_before"))
            .when((pl.col("kickoff_onside") == True).and_(pl.col("change_of_pos_team") == True))
            .then(pl.col("wp_after"))
            .when((pl.col("start.pos_team.id") != pl.col("end.pos_team.id")).and_(pl.col("scoringPlay") == False))
            .then(1 - pl.col("lead_wp_before"))
            .when((pl.col("start.pos_team.id") != pl.col("end.pos_team.id")).and_(pl.col("scoringPlay") == True))
            .then(pl.col("lead_wp_before"))
            .otherwise(pl.col("wp_after")),
        )
        .with_columns(
            def_wp_after=1 - pl.col("wp_after"),
        )
        .with_columns(
            home_wp_after=pl.when(pl.col("end.pos_team.id") == pl.col("homeTeamId"))
            .then(pl.col("wp_after"))
            .otherwise(pl.col("def_wp_after")),
            away_wp_after=pl.when(pl.col("end.pos_team.id") != pl.col("homeTeamId"))
            .then(pl.col("wp_after"))
            .otherwise(pl.col("def_wp_after")),
        )
        .with_columns(
            wpa=pl.col("wp_after") - pl.col("wp_before"),
        )
    )

    # First-class lowercase aliases for downstream contract parity.  The
    # per-play offense win probability is the pre-snap ``wp_before`` (matching
    # nflfastR's ``wp`` semantics); the home/away/def variants mirror the
    # ``_before`` perspective columns.
    play_df = play_df.with_columns(
        wp=pl.col("wp_before"),
        def_wp=pl.col("def_wp_before"),
        home_wp=pl.col("home_wp_before"),
        away_wp=pl.col("away_wp_before"),
    )

    return play_df
