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

import os
from functools import lru_cache
from importlib.resources import files as _resource_files
from pathlib import Path
from typing import TYPE_CHECKING, Optional, Union

if TYPE_CHECKING:
    import pandas as pd
    from xgboost import Booster

import numpy as np
import polars as pl

from sportsdataverse.nfl.model_vars import (
    _EP_POINT_VALUES,
    TOUCHBACK_YARDLINE_POST_2016,
    TOUCHBACK_YARDLINE_PRE_2016,
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
# XYAC — expected yards after catch feature contract (one multinomial model)
# ---------------------------------------------------------------------------
# Mirrors nflfastR's ``add_xyac`` / ``xyac_model_select``: a single
# ``multi:softprob`` XGBoost model with ``num_class=76`` (YAC buckets −5..70,
# label = clamp(yac, −5, 70) + 5) over 19 features.  The 5 nflfastR output
# columns are *derived* from the 76-class distribution — see
# :func:`calculate_xyac` — they are NOT direct model outputs.
#
# Feature order must match the trained model's ``feature_names`` exactly.
# ``distance_to_goal = yardline_100 - air_yards`` and
# ``distance_to_sticks = air_yards - ydstogo``.

XYAC_FEATURES: list[str] = [
    "air_yards",
    "yardline_100",
    "ydstogo",
    "distance_to_goal",
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
# XPASS — expected dropback (pass) feature contract (one binary:logistic model)
# ---------------------------------------------------------------------------
# Mirrors nflfastR's ``add_xpass`` / ``prepare_xpass_data`` (helper_add_xpass.R):
# a single ``binary:logistic`` XGBoost model over 17 features.  The model was
# trained without embedded ``feature_names`` (Booster.feature_names is None), so
# the input matrix MUST be supplied with columns in EXACTLY this order.  The
# era2..4 + outdoors/retractable/dome dummies + ``home`` indicator come from the
# same nflfastR make_model_mutations() logic that :func:`_make_cp_mutations`
# already builds — that helper is reused rather than re-deriving them.
XPASS_FEATURES: list[str] = [
    "down",
    "ydstogo",
    "yardline_100",
    "qtr",
    "wp",
    "vegas_wp",
    "era2",
    "era3",
    "era4",
    "score_differential",
    "home",
    "half_seconds_remaining",
    "posteam_timeouts_remaining",
    "defteam_timeouts_remaining",
    "outdoors",
    "retractable",
    "dome",
]

#: Number of YAC outcome classes in the multinomial xYAC model (yac = −5..70).
_XYAC_NUM_CLASSES: int = 76

# ---------------------------------------------------------------------------
# Model loading (lazy — avoids ImportError when .ubj files are absent)
# ---------------------------------------------------------------------------


#: Download-on-demand model URLs.  Models too large to bundle in the wheel
#: (the faithful 76-class ``xyac_model.ubj`` is ~34 MB) are published to the
#: ``nfl_model_artifacts`` GitHub release and fetched + cached on first use.
#: The EP/WP/CP/xpass models stay bundled under ``nfl/models/`` (each < 10 MB)
#: and are NOT listed here. ``xpass_model.ubj`` is the **faithful**
#: ``fastrmodels::xpass_model`` (the exact nflfastR ``add_xpass`` booster, 1121
#: trees) — NOT a self-trained approximation — so :func:`calculate_xpass`
#: reproduces nflverse's shipped ``xpass`` exactly.
_MODEL_URLS: dict[str, str] = {
    "xyac_model.ubj": (
        "https://github.com/sportsdataverse/sportsdataverse-data/releases/download/nfl_model_artifacts/xyac_model.ubj"
    ),
    # nfl4th 4th-down decision models (the fourth-down yards model ``fd_model.ubj``
    # is ~73 MB and the nfl4th win-probability model ``wp_model.ubj`` ~7.6 MB —
    # both too large to bundle).  Consumed by ``nfl/nfl_fourth_down.py``.
    "fd_model.ubj": (
        "https://github.com/sportsdataverse/sportsdataverse-data/releases/download/nfl_4th_down_models/fd_model.ubj"
    ),
    "wp_model.ubj": (
        "https://github.com/sportsdataverse/sportsdataverse-data/releases/download/nfl_4th_down_models/wp_model.ubj"
    ),
}


def _model_path(name: str) -> Path:
    return Path(str(_resource_files("sportsdataverse").joinpath(f"nfl/models/{name}")))


def _model_cache_dir() -> Path:
    """Directory for download-on-demand models: ``<cache_dir>/models``."""
    from sportsdataverse.nfl.config import get_config

    return get_config().cache_dir / "models"


def _load_booster_from(path: Path) -> "Booster":
    """Construct an XGBoost ``Booster`` from a model file on disk."""
    from xgboost import Booster

    b = Booster({"nthread": 4})
    b.load_model(str(path))
    return b


@lru_cache(maxsize=4)
def _load_model(name: str, models_dir: Optional[Union[str, Path]] = None) -> "Booster":
    """Load a named XGBoost Booster, with download-on-demand for large models.

    Resolution order:

    1. ``models_dir`` override — if given and ``Path(models_dir)/name`` exists,
       load it (offline / user-supplied model directory).
    2. Bundled — ``sportsdataverse/nfl/models/name`` (EP/WP/CP path; unchanged).
    3. Cache — ``<cache_dir>/models/name`` (a previously downloaded model).
    4. Download — if ``name`` is in :data:`_MODEL_URLS`, fetch the URL into the
       cache directory (written atomically via a ``.tmp`` sibling +
       :func:`os.replace`) and load it.  Any download / IO failure is wrapped
       in :class:`FileNotFoundError` so offline callers that ``except
       FileNotFoundError`` still degrade gracefully.
    5. Otherwise raise :class:`FileNotFoundError`.

    Cached per process via :func:`functools.lru_cache`, keyed on
    ``(name, models_dir)`` — a ``str`` ``models_dir`` is coerced to ``Path`` so
    the cache key is stable; ``None`` stays ``None``.
    """
    if isinstance(models_dir, str):
        models_dir = Path(models_dir)

    # 1. Explicit override directory.
    if models_dir is not None:
        override = models_dir / name
        if override.exists():
            return _load_booster_from(override)

    # 2. Bundled (EP/WP/CP).
    bundled = _model_path(name)
    if bundled.exists():
        return _load_booster_from(bundled)

    # 3. Cache (previously downloaded).
    cached = _model_cache_dir() / name
    if cached.exists():
        return _load_booster_from(cached)

    # 4. Download-on-demand.
    if name in _MODEL_URLS:
        from sportsdataverse.dl_utils import download
        from sportsdataverse.nfl.config import get_config

        cfg = get_config()
        url = _MODEL_URLS[name]
        dest = _model_cache_dir() / name
        try:
            dest.parent.mkdir(parents=True, exist_ok=True)
            if cfg.verbose:
                print(
                    f"Downloading {name} (~34 MB) from the nfl_model_artifacts release… (caching under {dest.parent})"
                )
            content = download(url, timeout=cfg.timeout, num_retries=5).content
            tmp = dest.with_suffix(dest.suffix + ".tmp")
            with open(tmp, "wb") as fh:
                fh.write(content)
            os.replace(tmp, dest)
            return _load_booster_from(dest)
        except Exception as exc:
            raise FileNotFoundError(
                f"Could not obtain '{name}' (bundled absent, cache miss, download from {url} "
                f"failed: {exc}). Callers that catch FileNotFoundError will skip this model "
                f"and continue offline; otherwise check network access or pre-place the file "
                f"under {_model_cache_dir()} or pass models_dir=."
            ) from exc

    # 5. Unknown model with no bundled/cached file and no download URL.
    raise FileNotFoundError(
        f"NFL model '{name}' not found (bundled: {bundled}, cache: {_model_cache_dir() / name}). "
        f"It is not registered for download in _MODEL_URLS. Run the track6 training pipeline to "
        f"produce the bundled model files, or pass models_dir= pointing at a directory containing it."
    )


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
    # receive_2h_ko: mirrors nflfastR helper_add_ep_wp.R — within a game, 1 when
    # the play is in the 1st half and the posteam is the team that received the
    # 2nd-half kickoff (i.e. the opening defense = first non-null defteam of the
    # game). Derived only when absent; the ESPN adapter (_espn_wp_features)
    # precomputes its own, so we never override an existing column.
    if "receive_2h_ko" not in df.columns:
        df = df.with_columns(
            pl.when(
                (pl.col("qtr") <= 2) & (pl.col("posteam") == pl.col("defteam").drop_nulls().first().over("game_id"))
            )
            .then(1)
            .otherwise(0)
            .alias("receive_2h_ko"),
        )

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
    - ``distance_to_sticks`` — air_yards - ydstogo (nflfastR sign; the models
      were trained on this orientation — see track6 features.py)
    - Era flags era2..4 (era0/era1 are intentionally excluded from CP)
    - ``home`` indicator (if not already present)
    - Roof one-hots (identical to :func:`_make_model_mutations`)
    - ``pass_middle`` — ``pass_location == "middle"`` coerced to int
    """
    df = df.with_columns(
        pl.when(pl.col("air_yards") == 0).then(1).otherwise(0).alias("air_is_zero"),
        (pl.col("air_yards") - pl.col("ydstogo")).alias("distance_to_sticks"),
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
        (pl.col(air_yards_col) - pl.col(ydstogo_col)).alias("_distance_to_sticks"),
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
    down1_col: str = "down_1",
    down2_col: str = "down_2",
    down3_col: str = "down_3",
    down4_col: str = "down_4",
    home_col: str = "start.is_home",
    qb_hit_col: str | None = None,
    pass_middle_col: str | None = None,
) -> np.ndarray:
    """Build the 19-feature XYAC matrix (nflfastR format) from ESPN play data.

    Mirrors :data:`XYAC_FEATURES` column order — the input feature vector to the
    single ``multi:softprob`` xYAC model (``num_class=76``).  Unlike the EP / WP
    / CP adapters this matrix carries NO ``cp`` / ``ep`` columns; the model is a
    pure YAC-distribution predictor.  ``distance_to_goal = yardline_100 -
    air_yards`` and ``distance_to_sticks = air_yards - ydstogo``.

    Intended for pass plays only — filter to ``air_yards`` not-null first.

    Args:
        play_df: ESPN-format pass-play DataFrame.
        air_yards_col: Air yards column.
        yardline_col: Yards to end zone.
        ydstogo_col: Yards to go.
        down1_col … down4_col: Boolean down-indicator columns.
        home_col: Boolean home-team indicator.
        qb_hit_col: QB-hit indicator column.  Defaults to 0 when absent.
        pass_middle_col: Middle-field pass column.  Defaults to 0 when absent.

    Returns:
        ``(N, 19)`` float32 ndarray in :data:`XYAC_FEATURES` column order.
    """
    df = play_df.with_columns(
        pl.when(pl.col(air_yards_col) == 0).then(1).otherwise(0).alias("_air_is_zero"),
        (pl.col(yardline_col) - pl.col(air_yards_col)).alias("_distance_to_goal"),
        (pl.col(air_yards_col) - pl.col(ydstogo_col)).alias("_distance_to_sticks"),
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
            pl.col("_distance_to_goal").alias("distance_to_goal"),
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

    prob_frame = pl.DataFrame(
        {name: pl.Series(name, probs[:, i], dtype=pl.Float64) for i, name in enumerate(_EP_CLASS_NAMES)}
    ).with_columns(ep=pl.Series("ep", ep, dtype=pl.Float64))

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
        has_spread = df["spread_line"].is_not_null().to_numpy()
        vegas_wp = np.where(has_spread, wp_spread, wp_naive)
    else:
        vegas_wp = wp_naive

    # NOTE: wp_naive is NOT gated to null-spread_line rows even though
    # vegas_wp already handles the spread/naive merge.  The `wp` column
    # intentionally exposes the naive model output for *every* row so
    # callers can compare naive vs spread-adjusted WP side-by-side
    # (mirroring nflfastR's calculate_win_probability which always
    # emits both).  Gating the predict to null-spread rows would silently
    # drop `wp` for all spread rows and break that contract.
    result = df.with_columns(
        wp=pl.Series("wp", wp_naive, dtype=pl.Float64),
        vegas_wp=pl.Series("vegas_wp", vegas_wp, dtype=pl.Float64),
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
    ``cpoe = 100 * (complete_pass - cp)`` is also added — on nflfastR's
    percentage-point scale (``add_cp`` in ``helper_add_cp_cpoe.R``).

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
        # nflfastR: cpoe = 100 * (complete_pass - cp)  (percentage points).
        result = result.with_columns((100.0 * (pl.col("complete_pass").cast(pl.Float64) - pl.col("cp"))).alias("cpoe"))
    else:
        result = result.with_columns(pl.lit(None).cast(pl.Float64).alias("cpoe"))

    if return_as_pandas:
        return result.to_pandas()
    return result


# ---------------------------------------------------------------------------
# XYAC public API
# ---------------------------------------------------------------------------

#: The five nflfastR xYAC output columns, derived from the 76-class outcome
#: distribution (NOT direct model outputs).  Order matches nflfastR's
#: ``drop.cols.xyac``.
_XYAC_OUT_COLS: tuple[str, ...] = (
    "xyac_epa",
    "xyac_mean_yardage",
    "xyac_median_yardage",
    "xyac_success",
    "xyac_fd",
)

#: The single multinomial xYAC model file.
_XYAC_MODEL_FILE: str = "xyac_model.ubj"


def _derive_xyac(
    pass_df: pl.DataFrame,
    probs: np.ndarray,
    *,
    compute_air_epa: bool = False,
) -> pl.DataFrame:
    """Derive the 5 nflfastR xYAC columns from the 76-class outcome distribution.

    Faithful polars port of the derivation in nflfastR's ``add_xyac`` (the
    ``xyac_vars`` pipeline).  Expands each play into 76 ``yac = -5..70`` outcome
    rows, truncates the tail probability mass into the field-boundary buckets,
    flips turnover-on-downs outcomes to the opponent perspective, decrements the
    half clock by 6 seconds, re-scores expected points on every outcome row via
    :func:`calculate_expected_points`, and reduces back to one row per play.

    The ``xyac_epa`` baseline is ``Σ((ep − original_ep)·prob) − air_epa``.  When
    the caller supplies ``air_epa`` (the nflverse path — it carries the column
    computed by nflfastR's ``add_air_yac_ep_variables``), that value is used
    verbatim so the output is byte-for-byte preserved.  When ``air_epa`` is
    absent (the Shield-native / ESPN path — see :func:`calculate_xyac`), pass
    ``compute_air_epa=True`` to derive it from the already-scored ``yac == 0``
    (catch-spot) outcome row: ``air_epa = ep(yac == 0) − original_ep``.  That
    row's ``ep`` carries the same TD (``yardline_100 == 0 → 7``) and
    turnover-on-downs (``ep → −ep``) handling as nflfastR's ``airEPA``, so the
    derived value is faithful to the R definition.

    Args:
        pass_df: Qualifying (``valid_pass``) plays, one row each, carrying the
            join columns (``_xyac_index``, ``distance_to_goal``, ``original_spot``
            = pre-play ``yardline_100``, ``original_ep``, ``original_ydstogo``,
            ``air_yards``, ``down``, ``ydstogo`` plus the EP inputs
            ``season``/``week``/``home_team``/``posteam``/``roof``/
            ``half_seconds_remaining``/``posteam_timeouts_remaining``/
            ``defteam_timeouts_remaining``).  Must also carry ``air_epa`` unless
            ``compute_air_epa`` is ``True``.
        probs: ``(n_plays, 76)`` float prob matrix from the multinomial model.
        compute_air_epa: When ``True``, derive ``air_epa`` from the ``yac == 0``
            outcome row instead of reading it from ``pass_df``.  The derived
            value is surfaced as an extra ``air_epa`` column on the returned
            frame so callers can fold it into the native dataset.

    Returns:
        One row per play keyed on ``_xyac_index`` with the 5 ``xyac_*`` columns
        (plus ``air_epa`` when ``compute_air_epa`` is ``True``).
    """
    n_plays = pass_df.height
    # yac outcome grid: -5..70 (76 buckets) repeated per play.
    yac_grid: np.ndarray = np.tile(np.arange(-5, 71, dtype=np.int64), n_plays)
    index_grid: np.ndarray = np.repeat(pass_df["_xyac_index"].to_numpy(), _XYAC_NUM_CLASSES)
    prob_flat: np.ndarray = probs.reshape(-1).astype(np.float64)

    long = pl.DataFrame(
        {
            "_xyac_index": index_grid,
            "yac": yac_grid,
            "prob": prob_flat,
        }
    ).join(
        pass_df.drop("prob") if "prob" in pass_df.columns else pass_df,
        on="_xyac_index",
        how="left",
    )

    # Decrement half clock by 6s for the outcome EP eval (clamped at 0).
    long = long.with_columns(
        pl.when(pl.col("half_seconds_remaining") <= 6)
        .then(pl.lit(0.0))
        .otherwise(pl.col("half_seconds_remaining") - 6.0)
        .alias("half_seconds_remaining"),
    )

    # Field-boundary truncation: absorb out-of-range tail mass into the boundary
    # buckets via the cumulative distribution (mirrors the R cumsum manipulation).
    long = long.with_columns(
        pl.when(pl.col("distance_to_goal") < 95)
        .then(pl.lit(-5))
        .otherwise(pl.col("distance_to_goal") - 99)
        .alias("max_loss"),
        pl.when(pl.col("distance_to_goal") > 70)
        .then(pl.lit(70))
        .otherwise(pl.col("distance_to_goal"))
        .alias("max_gain"),
    )
    long = long.with_columns(
        pl.col("prob").cum_sum().over("_xyac_index").alias("cum_prob"),
    )
    long = long.with_columns(
        pl.when(pl.col("yac") == pl.col("max_loss"))
        .then(pl.col("cum_prob"))
        .when(pl.col("yac") == pl.col("max_gain"))
        .then(1.0 - pl.col("cum_prob").shift(1).over("_xyac_index"))
        .otherwise(pl.col("prob"))
        .alias("prob"),
        # updated end result for each possibility
        (pl.col("distance_to_goal") - pl.col("yac")).alias("yardline_100"),
    )
    long = long.filter((pl.col("yac") >= pl.col("max_loss")) & (pl.col("yac") <= pl.col("max_gain"))).drop("cum_prob")

    # Down / distance bookkeeping + turnover-on-downs flip (opponent perspective).
    long = long.with_columns(
        pl.col("posteam_timeouts_remaining").alias("_pos_to_pre"),
        pl.col("defteam_timeouts_remaining").alias("_def_to_pre"),
        (pl.col("original_spot") - pl.col("yardline_100")).alias("gain"),
    )
    long = long.with_columns(
        pl.when((pl.col("down") == 4) & (pl.col("gain") < pl.col("ydstogo"))).then(1).otherwise(0).alias("turnover"),
    )
    long = long.with_columns(
        # down/ydstogo update for converted vs not (pre-turnover override)
        pl.when(pl.col("gain") >= pl.col("ydstogo"))
        .then(pl.lit(10))
        .otherwise(pl.col("ydstogo") - pl.col("gain"))
        .alias("ydstogo"),
    )
    long = long.with_columns(
        # save yardline before flip for the yards-gained calculation
        pl.col("yardline_100").alias("yardline_100_noflip"),
    )
    long = long.with_columns(
        # turnover overrides: ydstogo->10, flip yardline + timeouts
        pl.when(pl.col("turnover") == 1).then(pl.lit(10)).otherwise(pl.col("ydstogo")).alias("ydstogo"),
        pl.when(pl.col("turnover") == 1)
        .then(100 - pl.col("yardline_100"))
        .otherwise(pl.col("yardline_100"))
        .alias("yardline_100"),
        pl.when(pl.col("turnover") == 1)
        .then(pl.col("_def_to_pre"))
        .otherwise(pl.col("_pos_to_pre"))
        .alias("posteam_timeouts_remaining"),
        pl.when(pl.col("turnover") == 1)
        .then(pl.col("_pos_to_pre"))
        .otherwise(pl.col("_def_to_pre"))
        .alias("defteam_timeouts_remaining"),
    )
    long = long.with_columns(
        # ydstogo can't be bigger than the (post-flip) yardline
        pl.when(pl.col("ydstogo") >= pl.col("yardline_100"))
        .then(pl.col("yardline_100"))
        .otherwise(pl.col("ydstogo"))
        .alias("ydstogo"),
        # down: 1 on conversion / turnover, else +1
        pl.when((pl.col("turnover") == 1) | (pl.col("gain") >= pl.col("original_ydstogo")))
        .then(pl.lit(1))
        .otherwise(pl.col("down") + 1)
        .alias("down"),
    )

    # Re-score expected points on every outcome row (faithful EP model).
    long = calculate_expected_points(long)

    # TD / turnover EP adjustments, then probability-weighted reductions.
    long = long.with_columns(
        pl.when(pl.col("yardline_100") == 0)
        .then(pl.lit(7.0))
        .when(pl.col("turnover") == 1)
        .then(-1.0 * pl.col("ep"))
        .otherwise(pl.col("ep"))
        .alias("ep"),
    )
    long = long.with_columns(
        # epa = ep - original_ep; wt_epa = epa * prob  (nflfastR)
        (pl.col("ep") - pl.col("original_ep")).alias("_epa"),
        ((pl.col("ep") - pl.col("original_ep")) * pl.col("prob")).alias("_wt_epa"),
        (pl.col("yardline_100_noflip") * pl.col("prob")).alias("_wt_yardln"),
        ((pl.col("ep") > pl.col("original_ep")).cast(pl.Float64) * pl.col("prob")).alias("_wt_success"),
        ((pl.col("gain") >= pl.col("original_ydstogo")).cast(pl.Float64) * pl.col("prob")).alias("_wt_fd"),
    )
    if compute_air_epa:
        # Native path: air_epa = ep(yac == 0) - original_ep (catch-spot air EPA,
        # faithful to nflfastR's airEPA — the yac == 0 ep already carries the TD
        # and turnover-on-downs adjustments applied above).  Captured per play as
        # a single non-null value so .min() over the play group recovers it.
        long = long.with_columns(
            pl.when(pl.col("yac") == 0).then(pl.col("_epa")).otherwise(None).alias("air_epa"),
        )
    # median: first yac whose cumulative prob crosses 0.5
    long = long.with_columns(
        pl.col("prob").cum_sum().over("_xyac_index").alias("_cum2"),
    )
    long = long.with_columns(
        pl.when((pl.col("_cum2") > 0.5) & (pl.col("_cum2").shift(1).over("_xyac_index") < 0.5))
        .then(pl.col("yac"))
        .otherwise(0)
        .alias("_med"),
    )

    # air_epa baseline: the supplied (nflverse) column, or the yac == 0 derived
    # value when compute_air_epa is set.  ``.min()`` ignores the per-row nulls
    # introduced for the non-(yac == 0) rows in the computed case; in the supplied
    # case every row carries the same value so ``.first()``-equivalent semantics
    # hold either way.
    air_epa_expr = pl.col("air_epa").min().alias("_air_epa_base")
    agg = (
        long.group_by("_xyac_index")
        .agg(
            air_epa_expr,
            pl.col("_wt_epa").sum().alias("_sum_wt_epa"),
            ((pl.col("original_spot").first() - pl.col("air_yards").first()) - pl.col("_wt_yardln").sum()).alias(
                "xyac_mean_yardage"
            ),
            pl.col("_med").max().cast(pl.Float64).alias("xyac_median_yardage"),
            pl.col("_wt_success").sum().alias("xyac_success"),
            pl.col("_wt_fd").sum().alias("xyac_fd"),
        )
        .with_columns(
            (pl.col("_sum_wt_epa") - pl.col("_air_epa_base")).alias("xyac_epa"),
        )
    )
    if compute_air_epa:
        agg = agg.with_columns(pl.col("_air_epa_base").alias("air_epa"))
    return agg.drop("_sum_wt_epa", "_air_epa_base")


def calculate_xyac(
    pbp_data: pl.DataFrame,
    *,
    models_dir: Optional[Union[str, Path]] = None,
    return_as_pandas: bool = False,
) -> Union[pl.DataFrame, "pd.DataFrame"]:
    """Compute expected yards after catch (xYAC) for intended pass plays.

    Faithful polars port of nflfastR's ``add_xyac``.  Unlike a per-statistic
    regressor, xYAC is **one** ``multi:softprob`` model (``num_class=76``) that
    predicts a distribution over YAC buckets (``yac = -5..70``); the five output
    columns are *derived* from that distribution by re-scoring expected points on
    every outcome.  ``ep`` is **not** required on the input — it is recomputed on
    the outcome rows via :func:`calculate_expected_points`.  The play's pre-snap
    ``ep`` (``original_ep``) is the EPA baseline; ``air_epa`` is also part of the
    baseline (``xyac_epa = Σ((ep − original_ep)·prob) − air_epa``).  ``air_epa``
    is **optional**: when present (the nflverse path) it is used verbatim so
    parity is byte-for-byte preserved; when absent (the Shield-native / ESPN
    path) it is computed from the already-scored ``yac == 0`` (catch-spot)
    outcome — ``air_epa = ep(yac == 0) − original_ep`` — and, since it was
    genuinely missing, surfaced as an extra ``air_epa`` output column.

    Inference filter (nflfastR ``valid_pass`` ∧ ``distance_to_goal != 0``):
    ``complete_pass == 1`` OR ``incomplete_pass == 1`` OR ``interception == 1``,
    ``air_yards`` in ``[-15, 70)``, non-null ``receiver_player_name`` and
    ``pass_location``, and ``distance_to_goal != 0``.  Non-qualifying rows
    receive null in all five columns.  Drops and recomputes any existing xYAC
    output columns.

    The xYAC model (``xyac_model.ubj``, ~34 MB) is **not** bundled in the
    wheel: on first use it is downloaded from the ``nfl_model_artifacts``
    GitHub release and cached under ``<cache_dir>/models/`` (see
    :func:`sportsdataverse.nfl.get_config`).  Subsequent calls load it from the
    cache; ``clear_cache()`` deliberately preserves the ``models/`` subdir so a
    data-cache clear does not force a re-download.  Pass ``models_dir=`` to
    point at a local directory containing ``xyac_model.ubj`` (offline / custom
    model override).  If the model is genuinely unavailable (no cache + no
    network) the underlying loader raises :class:`FileNotFoundError`.

    Args:
        pbp_data: nflverse-format play-by-play DataFrame.  Required:
            ``air_yards``, ``season``, ``half_seconds_remaining``,
            ``yardline_100``, ``ydstogo``, ``down``, ``posteam``, ``home_team``,
            ``roof``, ``ep``, ``posteam_timeouts_remaining``,
            ``defteam_timeouts_remaining``, ``complete_pass``,
            ``incomplete_pass``, ``interception``, ``pass_location``,
            ``receiver_player_name``.  Optional: ``air_epa`` (used verbatim when
            present for byte-for-byte nflverse parity; computed from the
            ``yac == 0`` outcome and added as an output column when absent),
            ``qb_hit``.
        models_dir: Optional directory to load ``xyac_model.ubj`` from instead
            of downloading/caching it (offline use or a custom-trained model).
            When ``None`` (default) the model is resolved bundled → cache →
            downloaded-from-release.
        return_as_pandas: When ``True``, return a ``pandas.DataFrame``.

    Returns:
        DataFrame with the original columns plus the five nflfastR xYAC columns
        (``Float64``, null on non-qualifying rows):
        ``xyac_epa``, ``xyac_mean_yardage``, ``xyac_median_yardage``,
        ``xyac_success``, ``xyac_fd``.  When the input lacked ``air_epa`` and at
        least one qualifying pass was scored, a computed ``air_epa`` column
        (catch-spot air EPA) is also added.

    Example:
        Quick start::

            import polars as pl

            from sportsdataverse.nfl import load_nfl_pbp
            from sportsdataverse.nfl.ep_wp import calculate_xyac

            pbp = load_nfl_pbp([2023])
            pbp = calculate_xyac(pbp)
            print(pbp.select("xyac_epa", "xyac_mean_yardage").head())

        Pipeline next step (one line)::

            pbp.filter(pl.col("xyac_epa").is_not_null()).select("xyac_epa", "xyac_fd").head()

    See Also:
        * `nflfastR`_ -- the reference R implementation (``add_xyac``).
        * `nflreadpy`_ -- the Python NFL data loader this surface mirrors.

    .. _nflfastR: https://www.nflfastr.com
    .. _nflreadpy: https://github.com/nflverse/nflreadpy
    """
    from xgboost import DMatrix

    df = pbp_data.drop([c for c in _XYAC_OUT_COLS if c in pbp_data.columns])
    df = df.with_row_index("_xyac_index")

    # valid_pass ∧ distance_to_goal != 0
    pass_mask = (
        ((pl.col("complete_pass") == 1) | (pl.col("incomplete_pass") == 1) | (pl.col("interception") == 1))
        & pl.col("air_yards").is_not_null()
        & (pl.col("air_yards") >= -15)
        & (pl.col("air_yards") < 70)
        & pl.col("receiver_player_name").is_not_null()
        & pl.col("pass_location").is_not_null()
        & ((pl.col("yardline_100") - pl.col("air_yards")) != 0)
    )
    pass_df = df.filter(pass_mask)

    empty = pl.DataFrame(
        {"_xyac_index": pl.Series([], dtype=pl.UInt32)}
        | {col: pl.Series([], dtype=pl.Float64) for col in _XYAC_OUT_COLS}
    )

    if pass_df.height > 0:
        feats = _make_cp_mutations(pass_df).with_columns(
            (pl.col("yardline_100") - pl.col("air_yards")).alias("distance_to_goal"),
        )
        X = feats.select(XYAC_FEATURES).to_numpy(allow_copy=True).astype(np.float32)
        probs = _load_model(_XYAC_MODEL_FILE, models_dir=models_dir).predict(DMatrix(X, feature_names=XYAC_FEATURES))
        if probs.ndim == 1:
            probs = probs.reshape(-1, _XYAC_NUM_CLASSES)

        # Assemble the per-play join frame mirroring nflfastR's join_data.
        # Only the columns the EP re-score + derivation actually read are kept
        # (``calculate_expected_points`` needs season/posteam/home_team/roof/
        # half_seconds_remaining/yardline_100/down/ydstogo/timeouts).
        #
        # ``air_epa`` is an EPA *input*, not a passthrough: the nflverse path
        # carries it (computed by nflfastR's add_air_yac_ep_variables) and we use
        # it verbatim to preserve byte-for-byte parity.  The Shield-native / ESPN
        # path does NOT produce air_epa, so when it is absent we ask _derive_xyac
        # to compute it from the yac == 0 outcome (catch-spot air EPA) — this is
        # what unblocks xYAC on that path (previously a ColumnNotFoundError).
        has_air_epa = "air_epa" in pass_df.columns
        join_cols = [
            pl.col("_xyac_index"),
            pl.col("season"),
            pl.col("home_team"),
            pl.col("posteam"),
            pl.col("roof"),
            pl.col("half_seconds_remaining"),
            pl.col("posteam_timeouts_remaining"),
            pl.col("defteam_timeouts_remaining"),
            pl.col("air_yards"),
            (pl.col("yardline_100") - pl.col("air_yards")).alias("distance_to_goal"),
            pl.col("yardline_100").alias("original_spot"),
            pl.col("ep").alias("original_ep"),
            pl.col("down").cast(pl.Int64),
            pl.col("ydstogo").cast(pl.Int64),
            pl.col("ydstogo").cast(pl.Int64).alias("original_ydstogo"),
        ]
        if has_air_epa:
            join_cols.append(pl.col("air_epa"))
        join_df = pass_df.select(join_cols)

        derived = _derive_xyac(join_df, probs, compute_air_epa=not has_air_epa)
        # Surface the computed air_epa as an output column only when it was
        # genuinely absent from the input (never overwrite nflverse's column).
        out_cols = list(_XYAC_OUT_COLS)
        if not has_air_epa and "air_epa" not in df.columns:
            out_cols.append("air_epa")
        xyac_frame = derived.select("_xyac_index", *out_cols)
    else:
        xyac_frame = empty

    result = df.join(xyac_frame, on="_xyac_index", how="left").drop("_xyac_index")

    if return_as_pandas:
        return result.to_pandas()
    return result


# ---------------------------------------------------------------------------
# XPASS public API
# ---------------------------------------------------------------------------

#: The single binary:logistic expected-dropback (xpass) model file.  Published
#: to the ``nfl_model_artifacts`` GitHub release (see :data:`_MODEL_URLS`) and
#: resolved bundled → cache → downloaded by :func:`_load_model`.
_XPASS_MODEL_FILE: str = "xpass_model.ubj"


def calculate_xpass(
    pbp_data: pl.DataFrame,
    *,
    models_dir: Union[str, None] = None,
    return_as_pandas: bool = False,
) -> Union[pl.DataFrame, "pd.DataFrame"]:
    """Compute expected dropback probability (``xpass``) and ``pass_oe``.

    Faithful polars port of nflfastR's ``add_xpass`` /
    ``prepare_xpass_data`` (``helper_add_xpass.R``).  Scores a single
    ``binary:logistic`` XGBoost model (17 features, in :data:`XPASS_FEATURES`
    order) over the rows that satisfy nflfastR's ``valid_play`` filter:

    - ``season >= 2006`` (before this the NFL did not mark scrambles), and
    - ``play_type in {"no_play", "pass", "run"}``, and
    - none of ``posteam`` / ``down`` / ``defteam_timeouts_remaining`` /
      ``posteam_timeouts_remaining`` / ``yardline_100`` /
      ``score_differential`` is null.

    The era2..4 + ``outdoors`` / ``retractable`` / ``dome`` dummies and the
    ``home`` indicator are produced by :func:`_make_cp_mutations` (the same
    nflfastR ``make_model_mutations`` logic CP uses) rather than re-derived.
    ``wp`` / ``vegas_wp`` are the start-of-play win-probability columns and
    must already be present (run after the WP step / inside
    :func:`enrich_nfl_pbp`).

    The booster ships with no embedded ``feature_names``, so the DMatrix is
    built with :data:`XPASS_FEATURES` as the column order — feeding the
    features in any other order silently yields wrong predictions.

    Drops and recomputes any existing ``xpass`` / ``pass_oe`` columns.

    Args:
        pbp_data: nflverse-format play-by-play DataFrame.  Required:
            ``season``, ``play_type``, ``posteam``, ``home_team``, ``down``,
            ``ydstogo``, ``yardline_100``, ``qtr``, ``wp``, ``vegas_wp``,
            ``score_differential``, ``half_seconds_remaining``,
            ``posteam_timeouts_remaining``, ``defteam_timeouts_remaining``.
            Optional: ``roof`` (for the roof dummies), ``pass`` / ``rush``
            (the 0/1 dropback / rush indicators used by ``pass_oe``).
        models_dir: Optional directory to load ``xpass_model.ubj`` from
            instead of downloading / caching it (offline or custom model).
        return_as_pandas: When ``True``, return a ``pandas.DataFrame``.

    Returns:
        DataFrame with the original columns plus ``xpass`` (predicted pass
        probability, null outside the ``valid_play`` filter; float64) and
        ``pass_oe`` (``100 * (pass - xpass)``, null when ``xpass`` is null and
        null when ``rush == 0 & pass == 0``; float64).

    Raises:
        FileNotFoundError: when ``xpass_model.ubj`` cannot be located
            (no bundled / cached copy and no network).

    Example:
        Quick start::

            from sportsdataverse.nfl import load_nfl_pbp
            from sportsdataverse.nfl.ep_wp import enrich_nfl_pbp, calculate_xpass

            pbp = enrich_nfl_pbp(load_nfl_pbp([2023]))  # gives wp / vegas_wp
            pbp_xp = calculate_xpass(pbp)
            print(pbp_xp.select("xpass", "pass_oe").head())

        Pipeline next step::

            pbp_xp.filter(pl.col("play_type") == "pass").select("posteam", "xpass", "pass_oe").head()

        See Also:
            * `nflfastR`_ -- the R package whose ``add_xpass`` this mirrors.

        .. _nflfastR: https://www.nflfastr.com
    """
    from xgboost import DMatrix

    df = pbp_data.drop([c for c in ("xpass", "pass_oe") if c in pbp_data.columns])
    df = df.with_row_index("_xpass_index")

    valid = (
        (pl.col("season") >= 2006)
        & pl.col("play_type").is_in(["no_play", "pass", "run"])
        & pl.col("posteam").is_not_null()
        & pl.col("down").is_not_null()
        & pl.col("defteam_timeouts_remaining").is_not_null()
        & pl.col("posteam_timeouts_remaining").is_not_null()
        & pl.col("yardline_100").is_not_null()
        & pl.col("score_differential").is_not_null()
    )
    play_df = df.filter(valid)

    if len(play_df) > 0:
        # _make_cp_mutations is the shared era/roof/home/down helper, but it also
        # builds CP-only columns that read ``air_yards`` / ``ydstogo``.  xpass does
        # not use those, but the helper would raise if ``air_yards`` is absent —
        # so supply a benign placeholder when the (CP-only) column is missing.
        if "air_yards" not in play_df.columns:
            play_df = play_df.with_columns(pl.lit(0.0).alias("air_yards"))
        feats = _make_cp_mutations(play_df)
        X = feats.select(XPASS_FEATURES).to_numpy(allow_copy=True).astype(np.float32)
        preds = _load_model(_XPASS_MODEL_FILE, models_dir=models_dir).predict(DMatrix(X, feature_names=XPASS_FEATURES))
        xpass_frame = play_df.select("_xpass_index").with_columns(pl.Series("xpass", preds.tolist(), dtype=pl.Float64))
    else:
        xpass_frame = pl.DataFrame(
            {
                "_xpass_index": pl.Series([], dtype=pl.UInt32),
                "xpass": pl.Series([], dtype=pl.Float64),
            }
        )

    result = df.join(xpass_frame, on="_xpass_index", how="left").drop("_xpass_index")

    # pass_oe = 100 * (pass - xpass); null when xpass is null and null when
    # rush == 0 & pass == 0 (nflfastR add_xpass).  Be defensive: when the
    # nflverse pass / rush 0/1 indicators are absent, derive them from
    # play_type so pass_oe still resolves (and is null on neither-pass-nor-run).
    if "pass" in result.columns:
        pass_ind = pl.col("pass").cast(pl.Float64)
    elif "play_type" in result.columns:
        pass_ind = pl.when(pl.col("play_type") == "pass").then(1.0).otherwise(0.0)
    else:
        pass_ind = None

    if "rush" in result.columns:
        rush_ind = pl.col("rush").cast(pl.Float64)
    elif "play_type" in result.columns:
        rush_ind = pl.when(pl.col("play_type") == "run").then(1.0).otherwise(0.0)
    else:
        rush_ind = None

    if pass_ind is not None and rush_ind is not None:
        result = result.with_columns(
            pl.when(pl.col("xpass").is_null())
            .then(None)
            .when((rush_ind == 0) & (pass_ind == 0))
            .then(None)
            .otherwise(100.0 * (pass_ind - pl.col("xpass")))
            .cast(pl.Float64)
            .alias("pass_oe")
        )
    else:
        result = result.with_columns(pl.lit(None).cast(pl.Float64).alias("pass_oe"))

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
        For most use cases, call the high-level entry point instead.
        ``enrich_nfl_pbp`` scores EP, derives EPA, and adds WP/WPA/CP/CPOE
        in one shot on any nflverse-shape frame::

            from sportsdataverse.nfl import load_nfl_pbp
            from sportsdataverse.nfl.ep_wp import enrich_nfl_pbp

            pbp = load_nfl_pbp([2023])
            enriched = enrich_nfl_pbp(pbp)
            print(enriched.select("game_id", "ep", "epa").head())

        ``calculate_epa`` directly requires ESPN-internal columns
        (``EP_start``, ``EP_end``, ``EP_start_touchback``, ``type.text``,
        etc.) produced by ``NFLPlayProcess``.  It is called internally by
        ``NFLPlayProcess.__process_epa`` and by the ``enrich_nfl_pbp``
        orchestrator — a naked ``calculate_epa(load_nfl_pbp([2023]))``
        will raise ``KeyError`` because those columns are absent from a
        nflverse frame.

        See Also:
            * `nflfastR`_ -- R package whose EPA derivation this mirrors.
            * `nflreadpy`_ -- Python parity loader for nflverse frames.

        .. _nflfastR: https://www.nflfastr.com
        .. _nflreadpy: https://github.com/nflverse/nflreadpy
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
        For most use cases, call the high-level entry point instead.
        ``enrich_nfl_pbp`` scores WP, derives WPA, and adds EP/EPA/CP/CPOE
        in one shot on any nflverse-shape frame::

            from sportsdataverse.nfl import load_nfl_pbp
            from sportsdataverse.nfl.ep_wp import enrich_nfl_pbp

            pbp = load_nfl_pbp([2023])
            enriched = enrich_nfl_pbp(pbp)
            print(enriched.select("game_id", "wp", "def_wp", "home_wp", "away_wp", "wpa").head())

        ``calculate_wpa`` directly requires ESPN-internal columns
        (``wp_before``, ``wp_touchback``, ``wp_after``, ``homeTeamId``,
        ``start.pos_team.id``, etc.) produced by ``NFLPlayProcess``.  It is
        called internally by ``NFLPlayProcess.__process_wpa`` and by the
        ``enrich_nfl_pbp`` orchestrator — a naked
        ``calculate_wpa(load_nfl_pbp([2023]))`` will raise ``KeyError``
        because those columns are absent from a nflverse frame.

        See Also:
            * `nflfastR`_ -- R package whose WPA derivation this mirrors.
            * `nflreadpy`_ -- Python parity loader for nflverse frames.

        .. _nflfastR: https://www.nflfastr.com
        .. _nflreadpy: https://github.com/nflverse/nflreadpy
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


# ===========================================================================
# enrich_nfl_pbp — nflverse-native EP/EPA/WP/WPA/CP/CPOE/xYAC orchestrator
# ===========================================================================
#
# This is the public, nflverse-shape orchestrator (Task 4a).  Unlike
# ``calculate_epa`` / ``calculate_wpa`` (which consume ESPN-internal columns
# such as ``type.text`` / ``EP_start`` / ``change_of_pos_team``), the
# ``method="lead_diff"`` path derives EPA/WPA *natively* on nflverse columns by
# mirroring nflfastR's ``R/helper_add_ep_wp.R`` (``add_ep_variables`` +
# ``add_wp_variables``).  It must NOT call ``calculate_epa`` / ``calculate_wpa``.
#
# Parity map (R source -> Python):
#   * kickoff feature substitution      -> _apply_feature_substitution
#       (helper_add_ep_wp.R add_ep_variables L351-391: yardline_100 -> 80/75 by
#        season, down -> 1, ydstogo -> 10 for kickoffs; down-NA PAT/2pt rows get
#        the same down/ydstogo + touchback substitution)
#   * EP scoring + start-of-play ``ep``  -> calculate_expected_points (re-scored
#       on the substituted frame; ``ep`` is the start-of-play estimate)
#   * EPA lead-difference + overlays    -> _derive_epa
#       (L589-803: ep filled up per game, home_ep, home_epa = lead(home_ep) -
#        home_ep over game_id, posteam-perspective sign flip, TD/FG/PAT/2pt/
#        safety scoring overlays, end-of-half + OT overlays)
#   * WP from posteam perspective       -> calculate_win_probability
#   * WPA lead-of-home-WP + overlays    -> _derive_wpa
#       (L1074-1184: home_wp/vegas_home_wp posteam->home perspective, end-game
#        final_value, def_wp = 1 - wp, away_wp = 1 - home_wp, home_wpa =
#        lead(home_wp) - home_wp over game_id, wpa posteam-perspective sign flip,
#        kneel/end-game NA overlay)

#: Minimal columns the ``lead_diff`` derivation reads directly (beyond what the
#: scorers validate).  Used for the up-front contract check.
_ENRICH_REQUIRED_COLUMNS: tuple[str, ...] = (
    "game_id",
    "season",
    "posteam",
    "home_team",
    "yardline_100",
    "ydstogo",
    "down",
    "half_seconds_remaining",
    "posteam_timeouts_remaining",
    "defteam_timeouts_remaining",
)


def _validate_enrich_input(df: pl.DataFrame) -> None:
    """Raise a clear ``ValueError`` when required contract columns are absent."""
    from sportsdataverse.nfl.model_vars import NFLVERSE_FRAME_CONTRACT

    missing = [c for c in _ENRICH_REQUIRED_COLUMNS if c not in df.columns]
    if missing:
        raise ValueError(
            "enrich_nfl_pbp(method='lead_diff') input is missing required columns "
            f"{sorted(missing)}. Expected an nflverse-shape frame matching "
            f"NFLVERSE_FRAME_CONTRACT (got {len(df.columns)} columns). "
            f"Contract reference: {sorted(NFLVERSE_FRAME_CONTRACT)}"
        )


def _apply_feature_substitution(df: pl.DataFrame) -> pl.DataFrame:
    """Substitute EP model features for kickoffs and down-NA (PAT/2pt) plays.

    Mirrors nflfastR ``add_ep_variables`` (``helper_add_ep_wp.R`` L351-391):
    kickoff plays are scored as if receiving a touchback — ``yardline_100`` set
    to the touchback yardline (80 for ``season < 2016`` else 75), ``down`` set
    to 1, ``ydstogo`` set to 10.  Other plays with a missing ``down`` (PATs,
    two-point conversions) are likewise scored on a 1st-and-10 substitution at
    the touchback spot so the EP model never sees a NA ``down``.  Normal,
    downed plays are left untouched.

    The substitution is written to *copies* of the feature columns so the
    returned frame still carries the original raw values for the EPA overlays.
    """
    touchback = (
        pl.when(pl.col("season") < 2016)
        .then(pl.lit(TOUCHBACK_YARDLINE_PRE_2016))
        .otherwise(pl.lit(TOUCHBACK_YARDLINE_POST_2016))
    )

    is_kickoff = pl.lit(False)
    if "kickoff_attempt" in df.columns:
        is_kickoff = is_kickoff | (pl.col("kickoff_attempt") == 1)
    if "play_type" in df.columns:
        is_kickoff = is_kickoff | (pl.col("play_type") == "kickoff")

    needs_sub = is_kickoff | pl.col("down").is_null()

    return df.with_columns(
        pl.when(needs_sub)
        .then(touchback.cast(pl.Int64))
        .otherwise(pl.col("yardline_100").cast(pl.Int64))
        .alias("yardline_100"),
        pl.when(needs_sub).then(pl.lit(1)).otherwise(pl.col("down")).cast(pl.Int64).alias("down"),
        pl.when(needs_sub).then(pl.lit(10)).otherwise(pl.col("ydstogo").cast(pl.Int64)).alias("ydstogo"),
    )


def _is_real_play_expr(df: pl.DataFrame) -> pl.Expr:
    """Boolean expr: row is a real, model-scorable football play.

    nflfastR scores EP/WP only on real plays; marker / timeout rows (``END
    QUARTER``, ``END GAME``, ``Timeout ...``, suspension notices) carry no game
    situation and get NA model output, later inherited via
    ``tidyr::fill(.direction = "up")``.  Operationally a real play has a
    non-null ``down`` OR is a kickoff / extra-point / two-point attempt (those
    legitimately carry a null ``down`` but are still scored).
    """
    expr = pl.col("down").is_not_null()
    for flag in ("kickoff_attempt", "extra_point_attempt", "two_point_attempt"):
        if flag in df.columns:
            expr = expr | (pl.col(flag) == 1)
    return expr


def _fill_up_nonplay(df: pl.DataFrame, cols: tuple[str, ...]) -> pl.DataFrame:
    """NA the given model-output ``cols`` on non-play rows, then fill UP per game.

    Mirrors nflfastR's ``tidyr::fill(<col>, .direction = "up")`` applied after
    the marker / timeout rows received a NA model score.  Without this the
    streaming pipeline scores those rows through the EP/WP model and produces a
    garbage value (e.g. ``wp`` ~ 0.002 on a timeout), which then poisons the
    ``lead(home_ep)`` / ``lead(home_wp)`` of the *preceding* real play and
    wrecks ``epa`` / ``wpa`` parity.
    """
    is_play = _is_real_play_expr(df)
    present = [c for c in cols if c in df.columns]
    if not present:
        return df
    df = df.with_columns([pl.when(is_play).then(pl.col(c)).otherwise(None).alias(c) for c in present])
    return df.with_columns([pl.col(c).fill_null(strategy="backward").over("game_id") for c in present])


def _derive_epa(df: pl.DataFrame) -> pl.DataFrame:
    """Derive ``epa`` from start-of-play ``ep`` natively, per nflfastR.

    Mirrors ``add_ep_variables`` EPA block (``helper_add_ep_wp.R`` L589-803):

    * ``ep`` is filled *up* within each game so the last play inherits the
      following play's start-of-play EP (the lead).
    * ``home_ep`` flips ``ep`` to the home-team frame using the (filled-up)
      possession team; ``home_epa = lead(home_ep) - home_ep`` over ``game_id``;
      ``epa`` flips back to the possession-team frame.
    * Scoring overlays replace the lead difference with the realised value:
      touchdown (``7 - ep`` / ``-7 - ep``), made FG (``3 - ep``), made PAT
      (``1 - ep``), good two-point (``2 - ep``), failed PAT (``0 - ep``),
      safety (``±2 - ep``).
    * End-of-half / end-of-game / OT-last-play overlays set ``epa = 0 - ep`` for
      non-scoring plays and NA the ``ep`` / ``epa`` of terminal rows.

    Scoring overlays key on the **nflverse** result columns
    (``field_goal_result == "made"`` / ``extra_point_result == "good"`` /
    ``two_point_conv_result == "success"`` / ``safety == 1``), not the
    ESPN-internal ``field_goal_made`` / ``extra_point_good`` /
    ``two_point_*_good`` / ``safety_team`` flags (which are absent from a
    nflverse frame, leaving the overlays inert and the realised-score EPA
    silently wrong).

    All leads are grouped by ``game_id`` so no value leaks across games.
    """
    grp = "game_id"

    # NA the model-scored ``ep`` on non-play (marker / timeout) rows and fill UP
    # per game (R: tidyr::fill .direction="up") so a real play's lead never
    # reads a garbage marker-row EP.
    df = _fill_up_nonplay(df, ("ep",))

    df = df.with_columns(pl.col("posteam").alias("_tmp_posteam"))
    # fill ep + tmp_posteam UP within each game (R: tidyr::fill .direction="up")
    df = df.with_columns(
        pl.col("ep").fill_null(strategy="backward").over(grp),
        pl.col("_tmp_posteam").fill_null(strategy="backward").over(grp),
    )

    df = df.with_columns(
        pl.when(pl.col("_tmp_posteam") == pl.col("home_team"))
        .then(pl.col("ep"))
        .otherwise(-pl.col("ep"))
        .alias("_home_ep")
    )
    df = df.with_columns((pl.col("_home_ep").shift(-1).over(grp) - pl.col("_home_ep")).alias("_home_epa"))
    df = df.with_columns(
        pl.when(pl.col("_tmp_posteam") == pl.col("home_team"))
        .then(pl.col("_home_epa"))
        .otherwise(-pl.col("_home_epa"))
        .alias("epa")
    )

    def _has(col: str) -> bool:
        return col in df.columns

    # --- scoring overlays (each conditional on the realised scoring result) ---
    # Touchdown
    if _has("td_team"):
        df = df.with_columns(
            pl.when(pl.col("td_team").is_not_null() & (pl.col("td_team") == pl.col("posteam")))
            .then(7.0 - pl.col("ep"))
            .when(pl.col("td_team").is_not_null() & (pl.col("td_team") != pl.col("posteam")))
            .then(-7.0 - pl.col("ep"))
            .otherwise(pl.col("epa"))
            .alias("epa")
        )

    # nflverse-name realised-result predicates (the ESPN-internal *_made /
    # *_good / safety_team flags are absent from a nflverse frame).  Use
    # ``eq_missing`` / ``fill_null`` so a NULL result column yields ``False``
    # (not Kleene-NULL) — a NULL would otherwise poison the conjunction and
    # silently drop the overlay.
    _no_td = pl.col("td_team").is_null() if _has("td_team") else pl.lit(True)
    _fg_made = pl.col("field_goal_result").eq_missing("made") if _has("field_goal_result") else pl.lit(False)
    _xp_good = pl.col("extra_point_result").eq_missing("good") if _has("extra_point_result") else pl.lit(False)
    _two_good = (
        pl.col("two_point_conv_result").eq_missing("success") if _has("two_point_conv_result") else pl.lit(False)
    )
    _xp_failed = (
        pl.col("extra_point_result").is_in(["failed", "blocked", "aborted"]).fill_null(False)
        if _has("extra_point_result")
        else pl.lit(False)
    )
    _two_failed = (
        pl.col("two_point_conv_result").eq_missing("failure") if _has("two_point_conv_result") else pl.lit(False)
    )

    # Offense field goal (made) -> 3 - ep
    if _has("field_goal_result"):
        df = df.with_columns(pl.when(_no_td & _fg_made).then(3.0 - pl.col("ep")).otherwise(pl.col("epa")).alias("epa"))

    # Offense extra point (good) -> 1 - ep
    if _has("extra_point_result"):
        df = df.with_columns(
            pl.when(_no_td & ~_fg_made & _xp_good).then(1.0 - pl.col("ep")).otherwise(pl.col("epa")).alias("epa")
        )

    # Offense two-point conversion (success) -> 2 - ep
    if _has("two_point_conv_result"):
        df = df.with_columns(
            pl.when(_no_td & ~_fg_made & ~_xp_good & _two_good)
            .then(2.0 - pl.col("ep"))
            .otherwise(pl.col("epa"))
            .alias("epa")
        )

    # Failed PAT / failed two-point -> 0 - ep
    if _has("extra_point_result") or _has("two_point_conv_result"):
        df = df.with_columns(
            pl.when(_no_td & ~_fg_made & ~_xp_good & ~_two_good & (_xp_failed | _two_failed))
            .then(0.0 - pl.col("ep"))
            .otherwise(pl.col("epa"))
            .alias("epa")
        )

    # Defensive two-point conversion (opponent scores) -> -2 - ep
    if _has("defensive_two_point_conv"):
        df = df.with_columns(
            pl.when(pl.col("defensive_two_point_conv") == 1)
            .then(-2.0 - pl.col("ep"))
            .otherwise(pl.col("epa"))
            .alias("epa")
        )

    # Safety (nflverse: safety == 1; the possession team conceded, so the
    # defense scores -> posteam-frame epa = -2 - ep). nflfastR's companion
    # `safety_team == posteam -> +2 - ep` branch is unreachable: safety_team is
    # derived as the scoring (defending) team, never the posteam, so it is
    # intentionally omitted here.
    if _has("safety"):
        df = df.with_columns(
            pl.when(pl.col("safety") == 1).then(-2.0 - pl.col("ep")).otherwise(pl.col("epa")).alias("epa")
        )

    # --- end-of-half / end-of-game / OT overlays ---
    if _has("desc"):
        df = df.with_columns(
            pl.when(pl.col("desc").str.to_lowercase().str.contains(r"(?:end of game)|(?:end game)"))
            .then(pl.lit(1))
            .otherwise(pl.lit(0))
            .alias("_end_game")
        )
    else:
        df = df.with_columns(pl.lit(0).alias("_end_game"))

    if _has("qtr") and _has("sp"):
        next_qtr = pl.col("qtr").shift(-1).over(grp)
        next_desc = pl.col("desc").shift(-1).over(grp) if _has("desc") else pl.lit(None, dtype=pl.Utf8)
        next_end_game = pl.col("_end_game").shift(-1).over(grp)
        play_type_ok = pl.col("play_type").is_not_null() if _has("play_type") else pl.lit(True)

        end_half = (
            (
                (pl.col("qtr") == 2) & ((next_qtr == 3) | (next_desc == pl.lit("END QUARTER 2")))
                | (pl.col("qtr") == 4)
                & ((next_qtr == 5) | (next_desc == pl.lit("END QUARTER 4")) | (next_end_game == 1))
            )
            & (pl.col("sp") == 0)
            & play_type_ok
        )
        df = df.with_columns(pl.when(end_half).then(0.0 - pl.col("ep")).otherwise(pl.col("epa")).alias("epa"))
        # last play of OT
        ot_last = (pl.col("qtr") > 4) & (next_end_game == 1) & (pl.col("sp") == 0)
        df = df.with_columns(pl.when(ot_last).then(0.0 - pl.col("ep")).otherwise(pl.col("epa")).alias("epa"))

    if _has("desc"):
        df = df.with_columns(
            pl.when(pl.col("desc") == pl.lit("END QUARTER 2")).then(None).otherwise(pl.col("epa")).alias("epa"),
        )
    df = df.with_columns(
        pl.when(pl.col("_end_game") == 1).then(None).otherwise(pl.col("epa")).alias("epa"),
        pl.when(pl.col("_end_game") == 1).then(None).otherwise(pl.col("ep")).alias("ep"),
    )
    if _has("desc"):
        df = df.with_columns(
            pl.when(pl.col("desc") == pl.lit("END QUARTER 2")).then(None).otherwise(pl.col("ep")).alias("ep"),
        )

    return df.drop([c for c in ("_tmp_posteam", "_home_ep", "_home_epa", "_end_game") if c in df.columns])


def _derive_qb_epa(df: pl.DataFrame) -> pl.DataFrame:
    """Add ``qb_epa`` — EPA crediting the QB on completed-pass-then-fumble plays.

    Mirrors nflfastR's ``add_qb_epa`` (``helper_additional_functions.R``).  On
    every play ``qb_epa == epa`` EXCEPT plays where the receiver caught the ball
    and *then* lost a fumble (``complete_pass == 1 & fumble_lost == 1``).  Those
    plays are RE-SPOTTED as if the receiver had simply been tackled at the
    fumble spot (no turnover): the post-completion game state is recomputed
    (``yardline_100 -= yards_gained`` for the air+YAC gain, ``down`` / ``ydstogo``
    incl. first-down resets, turnover-on-downs → possession change with field /
    timeout flip, goal-line ``ydstogo`` clamp), EP is re-scored with the existing
    :func:`calculate_expected_points`, and ``qb_epa = ep_respotted - ep_before``
    (negated when the re-spot is a turnover on downs).  The QB thereby gets
    credit for the completion + YAC and is NOT penalised for the fumble turnover.

    The function is a no-op-shaped addition: on every play *not* matching the
    fumble condition (or when the required columns are absent), ``qb_epa == epa``
    exactly.  ``qb_epa`` is float64 and null-safe (null wherever ``epa`` is null).

    Args:
        df: Enriched frame carrying start-of-play ``ep`` and possession-team
            ``epa`` (i.e. post-:func:`_derive_epa`), plus the nflverse columns
            ``complete_pass``, ``fumble_lost``, ``yards_gained``, ``down``,
            ``ydstogo``, ``yardline_100``, ``half_seconds_remaining``,
            ``posteam_timeouts_remaining``, ``defteam_timeouts_remaining``,
            ``season``, ``posteam``, ``home_team`` (``roof`` optional).

    Returns:
        ``df`` with a ``qb_epa`` column added (float64).
    """
    import sportsdataverse.nfl.ep_wp as _self

    df = df.drop([c for c in ("qb_epa",) if c in df.columns])

    required = ("complete_pass", "fumble_lost", "yards_gained", "ep", "epa", "down", "posteam", "defteam")
    if any(c not in df.columns for c in required):
        # Required inputs absent — qb_epa is exactly epa (faithful no-op fallback).
        return df.with_columns(pl.col("epa").cast(pl.Float64).alias("qb_epa"))

    # Stable join key independent of play_id uniqueness.
    df = df.with_row_index("_qbepa_idx")

    fumbles = df.filter(
        (pl.col("complete_pass") == 1)
        & (pl.col("fumble_lost") == 1)
        & pl.col("epa").is_not_null()
        & pl.col("down").is_not_null()
    )

    if fumbles.height == 0:
        return df.drop("_qbepa_idx").with_columns(pl.col("epa").cast(pl.Float64).alias("qb_epa"))

    # Re-spot the play as if the receiver were tackled at the fumble spot.
    respotted = (
        fumbles.with_columns(
            # The play consumed ~6 seconds before the fumble was recovered.
            pl.when(pl.col("half_seconds_remaining") <= 6)
            .then(pl.lit(0.0))
            .otherwise(pl.col("half_seconds_remaining").cast(pl.Float64) - 6.0)
            .alias("half_seconds_remaining"),
            pl.col("down").cast(pl.Float64).alias("down"),
            pl.col("posteam_timeouts_remaining").alias("_pos_to_pre"),
            pl.col("defteam_timeouts_remaining").alias("_def_to_pre"),
            pl.col("posteam").alias("_posteam_pre"),
            pl.col("defteam").alias("_defteam_pre"),
            pl.col("ep").alias("_ep_old"),
        )
        # New yard line from the play result.
        .with_columns((pl.col("yardline_100") - pl.col("yards_gained")).alias("yardline_100"))
        # New down: 1st down if the gain made the sticks, else down + 1.
        .with_columns(
            pl.when(pl.col("yards_gained") >= pl.col("ydstogo"))
            .then(pl.lit(1.0))
            .otherwise(pl.col("down") + 1.0)
            .alias("down")
        )
        # down == 5 → turnover on downs at the fumble spot → possession change.
        .with_columns(
            pl.when(pl.col("down") == 5).then(pl.lit(1)).otherwise(pl.lit(0)).alias("_change"),
        )
        .with_columns(
            pl.when(pl.col("down") == 5).then(pl.lit(1.0)).otherwise(pl.col("down")).alias("down"),
        )
        # ydstogo: 10 on a fresh first down, else what's left after the gain.
        .with_columns(
            pl.when(pl.col("down") == 1)
            .then(pl.lit(10.0))
            .otherwise(pl.col("ydstogo") - pl.col("yards_gained"))
            .alias("ydstogo"),
        )
        # Possession change → 10 yards to go, flip field + timeouts.
        .with_columns(
            pl.when(pl.col("_change") == 1).then(pl.lit(10.0)).otherwise(pl.col("ydstogo")).alias("ydstogo"),
            pl.when(pl.col("_change") == 1)
            .then(100 - pl.col("yardline_100"))
            .otherwise(pl.col("yardline_100"))
            .alias("yardline_100"),
            pl.when(pl.col("_change") == 1)
            .then(pl.col("_def_to_pre"))
            .otherwise(pl.col("_pos_to_pre"))
            .alias("posteam_timeouts_remaining"),
            pl.when(pl.col("_change") == 1)
            .then(pl.col("_pos_to_pre"))
            .otherwise(pl.col("_def_to_pre"))
            .alias("defteam_timeouts_remaining"),
            # Flip possession too: calculate_expected_points derives features from
            # posteam/home_team, so a turnover-on-downs re-spot must score from the
            # NEW possessing team's perspective (then -ep negates back below).
            pl.when(pl.col("_change") == 1)
            .then(pl.col("_defteam_pre"))
            .otherwise(pl.col("_posteam_pre"))
            .alias("posteam"),
            pl.when(pl.col("_change") == 1)
            .then(pl.col("_posteam_pre"))
            .otherwise(pl.col("_defteam_pre"))
            .alias("defteam"),
        )
        # Goal-line clamp: can't have more yards to go than yards to the end zone.
        .with_columns(
            pl.when(pl.col("yardline_100") < pl.col("ydstogo"))
            .then(pl.col("yardline_100"))
            .otherwise(pl.col("ydstogo"))
            .alias("ydstogo"),
        )
        .with_columns(pl.col("down").cast(pl.Int64).alias("down"))
    )

    # Re-score EP on the re-spotted state with the existing scorer.
    scored = _self.calculate_expected_points(
        respotted.drop([c for c in ("ep", *_EP_CLASS_NAMES) if c in respotted.columns])
    )

    fixed = scored.select(
        pl.col("_qbepa_idx"),
        (pl.when(pl.col("_change") == 1).then(-pl.col("ep")).otherwise(pl.col("ep")) - pl.col("_ep_old")).alias(
            "_fixed_epa"
        ),
    )

    df = df.join(fixed, on="_qbepa_idx", how="left").with_columns(
        pl.coalesce(pl.col("_fixed_epa"), pl.col("epa")).cast(pl.Float64).alias("qb_epa")
    )

    return df.drop([c for c in ("_qbepa_idx", "_fixed_epa") if c in df.columns])


def _derive_wpa(df: pl.DataFrame) -> pl.DataFrame:
    """Derive home/away/def WP and ``wpa`` natively, per nflfastR.

    Mirrors ``add_wp_variables`` tail (``helper_add_ep_wp.R`` L1074-1184):

    * ``wp`` / ``vegas_wp`` filled *up* within each game; the possession team is
      likewise filled up to resolve the NA-posteam terminal rows.
    * ``home_wp`` / ``vegas_home_wp`` flip the possession-team WP into the home
      frame (``wp`` if ``posteam == home_team`` else ``1 - wp``).
    * On the end-of-game row both are set to ``final_value`` (1 / 0 / 0.5 by
      final score); ``away_wp = 1 - home_wp``; ``def_wp = 1 - wp``.
    * ``home_wpa = lead(home_wp) - home_wp`` over ``game_id``; ``wpa`` flips into
      the possession-team frame (``home_wpa`` if ``posteam == home_team`` else
      ``-home_wpa``).  Kneels and end-of-game rows get NA ``wpa`` / ``vegas_wpa``.

    All leads are grouped by ``game_id`` so no value leaks across games.
    """
    grp = "game_id"

    # NA the model-scored ``wp`` / ``vegas_wp`` on non-play (marker / timeout)
    # rows and fill UP per game (R: tidyr::fill .direction="up") so a real
    # play's lead never reads a garbage marker-row WP.
    df = _fill_up_nonplay(df, ("wp", "vegas_wp"))

    df = df.with_columns(pl.col("posteam").alias("_tmp_posteam"))
    df = df.with_columns(
        pl.col("wp").fill_null(strategy="backward").over(grp),
        pl.col("vegas_wp").fill_null(strategy="backward").over(grp),
        pl.col("_tmp_posteam").fill_null(strategy="backward").over(grp),
    )

    df = df.with_columns(
        pl.when(pl.col("_tmp_posteam") == pl.col("home_team"))
        .then(pl.col("wp"))
        .otherwise(1.0 - pl.col("wp"))
        .alias("home_wp"),
        pl.when(pl.col("_tmp_posteam") == pl.col("home_team"))
        .then(pl.col("vegas_wp"))
        .otherwise(1.0 - pl.col("vegas_wp"))
        .alias("vegas_home_wp"),
    )

    if "desc" in df.columns:
        df = df.with_columns(
            pl.when(pl.col("desc").str.to_lowercase().str.contains(r"(?:end of game)|(?:end game)"))
            .then(pl.lit(1))
            .otherwise(pl.lit(0))
            .alias("_end_game")
        )
    else:
        df = df.with_columns(pl.lit(0).alias("_end_game"))

    if "home_score" in df.columns and "away_score" in df.columns:
        final_value = (
            pl.when(pl.col("home_score") > pl.col("away_score"))
            .then(1.0)
            .when(pl.col("away_score") > pl.col("home_score"))
            .then(0.0)
            .otherwise(0.5)
        )
        df = df.with_columns(
            pl.when(pl.col("_end_game") == 1).then(final_value).otherwise(pl.col("home_wp")).alias("home_wp"),
            pl.when(pl.col("_end_game") == 1)
            .then(final_value)
            .otherwise(pl.col("vegas_home_wp"))
            .alias("vegas_home_wp"),
        )

    df = df.with_columns(
        (1.0 - pl.col("home_wp")).alias("away_wp"),
        pl.when(pl.col("_end_game") == 1).then(None).otherwise(pl.col("vegas_wp")).alias("vegas_wp"),
        pl.when(pl.col("_end_game") == 1).then(None).otherwise(pl.col("wp")).alias("wp"),
    )
    df = df.with_columns((1.0 - pl.col("wp")).alias("def_wp"))

    # WPA: lead of home WP, flipped into possession-team frame.
    df = df.with_columns(
        (pl.col("vegas_home_wp").shift(-1).over(grp) - pl.col("vegas_home_wp")).alias("_vegas_home_wpa"),
        (pl.col("home_wp").shift(-1).over(grp) - pl.col("home_wp")).alias("_home_wpa"),
    )
    df = df.with_columns(
        pl.when(pl.col("_tmp_posteam") == pl.col("home_team"))
        .then(pl.col("_vegas_home_wpa"))
        .otherwise(-pl.col("_vegas_home_wpa"))
        .alias("vegas_wpa"),
        pl.when(pl.col("_tmp_posteam") == pl.col("home_team"))
        .then(pl.col("_home_wpa"))
        .otherwise(-pl.col("_home_wpa"))
        .alias("wpa"),
    )

    if "desc" in df.columns:
        kneel_or_end = pl.col("desc").str.to_lowercase().str.contains(r"(?:\skneels\s)|(?:end of game)|(?:end game)")
        df = df.with_columns(
            pl.when(kneel_or_end).then(None).otherwise(pl.col("vegas_wpa")).alias("vegas_wpa"),
            pl.when(kneel_or_end).then(None).otherwise(pl.col("wpa")).alias("wpa"),
        )

    return df.drop(
        [c for c in ("_tmp_posteam", "_end_game", "_vegas_home_wpa", "_home_wpa", "vegas_home_wp") if c in df.columns]
    )


def enrich_nfl_pbp(
    df: pl.DataFrame,
    *,
    method: str = "lead_diff",
    models_dir: Union[str, None] = None,
    return_as_pandas: bool = False,
) -> Union[pl.DataFrame, "pd.DataFrame"]:
    """Enrich an nflverse-shape PBP frame with EP/EPA/WP/WPA/CP/CPOE/xYAC.

    ``method="lead_diff"`` (the default and only implemented method) is a
    NFLVERSE-NATIVE derivation that mirrors nflfastR's ``helper_add_ep_wp.R``.
    It runs EP -> EPA -> WP -> WPA -> CP/CPOE -> xYAC, deriving ``epa`` / ``wpa``
    with grouped lead-differences (over ``game_id``) and the nflfastR scoring /
    end-of-half / OT overlays — it does **not** call ``calculate_epa`` /
    ``calculate_wpa`` (those consume ESPN-internal columns absent from a
    nflverse frame).  Kickoffs and down-NA plays (PATs, two-point conversions)
    have their EP model features substituted (touchback ``yardline_100``,
    ``down=1``, ``ydstogo=10``) before EP scoring, matching nflfastR.

    The orchestrator is idempotent: stale ``ep`` / ``epa`` / ``wp`` / ``wpa`` /
    ``cp`` / ``cpoe`` / xYAC columns are recomputed on each call.

    Args:
        df: Play-by-play DataFrame in nflverse shape.  Must satisfy the
            ``NFLVERSE_FRAME_CONTRACT`` minimum (``game_id``, ``season``,
            ``posteam``, ``home_team``, ``yardline_100``, ``ydstogo``,
            ``down``, ``half_seconds_remaining``, the timeout columns, plus the
            WP inputs ``score_differential`` / ``game_seconds_remaining`` /
            ``spread_line`` / ``receive_2h_ko``).
        method: ``"lead_diff"`` (default) — the native, nflfastR-faithful
            EPA/WPA derivation and the canonical parity path. It is the only
            supported method; any other value raises ``ValueError``. (The param
            is retained for forward extensibility / API stability.)
        models_dir: Optional directory to load the xYAC model
            (``xyac_model.ubj``) from instead of downloading/caching it —
            for offline use or a custom-trained model.  When ``None``
            (default) the bundled EP/WP/CP models load from
            ``sportsdataverse/nfl/models/`` and the large xYAC model is
            resolved bundled → cache → downloaded-from-release.
        return_as_pandas: When ``True``, return a ``pandas.DataFrame``.

    Returns:
        polars.DataFrame (or pandas.DataFrame when *return_as_pandas* is
        ``True``) — the input frame with the following columns added or
        recomputed:

        * ``ep`` — start-of-play expected points (float64, clipped to [-10, 10]).
        * ``epa`` — expected points added (float64; null on terminal/timeout rows).
        * ``qb_epa`` — EPA crediting the QB (float64).  Equals ``epa`` on every
          play except completed passes the receiver then loses to a fumble
          (``complete_pass == 1 & fumble_lost == 1``), which are re-spotted at the
          fumble spot (no turnover) and re-scored so the QB keeps completion + YAC
          credit and is not penalised for the turnover (nflfastR ``add_qb_epa``).
        * ``wp`` — naive win probability from the ``wp_naive`` model (float64).
        * ``vegas_wp`` — spread-adjusted win probability from ``wp_spread``
          (falls back to ``wp`` when ``spread_line`` is null).
        * ``def_wp`` — defensive team win probability (``1 - wp``).
        * ``home_wp`` — home-team win probability (possession-team frame flip).
        * ``away_wp`` — away-team win probability (``1 - home_wp``).
        * ``wpa`` — win probability added (float64; null on kneel/terminal rows).
        * ``vegas_wpa`` — spread-adjusted WPA.
        * ``cp`` — completion probability for intended pass plays (null
          otherwise; float64).
        * ``cpoe`` — completion probability over expected, percentage-point
          scale (null when ``complete_pass`` absent; float64).
        * ``xpass`` — expected dropback (pass) probability from the
          ``binary:logistic`` xpass model (faithful to nflfastR's ``add_xpass``;
          null outside the ``valid_play`` filter — ``season < 2006`` or any
          required input null; float64).  ``xpass_model.ubj`` is **not** bundled
          — on first use it is downloaded from the ``nfl_model_artifacts``
          GitHub release and cached under ``<cache_dir>/models/``.  If the model
          is unavailable offline the xpass step is skipped with a
          ``RuntimeWarning`` and ``xpass`` / ``pass_oe`` stay null.
        * ``pass_oe`` — dropback percent over expected, ``100 * (pass - xpass)``
          (null when ``xpass`` is null and when ``rush == 0 & pass == 0``;
          float64).
        * ``xyac_epa``, ``xyac_mean_yardage``, ``xyac_median_yardage``,
          ``xyac_success``, ``xyac_fd`` — expected yards after catch, derived
          from the single multinomial xYAC model (faithful to nflfastR's
          ``add_xyac``; null on non-qualifying plays).  ``xyac_model.ubj``
          (~34 MB) is **not** bundled in the wheel — on first use it is
          downloaded from the ``nfl_model_artifacts`` GitHub release and cached
          under ``<cache_dir>/models/`` (preserved across ``clear_cache()``).
          If the model is unavailable offline (no cache + no network) the xYAC
          step is skipped with a ``RuntimeWarning`` and the five columns stay
          null while the rest of the enrichment proceeds.

    Raises:
        ValueError: when ``method`` is not ``"lead_diff"``, or required
            contract columns are absent from ``df``.

    Example:
        Quick start::

            from sportsdataverse.nfl import load_nfl_pbp
            from sportsdataverse.nfl.ep_wp import enrich_nfl_pbp

            pbp = load_nfl_pbp([2023])
            enriched = enrich_nfl_pbp(pbp)
            print(enriched.select("ep", "epa", "wp", "wpa").head())

        Pandas output::

            from sportsdataverse.nfl import load_nfl_pbp
            from sportsdataverse.nfl.ep_wp import enrich_nfl_pbp

            enriched_pd = enrich_nfl_pbp(load_nfl_pbp([2023]), return_as_pandas=True)
            print(enriched_pd[["ep", "epa", "wp", "wpa"]].head())

        Pipeline next step::

            enriched.filter(pl.col("play_type") == "pass").select("posteam", "epa", "cp", "cpoe").head()

        See Also:
            * `nflfastR`_ -- the R package whose ``helper_add_ep_wp.R`` this
              method mirrors.
            * `nflreadpy`_ -- Python parity wrapper for nflverse loaders.

        .. _nflfastR: https://www.nflfastr.com
        .. _nflreadpy: https://github.com/nflverse/nflreadpy
    """
    if method != "lead_diff":
        raise ValueError(
            f"enrich_nfl_pbp: unknown method {method!r}; expected 'lead_diff' (the only supported method)."
        )

    _validate_enrich_input(df)

    # Resolve the scorers off the module so monkeypatched stubs in tests and any
    # future indirection are honoured.
    import sportsdataverse.nfl.ep_wp as _self

    # 1. EP — score on the feature-substituted frame (kickoff / down-NA), then
    #    keep ``ep`` as the start-of-play estimate.  We score a substituted COPY
    #    so the original raw feature columns survive for the EPA overlays.
    substituted = _apply_feature_substitution(df)
    scored = _self.calculate_expected_points(substituted)
    # Carry the freshly-scored ``ep`` (+ class probs) back onto the ORIGINAL
    # (un-substituted) frame so downstream overlays read the real columns.
    ep_cols = ["ep", *_EP_CLASS_NAMES]
    raw = df.drop([c for c in ep_cols if c in df.columns])
    raw = pl.concat([raw, scored.select([c for c in ep_cols if c in scored.columns])], how="horizontal")

    # 2. EPA — native lead-difference + scoring/end-of-half/OT overlays.
    raw = _derive_epa(raw)

    # 2b. qb_epa — equals epa everywhere except completed-pass-then-fumble plays,
    #     which are re-spotted at the fumble spot (no turnover) and re-scored so
    #     the QB keeps completion + YAC credit (nflfastR add_qb_epa).
    raw = _derive_qb_epa(raw)

    # 3. WP — naive + spread-adjusted.
    raw = raw.drop([c for c in ("wp", "vegas_wp") if c in raw.columns])
    raw = _self.calculate_win_probability(raw)

    # 4. WPA + home/away/def WP — native lead-of-home-WP + posteam perspective.
    raw = raw.drop([c for c in ("home_wp", "away_wp", "def_wp", "wpa", "vegas_wpa") if c in raw.columns])
    raw = _derive_wpa(raw)

    # 5. CP / CPOE.
    raw = _self.calculate_completion_probability(raw)

    # 5b. xpass / pass_oe — faithful nflfastR add_xpass (single binary:logistic
    # xpass_model.ubj).  Needs wp / vegas_wp (present after the WP step).  When
    # the model is genuinely unavailable offline we skip gracefully with a
    # RuntimeWarning and emit null xpass / pass_oe so the column set stays stable.
    try:
        raw = _self.calculate_xpass(raw, models_dir=models_dir)
    except FileNotFoundError as exc:
        import warnings

        warnings.warn(
            f"enrich_nfl_pbp: skipping xpass step — {type(exc).__name__}: {exc}.",
            RuntimeWarning,
            stacklevel=2,
        )
        raw = raw.with_columns(
            [pl.lit(None, dtype=pl.Float64).alias(c) for c in ("xpass", "pass_oe") if c not in raw.columns]
        )

    # 6. xYAC — faithful nflfastR add_xyac (single multinomial xyac_model.ubj +
    # the EP-rescored derivation). When the model file is present this populates
    # the five xyac_* columns; if it is genuinely missing we skip gracefully with
    # a RuntimeWarning rather than failing the whole enrichment.
    try:
        raw = _self.calculate_xyac(raw, models_dir=models_dir)
    except (FileNotFoundError, pl.exceptions.ColumnNotFoundError) as exc:
        import warnings

        # FileNotFoundError → xyac_model.ubj genuinely unavailable (offline,
        # no cache).  ColumnNotFoundError is a defensive catch: calculate_xyac
        # now derives air_epa from the yac == 0 outcome when it is absent, so a
        # missing-column failure should no longer fire on the Shield-native
        # path — but if some other required input column is genuinely missing
        # we degrade gracefully rather than failing the whole enrichment.
        warnings.warn(
            f"enrich_nfl_pbp: skipping xYAC step — {type(exc).__name__}: {exc}.",
            RuntimeWarning,
            stacklevel=2,
        )
        # Schema stability: emit the five xyac_* columns as all-null so the
        # returned frame has a stable column set whether or not the model
        # could be obtained (matches calculate_xyac + the ESPN __process_xyac
        # contract — callers can always select the columns).
        raw = raw.with_columns(
            [pl.lit(None, dtype=pl.Float64).alias(c) for c in _XYAC_OUT_COLS if c not in raw.columns]
        )

    if return_as_pandas:
        return raw.to_pandas()
    return raw
