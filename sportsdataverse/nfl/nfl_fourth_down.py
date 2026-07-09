"""Fourth-down decision surface for the NFL (nfl4th port).

Faithful Python port of `nfl4th <https://github.com/nflverse/nfl4th>`_'s
``add_4th_probs()`` against the actual nfl4th model artifacts.  The surface
mirrors the R reference's three decision paths plus the max-WP recommendation
and nfl4th's headline ``go_boost`` number:

* **go** — :func:`get_go_wp` (nfl4th ``get_go_wp``): the 76-class yards-gained
  distribution (``fd_model``) is expanded per play, each outcome's hypothetical
  post-play game state is scored with win probability, the touchdown branch is
  routed through :func:`get_2pt_wp` (PAT vs 2-pt choice), and the option value is
  the prob-weighted WP.  Emits ``go_wp`` / ``first_down_prob`` / ``wp_succeed`` /
  ``wp_fail``.
* **field goal** — :func:`get_fg_wp` (nfl4th ``get_fg_wp``): the make
  probability comes from the self-trained ``fg_model`` (a ``binary:logistic``
  XGBoost re-train of the original mgcv GAM, features
  ``[yardline_100, fg_roof, fg_era]`` where ``fg_roof = (roof == "outdoors")``
  and ``fg_era = (season >= 2020)``) with the long-kick decay; it weights the
  made-FG WP (opponent receives a kickoff, +3) against the missed-FG WP
  (opponent takes over at the spot).  Emits ``fg_make_prob`` / ``make_fg_wp`` /
  ``miss_fg_wp`` / ``fg_wp``.
* **punt** — :func:`get_punt_wp` (nfl4th ``get_punt_wp``): a punt landing
  distribution (``punt_data``: ``yardline_after`` / ``pct`` / ``muff`` per
  ``yardline_100``) is joined per play, field + possession are flipped (with
  return-TD / muff handling), and the option value is the prob-weighted WP of the
  receiving team's ensuing drive (from the punting team's perspective).  Emits
  ``punt_wp``.

The win-probability engine is nfl4th's own ``calculate_win_probability``: the
**average** of the nfl4th home-win-probability model (``wp_model``) and the
nflfastR possession-team win-probability model (this package's
:func:`sportsdataverse.nfl.ep_wp.calculate_win_probability`), each flipped back
to the original possessing team's perspective.

The combiner :func:`get_4th_down_probs` adds all of the above plus
``fourth_down_recommendation`` (the max-WP choice among go/punt/field_goal), a
per-option ``*_wp_diff`` (option WP minus the recommended option's WP, so the
recommended option's diff is 0 and the others are <= 0), and ``go_boost``
(nfl4th's ``100 * (go_wp - max(fg_wp, punt_wp))`` in percentage points).

Input columns
-------------
The functions consume an nflverse-shape play-by-play frame (the output of
:func:`sportsdataverse.nfl.load_nfl_pbp`).  Required columns:
``game_id``, ``play_id``, ``season``, ``week``, ``season_type``, ``posteam``,
``defteam``, ``home_team``, ``away_team``, ``roof``, ``qtr``,
``quarter_seconds_remaining``, ``ydstogo``, ``yardline_100``,
``score_differential``, ``posteam_timeouts_remaining``,
``defteam_timeouts_remaining``, ``home_opening_kickoff``, ``spread_line``,
``total_line``.

Model availability
------------------
``fd_model.ubj`` (~73 MB) and ``wp_model.ubj`` (~7.6 MB) are NOT bundled in the
wheel: on first use they are downloaded from the ``nfl_4th_down_models`` GitHub
release and cached under ``<cache_dir>/models/`` (see
:func:`sportsdataverse.nfl.ep_wp._load_model`).  The 2-pt model, FG grid and
punt distribution are bundled under ``nfl/models/``.  When the downloaded models
cannot be obtained (offline + no cache), the corresponding columns are emitted as
nulls (never fabricated); check :data:`FD_MODEL_AVAILABLE` /
:data:`WP_MODEL_AVAILABLE` to know which mode is active.  The 2-pt model, FG
model (``fg_model.ubj``) and punt distribution are all bundled under
``nfl/models/`` (see :data:`FG_MODEL_AVAILABLE` / :data:`TWO_PT_MODEL_AVAILABLE`).
"""

from __future__ import annotations

from functools import lru_cache
from pathlib import Path
from typing import TYPE_CHECKING, Optional, Union

import numpy as np
import pandas as pd
import polars as pl

from sportsdataverse.nfl.ep_wp import (
    _load_model,
    calculate_expected_points,
    calculate_win_probability,
)

if TYPE_CHECKING:  # pragma: no cover
    from xgboost import Booster

__all__ = [
    "get_4th_down_probs",
    "get_go_wp",
    "get_fg_wp",
    "get_punt_wp",
    "get_2pt_wp",
    "fg_make_probability",
    "FD_MODEL_AVAILABLE",
    "WP_MODEL_AVAILABLE",
    "TWO_PT_MODEL_AVAILABLE",
    "FG_MODEL_AVAILABLE",
]

# --------------------------------------------------------------------------- #
# model artifacts
# --------------------------------------------------------------------------- #
_FD_MODEL_FILE = "fd_model.ubj"  # download-on-demand (multi:softprob, 76 classes)
_WP_MODEL_FILE = "wp_model.ubj"  # download-on-demand (nfl4th home-WP, binary:logistic)
_TWO_PT_MODEL_FILE = "two_pt_model.ubj"  # bundled (binary:logistic, 9 features)
_FG_MODEL_FILE = "fg_model.ubj"  # bundled (binary:logistic, 3 features)

#: ``fg_model`` feature order (decision_models ``FG_FEATURES``).  The booster carries its
#: own feature names, but the DMatrix is built in this exact order to match:
#: ``fg_roof = 1`` when ``roof == "outdoors"``, ``fg_era = 1`` when ``season >= 2020``.
FG_FEATURES: list[str] = [
    "yardline_100",
    "fg_roof",
    "era0",
    "era1",
    "era2",
    "era3",
    "era4",
]

#: ``fd_model`` feature order (nfl4th ``get_go_wp``).  The model has no embedded
#: feature names, so column order is load-bearing.
FD_FEATURES: list[str] = [
    "down",
    "ydstogo",
    "yardline_100",
    "era0",
    "era1",
    "era2",
    "era3",
    "era4",
    "outdoors",
    "retractable",
    "dome",
    "posteam_spread",
    "total_line",
    "posteam_total",
]
FD_NUM_CLASS: int = 76  # gain class k -> yards = k - 10, range -10..65

#: nfl4th home-WP model feature order (``apply_win_prob.R`` ``wp_model_select``).
WP_MODEL_FEATURES: list[str] = [
    "home_receive_2h_ko",
    "spread_time",
    "home_posteam",
    "half_seconds_remaining",
    "game_seconds_remaining",
    "Diff_Time_Ratio",
    "home_score_differential",
    "home_ep",
    "ydstogo",
    "home_yardline_100",
    "home_timeouts_remaining",
]

#: 2-pt conversion model feature order (nfl4th ``get_2pt_wp``).
TWO_PT_FEATURES: list[str] = [
    "era2",
    "era3",
    "era4",
    "outdoors",
    "retractable",
    "dome",
    "posteam_spread",
    "total_line",
    "posteam_total",
]


def _bundled_models_dir() -> Path:
    """Directory holding the bundled NFL model artifacts (``nfl/models/``)."""
    return Path(__file__).resolve().parent / "models"


@lru_cache(maxsize=1)
def _load_two_pt_model() -> Optional["Booster"]:
    """Load the bundled 2-pt conversion model (``two_pt_model.ubj``), or ``None``."""
    try:
        return _load_model(_TWO_PT_MODEL_FILE, models_dir=_bundled_models_dir())
    except Exception:  # pragma: no cover - depends on bundling
        return None


@lru_cache(maxsize=1)
def _load_fg_model() -> Optional["Booster"]:
    """Load the bundled FG make-probability model (``fg_model.ubj``), or ``None``."""
    try:
        return _load_model(_FG_MODEL_FILE, models_dir=_bundled_models_dir())
    except Exception:  # pragma: no cover - depends on bundling
        return None


def _fg_make_prob(yardline_100: np.ndarray, fg_roof: np.ndarray, era: np.ndarray) -> Optional[np.ndarray]:
    """Predict the FG make probability from the bundled ``fg_model``.

    Builds the feature matrix in :data:`FG_FEATURES` order
    (``yardline_100``, ``fg_roof``, ``fg_era``), predicts the ``binary:logistic``
    make probability, and applies nfl4th's long-kick post-processing: shrink by
    0.9 at/beyond ``yardline_100 = 38`` and zero at/beyond ``yardline_100 = 45``
    (>= ~63-yard kicks).  Returns ``None`` when the model is unavailable.
    """
    model = _load_fg_model()
    if model is None:
        return None
    from xgboost import DMatrix

    yl: np.ndarray = yardline_100.astype(float)
    x_fg = np.column_stack([yl, fg_roof.astype(float), era.astype(float)]).astype(np.float32)
    make_prob: np.ndarray = model.predict(DMatrix(x_fg, feature_names=FG_FEATURES)).astype(float)
    # nfl4th long-kick clamps: zero at/beyond the 45 (>= ~63 yd), shrink 0.9 at/beyond the 38.
    make_prob = np.where(yl >= 45, 0.0, make_prob)
    make_prob = np.where(yl >= 38, 0.9 * make_prob, make_prob)
    return make_prob


def fg_make_probability(yardline_100: np.ndarray, fg_roof: np.ndarray, era: np.ndarray) -> Optional[np.ndarray]:
    """Predict FG make probability from the bundled ``fg_model`` (public wrapper).

    Thin supported alias over the private underscore-prefixed helper so downstream
    consumers (e.g. the kicker-rating spine) reuse the shipped model through a
    public import instead of a private reach.

    Args:
        yardline_100: Kick spot (yards from the opponent end zone); the
            attempt distance is ``yardline_100 + 18``.
        fg_roof: 1.0 when ``roof == "outdoors"`` else 0.0, per kick.
        era: ``(n, 5)`` one-hot era matrix (``era0``..``era4``, season cuts
            2001/2005/2013/2017).

    Returns:
        Make probabilities (with nfl4th's long-kick clamps), or ``None``
        when the bundled model is unavailable.

    Example:
        Quick start::

            import numpy as np
            from sportsdataverse.nfl.nfl_fourth_down import fg_make_probability
            p = fg_make_probability(
                np.array([30.0]), np.array([1.0]),
                np.array([[0.0, 0.0, 0.0, 0.0, 1.0]]),
            )
            print(p)
    """
    return _fg_make_prob(yardline_100, fg_roof, era)


@lru_cache(maxsize=1)
def _load_punt_data() -> Optional[pl.DataFrame]:
    """Load the bundled punt landing distribution (``punt_data.parquet``)."""
    path = _bundled_models_dir() / "punt_data.parquet"
    if not path.exists():  # pragma: no cover - depends on bundling
        return None
    return pl.read_parquet(path)


def _try_load(name: str) -> Optional["Booster"]:
    """Resolve a download-on-demand booster, returning ``None`` when unobtainable.

    On first call this may download + cache the model (via ``ep_wp._load_model``);
    subsequent calls hit the lru-cache.  Used by the ``get_*`` functions at call
    time — NOT at import time (see :data:`FD_MODEL_AVAILABLE`).
    """
    try:
        return _load_model(name)
    except Exception:  # pragma: no cover - network/offline dependent
        return None


def _on_disk(name: str) -> bool:
    """``True`` when ``name`` is already resolvable from disk (bundled or cached).

    Filesystem-only — never triggers a network download — so it is safe to call
    at import time for the ``*_MODEL_AVAILABLE`` flags.
    """
    from sportsdataverse.nfl.ep_wp import _model_cache_dir

    return (_bundled_models_dir() / name).exists() or (_model_cache_dir() / name).exists()


#: ``True`` when the fourth-down yards model is already on disk (bundled/cached).
#: A missing model is still fetched on first ``get_go_wp`` call (download-on-demand).
FD_MODEL_AVAILABLE: bool = _on_disk(_FD_MODEL_FILE)
#: ``True`` when the nfl4th home-WP model is already on disk (bundled/cached).
WP_MODEL_AVAILABLE: bool = _on_disk(_WP_MODEL_FILE)
#: ``True`` when the bundled 2-pt conversion model is present.
TWO_PT_MODEL_AVAILABLE: bool = (_bundled_models_dir() / _TWO_PT_MODEL_FILE).exists()
#: ``True`` when the bundled FG make-probability model is present.
FG_MODEL_AVAILABLE: bool = (_bundled_models_dir() / _FG_MODEL_FILE).exists()


# --------------------------------------------------------------------------- #
# prep + helpers (nfl4th helpers.R / apply_win_prob.R, in nflverse column space)
# --------------------------------------------------------------------------- #
# Columns passed through to ep_wp.calculate_expected_points / calculate_win_probability.
_EPWP_IN: list[str] = [
    "game_id",
    "season",
    "week",
    "posteam",
    "defteam",
    "home_team",
    "away_team",
    "roof",
    "half_seconds_remaining",
    "game_seconds_remaining",
    "qtr",
    "down",
    "ydstogo",
    "yardline_100",
    "score_differential",
    "posteam_timeouts_remaining",
    "defteam_timeouts_remaining",
    "spread_line",
    "total_line",
    "home_opening_kickoff",
]


def _to_pandas(pbp: Union[pl.DataFrame, "pd.DataFrame"]) -> pd.DataFrame:
    """Accept polars or pandas; return a fresh pandas copy."""
    if hasattr(pbp, "to_pandas"):
        return pbp.to_pandas()
    return pd.DataFrame(pbp).reset_index(drop=True)


def _prepare(df: pd.DataFrame) -> pd.DataFrame:
    """nfl4th ``prepare_df`` + games-file mutations, in nflverse column space.

    Sets ``down=4``, era/roof dummies, the FG-grid roof key, posteam spread/total,
    home/away timeout split, the home-receive-2H-kickoff indicator and the
    ``spread_time`` time-decay column needed by the WP engine.
    """
    d = df.copy().reset_index(drop=True)
    roof = d["roof"]
    model_roof = np.where(
        roof.isin(["open", "closed"]) | roof.isna(),
        "retractable",
        np.where(roof == "retractable", "retractable", np.where(roof == "dome", "dome", "outdoors")),
    )
    d["model_roof"] = model_roof
    season = d["season"].to_numpy()
    d["era0"] = (season <= 2001).astype(int)
    d["era1"] = ((season > 2001) & (season <= 2005)).astype(int)
    d["era2"] = ((season > 2005) & (season <= 2013)).astype(int)
    d["era3"] = ((season > 2013) & (season <= 2017)).astype(int)
    d["era4"] = (season > 2017).astype(int)
    # fg_model features (decision_models FG_FEATURES): yardline_100, fg_roof, and the
    # era0..era4 one-hot above. fg is era-aware across all kicking eras; the PAT path
    # (fg @ yardline_100 = 15) reads the modern era directly via era4.
    d["fg_roof"] = np.where(roof.to_numpy() == "outdoors", 1, 0)
    d["home_total"] = (d["total_line"] + d["spread_line"]) / 2.0
    d["away_total"] = (d["total_line"] - d["spread_line"]) / 2.0
    d["retractable"] = (d["model_roof"] == "retractable").astype(int)
    d["dome"] = (d["model_roof"] == "dome").astype(int)
    d["outdoors"] = (d["model_roof"] == "outdoors").astype(int)
    d["roof"] = d["model_roof"]
    qtr = d["qtr"].to_numpy()
    d["home_receive_2h_ko"] = np.where(qtr <= 2, np.where(d["home_opening_kickoff"].to_numpy() == 1, -1, 1), 0)
    d["down"] = 4
    qsr = d["quarter_seconds_remaining"].to_numpy()
    d["half_seconds_remaining"] = np.where((qtr == 2) | (qtr == 4), qsr, qsr + 900)
    hsr = d["half_seconds_remaining"].to_numpy()
    d["game_seconds_remaining"] = np.where(qtr <= 2, hsr + 1800, hsr)
    d["elapsed_share"] = (3600.0 - d["game_seconds_remaining"].to_numpy()) / 3600.0
    d["spread_time"] = d["spread_line"].to_numpy() * np.exp(-4.0 * d["elapsed_share"].to_numpy())
    is_home = (d["posteam"] == d["home_team"]).to_numpy()
    d["posteam_spread"] = np.where(is_home, d["spread_line"].to_numpy(), -d["spread_line"].to_numpy())
    d["posteam_total"] = np.where(is_home, d["home_total"].to_numpy(), d["away_total"].to_numpy())
    d["home_timeouts_remaining"] = np.where(
        is_home, d["posteam_timeouts_remaining"].to_numpy(), d["defteam_timeouts_remaining"].to_numpy()
    )
    is_away = (d["posteam"] == d["away_team"]).to_numpy()
    d["away_timeouts_remaining"] = np.where(
        is_away, d["posteam_timeouts_remaining"].to_numpy(), d["defteam_timeouts_remaining"].to_numpy()
    )
    d["original_posteam"] = d["posteam"]
    return d


def _calc_wp(d: pd.DataFrame) -> np.ndarray:
    """nfl4th ``calculate_win_probability``: average of the home-WP model and the
    nflfastR possession-team WP model, each flipped to the original posteam.

    Mirrors ``apply_win_prob.R`` exactly: pos/def timeouts are recomputed from the
    stable home/away timeout counts based on the *current* posteam, the nflfastR
    EP feeds ``home_ep``, and ``receive_2h_ko`` is rebuilt for the nflfastR model.
    """
    n = len(d)
    if n == 0:
        return np.array([], dtype=float)
    d = d.copy()
    home = (d["posteam"] == d["home_team"]).to_numpy()
    away = (d["posteam"] == d["away_team"]).to_numpy()
    home_to = d["home_timeouts_remaining"].to_numpy()
    away_to = d["away_timeouts_remaining"].to_numpy()
    d["posteam_timeouts_remaining"] = np.where(away, away_to, home_to)
    d["defteam_timeouts_remaining"] = np.where(home, away_to, home_to)
    qtr = d["qtr"].to_numpy()
    hok = d["home_opening_kickoff"].to_numpy()
    receive_2h_ko = np.where(
        (qtr <= 2) & (hok == 1) & away,
        1,
        np.where((qtr <= 2) & (hok == 0) & home, 1, 0),
    )
    d2 = d.copy()
    d2["defteam"] = np.where(home, d2["away_team"], d2["home_team"])
    sub = d2[[c for c in _EPWP_IN if c in d2.columns]].copy()
    sub["receive_2h_ko"] = receive_2h_ko
    plf = pl.from_pandas(sub)

    # nflfastR EP -> home_ep, and nflfastR possession-team WP.
    ep = calculate_expected_points(plf, return_as_pandas=True)["ep"].to_numpy()
    vegas_wp_pos = calculate_win_probability(plf, return_as_pandas=True)["vegas_wp"].to_numpy()

    elapsed = (3600.0 - d["game_seconds_remaining"].to_numpy()) / 3600.0
    sd = d["score_differential"].to_numpy().astype(float)
    home_score_diff = np.where(home, sd, -sd)
    home_posteam = home.astype(int)
    yl = d["yardline_100"].to_numpy().astype(float)
    home_yardline = np.where(home, yl, 100.0 - yl)
    home_ep = np.where(home, ep, -ep)
    diff_time_ratio = home_score_diff / np.exp(-4.0 * elapsed)

    wp_model = _try_load(_WP_MODEL_FILE)
    if wp_model is None:
        return np.full(n, np.nan)
    from xgboost import DMatrix

    x_wp = np.column_stack(
        [
            d["home_receive_2h_ko"].to_numpy(),
            d["spread_time"].to_numpy(),
            home_posteam,
            d["half_seconds_remaining"].to_numpy(),
            d["game_seconds_remaining"].to_numpy(),
            diff_time_ratio,
            home_score_diff,
            home_ep,
            d["ydstogo"].to_numpy(),
            home_yardline,
            d["home_timeouts_remaining"].to_numpy(),
        ]
    ).astype(np.float32)
    vegas_home_wp = wp_model.predict(DMatrix(x_wp, feature_names=WP_MODEL_FEATURES))

    orig_away = (d["original_posteam"] == d["away_team"]).to_numpy()
    pos_changed = (d["posteam"] != d["original_posteam"]).to_numpy()
    vegas_home_wp = np.where(orig_away, 1.0 - vegas_home_wp, vegas_home_wp)
    vegas_wp_pos = np.where(pos_changed, 1.0 - vegas_wp_pos, vegas_wp_pos)
    return (vegas_home_wp + vegas_wp_pos) / 2.0


def _flip_team(d: pd.DataFrame) -> pd.DataFrame:
    """nfl4th ``flip_team``: hand the ball over, 1st-and-10, run off 6 seconds.

    Note (matching the R reference): ``yardline_100`` and the home/away timeout
    counts are NOT mutated here — callers set ``yardline_100`` per scenario, and
    :func:`_calc_wp` re-derives pos/def timeouts from the stable home/away counts.
    """
    d = d.copy()
    d["posteam"] = np.where(d["home_team"] == d["posteam"], d["away_team"], d["home_team"])
    d["score_differential"] = -d["score_differential"].to_numpy()
    d["down"] = 1
    d["ydstogo"] = 10
    d["half_seconds_remaining"] = np.maximum(d["half_seconds_remaining"].to_numpy() - 6.0, 0.0)
    d["game_seconds_remaining"] = np.maximum(d["game_seconds_remaining"].to_numpy() - 6.0, 0.0)
    return d


def _flip_half(d: pd.DataFrame) -> pd.DataFrame:
    """nfl4th ``flip_half``: on an end-of-2nd-quarter play, jump to start of 3Q."""
    d = d.copy()
    prior = d["posteam"].to_numpy().copy()
    eoh = (d["qtr"].to_numpy() == 2) & (d["half_seconds_remaining"].to_numpy() == 0)
    hok = d["home_opening_kickoff"].to_numpy()
    new_pos = np.where((hok == 1) & eoh, d["away_team"], np.where((hok == 0) & eoh, d["home_team"], d["posteam"]))
    d["posteam"] = new_pos
    d["qtr"] = np.where(eoh, 3, d["qtr"].to_numpy())
    d["down"] = np.where(eoh, 1, d["down"].to_numpy())
    d["ydstogo"] = np.where(eoh, 10, d["ydstogo"].to_numpy())
    d["yardline_100"] = np.where(eoh, 75, d["yardline_100"].to_numpy())
    d["half_seconds_remaining"] = np.where(eoh, 1800, d["half_seconds_remaining"].to_numpy())
    d["game_seconds_remaining"] = np.where(eoh, 1800, d["game_seconds_remaining"].to_numpy())
    pos_changed = (d["posteam"].to_numpy() != prior) & eoh
    d["score_differential"] = np.where(
        pos_changed, -d["score_differential"].to_numpy(), d["score_differential"].to_numpy()
    )
    d["home_receive_2h_ko"] = np.where(eoh, 0, d["home_receive_2h_ko"].to_numpy())
    return d


def _end_game(d: pd.DataFrame, wp: np.ndarray) -> np.ndarray:
    """nfl4th ``end_game_fn``: pin WP to 0 when the leading defense can kneel out.

    ``wp`` is the resulting state's win probability from the perspective of the
    team possessing in ``d`` (already flipped to the receiving/defending team in
    the punt/FG ensuing-drive frames before the final perspective flip).
    """
    d_home = (d["posteam"] == d["home_team"]).to_numpy()
    def_to = np.where(d_home, d["away_timeouts_remaining"].to_numpy(), d["home_timeouts_remaining"].to_numpy())
    sd = d["score_differential"].to_numpy()
    gsr = d["game_seconds_remaining"].to_numpy()
    wp = np.where((sd > 0) & (gsr < 120) & (def_to == 0), 0.0, wp)
    wp = np.where((sd > 0) & (gsr < 80) & (def_to == 1), 0.0, wp)
    wp = np.where((sd > 0) & (gsr < 40) & (def_to == 2), 0.0, wp)
    return wp


def _fd_long_frame(d: pd.DataFrame) -> pd.DataFrame:
    """Expand the fd_model 76-class distribution to a long (play × gain) frame.

    Returns a frame with one row per (``go_index``, ``gain``) carrying the play's
    state columns plus ``prob``.  ``None`` of the post-play mutations are applied
    here — :func:`get_go_wp` owns that.  Returns an empty frame when the model is
    unavailable.
    """
    n = len(d)
    fd_model = _try_load(_FD_MODEL_FILE)
    if fd_model is None:
        return pd.DataFrame()
    from xgboost import DMatrix

    x_fd = np.column_stack([d[c].to_numpy() for c in FD_FEATURES]).astype(np.float32)
    probs = fd_model.predict(DMatrix(x_fd, feature_names=FD_FEATURES))
    if probs.ndim == 1:
        probs = probs.reshape(n, FD_NUM_CLASS)
    gains = np.tile(np.arange(-10, FD_NUM_CLASS - 10), n)
    go_index: np.ndarray = np.repeat(d["go_index"].to_numpy(), FD_NUM_CLASS)
    long = pd.DataFrame({"go_index": go_index, "gain": gains, "prob": probs.reshape(-1)})
    return long.merge(d, on="go_index", how="left")


# --------------------------------------------------------------------------- #
# 2-pt path (nfl4th get_2pt_wp) — used by the go path's touchdown branch
# --------------------------------------------------------------------------- #
def get_2pt_wp(pbp_df: Union[pl.DataFrame, "pd.DataFrame"]) -> pd.DataFrame:
    """Win probability of the PAT-vs-2pt choice after a touchdown (nfl4th ``get_2pt_wp``).

    For each row, scores the post-touchdown state under three scoring outcomes
    (0 / 1 / 2 added points) from the kicking-off team's ensuing-drive WP, and
    combines them with the 2-pt conversion probability (``two_pt_model``) and the
    PAT make probability (the FG model at ``yardline_100 = 15``) into ``wp_td`` —
    the better of go-for-2 and kick-the-PAT.

    Args:
        pbp_df: Play-by-play frame (polars or pandas) of post-touchdown states,
            already carrying the prepared state columns (see module docstring).

    Returns:
        A pandas frame with ``go_index``, ``yardline_100`` (always 0) and
        ``wp_td``.  ``wp_td`` is NaN when the WP / 2-pt models are unavailable.

    Example:
        Quick start::

            from sportsdataverse.nfl.nfl_fourth_down import get_2pt_wp
            out = get_2pt_wp(touchdown_states)
            print(out[["go_index", "wp_td"]].head())
    """
    d = _to_pandas(pbp_df).reset_index(drop=True)
    n = len(d)
    if n == 0:
        return pd.DataFrame({"go_index": [], "yardline_100": [], "wp_td": []})

    two_pt = _load_two_pt_model()
    if two_pt is None or _load_fg_model() is None or _try_load(_WP_MODEL_FILE) is None:
        out = d[["go_index"]].copy()
        out["yardline_100"] = 0
        out["wp_td"] = np.nan
        return out[["go_index", "yardline_100", "wp_td"]]

    from xgboost import DMatrix

    x2 = np.column_stack(
        [
            np.zeros(n),  # era2 = 0 (nfl4th hard-codes era2=0 in get_2pt_wp)
            d["era3"].to_numpy(),
            d["era4"].to_numpy(),
            d["outdoors"].to_numpy(),
            d["retractable"].to_numpy(),
            d["dome"].to_numpy(),
            d["posteam_spread"].to_numpy(),
            d["total_line"].to_numpy(),
            d["posteam_total"].to_numpy(),
        ]
    ).astype(np.float32)
    conv_2pt = two_pt.predict(DMatrix(x2, feature_names=TWO_PT_FEATURES))

    # PAT make probability: the FG model at yardline_100 = 15 (the PAT spot).
    pat_yl: np.ndarray = np.full(n, 15, dtype=float)
    conv_1pt = _fg_make_prob(pat_yl, d["fg_roof"].to_numpy(), d[["era0", "era1", "era2", "era3", "era4"]].to_numpy())
    if conv_1pt is None:  # FG model unobtainable on the re-load (race/eviction) — emit NaN, never crash
        out = d[["go_index"]].copy()
        out["yardline_100"] = 0
        out["wp_td"] = np.nan
        return out[["go_index", "yardline_100", "wp_td"]]

    rows = []
    for pts in (0, 1, 2):
        r = d.copy()
        r["score_differential"] = -d["score_differential"].to_numpy() - pts
        r["posteam"] = np.where(d["home_team"] == d["posteam"], d["away_team"], d["home_team"])
        r["yardline_100"] = 75
        r["down"] = 1
        r["ydstogo"] = 10
        r["pts"] = pts
        rows.append(r)
    allr = pd.concat(rows, ignore_index=True)
    allr = _flip_half(allr)
    allr["vegas_wp"] = _calc_wp(allr)

    piv = allr.pivot_table(index="go_index", columns="pts", values="vegas_wp")
    res = pd.DataFrame(
        {"go_index": piv.index, "wp0": piv[0].to_numpy(), "wp1": piv[1].to_numpy(), "wp2": piv[2].to_numpy()}
    )
    res = res.merge(d[["go_index"]].assign(conv_2pt=conv_2pt, conv_1pt=conv_1pt), on="go_index", how="left")
    wp_go2 = (
        res["conv_2pt"].to_numpy() * res["wp2"].to_numpy() + (1.0 - res["conv_2pt"].to_numpy()) * res["wp0"].to_numpy()
    )
    wp_go1 = (
        res["conv_1pt"].to_numpy() * res["wp1"].to_numpy() + (1.0 - res["conv_1pt"].to_numpy()) * res["wp0"].to_numpy()
    )
    res["wp_td"] = np.where(wp_go1 > wp_go2, wp_go1, wp_go2)
    res["yardline_100"] = 0
    return res[["go_index", "yardline_100", "wp_td"]]


# --------------------------------------------------------------------------- #
# GO path (nfl4th get_go_wp)
# --------------------------------------------------------------------------- #
def get_go_wp(pbp_df: Union[pl.DataFrame, "pd.DataFrame"]) -> pd.DataFrame:
    """Expected win probability of going for it on 4th down (nfl4th ``get_go_wp``).

    The fd_model 76-class yards-gained distribution is expanded per play; each
    outcome's hypothetical post-play game state (turnover-on-downs flip, +6
    touchdown with the PAT/2-pt branch routed through :func:`get_2pt_wp`, 6-second
    runoff, goal-to-go distance shrink) is scored with win probability and the
    end-of-game kneel-out clamps are applied; the option value is the
    prob-weighted WP.

    Args:
        pbp_df: Play-by-play frame (polars or pandas) of fourth-down situations
            carrying the prepared state columns (see module docstring).  The
            frame is prepared internally if it lacks the derived columns.

    Returns:
        A pandas copy of ``pbp_df`` plus ``go_wp`` (prob-weighted WP of going for
        it), ``first_down_prob`` (P(conversion)), ``wp_succeed`` (mean WP over
        conversion outcomes) and ``wp_fail`` (mean WP over failure outcomes).
        All are NaN when the fourth-down / WP models are unavailable
        (:data:`FD_MODEL_AVAILABLE` / :data:`WP_MODEL_AVAILABLE`).

    Example:
        Quick start::

            from sportsdataverse.nfl import load_nfl_pbp
            from sportsdataverse.nfl.nfl_fourth_down import get_go_wp

            pbp = load_nfl_pbp([2023])
            fourth = pbp.filter((pl.col("down") == 4) & pl.col("yardline_100").is_not_null())
            out = get_go_wp(fourth)
            print(out[["go_wp", "first_down_prob"]].head())
    """
    base = _to_pandas(pbp_df)
    n = len(base)
    cols = ("go_wp", "first_down_prob", "wp_succeed", "wp_fail")
    if n == 0:
        out = base.copy()
        for c in cols:
            out[c] = pd.Series([], dtype=float)
        return out

    d = base if "posteam_spread" in base.columns else _prepare(base)
    d = d.reset_index(drop=True)
    d["go_index"] = np.arange(n)

    long = _fd_long_frame(d)
    if len(long) == 0 or _try_load(_WP_MODEL_FILE) is None:
        out = base.copy()
        for c in cols:
            out[c] = np.nan
        return out

    # cap at TD (gains longer than possible become a TD), then collapse duplicates
    long["gain"] = np.where(long["gain"] > long["yardline_100"], long["yardline_100"], long["gain"]).astype(int)
    agg = {"prob": "sum"}
    agg.update({c: "first" for c in d.columns if c not in ("go_index", "prob")})
    long = long.groupby(["go_index", "gain"], as_index=False).agg(agg)

    long["yardline_100"] = long["yardline_100"].to_numpy() - long["gain"].to_numpy()
    long["turnover"] = (long["gain"].to_numpy() < long["ydstogo"].to_numpy()).astype(int)
    long["down"] = 1
    to = long["turnover"].to_numpy() == 1
    long["yardline_100"] = np.where(to, 100 - long["yardline_100"].to_numpy(), long["yardline_100"].to_numpy())
    home_pos = (long["home_team"] == long["posteam"]).to_numpy()
    away_pos = (long["away_team"] == long["posteam"]).to_numpy()
    long["posteam"] = np.where(
        to & home_pos, long["away_team"], np.where(to & away_pos, long["home_team"], long["posteam"])
    )
    long["score_differential"] = np.where(
        to, -long["score_differential"].to_numpy(), long["score_differential"].to_numpy()
    )
    yl = long["yardline_100"].to_numpy()
    long["score_differential"] = np.where(
        yl == 0, long["score_differential"].to_numpy() + 6, long["score_differential"].to_numpy()
    )
    long["half_seconds_remaining"] = np.maximum(long["half_seconds_remaining"].to_numpy() - 6.0, 0.0)
    long["game_seconds_remaining"] = np.maximum(long["game_seconds_remaining"].to_numpy() - 6.0, 0.0)
    long["ydstogo"] = np.where(yl < 10, yl, 10)

    td = long[long["yardline_100"] == 0]
    if len(td) > 0:
        td_wp = get_2pt_wp(td)
    else:
        td_wp = pd.DataFrame({"go_index": [-1], "yardline_100": [99999], "wp_td": [np.nan]})
    long = long.merge(td_wp, on=["go_index", "yardline_100"], how="left")

    long = _flip_half(long)
    vw = _calc_wp(long)
    long["vegas_wp"] = np.where(long["yardline_100"].to_numpy() == 0, long["wp_td"].to_numpy(), vw)

    # end-of-game kneel-out clamps (nfl4th get_go_wp)
    d_home = (long["posteam"] == long["home_team"]).to_numpy()
    def_to = np.where(d_home, long["away_timeouts_remaining"].to_numpy(), long["home_timeouts_remaining"].to_numpy())
    sd = long["score_differential"].to_numpy()
    gsr = long["game_seconds_remaining"].to_numpy()
    tov = long["turnover"].to_numpy()
    ylf = long["yardline_100"].to_numpy()
    wp = long["vegas_wp"].to_numpy().copy()
    succ = (sd > 0) & (tov == 0) & (ylf > 0)
    wp = np.where(succ & (gsr < 120) & (def_to == 0), 1.0, wp)
    wp = np.where(succ & (gsr < 80) & (def_to == 1), 1.0, wp)
    wp = np.where(succ & (gsr < 40) & (def_to == 2), 1.0, wp)
    fail = (sd > 0) & (tov == 1)
    wp = np.where(fail & (gsr < 120) & (def_to == 0), 0.0, wp)
    wp = np.where(fail & (gsr < 80) & (def_to == 1), 0.0, wp)
    wp = np.where(fail & (gsr < 40) & (def_to == 2), 0.0, wp)
    long["vegas_wp"] = wp
    long["wt_wp"] = long["prob"].to_numpy() * long["vegas_wp"].to_numpy()

    go = long.groupby("go_index")["wt_wp"].sum().rename("go_wp")
    # Named agg (not groupby.apply) so this works on the pandas>=2.0 floor:
    # apply(include_groups=) only exists in pandas>=2.2.  wt_wp == prob*vegas_wp
    # (set above), so wp = sum(wt_wp)/sum(prob) is the prob-weighted mean.
    rep = long.groupby(["go_index", "turnover"], as_index=False).agg(pct=("prob", "sum"), _pwsum=("wt_wp", "sum"))
    rep["wp"] = rep["_pwsum"] / rep["pct"]
    rep = rep.drop(columns="_pwsum")
    piv = rep.pivot(index="go_index", columns="turnover")
    pct0 = piv["pct"].get(0)
    wp0 = piv["wp"].get(0)
    wp1 = piv["wp"].get(1)
    n_rep = len(piv.index)
    report = pd.DataFrame({"go_index": piv.index})
    report["first_down_prob"] = pct0.to_numpy() if pct0 is not None else np.zeros(n_rep)
    report["wp_succeed"] = wp0.to_numpy() if wp0 is not None else np.full(n_rep, np.nan)
    report["wp_fail"] = wp1.to_numpy() if wp1 is not None else np.full(n_rep, np.nan)

    merged = (
        pd.DataFrame({"go_index": np.arange(n)})
        .merge(go, on="go_index", how="left")
        .merge(report, on="go_index", how="left")
    )
    out = base.copy()
    out["go_wp"] = merged["go_wp"].to_numpy()
    out["first_down_prob"] = merged["first_down_prob"].to_numpy()
    out["wp_succeed"] = merged["wp_succeed"].to_numpy()
    out["wp_fail"] = merged["wp_fail"].to_numpy()
    return out


# --------------------------------------------------------------------------- #
# FG path (nfl4th get_fg_wp)
# --------------------------------------------------------------------------- #
def get_fg_wp(pbp_df: Union[pl.DataFrame, "pd.DataFrame"]) -> pd.DataFrame:
    """Expected win probability of attempting a field goal (nfl4th ``get_fg_wp``).

    The make probability comes from the self-trained ``fg_model`` (a
    ``binary:logistic`` XGBoost re-train of the original mgcv GAM, features
    ``[yardline_100, fg_roof, fg_era]``), shrunk by 0.9 for kicks at/beyond
    ``yardline_100 = 38`` and zeroed at/beyond ``yardline_100 = 45``
    (>= ~63-yard kicks).  The made-FG state (opponent receives a touchback
    kickoff at the 25, kicking team +3) and the missed-FG state (opponent takes
    over 8 yards back of the spot, capped at the 80) are each scored with win
    probability; ``fg_wp = make_prob * make_wp + (1 - make_prob) * miss_wp``.

    Args:
        pbp_df: Play-by-play frame (polars or pandas) of fourth-down situations.

    Returns:
        A pandas copy of ``pbp_df`` plus ``fg_make_prob``, ``make_fg_wp``,
        ``miss_fg_wp`` and ``fg_wp`` (from the kicking team's perspective).  All
        four are NaN when the FG model or WP model is unavailable.

    Example:
        Quick start::

            from sportsdataverse.nfl import load_nfl_pbp
            from sportsdataverse.nfl.nfl_fourth_down import get_fg_wp

            pbp = load_nfl_pbp([2023])
            fourth = pbp.filter((pl.col("down") == 4) & pl.col("yardline_100").is_not_null())
            out = get_fg_wp(fourth)
            print(out[["fg_make_prob", "fg_wp"]].head())
    """
    base = _to_pandas(pbp_df)
    n = len(base)
    cols = ("fg_make_prob", "make_fg_wp", "miss_fg_wp", "fg_wp")
    if n == 0:
        out = base.copy()
        for c in cols:
            out[c] = pd.Series([], dtype=float)
        return out

    d = base if "posteam_spread" in base.columns else _prepare(base)
    d = d.reset_index(drop=True)

    if _load_fg_model() is None or _try_load(_WP_MODEL_FILE) is None:
        out = base.copy()
        for c in cols:
            out[c] = np.nan
        return out

    yl = d["yardline_100"].to_numpy().astype(float)
    make_prob = _fg_make_prob(yl, d["fg_roof"].to_numpy(), d[["era0", "era1", "era2", "era3", "era4"]].to_numpy())
    assert make_prob is not None  # guarded above

    # made FG: flip to opponent receiving a touchback (the 25), kicking team +3.
    make_state = _flip_team(d)
    make_state["yardline_100"] = 75
    make_state["ydstogo"] = 10
    make_state["score_differential"] = make_state["score_differential"].to_numpy() - 3.0
    make_state = _flip_half(make_state)
    make_wp = _end_game(make_state, _calc_wp(make_state))

    # missed FG: opponent takes over 8 yards back of the spot, capped to [1, 80].
    miss_state = _flip_team(d)
    miss_yl = (100.0 - yl) - 8.0
    miss_yl = np.clip(miss_yl, 1.0, 80.0)
    miss_state["yardline_100"] = miss_yl
    miss_state["ydstogo"] = np.where(miss_yl < 10, miss_yl, 10)
    miss_state = _flip_half(miss_state)
    miss_wp = _end_game(miss_state, _calc_wp(miss_state))

    fg_wp = make_prob * make_wp + (1.0 - make_prob) * miss_wp
    out = base.copy()
    out["fg_make_prob"] = make_prob
    out["make_fg_wp"] = make_wp
    out["miss_fg_wp"] = miss_wp
    out["fg_wp"] = fg_wp
    return out


# --------------------------------------------------------------------------- #
# PUNT path (nfl4th get_punt_wp)
# --------------------------------------------------------------------------- #
def get_punt_wp(pbp_df: Union[pl.DataFrame, "pd.DataFrame"]) -> pd.DataFrame:
    """Expected win probability of punting on 4th down (nfl4th ``get_punt_wp``).

    The punt landing distribution (``punt_data``: ``yardline_after`` / ``pct`` /
    ``muff`` per ``yardline_100``) is joined per play; possession is flipped to
    the receiving team, with return-touchdown (``yardline_after == 100``) and muff
    (``muff == 1``) recoveries flipping the ball back to the punting team; each
    landing spot's ensuing-drive WP is scored and the option value is the
    prob-weighted WP from the punting team's perspective.

    Args:
        pbp_df: Play-by-play frame (polars or pandas) of fourth-down situations.

    Returns:
        A pandas copy of ``pbp_df`` plus ``punt_wp``.  ``punt_wp`` is NaN where
        the punt distribution has no support for the play's ``yardline_100``
        (inside the punting team's own 31, where the table is empty — matching the
        R reference's left-join NA behavior) or when the WP model is unavailable.

    Example:
        Quick start::

            from sportsdataverse.nfl import load_nfl_pbp
            from sportsdataverse.nfl.nfl_fourth_down import get_punt_wp

            pbp = load_nfl_pbp([2023])
            fourth = pbp.filter((pl.col("down") == 4) & pl.col("yardline_100").is_not_null())
            out = get_punt_wp(fourth)
            print(out[["punt_wp"]].head())
    """
    base = _to_pandas(pbp_df)
    n = len(base)
    if n == 0:
        out = base.copy()
        out["punt_wp"] = pd.Series([], dtype=float)
        return out

    d = base if "posteam_spread" in base.columns else _prepare(base)
    d = d.reset_index(drop=True)
    d["punt_index"] = np.arange(n)

    punt_data = _load_punt_data()
    if punt_data is None or _try_load(_WP_MODEL_FILE) is None:
        out = base.copy()
        out["punt_wp"] = np.nan
        return out

    pdist = punt_data.with_columns(pl.col("yardline_100").cast(pl.Float64)).to_pandas()
    long = d.merge(pdist, on="yardline_100", how="left")
    has = long["yardline_after"].notna().to_numpy()
    sup = long[has].reset_index(drop=True)
    if len(sup) == 0:
        out = base.copy()
        out["punt_wp"] = np.nan
        return out

    f = _flip_team(sup)
    ya = sup["yardline_after"].to_numpy()
    muff = sup["muff"].to_numpy()
    op = sup["original_posteam"].to_numpy()
    home_team = sup["home_team"].to_numpy()
    away_team = sup["away_team"].to_numpy()

    yl100 = 100.0 - ya
    rtd_or_muff = (ya == 100) | (muff == 1)
    # return-TD / muff recovery: ball flips back to the punting team
    f["posteam"] = np.where(
        rtd_or_muff & (op == away_team),
        away_team,
        np.where(rtd_or_muff & (op == home_team), home_team, f["posteam"].to_numpy()),
    )
    yl100 = np.where(muff == 1, 100.0 - yl100, yl100)
    yl100 = np.where(ya == 100, 75.0, yl100)
    sd = f["score_differential"].to_numpy().astype(float)  # already negated by _flip_team
    sd = np.where(ya == 100, -sd - 7.0, sd)
    sd = np.where(muff == 1, -sd, sd)
    f["yardline_100"] = yl100
    f["score_differential"] = sd
    f["ydstogo"] = np.where(yl100 < 10, yl100, f["ydstogo"].to_numpy())

    f = _flip_half(f)
    wp = _end_game(f, _calc_wp(f))

    sup2 = pd.DataFrame({"punt_index": sup["punt_index"].to_numpy(), "wt_wp": sup["pct"].to_numpy() * wp})
    agg = sup2.groupby("punt_index", as_index=False)["wt_wp"].sum().rename(columns={"wt_wp": "punt_wp"})
    merged = pd.DataFrame({"punt_index": np.arange(n)}).merge(agg, on="punt_index", how="left")
    out = base.copy()
    out["punt_wp"] = merged["punt_wp"].to_numpy()
    return out


# --------------------------------------------------------------------------- #
# combiner (nfl4th add_4th_probs) + recommendation
# --------------------------------------------------------------------------- #
def get_4th_down_probs(pbp_df: Union[pl.DataFrame, "pd.DataFrame"]) -> pd.DataFrame:
    """Full 4th-down decision surface (nfl4th ``add_4th_probs``) + recommendation.

    Runs :func:`get_go_wp`, :func:`get_fg_wp`, :func:`get_punt_wp` on the
    fourth-down rows and adds the combined option columns plus:

    * ``go_boost`` -- nfl4th's headline number: ``100 * (go_wp - max(fg_wp,
      punt_wp))`` in percentage points (a NaN ``punt_wp`` is treated as 0).
    * ``fourth_down_recommendation`` -- the max-WP choice among ``{go, punt,
      field_goal}`` (NaN options are excluded).
    * ``go_wp_diff`` / ``punt_wp_diff`` / ``fg_wp_diff`` -- each option's WP minus
      the recommended option's WP (the recommended option's diff is 0, the others
      <= 0).  NaN where the option WP is NaN.

    Args:
        pbp_df: Play-by-play frame (polars or pandas) of fourth-down situations
            (the nflverse-shape output of :func:`load_nfl_pbp`; see module
            docstring for required columns).

    Returns:
        A pandas copy of ``pbp_df`` with the decision columns added.  Empty input
        returns the input plus empty decision columns.

    Example:
        Quick start::

            from sportsdataverse.nfl import load_nfl_pbp
            from sportsdataverse.nfl.nfl_fourth_down import get_4th_down_probs

            pbp = load_nfl_pbp([2023])
            fourth = pbp.filter((pl.col("down") == 4) & pl.col("yardline_100").is_not_null())
            out = get_4th_down_probs(fourth)
            print(out[["go_wp", "punt_wp", "fg_wp", "go_boost", "fourth_down_recommendation"]].head())
    """
    base = _to_pandas(pbp_df)
    decision_cols = [
        "go_wp",
        "first_down_prob",
        "wp_succeed",
        "wp_fail",
        "fg_make_prob",
        "make_fg_wp",
        "miss_fg_wp",
        "fg_wp",
        "punt_wp",
        "go_boost",
        "go_wp_diff",
        "punt_wp_diff",
        "fg_wp_diff",
        "fourth_down_recommendation",
    ]
    if len(base) == 0:
        out = base.copy()
        for c in decision_cols:
            out[c] = pd.Series([], dtype=object if c == "fourth_down_recommendation" else float)
        return out

    prepped = _prepare(base)
    go = get_go_wp(prepped)
    fg = get_fg_wp(prepped)
    punt = get_punt_wp(prepped)

    out = base.copy().reset_index(drop=True)
    out["go_wp"] = go["go_wp"].to_numpy()
    out["first_down_prob"] = go["first_down_prob"].to_numpy()
    out["wp_succeed"] = go["wp_succeed"].to_numpy()
    out["wp_fail"] = go["wp_fail"].to_numpy()
    out["fg_make_prob"] = fg["fg_make_prob"].to_numpy()
    out["make_fg_wp"] = fg["make_fg_wp"].to_numpy()
    out["miss_fg_wp"] = fg["miss_fg_wp"].to_numpy()
    out["fg_wp"] = fg["fg_wp"].to_numpy()
    out["punt_wp"] = punt["punt_wp"].to_numpy()

    go_wp = out["go_wp"].to_numpy().astype(float)
    fg_wp = out["fg_wp"].to_numpy().astype(float)
    punt_wp = out["punt_wp"].to_numpy().astype(float)

    # nfl4th go_boost: 100 * (go_wp - max(fg_wp, punt_prob)); a NaN punt -> 0.
    punt_prob = np.where(np.isnan(punt_wp), 0.0, punt_wp)
    max_non_go = np.nanmax(np.vstack([fg_wp, punt_prob]), axis=0)
    out["go_boost"] = 100.0 * (go_wp - max_non_go)

    # recommendation: argmax over available options (NaN options excluded).
    option_names = np.array(["go", "field_goal", "punt"], dtype=object)
    stacked = np.vstack([go_wp, fg_wp, punt_wp])
    stacked_for_argmax = np.where(np.isnan(stacked), -np.inf, stacked)
    best_idx = np.argmax(stacked_for_argmax, axis=0)
    best_wp = stacked_for_argmax[best_idx, np.arange(stacked.shape[1])]
    rec = option_names[best_idx]
    rec = np.where(np.isneginf(best_wp), None, rec)
    out["fourth_down_recommendation"] = rec

    best_wp_clean = np.where(np.isneginf(best_wp), np.nan, best_wp)
    out["go_wp_diff"] = go_wp - best_wp_clean
    out["fg_wp_diff"] = fg_wp - best_wp_clean
    out["punt_wp_diff"] = punt_wp - best_wp_clean
    return out
