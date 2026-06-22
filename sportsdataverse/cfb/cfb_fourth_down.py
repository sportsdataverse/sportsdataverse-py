"""Fourth-down decision surface for college football (cfb4th port).

Faithful Python port of `cfb4th <https://github.com/sportsdataverse/cfb4th>`_'s
``add_4th_probs()`` against THIS package's bundled EP (8-feat softprob) and
WP-spread (13-feat logistic) boosters. The surface mirrors the R reference's
three decision paths plus the max-WP recommendation:

* **go** — :func:`get_go_wp` (cfb4th ``get_go_wp``): a 76-class yards-gained
  distribution (``fd_model``) is expanded per play, each outcome's hypothetical
  post-play game state is scored with EP -> WP, and the option value is the
  prob-weighted WP. Emits ``go_wp`` / ``first_down_prob`` / ``wp_succeed`` /
  ``wp_fail``.
* **punt** — :func:`get_punt_wp` (cfb4th ``get_punt_wp``): a bundled punt
  end-yardline distribution (``punt_distribution``) is joined per play, field +
  possession are flipped, and the option value is the prob-weighted WP of the
  receiving team's ensuing drive (from the punting team's perspective). Emits
  ``punt_wp``.
* **field goal** — :func:`get_fg_wp` (cfb4th ``get_fg_wp``): a make-probability
  model (``fg_model``, the cfb4th ``mgcv::bam(result ~ s(yards_to_goal))``
  binomial GAM) weights the made-FG WP (opponent receives a kickoff, +3) against
  the missed-FG WP (opponent takes over at the spot). Emits ``fg_make_prob`` /
  ``make_fg_wp`` / ``miss_fg_wp`` / ``fg_wp``.

The combiner :func:`get_4th_down_probs` adds all of the above plus a
``fourth_down_recommendation`` (the max-WP choice among go/punt/field_goal) and a
per-option ``*_wp_diff`` (option WP minus the recommended option's WP, so the
recommended option's diff is 0 and the others are <= 0).

Feature-contract mapping (cfb4th name -> this package's processed ``plays_json``
column), identical to the reviewed GO-path port in ``cfbfastR-cfb-data``::

    down                          start.down
    distance                      start.distance
    yards_to_goal                 start.yardsToEndzone
    pos_team_spread               start.pos_team_spread
    pos_score_diff_start          pos_score_diff_start
    TimeSecsRem                   start.TimeSecsRem
    adj_TimeSecsRem               start.adj_TimeSecsRem
    pos_team_receives_2H_kickoff  start.pos_team_receives_2H_kickoff
    pos_team_timeouts_rem_before  start.posTeamTimeouts
    def_pos_team_timeouts_rem_before  start.defPosTeamTimeouts
    is_home                       start.is_home
    period                        period
    season                        season
    overUnder                     overUnder
    homeTeamSpread                homeTeamSpread

FG-model availability
---------------------
The cfb4th field-goal make-probability model is an R ``mgcv::bam`` GAM serialized
inside ``cfb4th``'s ``sysdata.rda`` / ``cfbfastR-data/models/fg_model.Rdata``.
It is bundled here as ``cfb/models/fg_model.ubj`` **only when a usable conversion
is present**. When the bundled file is absent, the go + punt surface is fully
computed and the FG columns are emitted as nulls (never fabricated); the
recommendation is taken over whichever options are available. Check
:data:`FG_MODEL_AVAILABLE` to know which mode is active.
"""

from __future__ import annotations

import os
from functools import lru_cache
from pathlib import Path

import numpy as np
import pandas as pd
import polars as pl
from xgboost import Booster, DMatrix

from sportsdataverse.cfb.cfb_pbp import (
    _cfb_resource_filename,
    ep_model as _ep_model,
    wp_model as _wp_model,
)
from sportsdataverse.cfb.model_vars import (
    ep_class_to_score_mapping,
    ep_final_names,
    wp_final_names,
)

__all__ = [
    "get_4th_down_probs",
    "get_go_wp",
    "get_punt_wp",
    "get_fg_wp",
    "FG_MODEL_AVAILABLE",
]

# --- inference contracts (mirror cfbfastR-cfb-data fourth_down constants) ---
FD_FEATURES = ["down", "distance", "yards_to_goal", "posteam_total", "posteam_spread", "era"]
FD_NUM_CLASS = 76  # gain class k -> yards = k - 10, range -10..65
FD_ERA_BOUNDS = (2006, 2013, 2017)  # ordinal CFB rule-era factor cuts

EP_FEATURES = list(ep_final_names)
WP_SPREAD_FEATURES = list(wp_final_names)
_EP_SCORES = np.array(
    [ep_class_to_score_mapping[i] for i in range(len(ep_class_to_score_mapping))],
    dtype=np.float64,
)

# cfb4th state field -> processed plays_json column
_PBP_COLS = {
    "down": "start.down",
    "distance": "start.distance",
    "yards_to_goal": "start.yardsToEndzone",
    "pos_team_spread": "start.pos_team_spread",
    "pos_score_diff_start": "pos_score_diff_start",
    "TimeSecsRem": "start.TimeSecsRem",
    "adj_TimeSecsRem": "start.adj_TimeSecsRem",
    "pos_team_receives_2H_kickoff": "start.pos_team_receives_2H_kickoff",
    "pos_team_timeouts_rem_before": "start.posTeamTimeouts",
    "def_pos_team_timeouts_rem_before": "start.defPosTeamTimeouts",
    "is_home": "start.is_home",
    "period": "period",
    "season": "season",
    "overUnder": "overUnder",
    "homeTeamSpread": "homeTeamSpread",
}

# --- bundled small models ---
_punt_distribution_file = _cfb_resource_filename("sportsdataverse", "cfb/models/punt_distribution.parquet")
punt_distribution = pl.read_parquet(_punt_distribution_file)


# --- fourth-down yards model: download-on-demand (~16 MB, too large to bundle) ---
# Mirrors the NFL xYAC pattern: published to the espn_cfb_model_artifacts release
# and fetched + cached on first use under the CFB model cache dir.
_FD_MODEL_URL = (
    "https://github.com/sportsdataverse/sportsdataverse-data/releases/download/espn_cfb_model_artifacts/fd_model.ubj"
)


def _cfb_model_cache_dir() -> Path:
    """Cache dir for download-on-demand CFB models (override: ``SDV_PY_CFB_MODEL_DIR``)."""
    override = os.environ.get("SDV_PY_CFB_MODEL_DIR")
    return Path(override) if override else Path.home() / ".cache" / "sportsdataverse" / "cfb_models"


def _load_booster(path: Path | str) -> Booster:
    b = Booster({"nthread": 4})
    b.load_model(str(path))
    return b


@lru_cache(maxsize=1)
def _load_fd_model() -> Booster:
    """Load the fourth-down yards model (``fd_model.ubj``), downloading on demand.

    Resolution order: a bundled copy (normally absent — the model is ~16 MB) ->
    the cache dir -> download from the ``espn_cfb_model_artifacts`` release
    (written atomically). Raises :class:`FileNotFoundError` when unobtainable
    (offline + no cache), so callers / tests can skip or degrade gracefully.
    """
    name = "fd_model.ubj"
    bundled = Path(_cfb_resource_filename("sportsdataverse", f"cfb/models/{name}"))
    if bundled.exists():
        return _load_booster(bundled)
    cached = _cfb_model_cache_dir() / name
    if cached.exists():
        return _load_booster(cached)
    try:
        from sportsdataverse.dl_utils import download

        cached.parent.mkdir(parents=True, exist_ok=True)
        content = download(_FD_MODEL_URL, num_retries=5).content
        tmp = cached.with_suffix(cached.suffix + ".tmp")
        with open(tmp, "wb") as fh:
            fh.write(content)
        os.replace(tmp, cached)
        return _load_booster(cached)
    except Exception as exc:  # pragma: no cover - network dependent
        raise FileNotFoundError(
            f"Could not obtain the fourth-down model '{name}' (not bundled, cache miss, "
            f"download from {_FD_MODEL_URL} failed: {exc}). Pre-place it under "
            f"{_cfb_model_cache_dir()} or set SDV_PY_CFB_MODEL_DIR=<dir>."
        ) from exc


# FG make-probability model is optional: bundled cfb/models/fg_model.ubj when a
# usable conversion of the cfb4th GAM is present; otherwise FG columns are null.
fg_model: Booster | None = None
try:
    _fg_model_file = _cfb_resource_filename("sportsdataverse", "cfb/models/fg_model.ubj")
    import os as _os

    if _os.path.exists(_fg_model_file):
        fg_model = Booster({"nthread": 4})
        fg_model.load_model(_fg_model_file)
    else:  # pragma: no cover - depends on bundling
        fg_model = None
except Exception:  # pragma: no cover - defensive
    fg_model = None

FG_MODEL_AVAILABLE: bool = fg_model is not None


# --------------------------------------------------------------------------- #
# state -> EP / WP scorers (shared by go / punt / fg paths)
# --------------------------------------------------------------------------- #
def _to_pandas(df) -> pd.DataFrame:
    """Accept polars or pandas; return a pandas copy with the cfb4th-named state cols."""
    if hasattr(df, "to_pandas"):  # polars.DataFrame
        df = df.to_pandas()
    out = pd.DataFrame(index=range(len(df)))
    for short, src in _PBP_COLS.items():
        out[short] = df[src].to_numpy() if src in df.columns else np.nan
    return out


def _posteam_total(state: pd.DataFrame) -> np.ndarray:
    """(homeTeamSpread + overUnder)/2 if posteam is home else (overUnder - homeTeamSpread)/2."""
    is_home = state["is_home"].to_numpy().astype(bool)
    ou = state["overUnder"].to_numpy().astype(float)
    hs = state["homeTeamSpread"].to_numpy().astype(float)
    return np.where(is_home, (hs + ou) / 2.0, (ou - hs) / 2.0)


def _era(season: np.ndarray) -> np.ndarray:
    lo, mid, hi = FD_ERA_BOUNDS
    out = np.full(len(season), 3, dtype=np.int32)
    out = np.where(season <= hi, 2, out)
    out = np.where(season <= mid, 1, out)
    out = np.where(season <= lo, 0, out)
    return out


def _predict_ep(state: pd.DataFrame) -> np.ndarray:
    """EP for each state row: prep_ep + add_ep, using this package's softprob EP model."""
    down = state["down"].to_numpy().astype(int)
    X = pd.DataFrame(
        {
            "TimeSecsRem": state["TimeSecsRem"].to_numpy().astype(float),
            "yards_to_goal": state["yards_to_goal"].to_numpy().astype(float),
            "distance": state["distance"].to_numpy().astype(float),
            "down_1": (down == 1).astype(int),
            "down_2": (down == 2).astype(int),
            "down_3": (down == 3).astype(int),
            "down_4": (down == 4).astype(int),
            "pos_score_diff_start": state["pos_score_diff_start"].to_numpy().astype(float),
        }
    )[EP_FEATURES]
    probs = _ep_model.predict(DMatrix(X))
    if probs.ndim == 1:
        probs = probs.reshape(-1, len(_EP_SCORES))
    return probs @ _EP_SCORES


def _predict_wp(state: pd.DataFrame, ep: np.ndarray) -> np.ndarray:
    """WP for each state row: prep_wp (ExpScoreDiff/spread_time/...) + add_wp."""
    adj = state["adj_TimeSecsRem"].to_numpy().astype(float)
    pos_diff = state["pos_score_diff_start"].to_numpy().astype(float)
    exp_score_diff = pos_diff + ep
    exp_ratio = exp_score_diff / (adj + 1.0)
    elapsed_share = (3600.0 - adj) / 3600.0
    spread_time = (-1.0 * state["pos_team_spread"].to_numpy().astype(float)) * np.exp(-4.0 * elapsed_share)
    X = pd.DataFrame(
        {
            "pos_team_receives_2H_kickoff": state["pos_team_receives_2H_kickoff"].to_numpy().astype(float),
            "spread_time": spread_time,
            "TimeSecsRem": state["TimeSecsRem"].to_numpy().astype(float),
            "adj_TimeSecsRem": adj,
            "ExpScoreDiff_Time_Ratio": exp_ratio,
            "pos_score_diff_start": pos_diff,
            "down": state["down"].to_numpy().astype(float),
            "distance": state["distance"].to_numpy().astype(float),
            "yards_to_goal": state["yards_to_goal"].to_numpy().astype(float),
            "is_home": state["is_home"].to_numpy().astype(float),
            "pos_team_timeouts_rem_before": state["pos_team_timeouts_rem_before"].to_numpy().astype(float),
            "def_pos_team_timeouts_rem_before": state["def_pos_team_timeouts_rem_before"].to_numpy().astype(float),
            "period": state["period"].to_numpy().astype(float),
        }
    )[WP_SPREAD_FEATURES]
    return _wp_model.predict(DMatrix(X))


def _end_game_clamp(
    wp: np.ndarray,
    pos_diff: np.ndarray,
    adj: np.ndarray,
    period: np.ndarray,
    def_to: np.ndarray,
) -> np.ndarray:
    """cfb4th ``end_game_fn``: leading + late + defense out of timeouts -> WP clamps.

    When the *possessing* team is leading and the defense can no longer stop the
    clock, WP is pinned to the kneel-out outcome. Note this is the WP from the
    perspective of whoever holds the ball in ``state`` (already possession-correct
    for the punt / fg ensuing-drive frames before the final flip back).
    """
    lead = pos_diff > 0
    p4 = period == 4
    wp = np.where(lead & (adj < 120) & p4 & (def_to == 0), 0.0, wp)
    wp = np.where(lead & (adj < 80) & p4 & (def_to == 1), 0.0, wp)
    wp = np.where(lead & (adj < 40) & p4 & (def_to == 2), 0.0, wp)
    return wp


def _flip_team_state(state: pd.DataFrame) -> pd.DataFrame:
    """cfb4th ``flip_team``: hand the ball to the other team, 1st-and-10, 6s runoff.

    Mutates a copy of ``state`` to the possession-flipped frame (timeouts swapped,
    score negated, spread negated, 2H-kickoff indicator toggled in the 1st half,
    is_home toggled, down=1, distance=10). Yard line is NOT set here (each caller
    sets ``yards_to_goal`` to its scenario value).
    """
    s = state.copy()
    period = s["period"].to_numpy().astype(float)
    recv = s["pos_team_receives_2H_kickoff"].to_numpy().astype(float)
    recv = np.where((period <= 2) & (recv == 0), 1.0, np.where((period <= 2) & (recv == 1), 0.0, recv))
    pos_to = s["pos_team_timeouts_rem_before"].to_numpy().astype(float)
    def_to = s["def_pos_team_timeouts_rem_before"].to_numpy().astype(float)
    s["pos_team_timeouts_rem_before"] = def_to
    s["def_pos_team_timeouts_rem_before"] = pos_to
    s["pos_score_diff_start"] = -s["pos_score_diff_start"].to_numpy().astype(float)
    s["pos_team_spread"] = -s["pos_team_spread"].to_numpy().astype(float)
    s["pos_team_receives_2H_kickoff"] = recv
    s["is_home"] = 1.0 - s["is_home"].to_numpy().astype(float)
    s["down"] = 1
    s["distance"] = 10
    s["TimeSecsRem"] = np.maximum(s["TimeSecsRem"].to_numpy().astype(float) - 6.0, 0.0)
    s["adj_TimeSecsRem"] = np.maximum(s["adj_TimeSecsRem"].to_numpy().astype(float) - 6.0, 0.0)
    return s


# --------------------------------------------------------------------------- #
# GO path (faithful port of cfb4th get_go_wp)
# --------------------------------------------------------------------------- #
def get_go_wp(pbp_df) -> pd.DataFrame:
    """Expected win probability of going for it on 4th down (cfb4th ``get_go_wp``).

    Args:
        pbp_df: Play-by-play frame (polars or pandas) of fourth-down situations
            carrying the ``start.*`` state columns in :data:`_PBP_COLS`.

    Returns:
        A pandas copy of ``pbp_df`` plus ``go_wp`` (prob-weighted WP of going for
        it), ``first_down_prob`` (P(conversion)), ``wp_succeed`` (mean WP over
        conversion outcomes) and ``wp_fail`` (mean WP over failure outcomes).
        ``go_wp`` is always in [0, 1]; the conditional columns are in [0, 1] but
        can be NaN for degenerate goal-line plays where one outcome bucket is
        empty (matches the R reference ``pivot_wider`` NA behavior).

    Example:
        Quick start::

            from sportsdataverse.cfb.cfb_fourth_down import get_go_wp
            out = get_go_wp(fourth_down_rows)
            print(out[["go_wp", "first_down_prob"]].head())
    """
    n_plays = len(pbp_df)
    base = (pbp_df.to_pandas() if hasattr(pbp_df, "to_pandas") else pd.DataFrame(pbp_df)).reset_index(drop=True)
    if n_plays == 0:
        out = base.copy()
        for c in ("go_wp", "first_down_prob", "wp_succeed", "wp_fail"):
            out[c] = pd.Series([], dtype=float)
        return out

    st = _to_pandas(pbp_df)

    # step 1: fd_model 76-class yards-gained distribution per play
    fd_X = pd.DataFrame(
        {
            "down": st["down"].to_numpy().astype(float),
            "distance": st["distance"].to_numpy().astype(float),
            "yards_to_goal": st["yards_to_goal"].to_numpy().astype(float),
            "posteam_total": _posteam_total(st),
            "posteam_spread": st["pos_team_spread"].to_numpy().astype(float),
            "era": _era(st["season"].to_numpy().astype(float)),
        }
    )[FD_FEATURES]
    fd_probs = _load_fd_model().predict(DMatrix(fd_X))
    if fd_probs.ndim == 1:
        fd_probs = fd_probs.reshape(n_plays, FD_NUM_CLASS)

    # step 2: expand to long (play x gain)
    gains = np.arange(FD_NUM_CLASS) - 10  # -10..65
    play_idx = np.repeat(np.arange(n_plays), FD_NUM_CLASS)
    gain = np.tile(gains, n_plays).astype(np.int64)
    prob = fd_probs.reshape(-1).astype(np.float64)

    ytg0 = st["yards_to_goal"].to_numpy()[play_idx].astype(np.int64)

    # step 3: cap at TD, floor impossible loss (ball on the 1)
    gain = np.where(gain > ytg0, ytg0, gain)
    gain = np.where(ytg0 - gain >= 100, ytg0 - 99, gain)

    # collapse duplicate (play, gain) rows produced by the cap (combine TD prob mass)
    long = (
        pd.DataFrame({"play_idx": play_idx, "gain": gain, "prob": prob})
        .groupby(["play_idx", "gain"], as_index=False)["prob"]
        .sum()
    )
    play_idx = long["play_idx"].to_numpy()
    gain = long["gain"].to_numpy()
    prob = long["prob"].to_numpy()

    state = st.iloc[play_idx].reset_index(drop=True).copy()

    # step 4: update game situation per outcome
    ytg = state["yards_to_goal"].to_numpy().astype(np.int64) - gain
    turnover = (gain < state["distance"].to_numpy().astype(np.int64)).astype(int)
    state["down"] = 1

    to_mask = turnover == 1
    ytg = np.where(to_mask, 100 - ytg, ytg)

    pos_to = state["pos_team_timeouts_rem_before"].to_numpy().astype(float)
    def_to = state["def_pos_team_timeouts_rem_before"].to_numpy().astype(float)
    new_pos_to = np.where(to_mask, def_to, pos_to)
    new_def_to = np.where(to_mask, pos_to, def_to)

    period = state["period"].to_numpy().astype(float)
    recv = state["pos_team_receives_2H_kickoff"].to_numpy().astype(float)
    recv = np.where((period <= 2) & (recv == 0) & to_mask, 1.0, recv)
    recv = np.where((period <= 2) & (recv == 1) & to_mask, 0.0, recv)

    is_home = state["is_home"].to_numpy().astype(float)
    is_home = np.where(to_mask, 1.0 - is_home, is_home)

    spread = state["pos_team_spread"].to_numpy().astype(float)
    spread = np.where(to_mask, -spread, spread)
    pos_diff = state["pos_score_diff_start"].to_numpy().astype(float)
    pos_diff = np.where(to_mask, -pos_diff, pos_diff)

    # touchdown: ytg hit 0 (after TD cap). Score offense (+6), other team receives
    # kickoff at the 25 (ytg=75) -- same possession-flip bookkeeping again.
    td_mask = ytg == 0
    pos_diff = np.where(td_mask, -pos_diff - 6.0, pos_diff)
    ytg = np.where(td_mask, 75, ytg)
    td_pos_to = np.where(td_mask, new_def_to, new_pos_to)
    td_def_to = np.where(td_mask, new_pos_to, new_def_to)
    new_pos_to, new_def_to = td_pos_to, td_def_to
    recv = np.where((period <= 2) & (recv == 0) & td_mask, 1.0, recv)
    recv = np.where((period <= 2) & (recv == 1) & td_mask, 0.0, recv)
    is_home = np.where(td_mask, 1.0 - is_home, is_home)
    spread = np.where(td_mask, -spread, spread)

    tsr = np.maximum(state["TimeSecsRem"].to_numpy().astype(float) - 6.0, 0.0)
    adj = np.maximum(state["adj_TimeSecsRem"].to_numpy().astype(float) - 6.0, 0.0)
    distance = np.where(ytg < 10, ytg, 10)

    state["yards_to_goal"] = ytg
    state["distance"] = distance
    state["pos_team_timeouts_rem_before"] = new_pos_to
    state["def_pos_team_timeouts_rem_before"] = new_def_to
    state["pos_team_receives_2H_kickoff"] = recv
    state["is_home"] = is_home
    state["pos_team_spread"] = spread
    state["pos_score_diff_start"] = pos_diff
    state["TimeSecsRem"] = tsr
    state["adj_TimeSecsRem"] = adj

    # step 5: EP then WP of each resulting state
    ep = _predict_ep(state)
    wp = _predict_wp(state, ep)

    orig_is_home = st["is_home"].to_numpy().astype(float)[play_idx]
    flipped = is_home != orig_is_home
    wp = np.where(flipped, 1.0 - wp, wp)

    # end_game_fn kneel-out clamps
    succ_alive = (turnover == 0) & (~td_mask) & (ytg > 0) & (pos_diff > 0)
    wp = np.where(succ_alive & (adj < 120) & (new_def_to == 0), 1.0, wp)
    wp = np.where(succ_alive & (adj < 80) & (new_def_to == 1), 1.0, wp)
    wp = np.where(succ_alive & (adj < 40) & (new_def_to == 2), 1.0, wp)
    fail_lead = (turnover == 1) & (pos_diff < 0)
    wp = np.where(fail_lead & (adj < 120) & (new_def_to == 0), 0.0, wp)
    wp = np.where(fail_lead & (adj < 80) & (new_def_to == 1), 0.0, wp)
    wp = np.where(fail_lead & (adj < 40) & (new_def_to == 2), 0.0, wp)

    # step 6: aggregate
    res = pd.DataFrame({"play_idx": play_idx, "turnover": turnover, "prob": prob, "wp": wp})
    res["wt_wp"] = res["prob"] * res["wp"]
    go = res.groupby("play_idx").agg(go_wp=("wt_wp", "sum")).reset_index()

    grp = res.groupby(["play_idx", "turnover"])
    cond = grp.apply(
        lambda g: pd.Series({"pct": g["prob"].sum(), "wp": (g["prob"] * g["wp"]).sum() / g["prob"].sum()}),
        include_groups=False,
    ).reset_index()
    piv = cond.pivot(index="play_idx", columns="turnover")
    pct0 = piv["pct"].get(0)
    wp0 = piv["wp"].get(0)
    wp1 = piv["wp"].get(1)
    report = pd.DataFrame({"play_idx": piv.index})
    report["first_down_prob"] = (pct0 if pct0 is not None else 0.0).to_numpy()
    report["wp_succeed"] = (wp0 if wp0 is not None else np.nan).to_numpy()
    report["wp_fail"] = (wp1 if wp1 is not None else np.nan).to_numpy()

    merged = (
        pd.DataFrame({"play_idx": np.arange(n_plays)})
        .merge(go, on="play_idx", how="left")
        .merge(report, on="play_idx", how="left")
    )
    out = base.copy()
    out["go_wp"] = merged["go_wp"].to_numpy()
    out["first_down_prob"] = merged["first_down_prob"].to_numpy()
    out["wp_succeed"] = merged["wp_succeed"].to_numpy()
    out["wp_fail"] = merged["wp_fail"].to_numpy()
    return out


# --------------------------------------------------------------------------- #
# PUNT path (faithful port of cfb4th get_punt_wp)
# --------------------------------------------------------------------------- #
def get_punt_wp(pbp_df) -> pd.DataFrame:
    """Expected win probability of punting on 4th down (cfb4th ``get_punt_wp``).

    Args:
        pbp_df: Play-by-play frame (polars or pandas) of fourth-down situations.

    Returns:
        A pandas copy of ``pbp_df`` plus ``punt_wp`` (prob-weighted WP of punting,
        from the punting team's perspective). ``punt_wp`` is NaN where the punt
        end-yardline distribution has no support for the play's ``yards_to_goal``
        (e.g. inside the 31, where punting is dominated and the cfb4th table is
        empty -- matching the R reference's left-join NA behavior).
    """
    n_plays = len(pbp_df)
    base = (pbp_df.to_pandas() if hasattr(pbp_df, "to_pandas") else pd.DataFrame(pbp_df)).reset_index(drop=True)
    if n_plays == 0:
        out = base.copy()
        out["punt_wp"] = pd.Series([], dtype=float)
        return out

    st = _to_pandas(pbp_df)
    st["play_idx"] = np.arange(n_plays)

    pdist = punt_distribution.to_pandas()  # yards_to_goal, yards_to_goal_end, pct

    # join the punt end-yardline distribution per play
    long = st.merge(pdist, left_on="yards_to_goal", right_on="yards_to_goal", how="left", suffixes=("", "_dist"))
    has_dist = long["yards_to_goal_end"].notna().to_numpy()

    # plays with no distribution support -> punt_wp NaN (R left-join NA)
    supported = long[has_dist].reset_index(drop=True)
    if len(supported) == 0:
        out = base.copy()
        out["punt_wp"] = np.nan
        return out

    state = supported.copy()
    ytg_end = supported["yards_to_goal_end"].to_numpy().astype(np.int64)
    pct = supported["pct"].to_numpy().astype(float)

    # flip possession to the receiving team's ensuing drive
    flipped_state = _flip_team_state(state[list(_PBP_COLS.keys())])
    # receiving team starts at (100 - yards_to_goal_end); a return TD (end==100)
    # is handled by giving the punting team a kickoff at the 25 (handled via flip-back)
    return_td = ytg_end == 100
    new_ytg = 100 - ytg_end
    # return TD: receiving team scored, punting team now receives kickoff at the 25.
    # net effect on the (already once-flipped) receiving-team frame: flip score by
    # -7 and place ball at 75 from the punting team's perspective. cfb4th sets
    # yards_to_goal = 75 and pos_score_diff_start = -pos_diff - 7 on the punting team.
    new_ytg = np.where(return_td, 75, new_ytg)
    flipped_state["yards_to_goal"] = new_ytg
    # punt distance distribution already excludes muffs; on a return TD undo the
    # one flip so the punting team is the possessing team again (receives kickoff)
    pos_diff_flipped = flipped_state["pos_score_diff_start"].to_numpy().astype(float)
    # return TD: receiving team got 7; from punting-team perspective score diff is
    # -(orig_pos_diff) - 7. orig pos_diff (punting team) = -pos_diff_flipped.
    rtd_pos_diff = -(-pos_diff_flipped) - 7.0
    flipped_state["pos_score_diff_start"] = np.where(return_td, rtd_pos_diff, pos_diff_flipped)
    # on a return TD restore is_home / spread / timeouts / kickoff to the punting team
    orig = state[list(_PBP_COLS.keys())]
    for col in (
        "is_home",
        "pos_team_spread",
        "pos_team_receives_2H_kickoff",
        "pos_team_timeouts_rem_before",
        "def_pos_team_timeouts_rem_before",
    ):
        flipped_state[col] = np.where(
            return_td, orig[col].to_numpy().astype(float), flipped_state[col].to_numpy().astype(float)
        )
    # goal-to-go distance shrink on the new field position
    flipped_state["distance"] = np.where(new_ytg < 10, new_ytg, 10)

    ep = _predict_ep(flipped_state)
    wp = _predict_wp(flipped_state, ep)

    # WP is from whoever possesses in flipped_state; flip back to the punting team
    # unless we already restored possession (return TD).
    new_is_home = flipped_state["is_home"].to_numpy().astype(float)
    orig_is_home = orig["is_home"].to_numpy().astype(float)
    possession_changed = new_is_home != orig_is_home
    wp = np.where(possession_changed, 1.0 - wp, wp)

    # end_game_fn clamp from the punting team's perspective
    wp = _end_game_clamp(
        wp,
        orig["pos_score_diff_start"].to_numpy().astype(float),
        np.maximum(orig["adj_TimeSecsRem"].to_numpy().astype(float) - 6.0, 0.0),
        orig["period"].to_numpy().astype(float),
        flipped_state["pos_team_timeouts_rem_before"].to_numpy().astype(float),
    )

    agg = (
        pd.DataFrame({"play_idx": supported["play_idx"].to_numpy(), "wt_wp": pct * wp})
        .groupby("play_idx", as_index=False)["wt_wp"]
        .sum()
        .rename(columns={"wt_wp": "punt_wp"})
    )
    merged = pd.DataFrame({"play_idx": np.arange(n_plays)}).merge(agg, on="play_idx", how="left")
    out = base.copy()
    out["punt_wp"] = merged["punt_wp"].to_numpy()
    return out


# --------------------------------------------------------------------------- #
# FG path (faithful port of cfb4th get_fg_wp)
# --------------------------------------------------------------------------- #
def _fg_make_prob(st: pd.DataFrame) -> np.ndarray:
    """Make-probability per play from the cfb4th GAM (``fg_model``) + its clamps.

    cfb4th evaluates ``mgcv::bam(result ~ s(yards_to_goal))`` then applies two
    post-clamps: zero out beyond 42 yards-to-goal (>~59 yd kick) and shrink kicks
    at/over 35 yards-to-goal by 0.9 (bot conservatism). Returns NaN when the FG
    model isn't bundled (callers null the FG columns).
    """
    ytg = st["yards_to_goal"].to_numpy().astype(float)
    if fg_model is None:
        return np.full(len(st), np.nan)
    # The bundled fg_model is the converted GAM; predict make-probability on the
    # single yards_to_goal feature, then apply the cfb4th post-clamps.
    X = pd.DataFrame({"yards_to_goal": ytg})
    fnames = fg_model.feature_names or ["yards_to_goal"]
    prob = fg_model.predict(DMatrix(X[fnames] if set(fnames) <= set(X.columns) else X))
    prob = np.where(ytg > 42, 0.0, prob)
    prob = np.where(ytg >= 35, 0.9 * prob, prob)
    return prob


def get_fg_wp(pbp_df) -> pd.DataFrame:
    """Expected win probability of attempting a field goal (cfb4th ``get_fg_wp``).

    Args:
        pbp_df: Play-by-play frame (polars or pandas) of fourth-down situations.

    Returns:
        A pandas copy of ``pbp_df`` plus ``fg_make_prob``, ``make_fg_wp``,
        ``miss_fg_wp`` and ``fg_wp`` (= make_prob*make_wp + (1-make_prob)*miss_wp,
        from the kicking team's perspective). All four are NaN when the FG model
        is not bundled (:data:`FG_MODEL_AVAILABLE` is False) -- probabilities are
        never fabricated.
    """
    n_plays = len(pbp_df)
    base = (pbp_df.to_pandas() if hasattr(pbp_df, "to_pandas") else pd.DataFrame(pbp_df)).reset_index(drop=True)
    cols = ("fg_make_prob", "make_fg_wp", "miss_fg_wp", "fg_wp")
    if n_plays == 0:
        out = base.copy()
        for c in cols:
            out[c] = pd.Series([], dtype=float)
        return out

    st = _to_pandas(pbp_df)
    make_prob = _fg_make_prob(st)

    if fg_model is None:
        out = base.copy()
        for c in cols:
            out[c] = np.nan
        return out

    orig = st[list(_PBP_COLS.keys())]
    orig_is_home = orig["is_home"].to_numpy().astype(float)

    # made FG: kicking team kicks off, opponent receives touchback (ytg=75) and
    # the kicking team is now +3 (so the receiving team's pos_score_diff -3).
    make_state = _flip_team_state(orig)
    make_state["yards_to_goal"] = 75
    make_state["distance"] = 10
    make_state["pos_score_diff_start"] = make_state["pos_score_diff_start"].to_numpy().astype(float) - 3.0
    ep_make = _predict_ep(make_state)
    wp_make = _predict_wp(make_state, ep_make)
    make_changed = make_state["is_home"].to_numpy().astype(float) != orig_is_home
    wp_make = np.where(make_changed, 1.0 - wp_make, wp_make)
    wp_make = _end_game_clamp(
        wp_make,
        orig["pos_score_diff_start"].to_numpy().astype(float),
        np.maximum(orig["adj_TimeSecsRem"].to_numpy().astype(float) - 6.0, 0.0),
        orig["period"].to_numpy().astype(float),
        make_state["pos_team_timeouts_rem_before"].to_numpy().astype(float),
    )

    # missed FG: opponent takes over at the spot (ytg = 100 - kicking ytg, capped 80).
    miss_state = _flip_team_state(orig)
    miss_ytg = 100 - orig["yards_to_goal"].to_numpy().astype(float)
    miss_ytg = np.where(miss_ytg > 80, 80, miss_ytg)
    miss_ytg = np.where(miss_ytg < 1, 1, miss_ytg)
    miss_state["yards_to_goal"] = miss_ytg
    miss_state["distance"] = np.where(miss_ytg < 10, miss_ytg, 10)
    ep_miss = _predict_ep(miss_state)
    wp_miss = _predict_wp(miss_state, ep_miss)
    miss_changed = miss_state["is_home"].to_numpy().astype(float) != orig_is_home
    wp_miss = np.where(miss_changed, 1.0 - wp_miss, wp_miss)
    wp_miss = _end_game_clamp(
        wp_miss,
        orig["pos_score_diff_start"].to_numpy().astype(float),
        np.maximum(orig["adj_TimeSecsRem"].to_numpy().astype(float) - 6.0, 0.0),
        orig["period"].to_numpy().astype(float),
        miss_state["pos_team_timeouts_rem_before"].to_numpy().astype(float),
    )

    fg_wp = make_prob * wp_make + (1.0 - make_prob) * wp_miss
    out = base.copy()
    out["fg_make_prob"] = make_prob
    out["make_fg_wp"] = wp_make
    out["miss_fg_wp"] = wp_miss
    out["fg_wp"] = fg_wp
    return out


# --------------------------------------------------------------------------- #
# combiner (faithful port of cfb4th add_4th_probs) + recommendation
# --------------------------------------------------------------------------- #
def get_4th_down_probs(pbp_df) -> pd.DataFrame:
    """Full 4th-down decision surface (cfb4th ``add_4th_probs``) + recommendation.

    Runs :func:`get_go_wp`, :func:`get_fg_wp`, :func:`get_punt_wp` on the
    fourth-down rows and adds the combined option columns plus:

    * ``fourth_down_recommendation`` -- the max-WP choice among ``{go, punt,
      field_goal}`` (NaN options are excluded; when the FG model isn't bundled,
      ``field_goal`` is excluded from the comparison).
    * ``go_wp_diff`` / ``punt_wp_diff`` / ``fg_wp_diff`` -- each option's WP minus
      the recommended option's WP (the recommended option's diff is 0, the others
      <= 0). NaN where the option WP is NaN.
    * ``go_boost`` -- cfb4th's headline number: ``100 * (go_wp - max(fg_wp,
      punt_wp))`` in percentage points.

    Args:
        pbp_df: Play-by-play frame (polars or pandas) of fourth-down situations
            carrying the ``start.*`` state columns in :data:`_PBP_COLS`.

    Returns:
        A pandas copy of ``pbp_df`` with the decision columns added. Empty input
        returns the input plus empty decision columns.

    Example:
        Quick start::

            from sportsdataverse.cfb.cfb_fourth_down import get_4th_down_probs
            out = get_4th_down_probs(fourth_down_rows)
            print(out[["go_wp", "punt_wp", "fg_wp", "fourth_down_recommendation"]].head())
    """
    base = (pbp_df.to_pandas() if hasattr(pbp_df, "to_pandas") else pd.DataFrame(pbp_df)).reset_index(drop=True)
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

    go = get_go_wp(pbp_df)
    fg = get_fg_wp(pbp_df)
    punt = get_punt_wp(pbp_df)

    out = base.copy()
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

    # cfb4th go_boost: 100 * (go_wp - max(fg_wp, punt_prob)), punt NaN -> 0
    punt_prob = np.where(np.isnan(punt_wp), 0.0, punt_wp)
    max_non_go = np.nanmax(np.vstack([fg_wp, punt_prob]), axis=0)
    out["go_boost"] = 100.0 * (go_wp - max_non_go)

    # recommendation: argmax over available options
    option_names = np.array(["go", "field_goal", "punt"])
    stacked = np.vstack([go_wp, fg_wp, punt_wp])  # 3 x n
    # NaN options can't be chosen
    stacked_for_argmax = np.where(np.isnan(stacked), -np.inf, stacked)
    best_idx = np.argmax(stacked_for_argmax, axis=0)
    best_wp = stacked_for_argmax[best_idx, np.arange(stacked.shape[1])]
    rec = option_names[best_idx]
    rec = np.where(np.isneginf(best_wp), None, rec)  # no option available
    out["fourth_down_recommendation"] = rec

    best_wp_clean = np.where(np.isneginf(best_wp), np.nan, best_wp)
    out["go_wp_diff"] = go_wp - best_wp_clean
    out["fg_wp_diff"] = fg_wp - best_wp_clean
    out["punt_wp_diff"] = punt_wp - best_wp_clean
    return out
