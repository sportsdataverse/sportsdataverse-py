"""Two-point-conversion decision surface for college football (cfb4th port).

Faithful Python port of `cfb4th <https://github.com/sportsdataverse/cfb4th>`_'s
``get_2pt_wp()`` against THIS package's bundled EP (8-feat softprob) and
WP-spread (13-feat logistic) boosters plus a new bundled CFB two-point model.
Treats each row as "the scoring team just made a touchdown; decide between
kicking the extra point and going for two".

The logic, per play, enumerates the three possible point outcomes of the
try -- ``pts in {0, 1, 2}`` -- growing the scoring team's lead by ``pts``
(``pos_score_diff_start += pts``), flips to the OPPONENT's ensuing drive (the
scoring team kicks off, the opponent receives a touchback at the 25 -> 1st-&-10,
``yards_to_goal = 75``), scores EP -> WP, and flips the WP back to the scoring
team's perspective. (cfb4th writes ``-= pts`` because it adjusts the
already-flipped opponent frame; here we adjust the scoring-team frame pre-flip,
so the sign is ``+= pts`` -- see ``_wp_after_pts``.) The two option values are then::

    two_pt_wp = prob_2pt * wp(pts=2) + (1 - prob_2pt) * wp(pts=0)
    xp_wp     = prob_xp  * wp(pts=1) + (1 - prob_xp)  * wp(pts=0)

and the recommendation is ``go_for_2`` iff ``two_pt_wp > xp_wp`` (else
``kick_xp``).

Probability sources
-------------------
* ``prob_2pt`` -- a real bundled CFB two-point model (``two_pt_model.ubj``, a
  ``binary:logistic`` booster). Its four features, **in order**, are
  ``posteam_spread, posteam_total, pos_score_diff, era``. cfb4th hardcodes
  ``prob_2pt = 0.45``; we use the model instead.
* ``prob_xp`` -- the empirical CFB extra-point make rate
  :data:`_XP_MAKE_PROB` (0.9851). cfb4th derives the XP probability from its
  field-goal GAM evaluated at a 2-yard kick, but the empirical CFB rate is more
  accurate for college football, where XP success is near-constant.

The opponent-ensuing-drive frame after the score reuses the reviewed 4th-down
state machinery in :mod:`sportsdataverse.cfb.cfb_fourth_down`
(``_to_pandas`` / ``_predict_ep`` / ``_predict_wp`` / ``_flip_team_state`` /
``_PBP_COLS``) -- exactly the made-field-goal kickoff frame
(``yards_to_goal = 75``, ``down = 1``, ``distance = 10``).
"""

from __future__ import annotations

import os
from typing import Any

import numpy as np
import pandas as pd
from xgboost import Booster, DMatrix

from sportsdataverse.cfb.cfb_fourth_down import (
    FD_ERA_BOUNDS,
    _PBP_COLS,
    _flip_team_state,
    _posteam_total,
    _predict_ep,
    _predict_wp,
    _to_pandas,
)
from sportsdataverse.cfb.cfb_pbp import _cfb_resource_filename

__all__ = [
    "get_2pt_probs",
    "TWO_PT_MODEL_AVAILABLE",
    "TWO_PT_FEATURES",
]

# Two-point model feature contract (order matters -- matches the bundled booster).
TWO_PT_FEATURES = ["posteam_spread", "posteam_total", "pos_score_diff", "era"]

# Empirical CFB extra-point make rate. cfb4th derives prob_xp from its FG GAM
# evaluated at a 2-yard kick; the empirical CFB rate is more accurate for college,
# where XP success is near-constant. Documented as the XP-probability source.
_XP_MAKE_PROB: float = 0.9851

_DECISION_COLS = ["two_pt_wp", "xp_wp", "prob_2pt", "two_pt_recommendation", "two_pt_wp_diff"]


# --- bundled two-point model (binary:logistic, 4 features) ---
two_pt_model: Booster | None = None
try:
    _two_pt_model_file = _cfb_resource_filename("sportsdataverse", "cfb/models/two_pt_model.ubj")
    if os.path.exists(_two_pt_model_file):
        two_pt_model = Booster({"nthread": 4})
        two_pt_model.load_model(_two_pt_model_file)
    else:  # pragma: no cover - depends on bundling
        two_pt_model = None
except Exception:  # pragma: no cover - defensive
    two_pt_model = None

TWO_PT_MODEL_AVAILABLE: bool = two_pt_model is not None


def _era(season: np.ndarray) -> np.ndarray:
    """Ordinal CFB rule-era factor from season (<=2006->0, <=2013->1, <=2017->2, else 3)."""
    lo, mid, hi = FD_ERA_BOUNDS
    out = np.full(len(season), 3, dtype=np.int32)
    out = np.where(season <= hi, 2, out)
    out = np.where(season <= mid, 1, out)
    out = np.where(season <= lo, 0, out)
    return out


def _prob_2pt(st: pd.DataFrame) -> np.ndarray:
    """P(convert two-point try) per play from the bundled CFB two-point model.

    Features (in order): ``posteam_spread, posteam_total, pos_score_diff, era``.
    Returns NaN when the model isn't bundled (callers null the decision columns).
    """
    if two_pt_model is None:
        return np.full(len(st), np.nan)
    X = pd.DataFrame(
        {
            "posteam_spread": st["pos_team_spread"].to_numpy().astype(float),
            "posteam_total": _posteam_total(st),
            "pos_score_diff": st["pos_score_diff_start"].to_numpy().astype(float),
            "era": _era(st["season"].to_numpy().astype(float)),
        }
    )[TWO_PT_FEATURES]
    return two_pt_model.predict(DMatrix(X)).astype(float)


def _wp_after_pts(st: pd.DataFrame, pts: int) -> np.ndarray:
    """WP (scoring team's perspective) of the opponent's ensuing drive after ``pts``.

    The scoring team has just made the try worth ``pts`` (so their lead grows by
    ``pts``); the ball goes to the opponent at the 25 after a touchback
    (``yards_to_goal = 75``, ``down = 1``, ``distance = 10``), EP -> WP is scored,
    and the WP is flipped back to the scoring team.

    NOTE on the sign: ``st`` here is the SCORING team's frame, and
    ``_flip_team_state`` negates ``pos_score_diff_start`` when handing off, so we ADD
    ``pts`` to the scoring team's lead first -> after the flip the opponent correctly
    faces ``-(lead + pts)``. cfb4th's ``get_2pt_wp`` writes ``- pts`` because it
    operates on the ALREADY-flipped (opponent) frame ("this switch was all handled in
    get_go_wp()"); applying that ``- pts`` here, pre-flip, would invert the try value.
    """
    s = st.copy()
    s["pos_score_diff_start"] = s["pos_score_diff_start"].to_numpy().astype(float) + float(pts)
    flipped = _flip_team_state(s[list(_PBP_COLS.keys())])
    flipped["yards_to_goal"] = 75
    flipped["distance"] = 10
    ep = _predict_ep(flipped)
    wp = _predict_wp(flipped, ep)
    # _flip_team_state always toggles is_home, so possession always changed here;
    # flip the WP back to the scoring (originally possessing) team.
    orig_is_home = s["is_home"].to_numpy().astype(float)
    new_is_home = flipped["is_home"].to_numpy().astype(float)
    return np.where(new_is_home != orig_is_home, 1.0 - wp, wp)


def get_2pt_probs(pbp_df: Any) -> pd.DataFrame:
    """Two-point-conversion decision surface (cfb4th ``get_2pt_wp``).

    Treats each row as "the scoring team just made a touchdown; decide between
    the extra point and going for two". Enumerates the three point outcomes
    (``0`` / ``1`` / ``2``) of the try, scores the opponent's ensuing-drive WP for
    each from the scoring team's perspective, and combines them with the
    two-point conversion probability (bundled CFB model) and the empirical CFB
    extra-point make rate (:data:`_XP_MAKE_PROB`).

    Args:
        pbp_df: Play-by-play frame (polars or pandas) carrying the ``start.*``
            state columns in
            :data:`sportsdataverse.cfb.cfb_fourth_down._PBP_COLS`.

    Returns:
        A pandas copy of ``pbp_df`` plus:

        * ``two_pt_wp`` -- ``prob_2pt * wp(pts=2) + (1 - prob_2pt) * wp(pts=0)``.
        * ``xp_wp`` -- ``prob_xp * wp(pts=1) + (1 - prob_xp) * wp(pts=0)`` with
          ``prob_xp = _XP_MAKE_PROB``.
        * ``prob_2pt`` -- the bundled-model two-point conversion probability.
        * ``two_pt_recommendation`` -- ``"go_for_2"`` iff ``two_pt_wp > xp_wp``
          else ``"kick_xp"`` (None where the inputs are NaN).
        * ``two_pt_wp_diff`` -- ``two_pt_wp - xp_wp`` (positive => go for 2).

        When the two-point model isn't bundled
        (:data:`TWO_PT_MODEL_AVAILABLE` is False) or the required state columns
        are missing, all decision columns are null -- probabilities are never
        fabricated.

    Example:
        Quick start::

            from sportsdataverse.cfb.cfb_two_point import get_2pt_probs
            out = get_2pt_probs(touchdown_rows)
            print(out[["two_pt_wp", "xp_wp", "two_pt_recommendation"]].head())

        See Also:
            * `cfb4th <https://github.com/sportsdataverse/cfb4th>`_ -- R 4th-down / 2pt decision model
    """
    base = (pbp_df.to_pandas() if hasattr(pbp_df, "to_pandas") else pd.DataFrame(pbp_df)).reset_index(drop=True)
    n_plays = len(base)

    def _null_out() -> pd.DataFrame:
        out = base.copy()
        for c in _DECISION_COLS:
            out[c] = pd.Series([np.nan] * n_plays, dtype=object if c == "two_pt_recommendation" else float)
        if n_plays:
            out["two_pt_recommendation"] = None
        return out

    if n_plays == 0:
        out = base.copy()
        for c in _DECISION_COLS:
            out[c] = pd.Series([], dtype=object if c == "two_pt_recommendation" else float)
        return out

    # guard: required state columns must be present (mirrors other surfaces)
    required = set(_PBP_COLS.values())
    if not required.issubset(set(base.columns)) or two_pt_model is None:
        return _null_out()

    st = _to_pandas(pbp_df)

    wp0 = _wp_after_pts(st, 0)
    wp1 = _wp_after_pts(st, 1)
    wp2 = _wp_after_pts(st, 2)

    prob_2pt = _prob_2pt(st)
    prob_xp = _XP_MAKE_PROB

    two_pt_wp = prob_2pt * wp2 + (1.0 - prob_2pt) * wp0
    xp_wp = prob_xp * wp1 + (1.0 - prob_xp) * wp0
    two_pt_wp_diff = two_pt_wp - xp_wp

    rec = np.where(two_pt_wp > xp_wp, "go_for_2", "kick_xp")
    rec = np.where(np.isnan(two_pt_wp_diff), None, rec)

    out = base.copy()
    out["two_pt_wp"] = two_pt_wp
    out["xp_wp"] = xp_wp
    out["prob_2pt"] = prob_2pt
    out["two_pt_recommendation"] = rec
    out["two_pt_wp_diff"] = two_pt_wp_diff
    return out
