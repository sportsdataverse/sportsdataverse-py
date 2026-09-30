"""Overtime-aware win probability for college football.

The regulation win-probability boosters (``wp_spread`` / ``wp_naive``) were trained
on a frame that drops every game that reached overtime (cfbfastR-cfb-data
``model_training/ingest.py::clean_plays``). What they estimate is therefore
P(win | state, game settled in regulation):

* a tied game late in the fourth quarter is learned only from games somebody won in
  regulation, so the team with the ball is overrated (2022-25 holdout, tied with two
  minutes or less left: 0.77 predicted against 0.64 won);
* an overtime state was never seen at all. The clock reads 0, so it scores as the
  last snap of a tied regulation game that someone won (tied overtime: 0.94
  predicted against 0.54 won).

Two corrections turn that into P(win | state), both fitted by
``tools/fit_cfb_wp_overtime.py`` and read from ``models/wp_ot_reach.card.json``:

* **Regulation:** ``(1 - q) * wp + q * tie_value``, where ``q`` is P(the game reaches
  overtime | state) (``wp_ot_reach.ubj``) and ``tie_value`` is P(win | overtime
  reached), a logistic in the pregame spread.
* **Overtime:** the booster is not used. The possession is rolled through the
  overtime rules: the drive ends in a touchdown, a field goal or nothing (a
  multinomial logistic on yards to goal, down and distance), the team that had the
  ball first is answered by the other from the 25, and a tie after both possessions
  is worth ``tie_value`` again. Who had the ball first in the period matters (the
  same tie is a coin flip for the first team and a near-win for the second), so
  callers pass it when they know it; otherwise a non-zero margin implies the second
  possession and a tie implies the first.

``ponytail:`` the overtime rules are those of the first overtime periods (a kicked
extra point after a touchdown, no two-point shootout). Since 2019/2021 later periods
force two-point tries; model them when overtime decisions past the second period
matter.
"""

from __future__ import annotations

from typing import Any, Optional

import numpy as np
import numpy.typing as npt
import pandas as pd
from xgboost import Booster, DMatrix

from sportsdataverse._xgb import xgb_threads as _xgb_threads
from sportsdataverse.cfb.model_cards import _MODEL_DIR, card_features, load_model_card

__all__ = [
    "Q_FEATURES",
    "adjust_wp",
    "infer_second_possession",
    "ot_live_wp",
    "ot_possession_over_wp",
    "ot_touchdown_wp",
    "reach_overtime_prob",
    "regulation_final_wp",
    "tie_value",
]

Q_FEATURES = card_features("wp_ot_reach")
_OT = load_model_card("wp_ot_reach")["overtime"]
_SPREAD_SLOPE = float(_OT["spread_slope"])
_P_XP = float(_OT["p_extra_point"])
_P_2PT = float(_OT["p_two_point"])
_DRIVE_COEF = np.asarray(_OT["drive_model"]["coef"], dtype=float)
_DRIVE_INTERCEPT = np.asarray(_OT["drive_model"]["intercept"], dtype=float)

q_model = Booster({"nthread": _xgb_threads()})
q_model.load_model(str(_MODEL_DIR / "wp_ot_reach.ubj"))


def _f(x: npt.ArrayLike) -> np.ndarray:
    return np.asarray(x, dtype=float)


def tie_value(spread: Optional[npt.ArrayLike] = None, n: Optional[int] = None) -> np.ndarray:
    """P(win | the game reaches overtime) for the team whose pregame spread is given.

    ``None`` (the spread-free model) or a null spread is a coin flip.
    """
    if spread is None:
        return np.full(n or 0, 0.5)
    s = np.nan_to_num(_f(spread), nan=0.0)
    return np.asarray(1.0 / (1.0 + np.exp(-_SPREAD_SLOPE * s)), dtype=float)


def reach_overtime_prob(X: pd.DataFrame) -> np.ndarray:
    """P(the game reaches overtime | regulation state), on a WP feature frame."""
    return np.asarray(q_model.predict(DMatrix(X[Q_FEATURES].astype(float))), dtype=float)


def infer_second_possession(second: Optional[npt.ArrayLike], diff: npt.ArrayLike) -> np.ndarray:
    """Whether the team with the ball is second in its overtime period.

    Known values pass through; unknown ones (null, or no column at all) fall back
    on the margin: only the second team can be behind or ahead at the snap.
    """
    fallback = _f(diff) != 0
    if second is None:
        return fallback
    s = pd.to_numeric(pd.Series(np.asarray(second, dtype=object)), errors="coerce").to_numpy(dtype=float)
    return np.asarray(np.where(np.isnan(s), fallback, s > 0), dtype=bool)


def _drive_probs(ytg: npt.ArrayLike, down: npt.ArrayLike, distance: npt.ArrayLike) -> np.ndarray:
    """(n, 3) P(touchdown, field goal, no score) for the current overtime drive."""
    y, dn, dist = np.broadcast_arrays(_f(ytg), _f(down), _f(distance))
    y = np.clip(y, 1, 99) / 25.0
    ld = np.log1p(np.clip(dist, 1, 99))
    X = np.column_stack([y, y**2, dn == 2, dn == 3, dn == 4, ld, (dn == 4) * ld]).astype(float)
    z = X @ _DRIVE_COEF.T + _DRIVE_INTERCEPT
    z = np.exp(z - z.max(axis=1, keepdims=True))
    return np.asarray(z / z.sum(axis=1, keepdims=True), dtype=float)


_P25 = _drive_probs([25], [1], [10])[0]


def _g(margin: np.ndarray, tie: Any) -> np.ndarray:
    """Win probability once both teams have had the ball: ahead, behind, or level."""
    return np.asarray((margin > 0).astype(float) + tie * (margin == 0), dtype=float)


def _after_td(margin: np.ndarray, tie: Any) -> np.ndarray:
    """Second team's touchdown at ``margin`` (six counted): the better of kick and go for two."""
    kick = _P_XP * _g(margin + 1, tie) + (1 - _P_XP) * _g(margin, tie)
    two = _P_2PT * _g(margin + 2, tie) + (1 - _P_2PT) * _g(margin, tie)
    return np.asarray(np.maximum(kick, two), dtype=float)


def _answer(need: np.ndarray, tie_other: Any) -> np.ndarray:
    """The other team's win probability, second in the period, from its 25 at ``need``."""
    return np.asarray(
        _P25[0] * _after_td(need + 6, tie_other) + _P25[1] * _g(need + 3, tie_other) + _P25[2] * _g(need, tie_other),
        dtype=float,
    )


def ot_possession_over_wp(margin: npt.ArrayLike, tie: npt.ArrayLike, second: npt.ArrayLike) -> np.ndarray:
    """Win probability when the possession has just ended at ``margin`` (points counted).

    The second team's possession ends the period; the first team's hands the other
    team its turn from the 25.
    """
    m, t = _f(margin), _f(tie)
    return np.asarray(np.where(second, _g(m, t), 1.0 - _answer(-m, 1.0 - t)), dtype=float)


def ot_touchdown_wp(margin: npt.ArrayLike, tie: npt.ArrayLike, second: npt.ArrayLike) -> np.ndarray:
    """Win probability right after a touchdown, ``margin`` being the margin before it."""
    m, t = _f(margin), _f(tie)
    first = _P_XP * (1.0 - _answer(-(m + 7), 1.0 - t)) + (1 - _P_XP) * (1.0 - _answer(-(m + 6), 1.0 - t))
    return np.asarray(np.where(second, _after_td(m + 6, t), first), dtype=float)


def ot_live_wp(
    margin: npt.ArrayLike,
    down: npt.ArrayLike,
    distance: npt.ArrayLike,
    ytg: npt.ArrayLike,
    tie: npt.ArrayLike,
    second: npt.ArrayLike,
) -> np.ndarray:
    """Win probability of the team with the ball at a live overtime snap."""
    m = _f(margin)
    p = _drive_probs(ytg, down, distance)
    return np.asarray(
        p[:, 0] * ot_touchdown_wp(m, tie, second)
        + p[:, 1] * ot_possession_over_wp(m + 3, tie, second)
        + p[:, 2] * ot_possession_over_wp(m, tie, second),
        dtype=float,
    )


def regulation_final_wp(
    wp: npt.ArrayLike, margin: npt.ArrayLike, adj_secs: npt.ArrayLike, period: npt.ArrayLike, tie: npt.ArrayLike
) -> np.ndarray:
    """A state with no regulation time left (after the play) is decided: win, loss, or overtime."""
    m = _f(margin)
    final = (_f(period) == 4) & (_f(adj_secs) <= 0)
    return np.asarray(np.where(final, np.where(m > 0, 1.0, np.where(m < 0, 0.0, _f(tie))), _f(wp)), dtype=float)


def adjust_wp(wp: npt.ArrayLike, X: Any, second: Optional[npt.ArrayLike] = None) -> np.ndarray:
    """Turn a regulation booster's output into P(win | state).

    Args:
        wp: The booster's prediction on ``X`` (the team with the ball).
        X: The WP feature frame the booster scored (``wp_final_names`` or the naive
            set). The pregame spread is recovered from ``spread_time`` when the frame
            has it; the spread-free frame uses a coin-flip overtime.
        second: Optional per-row "second possession of the overtime period" flags.

    Returns:
        Regulation rows mixed with the overtime they may reach; overtime rows valued
        by the overtime rules.
    """
    if hasattr(X, "to_pandas"):  # polars
        X = X.to_pandas()
    w = _f(wp)
    period = _f(X["period"])
    adj = _f(X["adj_TimeSecsRem"])
    if "spread_time" in X.columns:
        # spread_time = pos_team_spread * exp(-4 * (3600 - adj) / 3600) (cfb_pbp __add_spread_time)
        tie = tie_value(_f(X["spread_time"]) * np.exp(4.0 * np.clip((3600.0 - adj) / 3600.0, 0.0, None)))
    else:
        tie = tie_value(None, n=len(X))
    q = reach_overtime_prob(X)
    out = (1.0 - q) * w + q * tie
    ot = period >= 5
    if ot.any():
        margin = _f(X["pos_score_diff_start"])
        sec = infer_second_possession(second, margin)
        live = ot_live_wp(margin, X["down"], X["distance"], X["yards_to_goal"], tie, sec)
        out = np.where(ot, live, out)
    return np.asarray(out, dtype=float)
