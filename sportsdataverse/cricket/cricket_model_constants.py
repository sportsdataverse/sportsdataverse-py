"""Cricket model constants + shared calibration metrics (T7.3).

This module is the single home of:

* the **format table** (:data:`FORMAT_TABLE` / :func:`get_format`) — per-format
  ball budgets and fitted par/sigma constants that parameterise the in-play
  win-probability and WPA models (``cricket_win_prob`` / ``cricket_wpa``);
* the **ODI regime table** (:data:`ODI_REGIMES` / :func:`cricket_regime`) — the
  fielding-restriction regimes a match date falls in, which key the per-regime
  resource surface and the per-regime constants in :data:`REGIME_CONSTANTS`; and
* the **calibration metrics** used by every cricket model gate.

The four league-agnostic metrics (:func:`brier_score`, :func:`log_loss_score`,
:func:`mae`, :func:`calibration_table`) are re-exported verbatim from
:mod:`sportsdataverse._common.metrics` so the identical implementation lives
exactly once; :func:`auc_score` is cricket-local (the shared module does not
ship it).

**League-agnostic algorithm, league-specific constants.** No format number is
hard-coded inside a model function — it is read from :data:`FORMAT_TABLE` via
:func:`get_format`. Test cricket is intentionally deferred, so ``get_format``
raises :class:`ValueError` for ``"test"`` (and any other unknown format).
"""

from __future__ import annotations

import datetime as dt
from dataclasses import dataclass, replace
from typing import Optional

import numpy as np

# Re-export the shared metrics (redundant-alias re-export; identical impl lives once).
from sportsdataverse._common.metrics import (
    brier_score as brier_score,
)
from sportsdataverse._common.metrics import (
    calibration_table as calibration_table,
)
from sportsdataverse._common.metrics import (
    log_loss_score as log_loss_score,
)
from sportsdataverse._common.metrics import (
    mae as mae,
)

__all__ = [
    "FORMAT_TABLE",
    "ODI_REGIMES",
    "POOLED_REGIME",
    "REGIME_CONSTANTS",
    "T20_REGIME",
    "FormatConstants",
    "auc_score",
    "brier_score",
    "calibration_table",
    "cricket_regime",
    "get_format",
    "log_loss_score",
    "mae",
]


def auc_score(y_true: np.ndarray, p_pred: np.ndarray) -> float:
    """Area under the ROC curve via the rank (Mann-Whitney U) identity.

    Computed without scikit-learn so the metric has no heavy import cost and a
    well-defined value on degenerate single-class inputs.

    Args:
        y_true: Array of binary outcomes (0/1).
        p_pred: Array of predicted scores/probabilities (higher = more likely 1).

    Returns:
        The AUC in ``[0, 1]``; ``0.5`` when either class is empty (undefined
        ranking), matching a no-skill baseline.

    Example:
        Quick start::

            import numpy as np
            from sportsdataverse.cricket.cricket_model_constants import auc_score
            auc_score(np.array([0, 0, 1, 1]), np.array([0.1, 0.2, 0.8, 0.9]))
    """
    y = np.asarray(y_true, dtype=float)
    p = np.asarray(p_pred, dtype=float)
    n_pos = float(np.sum(y == 1.0))
    n_neg = float(np.sum(y == 0.0))
    if n_pos == 0.0 or n_neg == 0.0:
        return 0.5
    # Tie-averaged ranks so tied scores each contribute 0.5 (Mann-Whitney U identity).
    _, inv, counts = np.unique(p, return_inverse=True, return_counts=True)
    cum: np.ndarray = np.cumsum(counts)
    start: np.ndarray = cum - counts
    avg_rank_by_group: np.ndarray = (start + cum + 1.0) / 2.0
    ranks: np.ndarray = avg_rank_by_group[inv]
    sum_ranks_pos = float(np.sum(ranks[y == 1.0]))
    return (sum_ranks_pos - n_pos * (n_pos + 1.0) / 2.0) / (n_pos * n_neg)


@dataclass(frozen=True)
class FormatConstants:
    """Per-format constants for the cricket win-probability / WPA models.

    Attributes:
        name: Canonical format slug (``"t20"`` / ``"odi"``).
        balls_total: Legal balls in a full innings (``120`` T20, ``300`` ODI).
        max_wickets: Wickets that end an innings (``10``).
        par_score: Fitted mean first-innings total for the format (the
            "setting" benchmark that centres the setting win-prob).
        sigma_set: Fitted probit scale for the first-innings ("setting") raw
            win-prob ``Phi((proj_final - par_score) / sigma_set)``.
        sigma_chase: Fitted probit scale for the second-innings ("chase") raw
            win-prob ``Phi((proj_final - target) / sigma_chase)``.
        resource_surface_path: Bundled resource-surface parquet resource name
            (loaded via ``importlib.resources`` from ``cricket.models``).
        calibration_path: Bundled isotonic-calibration lookup parquet resource
            name that recalibrates the parametric raw win-prob to the empirical
            win rate (per ``(fmt, phase)``).
    """

    name: str
    balls_total: int
    max_wickets: int
    par_score: float
    sigma_set: float
    sigma_chase: float
    resource_surface_path: str = "cricket_resource_surface.parquet"
    calibration_path: str = "cricket_winprob_calibration.parquet"


# par_score / sigma_* are FITTED on the real Cricsheet corpus (male T20I + ODI,
# 2002-2026; refit 2026-10-10, 3,581 T20I + 2,589 ODI) by tools/models/league_ports/fit_cricket_resource_surface.py. Re-running that
# script rewrites both this block and the bundled parquet artifacts; keep them in
# sync. balls_total / max_wickets are rule-fixed, not fitted.
FORMAT_TABLE: dict[str, FormatConstants] = {
    "t20": FormatConstants(
        name="t20",
        balls_total=120,
        max_wickets=10,
        par_score=149.5,
        sigma_set=42.0,
        sigma_chase=33.0,
    ),
    "odi": FormatConstants(
        name="odi",
        balls_total=300,
        max_wickets=10,
        par_score=249.3,
        sigma_set=76.0,
        sigma_chase=59.0,
    ),
}


#: The pooled 2002-2026 corpus: the regime of a state with no match date, and the surface a
#: regime whose own grid is thin falls back to (``cricket_win_prob.MIN_REGIME_CELLS``).
POOLED_REGIME = "pooled"
#: T20 internationals are one regime.
T20_REGIME = "t20"
#: ODI fielding-restriction regimes as ``(regime id, first match date)``, in order; a match
#: on or after a start date and before the next one is in that regime
#: (sdv-internal-refs ``rules/cricket.yaml``):
#:
#: * ``odi_pre2005`` -- two fielders outside the circle for 15 overs (the 1992 rule);
#: * ``odi_2005`` -- three powerplays, 10 + 5 + 5 (``cricket-2005-odi-powerplays``; the
#:   catalogue pins the season, not a date, so the season start is used);
#: * ``odi_2011`` -- the discretionary powerplays confined to overs 16-40 from 2011-10-01
#:   (``cricket-2011-odi-powerplay-window``);
#: * ``odi_2012`` -- bowling powerplay abolished, four fielders out, two new balls from
#:   2012-10-30 (``cricket-2012-odi-two-new-balls-four-out``);
#: * ``odi_2015`` -- batting powerplay removed, five fielders out in overs 41-50 from
#:   2015-07-05 (``cricket-2015-odi-batting-powerplay-removed``).
ODI_REGIMES: tuple[tuple[str, dt.date], ...] = (
    ("odi_pre2005", dt.date.min),
    ("odi_2005", dt.date(2005, 1, 1)),
    ("odi_2011", dt.date(2011, 10, 1)),
    ("odi_2012", dt.date(2012, 10, 30)),
    ("odi_2015", dt.date(2015, 7, 5)),
)
#: Per-regime ``par_score`` / ``sigma_set`` / ``sigma_chase`` overriding :data:`FORMAT_TABLE`,
#: fitted 2026-10-10 by ``tools/models/league_ports/fit_cricket_resource_surface.py`` on Cricsheet male ODIs
#: 2002-2026 (2,589 matches) for every regime with 100+ training matches and a thick grid (the
#: runtime's whole-regime rule, ``cricket_win_prob._usable_regimes``), each on its own grid. The
#: 13-month ``odi_2011`` window (rules/cricket.yaml#cricket-2011-odi-powerplay-window) has no entry
#: and falls back to the pooled surface and constants. Regimes per ``cricket-2005-odi-powerplays`` /
#: ``cricket-2012-odi-two-new-balls-four-out`` / ``cricket-2015-odi-batting-powerplay-removed``.
REGIME_CONSTANTS: dict[str, dict[str, float]] = {
    "odi_pre2005": {"par_score": 228.6, "sigma_set": 45.0, "sigma_chase": 44.0},
    "odi_2005": {"par_score": 242.8, "sigma_set": 63.0, "sigma_chase": 53.0},
    "odi_2012": {"par_score": 256.6, "sigma_set": 64.0, "sigma_chase": 61.0},
    "odi_2015": {"par_score": 255.3, "sigma_set": 91.0, "sigma_chase": 63.0},
}


def cricket_regime(fmt: str, match_date: Optional[dt.date]) -> str:
    """The fielding-restriction regime a match was played under.

    Args:
        fmt: Format slug (``"t20"`` / ``"odi"``).
        match_date: The match date; ``None`` when unknown.

    Returns:
        :data:`T20_REGIME` for a T20, :data:`POOLED_REGIME` for an ODI without a date, else
        the :data:`ODI_REGIMES` id whose window holds the date.

    Raises:
        ValueError: For ``"test"`` (deferred) or an unknown format.

    Example:
        Quick start::

            import datetime as dt
            from sportsdataverse.cricket.cricket_model_constants import cricket_regime
            cricket_regime("odi", dt.date(2013, 2, 1))  # "odi_2012"
    """
    if get_format(fmt).name == T20_REGIME:
        return T20_REGIME
    if match_date is None:
        return POOLED_REGIME
    regime = ODI_REGIMES[0][0]
    for name, start in ODI_REGIMES:
        if match_date >= start:
            regime = name
    return regime


def get_format(fmt: str, regime: Optional[str] = None) -> FormatConstants:
    """Resolve a format slug to its :class:`FormatConstants`.

    Args:
        fmt: Format slug — one of ``"t20"`` / ``"odi"`` (case-insensitive).
        regime: Optional regime id (:func:`cricket_regime`); when it has an entry in
            :data:`REGIME_CONSTANTS` the fitted per-regime par/sigma replace the pooled
            ones, otherwise the pooled constants are returned unchanged.

    Returns:
        The :class:`FormatConstants` for ``fmt`` (and ``regime``).

    Raises:
        ValueError: If ``fmt`` is ``"test"`` (Test cricket is deferred) or any
            other slug not in :data:`FORMAT_TABLE`.

    Example:
        Quick start::

            from sportsdataverse.cricket.cricket_model_constants import get_format
            fc = get_format("t20")
            fc.balls_total  # 120
    """
    key = (fmt or "").strip().lower()
    if key == "test":
        raise ValueError("Test cricket deferred")
    if key not in FORMAT_TABLE:
        raise ValueError(f"Unknown cricket format {fmt!r}; expected one of {sorted(FORMAT_TABLE)}")
    fc = FORMAT_TABLE[key]
    if regime is None or regime not in REGIME_CONSTANTS:
        return fc
    over = REGIME_CONSTANTS[regime]
    return replace(
        fc,
        par_score=over.get("par_score", fc.par_score),
        sigma_set=over.get("sigma_set", fc.sigma_set),
        sigma_chase=over.get("sigma_chase", fc.sigma_chase),
    )
