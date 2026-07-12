"""Cricket model constants + shared calibration metrics (T7.3).

This module is the single home of:

* the **format table** (:data:`FORMAT_TABLE` / :func:`get_format`) — per-format
  ball budgets and fitted par/sigma constants that parameterise the in-play
  win-probability and WPA models (``cricket_win_prob`` / ``cricket_wpa``); and
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

from dataclasses import dataclass

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
    "FormatConstants",
    "auc_score",
    "brier_score",
    "calibration_table",
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
        par_score: Fitted mean first-innings total for the format (a fair
            "setting" benchmark). Seeded, then overwritten by the Phase-2 fit.
        sigma_set: Fitted std of the first-innings ("setting") projection
            residual — the spread of ``proj_final`` around ``par_score``.
        sigma_chase: Fitted std of the second-innings ("chase") projection
            residual — the spread of ``proj_final`` around the target.
        resource_surface_path: Bundled resource-surface parquet resource name
            (loaded via ``importlib.resources`` from ``cricket.models``).
    """

    name: str
    balls_total: int
    max_wickets: int
    par_score: float
    sigma_set: float
    sigma_chase: float
    resource_surface_path: str = "cricket_resource_surface.parquet"


# NOTE: par_score / sigma_* below are SEEDS. The Phase-2 fitting script
# (dev/league_ports/fit_cricket_resource_surface.py) overwrites them with values
# fitted on the real Cricsheet corpus; see that script's printout and the
# committed constants block. balls_total / max_wickets are rule-fixed, not seeds.
FORMAT_TABLE: dict[str, FormatConstants] = {
    "t20": FormatConstants(
        name="t20",
        balls_total=120,
        max_wickets=10,
        par_score=160.0,  # seed — overwritten by fit
        sigma_set=18.0,  # seed — overwritten by fit
        sigma_chase=18.0,  # seed — overwritten by fit
    ),
    "odi": FormatConstants(
        name="odi",
        balls_total=300,
        max_wickets=10,
        par_score=250.0,  # seed — overwritten by fit
        sigma_set=40.0,  # seed — overwritten by fit
        sigma_chase=40.0,  # seed — overwritten by fit
    ),
}


def get_format(fmt: str) -> FormatConstants:
    """Resolve a format slug to its :class:`FormatConstants`.

    Args:
        fmt: Format slug — one of ``"t20"`` / ``"odi"`` (case-insensitive).

    Returns:
        The :class:`FormatConstants` for ``fmt``.

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
    return FORMAT_TABLE[key]
