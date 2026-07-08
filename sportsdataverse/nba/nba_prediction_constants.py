"""NBA/WNBA/G-League prediction-stack shared constants + validation metrics.

Home of the small, dependency-light pieces every module in the NBA prediction
& market stack (T3.3) shares:

* **Validation metrics** (:func:`brier_score`, :func:`log_loss_score`,
  :func:`spearman_corr`, :func:`mae`, :func:`calibration_table`) used by the
  phase oracle/backtest gates.
* **Per-``league_id`` fitted constants** (:data:`LEAGUE_CONSTANTS` /
  :func:`get_constants`) -- the algorithm/constants boundary described in the
  design spec: every league-specific number (home-court advantage, margin
  sigma, pace/efficiency baselines, game minutes) lives here, keyed by the
  stats.nba.com ``league_id`` (``"00"`` NBA, ``"10"`` WNBA, ``"20"``
  G-League) so a WNBA caller is a by-reference shim over this table -- the
  same pattern ``nba_possessions(league_id=...)`` / ``wnba_stats`` already
  use. No NBA-specific number may be hard-coded inside an algorithm function.
* **As-of-date leakage split** (:func:`as_of_ratings_split`) -- the boundary
  every predictive backtest uses so a game's own future never leaks into its
  own prediction.

*(T7.2-shared)*: the metrics + as-of-split pieces are byte-identical to the
MBB/WBB (``mbb_prediction_constants``) and CFB sibling modules and are a
prime candidate for the future cross-league infra factor-out; this module
implements them standalone per the T3.3 plan (mirroring MBB names).
"""

from __future__ import annotations

import datetime
from dataclasses import dataclass

import numpy as np
import polars as pl
from scipy.stats import rankdata

__all__ = [
    "LEAGUE_CONSTANTS",
    "LeagueConstants",
    "as_of_ratings_split",
    "brier_score",
    "calibration_table",
    "get_constants",
    "log_loss_score",
    "mae",
    "spearman_corr",
]


@dataclass(frozen=True)
class LeagueConstants:
    """Per-``league_id`` fitted constants for the NBA prediction & market stack.

    Attributes:
        hfa: Home-court advantage in points (fitted on the as-of-date
            backtest for the league; see ``dev/nba_prediction/fit_pregame.py``).
        margin_sd: Std. dev. of the game-margin residual (fitted jointly
            with ``hfa``; the Brier-minimizing sigma agrees to within a
            documented tolerance).
        avg_pace: League baseline possessions per team per game (adjusted-
            pace anchor for :func:`~sportsdataverse.nba.nba_team_ratings.adjust_pace`).
        avg_off_rtg: League baseline points per 100 possessions.
        game_minutes: Regulation game length in minutes (NBA/G-League 48,
            WNBA 40) -- structurally different, not a fitted number.
        in_game_wp_artifact: Filename of the bundled in-game-WP coefficients
            under ``sportsdataverse/nba/models`` (committed in Phase 3).
    """

    hfa: float
    margin_sd: float
    avg_pace: float
    avg_off_rtg: float
    game_minutes: int
    in_game_wp_artifact: str = "nba_in_game_wp.json"


# Seed values documented in the Phase-0 design (published references / league
# norms) so the engine imports and runs before the Phase-2/3/5 fitters
# overwrite hfa/margin_sd/in-game-WP-beta/prop-dispersion with values fitted
# on the committed 2023-24 (NBA) backtest -- see dev/nba_prediction/fit_*.py.
LEAGUE_CONSTANTS: dict[str, LeagueConstants] = {
    "00": LeagueConstants(
        hfa=2.8,
        margin_sd=13.0,
        avg_pace=99.5,
        avg_off_rtg=114.0,
        game_minutes=48,
        in_game_wp_artifact="nba_in_game_wp.json",
    ),
    "10": LeagueConstants(
        hfa=2.5,
        margin_sd=12.0,
        avg_pace=82.0,
        avg_off_rtg=101.0,
        game_minutes=40,
        in_game_wp_artifact="nba_in_game_wp.json",
    ),
    "20": LeagueConstants(
        hfa=2.5,
        margin_sd=14.0,
        avg_pace=101.0,
        avg_off_rtg=110.0,
        game_minutes=48,
        in_game_wp_artifact="nba_in_game_wp.json",
    ),
}


def get_constants(league_id: str) -> LeagueConstants:
    """Return the :class:`LeagueConstants` for a ``league_id``.

    Args:
        league_id: stats.nba.com league id -- ``"00"`` NBA, ``"10"`` WNBA,
            ``"20"`` G-League.

    Returns:
        The league's :class:`LeagueConstants`.

    Raises:
        ValueError: If ``league_id`` is not a known key of
            :data:`LEAGUE_CONSTANTS`.

    Example:
        Quick start::

            from sportsdataverse.nba.nba_prediction_constants import get_constants
            get_constants("00").hfa
    """
    try:
        return LEAGUE_CONSTANTS[league_id]
    except KeyError:
        raise ValueError(f"unknown league_id {league_id!r}; expected one of {sorted(LEAGUE_CONSTANTS)}") from None


def as_of_ratings_split(results: pl.DataFrame, cutoff_date: datetime.date) -> pl.DataFrame:
    """Return only games strictly before ``cutoff_date`` (the leakage boundary).

    Predictive backtests must rate a game using only games that finished
    before it -- this split enforces that as-of-date rule so no future
    information leaks into a game's own prediction.

    Args:
        results: A frame with a ``date`` column of dtype ``pl.Date``.
        cutoff_date: The date of the game being predicted; games on or after
            it are dropped.

    Returns:
        The subset of ``results`` with ``date < cutoff_date``.

    Example:
        Quick start::

            from sportsdataverse.nba.nba_prediction_constants import as_of_ratings_split
            prior = as_of_ratings_split(results, some_game_date)
    """
    return results.filter(pl.col("date") < cutoff_date)


def brier_score(y_true: np.ndarray, p_pred: np.ndarray) -> float:
    """Mean squared error between binary outcomes and predicted probabilities.

    Args:
        y_true: Array of realized binary outcomes (0/1).
        p_pred: Array of predicted probabilities in ``[0, 1]``.

    Returns:
        The Brier score (lower is better; 0.0 is perfect).

    Example:
        Perfect predictions score zero::

            import numpy as np
            from sportsdataverse.nba.nba_prediction_constants import brier_score
            brier_score(np.array([1, 0]), np.array([1.0, 0.0]))
    """
    return float(np.mean((np.asarray(p_pred, dtype=float) - np.asarray(y_true, dtype=float)) ** 2))


def log_loss_score(y_true: np.ndarray, p_pred: np.ndarray, eps: float = 1e-15) -> float:
    """Binary cross-entropy (log loss) between outcomes and probabilities.

    Args:
        y_true: Array of realized binary outcomes (0/1).
        p_pred: Array of predicted probabilities in ``[0, 1]``.
        eps: Clipping bound to keep the log finite at 0/1.

    Returns:
        The mean log loss (lower is better).

    Example:
        Quick start::

            import numpy as np
            from sportsdataverse.nba.nba_prediction_constants import log_loss_score
            log_loss_score(np.array([1, 0]), np.array([0.9, 0.1]))
    """
    p = np.clip(np.asarray(p_pred, dtype=float), eps, 1 - eps)
    y = np.asarray(y_true, dtype=float)
    return float(-np.mean(y * np.log(p) + (1 - y) * np.log(1 - p)))


def spearman_corr(a: np.ndarray, b: np.ndarray) -> float:
    """Spearman rank correlation between two arrays.

    Args:
        a: First array.
        b: Second array (same length as ``a``).

    Returns:
        The Spearman rank-correlation coefficient in ``[-1, 1]``.

    Example:
        A monotonic relationship scores 1.0::

            import numpy as np
            from sportsdataverse.nba.nba_prediction_constants import spearman_corr
            spearman_corr(np.array([1.0, 2.0, 3.0]), np.array([10.0, 20.0, 30.0]))
    """
    ra, rb = rankdata(a), rankdata(b)
    return float(np.corrcoef(ra, rb)[0, 1])


def mae(a: np.ndarray, b: np.ndarray) -> float:
    """Mean absolute error between two arrays.

    Args:
        a: First array.
        b: Second array (same length as ``a``).

    Returns:
        The mean absolute error.

    Example:
        Quick start::

            import numpy as np
            from sportsdataverse.nba.nba_prediction_constants import mae
            mae(np.array([1.0, 2.0]), np.array([1.5, 2.5]))
    """
    return float(np.mean(np.abs(np.asarray(a, dtype=float) - np.asarray(b, dtype=float))))


def calibration_table(y_true: np.ndarray, p_pred: np.ndarray, n_bins: int = 10) -> pl.DataFrame:
    """Bin predicted probabilities and compare mean-predicted vs mean-actual.

    Args:
        y_true: Array of realized binary outcomes (0/1).
        p_pred: Array of predicted probabilities in ``[0, 1]``.
        n_bins: Number of equal-width probability bins.

    Returns:
        A ``polars.DataFrame`` with columns ``bin_mid``, ``mean_pred``,
        ``mean_actual``, ``n`` (one row per non-empty bin, sorted ascending).

    Example:
        Quick start::

            import numpy as np
            from sportsdataverse.nba.nba_prediction_constants import calibration_table
            y = np.random.default_rng(0).integers(0, 2, 200)
            p = np.random.default_rng(1).random(200)
            calibration_table(y, p, n_bins=10)
    """
    df = pl.DataFrame(
        {
            "y": np.asarray(y_true, dtype=float),
            "p": np.asarray(p_pred, dtype=float),
        }
    )
    df = df.with_columns((pl.col("p").clip(0.0, 0.9999) * n_bins).floor().cast(pl.Int64).alias("bin"))
    return (
        df.group_by("bin")
        .agg(
            pl.col("p").mean().alias("mean_pred"),
            pl.col("y").mean().alias("mean_actual"),
            pl.len().alias("n"),
        )
        .sort("bin")
        .with_columns(((pl.col("bin") + 0.5) / n_bins).alias("bin_mid"))
        .select("bin_mid", "mean_pred", "mean_actual", "n")
    )
