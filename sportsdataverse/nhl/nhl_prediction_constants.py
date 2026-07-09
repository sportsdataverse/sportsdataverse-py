"""Prediction-spine shared constants + validation metrics (league-agnostic).

Single home for the NHL/PWHL prediction-and-market spine (T5.3): validation
metric functions (Brier, log-loss, Spearman, MAE, calibration table), the
as-of-date leakage split, and the per-league fitted-constants table
(``LEAGUE_CONSTANTS``). Every downstream module (:mod:`nhl_team_ratings`,
:mod:`nhl_market`, :mod:`nhl_player_props`) reads its HFA / sigma /
shrinkage / prop-prior numbers from :func:`get_constants` -- no NHL/PWHL
number is ever hard-coded inside an algorithm function.

Seeded constants (``margin_sd``, ``hfa``, ``shrink_k``, ``total_scale``,
``avg_*``) start from published references so the engine runs before
fitting, and are overwritten in-code by the committed fitting scripts in
``dev/nhl_prediction/`` (``fit_pregame.py`` for Task 2.3, ``fit_props.py``
for Task 4.2) once the 2023 backtest corpus is captured. Hockey is a
low-event-count, high-variance sport: ``margin_sd`` is deliberately wide and
``shrink_k`` (a games-played prior) is a first-class part of every rating,
not an afterthought -- see the design spec's hockey caveat
(``2026-07-07-nhl-prediction-market-design.md`` Sec 3.3/§9-4).

Example:
    Quick start::

        from sportsdataverse.nhl.nhl_prediction_constants import get_constants

        nhl = get_constants("nhl")
        print(nhl.margin_sd, nhl.hfa)

    Metric functions::

        from sportsdataverse.nhl.nhl_prediction_constants import brier_score
        import numpy as np
        brier_score(np.array([1, 0]), np.array([0.8, 0.3]))

See Also:
    * `nflfastR`_ -- the T7.2-shared Phi-margin power-rating core this spine mirrors.
    * `nhl-api-py`_ -- companion NHL Python client.

.. _nflfastR: https://www.nflfastr.com
.. _nhl-api-py: https://github.com/coreyjs/nhl-api-py
"""

from __future__ import annotations

import datetime as _dt
from dataclasses import dataclass

import numpy as np
import polars as pl
from scipy.stats import rankdata


def brier_score(y_true: np.ndarray, p_pred: np.ndarray) -> float:
    """Mean squared error between predicted probability and binary outcome.

    Args:
        y_true: binary outcomes (0/1).
        p_pred: predicted probabilities in [0, 1].

    Returns:
        The Brier score (lower is better; 0.0 is a perfect forecast).

    Example:
        Quick start::

            import numpy as np
            from sportsdataverse.nhl.nhl_prediction_constants import brier_score
            brier_score(np.array([1, 0]), np.array([0.8, 0.2]))
    """
    return float(np.mean((np.asarray(p_pred, dtype=float) - np.asarray(y_true, dtype=float)) ** 2))


def log_loss_score(y_true: np.ndarray, p_pred: np.ndarray, eps: float = 1e-15) -> float:
    """Binary log-loss (cross-entropy) between predicted probability and outcome.

    Args:
        y_true: binary outcomes (0/1).
        p_pred: predicted probabilities in [0, 1].
        eps: clipping floor/ceiling to avoid ``log(0)``.

    Returns:
        The mean log-loss (lower is better).

    Example:
        Quick start::

            import numpy as np
            from sportsdataverse.nhl.nhl_prediction_constants import log_loss_score
            log_loss_score(np.array([1, 0]), np.array([0.8, 0.2]))
    """
    p = np.clip(np.asarray(p_pred, dtype=float), eps, 1 - eps)
    y = np.asarray(y_true, dtype=float)
    return float(-np.mean(y * np.log(p) + (1 - y) * np.log(1 - p)))


def spearman_corr(a: np.ndarray, b: np.ndarray) -> float:
    """Spearman rank correlation between two arrays.

    Args:
        a: first array.
        b: second array (same length as ``a``).

    Returns:
        The Spearman rank correlation coefficient in [-1, 1].

    Example:
        Quick start::

            import numpy as np
            from sportsdataverse.nhl.nhl_prediction_constants import spearman_corr
            spearman_corr(np.array([1.0, 2.0, 3.0]), np.array([3.0, 1.0, 2.0]))
    """
    ra, rb = rankdata(a), rankdata(b)
    return float(np.corrcoef(ra, rb)[0, 1])


def mae(a: np.ndarray, b: np.ndarray) -> float:
    """Mean absolute error between two arrays.

    Args:
        a: first array (e.g. predicted values).
        b: second array (e.g. observed values).

    Returns:
        The mean absolute difference ``mean(|a - b|)``.

    Example:
        Quick start::

            import numpy as np
            from sportsdataverse.nhl.nhl_prediction_constants import mae
            mae(np.array([1.0, 2.0]), np.array([1.5, 2.5]))
    """
    return float(np.mean(np.abs(np.asarray(a, dtype=float) - np.asarray(b, dtype=float))))


def calibration_table(y_true: np.ndarray, p_pred: np.ndarray, n_bins: int = 10) -> pl.DataFrame:
    """Bucket predicted probabilities into ``n_bins`` and compare to realized rate.

    Args:
        y_true: binary outcomes (0/1).
        p_pred: predicted probabilities in [0, 1].
        n_bins: number of equal-width probability bins.

    Returns:
        A polars DataFrame with one row per non-empty bin.

        |col_name    |type   |
        |:-----------|:------|
        |bin_mid     |Float64|
        |mean_pred   |Float64|
        |mean_actual |Float64|
        |n           |Int64  |

    Example:
        Quick start::

            import numpy as np
            from sportsdataverse.nhl.nhl_prediction_constants import calibration_table
            rng = np.random.default_rng(0)
            calibration_table(rng.integers(0, 2, 200), rng.random(200))
    """
    df = pl.DataFrame({"y": np.asarray(y_true, dtype=float), "p": np.asarray(p_pred, dtype=float)})
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


def as_of_ratings_split(df: pl.DataFrame, cutoff_date: _dt.date, *, date_col: str = "date") -> pl.DataFrame:
    """Filter a frame to rows strictly before ``cutoff_date`` (the leakage boundary).

    Args:
        df: a polars DataFrame with a date column.
        cutoff_date: the game date being predicted; only strictly-earlier rows are kept.
        date_col: name of the date column (default ``"date"``).

    Returns:
        The subset of ``df`` with ``df[date_col] < cutoff_date``.

    Example:
        Quick start::

            import datetime as dt
            import polars as pl
            from sportsdataverse.nhl.nhl_prediction_constants import as_of_ratings_split
            df = pl.DataFrame({"date": [dt.date(2023, 1, 1), dt.date(2023, 1, 2)]})
            as_of_ratings_split(df, dt.date(2023, 1, 2))
    """
    return df.filter(pl.col(date_col) < cutoff_date)


@dataclass(frozen=True)
class LeagueConstants:
    """Fitted, league-specific constants for the NHL/PWHL prediction spine.

    Attributes:
        hfa: home-ice edge, expected-goals units.
        margin_sd: standard deviation of the final goal margin (deliberately WIDE for hockey).
        avg_xgf: league mean even-strength xG-for, per game.
        avg_total_goals: league mean total goals per game.
        total_scale: multiplier converting rating differential to total-goals deviation.
        shrink_k: games-played prior strength for rating shrinkage.
        prop_kappa: empirical-Bayes shrinkage strength per player-prop stat family.
        pos_priors: per-position (F/D) per-stat-family prior rates.
        in_game_wp_artifact: filename of the bundled in-game win-probability model
            under ``sportsdataverse/nhl/models/``.
        min_season: earliest season this league's prediction spine supports.
    """

    hfa: float
    margin_sd: float
    avg_xgf: float
    avg_total_goals: float
    total_scale: float
    shrink_k: float
    prop_kappa: dict
    pos_priors: dict
    in_game_wp_artifact: str
    min_season: int


LEAGUE_CONSTANTS: dict[str, LeagueConstants] = {
    # hfa/margin_sd/total_scale fitted 2026-07-08 by dev/nhl_prediction/fit_pregame.py
    # against the 2023 as-of-date backtest (1174 evaluated games, dates[20:] onward),
    # confirmed converged on a second pass (residual on top of the fitted seed shrank
    # to ~0.0003):
    #   hfa = 0.1630 (0.20 seed + -0.0373 residual on pass 1, ~0 residual on pass 2).
    #   margin_sd = 0.9085, fit by minimising Brier of Phi(exp_margin/margin_sd)
    #     directly -- NOTE this is NOT the real-world NHL goal-margin SD (that
    #     diagnostic residual comes out ~2.58 goals); margin_sd here operates on
    #     exp_margin's own compressed, shrunk-rating scale, which is much smaller
    #     than a real goal margin. Using the real-world ~2.2 seed value here would
    #     have been a scale mismatch, not a faithful sigma for this closed form.
    #   total_scale = 1.9105, the OLS slope of realized total goals on exp_total
    #     (shrinkage compresses exp_total's spread well below the real variance);
    #     applied in nhl_market.predict_total as
    #     avg_total_goals + total_scale * (raw_total - avg_total_goals).
    # avg_xgf/avg_total_goals remain published-reference seeds (not walked/fit).
    #
    # prop_kappa/pos_priors fitted 2026-07-08 by dev/nhl_prediction/fit_props.py
    # against season 2024 skater boxscores (load_nhl_skater_boxscores only
    # publishes seasons >= 2024 -- season 2024 == the 2023-24 season; Phase 4
    # therefore uses 2024 while ratings/market Phases 1-3 stay on 2023, see the
    # fixtures README). Method-of-moments EB kappa (within-player Poisson
    # variance / between-player variance), computed separately per position
    # then averaged (unweighted) into the single per-stat kappa this table
    # carries: shots F kappa=2.10/D kappa=3.40 -> 2.75; points F kappa=3.91/D
    # kappa=5.86 -> 4.88. pos_priors are the raw per-position mean rates.
    "nhl": LeagueConstants(
        hfa=0.1630,
        margin_sd=0.9085,
        avg_xgf=2.55,
        avg_total_goals=6.05,
        total_scale=1.9105,
        shrink_k=15.0,
        prop_kappa={"shots": 2.7498, "points": 4.8837},
        pos_priors={"shots": {"F": 1.5746, "D": 1.1910}, "points": {"F": 0.4125, "D": 0.2858}},
        in_game_wp_artifact="nhl_in_game_wp.json",
        min_season=2010,
    ),
    # PWHL: shorter league history + higher game-to-game variance -> stronger
    # shrinkage prior and wider margin SD. Fitted once PWHL xG-bearing pbp
    # lands in sdv-py (deferred per design spec Sec 9-7); shim ships now.
    "pwhl": LeagueConstants(
        hfa=0.15,
        margin_sd=2.35,
        avg_xgf=2.30,
        avg_total_goals=5.20,
        total_scale=1.0,
        shrink_k=25.0,
        prop_kappa={"shots": 8.0, "points": 10.0},
        pos_priors={"shots": {"F": 2.0, "D": 1.2}, "points": {"F": 0.50, "D": 0.28}},
        in_game_wp_artifact="pwhl_in_game_wp.json",
        min_season=2024,
    ),
}


def get_constants(league: str) -> LeagueConstants:
    """Resolve the fitted-constants row for a league.

    Args:
        league: ``"nhl"`` or ``"pwhl"``.

    Returns:
        The :class:`LeagueConstants` row for ``league``.

    Raises:
        ValueError: if ``league`` is not a known key of ``LEAGUE_CONSTANTS``.

    Example:
        Quick start::

            from sportsdataverse.nhl.nhl_prediction_constants import get_constants
            get_constants("nhl").margin_sd
    """
    try:
        return LEAGUE_CONSTANTS[league]
    except KeyError as exc:
        raise ValueError(f"unknown league {league!r}; expected one of {sorted(LEAGUE_CONSTANTS)}") from exc
