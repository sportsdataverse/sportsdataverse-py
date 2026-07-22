"""Tabulated in-game win-probability surface + calibration-at-scale.

The scalable half of in-game pricing:
:func:`~sportsdataverse.nba.nba_possession_sim.engine.in_game_win_prob`
re-simulates from one state (exact but O(n_sim) per query); this module
simulates the ensemble ONCE, tabulates ``P(home win | time bucket, margin)``
over every possession snapshot, and answers lookups in O(1). Cells are
empirical-Bayes shrunk toward a Brownian-motion prior (Stern's classic
``P(win) = Phi((margin + mu*f) / (sigma*sqrt(f)))`` with ``f`` the fraction
of the game remaining and ``mu``/``sigma`` fitted from the train paths'
final margins) — raw per-cell tabulation at typical path counts is
overconfident, and sign-pooled priors drag big leads toward coin flips;
the drift-diffusion prior is monotone in margin, time-sharpening, and
collapses to a step function at the buzzer.

:func:`held_out_calibration` is the self-consistency gate: fit the surface
on one half of the simulated paths and score it on the other half with
:func:`~sportsdataverse.modeling.eval.backtest`. A correctly tabulated
Markov engine MUST be calibrated on its own held-out paths, so a reliability
deviation there exposes a binning/tabulation/shrinkage bug, not model
opinion. :func:`real_path_snapshots` converts captured ``playbyplayv3``
games into the same snapshot schema so the surface is also scored against
realized score paths (the committed reliability artifact under
``tests/fixtures/calibration/`` carries both curves).
"""

from __future__ import annotations

import dataclasses
import math
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import polars as pl

from sportsdataverse.modeling.eval import BacktestResult, backtest
from sportsdataverse.nba.nba_possession_sim.engine import simulate_game_pbp
from sportsdataverse.nba.nba_possession_sim.factors import FactorAdjustment
from sportsdataverse.nba.nba_possession_sim.keygen import parse_clock
from sportsdataverse.nba.nba_possession_sim.rules import NBA_RULES, SportRules
from sportsdataverse.nba.nba_possession_sim.shelf import Shelf

_PATH_SCHEMA = {
    "path_id": pl.Int64,
    "seconds_remaining": pl.Float64,
    "margin": pl.Int64,
    "home_win": pl.Boolean,
}
_REQUIRED_PATH_COLS = frozenset(_PATH_SCHEMA)
#: Floor on the remaining-game fraction inside the prior (keeps sqrt(f)
#: finite at the buzzer while still saturating decided games to ~0/1).
_PRIOR_FRACTION_FLOOR = 1e-3


def _total_seconds_remaining(period_expr: pl.Expr, clock_expr: pl.Expr, rules: SportRules) -> pl.Expr:
    """Whole-game seconds remaining (OT periods count only their own clock)."""
    regulation_left = (rules.periods - period_expr).cast(pl.Float64) * rules.period_seconds + clock_expr
    return pl.when(period_expr <= rules.periods).then(regulation_left).otherwise(clock_expr)


def _norm_cdf(x: float) -> float:
    """Standard normal CDF via the stdlib error function."""
    return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))


def simulate_score_paths(
    shelf: Shelf,
    *,
    n_sim: int = 100,
    seed: Optional[int] = None,
    factors: Optional[FactorAdjustment] = None,
    rules: SportRules = NBA_RULES,
) -> pl.DataFrame:
    """Simulate ``n_sim`` games and emit every possession snapshot.

    Args:
        shelf: The PMF shelf.
        n_sim: Number of simulated games (paths).
        seed: RNG seed (same seed = identical paths).
        factors: Optional auditable PMF adjustment applied to every draw.
        rules: League clock structure.

    Returns:
        Long snapshot frame: ``path_id``, ``seconds_remaining`` (whole-game
        seconds left at the snapshot), ``margin`` (home minus away after the
        possession), ``home_win`` (that path's final outcome).

    Raises:
        ValueError: When ``n_sim < 1``.

    Example:
        Quick start::

            from sportsdataverse.nba.nba_possession_sim import simulate_score_paths
            paths = simulate_score_paths(shelf, n_sim=50, seed=7)
            paths.group_by("path_id").len().head()
    """
    if n_sim < 1:
        raise ValueError("n_sim must be >= 1")
    rng = np.random.default_rng(seed)
    rows: List[Dict[str, Any]] = []
    for path_id in range(n_sim):
        final, pbp = simulate_game_pbp(shelf, rng, rules=rules, factors=factors)
        home_win = final.score_home > final.score_away
        for possession in pbp:
            period = int(possession["period"])
            clock = float(possession["clock_seconds"])
            if period <= rules.periods:
                seconds_remaining = (rules.periods - period) * rules.period_seconds + clock
            else:
                seconds_remaining = clock
            rows.append(
                {
                    "path_id": path_id,
                    "seconds_remaining": seconds_remaining,
                    "margin": int(possession["score_home"]) - int(possession["score_away"]),
                    "home_win": home_win,
                }
            )
    return pl.DataFrame(rows, schema=_PATH_SCHEMA)


@dataclasses.dataclass(frozen=True)
class WPSurface:
    """Tabulated ``P(home win | time bucket, margin)`` over a Brownian prior.

    Attributes:
        time_bin_seconds: Width of the whole-game-seconds-remaining buckets.
        margin_cap: Margins are clipped to ``[-margin_cap, margin_cap]``.
        shrinkage: Pseudo-count weight of the prior each cell was shrunk
            toward at fit time (0 = raw tabulation).
        total_seconds: Whole-game length the remaining-fraction is measured
            against.
        drift_mu: Mean final home margin of the fitted paths (the prior's
            drift).
        drift_sigma: Final-margin standard deviation (the prior's diffusion
            scale).
        cells: ``(time_bucket, margin) -> (shrunk win rate, raw snapshot
            count)``.
        n_paths: Number of paths the surface was fitted on.
    """

    time_bin_seconds: float
    margin_cap: int
    shrinkage: float
    total_seconds: float
    drift_mu: float
    drift_sigma: float
    cells: Dict[Tuple[int, int], Tuple[float, int]]
    n_paths: int

    def prior(self, seconds_remaining: float, margin: float) -> float:
        """The drift-diffusion prior ``Phi((m + mu*f) / (sigma*sqrt(f)))``.

        Args:
            seconds_remaining: Whole-game seconds left.
            margin: Home minus away score.

        Returns:
            The prior ``P(home win)`` — also the fallback for game states
            no fitted cell covers.

        Example:
            Quick start::

                surface.prior(seconds_remaining=720.0, margin=8)
        """
        fraction = max(_PRIOR_FRACTION_FLOOR, min(1.0, max(0.0, seconds_remaining) / self.total_seconds))
        z = (float(margin) + self.drift_mu * fraction) / (self.drift_sigma * math.sqrt(fraction))
        return _norm_cdf(z)

    def predict(self, seconds_remaining: float, margin: float) -> float:
        """Home win probability for one game state (O(1) lookup).

        Args:
            seconds_remaining: Whole-game seconds left.
            margin: Home minus away score.

        Returns:
            The shrunk cell rate when the state was seen at fit time, else
            :meth:`prior` evaluated at the exact state.

        Example:
            Quick start::

                surface.predict(seconds_remaining=120.0, margin=5)
        """
        t_bucket = int(max(0.0, seconds_remaining) // self.time_bin_seconds)
        m = max(-self.margin_cap, min(self.margin_cap, int(round(margin))))
        cell = self.cells.get((t_bucket, m))
        if cell is not None:
            return cell[0]
        return self.prior(seconds_remaining, margin)

    def to_frame(self) -> pl.DataFrame:
        """The fitted cells as a tidy frame.

        Returns:
            One row per cell: ``time_bucket``, ``margin``, ``wp``, ``n``,
            sorted for stable diffs.

        Example:
            Quick start::

                surface.to_frame().filter(pl.col("time_bucket") == 0)
        """
        rows = [{"time_bucket": t, "margin": m, "wp": wp, "n": n} for (t, m), (wp, n) in sorted(self.cells.items())]
        schema = {"time_bucket": pl.Int64, "margin": pl.Int64, "wp": pl.Float64, "n": pl.Int64}
        return pl.DataFrame(rows, schema=schema)


def fit_wp_surface(
    paths: pl.DataFrame,
    *,
    time_bin_seconds: float = 60.0,
    margin_cap: int = 24,
    shrinkage: float = 20.0,
    total_seconds: Optional[float] = None,
) -> WPSurface:
    """Tabulate a :class:`WPSurface` from snapshot paths.

    Each cell's empirical win rate is shrunk toward the Brownian prior at
    the cell's state with weight ``n / (n + shrinkage)``. Raw tabulation is
    overconfident at typical path counts (noisy extreme cells sort into
    extreme prediction bins and regress to the mean on held-out paths — the
    held-out reliability gate caught exactly that), and the pseudo-count
    prior is the standard correction.

    Args:
        paths: Snapshot frame from :func:`simulate_score_paths` /
            :func:`real_path_snapshots` (``path_id``, ``seconds_remaining``,
            ``margin``, ``home_win``).
        time_bin_seconds: Time-bucket width in whole-game seconds.
        margin_cap: Clip bound for the margin dimension.
        shrinkage: Pseudo-count weight of the prior (0 = raw tabulation).
        total_seconds: Whole-game length for the prior's remaining
            fraction; defaults to NBA regulation (2880).

    Returns:
        The fitted surface (cells carry the shrunk rate + the raw count).

    Raises:
        ValueError: On an empty frame, missing columns, a non-positive
            ``time_bin_seconds``, or negative ``shrinkage``.

    Example:
        Quick start::

            from sportsdataverse.nba.nba_possession_sim import (
                fit_wp_surface, simulate_score_paths,
            )
            surface = fit_wp_surface(simulate_score_paths(shelf, n_sim=100, seed=7))
            surface.predict(600.0, -4)
    """
    missing = sorted(_REQUIRED_PATH_COLS - set(paths.columns))
    if missing:
        raise ValueError(f"paths frame is missing columns: {missing}")
    if paths.height == 0:
        raise ValueError("cannot fit a WP surface from an empty paths frame")
    if time_bin_seconds <= 0:
        raise ValueError("time_bin_seconds must be positive")
    if shrinkage < 0:
        raise ValueError("shrinkage must be >= 0")
    game_seconds = float(total_seconds) if total_seconds is not None else NBA_RULES.periods * NBA_RULES.period_seconds
    if game_seconds <= 0:
        raise ValueError("total_seconds must be positive")

    # The prior's drift/diffusion come from the paths' FINAL margins (the
    # last snapshot of each path), so real and simulated paths fit alike.
    # Sorted before reducing: group_by row order is nondeterministic, and
    # a different float-sum order shifts mu/sigma by ULPs run-to-run.
    finals = (
        paths.group_by("path_id").agg(pl.col("margin").last().cast(pl.Float64).alias("final_margin")).sort("path_id")
    )
    drift_mu = float(finals["final_margin"].mean())
    drift_sigma = max(1e-6, float(finals["final_margin"].std(ddof=0) or 0.0))

    seed_surface = WPSurface(
        time_bin_seconds=float(time_bin_seconds),
        margin_cap=int(margin_cap),
        shrinkage=float(shrinkage),
        total_seconds=game_seconds,
        drift_mu=drift_mu,
        drift_sigma=drift_sigma,
        cells={},
        n_paths=int(paths["path_id"].n_unique()),
    )

    binned = paths.with_columns(
        (pl.col("seconds_remaining").clip(lower_bound=0.0) // time_bin_seconds).cast(pl.Int64).alias("t_bucket"),
        pl.col("margin").clip(-margin_cap, margin_cap).cast(pl.Int64).alias("m"),
        pl.col("home_win").cast(pl.Float64).alias("win"),
    )
    cells: Dict[Tuple[int, int], Tuple[float, int]] = {}
    cell_frame = binned.group_by("t_bucket", "m").agg(pl.col("win").mean().alias("wp"), pl.len().alias("n"))
    for row in cell_frame.iter_rows(named=True):
        t_bucket, m, n = int(row["t_bucket"]), int(row["m"]), int(row["n"])
        prior = seed_surface.prior((t_bucket + 0.5) * time_bin_seconds, m)
        rate = (n * float(row["wp"]) + shrinkage * prior) / (n + shrinkage) if shrinkage else float(row["wp"])
        cells[(t_bucket, m)] = (rate, n)
    return dataclasses.replace(seed_surface, cells=cells)


def held_out_calibration(
    shelf: Shelf,
    *,
    n_train: int = 80,
    n_eval: int = 40,
    seed: Optional[int] = 7,
    time_bin_seconds: float = 60.0,
    margin_cap: int = 24,
    shrinkage: float = 20.0,
    factors: Optional[FactorAdjustment] = None,
    rules: SportRules = NBA_RULES,
) -> BacktestResult:
    """Self-calibration at scale: fit on train paths, score held-out paths.

    Simulates ``n_train + n_eval`` paths in one seeded stream, fits the
    surface on the first ``n_train``, and Brier-scores its predictions over
    every held-out snapshot against that path's realized winner (coin-flip
    baseline). The result carries the reliability table.

    Args:
        shelf: The PMF shelf.
        n_train: Paths the surface is fitted on.
        n_eval: Held-out paths scored snapshot-by-snapshot.
        seed: RNG seed for the shared path stream.
        time_bin_seconds: Surface time-bucket width.
        margin_cap: Surface margin clip bound.
        shrinkage: Pseudo-count weight of the prior (see
            :func:`fit_wp_surface`).
        factors: Optional auditable PMF adjustment applied to every draw.
        rules: League clock structure.

    Returns:
        The :class:`~sportsdataverse.modeling.eval.backtest.BacktestResult`
        (``metric="brier"``; ``calibration`` is the reliability table).

    Raises:
        ValueError: When either path count is below 1.

    Example:
        Quick start::

            from sportsdataverse.nba.nba_possession_sim import held_out_calibration
            res = held_out_calibration(shelf, n_train=80, n_eval=40, seed=7)
            res.score, res.baseline.beat_baseline
    """
    if n_train < 1 or n_eval < 1:
        raise ValueError("n_train and n_eval must both be >= 1")
    paths = simulate_score_paths(shelf, n_sim=n_train + n_eval, seed=seed, factors=factors, rules=rules)
    surface = fit_wp_surface(
        paths.filter(pl.col("path_id") < n_train),
        time_bin_seconds=time_bin_seconds,
        margin_cap=margin_cap,
        shrinkage=shrinkage,
        total_seconds=rules.periods * rules.period_seconds,
    )
    holdout = paths.filter(pl.col("path_id") >= n_train)
    units = list(holdout.iter_rows(named=True))
    return backtest(
        units,
        lambda unit: surface.predict(unit["seconds_remaining"], unit["margin"]),
        lambda unit: 1.0 if unit["home_win"] else 0.0,
        metric="brier",
        label_fn=lambda unit: unit["path_id"],
    )


def real_path_snapshots(pbp: pl.DataFrame, *, rules: SportRules = NBA_RULES) -> pl.DataFrame:
    """Realized ``playbyplayv3`` games as WP-surface snapshot paths.

    Args:
        pbp: Raw actions frame (one or more games) carrying ``game_id``,
            ``period``, ``clock``, ``scoreHome``, ``scoreAway`` — the shape
            the committed ``tests/fixtures/nba_engine`` captures load into.
        rules: League clock structure.

    Returns:
        Snapshot frame with the :func:`simulate_score_paths` schema
        (``path_id`` keeps the source ``game_id``): ``seconds_remaining``,
        ``margin`` (forward-filled running score), ``home_win`` (final
        margin of that game).

    Raises:
        ValueError: When a required column is absent.

    Example:
        Score a fitted surface on a realized game::

            snaps = real_path_snapshots(raw_actions)
            preds = [surface.predict(s["seconds_remaining"], s["margin"])
                     for s in snaps.iter_rows(named=True)]
    """
    required = ["game_id", "period", "clock", "scoreHome", "scoreAway"]
    missing = sorted(set(required) - set(pbp.columns))
    if missing:
        raise ValueError(f"pbp frame is missing columns: {missing}")
    clock_seconds = (
        pl.col("clock").cast(pl.Utf8).map_elements(parse_clock, return_dtype=pl.Float64).alias("clock_seconds")
    )
    period = pl.col("period").cast(pl.Int64)
    snapshots = (
        pbp.select(
            pl.col("game_id").alias("path_id"),
            period.alias("period"),
            clock_seconds,
            pl.col("scoreHome").cast(pl.Utf8).cast(pl.Int64, strict=False).forward_fill().over("game_id").alias("home"),
            pl.col("scoreAway").cast(pl.Utf8).cast(pl.Int64, strict=False).forward_fill().over("game_id").alias("away"),
        )
        .with_columns(pl.col("home").fill_null(0), pl.col("away").fill_null(0))
        .with_columns(
            _total_seconds_remaining(pl.col("period"), pl.col("clock_seconds"), rules).alias("seconds_remaining"),
            (pl.col("home") - pl.col("away")).alias("margin"),
        )
        .with_columns((pl.col("margin").last().over("path_id") > 0).alias("home_win"))
    )
    return snapshots.select("path_id", "seconds_remaining", "margin", "home_win")
