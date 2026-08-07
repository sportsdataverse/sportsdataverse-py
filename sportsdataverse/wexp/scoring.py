"""Scoring for the win-expectancy bake-off: ECE, FPI-style buckets, CLV, result rows.

Core proper-scoring metrics (Brier, log loss, MAE, Spearman, calibration
table) live in :mod:`sportsdataverse._common.metrics` and are re-exported
here so bake-off code imports from one place. This module adds only what
``_common`` lacks: expected calibration error, the favorite-perspective
bucket table (the FPI reporting pattern), closing line value, and the
append-only results-parquet contract keyed by
``(league, model_id, variant_hash, vintage_policy, season, week_slice, era)``.
"""

from __future__ import annotations

from pathlib import Path

import numpy as np
import polars as pl

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
from sportsdataverse._common.metrics import (
    spearman_corr as spearman_corr,
)

__all__ = [
    "RESULT_SCHEMA",
    "append_results",
    "brier_score",
    "calibration_table",
    "closing_line_value",
    "ece",
    "favorite_bucket_table",
    "log_loss_score",
    "mae",
    "result_rows",
    "spearman_corr",
    "winner_accuracy",
]

RESULT_SCHEMA: dict[str, pl.DataType] = {
    "league": pl.Utf8,
    "model_id": pl.Utf8,
    "variant_hash": pl.Utf8,
    "vintage_policy": pl.Utf8,
    "season": pl.Int32,
    "week_slice": pl.Utf8,
    "era": pl.Utf8,
    "metric": pl.Utf8,
    "value": pl.Float64,
    "n": pl.Int64,
}


def ece(y_true: np.ndarray, p_pred: np.ndarray, n_bins: int = 10) -> float:
    """Expected calibration error: n-weighted mean |mean_pred - mean_actual| per bin.

    Args:
        y_true: Array of binary outcomes (0/1).
        p_pred: Array of predicted probabilities in [0, 1].
        n_bins: Number of equal-width probability bins.

    Returns:
        The expected calibration error (0.0 is perfectly calibrated).

    Example:
        Quick start::

            import numpy as np
            from sportsdataverse.wexp.scoring import ece
            ece(np.array([1, 0, 1, 0]), np.array([0.9, 0.1, 0.8, 0.2]))
    """
    tbl = calibration_table(y_true, p_pred, n_bins=n_bins)
    if tbl.height == 0:
        return 0.0
    gaps = (tbl["mean_pred"] - tbl["mean_actual"]).abs()
    weights = tbl["n"] / tbl["n"].sum()
    return float((gaps * weights).sum())


def winner_accuracy(y_true: np.ndarray, p_pred: np.ndarray) -> float:
    """Fraction of games where the model's favorite (p >= 0.5) won.

    Args:
        y_true: Array of binary home-win outcomes (0/1).
        p_pred: Array of predicted home win probabilities.

    Returns:
        The winner accuracy in [0, 1].

    Example:
        Quick start::

            import numpy as np
            from sportsdataverse.wexp.scoring import winner_accuracy
            winner_accuracy(np.array([1, 0]), np.array([0.7, 0.6]))
    """
    y = np.asarray(y_true, dtype=float)
    p = np.asarray(p_pred, dtype=float)
    picks = (p >= 0.5).astype(float)
    return float(np.mean(picks == y))


def favorite_bucket_table(y_true: np.ndarray, p_pred: np.ndarray) -> pl.DataFrame:
    """FPI-style calibration buckets from the favorite's perspective.

    Folds every game to the predicted favorite (p_fav = max(p, 1-p)) and
    buckets into 50-60 / 60-70 / 70-80 / 80-90 / 90-100, comparing the mean
    predicted favorite probability to the favorite's actual win rate. The
    Phase-2 gate reads ``abs_gap`` on buckets with ``n >= 200``.

    Args:
        y_true: Array of binary home-win outcomes (0/1).
        p_pred: Array of predicted home win probabilities.

    Returns:
        A ``polars.DataFrame`` with columns ``bucket``, ``mean_pred``,
        ``mean_actual``, ``n``, ``abs_gap`` (one row per non-empty bucket).

    Example:
        Quick start::

            import numpy as np
            from sportsdataverse.wexp.scoring import favorite_bucket_table
            favorite_bucket_table(np.array([1, 0]), np.array([0.75, 0.4]))
    """
    y = np.asarray(y_true, dtype=float)
    p = np.asarray(p_pred, dtype=float)
    fav_home = p >= 0.5
    p_fav = np.where(fav_home, p, 1 - p)
    y_fav = np.where(fav_home, y, 1 - y)
    df = pl.DataFrame({"p": p_fav, "y": y_fav})
    df = df.with_columns((pl.col("p").clip(0.5, 0.9999) * 10).floor().cast(pl.Int64).alias("decile"))
    return (
        df.group_by("decile")
        .agg(
            pl.col("p").mean().alias("mean_pred"),
            pl.col("y").mean().alias("mean_actual"),
            pl.len().alias("n"),
        )
        .sort("decile")
        .with_columns(
            ((pl.col("decile") * 10).cast(pl.Utf8) + "-" + ((pl.col("decile") + 1) * 10).cast(pl.Utf8)).alias("bucket"),
            (pl.col("mean_pred") - pl.col("mean_actual")).abs().alias("abs_gap"),
        )
        .select("bucket", "mean_pred", "mean_actual", "n", "abs_gap")
    )


def closing_line_value(p_model: np.ndarray, p_bet: np.ndarray, p_close: np.ndarray) -> float:
    """Mean closing line value captured by betting the model's disagreements.

    Spec (documented, since no authoritative public one exists): for each
    game the model "bets" the side it likes more than the bet-time market
    (home if ``p_model > p_bet``, else away). CLV per game is the signed
    move of the vig-removed close toward that side:
    ``sign(p_model - p_bet) * (p_close - p_bet)``. Positive mean = the
    close moved toward the model's picks (the model bought value).

    Args:
        p_model: Model home win probabilities.
        p_bet: Vig-removed home probabilities at bet time (open or close).
        p_close: Vig-removed home probabilities at close.

    Returns:
        Mean CLV in probability points (positive = value captured).

    Example:
        Quick start::

            import numpy as np
            from sportsdataverse.wexp.scoring import closing_line_value
            closing_line_value(np.array([0.6]), np.array([0.5]), np.array([0.55]))
    """
    pm = np.asarray(p_model, dtype=float)
    pb = np.asarray(p_bet, dtype=float)
    pc = np.asarray(p_close, dtype=float)
    side = np.sign(pm - pb)
    return float(np.mean(side * (pc - pb)))


def result_rows(
    *,
    league: str,
    model_id: str,
    variant_hash: str,
    vintage_policy: str,
    season: int,
    week_slice: str,
    era: str,
    metrics: dict[str, float],
    n: int,
) -> pl.DataFrame:
    """Build long-format result rows (one row per metric) for the leaderboard.

    Args:
        league: League slug (``"nfl"`` / ``"cfb"``).
        model_id: Stable model family identifier.
        variant_hash: Stable hash of the full variant config.
        vintage_policy: Knowledge-cycle vintage (``"V0"``..``"V3"``).
        season: Season being scored.
        week_slice: Game slice label (``"all"``, ``"wk1-3"``, ...).
        era: Era label (``"covid"``, ``"post2017"``, ...).
        metrics: Mapping of metric name to value.
        n: Number of games scored.

    Returns:
        A ``polars.DataFrame`` matching :data:`RESULT_SCHEMA`.

    Example:
        Quick start::

            from sportsdataverse.wexp.scoring import result_rows
            result_rows(league="nfl", model_id="elo", variant_hash="a1",
                        vintage_policy="V2", season=2023, week_slice="all",
                        era="modern", metrics={"brier": 0.21}, n=272)
    """
    names = list(metrics)
    return pl.DataFrame(
        {
            "league": [league] * len(names),
            "model_id": [model_id] * len(names),
            "variant_hash": [variant_hash] * len(names),
            "vintage_policy": [vintage_policy] * len(names),
            "season": [season] * len(names),
            "week_slice": [week_slice] * len(names),
            "era": [era] * len(names),
            "metric": names,
            "value": [float(metrics[m]) for m in names],
            "n": [n] * len(names),
        },
        schema=RESULT_SCHEMA,
    )


def append_results(frame: pl.DataFrame, path: str | Path) -> None:
    """Append result rows to the append-only leaderboard parquet.

    Existing rows are never modified; the file is atomically replaced with
    existing + new. Schema is enforced against :data:`RESULT_SCHEMA`.

    Args:
        frame: Result rows (from :func:`result_rows`).
        path: Leaderboard parquet path (created on first call).

    Raises:
        ValueError: If ``frame`` does not match :data:`RESULT_SCHEMA`.

    Example:
        Quick start::

            from sportsdataverse.wexp.scoring import append_results, result_rows
            append_results(rows, "results/wexp/leaderboard.parquet")
    """
    if frame.schema != pl.Schema(RESULT_SCHEMA):
        raise ValueError(f"result frame schema mismatch: {frame.schema}")
    path = Path(path)
    if path.exists():
        frame = pl.concat([pl.read_parquet(path), frame], how="vertical")
    tmp = path.with_suffix(".parquet.tmp")
    path.parent.mkdir(parents=True, exist_ok=True)
    frame.write_parquet(tmp)
    tmp.replace(path)
