"""Walk-forward backtest driver: the harness's leakage boundary.

The driver walks ``(season, week)`` in play order and hands the engine
only what a forecaster could have held at kickoff: ``history`` (completed
games strictly earlier in the walk) and ``slate`` (the current week's
games with the outcome columns physically removed). Features join only
through :class:`~sportsdataverse.wexp.store.VintageStore`, whose
``join_asof`` is structurally leak-free. Market columns (``p_close``,
``spread_close``, ...) stay on the slate deliberately — they are bet-time
information and the ``market_only`` control needs them.

CFB postseason weeks reset to 1 in ESPN schedules; the driver offsets any
non-regular ``season_type`` week by :data:`POSTSEASON_WEEK_OFFSET` so the
walk (and every vintage join keyed on the slate's ``week``) orders
postseason strictly after the regular season. Engines see the offset week.
"""

from __future__ import annotations

from pathlib import Path
from typing import Callable, Optional, Union

import polars as pl

from sportsdataverse.wexp.elo import EloConfig, elo_ratings
from sportsdataverse.wexp.scoring import append_results, score_probs
from sportsdataverse.wexp.store import VintageStore
from sportsdataverse.wexp.variants import VariantConfig, variant_hash

__all__ = [
    "OUTCOME_COLUMNS",
    "POSTSEASON_WEEK_OFFSET",
    "WeekPredictor",
    "elo_predictor",
    "normalize_walk_weeks",
    "run_backtest",
]

# Columns stripped from the slate before an engine sees it.
OUTCOME_COLUMNS = ("home_margin", "home_win")

# Added to the week of any non-regular season_type row. 25 exceeds every
# regular-season week in both leagues, so postseason sorts strictly after.
POSTSEASON_WEEK_OFFSET = 25

_REGULAR_LABELS = ("REG", "regular")


def normalize_walk_weeks(games: pl.DataFrame) -> pl.DataFrame:
    """Offset non-regular ``season_type`` weeks so walk order is monotone.

    CFB postseason weeks reset to 1 in ESPN schedules; adding
    :data:`POSTSEASON_WEEK_OFFSET` to any non-regular week makes
    ``(season, week)`` a strict play-order key in both leagues. Vintage
    builders must apply the same normalization so their ``as_of_week``
    axis matches the weeks the driver hands to engines.

    Args:
        games: Frame with ``week`` and ``season_type`` columns.

    Returns:
        The frame with ``week`` replaced by the normalized week.

    Example:
        Quick start::

            from sportsdataverse.wexp.backtest import normalize_walk_weeks
            walk = normalize_walk_weeks(oracle)
    """
    # idempotent: an already-offset postseason week (>= OFFSET) is kept as-is,
    # so double-normalizing (driver + vintage builder) cannot shift it again
    return games.with_columns(
        pl.when(pl.col("season_type").is_in(_REGULAR_LABELS) | (pl.col("week") >= POSTSEASON_WEEK_OFFSET))
        .then(pl.col("week"))
        .otherwise(pl.col("week") + POSTSEASON_WEEK_OFFSET)
        .alias("week")
    )


WeekPredictor = Callable[
    [pl.DataFrame, pl.DataFrame, Optional[VintageStore]],
    Union[pl.Series, "list[float]"],
]


def elo_predictor(config: EloConfig = EloConfig()) -> WeekPredictor:
    """Wrap the margin-Elo engine (Axis A1) as a week predictor.

    Each week the engine replays Elo over the completed history plus the
    outcome-less slate; :func:`~sportsdataverse.wexp.elo.elo_ratings`
    rates unplayed games without updating on them, so the per-week refit
    reconstructs the exact chronological walk.

    Args:
        config: Elo tunables.

    Returns:
        A predictor callable for :func:`run_backtest`.

    Example:
        Quick start::

            from sportsdataverse.wexp.backtest import elo_predictor, run_backtest
            probs, rows = run_backtest(oracle, elo_predictor(), model_id="elo")
    """
    cols = ["game_id", "season", "week", "home_team", "away_team", "neutral_site"]

    def predict(history: pl.DataFrame, slate: pl.DataFrame, store: Optional[VintageStore]) -> pl.Series:
        combined = pl.concat(
            [
                history.select(*cols, "home_margin"),
                slate.select(*cols).with_columns(home_margin=pl.lit(None, dtype=pl.Float64)),
            ]
        )
        return elo_ratings(combined, config).tail(slate.height)["p_home"]

    return predict


def run_backtest(
    oracle: pl.DataFrame,
    predict: WeekPredictor,
    *,
    model_id: str,
    variant: Optional[VariantConfig] = None,
    store: Optional[VintageStore] = None,
    vintage_policy: str = "observed",
    week_slice: str = "all",
    era: str = "all",
    path: Optional[Union[str, Path]] = None,
) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Walk an oracle frame forward week by week and score the predictions.

    For each ``(season, week)`` in walk order the engine receives
    ``(history, slate, store)`` — history holds only completed games
    strictly earlier in the walk; the slate has :data:`OUTCOME_COLUMNS`
    removed and its ``week`` postseason-normalized. Predictions are
    validated (length, range) and scored per season + pooled via
    :func:`~sportsdataverse.wexp.scoring.score_probs`, keyed by
    ``variant_hash(variant)``.

    Args:
        oracle: Market-oracle frame (one row per game, unique ``game_id``).
        predict: Week predictor returning one P(home) per slate row
            (nulls allowed for games the engine declines to rate).
        model_id: Stable model family identifier for the result rows.
        variant: Variant config; its hash keys the result rows
            (``"none"`` when omitted).
        store: Vintage feature store handed to the engine each week.
        vintage_policy: Vintage label for the result rows.
        week_slice: Game-slice label.
        era: Era label.
        path: If given, result rows are appended to this leaderboard parquet.

    Returns:
        ``(probs, rows)`` — the oracle with ``p_home`` appended (original
        row order and original ``week`` labels), and the result rows.

    Raises:
        ValueError: On duplicate ``game_id`` rows, a prediction length
            mismatch, or probabilities outside ``[0, 1]``.

    Example:
        Quick start::

            from sportsdataverse.wexp.backtest import elo_predictor, run_backtest
            probs, rows = run_backtest(oracle, elo_predictor(), model_id="elo",
                                       path="results/wexp/leaderboard.parquet")
    """
    if oracle.height == 0:
        raise ValueError("oracle is empty")
    if oracle["league"].n_unique() != 1:
        raise ValueError("oracle mixes leagues — run one league per backtest")
    if oracle["game_id"].n_unique() != oracle.height:
        raise ValueError("oracle has duplicate game_id rows — the prediction join would fan out")
    league = oracle["league"][0]
    walk = normalize_walk_weeks(oracle)
    preds: list[pl.DataFrame] = []
    for season, week in walk.select("season", "week").unique().sort("season", "week").iter_rows():
        history = walk.filter(
            ((pl.col("season") < season) | ((pl.col("season") == season) & (pl.col("week") < week)))
            & pl.col("home_margin").is_not_null()
        )
        slate = walk.filter((pl.col("season") == season) & (pl.col("week") == week)).drop(*OUTCOME_COLUMNS)
        raw = predict(history, slate, store)
        p = (raw if isinstance(raw, pl.Series) else pl.Series(raw)).cast(pl.Float64).fill_nan(None).alias("p_home")
        if p.len() != slate.height:
            raise ValueError(f"predictor length mismatch in {season} week {week}: {p.len()} != {slate.height}")
        if ((p < 0) | (p > 1)).any():
            raise ValueError(f"predictor emitted probabilities outside [0, 1] in {season} week {week}")
        preds.append(slate.select("game_id").with_columns(p))
    probs = oracle.join(pl.concat(preds), on="game_id", how="left")
    rows = score_probs(
        probs,
        "p_home",
        league=league,
        model_id=model_id,
        variant_hash=variant_hash(variant) if variant is not None else "none",
        vintage_policy=vintage_policy,
        week_slice=week_slice,
        era=era,
    )
    if path is not None:
        append_results(rows, path)
    return probs, rows
