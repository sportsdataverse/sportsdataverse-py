"""Baseline oracles (RESEARCH §9.3) + the leaderboard scoring driver.

``baseline_probs`` appends per-game baseline probability columns to an
oracle frame; ``score_baselines`` scores every ``p_*`` column per season
and pooled into the append-only leaderboard contract. Frozen-V1 preseason
and the CFB AP-poll rule join once Phase-1 priors / rankings data land.
"""

from __future__ import annotations

from pathlib import Path

import polars as pl

from sportsdataverse.wexp.oracle_market import (
    CFB_MARGIN_SIGMA,
    NFL_MARGIN_SIGMA,
    _p_spread_series,
)
from sportsdataverse.wexp.scoring import append_results, score_probs

__all__ = ["BASELINE_HOME_RATE", "baseline_probs", "score_baselines"]

# Seed home-win rates for the home-rule baseline (RESEARCH: NFL home ~0.589
# when evenly matched; CFB home rates run higher). Tuned on the train slice
# in Phase 2 — these are seeds, not fitted constants.
BASELINE_HOME_RATE: dict[str, float] = {"nfl": 0.57, "cfb": 0.60}

_SIGMA: dict[str, float] = {"nfl": NFL_MARGIN_SIGMA, "cfb": CFB_MARGIN_SIGMA}


def baseline_probs(
    oracle: pl.DataFrame,
    *,
    elo: pl.DataFrame | None = None,
    home_rate: float | None = None,
    sigma: float | None = None,
) -> pl.DataFrame:
    """Append baseline probability columns to a market-oracle frame.

    Columns added: ``p_coin_flip`` (0.5), ``p_home_rule`` (constant home
    rate; 0.5 at neutral sites), ``p_market_close`` (= ``p_close``),
    ``p_market_open`` (normal link on ``spread_open``; null where the era
    holds no open — never imputed), and ``p_elo`` when an Elo frame is
    supplied.

    Args:
        oracle: Frame from an oracle builder (module contract columns).
        elo: Optional output of :func:`sportsdataverse.wexp.elo.elo_ratings`
            (joined on ``game_id``).
        home_rate: Home-rule probability; defaults to the league seed in
            :data:`BASELINE_HOME_RATE`.
        sigma: Margin SD for the open-spread link; defaults per league.

    Returns:
        ``oracle`` with the baseline ``p_*`` columns appended.

    Example:
        Quick start::

            from sportsdataverse.wexp.baselines import baseline_probs
            probs = baseline_probs(oracle, elo=elo_frame)
    """
    league = oracle["league"][0]
    rate = BASELINE_HOME_RATE[league] if home_rate is None else home_rate
    sig = _SIGMA[league] if sigma is None else sigma
    open_probs = _p_spread_series(oracle.select(pl.col("spread_open").alias("spread_close")), sig).alias(
        "p_market_open"
    )
    out = oracle.with_columns(
        p_coin_flip=pl.lit(0.5),
        p_home_rule=pl.when(pl.col("neutral_site") == True)  # noqa: E712
        .then(0.5)
        .otherwise(rate),
        p_market_close=pl.col("p_close"),
        p_market_open=open_probs,
    )
    if elo is not None:
        if out.schema["game_id"] != elo.schema["game_id"]:
            raise ValueError(
                f"join-key dtype mismatch: oracle game_id {out.schema['game_id']} vs elo {elo.schema['game_id']}"
            )
        out = out.join(elo.select("game_id", p_elo=pl.col("p_home")), on="game_id", how="left")
    return out


def score_baselines(
    probs: pl.DataFrame,
    *,
    vintage_policy: str = "observed",
    week_slice: str = "all",
    era: str = "all",
    path: str | Path | None = None,
) -> pl.DataFrame:
    """Score every ``p_*`` baseline column per season and pooled.

    Pooled rows use ``season = -1``. Metrics per model/season: ``brier``,
    ``log_loss``, ``winner_accuracy``, ``ece``. Ties (null ``home_win``)
    and null probabilities are dropped per model, so the open-market
    baseline is scored only on games that actually held an open.

    Args:
        probs: Output of :func:`baseline_probs`.
        vintage_policy: Vintage label for the result rows.
        week_slice: Game-slice label.
        era: Era label.
        path: If given, rows are appended to this leaderboard parquet.

    Returns:
        The result rows (long format, RESULT_SCHEMA).

    Example:
        Quick start::

            from sportsdataverse.wexp.baselines import score_baselines
            rows = score_baselines(probs, path="results/wexp/leaderboard.parquet")
    """
    league = probs["league"][0]
    model_cols = [
        c for c in probs.columns if c.startswith("p_") and c not in ("p_close_spread", "p_close_ml", "p_close")
    ]
    rows = pl.concat(
        [
            score_probs(
                probs,
                col,
                league=league,
                model_id=col.removeprefix("p_"),
                variant_hash="baseline",
                vintage_policy=vintage_policy,
                week_slice=week_slice,
                era=era,
            )
            for col in model_cols
        ],
        how="vertical",
    )
    if path is not None:
        append_results(rows, path)
    return rows
