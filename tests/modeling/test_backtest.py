"""Gates for the backtest harness — real in-game WP + harness mechanics.

The headline gate is real: it walks a fixture game's ACTUAL score/clock path,
predicts home win probability from the possession-sim shelf at each snapshot,
and scores those predictions against the realized winner — the in-game WP must
beat a coin-flip baseline and be nearly certain once the game is decided. The
harness plumbing (mae mode, reference ratio, default baselines, leakage split,
error paths) is pinned with small explicit arrays.
"""

from __future__ import annotations

import json
import pathlib

import numpy as np
import polars as pl
import pytest

from sportsdataverse.modeling.eval import backtest
from sportsdataverse.modeling.eval.backtest import as_of_holdout
from sportsdataverse.nba.nba_possession_sim import build_shelf, possessions_from_pbp
from sportsdataverse.nba.nba_possession_sim.engine import in_game_win_prob


def _real_snapshots(game_id: str, k: int = 10):
    payload = json.loads(
        pathlib.Path(f"tests/fixtures/nba_engine/{game_id}/playbyplayv3.json").read_text(encoding="utf-8")
    )
    acts = payload.get("game", {}).get("actions") or payload["actions"]
    raw = pl.DataFrame(acts, infer_schema_length=None).with_columns(pl.lit(game_id).alias("game_id"))
    scored = (
        raw.filter((pl.col("scoreHome").cast(pl.Utf8).str.len_chars() > 0) & pl.col("scoreHome").is_not_null())
        .with_columns(
            pl.col("scoreHome").cast(pl.Int64).alias("sh"),
            pl.col("scoreAway").cast(pl.Int64).alias("sa"),
            (
                pl.col("clock").str.extract(r"PT(\d+)M", 1).cast(pl.Int64) * 60
                + pl.col("clock").str.extract(r"M([\d.]+)S", 1).cast(pl.Float64)
            ).alias("clock_seconds"),
        )
        .select("period", "clock_seconds", "sh", "sa")
        .drop_nulls()
    )
    final = scored.tail(1).row(0, named=True)
    home_won = 1.0 if final["sh"] > final["sa"] else 0.0
    # sample k snapshots evenly across the scored path
    idx = np.linspace(0, scored.height - 1, num=min(k, scored.height)).round().astype(int)
    snaps = [scored.row(int(i), named=True) for i in dict.fromkeys(idx.tolist())]
    return raw, snaps, home_won


def test_backtest_scores_ingame_wp_on_real_game() -> None:
    raw, snaps, home_won = _real_snapshots("0022300001", k=10)
    assert home_won == 1.0 and len(snaps) >= 6
    shelf = build_shelf(possessions_from_pbp(raw))

    def predict(s: dict) -> float:
        return in_game_win_prob(
            shelf,
            score_home=int(s["sh"]),
            score_away=int(s["sa"]),
            period=int(s["period"]),
            clock_seconds=float(s["clock_seconds"]),
            offense_is_home=False,
            n_sim=150,
            seed=7,
        )

    res = backtest(
        snaps,
        predict,
        lambda _s: home_won,
        metric="brier",
        baseline_fn=lambda _s: 0.5,
        label_fn=lambda s: f"P{s['period']}:{int(s['clock_seconds'])}",
    )
    # the WP forecast beats a coin flip and the beat-baseline gate agrees
    assert res.score < 0.25
    assert res.baseline.beat_baseline and res.baseline.delta < 0
    assert res.n == len(snaps)
    assert res.calibration is not None and res.calibration.height >= 1
    # once the game is effectively over (home leading, clock ~0), WP is near-certain
    last = res.predictions.sort("prediction").tail(1).row(0, named=True)
    assert last["prediction"] > 0.9
    # deterministic under a fixed seed
    res2 = backtest(snaps, predict, lambda _s: home_won, metric="brier", baseline_fn=lambda _s: 0.5)
    assert res2.score == res.score


def test_as_of_holdout_enforces_boundary() -> None:
    import datetime as dt

    games = pl.DataFrame(
        {
            "game_id": ["a", "b", "c", "d"],
            "date": [dt.date(2023, 12, 1), dt.date(2023, 12, 20), dt.date(2024, 1, 5), dt.date(2024, 1, 9)],
        }
    )
    history, holdout = as_of_holdout(games, as_of=dt.date(2024, 1, 1))
    assert history["game_id"].to_list() == ["a", "b"]
    assert holdout["game_id"].to_list() == ["c", "d"]
    assert set(history["game_id"]).isdisjoint(set(holdout["game_id"]))
    with pytest.raises(ValueError, match="date_key"):
        as_of_holdout(games, as_of=dt.date(2024, 1, 1), date_key="missing")


def test_harness_mechanics() -> None:
    # mae mode with a default mean baseline
    units = [(0.0, 2.0), (1.0, 3.0), (2.0, 8.0)]  # (pred, actual)
    res = backtest(units, lambda u: u[0], lambda u: u[1], metric="mae")
    assert res.metric == "mae" and res.calibration is None
    assert res.predictions["baseline"].to_list() == [pytest.approx(np.mean([2.0, 3.0, 8.0]))] * 3
    # reference ratio: a perfect reference makes the ratio large (model worse)
    probs = [(0.6, 1.0), (0.4, 0.0), (0.7, 1.0)]
    ref = backtest(
        probs,
        lambda u: u[0],
        lambda u: u[1],
        metric="brier",
        reference_fn=lambda u: u[1],  # oracle reference
    )
    assert ref.reference_ratio is not None and ref.reference_ratio > 1.0
    # error paths
    with pytest.raises(ValueError, match="at least one unit"):
        backtest([], lambda u: 0.5, lambda u: 1.0)
    with pytest.raises(ValueError, match="unknown metric"):
        backtest(probs, lambda u: u[0], lambda u: u[1], metric="nope")
