"""Gates for the learned gamestate keyer, on real-fixture possessions.

Leave-one-game-out on the three committed captures: a learned-keyed shelf
must beat the hand-cut bins on held-out possession-outcome log-loss AND
exhibit the structural properties the coverage floor buys — no leaf below
``min_samples_leaf``, zero global-PMF fallback (every state maps to a
leaf), and exact agreement with sklearn's own leaf assignment. Thresholds
were pinned from observed values (learned LL 2.28-2.62 vs hand 10.2-12.6;
the hand-cut gap is real: sparse hand cells assign exact zero to outcomes
that then occur) — never lower them.
"""

from __future__ import annotations

import math

import numpy as np
import polars as pl
import pytest

from sportsdataverse.modeling.features.learned_bins import fit_learned_bins
from sportsdataverse.nba.nba_possession_sim import (
    build_shelf,
    fit_learned_gamestate_keyer,
    gamestate_key,
    possessions_from_pbp,
    shelf_to_parquet,
    simulate_ensemble,
)
from tools.calibration import build as calibration_build


@pytest.fixture(scope="module")
def poss() -> pl.DataFrame:
    return possessions_from_pbp(calibration_build.fixture_raw())


def _holdout_log_loss(shelf, holdout: pl.DataFrame) -> tuple[float, float]:
    total, n, fallbacks = 0.0, 0, 0
    for row in holdout.iter_rows(named=True):
        key = shelf.key_for(row["score_diff"], row["period"], row["clock_seconds"])
        pmf, fell = shelf.get_pmf(key)
        if fell:
            fallbacks += 1
        total -= math.log(max(1e-12, pmf.get(row["outcome"], 0.0)))
        n += 1
    return total / n, fallbacks / n


def test_keyer_matches_sklearn_assignment_exactly(poss: pl.DataFrame) -> None:
    keyer = fit_learned_gamestate_keyer(poss)
    outcomes = poss.filter(pl.col("kind") == "outcome")
    bins = fit_learned_bins(
        outcomes,
        features=["score_diff", "period", "clock_seconds"],
        target="outcome",
    )
    assigned = bins.assign(outcomes.select("score_diff", "period", "clock_seconds"))
    for row, leaf in zip(outcomes.iter_rows(named=True), assigned.to_list()):
        assert keyer.key(row["score_diff"], row["period"], row["clock_seconds"]) == f"leaf{leaf}"


def test_leaf_coverage_floor_and_audit_table(poss: pl.DataFrame) -> None:
    keyer = fit_learned_gamestate_keyer(poss)
    table = keyer.leaf_table
    assert int(table["n"].min()) >= 50  # the estimability floor (observed 51)
    assert table["rule"].null_count() == 0
    assert (table["rule"].str.len_chars() > 0).all()
    prob_cols = [c for c in table.columns if c.startswith("p_")]
    assert prob_cols
    sums = table.select(pl.sum_horizontal(prob_cols).alias("s"))["s"]
    assert np.allclose(sums.to_numpy(), 1.0)


def test_leave_one_game_out_beats_hand_cut_bins(poss: pl.DataFrame) -> None:
    games = sorted(poss["game_id"].unique().to_list())
    assert len(games) == 3
    for held in games:
        train = poss.filter(pl.col("game_id") != held)
        holdout = poss.filter((pl.col("game_id") == held) & (pl.col("kind") == "outcome"))
        hand_shelf = build_shelf(train)
        keyer = fit_learned_gamestate_keyer(train)  # TRAIN games only — no leakage
        learned_shelf = build_shelf(train, keyer=keyer)
        hand_ll, hand_fb = _holdout_log_loss(hand_shelf, holdout)
        learned_ll, learned_fb = _holdout_log_loss(learned_shelf, holdout)
        assert learned_ll < hand_ll  # observed 2.28-2.62 vs 10.2-12.6
        assert learned_ll < 3.0
        # structural: every gamestate maps to a leaf — the learned shelf
        # NEVER serves the global fallback; hand-cut bins routinely do
        assert learned_fb == 0.0
        assert hand_fb > 0.0


def test_learned_shelf_simulates_deterministically(poss: pl.DataFrame) -> None:
    keyer = fit_learned_gamestate_keyer(poss)
    shelf = build_shelf(poss, keyer=keyer)
    first = simulate_ensemble(shelf, n_sim=20, seed=7)
    second = simulate_ensemble(shelf, n_sim=20, seed=7)
    assert np.array_equal(first["score_home"], second["score_home"])
    assert np.array_equal(first["score_away"], second["score_away"])
    assert 180.0 < first["mean_total"] < 320.0
    assert shelf.fallback_rate() == 0.0
    # refitting with the same random_state reproduces the exact tree
    again = fit_learned_gamestate_keyer(poss)
    assert again.feature_index == keyer.feature_index
    assert again.left == keyer.left and again.right == keyer.right


def test_default_path_and_guards(poss: pl.DataFrame, tmp_path) -> None:
    hand_shelf = build_shelf(poss)
    assert hand_shelf.keyer is None
    for state in ((-9.0, 3, 75.0), (0.0, 1, 700.0), (12.0, 4, 30.0)):
        assert hand_shelf.key_for(*state) == gamestate_key(*state)
    keyer = fit_learned_gamestate_keyer(poss)
    learned_shelf = build_shelf(poss, keyer=keyer)
    with pytest.raises(ValueError, match="round-trip"):
        shelf_to_parquet(learned_shelf, tmp_path / "shelf.parquet")
    with pytest.raises(ValueError, match="no outcome events"):
        fit_learned_gamestate_keyer(poss.filter(pl.col("kind") == "rebound"))
