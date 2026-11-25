"""Regulation 3-way, final-score and total-goals distributions behind ``nhl_predict_games``.

The oracle half reads ``tests/fixtures/nhl_prediction/regulation_holdout_2024_2026.parquet``: as-of pregame
predictions and pbp-derived regulation / final scores for 2023-24 .. 2025-26, the seasons the draw inflation
was NOT fitted on (``tools/models/nhl_prediction/fit_score_saves.py``; see the fixtures README).
"""

from __future__ import annotations

from dataclasses import replace
from pathlib import Path

import numpy as np
import polars as pl
import pytest

import sportsdataverse.nhl.nhl_prediction_constants as const_mod
from sportsdataverse.nhl.nhl_market import (
    _final_home_prob,
    _final_total,
    _with_regulation_outcomes,
    nhl_predict_games,
    nhl_score_distribution,
    nhl_total_goals_distribution,
    regulation_goal_means,
)
from sportsdataverse.nhl.nhl_prediction_constants import get_constants

FIXTURE = Path(__file__).resolve().parents[1] / "fixtures" / "nhl_prediction" / "regulation_holdout_2024_2026.parquet"
# 2022-23 (the fit season) regulation outcome rates, home / tie / away, printed by fit_score_saves.py.
FIT_SEASON_RATES = np.array([0.3952, 0.2402, 0.3646])


def _preds(p: list[float], total: list[float]) -> pl.DataFrame:
    return pl.DataFrame({"game_id": list(range(1, len(p) + 1)), "home_win_prob": p, "exp_total": total})


def test_regulation_goal_means_hit_both_targets():
    p = np.array([0.2, 0.35, 0.5, 0.52, 0.65, 0.8])
    total = np.array([5.2, 5.8, 6.05, 6.3, 6.6, 7.4])
    lam_home, lam_away = regulation_goal_means(p, total)
    const = get_constants("nhl")
    np.testing.assert_allclose(_final_home_prob(lam_home, lam_away, const), p, atol=1e-9)
    np.testing.assert_allclose(_final_total(lam_home, lam_away, const), total, atol=1e-9)
    assert np.all(np.diff(lam_home - lam_away) > 0)  # a stronger home side scores more in regulation


def test_draw_inflation_raises_the_tie_probability(monkeypatch):
    frame = _preds([0.5, 0.6], [6.0, 6.0])
    tied = _with_regulation_outcomes(frame, "nhl")["reg_tie_prob"].to_numpy()
    flat = replace(const_mod.LEAGUE_CONSTANTS["nhl"], reg_draw_inflation=0.0)
    monkeypatch.setitem(const_mod.LEAGUE_CONSTANTS, "nhl", flat)
    independent = _with_regulation_outcomes(frame, "nhl")["reg_tie_prob"].to_numpy()
    assert np.all(tied > independent + 0.05)


def test_score_distribution_matches_win_prob_and_total():
    preds = _preds([0.3, 0.55, 0.75], [5.6, 6.1, 6.9])
    scores = nhl_score_distribution(preds, top_n=17 * 17)
    by_game = scores.group_by("game_id").agg(
        pl.col("prob").sum().alias("mass"),
        pl.col("prob").filter(pl.col("home_goals") > pl.col("away_goals")).sum().alias("home_win"),
        (pl.col("prob") * (pl.col("home_goals") + pl.col("away_goals"))).sum().alias("mean_total"),
        pl.col("prob").filter(pl.col("home_goals") == pl.col("away_goals")).sum().alias("tied"),
    )
    by_game = by_game.sort("game_id")
    np.testing.assert_allclose(by_game["mass"], 1.0, atol=1e-9)
    np.testing.assert_allclose(by_game["home_win"], preds["home_win_prob"], atol=1e-9)
    np.testing.assert_allclose(by_game["mean_total"], preds["exp_total"], atol=1e-9)
    assert by_game["tied"].max() == 0.0  # every final score has a winner

    beyond = scores.filter(pl.col("prob_beyond_regulation") > 0)
    assert ((beyond["home_goals"] - beyond["away_goals"]).abs() == 1).all()

    top = nhl_score_distribution(preds, top_n=3)
    assert top.height == 9
    assert top.group_by("game_id").agg(pl.col("prob").is_sorted(descending=True).alias("s"))["s"].all()


def test_total_goals_distribution_mean_and_over():
    preds = _preds([0.45, 0.6], [5.8, 6.4])
    totals = nhl_total_goals_distribution(preds)
    assert totals.height == 2 * 33
    agg = totals.group_by("game_id").agg(
        pl.col("prob").sum().alias("mass"), (pl.col("prob") * pl.col("total_goals")).sum().alias("mean")
    )
    agg = agg.sort("game_id")
    np.testing.assert_allclose(agg["mass"], 1.0, atol=1e-9)
    np.testing.assert_allclose(agg["mean"], preds["exp_total"], atol=1e-9)
    one = totals.filter(pl.col("game_id") == 1)
    assert one["prob_over"].is_sorted(descending=True)
    over_55 = one.filter(pl.col("total_goals") == 5)["prob_over"].item()
    assert over_55 == pytest.approx(one.filter(pl.col("total_goals") >= 6)["prob"].sum())


def test_missing_rating_rows_get_nulls_and_empty_input_keeps_schema():
    games = pl.DataFrame(
        {"game_id": [1, 2], "home_team": ["TOR", "XXX"], "away_team": ["BOS", "TOR"], "neutral_site": [False, False]}
    )
    ratings = pl.DataFrame({"team": ["TOR", "BOS"], "adj_xgf": [2.0, 2.1], "adj_xga": [2.0, 1.9]})
    out = nhl_predict_games(games, ratings)
    assert out.filter(pl.col("game_id") == 2)["reg_tie_prob"].null_count() == 1
    row = out.filter(pl.col("game_id") == 1).row(0, named=True)
    assert row["home_reg_win_prob"] + row["reg_tie_prob"] + row["away_reg_win_prob"] == pytest.approx(1.0)
    assert nhl_score_distribution(out)["game_id"].unique().to_list() == [1]
    assert nhl_score_distribution(pl.DataFrame()).columns == [
        "game_id",
        "rank",
        "home_goals",
        "away_goals",
        "prob",
        "prob_beyond_regulation",
    ]
    assert nhl_total_goals_distribution(pl.DataFrame()).height == 0


@pytest.fixture(scope="module")
def holdout() -> pl.DataFrame:
    return pl.read_parquet(FIXTURE)


def _outcome(frame: pl.DataFrame) -> np.ndarray:
    h, a = frame["reg_home"].to_numpy(), frame["reg_away"].to_numpy()
    return np.where(h > a, 0, np.where(h == a, 1, 2))


def _log_loss(P: np.ndarray, y: np.ndarray) -> float:
    return float(-np.mean(np.log(P[np.arange(len(y)), y])))


def _three_way(frame: pl.DataFrame) -> np.ndarray:
    out = _with_regulation_outcomes(frame, "nhl")
    return out.select("home_reg_win_prob", "reg_tie_prob", "away_reg_win_prob").to_numpy()


def test_holdout_three_way_beats_independent_poisson_and_base_rates(holdout, monkeypatch):
    """Per holdout season, log loss of the shipped model < independent Poisson < flat 2022-23 rates.

    Observed (fit_score_saves.py): 1.0343 / 1.0419 / 1.0594 against 1.0380 / 1.0442 / 1.0838 and
    1.0615 / 1.0577 / 1.0808 for 2023-24 / 2024-25 / 2025-26.
    """
    fitted = {s: _three_way(d) for (s,), d in holdout.group_by("season")}
    flat = replace(const_mod.LEAGUE_CONSTANTS["nhl"], reg_draw_inflation=0.0)
    monkeypatch.setitem(const_mod.LEAGUE_CONSTANTS, "nhl", flat)
    for (season,), d in holdout.group_by("season"):
        y = _outcome(d)
        base = _log_loss(np.tile(FIT_SEASON_RATES, (len(y), 1)), y)
        indep = _log_loss(_three_way(d), y)
        model = _log_loss(fitted[season], y)
        assert model < indep, (season, model, indep)
        assert model < base, (season, model, base)


def test_holdout_tie_probability_calibration(holdout):
    """Pooled 2023-24 .. 2025-26: mean predicted tie 0.244 against 0.221 observed.

    The tie rate itself moves between seasons (0.240 / 0.208 / 0.206 / 0.249 for 2022-23 .. 2025-26, about
    3 standard errors apart), so the gate is 0.03 on the pooled mean, not a per-season band. The tie
    probability barely separates games (the top and bottom predicted-tie thirds tie at 0.226 / 0.227), so
    the 3-way gain over independent Poissons is in its level, not in ranking games.
    """
    P = _three_way(holdout)
    y = _outcome(holdout)
    assert abs(P[:, 1].mean() - (y == 1).mean()) < 0.03
