"""Regulation 3-way, final-score and total-goals distributions behind ``nhl_predict_games``.

The oracle half reads ``tests/fixtures/nhl_prediction/regulation_holdout_2024_2026.parquet``: as-of pregame
predictions and pbp-derived regulation / final scores for 2023-24 .. 2025-26. The shipped ``reg_draw_inflation``
is fitted on all four seasons 2022-23 .. 2025-26, so it is never scored here. Each season is scored leave one
season out instead: with the draw inflation and flat outcome rates ``tools/models/nhl_prediction/fit_score_saves.py``
fits on the other three seasons (see the fixtures README).

Leave one season out holds out the draw inflation only. A season's theta is fitted partly on later seasons,
and ``avg_total_goals`` (6.19) is the mean of exactly these three seasons, so the ``exp_total`` level the tie
probability rides on is in-sample. Both effects are small: a forward-only theta (2022-23 alone, 0.661)
still scores 1.0343 against 1.0380 on 2023-24, and moving every ``exp_total`` by -0.15 shifts the pooled
predicted tie by +0.003. The ``LOSO`` constants cannot be recomputed from the fixture (2022-23 is not in it);
``fit_score_saves.py`` prints them.
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
# Leave one season out, printed by fit_score_saves.py: for each held-out season, the draw-inflation MLE and the
# home / tie / away regulation rates on the other three of 2022-23 .. 2025-26.
LOSO = {
    2024: (0.5601, np.array([0.4182, 0.2315, 0.3502])),
    2025: (0.5696, np.array([0.4068, 0.2324, 0.3608])),
    2026: (0.4457, np.array([0.4266, 0.2178, 0.3556])),
}


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


def _three_way(frame: pl.DataFrame, theta: float, monkeypatch: pytest.MonkeyPatch) -> np.ndarray:
    monkeypatch.setitem(
        const_mod.LEAGUE_CONSTANTS, "nhl", replace(const_mod.LEAGUE_CONSTANTS["nhl"], reg_draw_inflation=theta)
    )
    out = _with_regulation_outcomes(frame, "nhl")
    return out.select("home_reg_win_prob", "reg_tie_prob", "away_reg_win_prob").to_numpy()


def test_leave_one_season_out_beats_independent_poisson_and_flat_rates(holdout, monkeypatch):
    """Each held-out season: log loss with its leave-one-out draw inflation < independent Poisson < flat rates.

    Observed (fit_score_saves.py): 1.0324 / 1.0401 / 1.0619 against 1.0380 / 1.0442 / 1.0838 for independent
    Poissons and 1.0598 / 1.0548 / 1.0834 for the other seasons' flat rates (2023-24 / 2024-25 / 2025-26).
    """
    for (season,), d in holdout.group_by("season"):
        theta, rates = LOSO[season]
        y = _outcome(d)
        model = _log_loss(_three_way(d, theta, monkeypatch), y)
        indep = _log_loss(_three_way(d, 0.0, monkeypatch), y)
        flat = _log_loss(np.tile(rates, (len(y), 1)), y)
        assert model < indep, (season, model, indep)
        assert model < flat, (season, model, flat)


def test_leave_one_season_out_tie_calibration(holdout, monkeypatch):
    """Pooled over the three held-out seasons: mean predicted tie 0.2285 against 0.2207 observed.

    The tie rate itself moves between seasons (0.240 / 0.208 / 0.206 / 0.249 for 2022-23 .. 2025-26, about
    3 standard errors apart), so per season the leave-one-out prediction runs 2.4 / 2.8 points high and 3.0
    low; the gate is on the pooled mean, 0.02 (about 3 pooled standard errors). The tie probability barely
    separates games (the top and bottom predicted-tie thirds tie at 0.226 / 0.227), so the gain over
    independent Poissons is in its level, not in ranking games.
    """
    predicted, tied = [], []
    for (season,), d in holdout.group_by("season"):
        predicted.append(_three_way(d, LOSO[season][0], monkeypatch)[:, 1])
        tied.append(_outcome(d) == 1)
    assert abs(np.concatenate(predicted).mean() - np.concatenate(tied).mean()) < 0.02


def _loso_frames(holdout: pl.DataFrame, monkeypatch: pytest.MonkeyPatch) -> tuple[pl.DataFrame, np.ndarray]:
    """Every held-out game's regulation columns under its own leave-one-out draw inflation, plus independent Poisson log loss."""
    parts, indep = [], []
    for (season,), d in holdout.group_by("season", maintain_order=True):
        for theta, sink in ((LOSO[season][0], parts), (0.0, indep)):
            monkeypatch.setitem(
                const_mod.LEAGUE_CONSTANTS, "nhl", replace(const_mod.LEAGUE_CONSTANTS["nhl"], reg_draw_inflation=theta)
            )
            sink.append(_with_regulation_outcomes(d, "nhl"))
    return pl.concat(parts), pl.concat(indep)


def test_leave_one_season_out_gain_over_independent_poisson_is_not_noise(holdout, monkeypatch):
    """Pooled paired log-loss gain over independent Poissons: 0.0105 +/- 0.0030 (3538 games).

    Per season the gain is 0.0057 / 0.0042 / 0.0220 (each +/- ~0.005), so two of three alone sit within one
    standard error; the pooled gain is the claim. Floor 0.0045 = observed minus two standard errors. Never
    lower a gate in this module to make a refit pass.
    """
    model, indep = _loso_frames(holdout, monkeypatch)
    cols = ["home_reg_win_prob", "reg_tie_prob", "away_reg_win_prob"]
    y = _outcome(model)
    rows = np.arange(len(y))
    gain = -np.log(indep.select(cols).to_numpy()[rows, y]) + np.log(model.select(cols).to_numpy()[rows, y])
    assert gain.mean() > 0.0045, gain.mean()


def test_leave_one_season_out_home_away_and_goal_levels(holdout, monkeypatch):
    """Pooled level of the other two classes and of the regulation goal means.

    Observed: home regulation win 0.4028 predicted against 0.4282, away 0.3687 against 0.3510 (gate 0.035;
    the home shortfall is inherited from ``home_win_prob``, which runs 0.521 against 0.569 on 2024-25);
    regulation goals 3.062 / 2.944 predicted against 3.097 / 2.859 (gate 0.15, about observed plus two
    standard errors).
    """
    model, _ = _loso_frames(holdout, monkeypatch)
    y = _outcome(model)
    assert abs(model["home_reg_win_prob"].mean() - (y == 0).mean()) < 0.035
    assert abs(model["away_reg_win_prob"].mean() - (y == 2).mean()) < 0.035
    assert abs(model["exp_home_reg_goals"].mean() - model["reg_home"].mean()) < 0.15
    assert abs(model["exp_away_reg_goals"].mean() - model["reg_away"].mean()) < 0.15


def test_pwhl_regulation_and_saves_terms_are_the_nhl_seed():
    """PWHL has no regulation-score or goalie-box fit yet: its row carries the NHL values on purpose.

    A PWHL fit (it needs a PWHL regulation-score and goalie-box fixture) must change this test deliberately.
    """
    nhl, pwhl = get_constants("nhl"), get_constants("pwhl")
    for field in (
        "reg_draw_inflation",
        "avg_shots_against",
        "league_shots_k",
        "shots_hfa",
        "starter_shot_share",
        "save_pct_prior",
        "save_pct_kappa",
        "shots_nb_size",
    ):
        assert getattr(pwhl, field) == getattr(nhl, field), field
