"""Backtest harness for the MBB prediction stack.

Task 0.4: the as-of-date leakage split + the shared ``oracle_corpus`` fixture.
Task 2.3: the pregame gates (win-prob Brier vs ESPN-BPI, spread/total MAE vs
the closing line) over a weekly as-of walk of the 2024 season.
"""

from __future__ import annotations

import datetime
from pathlib import Path

import numpy as np
import polars as pl
import pytest

from sportsdataverse.mbb.mbb_game_predict import mbb_in_game_win_prob, mbb_predict_games
from sportsdataverse.mbb.mbb_prediction_constants import (
    as_of_ratings_split,
    brier_score,
    calibration_table,
    mae,
)
from sportsdataverse.mbb.mbb_team_ratings import adjust_efficiency, adjust_tempo, raw_game_efficiency

FIX_DIR = Path(__file__).resolve().parents[1] / "fixtures" / "mbb_prediction"

_CORE_FIXTURES = ("results_2024", "team_box_2024", "torvik_2024")
_OPTIONAL_FIXTURES = ("espn_predictor_sample", "espn_odds_sample", "espn_bpi_2024", "pbp_sample_2024")

# A game enters the backtest only when both teams have this many prior games at
# the as-of date (data sufficiency for the in-season engine, mirrors
# dev/mbb_prediction/fit_pregame.py).
_MIN_PRIOR_GAMES = 8


@pytest.fixture
def oracle_corpus() -> dict[str, pl.DataFrame]:
    """Load the committed Task-0.1 oracle fixtures into a dict keyed by name.

    The three core fixtures (results / team_box / torvik) are always present;
    the ESPN per-game samples are loaded when present (captured in Phase 2/4).
    """
    corpus: dict[str, pl.DataFrame] = {}
    for name in (*_CORE_FIXTURES, *_OPTIONAL_FIXTURES):
        path = FIX_DIR / f"{name}.parquet"
        if path.exists():
            corpus[name] = pl.read_parquet(path)
    return corpus


def test_as_of_ratings_split_excludes_same_and_future():
    df = pl.DataFrame(
        {
            "game_id": ["a", "b", "c"],
            "date": [
                datetime.date(2024, 1, 1),
                datetime.date(2024, 1, 10),
                datetime.date(2024, 1, 20),
            ],
        }
    )
    out = as_of_ratings_split(df, datetime.date(2024, 1, 10))
    # strictly before cutoff: the 2024-01-10 game (same day) and later are excluded
    assert out["game_id"].to_list() == ["a"]


def test_oracle_corpus_core_fixtures_nonempty(oracle_corpus):
    for name in _CORE_FIXTURES:
        assert name in oracle_corpus, f"missing core fixture {name}"
        assert oracle_corpus[name].height > 0, f"empty core fixture {name}"


def test_oracle_corpus_id_columns_are_utf8(oracle_corpus):
    results = oracle_corpus["results_2024"]
    for col in ("game_id", "home_team_id", "away_team_id"):
        assert results.schema[col] == pl.Utf8
    assert oracle_corpus["torvik_2024"].schema["team_id"] == pl.Utf8


@pytest.fixture(scope="module")
def weekly_backtest() -> pl.DataFrame:
    """As-of pregame predictions for the 2024 season, weekly cutoffs.

    Ratings for each game are built from games strictly before the Monday of
    the game's week (cutoff <= game date, so the strictly-before rule holds).
    One row per eligible game: the ``mbb_predict_games`` output columns plus
    ``actual_margin`` / ``actual_total``.
    """
    results = pl.read_parquet(FIX_DIR / "results_2024.parquet").with_columns(
        pl.col("date").dt.truncate("1w").alias("cutoff")
    )
    box = pl.read_parquet(FIX_DIR / "team_box_2024.parquet")
    frames = []
    for (cutoff,), week in results.group_by("cutoff", maintain_order=True):
        prior = results.filter(pl.col("date") < cutoff)
        if prior.height < 300:  # engine has nothing useful in the opening days
            continue
        eff = raw_game_efficiency(prior, box.filter(pl.col("game_date") < cutoff))
        counts = eff.group_by("team_id").agg(pl.len().alias("n"))
        ratings = (
            adjust_efficiency(eff)
            .join(adjust_tempo(eff), on=["season", "team_id"])
            .select("team_id", "adj_o", "adj_d", "adj_em", "adj_tempo")
        )
        preds = mbb_predict_games(week.select("game_id", "home_team_id", "away_team_id", "neutral_site"), ratings)
        frames.append(
            preds.join(
                week.select(
                    "game_id",
                    (pl.col("home_score") - pl.col("away_score")).cast(pl.Float64).alias("actual_margin"),
                    (pl.col("home_score") + pl.col("away_score")).cast(pl.Float64).alias("actual_total"),
                ),
                on="game_id",
            )
            .join(counts.rename({"team_id": "home_team_id", "n": "home_n"}), on="home_team_id", how="left")
            .join(counts.rename({"team_id": "away_team_id", "n": "away_n"}), on="away_team_id", how="left")
        )
    return pl.concat(frames).filter(
        (pl.col("home_n") >= _MIN_PRIOR_GAMES)
        & (pl.col("away_n") >= _MIN_PRIOR_GAMES)
        & pl.col("exp_margin").is_not_null()
    )


def test_pregame_brier_beats_espn_bpi(weekly_backtest, oracle_corpus):
    """Task 2.3 gate: win-prob Brier <= ESPN-BPI predictor Brier + 0.01.

    Observed at fit time (weekly walk): ours 0.2006 vs ESPN 0.2031 (n=218).
    """
    pred = oracle_corpus["espn_predictor_sample"]
    j = weekly_backtest.join(pred.select("game_id", pl.col("home_win_prob").alias("espn_p")), on="game_id", how="inner")
    assert j.height >= 150, f"backtest/predictor intersection too small: {j.height}"
    y = (j.get_column("actual_margin").to_numpy() > 0).astype(float)
    b_ours = brier_score(y, j.get_column("home_win_prob").to_numpy())
    b_espn = brier_score(y, j.get_column("espn_p").to_numpy())
    assert b_ours <= b_espn + 0.01, f"brier ours={b_ours:.5f} espn={b_espn:.5f}"


def test_pregame_spread_mae_vs_closing_line(weekly_backtest, oracle_corpus):
    """Task 2.3 gate: exp_margin within 2.5 points MAE of the closing spread.

    Observed at fit time (weekly walk): 1.95 (n=218). ESPN quotes the home
    spread with negative = home favored, so the comparable margin is its
    negation.
    """
    odds = oracle_corpus["espn_odds_sample"]
    j = weekly_backtest.join(odds, on="game_id", how="inner")
    assert j.height >= 150, f"backtest/odds intersection too small: {j.height}"
    m = mae(j.get_column("exp_margin").to_numpy(), -j.get_column("close_spread_home").to_numpy())
    assert m <= 2.5, f"spread MAE vs close = {m:.3f}"


def test_in_game_wp_decile_calibration(oracle_corpus):
    """Task 3.4 gate: |mean_pred - mean_actual| <= 0.03 in every predicted decile.

    Out-of-sample by construction: the bundled artifact is trained on 2023,
    the sample is 2024 (every 25th play of all 4,326 eligible games -- a few
    plays from every game beats every play from few games, because plays
    within a game are correlated and per-decile power is driven by game
    count). Observed at train time: max gap 0.0298 (57,887 plays).
    """
    sample = oracle_corpus["pbp_sample_2024"]
    preds, obs = [], []
    for (_gid,), sub in sample.group_by("game_id", maintain_order=True):
        wp = mbb_in_game_win_prob(sub, float(sub["pregame_home_prob"][0]))
        preds.append(wp.get_column("home_win_prob").to_numpy())
        obs.append(sub.get_column("home_win").to_numpy())
    p, y = np.concatenate(preds), np.concatenate(obs)
    tbl = calibration_table(y, p, n_bins=10)
    gaps = (tbl.get_column("mean_pred") - tbl.get_column("mean_actual")).abs()
    assert float(gaps.max()) <= 0.03, str(tbl.with_columns(gaps.alias("gap")))


def test_pregame_total_mae_vs_closing_line(weekly_backtest, oracle_corpus):
    """Task 2.3 gate: exp_total within 3.5 points MAE of the closing total.

    Observed at fit time (weekly walk, fitted avg_tempo anchor): 2.90 (n=218).
    Also pins the calibration: mean(exp_total) within 2 points of the mean
    closing total (the seeded anchor was +6.4 biased before the fit).
    """
    odds = oracle_corpus["espn_odds_sample"]
    j = weekly_backtest.join(odds, on="game_id", how="inner")
    exp_t = j.get_column("exp_total").to_numpy()
    close_t = j.get_column("close_total").to_numpy()
    m = mae(exp_t, close_t)
    assert m <= 3.5, f"total MAE vs close = {m:.3f}"
    assert abs(float(np.mean(exp_t)) - float(np.mean(close_t))) <= 2.0
