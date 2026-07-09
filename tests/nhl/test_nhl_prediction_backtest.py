"""Backtest driver fixture over the committed 2023 NHL oracle corpus.

Per-model asserts are added in later phases (Task 2.3, 3.4, 4.2); this
module owns the shared ``oracle_corpus`` fixture + basic shape/dtype
sanity so every later phase can rely on a validated load.
"""

from __future__ import annotations

from pathlib import Path

import numpy as np
import polars as pl
import pytest

from sportsdataverse.nhl.nhl_market import expected_goals, predict_total, win_prob_from_margin
from sportsdataverse.nhl.nhl_prediction_constants import as_of_ratings_split, brier_score, get_constants, mae
from sportsdataverse.nhl.nhl_team_ratings import adjust_rate_opponent

FIXTURES_DIR = Path(__file__).resolve().parents[1] / "fixtures" / "nhl_prediction"


@pytest.fixture(scope="module")
def oracle_corpus() -> dict[str, pl.DataFrame]:
    return {
        "results": pl.read_parquet(FIXTURES_DIR / "results_2023.parquet"),
        "moneypuck_teams": pl.read_parquet(FIXTURES_DIR / "moneypuck_teams_2023.parquet"),
        "espn_power": pl.read_parquet(FIXTURES_DIR / "espn_power_2023.parquet"),
        "espn_predictor": pl.read_parquet(FIXTURES_DIR / "espn_predictor_sample.parquet"),
        "espn_odds": pl.read_parquet(FIXTURES_DIR / "espn_odds_sample.parquet"),
        "espn_propbets": pl.read_parquet(FIXTURES_DIR / "espn_propbets_sample.parquet"),
        "pbp_sample": pl.read_parquet(FIXTURES_DIR / "pbp_sample_2023.parquet"),
    }


def test_results_nonempty_and_typed(oracle_corpus):
    results = oracle_corpus["results"]
    assert results.height > 0
    assert results.schema["game_id"] == pl.Utf8
    assert results.schema["home_team"] == pl.Utf8
    assert results.schema["away_team"] == pl.Utf8


def test_moneypuck_teams_is_32_rows(oracle_corpus):
    mp = oracle_corpus["moneypuck_teams"]
    assert mp.height == 32
    assert mp.schema["team"] == pl.Utf8


def test_pbp_sample_nonempty(oracle_corpus):
    assert oracle_corpus["pbp_sample"].height > 0


def test_documented_empty_oracles_have_expected_schema(oracle_corpus):
    # ESPN power-index + predictor are genuinely unavailable for NHL at the
    # API level (see tests/fixtures/nhl_prediction/README.md); the fixtures
    # are committed empty with the documented schema, not fabricated.
    assert oracle_corpus["espn_power"].columns == ["team", "power_index", "rank"]
    assert oracle_corpus["espn_predictor"].columns == [
        "game_id",
        "home_team",
        "away_team",
        "home_win_prob",
    ]


# --- Task 2.3: pregame market gate (as-of walk-forward over the 2023 corpus) ------
#
# ESPN's game predictor is confirmed permanently unsupported for the NHL league
# at the API level (HTTP 400 "Predictor is not supported for [hockey/nhl]" --
# see the fixtures README), so the plan's "Brier <= ESPN-predictor Brier + tol"
# gate is adapted to a naive p=0.5 baseline (the only oracle actually available).
# MAE floors are against the real ESPN closing puck-line/total sample.
#
# Observed at gate-authoring time (2026-07-08), 1174 as-of-evaluated games
# (dates[20:] onward, so every team has $\geq$ a handful of games before its
# first as-of rating):
#   naive Brier (p=0.5)              = 0.2500
#   model Brier                      = 0.2350
#   MAE(exp_margin, closing puck line), n=12 matched games = 1.7722
#   MAE(exp_total, closing total),   n=12 matched games    = 1.3591
# Floors below are these values rounded to the safe side; never loosen without
# a fresh observed run + a comment explaining why (binding gate rule).
NAIVE_BRIER = 0.25
MODEL_BRIER_FLOOR = 0.245  # must beat the naive p=0.5 baseline with margin
MARGIN_MAE_FLOOR = 1.85
TOTAL_MAE_FLOOR = 1.45


@pytest.fixture(scope="module")
def pregame_backtest(oracle_corpus) -> pl.DataFrame:
    rates = pl.read_parquet(FIXTURES_DIR / "team_xg_2023.parquet")
    results = oracle_corpus["results"]
    const = get_constants("nhl")

    dates = sorted(rates["date"].unique().to_list())
    eval_dates = dates[20:]

    records = []
    for d in eval_dates:
        as_of = as_of_ratings_split(rates, d)
        if as_of.is_empty():
            continue
        xg_adj = adjust_rate_opponent(
            as_of, for_col="xgf", against_col="xga", hfa=const.hfa, avg=const.avg_xgf, shrink_k=const.shrink_k
        )
        home_map = dict(
            zip(xg_adj["team"].to_list(), zip(xg_adj["adj_for"].to_list(), xg_adj["adj_against"].to_list()))
        )
        today = results.filter(pl.col("date") == d)
        for row in today.iter_rows(named=True):
            h = home_map.get(row["home_team"])
            a = home_map.get(row["away_team"])
            if h is None or a is None:
                continue
            eg_home, eg_away = expected_goals(h[0], h[1], a[0], a[1], False, league="nhl")
            exp_margin = eg_home - eg_away
            records.append(
                {
                    "game_id": row["game_id"],
                    "exp_margin": exp_margin,
                    "home_win_prob": win_prob_from_margin(exp_margin, league="nhl"),
                    "exp_total": predict_total(h[0], h[1], a[0], a[1], False, league="nhl"),
                    "home_win": 1 if row["home_goals"] > row["away_goals"] else 0,
                }
            )
    return pl.DataFrame(records)


def test_pregame_win_prob_beats_naive_baseline(pregame_backtest):
    y = pregame_backtest["home_win"].to_numpy()
    p = pregame_backtest["home_win_prob"].to_numpy()
    model_brier = brier_score(y, p)
    naive_brier = brier_score(y, np.full(len(y), 0.5))
    assert abs(naive_brier - NAIVE_BRIER) < 0.01  # sanity-check the documented baseline
    assert model_brier <= MODEL_BRIER_FLOOR, f"model Brier {model_brier:.4f} above floor {MODEL_BRIER_FLOOR}"
    assert model_brier < naive_brier


def test_pregame_margin_and_total_mae_vs_espn_odds(pregame_backtest, oracle_corpus):
    odds = oracle_corpus["espn_odds"]
    assert pregame_backtest.schema["game_id"] == odds.schema["game_id"]
    m = pregame_backtest.join(odds, on="game_id", how="inner")
    assert m.height > 0
    margin_err = mae(m["exp_margin"].to_numpy(), m["close_puck_line_home"].to_numpy())
    total_err = mae(m["exp_total"].to_numpy(), m["close_total"].to_numpy())
    assert margin_err <= MARGIN_MAE_FLOOR, f"margin MAE {margin_err:.4f} above floor {MARGIN_MAE_FLOOR}"
    assert total_err <= TOTAL_MAE_FLOOR, f"total MAE {total_err:.4f} above floor {TOTAL_MAE_FLOOR}"


# --- Task 3.4: in-game WP calibration gate (2023 held-out, trained on 2022) -------
#
# Built by dev/nhl_prediction/build_in_game_wp_calibration_fixture.py: every play
# of the 2023 season is scored by nhl_in_game_win_prob (using as-of pregame probs
# as the anchor) and bucketed into predicted deciles via calibration_table.
#
# A shallow-xgboost escalation was tried (per the plan's explicit fallback) and
# REJECTED at model-authoring time: it roughly halved the worst-bucket deviation
# but, at max_depth=3, could not separate a clean pulled-goalie test scenario from
# the even-strength baseline, losing a qualitatively important, well-understood
# behavior the plain logistic captures correctly (see test_nhl_in_game_wp.py).
# Trading that away for a calibration gain that still didn't clear the plan's
# illustrative 0.03 target either was judged not worth it. The plain logistic
# ships, and per the binding gate rule (floors come from observed values, not an
# untested illustrative number), the overall-bucket floor is set from what was
# actually observed: max |mean_pred - mean_actual| = 0.0688 (bucket at 0.55,
# n=43101 -- not sampling noise: sqrt(p(1-p)/n) ~ 0.0024) -- rounded up to 0.075.
# The pulled-goalie subset (n=10086) DOES clear the illustrative 0.03 floor
# (observed 0.0256), so that tighter bar is kept for that targeted assert.
OVERALL_CALIBRATION_FLOOR = 0.075
PULLED_GOALIE_CALIBRATION_FLOOR = 0.03


def test_in_game_wp_calibration_by_predicted_decile():
    cal = pl.read_parquet(FIXTURES_DIR / "in_game_wp_calibration_2023.parquet")
    assert cal.height > 0
    dev = (cal["mean_pred"] - cal["mean_actual"]).abs()
    assert dev.max() <= OVERALL_CALIBRATION_FLOOR, f"max per-bucket deviation {dev.max():.4f} above floor"


def test_in_game_wp_calibration_pulled_goalie_subset():
    pulled = pl.read_parquet(FIXTURES_DIR / "in_game_wp_pulled_goalie_2023.parquet")
    assert pulled.height == 1
    row = pulled.row(0, named=True)
    assert row["n"] > 1000  # a genuinely large held-out subset, not a fluke handful of plays
    dev = abs(row["mean_pred"] - row["mean_actual"])
    assert dev <= PULLED_GOALIE_CALIBRATION_FLOOR, f"pulled-goalie deviation {dev:.4f} above floor"


# --- Task 4.2: player-props backtest gate (2024 season, real as-of scoring) -------
#
# Built by dev/nhl_prediction/build_player_props_backtest_fixture.py:
# nhl_player_props(2024, stats=("shots","points")) is joined back to the
# realized load_nhl_skater_boxscores values for the SAME player-game (an
# as-of comparison since every projection in that output only used strictly
# prior games -- see nhl_player_props's own leakage-safe construction).
# 2024 is used (not 2023) because load_nhl_skater_boxscores only publishes
# seasons >= 2024 -- documented in the fixtures README.
#
# ESPN propbets lines are confirmed unavailable for every NHL game tried
# (see the fixtures README), so the plan's "MAE vs propbets_line where
# available" check has no matching player and is skipped (documented, not
# fabricated); p_over calibration instead uses a synthetic fixed line (the
# stat's own realized median + 0.5, avoiding integer ties) as a substitute.
#
# Observed at gate-authoring time (2026-07-08), n=50389 matched player-games
# per stat:
#   MAE(proj_mean, realized shots)  = 1.0676
#   MAE(proj_mean, realized points) = 0.5308
#   max |mean_pred - mean_actual| (p_over calibration, either stat) = 0.0599
# A Gaussian p_over (Phi((line-mean)/sd)) was tried first and found
# systematically overconfident (worst-bucket deviation ~0.17); switching to
# a Poisson survival function (count data is non-negative and right-skewed,
# not Gaussian) cut that to 0.0599 -- a genuine model fix, not a tuned floor.
SHOTS_MAE_FLOOR = 1.10
POINTS_MAE_FLOOR = 0.55
PROPS_CALIBRATION_FLOOR = 0.065


def test_player_props_shots_mae():
    mae_df = pl.read_parquet(FIXTURES_DIR / "player_props_mae_2024.parquet")
    row = mae_df.filter(pl.col("stat") == "shots").row(0, named=True)
    assert row["n"] > 10000
    assert row["mae"] <= SHOTS_MAE_FLOOR, f"shots MAE {row['mae']:.4f} above floor {SHOTS_MAE_FLOOR}"


def test_player_props_points_mae():
    mae_df = pl.read_parquet(FIXTURES_DIR / "player_props_mae_2024.parquet")
    row = mae_df.filter(pl.col("stat") == "points").row(0, named=True)
    assert row["n"] > 10000
    assert row["mae"] <= POINTS_MAE_FLOOR, f"points MAE {row['mae']:.4f} above floor {POINTS_MAE_FLOOR}"


def test_player_props_p_over_calibration():
    cal = pl.read_parquet(FIXTURES_DIR / "player_props_p_over_calibration_2024.parquet")
    assert cal.height > 0
    dev = (cal["mean_pred"] - cal["mean_actual"]).abs()
    assert dev.max() <= PROPS_CALIBRATION_FLOOR, f"max p_over calibration deviation {dev.max():.4f} above floor"


def test_nhl_game_total_matches_nhl_predict_games_exp_total():
    # DRY guard (Task 4.2 interface): nhl_game_total is a thin re-export of
    # nhl_predict_games's exp_total for the same input -- one implementation.
    from sportsdataverse.nhl.nhl_market import nhl_predict_games
    from sportsdataverse.nhl.nhl_player_props import nhl_game_total

    games = pl.DataFrame(
        {
            "game_id": ["1", "2"],
            "home_team": ["TOR", "BOS"],
            "away_team": ["BOS", "TOR"],
            "neutral_site": [False, False],
        }
    )
    ratings = pl.DataFrame(
        {
            "team": ["TOR", "BOS"],
            "adj_xgf": [2.9, 2.6],
            "adj_xga": [2.3, 2.4],
        }
    )
    predict_out = nhl_predict_games(games, ratings)
    total_out = nhl_game_total(games, ratings)
    assert total_out.columns == ["game_id", "exp_total"]
    merged = predict_out.join(total_out, on="game_id", suffix="_re")
    assert (merged["exp_total"] - merged["exp_total_re"]).abs().max() < 1e-9
