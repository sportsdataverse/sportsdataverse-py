"""Phase 0/2 backtest harness + the pregame-trio oracle gate (Task 0.4 / Task 2.3).

Gate rule (binding, per plan/spec): never lower a gate to make it pass --
debug the model. Floors are set from the observed value at gate time
(rounded to the safe side) and documented here.
"""

from __future__ import annotations

import importlib
from pathlib import Path

import polars as pl
import pytest

from sportsdataverse.nba.nba_game_predict import nba_predict_games
from sportsdataverse.nba.nba_prediction_constants import brier_score, mae

FIXTURE_DIR = Path(__file__).resolve().parents[1] / "fixtures" / "nba_prediction"


@pytest.fixture
def oracle_corpus() -> dict[str, pl.DataFrame]:
    """Load the committed 2023-24 fixture corpus into a dict keyed by fixture name."""
    names = [
        "results_2024",
        "team_box_2024",
        "player_box_logs_2024",
        "team_ratings_oracle_2024",
        "espn_predictor_sample_2024",
        "espn_odds_sample_2024",
        "winprob_sample_2024",
        "clutch_team_2023",
        "clutch_team_2024",
    ]
    return {name: pl.read_parquet(FIXTURE_DIR / f"{name}.parquet") for name in names}


def test_oracle_corpus_row_counts(oracle_corpus: dict[str, pl.DataFrame]) -> None:
    for name, df in oracle_corpus.items():
        if name == "winprob_sample_2024":
            continue  # documented dead-endpoint fixture; zero rows is the expected state
        assert df.height > 0, f"{name} fixture is unexpectedly empty"


def test_as_of_ratings_split_used_by_backtest(oracle_corpus: dict[str, pl.DataFrame]) -> None:
    from sportsdataverse.nba.nba_prediction_constants import as_of_ratings_split

    results = oracle_corpus["results_2024"]
    cutoff = results.sort("date")["date"][100]
    prior = as_of_ratings_split(results, cutoff)
    assert prior["date"].max() < cutoff


def test_pregame_trio_vs_espn_predictor_and_closing_line(monkeypatch, oracle_corpus: dict[str, pl.DataFrame]) -> None:
    results = oracle_corpus["results_2024"]
    team_box = oracle_corpus["team_box_2024"]
    pred = oracle_corpus["espn_predictor_sample_2024"]
    odds = oracle_corpus["espn_odds_sample_2024"].filter(pl.col("close_spread_home").is_not_null())

    # sportsdataverse.nba.__init__ imports the nba_team_ratings FUNCTION under the same
    # name as this module, shadowing `sportsdataverse.nba.nba_team_ratings` (the module)
    # via attribute lookup on `import ... as mod`; importlib.import_module bypasses that.
    ratings_mod = importlib.import_module("sportsdataverse.nba.nba_team_ratings")

    monkeypatch.setattr(ratings_mod, "load_nba_schedule", lambda seasons: results)
    monkeypatch.setattr(ratings_mod, "load_nba_team_boxscore", lambda seasons: team_box)

    from sportsdataverse.nba.nba_team_ratings import nba_team_ratings

    sample_games = pred.join(results.select("game_id", "date", "neutral_site"), on="game_id", how="inner").sort("date")

    rows = []
    for g in sample_games.iter_rows(named=True):
        as_of_ratings = nba_team_ratings(2024, league_id="00", as_of_date=g["date"])
        one_game = pl.DataFrame(
            {
                "game_id": [g["game_id"]],
                "home_team_id": [g["home_team_id"]],
                "away_team_id": [g["away_team_id"]],
                "neutral_site": [g["neutral_site"]],
            }
        )
        pred_row = nba_predict_games(one_game, as_of_ratings, league_id="00")
        if pred_row.height and pred_row["exp_margin"][0] is not None:
            rows.append(pred_row.row(0, named=True))
    preds = pl.DataFrame(rows)
    assert preds.height >= 40  # most of the 53-game sample should resolve (>=200-game warmup)

    joined_pred = preds.join(pred, on="game_id", how="inner")
    actual_home_win = joined_pred.join(
        results.select("game_id", "home_score", "away_score"), on="game_id"
    ).with_columns((pl.col("home_score") > pl.col("away_score")).cast(pl.Int64).alias("home_win"))
    y = actual_home_win["home_win"].to_numpy()
    p_mine = actual_home_win["home_win_prob"].to_numpy()
    p_espn = actual_home_win["home_win_prob_right"].to_numpy()

    brier_mine = brier_score(y, p_mine)
    brier_espn = brier_score(y, p_espn)
    # Observed at gate time (2026-07-08, 52-of-53-game predictor sample resolved,
    # as-of-date ratings): brier_mine=0.201, brier_espn=0.175 (diff 0.026). Floor
    # below is rounded up from that observed diff; do not lower.
    assert brier_mine <= brier_espn + 0.06, (
        f"model Brier {brier_mine:.3f} exceeds ESPN-predictor Brier {brier_espn:.3f} + 0.06 tolerance"
    )

    joined_odds = preds.join(odds, on="game_id", how="inner")
    if joined_odds.height >= 10:
        # ESPN's close_spread_home is a bookmaker SPREAD (negative = home favored);
        # exp_margin is a MARGIN (positive = home favored) -- opposite sign convention,
        # confirmed against the raw ESPN payload (e.g. "GS -2.5" -> home_team_odds
        # close.pointSpread.value == -2.5 for a home-favored game). Negate to compare.
        mae_spread = mae(joined_odds["exp_margin"].to_numpy(), -joined_odds["close_spread_home"].to_numpy())
        mae_total = mae(joined_odds["exp_total"].to_numpy(), joined_odds["close_total"].to_numpy())
        # Observed at gate time (2026-07-08, 49-game odds sample overlapping the
        # resolved-prediction set): mae_spread=3.52, mae_total=6.53. Floors below are
        # rounded up from those observed values; do not lower.
        assert mae_spread <= 4.5, f"spread MAE {mae_spread:.3f} above 4.5-point floor"
        assert mae_total <= 8.0, f"total MAE {mae_total:.3f} above 8.0-point floor"
