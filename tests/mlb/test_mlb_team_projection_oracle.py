"""Team-projection as-of-date backtest gate on the real results corpus.

Corpus: tests/fixtures/mlb_game_state/results_corpus.parquet (1999-2002 April-
June windows, real statsapi schedule scores -- see
tests/fixtures/mlb_game_state/README.md for provenance).

Gate (never lower to pass -- debug the model; floors below are set from the
observed backtest on the committed corpus, documented at capture time):
  - pythagenpat: MAE(pythag_win_pct, realized win_pct) <= FLOOR_PYTHAG_MAE
  - Elo (strictly as-of-date -- mlb_team_elo only updates a team's rating
    after its own game is scored): Brier(home_win_prob_elo) <=
    Brier(constant home-win-rate baseline) - FLOOR_ELO_MARGIN
"""

import numpy as np
import polars as pl

from sportsdataverse.mlb.mlb_game_state_constants import brier_score, mae
from sportsdataverse.mlb.mlb_team_projection import mlb_pythagenpat_table, mlb_team_elo

FIXTURE_DIR = "tests/fixtures/mlb_game_state"

# Floors set from the observed backtest on the committed corpus (4726 games,
# 120 team-seasons, 1999-2002 April-June windows): observed pythagenpat MAE
# 0.0289, observed Elo-vs-baseline Brier margin 0.00277. Floors below give
# headroom for normal run-to-run noise in a re-captured corpus of similar
# size -- do not lower further to chase a marginal improvement.
FLOOR_PYTHAG_MAE = 0.035
FLOOR_ELO_BRIER_MARGIN = 0.0015


def test_pythagenpat_mae_vs_realized_win_pct():
    results = pl.read_parquet(f"{FIXTURE_DIR}/results_corpus.parquet")
    pythag = mlb_pythagenpat_table(results)
    assert pythag.height > 50, f"only {pythag.height} team-seasons -- corpus too small for a meaningful backtest"

    observed_mae = mae(pythag["pythag_win_pct"].to_numpy(), pythag["win_pct"].to_numpy())
    assert observed_mae <= FLOOR_PYTHAG_MAE, f"pythagenpat MAE = {observed_mae:.4f} (floor {FLOOR_PYTHAG_MAE})"


def test_elo_brier_beats_constant_baseline_as_of_date():
    results = pl.read_parquet(f"{FIXTURE_DIR}/results_corpus.parquet")
    elo = mlb_team_elo(results)  # as-of-date by construction -- see mlb_team_elo docstring
    home_won = results.select(
        "game_id", (pl.col("home_score") > pl.col("away_score")).cast(pl.Float64).alias("home_won")
    )
    assert elo.schema["game_id"] == home_won.schema["game_id"]
    chk = elo.join(home_won, on="game_id", how="inner")
    assert chk.height >= results.height - 5, f"only {chk.height}/{results.height} games matched on join"

    baseline_rate = float(chk["home_won"].mean())
    y = chk["home_won"].to_numpy()
    model_brier = brier_score(y, chk["home_win_prob_elo"].to_numpy())
    baseline_brier = brier_score(y, np.full(chk.height, baseline_rate))
    margin = baseline_brier - model_brier
    assert margin >= FLOOR_ELO_BRIER_MARGIN, (
        f"Elo Brier improvement over constant-home-rate baseline = {margin:.4f} (floor {FLOOR_ELO_BRIER_MARGIN})"
    )
