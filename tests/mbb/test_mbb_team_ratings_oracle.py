"""Phase-1 oracle gate: AdjEM vs barttorvik (2024 fixture, offline).

Builds the adjusted-efficiency ratings from the committed oracle corpus and
requires them to track barttorvik's AdjEM. Observed on the 2024 fixture:
Spearman 0.9519, MAE(AdjEM) 2.785. The gate is the plan's 0.95 floor plus an
MAE ceiling set from the observed value with margin.

**Never lower these gates to make the test pass** -- if they regress, debug the
possession formula / HFA sign / iteration convergence first.
"""

from __future__ import annotations

from pathlib import Path

import polars as pl

from sportsdataverse.mbb.mbb_prediction_constants import mae, spearman_corr
from sportsdataverse.mbb.mbb_team_ratings import adjust_efficiency, raw_game_efficiency

FIX = Path(__file__).resolve().parents[1] / "fixtures" / "mbb_prediction"

SPEARMAN_FLOOR = 0.95
ADJ_EM_MAE_CEILING = 3.5  # observed 2.785; ceiling with margin


def test_adjem_matches_torvik_2024():
    sched = pl.read_parquet(FIX / "results_2024.parquet")
    box = pl.read_parquet(FIX / "team_box_2024.parquet")
    tor = pl.read_parquet(FIX / "torvik_2024.parquet")

    ratings = adjust_efficiency(raw_game_efficiency(sched, box), league="mens")

    # join-key dtype agreement (ID discipline) before the oracle join
    assert ratings.schema["team_id"] == tor.schema["team_id"]
    joined = ratings.join(tor, on="team_id", how="inner", suffix="_tor")
    assert joined.height >= 300  # most of D1 matched to a torvik row

    mine = joined["adj_em"].to_numpy()
    oracle = joined["adj_em_tor"].to_numpy()
    spearman = spearman_corr(mine, oracle)
    adj_em_mae = mae(mine, oracle)

    assert spearman >= SPEARMAN_FLOOR, f"Spearman(adj_em, torvik) {spearman:.4f} < {SPEARMAN_FLOOR}"
    assert adj_em_mae <= ADJ_EM_MAE_CEILING, f"MAE(adj_em) {adj_em_mae:.3f} > {ADJ_EM_MAE_CEILING}"


def test_top_teams_are_plausible():
    sched = pl.read_parquet(FIX / "results_2024.parquet")
    box = pl.read_parquet(FIX / "team_box_2024.parquet")
    ratings = adjust_efficiency(raw_game_efficiency(sched, box), league="mens")
    # 2024 national champion UConn (ESPN team_id 41) should be a top-3 team
    top3 = ratings.sort("adj_em", descending=True).head(3)["team_id"].to_list()
    assert "41" in top3
