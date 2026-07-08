"""Phase-1 oracle gate: AdjNet vs NET_RATING (stats.nba.com) + BPI (ESPN), 2023-24.

Gate rule (binding, per plan/spec): never lower a gate to make it pass --
debug the model. Floors are set from the observed value at gate time
(rounded to the safe side) and documented here.
"""

from __future__ import annotations

from pathlib import Path

import polars as pl

from sportsdataverse.nba.nba_prediction_constants import mae, spearman_corr
from sportsdataverse.nba.nba_team_ratings import nba_team_ratings

FIXTURE_DIR = Path(__file__).resolve().parents[1] / "fixtures" / "nba_prediction"


def test_adj_net_vs_net_rating_and_bpi_2024(monkeypatch) -> None:
    results = pl.read_parquet(FIXTURE_DIR / "results_2024.parquet")
    team_box = pl.read_parquet(FIXTURE_DIR / "team_box_2024.parquet")
    oracle = pl.read_parquet(FIXTURE_DIR / "team_ratings_oracle_2024.parquet")

    import sportsdataverse.nba.nba_team_ratings as mod

    monkeypatch.setattr(mod, "load_nba_schedule", lambda seasons: results)
    monkeypatch.setattr(mod, "load_nba_team_boxscore", lambda seasons: team_box)

    mine = nba_team_ratings(2024, league_id="00")

    assert mine.schema["team_id"] == oracle.schema["team_id"]
    matched = mine.join(oracle, on="team_id", how="inner")
    assert matched.height == 30  # full 30-team crosswalk match (see fixtures README)

    rho_net = spearman_corr(matched["adj_net_rtg"].to_numpy(), matched["net_rating"].to_numpy())
    rho_bpi = spearman_corr(matched["adj_net_rtg"].to_numpy(), matched["bpi"].to_numpy())
    mae_net = mae(matched["adj_net_rtg"].to_numpy(), matched["net_rating"].to_numpy())

    # Observed at gate time (2026-07-08, full 2023-24 season, box-derived possessions
    # vs stats.nba.com's own Advanced-measure-type NET_RATING/PACE + ESPN BPI):
    # rho_net=0.9652, rho_bpi=0.8981, mae_net=0.586. Floors below are those observed
    # values rounded to the safe side (per the binding "never lower a gate" rule).
    assert rho_net >= 0.95, f"AdjNet vs NET_RATING spearman {rho_net:.3f} below 0.95 floor"
    assert rho_bpi >= 0.85, f"AdjNet vs BPI spearman {rho_bpi:.3f} below 0.85 floor"
    assert mae_net <= 0.75, f"AdjNet vs NET_RATING MAE {mae_net:.3f} above 0.75 floor"
