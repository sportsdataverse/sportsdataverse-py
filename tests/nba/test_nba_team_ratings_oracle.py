"""Phase-1 oracle gate: AdjNet vs NET_RATING (stats.nba.com) + BPI (ESPN), 2023-24.

Gate rule (binding, per plan/spec): never lower a gate to make it pass --
debug the model. Floors are set from the observed value at gate time
(rounded to the safe side) and documented here.
"""

from __future__ import annotations

import importlib
from pathlib import Path

import numpy as np
import polars as pl

from sportsdataverse.nba.nba_prediction_constants import mae, spearman_corr
from sportsdataverse.nba.nba_team_ratings import nba_team_ratings

FIXTURE_DIR = Path(__file__).resolve().parents[1] / "fixtures" / "nba_prediction"


def _ratings_2024(monkeypatch) -> pl.DataFrame:
    results = pl.read_parquet(FIXTURE_DIR / "results_2024.parquet")
    team_box = pl.read_parquet(FIXTURE_DIR / "team_box_2024.parquet")

    # sportsdataverse.nba.__init__ imports the nba_team_ratings FUNCTION under the same
    # name as this module, shadowing `sportsdataverse.nba.nba_team_ratings` (the module)
    # via attribute lookup on `import ... as mod`; importlib.import_module bypasses that.
    mod = importlib.import_module("sportsdataverse.nba.nba_team_ratings")

    monkeypatch.setattr(mod, "load_nba_schedule", lambda seasons: results)
    monkeypatch.setattr(mod, "load_nba_team_boxscore", lambda seasons: team_box)

    return nba_team_ratings(2024, league_id="00")


def test_adj_net_vs_net_rating_and_bpi_2024(monkeypatch) -> None:
    oracle = pl.read_parquet(FIXTURE_DIR / "team_ratings_oracle_2024.parquet")
    mine = _ratings_2024(monkeypatch)

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


# --- Level gates beside the rank gates (C-L22) -----------------------------------------
# The gate above is Spearman + an MAE against stats.nba.com's NET_RATING, which is NOT
# opponent-adjusted: skipping the opponent adjustment entirely (max_iter -> 0) moves
# adj_net CLOSER to it (MAE 0.586 -> 0.396) and still clears rho >= 0.95 / bpi >= 0.85.
# Observed 2026-10-11 on the committed 2023-24 corpus; floors rounded to the safe side.


def test_opponent_adjustment_moves_net_toward_the_adjusted_oracle(monkeypatch) -> None:
    """The adjustment must move ratings (mean |adj - raw| 0.3253) and in the direction ESPN's
    opponent-adjusted BPI departs from the raw NET_RATING (corr 0.4176); a no-op gives 0 / nan."""
    oracle = pl.read_parquet(FIXTURE_DIR / "team_ratings_oracle_2024.parquet")
    m = _ratings_2024(monkeypatch).join(oracle, on="team_id", how="inner")
    assert m.height == 30
    correction = (m["adj_net_rtg"] - (m["raw_off_rtg"] - m["raw_def_rtg"])).to_numpy()
    departure = (m["bpi"] - m["net_rating"]).to_numpy()
    moved = float(np.abs(correction).mean())
    assert 0.30 <= moved <= 0.36, f"mean |adj_net - raw_net| {moved:.4f} outside the observed 0.3253 band"
    assert np.std(correction) > 0 and float(np.corrcoef(correction, departure)[0, 1]) >= 0.40
    print("NBA_ADJUSTMENT_LEVEL_OK", round(moved, 4))


def test_adj_pace_level_vs_stats_nba_pace(monkeypatch) -> None:
    """adj_pace vs stats.nba.com PACE: MAE 1.3388 / max |diff| 2.43728 observed; the
    pace fixed point disabled (max_iter -> 0) reads 1.3477 / 2.4748."""
    oracle = pl.read_parquet(FIXTURE_DIR / "team_ratings_oracle_2024.parquet")
    m = _ratings_2024(monkeypatch).join(oracle, on="team_id", how="inner")
    d = np.abs((m["adj_pace"] - m["pace"]).to_numpy())
    assert d.mean() <= 1.3390, d.mean()
    assert d.max() <= 2.4374, d.max()
    print("NBA_PACE_LEVEL_OK", round(float(d.mean()), 5), round(float(d.max()), 5))
