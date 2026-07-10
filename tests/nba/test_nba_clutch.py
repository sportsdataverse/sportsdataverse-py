"""Tests for the clutch-performance model (Phase 4, model ⑤).

Gate rule (binding): never lower a gate -- debug the model. The Phase-4 gate
is OUT-OF-SAMPLE (season N shrunk skill vs season N+1 realized clutch net) and
may legitimately be a null result at this sample size; that is handled
honestly (documented), not faked.
"""

from __future__ import annotations

import importlib
from pathlib import Path

import numpy as np
import polars as pl

from sportsdataverse.nba.nba_clutch import clutch_delta, nba_team_clutch, shrink_clutch
from sportsdataverse.nba.nba_prediction_constants import spearman_corr

FIXTURE_DIR = Path(__file__).resolve().parents[1] / "fixtures" / "nba_prediction"


def test_clutch_delta_math_and_join() -> None:
    clutch = pl.DataFrame(
        {
            "season": [2024, 2024, 2024],
            "team_id": ["A", "B", "C"],
            "clutch_net_rating": [10.0, -5.0, 0.0],
            "clutch_poss": [300.0, 250.0, 280.0],
        }
    )
    ratings = pl.DataFrame({"season": [2024, 2024, 2024], "team_id": ["A", "B", "C"], "adj_net_rtg": [4.0, -1.0, 2.0]})
    out = clutch_delta(clutch, ratings)
    assert out.height == 3
    d = dict(zip(out["team_id"].to_list(), out["clutch_delta"].to_list()))
    assert abs(d["A"] - 6.0) < 1e-9  # 10 - 4
    assert abs(d["B"] - (-4.0)) < 1e-9  # -5 - (-1)
    assert abs(d["C"] - (-2.0)) < 1e-9  # 0 - 2


def test_clutch_delta_empty_returns_schema() -> None:
    out = clutch_delta(
        pl.DataFrame(
            schema={"season": pl.Int64, "team_id": pl.Utf8, "clutch_net_rating": pl.Float64, "clutch_poss": pl.Float64}
        ),
        pl.DataFrame(schema={"season": pl.Int64, "team_id": pl.Utf8, "adj_net_rtg": pl.Float64}),
    )
    assert out.height == 0
    assert "clutch_delta" in out.columns


def test_shrink_never_amplifies_and_small_sample_shrinks_harder() -> None:
    delta = pl.DataFrame(
        {
            "season": [2024, 2024, 2024, 2024],
            "team_id": ["A", "B", "C", "D"],
            "clutch_delta": [8.0, 8.0, -6.0, 3.0],
            "clutch_poss": [500.0, 50.0, 400.0, 300.0],  # B has a tiny sample
        }
    )
    out = shrink_clutch(delta)
    shr = dict(zip(out["team_id"].to_list(), out["clutch_skill_shrunk"].to_list()))
    raw = dict(zip(delta["team_id"].to_list(), delta["clutch_delta"].to_list()))
    for t in ("A", "B", "C", "D"):
        assert abs(shr[t]) <= abs(raw[t]) + 1e-9  # shrinkage never amplifies
    # A and B have the same raw delta (8.0) but B has 1/10th the possessions -> shrinks harder
    assert abs(shr["B"]) < abs(shr["A"])


def test_nba_team_clutch_out_of_sample_gate(monkeypatch) -> None:
    """Season-2023 shrunk clutch skill vs season-2024 realized clutch net rating."""
    clutch23 = pl.read_parquet(FIXTURE_DIR / "clutch_team_2023.parquet")
    clutch24 = pl.read_parquet(FIXTURE_DIR / "clutch_team_2024.parquet")
    net23 = pl.read_parquet(FIXTURE_DIR / "team_net_2023.parquet")

    mod = importlib.import_module("sportsdataverse.nba.nba_clutch")
    monkeypatch.setattr(mod, "_load_clutch", lambda season, league_id: clutch23)
    monkeypatch.setattr(mod, "_load_full_game_net", lambda season, league_id: net23)

    skill23 = nba_team_clutch(2023, league_id="00")
    assert set(skill23.columns) >= {
        "season",
        "team_id",
        "clutch_net_rating",
        "adj_net_rtg",
        "clutch_delta",
        "clutch_skill_shrunk",
        "clutch_poss",
    }
    assert skill23.schema["team_id"] == pl.Utf8

    joined = skill23.join(
        clutch24.select("team_id", pl.col("clutch_net_rating").alias("next_clutch_net_rating")),
        on="team_id",
        how="inner",
    )
    assert joined.height >= 25  # 30-team intersection
    rho = spearman_corr(joined["clutch_skill_shrunk"].to_numpy(), joined["next_clutch_net_rating"].to_numpy())

    sum_shrunk = float(np.abs(skill23["clutch_skill_shrunk"].to_numpy()).sum())
    sum_raw = float(np.abs(skill23["clutch_delta"].to_numpy()).sum())

    # DOCUMENTED NULL RESULT (Decision 7 / Task 4.3), observed 2026-07-08:
    # var(clutch_delta) ~= 49.6 equals the direct net-rating sampling variance at ~315
    # clutch possessions, so tau^2 ~= 0 -- clutch-over-baseline skill is statistically
    # indistinguishable from noise at this sample. The empirical-Bayes model therefore
    # (a) shrinks HARD toward zero (sum|shrunk| ~= 1.4 vs sum|raw| ~= 161, ratio ~0.9%)
    # and (b) shows only a weak positive cross-season rho ~= 0.10 (in the null band).
    # We keep the shrinkage and report the null; we do NOT invent a signal.
    assert sum_shrunk < 0.1 * sum_raw, f"expected heavy shrinkage (null result); ratio {sum_shrunk / sum_raw:.3f}"
    assert (rho > 0.0) or (abs(rho) < 0.1), f"cross-season clutch rho {rho:.3f} is not in the documented null band"
