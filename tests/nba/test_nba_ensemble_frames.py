"""Gates for the ensemble dataset frames, on real-fixture simulations.

The converters must be exact views of the sample vectors (identity and
conservation hold row-wise, not statistically), the market summary must
reproduce the numpy computations, and the samples frame must behave like
any published dataset — a contract derived from one run validates another
seed's run without blocking findings.
"""

from __future__ import annotations

import numpy as np
import polars as pl
import pytest

from sportsdataverse.modeling.integrity import derive_contract, validate_frame
from sportsdataverse.nba.nba_possession_sim import (
    PlayerAttribution,
    ensemble_market_summary,
    ensemble_samples,
    player_game_logs_from_pbp,
    player_points_long,
    simulate_ensemble,
)
from tools.calibration import build as calibration_build


@pytest.fixture(scope="module")
def sim():
    raw = calibration_build.fixture_raw()
    shelf = calibration_build.fixture_shelf(raw)
    logs = player_game_logs_from_pbp(raw)
    game = logs.filter(pl.col("game_id") == "0022300001")
    teams = sorted(int(t) for t in game["team_id"].drop_nulls().unique().to_list())
    att = PlayerAttribution.from_logs(logs, home_team_id=teams[0], away_team_id=teams[1])
    ens = simulate_ensemble(shelf, n_sim=200, seed=7, attribution=att)
    return shelf, ens


def test_samples_frame_is_an_exact_view(sim) -> None:
    _shelf, ens = sim
    samples = ensemble_samples(ens)
    assert samples.schema["sim_id"] == pl.Int64
    assert samples["sim_id"].to_list() == list(range(ens["n_sim"]))
    assert np.array_equal(samples["score_home"].to_numpy(), ens["score_home"])
    assert np.array_equal(samples["score_away"].to_numpy(), ens["score_away"])
    assert np.array_equal(samples["total"].to_numpy(), ens["score_home"] + ens["score_away"])
    assert np.array_equal(samples["margin"].to_numpy(), ens["score_home"] - ens["score_away"])
    assert samples.filter(pl.col("home_win") == (pl.col("margin") > 0)).height == samples.height
    with pytest.raises(ValueError, match="sample vectors"):
        ensemble_samples({"n_sim": 3})


def test_player_points_conserve_per_simulation(sim) -> None:
    _shelf, ens = sim
    samples = ensemble_samples(ens)
    long = player_points_long(ens)
    assert long.schema == {"sim_id": pl.Int64, "player_id": pl.Int64, "pts": pl.Int64}
    per_sim = long.group_by("sim_id").agg(pl.col("pts").sum().alias("player_total"))
    joined = samples.join(per_sim, on="sim_id", how="left")
    assert joined.filter(pl.col("player_total") == pl.col("total")).height == samples.height


def test_market_summary_matches_numpy(sim) -> None:
    _shelf, ens = sim
    summary = ensemble_market_summary(ens)
    assert summary.height == 1
    row = summary.to_dicts()[0]
    total = (ens["score_home"] + ens["score_away"]).astype(float)
    margin = (ens["score_home"] - ens["score_away"]).astype(float)
    assert row["n_sim"] == ens["n_sim"]
    assert row["win_prob_home"] == pytest.approx(ens["win_prob_home"])
    assert row["total_mean"] == pytest.approx(ens["mean_total"])
    assert row["total_std"] == pytest.approx(float(np.std(total)))
    assert row["margin_p50"] == pytest.approx(float(np.quantile(margin, 0.5)))
    assert row["total_p10"] <= row["total_p50"] <= row["total_p90"]


def test_no_attribution_yields_documented_empty_schema(sim) -> None:
    shelf, _ens = sim
    plain = simulate_ensemble(shelf, n_sim=20, seed=11)
    long = player_points_long(plain)
    assert long.height == 0
    assert long.schema == {"sim_id": pl.Int64, "player_id": pl.Int64, "pts": pl.Int64}


def test_samples_behave_like_a_published_dataset(sim) -> None:
    shelf, ens = sim
    samples = ensemble_samples(ens)
    contract = derive_contract(samples, name="nba_ensemble_samples", key=["sim_id"])
    assert validate_frame(samples, contract).ok
    # a different seed's run satisfies the same contract without blocking
    # findings (bounds/domain movement is drift-class by design)
    other = ensemble_samples(simulate_ensemble(shelf, n_sim=200, seed=11))
    assert validate_frame(other, contract).ok
    # a vanished column is a completeness-class block
    report = validate_frame(samples.drop("margin"), contract)
    assert not report.ok
    assert any(violation.column == "margin" for violation in report.blocking)
