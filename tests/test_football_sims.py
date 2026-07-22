"""Football sim gates — NFL + CFB on real ESPN drive fixtures.

Same discipline as the basketball/baseball families: conservation exact,
full pbp emission with correct structure, and event/scoring distributions
within reason of the real games (v1 single-game PMFs; the bands note it).
"""

from __future__ import annotations

import json
import pathlib

import numpy as np
import polars as pl
import pytest

from sportsdataverse.nfl.nfl_drive_sim import (
    SNAP_CLASSES,
    build_football_shelf,
    espn_football_final_total,
    plays_from_espn_drives,
    simulate_football_ensemble,
    simulate_football_game_pbp,
)

LEAGUES = ("nfl", "cfb")


def _summary(league: str) -> dict:
    return json.loads(pathlib.Path(f"tests/fixtures/espn/summary_{league}.json").read_text(encoding="utf-8"))


@pytest.mark.parametrize("league", LEAGUES)
def test_conservation_reconstructs_real_final(league: str) -> None:
    summary = _summary(league)
    plays = plays_from_espn_drives(summary)
    assert int(plays["points_delta"].sum()) == espn_football_final_total(summary)


@pytest.mark.parametrize("league", LEAGUES)
def test_shelf_pmfs_and_parameters(league: str) -> None:
    shelf = build_football_shelf(plays_from_espn_drives(_summary(league)))
    for key, pmf in {**shelf.snap_pmfs, "all": shelf.all_pmf}.items():
        assert set(pmf) == set(SNAP_CLASSES), key
        assert sum(pmf.values()) == pytest.approx(1.0), key
    assert 25.0 <= shelf.punt_net <= 55.0
    assert 15.0 < shelf.seconds_per_snap < 45.0
    for cls in ("rush", "pass_complete", "sack"):
        assert shelf.yards[cls].size > 0


@pytest.mark.parametrize("league", LEAGUES)
def test_full_pbp_sim_structure(league: str) -> None:
    shelf = build_football_shelf(plays_from_espn_drives(_summary(league)))
    final, pbp = simulate_football_game_pbp(shelf, np.random.default_rng(13))
    again_final, again = simulate_football_game_pbp(shelf, np.random.default_rng(13))
    assert [r["play_class"] for r in pbp] == [r["play_class"] for r in again]  # deterministic
    assert len(pbp) > 80
    quarters = {r["quarter"] for r in pbp}
    assert {1, 2, 3, 4} <= quarters
    assert all(1 <= r["down"] <= 4 for r in pbp if r["play_class"] in SNAP_CLASSES)
    assert all(0 <= r["clock_seconds"] <= 900 for r in pbp)
    # running scores are monotone and end at the final
    totals = [r["score_home"] + r["score_away"] for r in pbp]
    assert totals == sorted(totals)
    assert pbp[-1]["score_home"] == final.score_home
    assert pbp[-1]["score_away"] == final.score_away


@pytest.mark.parametrize("league", LEAGUES)
def test_sim_distribution_within_reason(league: str) -> None:
    summary = _summary(league)
    plays = plays_from_espn_drives(summary)
    real_total = espn_football_final_total(summary)
    shelf = build_football_shelf(plays)
    ens = simulate_football_ensemble(shelf, n_sim=150, seed=17)
    b = simulate_football_ensemble(shelf, n_sim=150, seed=17)
    assert np.array_equal(ens["score_home"], b["score_home"])  # deterministic

    # scoring: near the real game AND inside the league-plausible window
    # (one real game is itself a noisy target — v1 single-game PMFs)
    assert ens["mean_total"] == pytest.approx(real_total, rel=0.35)
    assert 25.0 < ens["mean_total"] < 80.0

    counts = ens["event_counts"]
    snaps_real = plays.filter(pl.col("play_class").is_in(list(SNAP_CLASSES)))
    sim_snaps = sum(float(counts[c].mean()) for c in SNAP_CLASSES)
    assert sim_snaps == pytest.approx(snaps_real.height, rel=0.20)

    real_pass_share = snaps_real.filter(pl.col("play_class").str.starts_with("pass")).height / snaps_real.height
    sim_pass_share = (float(counts["pass_complete"].mean()) + float(counts["pass_incomplete"].mean())) / sim_snaps
    assert abs(sim_pass_share - real_pass_share) < 0.10

    assert 1.0 <= float(counts["punt"].mean()) <= 14.0
    assert 0.0 <= float(counts["fg_good"].mean()) <= 8.0
    assert 1.0 <= float(counts["touchdown"].mean()) <= 12.0
    # ties are rare (post-OT ties allowed, NFL-style)
    assert float((ens["margin"] == 0).mean()) < 0.10


def test_cfb_shim_end_to_end() -> None:
    from sportsdataverse.cfb.cfb_drive_sim import (
        cfb_shelf_from_espn_summary,
        cfb_simulate_ensemble,
        cfb_simulate_game_pbp,
    )

    shelf = cfb_shelf_from_espn_summary(_summary("cfb"))
    ens = cfb_simulate_ensemble(shelf, n_sim=60, seed=3)
    assert 0.0 <= ens["win_prob_home"] <= 1.0
    final, pbp = cfb_simulate_game_pbp(shelf, np.random.default_rng(5))
    assert len(pbp) > 80


def test_cfb_college_overtime_never_ties() -> None:
    from sportsdataverse.cfb.cfb_drive_sim import (
        cfb_shelf_from_espn_summary,
        cfb_simulate_ensemble,
        cfb_simulate_game_pbp,
    )

    shelf = cfb_shelf_from_espn_summary(_summary("cfb"))
    ens = cfb_simulate_ensemble(shelf, n_sim=200, seed=41)
    # the college format resolves every game
    assert float((ens["margin"] == 0).mean()) == 0.0
    # regulation ties do occur and route through the OT rounds
    rng = np.random.default_rng(11)
    ot_games = 0
    for _ in range(150):
        final, pbp = cfb_simulate_game_pbp(shelf, rng)
        assert final.score_home != final.score_away
        ot_rows = [r for r in pbp if r["quarter"] >= 5]
        if ot_rows:
            ot_games += 1
            # untimed possessions; rows stamp POST-play state, so the start
            # spec shows as a fresh 1st-and-10 inside the opponent half
            # (sacks can push a drive back past the 25 mid-possession)
            assert all(r["clock_seconds"] == 0.0 for r in ot_rows)
            assert any(r["down"] == 1 and r["distance"] == 10 for r in ot_rows)
            assert all(r["yards_to_endzone"] <= 60 for r in ot_rows)
    assert ot_games >= 1


def test_penalty_node_replays_downs_without_scoring() -> None:
    """Penalties are fitted no-plays: yardage moves, downs replay, no points."""
    from sportsdataverse.nfl.nfl_drive_sim import (
        build_football_shelf,
        plays_from_espn_drives,
        simulate_football_game_pbp,
    )

    plays = plays_from_espn_drives(_summary("nfl"))
    real_rate = plays.filter(pl.col("play_class") == "penalty").height
    assert real_rate > 0  # the fixture carries accepted penalties
    shelf = build_football_shelf(plays)
    assert 0.0 < shelf.penalty_rate < 0.2
    rng = np.random.default_rng(11)
    _, pbp = simulate_football_game_pbp(shelf, rng)
    pens = [r for r in pbp if r["play_class"] == "penalty"]
    assert pens, "penalty node never fired"
    for row in pens:
        assert row["points"] == 0
        assert 3 <= abs(int(row["yards"])) <= 20
        assert 1 <= int(row["down"]) <= 4
