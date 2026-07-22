"""Gates for the NBA possession sim (WS4) — real-fixture oracles only.

Every gate runs against the three committed REAL stats.nba.com
``playbyplayv3`` games under ``tests/fixtures/nba_engine/`` (see that
directory's README for provenance). No synthetic payloads.
"""

from __future__ import annotations

import json
import pathlib

import numpy as np
import polars as pl
import pytest

from sportsdataverse.nba.nba_possession_sim import (
    OUTCOMES,
    build_shelf,
    clock_bin,
    gamestate_key,
    in_game_win_prob,
    parse_clock,
    period_bin,
    player_game_logs_from_pbp,
    player_shot_mix_priors,
    player_usage_priors,
    possessions_from_pbp,
    score_diff_bin,
    shelf_from_parquet,
    shelf_to_parquet,
    simulate_ensemble,
    simulate_possession,
)

FXROOT = pathlib.Path("tests/fixtures/nba_engine")
GAME_IDS = ("0022100001", "0022200001", "0022300001")


def _actions() -> pl.DataFrame:
    frames = []
    for gid in GAME_IDS:
        payload = json.loads((FXROOT / gid / "playbyplayv3.json").read_text(encoding="utf-8"))
        acts = payload.get("game", {}).get("actions") or payload["actions"]
        frames.append(pl.DataFrame(acts, infer_schema_length=None).with_columns(pl.lit(gid).alias("game_id")))
    return pl.concat(frames, how="diagonal_relaxed")


@pytest.fixture(scope="module")
def actions() -> pl.DataFrame:
    return _actions()


@pytest.fixture(scope="module")
def events(actions: pl.DataFrame) -> pl.DataFrame:
    return possessions_from_pbp(actions)


@pytest.fixture(scope="module")
def shelf(events: pl.DataFrame):
    return build_shelf(events)


# -------------------------------------------------------------------- keygen


def test_keygen_bins() -> None:
    assert parse_clock("PT11M22.00S") == pytest.approx(682.0)
    assert parse_clock("PT0M59.50S") == pytest.approx(59.5)
    assert parse_clock("") == 0.0
    assert score_diff_bin(-9) == -2
    assert score_diff_bin(40) == 4
    assert period_bin(6) == 4
    assert clock_bin(500) == "early"
    assert clock_bin(59) == "clutch"
    assert gamestate_key(-9, 3, 75.0) == "d-2|p3|late"


# --------------------------------------------------- classifier (real games)


def test_conservation_event_points_equal_real_finals(actions: pl.DataFrame, events: pl.DataFrame) -> None:
    """THE oracle gate: classified event points reconstruct every real final score."""
    finals = {}
    for gid, game in actions.group_by("game_id"):
        scored = game.filter(pl.col("scoreHome").cast(pl.Utf8, strict=False) != "").sort("actionNumber")
        finals[str(gid[0])] = int(scored["scoreHome"][-1]) + int(scored["scoreAway"][-1])
    observed = {
        row["game_id"]: row["points"] for row in events.group_by("game_id").agg(pl.col("points").sum()).to_dicts()
    }
    assert observed == finals


def test_classifier_covers_full_vocabulary(events: pl.DataFrame) -> None:
    seen = set(events.filter(pl.col("kind") == "outcome")["outcome"].unique().to_list())
    assert seen == set(OUTCOMES)
    rebs = events.filter(pl.col("kind") == "rebound")
    assert set(rebs["outcome"].unique().to_list()) == {"oreb", "dreb"}


# --------------------------------------------------------------------- shelf


def test_shelf_pmfs_are_distributions(shelf) -> None:
    for key, pmf in {**shelf.outcome_pmfs, "all": shelf.all_pmf}.items():
        assert set(pmf) == set(OUTCOMES), key
        assert sum(pmf.values()) == pytest.approx(1.0), key
    assert 0.0 < shelf.oreb_rate < 1.0
    assert 0.5 < shelf.ft_pct < 1.0
    assert 4.0 < shelf.mean_possession_seconds < 30.0


def test_shelf_coverage_accounting(shelf) -> None:
    shelf.reset_coverage()
    known = next(iter(shelf.outcome_pmfs))
    _, fb1 = shelf.get_pmf(known)
    _, fb2 = shelf.get_pmf("d4|p4|clutch-nonexistent")
    assert (fb1, fb2) == (False, True)
    assert shelf.fallback_rate() == pytest.approx(0.5)
    shelf.reset_coverage()
    assert shelf.fallback_rate() == 0.0


def test_shelf_parquet_round_trip(shelf, tmp_path: pathlib.Path) -> None:
    path = shelf_to_parquet(shelf, tmp_path / "shelf.parquet")
    loaded = shelf_from_parquet(path)
    assert loaded.outcome_pmfs == shelf.outcome_pmfs
    assert loaded.all_pmf == shelf.all_pmf
    assert loaded.ft_pct == pytest.approx(shelf.ft_pct)
    assert loaded.meta == shelf.meta


# ------------------------------------------- FeatureSet pilots (real logs)


def test_player_priors_on_real_logs(actions: pl.DataFrame) -> None:
    logs = player_game_logs_from_pbp(actions)
    assert logs.height > 50
    usage = player_usage_priors(logs)
    mix = player_shot_mix_priors(logs)
    assert "fga_sum___5" in usage.columns
    assert "fg3a_mean___0" in mix.columns
    assert usage.height == logs["player_id"].n_unique()


def test_player_priors_as_of_is_leak_safe(actions: pl.DataFrame) -> None:
    logs = player_game_logs_from_pbp(actions)
    # cutoff at the FIRST game id: nothing strictly earlier exists
    first = min(GAME_IDS)
    priors = player_usage_priors(logs, as_of=first)
    assert priors.height == 0 or priors["fga_sum___0"].null_count() == priors.height
    # cutoff at the last game: only the two earlier games contribute
    last = max(GAME_IDS)
    priors_last = player_usage_priors(logs, as_of=last)
    contributing = logs.filter(pl.col("game_id") < last)
    assert priors_last.filter(pl.col("fga_sum___0").is_not_null()).height == pytest.approx(
        contributing["player_id"].n_unique(), abs=0
    )


# -------------------------------------------------------------------- engine


def test_possession_walk_points_match_events(shelf) -> None:
    rng = np.random.default_rng(11)
    for _ in range(300):
        points, trail = simulate_possession(shelf, score_diff=0.0, period=1, clock_seconds=600.0, rng=rng)
        expected = 0
        for event in trail:
            if event in ("rim_make", "mid_make"):
                expected += 2
            elif event == "three_make":
                expected += 3
            elif event.startswith("ft_made_"):
                expected += int(event.rsplit("_", 1)[1])
        assert points == expected


def test_ensemble_deterministic_and_calibrated(shelf, events: pl.DataFrame) -> None:
    a = simulate_ensemble(shelf, n_sim=300, seed=42)
    b = simulate_ensemble(shelf, n_sim=300, seed=42)
    assert np.array_equal(a["score_home"], b["score_home"])
    assert np.array_equal(a["score_away"], b["score_away"])
    # calibration sanity vs the REAL fixture games (mean total 237)
    real_mean_total = events.group_by("game_id").agg(pl.col("points").sum())["points"].mean()
    assert a["mean_total"] == pytest.approx(float(real_mean_total), abs=30.0)
    assert 0.0 < a["win_prob_home"] < 1.0
    # coverage stays loud: fallback rate recorded and bounded on sim traffic
    assert shelf.fallback_rate() < 0.5


def test_in_game_win_prob_monotone_in_lead(shelf) -> None:
    up10 = in_game_win_prob(
        shelf,
        score_home=100,
        score_away=90,
        period=4,
        clock_seconds=120.0,
        offense_is_home=True,
        n_sim=300,
        seed=7,
    )
    down10 = in_game_win_prob(
        shelf,
        score_home=90,
        score_away=100,
        period=4,
        clock_seconds=120.0,
        offense_is_home=True,
        n_sim=300,
        seed=7,
    )
    assert up10 > 0.9
    assert down10 < 0.1
    assert up10 > down10


def test_ensemble_prices_with_odds_math(shelf) -> None:
    from sportsdataverse.odds.odds_math import calc_stats, prob_over

    ens = simulate_ensemble(shelf, n_sim=300, seed=5)
    p = prob_over(ens["total"], float(np.median(ens["total"])) - 0.5)
    assert 0.3 < p < 0.7
    stats = calc_stats(ens["margin"], stats=("mean", "median"))
    assert isinstance(stats["mean"], float)
