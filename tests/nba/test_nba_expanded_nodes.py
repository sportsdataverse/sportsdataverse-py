"""Gates for the fully expanded node taxonomy — real-fixture oracles."""

from __future__ import annotations

import json
import pathlib
from collections import Counter

import numpy as np
import polars as pl
import pytest

from sportsdataverse.nba.nba_possession_sim import (
    build_shelf,
    possessions_from_pbp,
    simulate_game_pbp,
)
from sportsdataverse.nba.nba_possession_sim.expanded_nodes import (
    DEFAULT_AUX,
    aux_params_from_espn,
    aux_params_from_pbp,
    simulate_possession_expanded,
)

FXROOT = pathlib.Path("tests/fixtures/nba_engine")
GAME_IDS = ("0022100001", "0022200001", "0022300001")


@pytest.fixture(scope="module")
def actions() -> pl.DataFrame:
    frames = []
    for gid in GAME_IDS:
        payload = json.loads((FXROOT / gid / "playbyplayv3.json").read_text(encoding="utf-8"))
        acts = payload.get("game", {}).get("actions") or payload["actions"]
        frames.append(pl.DataFrame(acts, infer_schema_length=None).with_columns(pl.lit(gid).alias("game_id")))
    return pl.concat(frames, how="diagonal_relaxed")


@pytest.fixture(scope="module")
def shelf(actions: pl.DataFrame):
    s = build_shelf(possessions_from_pbp(actions))
    s.aux = aux_params_from_pbp(actions)
    return s


def test_aux_rates_fit_from_real_v3(actions: pl.DataFrame) -> None:
    aux = aux_params_from_pbp(actions)
    assert 0.3 < aux["steal_share"] < 0.8  # live-ball share of real turnovers
    assert 0.02 < aux["and1_rate"] < 0.15
    # v3 carries no block annotation: documented default, not a false zero
    assert aux["block_rate"] == DEFAULT_AUX["block_rate"]
    assert 0.0 < aux["def_foul_rate"] < 0.5


def test_aux_rates_fit_from_real_espn_text() -> None:
    summary = json.loads(pathlib.Path("tests/fixtures/espn/summary_wnba.json").read_text(encoding="utf-8"))
    aux = aux_params_from_espn(summary)
    assert 0.03 < aux["block_rate"] < 0.25  # fitted from "blocks" text
    assert 0.3 < aux["steal_share"] < 0.8  # fitted from "steals" text


def test_no_signal_keeps_defaults() -> None:
    summary = json.loads(pathlib.Path("tests/fixtures/espn/summary_mbb.json").read_text(encoding="utf-8"))
    aux = aux_params_from_espn(summary)
    # this capture's texts carry no block/steal annotations
    assert aux["block_rate"] == DEFAULT_AUX["block_rate"]
    assert aux["steal_share"] == DEFAULT_AUX["steal_share"]


def test_expanded_walk_conserves_points(shelf) -> None:
    rng = np.random.default_rng(19)
    for _ in range(300):
        points, trail, _ = simulate_possession_expanded(shelf, score_diff=0.0, period=1, clock_seconds=600.0, rng=rng)
        expected = 0
        for event in trail:
            if event in ("rim_make", "mid_make"):
                expected += 2
            elif event == "three_make":
                expected += 3
            elif event.startswith("ft_made_"):
                expected += int(event.rsplit("_", 1)[1])
        assert points == expected


def test_expanded_trail_carries_full_taxonomy(shelf) -> None:
    rng = np.random.default_rng(23)
    tags: Counter = Counter()
    # cycle real gamestates — a single sparse key could legitimately lack a
    # shot type; the taxonomy check is about the TREE, not one cell
    states = [(d, p, c) for d in (-6.0, 0.0, 6.0) for p in (1, 2, 3, 4) for c in (600.0, 300.0, 45.0)]
    for i in range(600):
        diff, period, clock = states[i % len(states)]
        _, trail, _ = simulate_possession_expanded(shelf, score_diff=diff, period=period, clock_seconds=clock, rng=rng)
        tags.update(trail)
    # every expanded node fires at plausible frequency across 600 possessions
    for tag in (
        "rim_attempt",
        "mid_attempt",
        "three_attempt",
        "make",
        "miss",
        "stl",
        "tov_dead",
        "oreb",
        "dreb",
        "and1",
        "blk",
        "side:timeout",
        "side:def_foul",
    ):
        assert tags[tag] > 0, tag
    # collapsed terminal tokens still present (10-outcome contract)
    assert tags["three_make"] > 0 and tags["tov"] > 0


def test_expanded_full_game_pbp(shelf) -> None:
    final, pbp = simulate_game_pbp(shelf, np.random.default_rng(29), expanded=True)
    again_final, again = simulate_game_pbp(shelf, np.random.default_rng(29), expanded=True)
    assert [r["events"] for r in pbp] == [r["events"] for r in again]  # deterministic
    total = final.score_home + final.score_away
    assert 150 < total < 350
    per_game = Counter(t for row in pbp for t in row["events"])
    # expanded box-stat events land in NBA-plausible per-game ranges
    assert 2 <= per_game["and1"] <= 20
    assert 2 <= per_game["blk"] <= 25
    assert 2 <= per_game["stl"] <= 30
    assert per_game["side:def_foul"] >= 5
