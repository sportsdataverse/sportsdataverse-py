"""G-League sport-parameterization gate for the possession sim (WS4).

The engine must run unmodified on the committed REAL G-League
``playbyplayv3`` fixtures (``tests/fixtures/nbagl_engine/``) — the reference
"one engine, league arg" lesson. Same conservation oracle as the NBA suite.
"""

from __future__ import annotations

import json
import pathlib

import polars as pl
import pytest

from sportsdataverse.nba.nba_possession_sim import (
    build_shelf,
    possessions_from_pbp,
    simulate_ensemble,
)

FXROOT = pathlib.Path("tests/fixtures/nbagl_engine")
GAME_IDS = ("2022400003", "2022400009")


@pytest.fixture(scope="module")
def actions() -> pl.DataFrame:
    frames = []
    for gid in GAME_IDS:
        payload = json.loads((FXROOT / gid / "playbyplayv3.json").read_text(encoding="utf-8"))
        acts = payload.get("game", {}).get("actions") or payload["actions"]
        frames.append(pl.DataFrame(acts, infer_schema_length=None).with_columns(pl.lit(gid).alias("game_id")))
    return pl.concat(frames, how="diagonal_relaxed")


def test_gleague_conservation_and_sim(actions: pl.DataFrame) -> None:
    events = possessions_from_pbp(actions)
    # conservation oracle: classified event points reconstruct the real finals
    finals = {}
    for gid, game in actions.group_by("game_id"):
        scored = game.filter(pl.col("scoreHome").cast(pl.Utf8, strict=False) != "")
        # cumulative scores never decrease, so the max total IS the final —
        # robust to string-typed actionNumber sort order in this capture
        totals = scored["scoreHome"].cast(pl.Int64) + scored["scoreAway"].cast(pl.Int64)
        finals[str(gid[0])] = int(totals.max())
    observed = {
        row["game_id"]: row["points"] for row in events.group_by("game_id").agg(pl.col("points").sum()).to_dicts()
    }
    assert observed == finals
    # the same engine simulates G-League ball unmodified
    shelf = build_shelf(events)
    ensemble = simulate_ensemble(shelf, n_sim=150, seed=9)
    real_mean_total = events.group_by("game_id").agg(pl.col("points").sum())["points"].mean()
    assert ensemble["mean_total"] == pytest.approx(float(real_mean_total), abs=35.0)
    assert 0.0 < ensemble["win_prob_home"] < 1.0
