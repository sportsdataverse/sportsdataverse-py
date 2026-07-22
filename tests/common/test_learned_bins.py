"""Tests for learned discretization — fitted on REAL possession events."""

from __future__ import annotations

import json
import pathlib

import polars as pl
import pytest

from sportsdataverse._common.learned_bins import fit_learned_bins
from sportsdataverse.nba.nba_possession_sim import possessions_from_pbp

FXROOT = pathlib.Path("tests/fixtures/nba_engine")
GAME_IDS = ("0022100001", "0022200001", "0022300001")


@pytest.fixture(scope="module")
def events() -> pl.DataFrame:
    frames = []
    for gid in GAME_IDS:
        payload = json.loads((FXROOT / gid / "playbyplayv3.json").read_text(encoding="utf-8"))
        acts = payload.get("game", {}).get("actions") or payload["actions"]
        frames.append(pl.DataFrame(acts, infer_schema_length=None).with_columns(pl.lit(gid).alias("game_id")))
    return possessions_from_pbp(pl.concat(frames, how="diagonal_relaxed")).filter(pl.col("kind") == "outcome")


def test_fit_on_real_gamestates(events: pl.DataFrame) -> None:
    bins = fit_learned_bins(
        events,
        features=["score_diff", "period", "clock_seconds"],
        target="outcome",
        min_samples_leaf=25,
    )
    table = bins.leaf_table
    assert table.height >= 2  # real gamestates split at least once
    assert int(table["n"].min()) >= 25  # the coverage floor holds
    assert int(table["n"].sum()) == events.height
    # every leaf's class probabilities form a distribution
    prob_cols = [c for c in table.columns if c.startswith("p_")]
    sums = table.select(pl.sum_horizontal(prob_cols).alias("s"))["s"]
    assert all(abs(v - 1.0) < 1e-9 for v in sums)
    # rules are auditable, human-readable constraint strings
    assert all(("<=" in r or ">" in r or r == "(root)") for r in table["rule"].to_list())


def test_assign_maps_rows_to_leaves(events: pl.DataFrame) -> None:
    bins = fit_learned_bins(
        events,
        features=["score_diff", "period", "clock_seconds"],
        target="outcome",
        min_samples_leaf=25,
    )
    leaves = bins.assign(events)
    assert leaves.len() == events.height
    assert set(leaves.unique().to_list()) == set(bins.leaf_table["leaf_id"].to_list())


def test_validation() -> None:
    with pytest.raises(ValueError, match="no usable rows"):
        fit_learned_bins(
            pl.DataFrame({"a": [None], "y": [None]}, schema={"a": pl.Float64, "y": pl.Utf8}),
            features=["a"],
            target="y",
        )
