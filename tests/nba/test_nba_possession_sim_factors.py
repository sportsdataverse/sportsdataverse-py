"""Tests for the auditable factor-adjustment layer (v2 seam)."""

from __future__ import annotations

import json
import pathlib

import polars as pl
import pytest

from sportsdataverse.nba.nba_possession_sim import (
    FactorAdjustment,
    build_shelf,
    possessions_from_pbp,
    simulate_ensemble,
)

FXROOT = pathlib.Path("tests/fixtures/nba_engine")
GAME_IDS = ("0022100001", "0022200001", "0022300001")


@pytest.fixture(scope="module")
def shelf():
    frames = []
    for gid in GAME_IDS:
        payload = json.loads((FXROOT / gid / "playbyplayv3.json").read_text(encoding="utf-8"))
        acts = payload.get("game", {}).get("actions") or payload["actions"]
        frames.append(pl.DataFrame(acts, infer_schema_length=None).with_columns(pl.lit(gid).alias("game_id")))
    return build_shelf(possessions_from_pbp(pl.concat(frames, how="diagonal_relaxed")))


def test_factor_validation_bounds() -> None:
    with pytest.raises(ValueError, match="out of"):
        FactorAdjustment({"three_make": 9.0})
    with pytest.raises(ValueError, match="out of"):
        FactorAdjustment({"tov": -0.1})


def test_adjust_renormalizes_and_counts() -> None:
    fa = FactorAdjustment({"three_make": 2.0})
    pmf = {"three_make": 0.2, "rim_make": 0.3, "tov": 0.5}
    out = fa.adjust(pmf)
    assert sum(out.values()) == pytest.approx(1.0)
    assert out["three_make"] > pmf["three_make"]
    assert fa.n_applied == 1
    assert pmf["three_make"] == 0.2  # input not mutated
    assert fa.summary() == {"factors": {"three_make": 2.0}, "n_applied": 1}


def test_ensemble_with_scoring_boost_scores_more(shelf) -> None:
    base = simulate_ensemble(shelf, n_sim=200, seed=13)
    boost = FactorAdjustment({"three_make": 1.5, "rim_make": 1.5, "tov": 0.6})
    boosted = simulate_ensemble(shelf, n_sim=200, seed=13, factors=boost)
    assert boosted["mean_total"] > base["mean_total"]
    # the audit rode along with the ensemble
    assert base["factors"] is None
    assert boosted["factors"]["factors"]["tov"] == 0.6
    assert boosted["factors"]["n_applied"] > 0
