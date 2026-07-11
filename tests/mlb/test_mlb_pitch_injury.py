"""Tests for the pitcher injury-risk index (model ⑦)."""

from __future__ import annotations


import polars as pl

from sportsdataverse.mlb.mlb_pitch_injury import mlb_injury_risk, pitcher_appearance_trends
from sportsdataverse.mlb.mlb_pitching_constants import spearman_corr


def test_pitcher_appearance_trends_empty_input():
    out = pitcher_appearance_trends(pl.DataFrame())
    assert out.height == 0 and "velo_drop" in out.columns


def test_mlb_injury_risk_empty_input():
    out = mlb_injury_risk(pl.DataFrame())
    assert out.height == 0 and "injury_risk_index" in out.columns


def test_mlb_injury_risk_real_fixture_shape():
    fixture = pl.read_parquet("tests/fixtures/mlb_pitching/pitcher_season_pitches_2023_sample.parquet")
    out = mlb_injury_risk(fixture)
    assert out.height > 0
    assert "injury_risk_index" in out.columns


#: Task 8.2 gate: observed Spearman(injury_risk_index, next-appearance velo
#: drop) on the real 2023 pitcher-season sample fixture: 0.382 (n=134). Floor
#: rounded down to a documented margin below the observed value. If a future
#: run falls below this, debug the trailing-window construction / weighting
#: -- do not lower the floor to pass.
FLOOR_SELF_SUPERVISED = 0.20


def test_self_supervised_gate_index_ranks_subsequent_velo_drop():
    """Task 8.2 gate: for each appearance, ``injury_risk_index`` (computed from
    TRAILING data only) should positively rank-correlate with the velo drop
    OBSERVED IN THE NEXT appearance (a self-supervised, forward-looking
    label distinct from the trailing features used to build the index)."""
    fixture = pl.read_parquet("tests/fixtures/mlb_pitching/pitcher_season_pitches_2023_sample.parquet")
    trends = pitcher_appearance_trends(fixture, window=5).sort(["pitcher", "game_date"])
    risk = mlb_injury_risk(fixture, window=5).sort(["pitcher", "game_date"])

    # next-appearance fb_velo drop vs this-appearance's own fb_velo, per pitcher
    trends = trends.with_columns(
        (pl.col("fb_velo") - pl.col("fb_velo").shift(-1).over("pitcher")).alias("next_velo_drop")
    )
    joined = risk.join(
        trends.select("pitcher", "game_pk", "game_date", "next_velo_drop"),
        on=["pitcher", "game_pk", "game_date"],
    ).drop_nulls(subset=["injury_risk_index", "next_velo_drop"])

    assert joined.height >= 20  # observed: 134 rows on the real fixture
    corr = spearman_corr(joined["injury_risk_index"].to_numpy(), joined["next_velo_drop"].to_numpy())
    assert corr >= FLOOR_SELF_SUPERVISED
