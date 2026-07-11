"""Command+/Location+ (②) oracle gate — the Phase 3 gate.

Validates the bundled Command+ model against REAL committed Savant captures,
mirroring the Stuff+ oracle gate's design (see
``tests/mlb/test_mlb_stuff_plus_oracle.py`` for the full rationale on fixture
choice and the pitch-level-vs-arsenal-level calibration distinction):

(a) internal calibration — the pitch-level mean ``command_plus`` over
``pitcher_holdout_season_2024.parquet`` matches 100 to float precision (the
centering constants were computed from this exact population).

(b) concurrent validity — Spearman(``command_plus``, Savant arsenal run
value) on qualified (>=10-pitch) arsenals.

**Oracle-source caveat**: Savant does not publish a location-isolated run
value or called-strike-above-average leaderboard (the plan's intended
oracle). The only real, capturable Savant leaderboard that scores per
``(pitcher, pitch_type)`` run value is ``pitch-arsenal-stats``, which
conflates *stuff* and *location* value together. This is documented as a
weaker, proxy cross-check for Command+ specifically (it validates "is this
pitch's overall value reasonable," not "is this pitch's location value
isolated correctly") — the floor is set conservatively low to reflect that
the comparison target is not a pure match for what Command+ measures.
"""

from __future__ import annotations

import polars as pl

from sportsdataverse.mlb.mlb_command_plus import mlb_command_plus
from sportsdataverse.mlb.mlb_pitch_features import pitch_features
from sportsdataverse.mlb.mlb_pitching_constants import spearman_corr

FIX = "tests/fixtures/mlb_pitching"

#: Observed Spearman(command_plus, -run_value) on the qualified (>=10-pitch)
#: held-out-2024 arsenals vs the real 2024 Savant arsenal run-value
#: leaderboard: 0.140. Floor rounded down; see the proxy-oracle caveat above.
FLOOR_RV = 0.10

MIN_ARSENAL_PITCHES = 10


def test_command_plus_mean_is_100_internal_calibration():
    fixture = pl.read_parquet(f"{FIX}/pitcher_holdout_season_2024.parquet")
    feats = pitch_features(fixture)
    pitch_level = mlb_command_plus(feats, level="pitch")
    assert abs(pitch_level["command_plus"].mean() - 100.0) <= 0.5


def test_command_plus_spearman_vs_savant_arsenal_run_value_proxy():
    fixture = pl.read_parquet(f"{FIX}/pitcher_holdout_season_2024.parquet")
    feats = pitch_features(fixture)
    pitch_level = mlb_command_plus(feats, level="pitch")

    arsenal = pitch_level.group_by("pitcher", "pitch_type").agg(pl.col("command_plus").mean(), pl.len().alias("n"))
    arsenal = arsenal.filter(pl.col("n") >= MIN_ARSENAL_PITCHES)

    savant = pl.read_parquet(f"{FIX}/savant_pitch_arsenal_stats_2024.parquet")
    assert arsenal.schema["pitcher"] == savant.schema["pitcher"]

    joined = arsenal.join(
        savant.select("pitcher", "pitch_type", "run_value"), on=["pitcher", "pitch_type"], how="inner"
    )
    assert joined.height >= 20  # observed: 42 rows on the qualified holdout population

    corr = spearman_corr(joined["command_plus"].to_numpy(), (-joined["run_value"]).to_numpy())
    assert corr >= FLOOR_RV
