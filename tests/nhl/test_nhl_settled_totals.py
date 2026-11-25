"""Settled-score labels for the pregame total and the ``avg_total_goals`` seed they set.

Fixtures (see ``tests/fixtures/nhl_prediction/README.md``):

* ``pbp_goals_settle_2026.parquet``: the GOAL events of three 2025-26 games from ``load_nhl_pbp_full``,
  one decided in regulation (2025020001), one in overtime (2025020008), one in a shootout with three
  scoring attempts (2025020006).
* ``settled_totals_2024_2026.parquet``: every 2023-24 .. 2025-26 regular-season game's settled score from
  ``tools/models/nhl_prediction/fit_pregame.py``, beside the schedule's final score.
"""

from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest

from sportsdataverse.nhl.nhl_prediction_constants import _settled_scores, get_constants

FIXTURES = Path(__file__).resolve().parents[1] / "fixtures" / "nhl_prediction"
# load_nhl_schedules([2026]) home_score / away_score for the three games (real scores from 2023-24 on).
SCHEDULE_FINALS = {2025020001: (3, 2), 2025020006: (3, 4), 2025020008: (4, 3)}


def test_a_shootout_counts_one_goal_for_its_winner():
    pbp = pl.read_parquet(FIXTURES / "pbp_goals_settle_2026.parquet")
    assert pbp.filter((pl.col("game_id") == 2025020006) & (pl.col("period_type") == "SHOOTOUT")).height == 3
    settled = _settled_scores(pbp).sort("game_id")
    assert settled.schema == pl.Schema({"game_id": pl.Int64, "home_goals": pl.Int64, "away_goals": pl.Int64})
    got = {r["game_id"]: (r["home_goals"], r["away_goals"]) for r in settled.iter_rows(named=True)}
    assert got == SCHEDULE_FINALS


@pytest.fixture(scope="module")
def settled_totals() -> pl.DataFrame:
    return pl.read_parquet(FIXTURES / "settled_totals_2024_2026.parquet")


def test_settled_labels_match_the_schedule_final_scores(settled_totals):
    assert settled_totals.group_by("season").len().sort("season")["len"].to_list() == [1312, 1312, 1312]
    mismatched = settled_totals.filter(
        (pl.col("home_goals") != pl.col("home_score")) | (pl.col("away_goals") != pl.col("away_score"))
    )
    assert mismatched.height == 0


def test_avg_total_goals_is_the_three_season_settled_mean(settled_totals):
    """6.226 / 6.081 / 6.254 for 2023-24 / 2024-25 / 2025-26 -> 6.19 (the seed for 2026-27)."""
    per_season = settled_totals.group_by("season").agg((pl.col("home_goals") + pl.col("away_goals")).mean().alias("t"))
    assert get_constants("nhl").avg_total_goals == round(float(per_season["t"].mean()), 2)
