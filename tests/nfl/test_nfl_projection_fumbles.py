"""Fumbles lost reach the NFL projection from the real loader output.

``load_nfl_player_stats`` emits fumbles lost split by play type (sack / rushing /
receiving) and no single ``fumbles_lost`` column, while the scoring formats charge
``fumbles_lost``. These tests run the real 2024 weekly release slice through the
loader's own reconciliation so they see exactly the columns callers get.
"""

from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest

from sportsdataverse.nfl.nfl_loaders import _player_stats_to_legacy
from sportsdataverse.nfl.nfl_projection import (
    _with_fumbles_lost,
    nfl_fantasy_projection,
    score_fantasy,
    season_player_rates,
)
from sportsdataverse.nfl.nfl_projection_constants import SCORING_PPR

FIXTURES = Path(__file__).parent.parent / "fixtures" / "nfl_player_stats"
PARTS = ["sack_fumbles_lost", "rushing_fumbles_lost", "receiving_fumbles_lost"]


@pytest.fixture(scope="module")
def raw() -> pl.DataFrame:
    return pl.read_parquet(FIXTURES / "stats_player_week_2024_fumbles_slice.parquet")


@pytest.fixture(scope="module")
def weekly(raw: pl.DataFrame) -> pl.DataFrame:
    return _player_stats_to_legacy(raw, kicking=False)


@pytest.fixture(scope="module")
def rosters() -> pl.DataFrame:
    return pl.read_parquet(FIXTURES / "rosters_2024_fumbles_slice.parquet")


def test_loader_output_has_only_the_split_columns(weekly: pl.DataFrame) -> None:
    """The premise: the loader never emits a ``fumbles_lost`` column."""
    assert "fumbles_lost" not in weekly.columns
    assert set(PARTS) <= set(weekly.columns)
    # every play type is represented in the slice (Maye: sack + rushing, Jacobs: rushing, Metcalf: receiving)
    assert all(weekly[c].sum() > 0 for c in PARTS)


def test_season_rates_count_every_fumble_lost(weekly: pl.DataFrame, rosters: pl.DataFrame, raw: pl.DataFrame) -> None:
    rates = season_player_rates(weekly, rosters)
    assert "fumbles_lost_rate" in rates.columns
    expected = raw.group_by("player_id").agg(pl.sum_horizontal(PARTS).sum().cast(pl.Float64).alias("fl"))
    got = rates.select("player_id", (pl.col("fumbles_lost_rate") * pl.col("games")).alias("fl_rates"))
    assert got.schema["player_id"] == expected.schema["player_id"]
    joined = got.join(expected, on="player_id", how="inner")
    assert joined.height == 3
    assert joined.filter((pl.col("fl_rates") - pl.col("fl")).abs() > 1e-9).height == 0
    assert dict(joined.select("player_id", "fl").iter_rows()) == {
        "00-0039851": 6.0,  # Drake Maye: 4 sack + 2 rushing
        "00-0035700": 3.0,  # Josh Jacobs: 3 rushing
        "00-0035640": 2.0,  # DK Metcalf: 2 receiving
    }


def test_fumbles_lost_matches_the_release_fantasy_points(weekly: pl.DataFrame) -> None:
    """Oracle: with ``fumbles_lost`` derived, PPR scoring reproduces the release's own
    ``fantasy_points_ppr`` on every week (2-pt conversions and return TDs added back,
    since the scoring formats leave them out)."""
    scored = _with_fumbles_lost(weekly)
    extra = 2.0 * pl.sum_horizontal(
        "passing_2pt_conversions", "rushing_2pt_conversions", "receiving_2pt_conversions"
    ) + 6.0 * pl.col("special_teams_tds").fill_null(0.0)
    ours = score_fantasy(scored, SCORING_PPR) + scored.select(extra.alias("x"))["x"]
    diff = (ours - scored["fantasy_points_ppr"]).abs()
    assert diff.max() < 1e-6
    # the fumble weeks are where the charge matters
    assert scored.filter(pl.col("fumbles_lost") > 0).height >= 8


def test_fantasy_projection_charges_fumbles(
    monkeypatch: pytest.MonkeyPatch, weekly: pl.DataFrame, rosters: pl.DataFrame
) -> None:
    import sportsdataverse.nfl.nfl_projection as mod

    monkeypatch.setattr(mod, "load_nfl_player_stats", lambda *a, **k: weekly)
    monkeypatch.setattr(mod, "load_nfl_rosters", lambda *a, **k: rosters)
    with_fl = nfl_fantasy_projection([2024], 2025, calibrate=False)
    no_fl = nfl_fantasy_projection([2024], 2025, scoring={**SCORING_PPR, "fumbles_lost": 0.0}, calibrate=False)
    key = ["player_id", "position_group"]
    assert with_fl.schema["player_id"] == no_fl.schema["player_id"]
    j = with_fl.join(no_fl, on=key, suffix="_nofl").with_columns(
        (pl.col("proj_fantasy_points_nofl") - pl.col("proj_fantasy_points")).alias("charge")
    )
    assert j.height == 3
    assert j.filter(pl.col("charge") > 0).height == 3, j
