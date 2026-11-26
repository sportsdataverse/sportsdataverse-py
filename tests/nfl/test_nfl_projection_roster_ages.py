"""Live `load_nfl_rosters` frames reach the projection's aging step (real 2020-2023 fixtures).

`load_nfl_rosters` returns ``gsis_id`` and ``birth_date``; the projection read ``player_id`` and ``age``,
so live it matched no roster row, every age was null, and ``aging_curve`` raised on the null age group
(availability fell back to the DEFAULT prior: every position was null). ``rosters_live_2020_2023`` is the
loader's own rows for the projection corpus's players (``tests/fixtures/nfl_projection/README.md``).
"""

from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest
from polars.testing import assert_frame_equal

import sportsdataverse.nfl.nfl_projection as pm
from sportsdataverse.nfl.nfl_availability import season_availability
from sportsdataverse.nfl.nfl_projection_constants import _season_rosters

FIX = Path(__file__).resolve().parents[1] / "fixtures" / "nfl_projection"


@pytest.fixture(scope="module")
def frames() -> dict[str, pl.DataFrame]:
    return {
        "weekly": pl.read_parquet(FIX / "player_stats_2020_2023.parquet"),
        "corpus": pl.read_parquet(FIX / "rosters_2020_2023.parquet"),
        "live": pl.read_parquet(FIX / "rosters_live_2020_2023.parquet"),
        "snaps": pl.read_parquet(FIX / "snap_counts_2020_2023.parquet"),
    }


def _project(frames, rosters: pl.DataFrame, monkeypatch) -> pl.DataFrame:
    monkeypatch.setattr(pm, "load_nfl_player_stats", lambda *a, **k: frames["weekly"], raising=True)
    monkeypatch.setattr(pm, "load_nfl_rosters", lambda *a, **k: rosters, raising=True)
    return pm.nfl_player_projection([2020, 2021, 2022, 2023], 2024).sort("player_id")


def test_live_roster_ages_are_the_ages_the_corpus_was_built_on(frames):
    assert frames["live"].select("gsis_id", "season").is_unique().all()  # one birth date per player-season
    live = _season_rosters(frames["live"])
    assert live is not None and live.schema["player_id"] == frames["corpus"].schema["player_id"] == pl.Utf8
    both = frames["corpus"].join(live, on=["player_id", "season"], how="inner", suffix="_live")
    aged = both.filter(pl.col("age").is_not_null())
    assert aged.height == 11_689
    assert (aged["age"] - aged["age_live"]).abs().max() < 1e-9


def test_season_rates_carry_ages_from_a_live_roster(frames):
    rates = pm.season_player_rates(frames["weekly"], frames["live"])
    assert rates["age"].null_count() < 0.05 * rates.height
    corpus = pm.season_player_rates(frames["weekly"], frames["corpus"])
    assert_frame_equal(rates.sort("player_id", "season"), corpus.sort("player_id", "season"))


def test_the_projection_ages_players_from_a_live_roster(frames, monkeypatch):
    live = _project(frames, frames["live"], monkeypatch)
    corpus = _project(frames, frames["corpus"], monkeypatch)
    assert_frame_equal(live, corpus, check_exact=False, abs_tol=1e-9)
    # the aging step now moves projections (WR is the position whose fitted damping applies the curve)
    wr = live.filter(pl.col("position_group") == "WR")
    assert (wr["aging_mult"] != 1.0).sum() > 50
    unaged = _project(frames, frames["live"].drop("birth_date"), monkeypatch)
    assert (unaged["aging_mult"] == 1.0).all()
    moved = live.join(unaged, on="player_id", suffix="_unaged").filter(pl.col("position_group") == "WR")
    assert ((moved["proj_ppg"] - moved["proj_ppg_unaged"]).abs() > 1e-6).sum() > 50


def test_availability_reads_positions_from_a_live_roster(frames):
    live = season_availability(frames["snaps"], frames["live"])
    corpus = season_availability(frames["snaps"], frames["corpus"])
    assert live["position"].null_count() == 0 and live.height > 2000
    assert_frame_equal(live.sort("player_id", "season"), corpus.sort("player_id", "season"))
    blind = season_availability(frames["snaps"], frames["live"].drop("position", "birth_date"))
    assert blind["position"].null_count() == blind.height  # what the live path produced before
