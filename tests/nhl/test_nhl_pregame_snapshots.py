"""``load_nhl_pregame_snapshots`` + ``pregame_state_as_of`` on a real compiled day, offline.

``tests/fixtures/nhl_pregame/nhl_pregame_snapshots_2027.parquet`` is fastRhockey-nhl-data's
``nhl_data_build.pregame`` output for 2026-11-25 (14 games), compiled from real ESPN probes
every 15 minutes from 00:10Z (README in that folder). It stands in for the release asset.
"""

from __future__ import annotations

from datetime import timedelta
from pathlib import Path

import polars as pl
import pytest

import sportsdataverse.nhl.nhl_loaders as nhl
from sportsdataverse.espn_snapshots import pregame_state_as_of

FIX = Path(__file__).resolve().parents[1] / "fixtures" / "nhl_pregame"


@pytest.fixture(autouse=True)
def _offline(monkeypatch: pytest.MonkeyPatch) -> None:
    def read(url: str) -> pl.DataFrame:
        assert url.endswith("nhl_pregame_snapshots/nhl_pregame_snapshots_2027.parquet"), url
        return pl.read_parquet(FIX / "nhl_pregame_snapshots_2027.parquet")

    monkeypatch.setattr(nhl, "_read_release_parquet", read)


def test_the_loader_returns_the_published_intervals() -> None:
    df = nhl.load_nhl_pregame_snapshots(2027)
    assert df.schema["valid_from"] == df.schema["valid_to"] == pl.Datetime("us", "UTC")
    assert df.schema["game_id"] == df.schema["player_id"] == pl.Int64
    assert set(df["kind"]) == {"probable_goalie", "injury"}
    assert df["game_id"].n_unique() == 14 and df["game_id"].null_count() == 0
    with pytest.raises(nhl.SeasonNotFoundError):
        nhl.load_nhl_pregame_snapshots(2026)


def test_as_of_puck_drop_reads_one_goalie_per_team_and_nothing_later() -> None:
    df = nhl.load_nhl_pregame_snapshots(2027)
    first = df["valid_from"].min()
    # games that started after the first capture: their goalies are known at puck drop minus 1 s
    late = df.filter(pl.col("start_time_utc") > first + timedelta(minutes=1))
    known = pregame_state_as_of(late, pl.col("start_time_utc") - pl.duration(seconds=1))
    goalies = known.filter(pl.col("kind") == "probable_goalie")
    assert goalies.height > 0
    assert goalies.group_by("game_id", "team_id").len()["len"].max() == 1  # one state per slot at t
    assert goalies.select(pl.col("game_id").n_unique() * 2).item() == goalies.height  # both teams
    assert (known["valid_from"] < known["start_time_utc"]).all()
    # nothing is known before the first capture
    assert pregame_state_as_of(df, first - timedelta(seconds=1)).height == 0
