"""Cross-league parity for the hand-written ``espn_<league>_player_stats``.

Every ESPN league exposes a BARE ``espn_<league>_player_stats`` (core-v2
season line, one wide row) plus a generated ``espn_<league>_player_stats_v3``
(web-v3 comprehensive). This mirrors the hoopR / wehoop / cfbfastR
convention: bare = season line, ``_v3`` = comprehensive.
"""

from __future__ import annotations

import importlib

import pandas as pd
import polars as pl
import pytest

from tests.conftest import skip_if_no_live

# (league, athlete_id, season) -- stable athletes with a known season line.
CASES: list[tuple[str, int, int]] = [
    ("nba", 1966, 2023),  # LeBron James
    ("mbb", 4395624, 2023),  # Michigan roster athlete
    ("nfl", 3139477, 2023),  # Patrick Mahomes
    ("nhl", 3895074, 2023),  # Connor McDavid
    ("mlb", 33192, 2023),  # Aaron Judge
    ("cfb", 4426338, 2023),  # Bo Nix
    ("wbb", 4433985, 2025),  # Kylie Feuerbach
    ("wnba", 3149391, 2024),  # A'ja Wilson
]

ALL_LEAGUES = [lg for lg, _, _ in CASES]

IDENTITY_COLS: set[str] = {
    "season",
    "season_type",
    "total",
    "athlete_id",
    "full_name",
    "team_id",
    "team_display_name",
}


def _fn(league: str):
    pkg = importlib.import_module(f"sportsdataverse.{league}")
    return getattr(pkg, f"espn_{league}_player_stats")


@pytest.mark.parametrize("league", ALL_LEAGUES)
def test_every_league_has_bare_and_v3(league: str):
    """Bare player_stats is hand-written (core-v2); _v3 is generated (web-v3)."""
    pkg = importlib.import_module(f"sportsdataverse.{league}")
    bare = getattr(pkg, f"espn_{league}_player_stats", None)
    v3 = getattr(pkg, f"espn_{league}_player_stats_v3", None)
    assert bare is not None, f"{league}: missing bare player_stats"
    assert v3 is not None, f"{league}: missing player_stats_v3"
    assert bare.__module__.endswith("_player_stats"), f"{league}: bare should be hand-written"
    assert v3.__module__.endswith("_espn_ext"), f"{league}: _v3 should be generated"


@pytest.mark.parametrize("league", ALL_LEAGUES)
def test_rejects_bad_season_type(league: str):
    with pytest.raises(ValueError, match="season_type"):
        _fn(league)(athlete_id=1, season=2023, season_type="nope")


@skip_if_no_live
@pytest.mark.parametrize("league,athlete_id,season", CASES)
def test_player_stats_returns_single_wide_row(league: str, athlete_id: int, season: int):
    from sportsdataverse.errors import NoESPNDataError

    try:
        df = _fn(league)(athlete_id=athlete_id, season=season)
    except NoESPNDataError:
        pytest.skip(f"{league}: ESPN returned no data for {athlete_id}/{season} at test time")

    assert isinstance(df, pl.DataFrame)
    assert df.height == 1, f"{league}: expected a single wide row"
    assert df.width > 20, f"{league}: expected a wide frame"
    missing = IDENTITY_COLS - set(df.columns)
    assert not missing, f"{league}: missing identity columns: {missing}"
    row = df.to_dicts()[0]
    assert row["season"] == season
    assert row["athlete_id"] == athlete_id
    assert row["season_type"] == "regular"


@skip_if_no_live
def test_return_as_pandas_round_trip():
    df = _fn("nba")(athlete_id=1966, season=2023, return_as_pandas=True)
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 1
