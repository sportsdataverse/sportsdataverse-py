"""``ncaa_espn_schools`` / ``ncaa_espn_school_seasons``: the static ``{mbb,wbb}_crosswalk`` school tables, offline.

Real release rows (``tests/fixtures/ncaa_espn_schools/README.md``) stand in for the asset, so the tests exercise
the published dtypes (``espn_team_id`` is String in both files) and the published two-sport ``sport`` column.
"""

from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest

import sportsdataverse.mbb.mbb_ncaa_espn_crosswalk as xw
from sportsdataverse.errors import AssetFetchError
from sportsdataverse.wbb import ncaa_espn_school_seasons, ncaa_espn_schools

FIX = Path(__file__).resolve().parents[1] / "fixtures" / "ncaa_espn_schools"


@pytest.fixture
def urls(monkeypatch: pytest.MonkeyPatch) -> list[str]:
    seen: list[str] = []

    def read(url: str) -> pl.DataFrame:
        seen.append(url)
        return pl.read_parquet(FIX / url.rsplit("/", 1)[-1])

    monkeypatch.setattr(xw, "_read_release_parquet", read)
    return seen


def test_the_fixtures_carry_the_published_string_ids() -> None:
    assert pl.read_parquet_schema(FIX / "ncaa_espn_schools.parquet")["espn_team_id"] == pl.String
    assert pl.read_parquet_schema(FIX / "ncaa_espn_school_seasons.parquet")["espn_team_id"] == pl.String


@pytest.mark.parametrize("league", ["mbb", "wbb"])
def test_schools_read_the_league_tag_with_int_ids(league: str, urls: list[str]) -> None:
    df = ncaa_espn_schools(league=league)
    assert urls == [f"{xw.SDVRELEASES}{league}_crosswalk/ncaa_espn_schools.parquet"]
    assert df.schema == pl.Schema(xw._SCHOOLS_SCHEMA)
    assert df.filter(pl.col("ncaa_team") == "Alabama")["espn_team_id"].to_list() == [333]


@pytest.mark.parametrize("league", ["mbb", "wbb"])
def test_school_seasons_keep_only_the_league(league: str, urls: list[str]) -> None:
    df = ncaa_espn_school_seasons(league=league)
    assert df.schema == pl.Schema(xw._SCHOOL_SEASONS_SCHEMA)
    assert df.height == 10 and df["sport"].unique().to_list() == [league]


def test_school_seasons_join_schools_on_the_org_id(urls: list[str]) -> None:
    schools, seasons = ncaa_espn_schools(), ncaa_espn_school_seasons()
    assert schools.schema["ncaa_org_id"] == seasons.schema["ncaa_org_id"]
    joined = seasons.join(schools, on="ncaa_org_id", how="left", suffix="_school")
    assert joined["ncaa_team"].null_count() == 0
    assert (joined["espn_team_id"] == joined["espn_team_id_school"]).all()


def test_absent_asset_is_the_empty_schema_with_a_warning(monkeypatch: pytest.MonkeyPatch) -> None:
    warned: list[str] = []
    monkeypatch.setattr(xw, "_read_release_parquet", lambda url: None)
    monkeypatch.setattr(xw, "cli_warn", warned.append)
    df = ncaa_espn_school_seasons(league="wbb")
    assert df.height == 0 and df.schema == pl.Schema(xw._SCHOOL_SEASONS_SCHEMA)
    assert warned and "no published asset" in warned[0]


def test_failed_fetch_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    def fail(url: str) -> pl.DataFrame:
        raise AssetFetchError(f"HTTP 403: {url}")

    monkeypatch.setattr(xw, "_read_release_parquet", fail)
    with pytest.raises(AssetFetchError):
        ncaa_espn_schools()


def test_rejects_unknown_league() -> None:
    with pytest.raises(ValueError, match="league must be one of"):
        ncaa_espn_schools(league="nba")
