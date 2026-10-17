"""Contract tests for the stats.ncaa.org <-> ESPN team-id crosswalk.

The contract tests read the copy bundled with sdv-py (the loader's fallback);
the loader itself reads the ``{league}_crosswalk`` release first. Covers both
leagues from one file because the loader is league-parameterized. The build and
its curated alias table live in sdv-reference-data (``sdv_reference/crosswalks/``).
"""

from __future__ import annotations

import polars as pl
import pytest

from sportsdataverse.errors import AssetFetchError
from sportsdataverse.mbb import mbb_ncaa_espn_crosswalk as xw
from sportsdataverse.mbb import ncaa_espn_team_crosswalk, ncaa_mbb_team_ids
from sportsdataverse.wbb import ncaa_wbb_team_ids
from tests.conftest import skip_if_no_live

LEAGUES = ("mbb", "wbb")

#: Observed floors, NOT aspirational: the build currently resolves 100% of every
#: season in both leagues. Held a little under that so a single upstream rename
#: is a warning shot rather than an instant red, but high enough that a broken
#: normalizer or a stale ESPN reference table fails immediately.
RECENT_FLOOR = 0.97
OVERALL_FLOOR = 0.95
RECENT_SEASONS = ("2024-25", "2025-26", "2026-27")


@pytest.fixture(scope="module", params=LEAGUES)
def league(request) -> str:
    return request.param


@pytest.fixture(scope="module")
def crosswalks() -> "dict[str, pl.DataFrame]":
    return {lg: xw._packaged_crosswalk(lg) for lg in LEAGUES}


@pytest.fixture
def offline(monkeypatch):
    """The public loader without the network: no release, so the bundled copy answers."""
    monkeypatch.setattr(xw, "_CROSSWALK_CACHE", {})
    monkeypatch.setattr(xw, "_release_crosswalk", lambda league: None)


def test_rejects_unknown_league():
    with pytest.raises(ValueError, match="league must be one of"):
        ncaa_espn_team_crosswalk(league="nba")


def test_id_dtypes(crosswalks, league):
    df = crosswalks[league]
    assert df.schema["ncaa_team_id"] == pl.Int64
    assert df.schema["espn_team_id"] == pl.Utf8, "ESPN ids are Utf8 everywhere else in sdv-py"
    assert df.schema["espn_conference_id"] == pl.Utf8
    assert df.schema["season"] == pl.Utf8


def test_pandas_round_trip(league, offline):
    assert ncaa_espn_team_crosswalk(league=league, return_as_pandas=True).shape[1] == 13


def test_no_team_is_dropped(crosswalks, league):
    """Every (season, team) in the source id table survives into the crosswalk."""
    source = ncaa_mbb_team_ids() if league == "mbb" else ncaa_wbb_team_ids()
    df = crosswalks[league]
    assert df.height == source.height
    assert set(zip(df["season"], df["ncaa_team_id"])) == set(zip(source["season"], source["id"]))


def test_no_duplicate_ncaa_key(crosswalks, league):
    df = crosswalks[league]
    dupes = df.group_by("season", "ncaa_team_id").agg(pl.len()).filter(pl.col("len") > 1)
    assert dupes.height == 0, dupes.to_dicts()


def test_no_espn_id_shared_by_two_teams_in_a_season(crosswalks, league):
    """An ESPN id must identify at most one NCAA program per season."""
    collisions = (
        crosswalks[league]
        .filter(pl.col("espn_team_id").is_not_null())
        .group_by("season", "espn_team_id")
        .agg(pl.col("ncaa_team").unique().alias("ncaa_teams"))
        .filter(pl.col("ncaa_teams").list.len() > 1)
    )
    assert collisions.height == 0, collisions.to_dicts()


def test_match_method_is_consistent_with_the_id(crosswalks, league):
    df = crosswalks[league]
    assert set(df["match_method"].unique()) <= {"exact", "dict", "alias", "unmatched"}
    assert df.filter((pl.col("match_method") == "unmatched") & pl.col("espn_team_id").is_not_null()).height == 0
    assert df.filter((pl.col("match_method") != "unmatched") & pl.col("espn_team_id").is_null()).height == 0


def test_recent_season_match_rate_floor(crosswalks, league):
    df = crosswalks[league]
    for season in RECENT_SEASONS:
        rows = df.filter(pl.col("season") == season)
        assert rows.height > 300, f"{league} {season} looks truncated"
        rate = rows.filter(pl.col("espn_team_id").is_not_null()).height / rows.height
        assert rate >= RECENT_FLOOR, f"{league} {season} match rate {rate:.3%} < {RECENT_FLOOR:.0%}"


def test_every_season_clears_the_overall_floor(crosswalks, league):
    rates = (
        crosswalks[league]
        .group_by("season")
        .agg((pl.col("espn_team_id").is_not_null().sum() / pl.len()).alias("rate"))
        .filter(pl.col("rate") < OVERALL_FLOOR)
    )
    assert rates.height == 0, rates.sort("season").to_dicts()


def test_espn_columns_are_populated_when_matched(crosswalks, league):
    """A matched row carries ESPN identity, not just an id."""
    matched = crosswalks[league].filter(pl.col("espn_team_id").is_not_null())
    for col in ("espn_display_name", "espn_location", "espn_mascot"):
        assert matched[col].null_count() == 0, col


def test_known_hard_cases(crosswalks):
    """Spot-checks for the traps: Saint-vs-State, renames, AP abbreviations."""
    mbb = crosswalks["mbb"].filter(pl.col("season") == "2025-26")
    lookup = dict(zip(mbb["ncaa_team"], mbb["espn_team_id"]))
    expected = {
        "Ohio St.": "194",  # trailing "St." is State
        "St. John's (NY)": "2599",  # leading "St." is Saint
        "Saint Mary's (CA)": "2608",
        "Saint Francis": "2598",  # NCAA dropped the (PA) qualifier
        "CSU Bakersfield": "2934",  # Cal State Bakersfield
        "Queens (NC)": "2511",
        "Southern Ind.": "88",
    }
    assert {k: lookup.get(k) for k in expected} == expected


#: (school, last season before the move, first season after, conference before, after) --
#: realignments that are true in both leagues. SDV group ids, prefixed by the league.
REALIGNMENTS = [
    ("Maryland", "2013-14", "2014-15", "acc", "big-ten"),
    ("Creighton", "2012-13", "2013-14", "mvc", "big-east"),
    ("Wichita St.", "2016-17", "2017-18", "mvc", "american"),
    ("Southern California", "2023-24", "2024-25", "pac-12", "big-ten"),
    ("Texas", "2023-24", "2024-25", "big-12", "sec"),
    ("Oklahoma", "2023-24", "2024-25", "big-12", "sec"),
    ("Lamar University", "2021-22", "2022-23", "wac", "southland"),  # one WAC season, then back to the Southland
]


@pytest.mark.parametrize(("team", "before", "after", "conf_before", "conf_after"), REALIGNMENTS)
def test_conference_columns_follow_realignment(crosswalks, league, team, before, after, conf_before, conf_after):
    """Fails if any conference column goes back to one flat value per school."""
    rows = crosswalks[league].filter((pl.col("ncaa_team") == team) & pl.col("season").is_in([before, after]))
    by_season = {r["season"]: r for r in rows.iter_rows(named=True)}
    assert by_season[before]["conference_id"] == f"{league}:{conf_before}"
    assert by_season[after]["conference_id"] == f"{league}:{conf_after}"
    for col in ("ncaa_conference", "espn_conference_id", "espn_conference_name"):
        assert by_season[before][col] != by_season[after][col], (team, col)


def test_conference_columns_populated(crosswalks, league):
    """Every matched team has a conference that season, except rows the groups table lacks."""
    df = crosswalks[league]
    assert df["ncaa_conference"].null_count() == 0
    missing = df.filter(pl.col("espn_team_id").is_not_null() & pl.col("conference_id").is_null())
    # WBB: "LIU" is aliased to the post-2019 merged LIU id for all seasons (10 rows, 2009-10..2018-19).
    # Any other gap in the groups table fails here.
    expected = {("LIU", "112358", f"{y}-{(y + 1) % 100:02d}") for y in range(2009, 2019)} if league == "wbb" else set()
    got = {(r["ncaa_team"], str(r["espn_team_id"]), r["season"]) for r in missing.iter_rows(named=True)}
    assert got == expected, sorted(got ^ expected)
    both = df.filter(pl.col("conference_id").is_not_null())
    assert both["espn_conference_id"].null_count() == 0
    assert both["espn_conference_name"].null_count() == 0


def _release_like(league: str) -> pl.DataFrame:
    """What the release carries: the bundled rows plus ncaa_org_id and a season the bundle lacks."""
    base = xw._packaged_crosswalk(league).with_columns(pl.lit(1, dtype=pl.Int64).alias("ncaa_org_id"))
    extra = base.head(1).with_columns(
        pl.lit("2026-27").alias("season"), pl.lit(999999, dtype=pl.Int64).alias("ncaa_team_id")
    )
    return pl.concat([base, extra])


def test_reads_the_release_first(monkeypatch):
    monkeypatch.setattr(xw, "_CROSSWALK_CACHE", {})
    monkeypatch.setattr(xw, "_read_release_parquet", lambda url: _release_like("mbb"))
    df = ncaa_espn_team_crosswalk(league="mbb")
    assert df.columns == list(xw._SCHEMA) and dict(df.schema) == xw._SCHEMA
    assert "2026-27" in df["season"].to_list()


@pytest.mark.parametrize(
    "read",
    [lambda url: None, lambda url: (_ for _ in ()).throw(AssetFetchError("release asset fetch failed with HTTP 403"))],
    ids=["absent", "fetch-failed"],
)
def test_falls_back_to_the_bundled_copy(monkeypatch, read):
    monkeypatch.setattr(xw, "_CROSSWALK_CACHE", {})
    monkeypatch.setattr(xw, "_read_release_parquet", read)
    with pytest.warns(UserWarning, match="using the bundled copy"):
        df = ncaa_espn_team_crosswalk(league="wbb")
    assert df.equals(xw._packaged_crosswalk("wbb"))


@skip_if_no_live
def test_release_covers_the_bundled_seasons(league):
    release = xw._release_crosswalk(league)
    assert release is not None and dict(release.schema) == xw._SCHEMA
    assert set(xw._packaged_crosswalk(league)["season"]) <= set(release["season"])


def test_failed_release_read_is_not_cached(monkeypatch):
    """A transient release failure serves the bundled copy but is retried on the next call."""
    import polars as pl

    import sportsdataverse.mbb.mbb_ncaa_espn_crosswalk as mod

    release = pl.DataFrame({"season": ["2026-27"], "ncaa_team_id": ["1"]})
    calls = iter([None, release])
    monkeypatch.setattr(mod, "_release_crosswalk", lambda league: next(calls))
    monkeypatch.setattr(mod, "_CROSSWALK_CACHE", {})
    first = mod.ncaa_espn_team_crosswalk(league="mbb")
    assert first.height != release.height or first.columns != release.columns  # bundled copy
    assert "mbb" not in mod._CROSSWALK_CACHE
    second = mod.ncaa_espn_team_crosswalk(league="mbb")
    assert second.equals(release)
    assert mod._CROSSWALK_CACHE["mbb"].equals(release)
