"""Contract tests for the bundled stats.ncaa.org <-> ESPN team-id crosswalk.

Covers both leagues from one file because the loader is league-parameterized
and the alias table is shared.
"""

from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest

from sportsdataverse.mbb import ncaa_espn_team_crosswalk, ncaa_mbb_team_ids
from sportsdataverse.wbb import ncaa_wbb_team_ids

LEAGUES = ("mbb", "wbb")
ALIAS_CSV = Path(__file__).resolve().parents[2] / "tools" / "crosswalk" / "alias_ncaa_espn.csv"

#: Observed floors, NOT aspirational: the build currently resolves 100% of every
#: season in both leagues. Held a little under that so a single upstream rename
#: is a warning shot rather than an instant red, but high enough that a broken
#: normalizer or a stale ESPN reference table fails immediately.
RECENT_FLOOR = 0.97
OVERALL_FLOOR = 0.95
RECENT_SEASONS = ("2023-24", "2024-25", "2025-26")


@pytest.fixture(scope="module", params=LEAGUES)
def league(request) -> str:
    return request.param


@pytest.fixture(scope="module")
def crosswalks() -> "dict[str, pl.DataFrame]":
    return {lg: ncaa_espn_team_crosswalk(league=lg) for lg in LEAGUES}


def test_rejects_unknown_league():
    with pytest.raises(ValueError, match="league must be one of"):
        ncaa_espn_team_crosswalk(league="nba")


def test_id_dtypes(crosswalks, league):
    df = crosswalks[league]
    assert df.schema["ncaa_team_id"] == pl.Int64
    assert df.schema["espn_team_id"] == pl.Utf8, "ESPN ids are Utf8 everywhere else in sdv-py"
    assert df.schema["espn_conference_id"] == pl.Utf8
    assert df.schema["season"] == pl.Utf8


def test_pandas_round_trip(league):
    assert ncaa_espn_team_crosswalk(league=league, return_as_pandas=True).shape[1] == 12


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


def test_alias_table_has_no_unused_rows(crosswalks):
    """Every hand-curated alias must still be earning its keep in some league."""
    alias = pl.read_csv(ALIAS_CSV, schema_overrides={"espn_team_id": pl.Utf8})
    used = set()
    for df in crosswalks.values():
        used |= set(df.filter(pl.col("match_method") == "alias")["ncaa_team"].unique())
    unused = sorted(set(alias["ncaa_team"]) - used)
    assert not unused, f"alias rows matched nothing: {unused}"


def test_alias_table_is_unambiguous():
    alias = pl.read_csv(ALIAS_CSV, schema_overrides={"espn_team_id": pl.Utf8})
    assert alias["ncaa_team"].n_unique() == alias.height
    assert alias["espn_team_id"].null_count() == 0
    assert alias["note"].null_count() == 0, "every alias needs a written justification"


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
