"""Offline + live tests for :mod:`sportsdataverse.espn_snapshots`.

Every offline assertion runs against a REAL captured payload from
``tests/fixtures/espn/`` (32 NFL teams / 800 athlete records, 23 CFB teams /
27 records) — not a hand-written fixture. The bugs this module can plausibly
ship (a lost athlete id, a float-stringified id, an empty payload counted as a
row) are all invisible to a synthetic capture.
"""

from __future__ import annotations

import re
from datetime import date

import polars as pl
import pytest

from sportsdataverse.espn_snapshots import (
    ESPN_INJURY_LEAGUES,
    INJURY_SNAPSHOT_SCHEMA,
    _as_id,
    espn_injuries_snapshot,
    parse_injuries_snapshot,
)
from tests.conftest import load_fixture, skip_if_no_live

STAMP = date(2026, 9, 2)
_ID_COLUMNS = ("team_id", "injury_id", "athlete_id", "type_id", "source_id")


def _snapshot(league: str) -> pl.DataFrame:
    return parse_injuries_snapshot(load_fixture("espn", f"injuries_{league}"), league=league, as_of_date=STAMP)


# ---------------------------------------------------------------------------
# Shape + contract
# ---------------------------------------------------------------------------


def test_nfl_fixture_explodes_to_the_athlete_grain():
    """The raw payload is one row per TEAM; the snapshot is one row per injury."""
    raw = load_fixture("espn", "injuries_nfl")
    expected = sum(len(t.get("injuries") or []) for t in raw["injuries"])
    df = _snapshot("nfl")
    assert expected == 800, "fixture drifted; re-derive the expected count"
    assert df.height == expected


def test_schema_matches_the_declared_contract():
    df = _snapshot("nfl")
    assert list(df.columns) == list(INJURY_SNAPSHOT_SCHEMA)
    assert dict(df.schema) == INJURY_SNAPSHOT_SCHEMA


def test_every_row_carries_the_observation_stamp_and_league():
    df = _snapshot("cfb")
    assert df.height == 27
    assert df["as_of_date"].unique().to_list() == [STAMP]
    assert df["league"].unique().to_list() == ["cfb"]


# ---------------------------------------------------------------------------
# Id discipline — these are join keys
# ---------------------------------------------------------------------------


def test_athlete_id_is_recovered_from_the_player_card_link():
    """ESPN omits ``athlete.id`` from this payload on every record.

    Verified against the capture: 0 of 800 NFL records carry ``athlete.id``.
    Reading it directly would null the column and make the snapshot unjoinable
    to any roster, so it is parsed out of the player-card href instead.
    """
    raw = load_fixture("espn", "injuries_nfl")
    direct = sum(
        1 for team in raw["injuries"] for rec in team.get("injuries") or [] if (rec.get("athlete") or {}).get("id")
    )
    assert direct == 0
    df = _snapshot("nfl")
    assert df["athlete_id"].null_count() == 0
    assert df["athlete_id"].str.contains(r"^\d+$").all()


def test_ids_are_utf8_and_never_float_stringified():
    df = _snapshot("nfl")
    for column in _ID_COLUMNS:
        assert df.schema[column] == pl.Utf8, column
        assert not df[column].drop_nulls().str.contains(r"\.").any(), column


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        (123, "123"),
        ("123", "123"),
        (123.0, "123"),  # never "123.0"
        (12.5, None),
        (True, None),  # bool is an int subclass; not an id
        (None, None),
        ("", None),
    ],
)
def test_as_id_coerces_without_going_through_float(raw, expected):
    assert _as_id(raw) == expected


# ---------------------------------------------------------------------------
# Empty is an observation, not a row
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("payload", [None, {}, {"injuries": []}, {"injuries": None}])
def test_empty_payload_yields_zero_rows_with_the_full_schema(payload):
    df = parse_injuries_snapshot(payload, league="mbb", as_of_date=STAMP)
    assert df.height == 0
    assert dict(df.schema) == INJURY_SNAPSHOT_SCHEMA


def test_out_of_season_league_contributes_no_placeholder_row():
    """``mbb`` answers 200 with an empty list in September — 0 rows, not 1 null row."""
    df = _snapshot("mbb")
    assert df.height == 0


def test_malformed_entries_are_skipped_not_raised():
    payload = {"injuries": ["junk", {"id": "1", "injuries": ["junk", {"id": "9"}]}]}
    df = parse_injuries_snapshot(payload, league="nfl", as_of_date=STAMP)
    assert df.height == 1
    assert df["injury_id"].to_list() == ["9"]
    assert df["athlete_id"].to_list() == [None]


#: Every nested container ESPN could collapse to the wrong shape. Each of these
#: raised before the ``_list`` / ``_mapping`` guards: a truthy scalar raises
#: ``TypeError`` when iterated and ``AttributeError`` on ``.get``, and neither
#: ``value or []`` nor ``value or {}`` catches a truthy one. The parser's contract
#: is to degrade to zero rows on malformed input, so all of these must parse.
_MALFORMED = {
    "payload_is_not_a_mapping": ["nope"],
    "outer_injuries_is_a_scalar": {"injuries": 5},
    "team_injuries_is_a_scalar": {"injuries": [{"id": "1", "injuries": 5}]},
    "team_injuries_is_a_string": {"injuries": [{"id": "1", "injuries": "oops"}]},
    "team_injuries_is_a_dict": {"injuries": [{"id": "1", "injuries": {"a": 1}}]},
}


@pytest.mark.parametrize("payload", _MALFORMED.values(), ids=list(_MALFORMED))
def test_malformed_containers_degrade_to_zero_rows(payload):
    df = parse_injuries_snapshot(payload, league="nfl", as_of_date=STAMP)
    assert df.height == 0
    assert dict(df.schema) == INJURY_SNAPSHOT_SCHEMA


@pytest.mark.parametrize(
    "record",
    [
        {"id": "9", "type": 5},
        {"id": "9", "source": "x"},
        {"id": "9", "details": 1},
        {"id": "9", "athlete": 2},
        {"id": "9", "athlete": {"links": 7}},
        {"id": "9", "athlete": {"links": [3]}},
        {"id": "9", "athlete": {"position": 4}},
    ],
    ids="type source details athlete links link_elem position".split(),
)
def test_collapsed_nested_objects_null_their_fields_instead_of_raising(record):
    df = parse_injuries_snapshot(
        {"injuries": [{"id": "1", "injuries": [record]}]},
        league="nfl",
        as_of_date=STAMP,
    )
    assert df.height == 1
    assert df["injury_id"].to_list() == ["9"]


def test_a_malformed_team_does_not_cost_the_valid_teams():
    """The whole run must not abort on one bad team -- and the good one still parses."""
    payload = {
        "injuries": [
            {"id": "1", "injuries": 5},
            {
                "id": "2",
                "injuries": [
                    {
                        "id": "9",
                        "athlete": {"links": [{"href": "https://e.com/id/4870808/x"}]},
                    }
                ],
            },
        ]
    }
    df = parse_injuries_snapshot(payload, league="nfl", as_of_date=STAMP)
    assert df.height == 1
    assert df["team_id"].to_list() == ["2"]
    assert df["athlete_id"].to_list() == ["4870808"]


# ---------------------------------------------------------------------------
# Multi-league concat + fetch plumbing
# ---------------------------------------------------------------------------


def test_leagues_concat_on_one_stable_schema():
    df = pl.concat([_snapshot("nfl"), _snapshot("cfb")], how="vertical")
    assert df.height == 827
    assert sorted(df["league"].unique().to_list()) == ["cfb", "nfl"]


def test_unknown_league_raises_rather_than_returning_empty():
    with pytest.raises(ValueError, match="no espn_notaleague_injuries"):
        espn_injuries_snapshot("notaleague", request_delay=0)


def test_fetch_stamps_one_date_across_leagues(monkeypatch):
    """A run that straddles UTC midnight must not stamp two different dates."""
    import sportsdataverse.espn_snapshots as mod

    calls: list[str] = []

    def fake(league: str):
        calls.append(league)
        return load_fixture("espn", f"injuries_{league}")

    monkeypatch.setattr(
        mod.importlib,
        "import_module",
        lambda name: type(
            "M",
            (),
            {
                f"espn_{name.split('.')[-1]}_injuries": staticmethod(
                    lambda return_parsed=True, _lg=name.split(".")[-1]: fake(_lg)
                )
            },
        ),
    )
    df = espn_injuries_snapshot(["nfl", "cfb"], as_of_date=STAMP, request_delay=0)
    assert calls == ["nfl", "cfb"]
    assert df["as_of_date"].unique().to_list() == [STAMP]
    assert df.height == 827


def test_league_roster_is_documented():
    assert set(ESPN_INJURY_LEAGUES) >= {"nfl", "nba", "wnba", "nhl", "mlb", "cfb"}


# ---------------------------------------------------------------------------
# Live
# ---------------------------------------------------------------------------


@skip_if_no_live
def test_live_nfl_snapshot_returns_joinable_rows():
    df = espn_injuries_snapshot("nfl", request_delay=0)
    assert df.height > 0
    assert dict(df.schema) == INJURY_SNAPSHOT_SCHEMA
    assert df["athlete_id"].null_count() == 0
    assert df["team_id"].null_count() == 0
    assert df["as_of_date"].n_unique() == 1


@skip_if_no_live
def test_live_multi_league_snapshot_is_long_over_league():
    df = espn_injuries_snapshot(["nfl", "nhl"], request_delay=1.5)
    assert sorted(df["league"].unique().to_list()) == ["nfl", "nhl"]
    assert not re.search(r"\.\d", "".join(df["team_id"].drop_nulls().to_list()))
