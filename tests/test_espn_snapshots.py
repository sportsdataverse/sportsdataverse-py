"""Offline + live tests for :mod:`sportsdataverse.espn_snapshots`.

Every offline assertion runs against a REAL captured payload from
``tests/fixtures/espn/`` (injuries: 32 NFL teams / 800 athlete records, 23 CFB
teams / 27 records; depth charts: one team each for NFL 68 slots / MLB 76 /
NBA 39, plus an NHL capture that carries no depth chart at all) — not a
hand-written fixture. The bugs this module can plausibly
ship (a lost athlete id, a float-stringified id, an empty payload counted as a
row) are all invisible to a synthetic capture.
"""

from __future__ import annotations

import re
from datetime import date

import polars as pl
import pytest

from sportsdataverse.espn_snapshots import (
    DEPTHCHART_SNAPSHOT_SCHEMA,
    ESPN_DEPTHCHART_LEAGUES,
    ESPN_INJURY_LEAGUES,
    INJURY_SNAPSHOT_SCHEMA,
    _as_id,
    espn_depthcharts_snapshot,
    espn_injuries_snapshot,
    parse_depthchart_snapshot,
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


def test_athlete_id_falls_back_to_the_headshot_when_the_link_is_gone():
    """The link recovery is the only route ESPN leaves open — until it isn't.

    The id is recovered, not read, so a link reshape would null every id at once
    and the rows would still look joinable. The headshot carries the same id
    (98.7% of the 1,291 live injury records on 2026-09-02) and is the backstop.
    """
    raw = load_fixture("espn", "injuries_cfb")
    stripped = {
        "injuries": [
            {
                **team,
                "injuries": [{**rec, "athlete": {**rec["athlete"], "links": []}} for rec in team["injuries"]],
            }
            for team in raw["injuries"]
        ]
    }
    with_headshot = parse_injuries_snapshot(stripped, league="cfb", as_of_date=STAMP)
    linked = _snapshot("cfb")

    # every record in this fixture carries a headshot, so nothing is lost
    assert with_headshot.height == linked.height
    assert with_headshot["athlete_id"].null_count() == 0
    assert with_headshot["athlete_id"].to_list() == linked["athlete_id"].to_list()

    # ...and with neither route the id is null, never guessed
    blind = {
        "injuries": [
            {
                **team,
                "injuries": [
                    {**rec, "athlete": {**rec["athlete"], "links": [], "headshot": {}}} for rec in team["injuries"]
                ],
            }
            for team in raw["injuries"]
        ]
    }
    assert parse_injuries_snapshot(blind, league="cfb", as_of_date=STAMP)["athlete_id"].null_count() == 27


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


#: The only columns a record carrying nothing but ``id`` can populate. Every other
#: column in the schema is derived from a nested object, so when that object is
#: collapsed to the wrong shape its columns MUST come back null. Asserting the
#: WHOLE row against this set, rather than a per-case list of the fields the case
#: names, is what makes the test able to fail: it also catches a leak into a field
#: the case did not name.
_IDENTITY_COLUMNS = {"as_of_date", "league", "team_id", "injury_id"}


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
    row = df.row(0, named=True)
    # Identity survives...
    assert row["injury_id"] == "9"
    assert row["team_id"] == "1"
    # ...and NOTHING derived from the collapsed object leaks a value.
    assert {c for c, v in row.items() if v is not None} == _IDENTITY_COLUMNS


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


# ---------------------------------------------------------------------------
# Depth charts
# ---------------------------------------------------------------------------


def _depthchart(league: str) -> pl.DataFrame:
    return parse_depthchart_snapshot(load_fixture("espn", f"depthcharts_{league}"), league=league, as_of_date=STAMP)


@pytest.mark.parametrize(("league", "groups", "slots"), [("nfl", 3, 68), ("nba", 1, 39), ("mlb", 1, 76)])
def test_depthchart_explodes_to_the_athlete_slot_grain(league, groups, slots):
    """The payload nests group -> position -> athletes; a row is one athlete slot."""
    raw = load_fixture("espn", f"depthcharts_{league}")
    expected = sum(len(slot["athletes"]) for group in raw["depthchart"] for slot in group["positions"].values())
    assert (len(raw["depthchart"]), expected) == (groups, slots), "fixture drifted; re-derive"
    assert _depthchart(league).height == expected


def test_depthchart_schema_matches_the_declared_contract():
    df = _depthchart("nfl")
    assert list(df.columns) == list(DEPTHCHART_SNAPSHOT_SCHEMA)
    assert dict(df.schema) == DEPTHCHART_SNAPSHOT_SCHEMA


def test_depth_rank_is_the_order_espn_published():
    """The athlete array order IS the depth; the rank makes it survive a parquet
    round-trip, a sort, and a concat with another day."""
    raw = load_fixture("espn", "depthcharts_nfl")
    df = _depthchart("nfl")

    for group in raw["depthchart"]:
        for slot_key, slot in group["positions"].items():
            if not slot["athletes"]:
                continue
            rows = df.filter((pl.col("group_id") == str(group["id"])) & (pl.col("position_slot") == slot_key)).sort(
                "depth_rank"
            )
            assert rows["depth_rank"].to_list() == list(range(1, len(slot["athletes"]) + 1))
            assert rows["athlete_id"].to_list() == [str(a["id"]) for a in slot["athletes"]]


def test_the_slot_key_is_what_makes_the_grain_unique():
    """NFL's 3WR package ships wr1/wr2/wr3 -- three distinct depth-chart slots
    that all carry position id 1 and abbreviation WR. Keyed on the position
    alone they are indistinguishable, and "who is the WR1" is unanswerable."""
    df = _depthchart("nfl")
    wrs = df.filter((pl.col("group_name") == "3WR 1TE") & (pl.col("position_abbreviation") == "WR"))

    assert wrs["position_id"].unique().to_list() == ["1"]
    assert sorted(wrs["position_slot"].unique().to_list()) == ["wr1", "wr2", "wr3"]

    grain = ("team_id", "group_id", "position_slot", "depth_rank")
    assert df.select(grain).is_duplicated().sum() == 0


def test_a_league_espn_publishes_no_depthchart_for_yields_no_rows():
    """NHL answers 200 with the ``depthchart`` key absent entirely. That is an
    observation of nothing -- zero rows carrying the schema, never a placeholder
    row that a caller would count, persist, or publish as data."""
    df = _depthchart("nhl")
    assert df.is_empty()
    assert list(df.columns) == list(DEPTHCHART_SNAPSHOT_SCHEMA)
    assert "depthchart" not in load_fixture("espn", "depthcharts_nhl")


@pytest.mark.parametrize(
    "payload",
    [None, {}, {"depthchart": []}, {"depthchart": 7}, {"depthchart": [{"positions": 3}]}, "junk"],
)
def test_malformed_depthchart_payloads_degrade_to_zero_rows(payload):
    df = parse_depthchart_snapshot(payload, league="nfl", as_of_date=STAMP)
    assert df.is_empty()
    assert list(df.columns) == list(DEPTHCHART_SNAPSHOT_SCHEMA)


def test_depthchart_ids_are_utf8_and_never_float_stringified():
    df = _depthchart("mlb")
    for column in ("team_id", "group_id", "position_id", "athlete_id"):
        assert df.schema[column] == pl.Utf8
        assert not [v for v in df[column].drop_nulls().to_list() if "." in v]
    assert df["athlete_id"].null_count() == 0


def test_depthchart_fetch_stamps_one_date_and_paces_the_teams(monkeypatch):
    """One request per team, sequential, and every row carries the same stamp --
    a team fetched at 23:59:59 must not land on a different date than the first."""
    import sportsdataverse.espn_snapshots as mod

    raw = load_fixture("espn", "depthcharts_nba")
    seen: list[str] = []
    slept: list[float] = []

    def fake_fetch(team_id, return_parsed=False):
        seen.append(team_id)
        return raw

    monkeypatch.setattr(mod.time, "sleep", lambda s: slept.append(s))
    # no raising=False: if the wrapper is renamed or gone, the patch must fail
    # rather than pass silently against a function this test never exercised
    monkeypatch.setattr("sportsdataverse.nba.espn_nba_team_depthcharts", fake_fetch)

    df = espn_depthcharts_snapshot("nba", team_ids=[1, 2, 17], as_of_date=STAMP)

    assert seen == ["1", "2", "17"]
    assert slept == [1.5, 1.5], "the first request must not sleep, the rest must"
    assert df["as_of_date"].unique().to_list() == [STAMP]
    assert df.height == 39 * 3


def test_depthchart_unknown_league_raises_rather_than_returning_empty():
    with pytest.raises(ValueError, match="depth charts"):
        espn_depthcharts_snapshot("kabaddi")


def test_depthchart_league_roster_is_documented():
    """nhl/wnba/cfb are absent on purpose: ESPN publishes no depth chart for them
    (probed live 2026-09-02, 3 teams each, key absent on every response)."""
    assert ESPN_DEPTHCHART_LEAGUES == ("nfl", "nba", "mlb")


@skip_if_no_live
def test_live_depthchart_snapshot_returns_ranked_joinable_rows():
    df = espn_depthcharts_snapshot("nfl", team_ids=[22])
    assert df.height > 30
    assert df["athlete_id"].null_count() == 0
    assert df["depth_rank"].min() == 1


def test_a_league_without_a_teams_wrapper_raises_the_same_clear_error(monkeypatch):
    """The teams lookup is resolved in the same guard as the depthcharts wrapper,
    so a half-wired league fails with the documented ValueError rather than an
    AttributeError from three lines later."""
    import sportsdataverse.nba as nba

    monkeypatch.delattr(nba, "espn_nba_teams", raising=True)
    with pytest.raises(ValueError, match="depth charts"):
        espn_depthcharts_snapshot("nba")
