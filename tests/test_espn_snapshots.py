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
    assert df.schema["athlete_id"] == pl.Int64 and (df["athlete_id"] > 0).all()


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


def test_ids_are_int64_and_never_float_stringified():
    """B-M15 (task 5.2, inverted from the Utf8 pin): ids are Int64 like every ESPN id, read from the integer."""
    raw = load_fixture("espn", "injuries_nfl")
    df = _snapshot("nfl")
    for column in _ID_COLUMNS:
        assert df.schema[column] == pl.Int64, column
        assert not df[column].cast(pl.Utf8).drop_nulls().str.contains(r"\.").any(), column
    want = [int(t["id"]) for t in raw["injuries"] for _ in t.get("injuries") or []]
    assert df["team_id"].to_list() == want
    assert df["injury_id"].to_list() == [int(i["id"]) for t in raw["injuries"] for i in t.get("injuries") or []]


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        (123, 123),
        ("123", 123),
        (" 123 ", 123),
        ("-1975351", -1975351),  # ESPN's injury ids are negative
        ("0", 0),
        (123.0, 123),  # never "123.0"
        (None, None),
        ("", None),
        ("  ", None),
    ],
)
def test_as_id_coerces_without_going_through_float(raw, expected):
    assert _as_id(raw, "team.id") == expected


@pytest.mark.parametrize("raw", [12.5, "abc", "007", True])
def test_as_id_warns_on_a_value_that_is_not_an_integer_id(raw):
    """R981-4: the column is Int64, so a non-integer id cannot be kept; it is null WITH a warning naming the field
    (never silently), and a zero-padded "007" is not read as 7."""
    from sportsdataverse.errors import SchemaDriftWarning

    with pytest.warns(SchemaDriftWarning, match=r"ESPN snapshot id injury\.id: .* is not an integer id"):
        assert _as_id(raw, "injury.id") is None


def test_a_padded_injury_id_on_the_real_capture_is_null_with_a_warning_and_spares_the_rest():
    """R981-4 on the real nfl capture: one record's id zero-padded is null (not unpadded) and warns; every other row
    keeps its id."""
    from sportsdataverse.errors import SchemaDriftWarning

    raw = load_fixture("espn", "injuries_nfl")
    rec = raw["injuries"][0]["injuries"][0]
    rec["id"] = "00" + str(rec["id"]).lstrip("-")
    with pytest.warns(SchemaDriftWarning, match=r"injury\.id: '00\d+' is not an integer id"):
        df = parse_injuries_snapshot(raw, league="nfl", as_of_date=STAMP)
    assert df["injury_id"][0] is None and df["injury_id"].null_count() == 1


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
    assert df["injury_id"].to_list() == [9]
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
    assert row["injury_id"] == 9
    assert row["team_id"] == 1
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
    assert df["team_id"].to_list() == [2]
    assert df["athlete_id"].to_list() == [4870808]


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
            rows = df.filter((pl.col("group_id") == int(group["id"])) & (pl.col("position_slot") == slot_key)).sort(
                "depth_rank"
            )
            assert rows["depth_rank"].to_list() == list(range(1, len(slot["athletes"]) + 1))
            assert rows["athlete_id"].to_list() == [int(a["id"]) for a in slot["athletes"]]


def test_the_slot_key_is_what_makes_the_grain_unique():
    """NFL's 3WR package ships wr1/wr2/wr3 -- three distinct depth-chart slots
    that all carry position id 1 and abbreviation WR. Keyed on the position
    alone they are indistinguishable, and "who is the WR1" is unanswerable."""
    df = _depthchart("nfl")
    wrs = df.filter((pl.col("group_name") == "3WR 1TE") & (pl.col("position_abbreviation") == "WR"))

    assert wrs["position_id"].unique().to_list() == [1]
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


def test_depthchart_ids_are_int64_and_never_float_stringified():
    df = _depthchart("mlb")
    for column in ("team_id", "group_id", "position_id", "athlete_id"):
        assert df.schema[column] == pl.Int64
        assert not [v for v in df[column].cast(pl.Utf8).drop_nulls().to_list() if "." in v]
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


def test_an_empty_teams_directory_raises_instead_of_reporting_no_depth_charts(monkeypatch):
    """Zero teams is an ESPN outage; zero depth charts is a fact about the league.

    Left unguarded both render as a zero-row frame, and the second is exactly what
    nhl/wnba/cfb legitimately return -- so an outage would silently read as
    "this league publishes no depth chart" and be skipped without a cause.
    """
    monkeypatch.setattr(
        "sportsdataverse.nba.espn_nba_teams",
        lambda *a, **k: pl.DataFrame({"team_id": []}, schema={"team_id": pl.Utf8}),
    )
    with pytest.raises(ValueError, match="returned no teams"):
        espn_depthcharts_snapshot("nba")

    # an explicitly empty team list is the caller's choice, and stays a no-op
    df = espn_depthcharts_snapshot("nba", team_ids=[], as_of_date=STAMP)
    assert df.is_empty()
    assert list(df.columns) == list(DEPTHCHART_SNAPSHOT_SCHEMA)


def _chart(athletes: list) -> dict:
    return {
        "season": {"year": 2026},
        "team": {"id": "22"},
        "depthchart": [
            {
                "id": "1",
                "name": "D",
                "positions": {
                    "qb": {
                        "position": {"id": "8", "abbreviation": "QB"},
                        "athletes": athletes,
                    }
                },
            }
        ],
    }


def test_a_malformed_athlete_entry_is_not_a_depth_slot():
    """`_mapping` would turn a non-mapping entry into an all-null row that still
    counts as a slot -- the injuries parser already skips its equivalent."""
    assert parse_depthchart_snapshot(_chart([None]), league="nfl").is_empty()
    assert parse_depthchart_snapshot(_chart([None, 7, "x"]), league="nfl").is_empty()


def test_skipping_a_malformed_entry_does_not_promote_the_players_behind_it():
    """Depth IS the array position, so a dropped entry must leave a gap. Renumbering
    would silently make the third-string QB the backup."""
    df = parse_depthchart_snapshot(
        _chart([{"id": "1", "displayName": "Starter"}, None, {"id": "3", "displayName": "Third"}]),
        league="nfl",
    )

    assert df["depth_rank"].to_list() == [1, 3]
    assert df["athlete_display_name"].to_list() == ["Starter", "Third"]


# ---------------------------------------------------------------------------
# One failed team / league does not cost the day (B-L7, task 4.5)
# ---------------------------------------------------------------------------


def test_one_failed_team_warns_and_keeps_the_other_teams(monkeypatch):
    from sportsdataverse.errors import AssetFetchError

    raw = load_fixture("espn", "depthcharts_nba")

    def fake_fetch(team_id, return_parsed=False):
        if team_id == "2":
            raise AssetFetchError("espn answered 403")
        return raw

    monkeypatch.setattr("sportsdataverse.nba.espn_nba_team_depthcharts", fake_fetch)
    with pytest.warns(UserWarning, match=r"1 of 3 fetches failed \(2\).*espn answered 403"):
        df = espn_depthcharts_snapshot("nba", team_ids=[1, 2, 17], as_of_date=STAMP, request_delay=0)
    assert df.height == 39 * 2


def test_a_parser_bug_is_not_swallowed_as_a_failed_team(monkeypatch):
    """R4.5-5: only the two fetch errors are skipped; a bug in the parse step propagates."""
    import sportsdataverse.espn_snapshots as mod

    raw = load_fixture("espn", "depthcharts_nba")
    odd = {"odd": "team 2's payload"}
    monkeypatch.setattr(
        "sportsdataverse.nba.espn_nba_team_depthcharts",
        lambda team_id, return_parsed=False: odd if team_id == "2" else raw,
    )
    real = mod.parse_depthchart_snapshot

    def buggy(payload, **kwargs):
        if payload is odd:
            raise KeyError("athletes")
        return real(payload, **kwargs)

    monkeypatch.setattr(mod, "parse_depthchart_snapshot", buggy)
    # team 1 parses fine, so a catch-all would warn and return team 1 instead of raising
    with pytest.raises(KeyError, match="athletes"):
        espn_depthcharts_snapshot("nba", team_ids=[1, 2], as_of_date=STAMP, request_delay=0)


def test_every_team_failing_raises_instead_of_an_empty_day(monkeypatch):
    from sportsdataverse.errors import AssetFetchError

    def fake_fetch(team_id, return_parsed=False):
        raise AssetFetchError("espn answered 403")

    monkeypatch.setattr("sportsdataverse.nba.espn_nba_team_depthcharts", fake_fetch)
    with pytest.raises(AssetFetchError, match="403"):
        espn_depthcharts_snapshot("nba", team_ids=[1, 2], as_of_date=STAMP, request_delay=0)


def test_one_failed_league_warns_and_keeps_the_other_leagues(monkeypatch):
    import sportsdataverse.espn_snapshots as mod
    from sportsdataverse.errors import NoDataError

    def fake(league: str):
        if league == "cfb":
            raise NoDataError("espn answered 404")
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
    with pytest.warns(UserWarning, match=r"1 of 2 fetches failed \(cfb\)"):
        df = espn_injuries_snapshot(["nfl", "cfb"], as_of_date=STAMP, request_delay=0)
    assert df["league"].unique().to_list() == ["nfl"] and df.height > 0


def _pandas_ids_are_int64(pdf, df):
    ids = [c for c, t in df.schema.items() if t == pl.Int64 and (c == "id" or c.endswith("_id"))]
    assert ids and {c: str(pdf[c].dtype) for c in ids} == dict.fromkeys(ids, "Int64")


def test_injuries_snapshot_pandas_ids_are_nullable_int64(monkeypatch):
    """R981-8: on the real nfl capture with one record's ``source.id`` missing, a plain ``to_pandas()`` floated
    ``source_id`` (``1.0``); the parser and the fetcher both hand pandas' nullable ``Int64``."""
    import sportsdataverse.espn_snapshots as mod

    raw = load_fixture("espn", "injuries_nfl")
    raw["injuries"][0]["injuries"][0]["source"].pop("id")
    df = parse_injuries_snapshot(raw, league="nfl", as_of_date=STAMP)
    assert df["source_id"].null_count() == 1
    _pandas_ids_are_int64(parse_injuries_snapshot(raw, league="nfl", as_of_date=STAMP, return_as_pandas=True), df)
    monkeypatch.setattr(
        mod.importlib,
        "import_module",
        lambda name: type("M", (), {"espn_nfl_injuries": staticmethod(lambda return_parsed=True: raw)}),
    )
    pdf = espn_injuries_snapshot("nfl", as_of_date=STAMP, request_delay=0, return_as_pandas=True)
    _pandas_ids_are_int64(pdf, df)
    assert int(pdf["source_id"].isna().sum()) == 1


def test_depthchart_snapshot_pandas_ids_are_nullable_int64(monkeypatch):
    """R981-8: the depth-chart parser and fetcher hand their ids as pandas' nullable ``Int64`` (real nba capture, one
    group id missing)."""
    import sportsdataverse.espn_snapshots as mod

    raw = load_fixture("espn", "depthcharts_nba")
    group = raw["depthchart"][0]
    group["id"] = None
    df = parse_depthchart_snapshot(raw, league="nba", as_of_date=STAMP)
    assert 0 < df["group_id"].null_count()
    _pandas_ids_are_int64(parse_depthchart_snapshot(raw, league="nba", as_of_date=STAMP, return_as_pandas=True), df)
    monkeypatch.setattr(mod.time, "sleep", lambda s: None)
    monkeypatch.setattr("sportsdataverse.nba.espn_nba_team_depthcharts", lambda team_id, return_parsed=False: raw)
    _pandas_ids_are_int64(espn_depthcharts_snapshot("nba", team_ids=[1], as_of_date=STAMP, return_as_pandas=True), df)


def test_a_bad_athlete_id_the_link_recovers_says_so():
    """R981-10: a text ``athlete.id`` whose player-card link still carries the id keeps the recovered id, and the
    warning says the link gave it rather than claiming the id was read as null."""
    from sportsdataverse.errors import SchemaDriftWarning

    raw = load_fixture("espn", "injuries_nfl")
    clean = parse_injuries_snapshot(raw, league="nfl", as_of_date=STAMP)["athlete_id"][0]
    raw["injuries"][0]["injuries"][0]["athlete"]["id"] = "abc"
    with pytest.warns(
        SchemaDriftWarning, match=rf"athlete\.id: 'abc' is not an integer id; the player link gives {clean}$"
    ):
        df = parse_injuries_snapshot(raw, league="nfl", as_of_date=STAMP)
    assert clean is not None and df["athlete_id"][0] == clean


def test_an_athlete_id_wins_over_a_link_that_disagrees():
    """R981-12: ``athlete.id`` is read first; the player link is only the fallback. On the real nfl capture with an
    integer ``athlete.id`` that differs from the link's, the direct id is the one kept (no warning: it is an id)."""
    raw = load_fixture("espn", "injuries_nfl")
    athlete = raw["injuries"][0]["injuries"][0]["athlete"]
    linked = parse_injuries_snapshot(raw, league="nfl", as_of_date=STAMP)["athlete_id"][0]
    athlete["id"] = str(linked + 1)
    assert parse_injuries_snapshot(raw, league="nfl", as_of_date=STAMP)["athlete_id"][0] == linked + 1


def test_a_bad_athlete_id_the_headshot_recovers_names_the_headshot():
    """R981-12: when only the headshot carries the id, the warning names the headshot, not the player link."""
    from sportsdataverse.errors import SchemaDriftWarning

    raw = load_fixture("espn", "injuries_nfl")
    athlete = raw["injuries"][0]["injuries"][0]["athlete"]
    clean = parse_injuries_snapshot(raw, league="nfl", as_of_date=STAMP)["athlete_id"][0]
    athlete["id"], athlete["links"] = "abc", []
    with pytest.warns(
        SchemaDriftWarning, match=rf"athlete\.id: 'abc' is not an integer id; the headshot gives {clean}$"
    ):
        df = parse_injuries_snapshot(raw, league="nfl", as_of_date=STAMP)
    assert df["athlete_id"][0] == clean
