"""Tests for the cross-provider CFB crosswalks (``sportsdataverse.cfb.cfb_crosswalk``).

The bulk of this file is **offline**: the normalization layer and the pure
``_merge_*`` builders need no network, and the network adapters + public
functions are exercised by monkeypatching the provider wrappers with tiny
synthetic frames. A small live-gated section at the bottom (``SDV_PY_LIVE_TESTS=1``)
smoke-tests the real endpoints.
"""

from __future__ import annotations

from typing import Any, Dict

import pandas as pd
import polars as pl
import pytest

import sportsdataverse.cfb.cfb_crosswalk as cw
from sportsdataverse.cfb.cfb_crosswalk import (
    _ascii_fold,
    _iso_date,
    _matched_sources,
    _matchup_key,
    _materialize,
    _merge_odds,
    _merge_rosters,
    _merge_schedule,
    _merge_teams,
    _norm_jersey,
    _norm_person,
    _norm_team,
    _pick,
    _yahoo_date,
    cfb_odds_events_crosswalk,
    cfb_rosters_crosswalk,
    cfb_schedule_crosswalk,
    cfb_teams_crosswalk,
)
from sportsdataverse.cfb.cfb_crosswalk import (
    _ODDS_SCHEMA,
    _ROSTER_SCHEMA,
    _SCHEDULE_SCHEMA,
    _TEAMS_SCHEMA,
)
from tests.conftest import skip_if_no_live

# ===========================================================================
# Normalization layer
# ===========================================================================


@pytest.mark.parametrize(
    "raw,expected",
    [
        ("Ohio State Buckeyes", "ohio state buckeyes"),
        ("OHIO STATE BUCKEYES", "ohio state buckeyes"),  # case-fold
        ("San José State Spartans", "san jose state spartans"),  # accents
        ("Texas A&M Aggies", "texas a m aggies"),  # punctuation -> space
        ("Miami (FL) Hurricanes", "miami hurricanes"),  # alias collapse
        ("Ole Miss Rebels", "mississippi rebels"),  # alias
        ("UConn Huskies", "connecticut huskies"),  # alias
        ("North Carolina State Wolfpack", "nc state wolfpack"),  # Fox vs ESPN/Yahoo
        ("Grambling State Tigers", "grambling tigers"),  # FCS alias (drops "State")
        ("Delaware Fightin' Blue Hens", "delaware blue hens"),  # FCS alias
        ("Tennessee-Martin Skyhawks", "ut martin skyhawks"),  # FCS alias
        ("  Florida   Gators  ", "florida gators"),  # whitespace collapse
        (None, ""),
        ("", ""),
    ],
)
def test_norm_team(raw: str, expected: str) -> None:
    assert _norm_team(raw) == expected


def test_norm_team_aliases_are_non_colliding() -> None:
    """Every alias value is a distinct team key (aliases only unify, never merge
    two different teams)."""
    values = list(cw._TEAM_ALIASES.values())
    # no alias maps onto another alias's *source* (would chain unexpectedly)
    assert not (set(cw._TEAM_ALIASES) & set(values)), "alias source/target overlap"


@pytest.mark.parametrize(
    "raw,expected",
    [
        ("C.J. Stroud", "c j stroud"),
        ("Stroud, C.J.", "c j stroud"),  # inverted "Last, First"
        ("Marvin Harrison Jr.", "marvin harrison jr"),
        ("Kenneth Walker III", "kenneth walker iii"),
        (None, ""),
        ("", ""),
    ],
)
def test_norm_person(raw: str, expected: str) -> None:
    assert _norm_person(raw) == expected


@pytest.mark.parametrize(
    "raw,expected",
    [("07", "7"), ("#0", "0"), ("23", "23"), ("", ""), (None, ""), (12, "12"), ("00", "0")],
)
def test_norm_jersey(raw: Any, expected: str) -> None:
    assert _norm_jersey(raw) == expected


def test_matchup_key_is_order_independent() -> None:
    assert _matchup_key("ohio state buckeyes", "akron zips") == _matchup_key("akron zips", "ohio state buckeyes")
    assert _matchup_key("b team", "") == "b team"  # empty side dropped


def test_yahoo_date_parses_and_tolerates_garbage() -> None:
    assert _yahoo_date("Sat, 29 Aug 2026 16:00:00 +0000") == "2026-08-29"
    assert _yahoo_date("not a date") is None
    assert _yahoo_date(None) is None


def test_iso_date_takes_prefix() -> None:
    assert _iso_date("2024-09-07T23:30Z") == "2024-09-07"
    assert _iso_date(None) is None


def test_ascii_fold_and_pick_and_matched_sources() -> None:
    assert _ascii_fold("Hawaiʻi") == "Hawaii"
    assert _pick({"a": None, "b": 2}, "a", "b") == 2
    assert _pick({"a": None}, "a", "missing") is None
    assert _matched_sources([("espn", True), ("fox", False), ("yahoo", True)]) == "espn+yahoo"


# ===========================================================================
# Pure merge builders
# ===========================================================================


def _team(norm: str, tid: Any, name: str, abbr: str) -> Dict[str, Any]:
    return {"norm_key": norm, "team_id": tid, "name": name, "abbreviation": abbr}


def test_merge_teams_full_and_partial_matches() -> None:
    espn = [
        _team("ohio state buckeyes", 194, "Ohio State Buckeyes", "OSU"),
        _team("michigan wolverines", 130, "Michigan Wolverines", "MICH"),
    ]
    fox = [
        _team("ohio state buckeyes", "25", "OHIO STATE BUCKEYES", "OSU"),
        _team("akron zips", "1", "AKRON ZIPS", "AKR"),  # fox-only
    ]
    yahoo = [
        _team("ohio state buckeyes", "ncaaf.t.194", "Ohio State Buckeyes", "OSU"),
        _team("toledo rockets", "ncaaf.t.99", "Toledo Rockets", "TOL"),  # yahoo-only
    ]
    rows = _merge_teams(espn, fox, yahoo)
    by = {r["norm_key"]: r for r in rows}

    assert by["ohio state buckeyes"]["matched_sources"] == "espn+fox+yahoo"
    assert by["ohio state buckeyes"]["fox_team_id"] == "25"
    assert by["ohio state buckeyes"]["yahoo_team_id"] == "ncaaf.t.194"
    # espn-only
    assert by["michigan wolverines"]["matched_sources"] == "espn"
    assert by["michigan wolverines"]["fox_team_id"] is None
    # fox-only and yahoo-only rows appended
    assert by["akron zips"]["matched_sources"] == "fox"
    assert by["akron zips"]["espn_team_id"] is None
    assert by["toledo rockets"]["matched_sources"] == "yahoo"
    assert len(rows) == 4


def test_merge_teams_empty_inputs() -> None:
    assert _merge_teams([], [], []) == []


def _game(mk: str, gid: Any, home: str, away: str, date: str, **extra: Any) -> Dict[str, Any]:
    base = {
        "matchup_key": mk,
        "game_id": gid,
        "home_team": home,
        "away_team": away,
        "date": date,
        "home_norm": _norm_team(home),
        "away_norm": _norm_team(away),
    }
    base.update(extra)
    return base


def test_merge_schedule_matches_on_unordered_pair() -> None:
    # ESPN lists OSU as home; Fox & Yahoo list Akron as home. Each side derives
    # its own key from its own (oppositely-ordered) names -> the keys must still
    # be equal for the three-way merge to join them, which is the property tested.
    espn_mk = _matchup_key(_norm_team("Ohio State Buckeyes"), _norm_team("Akron Zips"))
    other_mk = _matchup_key(_norm_team("Akron Zips"), _norm_team("Ohio State Buckeyes"))
    assert espn_mk == other_mk, "order-independent key precondition"
    espn = [_game(espn_mk, 401752687, "Ohio State Buckeyes", "Akron Zips", "2024-08-31")]
    fox = [_game(other_mk, "41999", "Akron Zips", "Ohio State Buckeyes", "2024-08-31")]
    yahoo = [
        _game(
            other_mk,
            "ncaaf.g.202408310194",
            "Akron Zips",
            "Ohio State Buckeyes",
            "2024-08-31",
            global_game_id="ncaaf.g.123",
        )
    ]
    rows = _merge_schedule(espn, fox, yahoo)
    assert len(rows) == 1
    assert rows[0]["matched_sources"] == "espn+fox+yahoo"
    assert rows[0]["espn_game_id"] == 401752687
    assert rows[0]["fox_game_id"] == "41999"
    assert rows[0]["yahoo_game_id"] == "ncaaf.g.202408310194"
    assert rows[0]["yahoo_global_game_id"] == "ncaaf.g.123"


def test_merge_schedule_fox_only_games_are_dropped() -> None:
    # A Fox segment spans a whole phase, so a fox-only game (no ESPN/Yahoo match)
    # is a different week's game -> it must NOT be appended. Yahoo-only games ARE.
    espn = [_game("a|b", 1, "A", "B", "2024-01-01")]
    fox = [_game("a|b", "f1", "A", "B", "2024-01-01"), _game("x|z", "f2", "X", "Z", "2024-01-01")]
    yahoo = [_game("c|d", "y", "C", "D", "2024-01-01")]
    rows = _merge_schedule(espn, fox, yahoo)
    by_src = sorted(r["matched_sources"] for r in rows)
    assert by_src == ["espn+fox", "yahoo"]  # fox-only "x|z" dropped; yahoo-only "c|d" kept
    espn_row = next(r for r in rows if r["espn_game_id"] == 1)
    assert espn_row["fox_game_id"] == "f1"


# ---- full-season (date-aware) merge -------------------------------------------------


def test_date_dist() -> None:
    assert cw._date_dist("2024-09-28", "2024-09-30") == 2
    assert cw._date_dist("2024-09-28T20:00Z", "2024-09-28") == 0  # tolerates ISO suffix
    assert cw._date_dist(None, "2024-01-01") == 10**6
    assert cw._date_dist("garbage", "2024-01-01") == 10**6


def test_pick_match_prefers_exact_then_nearest() -> None:
    cands = [{"date": "2024-09-28", "game_id": "a"}, {"date": "2024-12-07", "game_id": "b"}]
    assert cw._pick_match(cands, "2024-12-07")["game_id"] == "b"  # exact date
    assert cw._pick_match(cands, "2024-12-05")["game_id"] == "b"  # nearest date
    assert cw._pick_match([], "x") is None
    assert cw._pick_match([{"game_id": "z"}], None)["game_id"] == "z"  # sole candidate


def test_merge_schedule_full_rematch_disambiguated_by_date() -> None:
    # Georgia and Alabama meet twice (regular game + SEC championship). The
    # date-aware merge must pair each ESPN game with the same-date Fox/Yahoo game.
    mk = _matchup_key("alabama crimson tide", "georgia bulldogs")
    espn = [
        _game(mk, 1, "Alabama Crimson Tide", "Georgia Bulldogs", "2024-09-28"),
        _game(mk, 2, "Georgia Bulldogs", "Alabama Crimson Tide", "2024-12-07"),
    ]
    fox = [_game(mk, "f1", "A", "B", "2024-09-28"), _game(mk, "f2", "B", "A", "2024-12-07")]
    yahoo = [_game(mk, "y1", "A", "B", "2024-09-28"), _game(mk, "y2", "B", "A", "2024-12-07")]
    rows = cw._merge_schedule_full(espn, fox, yahoo)
    by_espn = {r["espn_game_id"]: r for r in rows if r["espn_game_id"] is not None}
    assert by_espn[1]["fox_game_id"] == "f1" and by_espn[1]["yahoo_game_id"] == "y1"
    assert by_espn[2]["fox_game_id"] == "f2" and by_espn[2]["yahoo_game_id"] == "y2"


def test_merge_schedule_full_graceful_and_yahoo_only() -> None:
    espn = [_game("a|b", 1, "A", "B", "2024-09-01")]  # no Fox match -> graceful null
    fox = [_game("x|z", "f9", "X", "Z", "2024-12-30")]  # fox-only -> dropped
    yahoo = [_game("a|b", "y1", "A", "B", "2024-09-01"), _game("c|d", "y2", "C", "D", "2024-09-01")]
    rows = cw._merge_schedule_full(espn, fox, yahoo)
    by_src = sorted(r["matched_sources"] for r in rows)
    assert by_src == ["espn+yahoo", "yahoo"]  # espn game (fox null) + yahoo-only "c|d"; fox-only dropped
    assert next(r for r in rows if r["espn_game_id"] == 1)["fox_game_id"] is None


def test_merge_rosters_name_jersey_and_conflict() -> None:
    espn = [
        {
            "person_key": "c j stroud",
            "jersey_key": "7",
            "athlete_id": 4432,
            "name": "C.J. Stroud",
            "jersey": "7",
            "position": "QB",
        },
        {
            "person_key": "marvin harrison jr",
            "jersey_key": "18",
            "athlete_id": 4433,
            "name": "Marvin Harrison Jr.",
            "jersey": "18",
            "position": "WR",
        },
        {
            "person_key": "espn only",
            "jersey_key": "",
            "athlete_id": 1,
            "name": "Espn Only",
            "jersey": None,
            "position": "RB",
        },
    ]
    fox = [
        {
            "person_key": "c j stroud",
            "jersey_key": "7",
            "athlete_id": "f7",
            "name": "C.J. Stroud",
            "jersey": "7",
            "position": "QB",
        },
        {
            "person_key": "marvin harrison jr",
            "jersey_key": "80",
            "athlete_id": "f80",
            "name": "Marvin Harrison Jr.",
            "jersey": "80",
            "position": "WR",
        },
        {
            "person_key": "fox only",
            "jersey_key": "",
            "athlete_id": "fx",
            "name": "Fox Only",
            "jersey": None,
            "position": "OL",
        },
    ]
    rows = _merge_rosters(espn, fox)
    by = {r["person_key"]: r for r in rows}
    assert by["c j stroud"]["match_method"] == "name_jersey"
    assert by["c j stroud"]["matched_sources"] == "espn+fox"
    assert by["marvin harrison jr"]["match_method"] == "name_jersey_conflict"
    assert by["espn only"]["match_method"] == "unmatched"
    assert by["fox only"]["match_method"] == "unmatched"
    assert by["fox only"]["espn_athlete_id"] is None


def test_merge_rosters_name_only_when_no_jersey() -> None:
    espn = [
        {
            "person_key": "seuseu alofaituli",
            "jersey_key": "",
            "athlete_id": 9,
            "name": "Seuseu Alofaituli",
            "jersey": None,
            "position": "OL",
        }
    ]
    fox = [
        {
            "person_key": "seuseu alofaituli",
            "jersey_key": "",
            "athlete_id": "f9",
            "name": "Seuseu Alofaituli",
            "jersey": None,
            "position": "OL",
        }
    ]
    rows = _merge_rosters(espn, fox)
    assert rows[0]["match_method"] == "name"


def test_merge_odds_matches_to_espn() -> None:
    mk = _matchup_key("ohio state buckeyes", "akron zips")
    events = [
        {
            "matchup_key": mk,
            "event_id": "abc123",
            "commence_time": "2024-08-31T23:30:00Z",
            "home_team": "Ohio State Buckeyes",
            "away_team": "Akron Zips",
        }
    ]
    games = [{"matchup_key": mk, "game_id": 401752687, "date": "2024-08-31"}]
    rows = _merge_odds(events, games)
    assert rows[0]["odds_event_id"] == "abc123"
    assert rows[0]["espn_game_id"] == 401752687
    assert rows[0]["matched_sources"] == "odds+espn"


def test_merge_odds_unmatched_event() -> None:
    events = [{"matchup_key": "x|y", "event_id": "e1", "commence_time": None, "home_team": "X", "away_team": "Y"}]
    rows = _merge_odds(events, [])
    assert rows[0]["espn_game_id"] is None
    assert rows[0]["matched_sources"] == "odds"


# ===========================================================================
# Materialization
# ===========================================================================


def test_materialize_empty_keeps_schema() -> None:
    for schema in (_TEAMS_SCHEMA, _SCHEDULE_SCHEMA, _ODDS_SCHEMA, _ROSTER_SCHEMA):
        df = _materialize([], schema, return_as_pandas=False)
        assert df.height == 0
        assert df.columns == list(schema)


def test_materialize_coerces_int_ids_and_pandas_path() -> None:
    rows = [{"norm_key": "x", "espn_team_id": "194", "fox_team_id": 25}]
    df = _materialize(rows, _TEAMS_SCHEMA, return_as_pandas=False)
    assert df.schema["espn_team_id"] == pl.Int64
    assert df["espn_team_id"][0] == 194  # str -> int coercion
    assert df.schema["fox_team_id"] == pl.Utf8
    assert df["fox_team_id"][0] == "25"  # int -> str
    pdf = _materialize(rows, _TEAMS_SCHEMA, return_as_pandas=True)
    assert isinstance(pdf, pd.DataFrame)


def test_materialize_bad_int_becomes_null() -> None:
    rows = [{"espn_team_id": "not-an-int"}]
    df = _materialize(rows, _TEAMS_SCHEMA, return_as_pandas=False)
    assert df["espn_team_id"][0] is None


# ===========================================================================
# Network adapters (monkeypatched providers -> no network)
# ===========================================================================


def test_espn_team_dir_projection(monkeypatch: pytest.MonkeyPatch) -> None:
    fake = pl.DataFrame(
        {
            "team_id": [194],
            "team_abbreviation": ["OSU"],
            "team_display_name": ["Ohio State Buckeyes"],
            "team_location": ["Ohio State"],
            "team_name": ["Buckeyes"],
        }
    )
    monkeypatch.setattr(cw, "espn_cfb_teams", lambda **k: fake)
    out = cw._espn_team_dir()
    assert out == [
        {"norm_key": "ohio state buckeyes", "team_id": 194, "name": "Ohio State Buckeyes", "abbreviation": "OSU"}
    ]


def test_fox_team_dir_projection(monkeypatch: pytest.MonkeyPatch) -> None:
    fake = pl.DataFrame(
        {
            "fox_team_id": ["25"],
            "abbreviation": ["OSU"],
            "name": ["OHIO STATE BUCKEYES"],
            "slug": ["ohio-state-buckeyes"],
            "color": ["1"],
            "logo_url": ["u"],
        }
    )
    monkeypatch.setattr(cw, "fox_cfb_teams", lambda **k: fake)
    out = cw._fox_team_dir()
    assert out == [
        {"norm_key": "ohio state buckeyes", "team_id": "25", "name": "OHIO STATE BUCKEYES", "abbreviation": "OSU"}
    ]


def test_yahoo_team_dir_projection(monkeypatch: pytest.MonkeyPatch) -> None:
    fake = pl.DataFrame(
        {
            "team_id": ["ncaaf.t.194"],
            "abbreviation": ["OSU"],
            "display_name": ["Ohio State"],
            "full_name": ["Ohio State Buckeyes"],
        }
    )
    monkeypatch.setattr(cw, "yahoo_cfb_teams", lambda season, week, **k: fake)
    out = cw._yahoo_team_dir(2024, 1)
    assert out[0]["norm_key"] == "ohio state buckeyes"
    assert out[0]["team_id"] == "ncaaf.t.194"


def test_espn_games_projection(monkeypatch: pytest.MonkeyPatch) -> None:
    fake = pl.DataFrame(
        {
            "game_id": [401752687],
            "date": ["2024-08-31T23:30Z"],
            "home_display_name": ["Ohio State Buckeyes"],
            "away_display_name": ["Akron Zips"],
        }
    )
    monkeypatch.setattr(cw, "espn_cfb_schedule", lambda **k: fake)
    out = cw._espn_games(2024, 1, 2)
    assert out[0]["game_id"] == 401752687
    assert out[0]["date"] == "2024-08-31"
    assert out[0]["matchup_key"] == _matchup_key("ohio state buckeyes", "akron zips")


def test_yahoo_games_projection(monkeypatch: pytest.MonkeyPatch) -> None:
    raw = {
        "service": {
            "scoreboard": {
                "teams": {
                    "ncaaf.t.194": {"full_name": "Ohio State Buckeyes"},
                    "ncaaf.t.1": {"full_name": "Akron Zips"},
                },
                "games": {
                    "ncaaf.g.202408310194": {
                        "home_team_id": "ncaaf.t.194",
                        "away_team_id": "ncaaf.t.1",
                        "global_gameid": "ncaaf.g.123",
                        "start_time": "Sat, 31 Aug 2024 23:30:00 +0000",
                    }
                },
            }
        }
    }
    monkeypatch.setattr(cw, "yahoo_cfb_scoreboard", lambda season, week, **k: raw)
    out = cw._yahoo_games(2024, 1)
    assert out[0]["game_id"] == "ncaaf.g.202408310194"
    assert out[0]["matchup_key"] == _matchup_key("ohio state buckeyes", "akron zips")
    assert out[0]["date"] == "2024-08-31"


def test_yahoo_games_team_id_missing_from_map(monkeypatch: pytest.MonkeyPatch) -> None:
    # A game can reference a team id absent from the embedded teams map
    # (id_to_name.get -> None). The adapter must degrade gracefully, not crash.
    raw = {
        "service": {
            "scoreboard": {
                "teams": {"ncaaf.t.194": {"full_name": "Ohio State Buckeyes"}},
                "games": {
                    "ncaaf.g.1": {
                        "home_team_id": "ncaaf.t.194",
                        "away_team_id": "ncaaf.t.999",  # not in teams map
                        "start_time": "Sat, 31 Aug 2024 23:30:00 +0000",
                    }
                },
            }
        }
    }
    monkeypatch.setattr(cw, "yahoo_cfb_scoreboard", lambda season, week, **k: raw)
    out = cw._yahoo_games(2024, 1)
    assert out[0]["away_team"] is None
    assert out[0]["home_team"] == "Ohio State Buckeyes"
    # only the resolvable side contributes to the (single-team) matchup key
    assert out[0]["matchup_key"] == "ohio state buckeyes"


def test_fox_games_projection(monkeypatch: pytest.MonkeyPatch) -> None:
    fake = pl.DataFrame(
        {
            "game_id": ["41999"],
            "date": ["2024-08-31T20:00:00Z"],
            "home_team": ["Ohio State Buckeyes"],
            "away_team": ["Akron Zips"],
        }
    )
    captured: dict[str, Any] = {}

    def fake_sched(*, segment_id: str, **k: Any) -> pl.DataFrame:
        captured["segment_id"] = segment_id
        return fake

    monkeypatch.setattr(cw, "fox_cfb_schedule", fake_sched)
    out = cw._fox_games(2024, 5)
    # the adapter fetches exactly the regular-season week segment ("-1" suffix)
    assert captured["segment_id"] == "2024-5-1"
    assert out[0]["game_id"] == "41999"
    assert out[0]["date"] == "2024-08-31"
    assert out[0]["matchup_key"] == _matchup_key("ohio state buckeyes", "akron zips")


def test_fox_games_swallows_errors(monkeypatch: pytest.MonkeyPatch) -> None:
    # Fox is best-effort: a failure must yield [] so the ESPN/Yahoo core survives.
    def boom(**k: Any) -> pl.DataFrame:
        raise RuntimeError("fox is down")

    monkeypatch.setattr(cw, "fox_cfb_schedule", boom)
    assert cw._fox_games(2024, 1) == []


def test_fox_cfb_schedule_parses_segment(monkeypatch: pytest.MonkeyPatch) -> None:
    # Exercise the real fox_cfb_schedule parser (home/away resolved via tokens).
    import sportsdataverse.cfb.cfb_fox_ext as fox

    payload = {
        "sectionList": [
            {
                "title": "WEEK 1",
                "events": [
                    {
                        "contentUri": "football/cfb/events/42816",
                        "eventTime": "2025-12-13T20:00:00Z",
                        "statusLine": "FINAL",
                        "entityLink": {
                            "layout": {
                                "tokens": {
                                    "id": "42816",
                                    "homeUri": "football/cfb/teams/62",
                                    "awayUri": "football/cfb/teams/47",
                                }
                            }
                        },
                        "upperTeam": {
                            "uri": "football/cfb/teams/47",
                            "stackedNameTop": "Army",
                            "stackedNameBottom": "Black Knights",
                        },
                        "lowerTeam": {
                            "uri": "football/cfb/teams/62",
                            "stackedNameTop": "Navy",
                            "stackedNameBottom": "Midshipmen",
                        },
                    }
                ],
            }
        ]
    }
    monkeypatch.setattr(fox, "_fox_get", lambda path, **k: payload)
    df = fox.fox_cfb_schedule(segment_id="2025-bowls-2")
    row = df.row(0, named=True)
    assert row["game_id"] == "42816"
    assert row["home_team"] == "Navy Midshipmen" and row["home_team_id"] == "62"
    assert row["away_team"] == "Army Black Knights" and row["away_team_id"] == "47"
    assert row["week_label"] == "WEEK 1" and row["segment_id"] == "2025-bowls-2"


def test_fox_segment_ids_enumerates_and_remaps_season(monkeypatch: pytest.MonkeyPatch) -> None:
    import sportsdataverse.cfb.cfb_fox_ext as fox

    main = {
        "selectionGroupList": [
            {"title": "REGULAR SEASON", "selectionList": [{"id": "2030-1-1"}, {"id": "2030-2-1"}]},
            {"title": "Bowls", "selectionList": [{"id": "2030-bowls-2"}]},
        ]
    }
    cfp_main = {"selectionGroupList": [{"title": "Bowls", "selectionList": [{"id": "2030-cfp-2"}]}]}
    monkeypatch.setattr(
        fox, "_fox_get", lambda path, params=None, **k: cfp_main if (params or {}).get("groupId") == "cfp" else main
    )
    # enumerated for a *different* season -> prefix remapped to 2024, deduped, ordered
    ids = fox._fox_segment_ids(2024, "2")
    assert ids == ["2024-1-1", "2024-2-1", "2024-bowls-2", "2024-cfp-2"]


def test_fox_cfb_schedule_full_season_unions_and_dedups(monkeypatch: pytest.MonkeyPatch) -> None:
    import sportsdataverse.cfb.cfb_fox_ext as fox

    main = {"selectionGroupList": [{"selectionList": [{"id": "2025-1-1"}, {"id": "2025-bowls-2"}]}]}
    cfp_main = {"selectionGroupList": [{"selectionList": [{"id": "2025-cfp-2"}]}]}

    def seg(gid: str, *ids: str) -> Dict[str, Any]:
        return {
            "sectionList": [
                {
                    "title": "S",
                    "events": [
                        {"entityLink": {"layout": {"tokens": {"id": i}}}, "eventTime": "2025-01-01T00:00Z"} for i in ids
                    ],
                }
            ]
        }

    payloads = {
        "cfb/league/scores-segment/2025-1-1": seg("w1", "100", "101"),
        "cfb/league/scores-segment/2025-bowls-2": seg("b", "900", "901"),
        "cfb/league/scores-segment/2025-cfp-2": seg("c", "900"),  # 900 overlaps bowls -> deduped
    }

    def fake_get(path: str, params: Any = None, **k: Any) -> Dict[str, Any]:
        if path == "cfb/scoreboard/main":
            return cfp_main if (params or {}).get("groupId") == "cfp" else main
        return payloads[path]

    monkeypatch.setattr(fox, "_fox_get", fake_get)
    df = fox.fox_cfb_schedule(2025)
    ids = df["game_id"].to_list()
    assert ids == ["100", "101", "900", "901"]  # cfp's duplicate 900 dropped
    assert df["game_id"].n_unique() == df.height


def test_espn_roster_projection(monkeypatch: pytest.MonkeyPatch) -> None:
    fake = pl.DataFrame({"id": [4432], "full_name": ["C.J. Stroud"], "jersey": ["7"], "position_abbreviation": ["QB"]})
    monkeypatch.setattr(cw, "espn_cfb_team_roster", lambda tid, **k: fake)
    out = cw._espn_roster(194)
    assert out == [
        {
            "person_key": "c j stroud",
            "jersey_key": "7",
            "athlete_id": 4432,
            "name": "C.J. Stroud",
            "jersey": "7",
            "position": "QB",
        }
    ]


def test_fox_roster_projection_no_jersey_column(monkeypatch: pytest.MonkeyPatch) -> None:
    # real Fox roster has OFFENSE/POS/CLS/HT/WT -> no jersey number
    fake = pl.DataFrame(
        {
            "athlete_id": ["f9"],
            "player": ["Seuseu Alofaituli"],
            "pos": ["OL"],
            "cls": ["FR"],
            "ht": ["6'2\""],
            "wt": ["290 lbs"],
        }
    )
    monkeypatch.setattr(cw, "fox_cfb_team_roster", lambda tid, **k: fake)
    out = cw._fox_roster(25)
    assert out[0]["athlete_id"] == "f9"
    assert out[0]["person_key"] == "seuseu alofaituli"
    assert out[0]["jersey_key"] == ""  # no jersey column -> empty
    assert out[0]["position"] == "OL"


def test_odds_events_projection(monkeypatch: pytest.MonkeyPatch) -> None:
    fake = pl.DataFrame(
        {
            "id": ["abc"],
            "sport_key": ["americanfootball_ncaaf"],
            "commence_time": ["2024-08-31T23:30:00Z"],
            "home_team": ["Ohio State Buckeyes"],
            "away_team": ["Akron Zips"],
        }
    )
    import sportsdataverse.odds as odds

    monkeypatch.setattr(odds, "toa_sports_events", lambda **k: fake)
    out = cw._odds_events("americanfootball_ncaaf")
    assert out[0]["event_id"] == "abc"
    assert out[0]["date"] == "2024-08-31"
    assert out[0]["matchup_key"] == _matchup_key("ohio state buckeyes", "akron zips")


# ===========================================================================
# Public API end-to-end (monkeypatched -> offline)
# ===========================================================================


def test_cfb_teams_crosswalk_end_to_end(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        cw,
        "espn_cfb_teams",
        lambda **k: pl.DataFrame(
            {"team_id": [194], "team_abbreviation": ["OSU"], "team_display_name": ["Ohio State Buckeyes"]}
        ),
    )
    monkeypatch.setattr(
        cw,
        "fox_cfb_teams",
        lambda **k: pl.DataFrame({"fox_team_id": ["25"], "abbreviation": ["OSU"], "name": ["OHIO STATE BUCKEYES"]}),
    )
    monkeypatch.setattr(
        cw,
        "yahoo_cfb_teams",
        lambda season, week, **k: pl.DataFrame(
            {
                "team_id": ["ncaaf.t.194"],
                "abbreviation": ["OSU"],
                "full_name": ["Ohio State Buckeyes"],
                "display_name": ["Ohio State"],
            }
        ),
    )
    df = cfb_teams_crosswalk(season=2024)
    assert isinstance(df, pl.DataFrame)
    assert df.height == 1
    row = df.row(0, named=True)
    assert row["espn_team_id"] == 194 and row["fox_team_id"] == "25" and row["yahoo_team_id"] == "ncaaf.t.194"
    assert row["matched_sources"] == "espn+fox+yahoo"


def test_cfb_teams_crosswalk_default_season(monkeypatch: pytest.MonkeyPatch) -> None:
    seen: Dict[str, Any] = {}

    def fake_yahoo(season: int, week: int, **k: Any) -> pl.DataFrame:
        seen["season"] = season
        return pl.DataFrame({"team_id": [], "abbreviation": [], "full_name": [], "display_name": []})

    monkeypatch.setattr(
        cw,
        "espn_cfb_teams",
        lambda **k: pl.DataFrame({"team_id": [], "team_abbreviation": [], "team_display_name": []}),
    )
    monkeypatch.setattr(
        cw, "fox_cfb_teams", lambda **k: pl.DataFrame({"fox_team_id": [], "abbreviation": [], "name": []})
    )
    monkeypatch.setattr(cw, "yahoo_cfb_teams", fake_yahoo)
    cfb_teams_crosswalk()  # season=None -> most_recent_cfb_season()
    assert isinstance(seen["season"], int) and seen["season"] >= 2024


def test_cfb_schedule_crosswalk_end_to_end(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        cw,
        "espn_cfb_schedule",
        lambda **k: pl.DataFrame(
            {
                "game_id": [401752687],
                "date": ["2024-08-31T23:30Z"],
                "home_display_name": ["Ohio State Buckeyes"],
                "away_display_name": ["Akron Zips"],
            }
        ),
    )
    raw = {
        "service": {
            "scoreboard": {
                "teams": {
                    "ncaaf.t.194": {"full_name": "Ohio State Buckeyes"},
                    "ncaaf.t.1": {"full_name": "Akron Zips"},
                },
                "games": {
                    "ncaaf.g.1": {
                        "home_team_id": "ncaaf.t.1",
                        "away_team_id": "ncaaf.t.194",
                        "global_gameid": "g",
                        "start_time": "Sat, 31 Aug 2024 23:30:00 +0000",
                    }
                },
            }
        }
    }
    monkeypatch.setattr(cw, "yahoo_cfb_scoreboard", lambda season, week, **k: raw)
    monkeypatch.setattr(
        cw,
        "fox_cfb_schedule",
        lambda **k: pl.DataFrame(
            {
                "game_id": ["41999"],
                "date": ["2024-08-31T20:00:00Z"],
                "home_team": ["Akron Zips"],
                "away_team": ["Ohio State Buckeyes"],
            }
        ),
    )
    df = cfb_schedule_crosswalk(2024, 1)
    assert df.height == 1
    row = df.row(0, named=True)
    assert row["matched_sources"] == "espn+fox+yahoo"
    assert row["espn_game_id"] == 401752687 and row["fox_game_id"] == "41999"


def test_yahoo_season_games_swallows_week_errors(monkeypatch: pytest.MonkeyPatch) -> None:
    def fake_week(season: int, week: int, **k: Any) -> list:
        if week in (3, 5):  # a per-week parser hiccup must not sink the season
            raise RuntimeError("yahoo parser bug")
        if week > 2:
            return []
        return [
            {"matchup_key": f"k{week}", "game_id": f"g{week}", "date": "2024-09-01", "home_team": "A", "away_team": "B"}
        ]

    monkeypatch.setattr(cw, "_yahoo_games", fake_week)
    out = cw._yahoo_season_games(2024)
    assert sorted(r["game_id"] for r in out) == ["g1", "g2"]


def test_espn_season_games_default_slots_when_calendar_fails(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list = []

    def boom_cal(season: int, **k: Any) -> pl.DataFrame:
        raise RuntimeError("no calendar")

    def fake_sched(dates: int, week: int, season_type: int, **k: Any) -> pl.DataFrame:
        calls.append((week, season_type))
        if week == 1 and season_type == 2:
            return pl.DataFrame(
                {
                    "game_id": [1],
                    "date": ["2024-08-31"],
                    "home_display_name": ["Ohio State Buckeyes"],
                    "away_display_name": ["Akron Zips"],
                }
            )
        return pl.DataFrame({"game_id": [], "date": [], "home_display_name": [], "away_display_name": []})

    monkeypatch.setattr(cw, "espn_cfb_calendar", boom_cal)
    monkeypatch.setattr(cw, "espn_cfb_schedule", fake_sched)
    out = cw._espn_season_games(2024)
    # default slots cover regular weeks + bowls (week 1, st 3) + CFP (week 999, st 3)
    assert (16, 2) in calls and (1, 3) in calls and (999, 3) in calls
    assert any(r["game_id"] == 1 for r in out)


def test_cfb_schedule_crosswalk_full_season_end_to_end(monkeypatch: pytest.MonkeyPatch) -> None:
    mk = _matchup_key("ohio state buckeyes", "akron zips")
    monkeypatch.setattr(
        cw,
        "_espn_season_games",
        lambda season, **k: [
            {
                "matchup_key": mk,
                "game_id": 100,
                "date": "2024-08-31",
                "home_team": "Ohio State Buckeyes",
                "away_team": "Akron Zips",
            }
        ],
    )
    monkeypatch.setattr(
        cw,
        "_fox_season_games",
        lambda season, **k: [
            {
                "matchup_key": mk,
                "game_id": "f",
                "date": "2024-08-31",
                "home_team": "Akron Zips",
                "away_team": "Ohio State Buckeyes",
            }
        ],
    )
    monkeypatch.setattr(
        cw,
        "_yahoo_season_games",
        lambda season, **k: [
            {
                "matchup_key": mk,
                "game_id": "y",
                "date": "2024-08-31",
                "home_team": "Akron Zips",
                "away_team": "Ohio State Buckeyes",
            }
        ],
    )
    df = cfb_schedule_crosswalk(2024)  # week omitted -> full season
    assert df.height == 1
    row = df.row(0, named=True)
    assert row["matched_sources"] == "espn+fox+yahoo"
    assert row["espn_game_id"] == 100 and row["fox_game_id"] == "f" and row["yahoo_game_id"] == "y"


def test_cfb_rosters_crosswalk_end_to_end(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        cw,
        "espn_cfb_team_roster",
        lambda tid, **k: pl.DataFrame(
            {"id": [4432], "full_name": ["C.J. Stroud"], "jersey": ["7"], "position_abbreviation": ["QB"]}
        ),
    )
    monkeypatch.setattr(
        cw,
        "fox_cfb_team_roster",
        lambda tid, **k: pl.DataFrame({"athlete_id": ["f7"], "player": ["C.J. Stroud"], "pos": ["QB"], "cls": ["JR"]}),
    )
    df = cfb_rosters_crosswalk(194, 25)
    assert df.height == 1
    row = df.row(0, named=True)
    assert row["espn_athlete_id"] == 4432 and row["fox_athlete_id"] == "f7"
    assert row["match_method"] == "name"  # Fox has no jersey -> name-only


def test_cfb_odds_events_crosswalk_end_to_end(monkeypatch: pytest.MonkeyPatch) -> None:
    import sportsdataverse.odds as odds

    monkeypatch.setattr(
        odds,
        "toa_sports_events",
        lambda **k: pl.DataFrame(
            {
                "id": ["abc"],
                "commence_time": ["2024-08-31T23:30:00Z"],
                "home_team": ["Ohio State Buckeyes"],
                "away_team": ["Akron Zips"],
            }
        ),
    )
    monkeypatch.setattr(
        cw,
        "espn_cfb_schedule",
        lambda **k: pl.DataFrame(
            {
                "game_id": [401752687],
                "date": ["2024-08-31T23:30Z"],
                "home_display_name": ["Ohio State Buckeyes"],
                "away_display_name": ["Akron Zips"],
            }
        ),
    )
    df = cfb_odds_events_crosswalk(season=2024, week=1)
    assert df.height == 1
    row = df.row(0, named=True)
    assert row["odds_event_id"] == "abc" and row["espn_game_id"] == 401752687


# ===========================================================================
# Live smoke tests (gated by SDV_PY_LIVE_TESTS=1)
# ===========================================================================


@skip_if_no_live
def test_live_fox_cfb_teams() -> None:
    df = cw.fox_cfb_teams()
    assert isinstance(df, pl.DataFrame) and df.height > 100
    assert {"fox_team_id", "abbreviation", "name"}.issubset(df.columns)


@skip_if_no_live
def test_live_yahoo_cfb_teams() -> None:
    df = cw.yahoo_cfb_teams(season=2024)
    assert isinstance(df, pl.DataFrame) and df.height > 100
    assert {"team_id", "abbreviation", "full_name"}.issubset(df.columns)


@skip_if_no_live
def test_live_cfb_teams_crosswalk_matches_majority() -> None:
    df = cfb_teams_crosswalk(season=2024)
    assert df.height > 100
    # the big-brand teams should hit all three providers
    full = df.filter(pl.col("matched_sources") == "espn+fox+yahoo")
    assert full.height > 80, f"only {full.height} fully-matched teams"
    # ESPN's directory is near-exhaustive, so (with the alias table) essentially
    # every Yahoo team should resolve to an ESPN id. Allow a tiny margin for
    # future Yahoo additions that predate an alias entry.
    yahoo = df.filter(pl.col("yahoo_team_id").is_not_null())
    matched = yahoo.filter(pl.col("espn_team_id").is_not_null())
    rate = matched.height / yahoo.height
    assert rate >= 0.97, f"Yahoo->ESPN match rate {rate:.3f} ({matched.height}/{yahoo.height})"


@skip_if_no_live
def test_live_fox_cfb_schedule_regular_week() -> None:
    df = cw.fox_cfb_schedule(segment_id="2025-5-1")  # regular-season week 5
    assert isinstance(df, pl.DataFrame) and df.height > 0
    assert {"game_id", "date", "home_team", "away_team", "segment_id"}.issubset(df.columns)
    # the "-1" suffix must yield real September dates, not relabeled postseason
    assert all(d.startswith("2025-09") for d in df["date"].str.slice(0, 7).to_list())


@skip_if_no_live
def test_live_fox_cfb_schedule_full_season() -> None:
    df = cw.fox_cfb_schedule(2025)  # full season: regular + conf champs + bowls + CFP
    assert df.height > 700, f"expected a full season, got {df.height}"
    assert df["game_id"].n_unique() == df.height, "game_ids must be deduped across segments"
    dates = df["date"].str.slice(0, 10)
    assert dates.min() < "2025-09-15"  # opens in (Aug) regular season
    assert dates.max() > "2026-01-01"  # runs through the bowls/CFP


@skip_if_no_live
def test_live_cfb_schedule_crosswalk_week() -> None:
    df = cfb_schedule_crosswalk(2025, 5)
    assert df.height > 0
    assert "fox_game_id" in df.columns
    # With the "-1" regular-season fix, Fox now resolves the week, so the three
    # providers should agree on most games.
    fox_matched = df.filter(pl.col("fox_game_id").is_not_null())
    assert fox_matched.height > 0, "Fox should match regular-season games"


@skip_if_no_live
def test_live_cfb_schedule_crosswalk_full_season() -> None:
    df = cfb_schedule_crosswalk(2025)  # whole season: regular + bowls + CFP
    espn = df.filter(pl.col("espn_game_id").is_not_null())
    assert espn.height > 800, f"expected a full ESPN season, got {espn.height}"
    # date span proves regular season + postseason are both present
    dates = df["espn_date"].drop_nulls().str.slice(0, 10)
    assert dates.min() < "2025-09-15" and dates.max() > "2026-01-01"
    # postseason bowls should match all three providers (ESPN's resolved bracket
    # + Fox bowls segment + Yahoo postseason weeks)
    jan = espn.filter(pl.col("espn_date").str.slice(0, 7) >= "2025-12")
    assert jan.filter(pl.col("matched_sources") == "espn+fox+yahoo").height > 10
