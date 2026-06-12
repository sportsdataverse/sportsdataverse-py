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
    # ESPN lists OSU as home; Yahoo lists Akron as home. Each side derives its
    # own key from its own (oppositely-ordered) names -> the keys must still be
    # equal for the merge to join them, which is the property under test.
    espn_mk = _matchup_key(_norm_team("Ohio State Buckeyes"), _norm_team("Akron Zips"))
    yahoo_mk = _matchup_key(_norm_team("Akron Zips"), _norm_team("Ohio State Buckeyes"))
    assert espn_mk == yahoo_mk, "order-independent key precondition"
    espn = [_game(espn_mk, 401752687, "Ohio State Buckeyes", "Akron Zips", "2024-08-31")]
    yahoo = [
        _game(
            yahoo_mk,
            "ncaaf.g.202408310194",
            "Akron Zips",
            "Ohio State Buckeyes",
            "2024-08-31",
            global_game_id="ncaaf.g.123",
        )
    ]
    rows = _merge_schedule(espn, yahoo)
    assert len(rows) == 1
    assert rows[0]["matched_sources"] == "espn+yahoo"
    assert rows[0]["espn_game_id"] == 401752687
    assert rows[0]["yahoo_game_id"] == "ncaaf.g.202408310194"
    assert rows[0]["yahoo_global_game_id"] == "ncaaf.g.123"


def test_merge_schedule_unmatched_both_sides() -> None:
    espn = [_game("a|b", 1, "A", "B", "2024-01-01")]
    yahoo = [_game("c|d", "y", "C", "D", "2024-01-01")]
    rows = _merge_schedule(espn, yahoo)
    methods = sorted(r["matched_sources"] for r in rows)
    assert methods == ["espn", "yahoo"]


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
    df = cfb_schedule_crosswalk(2024, 1)
    assert df.height == 1
    assert df.row(0, named=True)["matched_sources"] == "espn+yahoo"


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
def test_live_cfb_schedule_crosswalk_week() -> None:
    df = cfb_schedule_crosswalk(2024, 5)
    assert df.height > 0
    both = df.filter(pl.col("matched_sources") == "espn+yahoo")
    assert both.height > 0
