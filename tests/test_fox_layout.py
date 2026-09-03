"""Offline tests for the shared Fox Bifrost layout layer (``_fox_layout``).

Every assertion runs against a committed **real capture** (see
``tests/fixtures/fox/README.md``) -- no network. Two jobs:

1. **Regression**: the parsers the eight shipped league modules already depend
   on (``parse_roster`` / ``parse_team_stats`` / ``parse_team_gamelog`` /
   ``parse_odds`` / ``parse_boxscore`` / ``parse_period_pbp`` /
   ``parse_standings`` / ``parse_league_leaders`` / ``parse_teams``) still emit
   the same rows and the same key sets after the module was extended.
2. **Coverage**: the parsers + per-league builder added for the rest of the
   documented Bifrost surface.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List

import polars as pl
import pytest

import sportsdataverse._fox_layout as fox_layout
from sportsdataverse._fox_layout import (
    fox_get,
    fox_get_feed,
    frame,
    parse_boxscore,
    parse_drive_pbp,
    parse_header,
    parse_league_leaders,
    parse_matchup,
    parse_nav_items,
    parse_odds,
    parse_odds_board,
    parse_period_pbp,
    parse_player_news,
    parse_roster,
    parse_search_results,
    parse_segment_events,
    parse_selection_nav,
    parse_standings,
    parse_stat_leaders,
    parse_team_gamelog,
    parse_team_stats,
    parse_teams,
    parse_top_performers,
    parse_trending,
    register_league_endpoints,
)

FIX = Path(__file__).parent / "fixtures" / "fox"
CFB_FIX = Path(__file__).parent / "cfb" / "fixtures"

# Every parser in the module, in (callable, extra positional args) form -- used
# by the "empty payload never raises" sweep.
ALL_PARSERS = [
    (parse_roster, ("1",)),
    (parse_team_stats, ("1",)),
    (parse_team_gamelog, ("1",)),
    (parse_standings, ()),
    (parse_teams, ()),
    (parse_league_leaders, ()),
    (parse_odds, ("1",)),
    (parse_boxscore, ("1",)),
    (parse_period_pbp, ("1",)),
    (parse_drive_pbp, ("1",)),
    (parse_selection_nav, ()),
    (parse_segment_events, ()),
    (parse_nav_items, ()),
    (parse_header, ()),
    (parse_player_news, ()),
    (parse_stat_leaders, ()),
    (parse_odds_board, ()),
    (parse_matchup, ("1",)),
    (parse_top_performers, ("1",)),
    (parse_search_results, ()),
    (parse_trending, ()),
]


def _load(name: str, base: Path = FIX) -> Dict[str, Any]:
    return json.loads((base / name).read_text(encoding="utf-8"))


def _ids_are_clean(rows: List[Dict[str, Any]], key: str) -> None:
    """Ids stay ``str`` end to end -- never a float that stringifies as ``123.0``."""
    for r in rows:
        val = r.get(key)
        if val is None:
            continue
        assert isinstance(val, str), f"{key} must be str, got {type(val)}"
        assert not val.endswith(".0"), f"{key}={val!r} looks float-origin"


# --------------------------------------------------------------------------
# 1. Regression: the parsers the shipped fox_* surface already depends on
# --------------------------------------------------------------------------
def test_parse_roster_unchanged():
    rows = parse_roster(_load("cbk_team_roster.json"), 27)
    assert len(rows) > 0
    assert all(r["team_id"] == "27" for r in rows)
    assert all(r.get("athlete_id") for r in rows), "athletes-only filter still applies"
    assert {"team_id", "position_group", "player", "athlete_id"}.issubset(rows[0])
    _ids_are_clean(rows, "athlete_id")


def test_parse_team_stats_unchanged():
    rows = parse_team_stats(_load("wnba_team_stats.json"), 3)
    assert len(rows) > 0
    assert set(rows[0]) == {"team_id", "category", "stat", "stat_abbreviation", "player", "value"}
    assert all(r["team_id"] == "3" for r in rows)


def test_parse_team_gamelog_unchanged():
    rows = parse_team_gamelog(_load("cbk_team_gamelog.json"), 27)
    assert len(rows) > 0
    assert set(rows[0]) == {
        "team_id",
        "season_type",
        "category",
        "game_id",
        "game_date",
        "opponent",
        "stat",
        "value",
    }
    _ids_are_clean(rows, "game_id")


def test_parse_odds_unchanged():
    rows = parse_odds(_load("ufl_event_odds.json"), 123)
    assert len(rows) == 2, "the six-pack carries exactly one row per team"
    assert all(r["game_id"] == "123" for r in rows)
    assert all(r["team"] for r in rows)


def test_parse_league_leaders_unchanged():
    rows = parse_league_leaders(_load("ufl_league_stats_con_team.json"))
    assert len(rows) > 0
    assert "entity_id" in rows[0]
    _ids_are_clean(rows, "entity_id")


def test_parse_standings_unchanged():
    rows = parse_standings(_load("wcbk_team_11_standings.json"), 11)
    assert len(rows) > 0
    assert all(r["team_id"] == "11" for r in rows)
    assert all("section" in r for r in rows)
    # the team_id-less call path (league/polls, league/standings, event/standings)
    plain = parse_standings(_load("wcbk_team_11_standings.json"))
    assert len(plain) == len(rows)
    assert "team_id" not in plain[0]


def test_parse_teams_unchanged():
    rows = parse_teams(_load("nba_team_1_standings.json"))
    assert len(rows) == 30
    assert all(set(r) == {"fox_team_id", "fox_team_name", "fox_section"} for r in rows)
    _ids_are_clean(rows, "fox_team_id")


def test_parse_boxscore_unchanged():
    raw = _load("fox_cfb_event_41616_data.json", CFB_FIX)
    rows = parse_boxscore(raw, 41616)
    assert len(rows) > 0
    assert set(rows[0]) == {"game_id", "team", "stat_group", "player", "athlete_id", "stat", "value"}
    assert {"KENT STATE", "FLORIDA STATE"} == {r["team"] for r in rows}
    _ids_are_clean(rows, "athlete_id")


def test_parse_period_pbp_unchanged():
    rows = parse_period_pbp(_load("cbk_event_data_pbp_first_half.json"), 262052)
    assert len(rows) == 158, "1ST HALF of the captured CBK game"
    assert {r["period"] for r in rows} == {"1ST HALF"}
    assert set(rows[0]) == {
        "game_id",
        "period",
        "left_team",
        "right_team",
        "play_id",
        "clock",
        "team",
        "left_score_change",
        "right_score_change",
        "play_text",
    }
    assert all(r["game_id"] == "262052" for r in rows)


def test_fox_get_builds_the_same_request(monkeypatch):
    seen: Dict[str, Any] = {}

    def fake_get(url, params=None, headers=None, **kwargs):
        seen.update(url=url, params=params, headers=headers)
        return {}

    monkeypatch.setattr(fox_layout, "_get", fake_get)
    fox_get("cbk/team/11/roster")
    assert seen["url"] == "https://api.foxsports.com/bifrost/v1/cbk/team/11/roster"
    assert seen["params"] == {"apikey": fox_layout.DATA_KEY, "api-version": "1.1"}
    assert seen["headers"]["Origin"] == "https://www.foxsports.com"


# --------------------------------------------------------------------------
# 2. New parsers, asserted against real captures
# --------------------------------------------------------------------------
def test_parse_drive_pbp():
    rows = parse_drive_pbp(_load("fox_cfb_event_41616_data.json", CFB_FIX), 41616)
    assert len(rows) > 0
    assert set(rows[0]) == {
        "game_id",
        "quarter",
        "drive_id",
        "drive_result",
        "drive_summary",
        "drive_team",
        "play_id",
        "period",
        "clock",
        "field_position",
        "play_text",
        "play_team",
    }
    assert all(r["game_id"] == "41616" for r in rows)
    assert len({r["drive_id"] for r in rows}) > 1, "plays span multiple drives"


def test_parse_drive_pbp_matches_the_shipped_cfb_wrapper(monkeypatch):
    """The shared parser reproduces cfb_fox_ext's inline drive flattener byte for byte."""
    import sportsdataverse.cfb.cfb_fox_ext as cfb_ext

    raw = _load("fox_cfb_event_41616_data.json", CFB_FIX)
    monkeypatch.setattr(cfb_ext, "_fox_get", lambda *a, **k: raw)
    shipped = cfb_ext.fox_cfb_pbp("41616")
    assert shipped.height > 0
    assert shipped.equals(pl.DataFrame(parse_drive_pbp(raw, "41616")))


def test_parse_selection_nav():
    rows = parse_selection_nav(_load("cbk_league_scores.json"))
    assert len(rows) > 0
    assert set(rows[0]) == {
        "selection_list",
        "id",
        "title",
        "date",
        "uri",
        "web_url",
        "selected",
        "group_id",
    }
    assert {"groupList", "dailyList"} & {r["selection_list"] for r in rows}


def test_parse_segment_events_scores_segment():
    rows = parse_segment_events(_load("soccer_league_scores_segment.json"), "c1d20260519")
    assert len(rows) == 2
    assert all(r["segment_id"] == "c1d20260519" for r in rows)
    r = rows[0]
    assert r["game_id"] == "627047"
    # the capture's layout tokens make the UPPER team the home side here --
    # the tokens win over the away-on-top convention, which is the point.
    assert r["home_team"] == "Bournemouth" and r["away_team"] == "Man. City"
    assert r["status"] == "FINAL"
    _ids_are_clean(rows, "game_id")
    _ids_are_clean(rows, "home_team_id")
    _ids_are_clean(rows, "away_team_id")


def test_parse_segment_events_topevents():
    rows = parse_segment_events(_load("topevents_scoreboard_segment_1.json"), 1)
    assert len(rows) > 0
    assert all(r["segment_id"] == "1" for r in rows)
    assert any(r["league"] for r in rows), "the cross-sport board stamps a league per row"
    _ids_are_clean(rows, "game_id")


def test_parse_nav_items_teamnav():
    rows = parse_nav_items(_load("cbk_league_teamnav.json"))
    assert len(rows) > 0
    assert set(rows[0]) == {
        "group",
        "fox_id",
        "abbreviation",
        "name",
        "content_uri",
        "content_type",
        "web_url",
        "color",
        "logo_url",
    }
    assert all(r["group"] is None for r in rows), "teamnav is a flat navItems list"
    assert all(r["fox_id"] for r in rows)
    _ids_are_clean(rows, "fox_id")


def test_parse_nav_items_conferences():
    rows = parse_nav_items(_load("cbk_league_conferences.json"))
    assert len(rows) > 0
    assert all(r["content_uri"] and "/groups/" in r["content_uri"] for r in rows)
    assert "Atlantic Coast" in {r["abbreviation"] for r in rows}
    _ids_are_clean(rows, "fox_id")


def test_parse_nav_items_explore_browse():
    rows = parse_nav_items(_load("explore_browse_sports_main.json"))
    assert len(rows) > 0
    assert {"SPORTS"} == {r["group"] for r in rows}
    assert "NFL" in {r["abbreviation"] for r in rows}
    # league rows (NFL, MLB, ...) link by `uri` only -> fox_id is null, not a
    # crash; the entity-linked rows (UFC, WWE, ...) still resolve an id.
    assert any(r["fox_id"] is None for r in rows)
    assert {"1", "4"} & {r["fox_id"] for r in rows if r["fox_id"]}
    _ids_are_clean(rows, "fox_id")


def test_parse_header_league_and_team():
    league = parse_header(_load("nfl_league_header.json"))
    assert len(league) == 1
    team = parse_header(_load("nfl_team_header.json"))
    assert len(team) == 1
    row = team[0]
    assert row["title"] == "DENVER BRONCOS"
    assert row["entity_id"] == "10"
    assert row["content_uri"] == "football/nfl/teams/10"
    assert "AFC WEST" in row["details"]
    _ids_are_clean(team, "entity_id")


def test_parse_player_news():
    rows = parse_player_news(_load("nfl_league_playernews.json"))
    assert len(rows) > 0
    assert set(rows[0]) == {
        "title",
        "subtitle",
        "headline",
        "description",
        "impact_title",
        "impact",
        "date",
        "source",
        "athlete_id",
        "content_uri",
        "web_url",
    }
    assert any(r["headline"] for r in rows)
    _ids_are_clean(rows, "athlete_id")


def test_parse_stat_leaders():
    rows = parse_stat_leaders(_load("nfl_league_stats.json"))
    assert len(rows) > 0
    assert set(rows[0]) == {"category", "stat", "stat_abbreviation", "player", "value"}
    assert len({r["category"] for r in rows}) > 1


def test_parse_odds_board():
    rows = parse_odds_board(_load("nba_league_odds.json"))
    assert len(rows) > 0
    assert len(rows) % 2 == 0, "the board emits one row per team, two per game"
    assert all(r["team"] for r in rows)
    assert any(r["game_id"] for r in rows)
    _ids_are_clean(rows, "game_id")


def test_parse_matchup():
    rows = parse_matchup(_load("nba_event_matchup.json"), 106422)
    assert len(rows) == 5
    assert set(rows[0]) == {
        "game_id",
        "stat",
        "left_team",
        "left_team_id",
        "left_value",
        "left_emphasized",
        "right_team",
        "right_team_id",
        "right_value",
        "right_emphasized",
    }
    assert rows[0]["stat"] == "PPG"
    assert rows[0]["left_team"] == "UTA" and rows[0]["right_team"] == "HOU"
    assert rows[0]["left_team_id"] == "20"
    _ids_are_clean(rows, "left_team_id")
    _ids_are_clean(rows, "right_team_id")


def test_parse_top_performers():
    rows = parse_top_performers(_load("nba_event_recap.json"), 106422)
    assert len(rows) > 0
    assert set(rows[0]) == {
        "game_id",
        "player",
        "team_position",
        "stat_line",
        "athlete_id",
        "content_uri",
        "web_url",
    }
    assert rows[0]["player"] == "Cody Williams"
    assert rows[0]["athlete_id"] == "3995"
    _ids_are_clean(rows, "athlete_id")


def test_parse_search_results():
    rows = parse_search_results(_load("search_entities.json"))
    assert len(rows) > 0
    assert set(rows[0]) == {
        "group",
        "type",
        "entity_id",
        "title",
        "subtitle",
        "content_type",
        "content_uri",
        "web_url",
        "analytics_name",
        "image_url",
    }
    assert "Kansas City Chiefs" in {r["title"] for r in rows}
    _ids_are_clean(rows, "entity_id")


def test_parse_trending():
    rows = parse_trending(_load("trending_videos.json"))
    assert len(rows) == 3
    assert set(rows[0]) == {
        "id",
        "spark_id",
        "title",
        "description",
        "content_type",
        "component_type",
        "publication_date",
        "last_published_date",
        "canonical_url",
        "thumbnail_url",
        "playback_url",
    }
    assert all(r["title"] and r["canonical_url"] for r in rows)
    assert any(r["playback_url"] for r in rows)


def test_parse_standings_on_event_standings_capture():
    """event/{id}/standings reuses the shipped parse_standings unchanged."""
    rows = parse_standings(_load("cfb_event_standings.json"))
    assert len(rows) > 0
    assert all("section" in r for r in rows)
    _ids_are_clean(rows, "entity_id")


# --------------------------------------------------------------------------
# 3. Empty / malformed payloads never raise
# --------------------------------------------------------------------------
@pytest.mark.parametrize("payload", [{}, {"sectionList": None}, {"groups": None}, {"data": None}])
def test_every_parser_tolerates_a_junk_payload(payload):
    for parser, extra in ALL_PARSERS:
        rows = parser(payload, *extra)
        assert rows == [], f"{parser.__name__} should return [] for {payload!r}"
        assert frame(rows, False).height == 0


# --------------------------------------------------------------------------
# 4. The per-league builder
# --------------------------------------------------------------------------
def test_register_league_endpoints_registers_every_short_name():
    ns: Dict[str, Any] = {"__name__": "sportsdataverse.nba.nba_fox_ext"}
    names = register_league_endpoints("nba", "nba", ns)
    assert len(names) == len(fox_layout._LEAGUE_ENDPOINTS) == 17
    assert names[0] == "fox_nba_scoreboard"
    for n in names:
        fn = ns[n]
        assert fn.__name__ == n
        assert "Args:" in fn.__doc__ and "Returns:" in fn.__doc__ and "Example:" in fn.__doc__
        assert ">>>" not in fn.__doc__


@pytest.mark.parametrize(
    ("prefix", "sport"),
    [("cfb", "cfb"), ("mbb", "cbk"), ("wbb", "wcbk"), ("nba", "nba"), ("wnba", "wnba"), ("nhl", "nhl"), ("mlb", "mlb")],
)
def test_every_shipped_league_module_gained_the_endpoints(prefix, sport):
    mod = __import__(f"sportsdataverse.{prefix}", fromlist=["*"])
    for short, *_ in fox_layout._LEAGUE_ENDPOINTS:
        assert hasattr(mod, f"fox_{prefix}_{short}"), f"fox_{prefix}_{short} missing"


def test_nfl_fox_ext_module_exists_and_is_complete():
    from sportsdataverse.nfl import nfl_fox_ext

    for short, *_ in fox_layout._LEAGUE_ENDPOINTS:
        assert f"fox_nfl_{short}" in nfl_fox_ext.__all__
    for base in ("pbp", "boxscore", "odds", "team_roster", "team_stats", "team_gamelog", "standings", "league_leaders"):
        assert f"fox_nfl_{base}" in nfl_fox_ext.__all__


def test_builder_wrappers_build_the_right_url(monkeypatch):
    from sportsdataverse.nba import fox_nba_league_odds, fox_nba_team_header

    seen: Dict[str, Any] = {}

    def fake_get(url, params=None, headers=None, **kwargs):
        seen.update(url=url, params=params)
        return _load("nfl_team_header.json")

    monkeypatch.setattr(fox_layout, "_get", fake_get)

    df = fox_nba_team_header(13)
    assert seen["url"].endswith("/nba/team/13/header")
    assert isinstance(df, pl.DataFrame) and df.height == 1

    fox_nba_team_header(team_id=13)
    assert seen["url"].endswith("/nba/team/13/header")

    fox_nba_league_odds(group_id="top25")
    assert seen["url"].endswith("/nba/league/odds")
    assert seen["params"]["groupId"] == "top25"


def test_builder_wrapper_return_flags(monkeypatch):
    from sportsdataverse.nhl import fox_nhl_league_stat_leaders

    monkeypatch.setattr(fox_layout, "_get", lambda *a, **k: _load("nfl_league_stats.json"))
    assert isinstance(fox_nhl_league_stat_leaders(), pl.DataFrame)
    assert isinstance(fox_nhl_league_stat_leaders(return_parsed=False), dict)
    pdf = fox_nhl_league_stat_leaders(return_as_pandas=True)
    assert type(pdf).__module__.startswith("pandas")


def test_builder_scorechip_is_raw_only(monkeypatch):
    from sportsdataverse.mlb import fox_mlb_scorechip

    monkeypatch.setattr(fox_layout, "_get", lambda *a, **k: {"chip": 1})
    assert fox_mlb_scorechip("mlb95682") == {"chip": 1}


def test_builder_missing_required_arg_raises():
    from sportsdataverse.mbb import fox_mbb_event_matchup

    with pytest.raises(TypeError, match="missing required argument: 'game_id'"):
        fox_mbb_event_matchup()


# --------------------------------------------------------------------------
# 5. Cross-sport wrappers
# --------------------------------------------------------------------------
def test_cross_sport_wrappers_hit_the_documented_paths(monkeypatch):
    from sportsdataverse._fox_layout import (
        fox_explore_browse,
        fox_explore_odds,
        fox_polls,
        fox_search_entities,
        fox_topevents_segment,
        fox_trending_videos,
    )

    calls: List[Dict[str, Any]] = []

    def fake_get(url, params=None, headers=None, **kwargs):
        calls.append({"url": url, "params": params})
        return {}

    monkeypatch.setattr(fox_layout, "_get", fake_get)

    fox_search_entities("chiefs")
    fox_explore_browse("sports")
    fox_explore_odds()
    fox_topevents_segment(1)
    fox_trending_videos()
    fox_polls("football/nfl/teams/11")

    urls = [c["url"] for c in calls]
    assert urls == [
        "https://api.foxsports.com/bifrost/v1/search/entities",
        "https://api.foxsports.com/bifrost/v1/explore/browse/sports/main",
        "https://api.foxsports.com/bifrost/v1/explore/odds/main",
        "https://api.foxsports.com/bifrost/v1/topevents/scoreboard/segment/1",
        "https://api.foxsports.com/bifrost/v1/general/trending/videos",
        "https://api.foxsports.com/foxpolls/v1/polls",
    ]
    assert calls[0]["params"]["text"] == "chiefs"
    # the trending + polls tiers use the feed key, everything else the data key
    assert calls[4]["params"]["apikey"] == fox_layout.FEED_KEY
    assert calls[5]["params"]["apikey"] == fox_layout.FEED_KEY
    assert calls[0]["params"]["apikey"] == fox_layout.DATA_KEY


def test_fox_get_feed_targets_the_host_root(monkeypatch):
    seen: Dict[str, Any] = {}
    monkeypatch.setattr(fox_layout, "_get", lambda url, **k: seen.update(url=url, **k) or {})
    fox_get_feed("foxpolls/v1/polls")
    assert seen["url"] == "https://api.foxsports.com/foxpolls/v1/polls"
