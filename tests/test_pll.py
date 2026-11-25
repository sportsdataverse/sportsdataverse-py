"""Offline tests for the Premier Lacrosse League stats family (``pll_*``): runtime getter and parsers."""

from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import polars as pl
import pytest

from sportsdataverse.errors import AssetFetchError
from sportsdataverse.lacrosse import pll_parsers as P
from sportsdataverse.lacrosse import pll_runtime as rt

FIXTURES = Path(__file__).parent / "fixtures" / "pll"

CASES = [
    ("schedule", "schedule_2026.json", P.SCHEDULE_SCHEMA, 62),
    ("schedule", "glg_schedule_intlm_2026.json", P.SCHEDULE_SCHEMA, 7),
    ("schedule", "wll_schedule_2026.json", P.SCHEDULE_SCHEMA, 10),
    ("game", "game_2026-ev-4.json", P.GAME_DETAIL_SCHEMA, 1),
    ("play_by_play", "play_by_play_2026-ev-4.json", P.PLAY_BY_PLAY_SCHEMA, 200),
    ("play_by_play", "play_by_play_cannons-redwoods-2021-6-04.json", P.PLAY_BY_PLAY_SCHEMA, 300),
    ("player_box", "player_box_2026-ev-4.json", P.PLAYER_BOX_SCHEMA, 36),
    ("team_box", "team_box_2026-ev-4.json", P.TEAM_BOX_SCHEMA, 2),
    ("game_insights", "game_insights_2026-ev-4.json", P.GAME_INSIGHTS_SCHEMA, 5),
    ("standings", "standings_2026.json", P.STANDINGS_SCHEMA, 8),
    ("standings", "glg_standings_intlm_2026.json", P.STANDINGS_SCHEMA, 4),
    ("team_season_stats", "team_season_stats_2026_regular.json", P.TEAM_SEASON_STATS_SCHEMA, 8),
    ("team_stats", "team_stats_waterdogs_2026.json", P.TEAM_STATS_SCHEMA, 1),
    ("team_roster", "team_roster_waterdogs_2026_regular.json", P.TEAM_ROSTER_SCHEMA, 20),
    ("player_season_stats", "player_season_stats_2026_regular.json", P.PLAYER_SEASON_STATS_SCHEMA, 250),
    ("player_advanced_stats", "player_advanced_stats_2026_regular.json", P.PLAYER_ADVANCED_STATS_SCHEMA, 200),
    ("season_leaders", "season_leaders_2026_regular.json", P.SEASON_LEADERS_SCHEMA, 30),
    ("career_leaders", "career_leaders_points_goals_saves.json", P.CAREER_LEADERS_SCHEMA, 15),
    ("player", "player_michael-sowers.json", P.PLAYER_SCHEMA, 1),
    ("player_seasons", "player_seasons_michael-sowers.json", P.PLAYER_SEASONS_SCHEMA, 5),
]


def _load(name: str) -> dict:
    return json.loads((FIXTURES / name).read_text(encoding="utf-8"))


@pytest.mark.parametrize("short, fixture, schema, min_rows", CASES)
def test_parser_rows_and_schema(short, fixture, schema, min_rows):
    parse = getattr(P, f"parse_pll_{short}")
    df = parse(_load(fixture))
    assert df.schema == pl.Schema(schema)
    assert df.height >= min_rows
    assert isinstance(parse(_load(fixture), return_as_pandas=True), pd.DataFrame)


@pytest.mark.parametrize("short, fixture, schema, min_rows", CASES)
@pytest.mark.parametrize("payload", [None, {}, [], "x", {"items": "x"}, {"items": [1, "a"]}, {"_request": {}}])
def test_empty_or_malformed_is_zero_rows_with_schema(short, fixture, schema, min_rows, payload):
    df = getattr(P, f"parse_pll_{short}")(payload)
    assert df.height == 0
    assert df.schema == pl.Schema(schema)


def test_malformed_cells_are_null_not_errors():
    raw = _load("player_box_2026-ev-4.json")
    raw["items"][0] = {**raw["items"][0], "points": "x", "shotPct": float("nan"), "officialId": 3009.0, "jerseyNum": 22}
    df = P.parse_pll_player_box(raw)
    assert df["points"][0] is None and df["shot_pct"][0] is None
    assert df["player_id"][0] == "3009" and df["jersey_number"][0] == "22"


def test_ids_are_strings_and_frames_join():
    box = P.parse_pll_player_box(_load("player_box_2026-ev-4.json"))
    pbp = P.parse_pll_play_by_play(_load("play_by_play_2026-ev-4.json"))
    season = P.parse_pll_player_season_stats(_load("player_season_stats_2026_regular.json"))
    sched = P.parse_pll_schedule(_load("schedule_2026.json"))
    for df, col in ((box, "player_id"), (pbp, "shooter_id"), (season, "player_id"), (sched, "game_id")):
        assert df.schema[col] == pl.String
    assert box["player_id"].str.len_chars().unique().to_list() == [6]  # zero-padded, never 3009
    assert box.schema["player_id"] == season.schema["player_id"]
    shooters = set(pbp["shooter_id"].drop_nulls())
    assert shooters and shooters <= set(box["player_id"])  # every shooter is in the box score
    assert set(box["player_id"]) <= set(season["player_id"])
    assert "2026-ev-4" in set(sched["event_slug"]) and box["event_slug"].unique().to_list() == ["2026-ev-4"]


def test_box_score_adds_up():
    """The team box equals the sum of its players' lines (goals, assists, shots), and points are 1 + 2x2 + assists."""
    box = P.parse_pll_player_box(_load("player_box_2026-ev-4.json"))
    team = P.parse_pll_team_box(_load("team_box_2026-ev-4.json"))
    sums = box.group_by("team_id").agg(pl.col("goals", "assists", "shots").sum()).sort("team_id")
    assert sums.to_dicts() == team.select("team_id", "goals", "assists", "shots").sort("team_id").to_dicts()
    assert (box["points"] == box["one_point_goals"] + 2 * box["two_point_goals"] + box["assists"]).all()


def test_schedule_scores_and_start_time():
    sched = P.parse_pll_schedule(_load("schedule_2026.json"))
    game = sched.filter(pl.col("event_slug") == "2026-ev-4").row(0, named=True)
    detail = P.parse_pll_game(_load("game_2026-ev-4.json")).row(0, named=True)
    assert (game["home_team_id"], game["away_team_id"]) == (detail["home_team_id"], detail["away_team_id"])
    assert (game["home_score"], game["away_score"]) == (detail["home_score"], detail["away_score"])
    assert sum(detail["home_period_scores"]) == detail["home_score"]
    assert sum(detail["away_period_scores"]) == detail["away_score"]
    assert sched.schema["start_time"] == pl.Datetime("ms", "UTC") and sched["start_time"].null_count() == 0
    assert set(sched["season_segment"]) >= {"regular", "post", "champseries"}


def test_play_by_play_order_and_shot_details():
    pbp = P.parse_pll_play_by_play(_load("play_by_play_2026-ev-4.json"))
    assert pbp["play_index"].to_list() == list(range(pbp.height))
    assert pbp["seconds_passed"].is_sorted()
    shots = pbp.filter(pl.col("event_type").is_in(["shot", "goal"]))
    assert shots["shot_on_goal"].null_count() < shots.height  # the 2026 log fills its shot details
    old = P.parse_pll_play_by_play(_load("play_by_play_cannons-redwoods-2021-6-04.json"))
    assert old["shot_on_goal"].null_count() == old.height  # 2021: details are empty objects


def test_leaders_are_long_and_ranked():
    lead = P.parse_pll_season_leaders(_load("season_leaders_2026_regular.json"))
    first = lead.filter(pl.col("rank") == 1)
    assert first.height == lead["stat"].n_unique() and lead["rank"].min() == 1  # 1-based
    assert "save_pct" in set(lead["stat"])  # snake-cased stat names
    career = P.parse_pll_career_leaders(_load("career_leaders_points_goals_saves.json"))
    assert set(career["stat"]) == {"points", "goals", "saves"}
    pts = career.filter(pl.col("stat") == "points").sort("rank")["stat_value"].to_list()
    assert pts == sorted(pts, reverse=True)


def test_request_stamp_reaches_the_frames():
    roster = P.parse_pll_team_roster(_load("team_roster_waterdogs_2026_regular.json"))
    assert roster["team_slug"].unique().to_list() == ["waterdogs"] and roster["season"].unique().to_list() == [2026]
    stand = P.parse_pll_standings(_load("standings_2026.json"))
    assert stand["league"].unique().to_list() == ["PLL"] and stand["champ_series"].unique().to_list() == [False]
    glg = P.parse_pll_standings(_load("glg_standings_intlm_2026.json"))
    assert glg["league"].unique().to_list() == ["INTLM"]
    seasons = P.parse_pll_player_seasons(_load("player_seasons_michael-sowers.json"))
    assert seasons["player_slug"].unique().to_list() == ["michael-sowers"]


@pytest.mark.parametrize(
    "url, params, want",
    [
        ("https://h/api/v4/events/2026-ev-4/playlogs", {}, {"event_slug": "2026-ev-4"}),
        ("https://h/api/v4/games/2026-ev-4", {}, {"event_slug": "2026-ev-4"}),
        ("https://h/api/v4/events", {"year": 2026}, {"year": 2026}),
        ("https://h/api/v4/teams/season-stats", {"year": 2026}, {"year": 2026}),
        (
            "https://h/api/v4/teams/waterdogs/roster",
            {"seasonSegment": "post"},
            {"team_slug": "waterdogs", "seasonSegment": "post"},
        ),
        ("https://h/api/v4/players/stats/season-leaders", {}, {}),
        ("https://h/api/v4/players/season-stats", {}, {}),
        ("https://h/api/v4/players/michael-sowers/stats/seasons", {}, {"player_slug": "michael-sowers"}),
        ("https://h/api/glg/intlw/standings", {}, {"league": "intlw"}),
    ],
)
def test_stamp(url, params, want):
    assert rt._stamp(url, params) == want


def test_wire_lowercases_bools_and_keeps_lists():
    assert rt._wire({"champSeries": True, "includeCS": False, "stats": ["points", "goals"], "year": None}) == {
        "champSeries": "true",
        "includeCS": "false",
        "stats": ["points", "goals"],
    }


def test_get_sends_the_referer_and_returns_stamped_data(monkeypatch):
    seen = {}

    def fake(url, params=None, **kw):
        seen.update(url=url, params=params, headers=kw.get("headers"))
        return {"data": {"items": [{"id": 1}]}}

    monkeypatch.setattr(rt._rt, "_get", fake)
    monkeypatch.setattr(rt, "MIN_INTERVAL", 0)
    out = rt._get(f"{rt.HOST}/api/v4/standings", params={"year": 2026, "champSeries": True})
    assert seen["headers"]["Referer"] == "https://stats.premierlacrosseleague.com/"
    assert "Authorization" not in seen["headers"] and seen["params"] == {"year": 2026, "champSeries": "true"}
    assert out == {"items": [{"id": 1}], "_request": {"year": 2026, "champSeries": "true"}}


@pytest.mark.parametrize("body", [{"error": "Origin not allowed"}, {"data": []}, {"data": None}, [], {"items": []}])
def test_get_refuses_a_body_that_is_not_the_envelope(monkeypatch, body):
    monkeypatch.setattr(rt._rt, "_get", lambda *a, **k: body)
    monkeypatch.setattr(rt, "MIN_INTERVAL", 0)
    with pytest.raises(AssetFetchError):
        rt._get(f"{rt.HOST}/api/v4/standings", params={"year": 2026})


def test_get_names_the_origin_gate_on_403(monkeypatch):
    def refused(*a, **k):
        raise AssetFetchError(
            'stats.premierlacrosseleague.com/api/v4/standings answered HTTP 403: {"error":"Origin not allowed"}'
        )

    monkeypatch.setattr(rt._rt, "_get", refused)
    monkeypatch.setattr(rt, "MIN_INTERVAL", 0)
    with pytest.raises(AssetFetchError, match="Referer"):
        rt._get(f"{rt.HOST}/api/v4/standings", params={"year": 2026})


def test_throttle_spaces_requests(monkeypatch):
    slept = []
    clock = iter([100.0, 100.0, 100.1, 100.1])
    monkeypatch.setattr(rt.time, "monotonic", lambda: next(clock))
    monkeypatch.setattr(rt.time, "sleep", slept.append)
    monkeypatch.setattr(rt, "_last", [0.0])
    rt._throttle()
    rt._throttle()
    assert slept == [pytest.approx(rt.MIN_INTERVAL - 0.1)]


def test_wrapper_builds_the_route_and_validates_choices(monkeypatch):
    from sportsdataverse.lacrosse import pll

    calls = []
    monkeypatch.setattr(pll, "_get", lambda url, params=None, **kw: calls.append((url, params)) or {"items": []})
    assert pll.pll_play_by_play("2026-ev-4").height == 0
    pll.pll_season_leaders(year=2026, stats=["points", "assists"])
    assert calls[0][0] == "https://stats.premierlacrosseleague.com/api/v4/events/2026-ev-4/playlogs"
    assert calls[1][1]["stats"] == ["points", "assists"] and calls[1][1]["seasonSegment"] == "regular"
    with pytest.raises(ValueError):
        pll.pll_player_season_stats(year=2026, season_segment="allstar")
    with pytest.raises(ValueError):
        pll.pll_glg_schedule(glg_league="intlx")


def test_get_answers_a_204_with_an_empty_dict(monkeypatch):
    class Resp:
        status_code, headers, text, content = 204, {}, "", b""

    monkeypatch.setattr(rt._rt, "download", lambda **kw: Resp())
    monkeypatch.setattr(rt, "MIN_INTERVAL", 0)
    assert rt._get(f"{rt.HOST}/api/v4/standings", params={"year": 2026}) == {}


@pytest.mark.parametrize("slug", ["a/b", "x?year=1", "x#y", "x y"])
def test_get_refuses_a_slug_that_would_change_the_route(monkeypatch, slug):
    monkeypatch.setattr(rt._rt, "_get", lambda *a, **k: pytest.fail("sent"))
    with pytest.raises(ValueError, match="not a PLL stats route"):
        rt._get(f"{rt.HOST}/api/v4/events/{slug}/playlogs")


def test_get_does_not_retry_the_origin_refusal(monkeypatch):
    seen = {}
    monkeypatch.setattr(rt._rt, "_get", lambda *a, **k: seen.update(k) or {"data": {}})
    monkeypatch.setattr(rt, "MIN_INTERVAL", 0)
    rt._get(f"{rt.HOST}/api/v4/standings", params={"year": 2026})
    assert 403 not in seen["retry_statuses"] and 429 in seen["retry_statuses"]


def test_out_of_range_numbers_are_null_not_errors():
    df = P.parse_pll_schedule({"items": [{"slugname": "x", "startTime": 10**20, "week": 2**70, "id": 1}]})
    assert df.height == 1 and df["start_time"][0] is None and df["week"][0] is None
