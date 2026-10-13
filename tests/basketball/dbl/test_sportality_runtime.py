"""The Basketligaen runtime maps END-year seasons to ssgts and refuses every answer the refs capture refuses.

Transport is mocked at ``sportality_runtime.download``; bodies are the committed real captures
(``tests/fixtures/sportality/``), cut or altered only where a refusal case needs it. No network.
"""

from __future__ import annotations

import copy
import json
from pathlib import Path
from typing import Any, Dict, List, Tuple
from urllib.parse import urlsplit

import polars as pl
import pytest

from sportsdataverse.basketball.dbl import sportality_runtime as rt
from sportsdataverse.errors import AssetFetchError, NoDataError

FIXTURES = Path(__file__).parents[2] / "fixtures" / "sportality"
FULL = FIXTURES / "full"
LAYOUT = json.loads((FULL / "statistics-v2__layout-info__directory.json").read_text(encoding="utf-8"))
B = rt.BASE
GAME = "c4hh5nig6t"
SSGT_2026 = "qzw34da6jg"  # 2025/26 regular season = END year 2026


def _load(name: str) -> Any:
    return json.loads((FIXTURES / name).read_text(encoding="utf-8"))


def _full(name: str) -> Any:
    return json.loads((FULL / name).read_text(encoding="utf-8"))


def _pbp() -> List[dict]:
    return _full("gameday__play-by-play__c4hh5nig6t.json")


def _box() -> dict:
    return _full("gameday__boxscore__c4hh5nig6t.json")


class _Resp:
    def __init__(self, body: Any = None, status: int = 200, text: str = None) -> None:
        self.status_code = status
        self.text = json.dumps(body) if text is None else text

    def json(self) -> Any:
        return json.loads(self.text)


@pytest.fixture
def served(monkeypatch: pytest.MonkeyPatch):
    """Route URLs to canned responses; record every request and each one sent over the polite session."""
    routes: Dict[str, _Resp] = {"/statistics-v2/layout-info": _Resp(LAYOUT)}
    calls: List[Tuple[str, Dict[str, Any]]] = []
    waits: List[str] = []

    def fake_download(url: str, params: Any = None, **kw: Any) -> _Resp:
        calls.append((url, dict(params or {})))
        if kw.get("session") is rt.SESSION:  # the session waits for _polite_wait on every attempt
            waits.append(url)
        path = urlsplit(url).path.split("/api", 1)[1]
        if path not in routes:
            raise AssertionError(f"unexpected request {path}")
        return routes[path]

    monkeypatch.setattr(rt, "download", fake_download)
    rt._directory.cache_clear()
    yield routes, calls, waits
    rt._directory.cache_clear()


# --------------------------------------------------------------------------- season -> ssgt


def test_stats_info_maps_the_end_year_and_always_sends_count_1000(served):
    routes, calls, waits = served
    routes["/statistics-v2/stats-info/players_summary"] = _Resp(
        _full("statistics-v2__stats-info__players_summary__qzw34da6jg.json")
    )
    rt._get(f"{B}/statistics-v2/stats-info/players_summary", {"season": 2026, "gameType": "regular", "count": 50})
    assert calls[-1][1] == {"ssgtUuid": SSGT_2026, "count": 1000}
    assert len(waits) == len(calls) == 2  # layout-info + the table, each over the polite session


def test_playoffs_map_to_the_finals_ssgt(served):
    assert rt.sportality_ssgt_uuid(2026, "finals") == "al4a0u2go5"
    assert rt.sportality_ssgt_uuid(2003) == "rejluryl18"  # 2002/03, the earliest season on the site
    assert rt.sportality_ssgt_uuid(2027) == "xs07bd3fmp"  # 2026/27, the active season


def test_layout_info_is_fetched_once_per_process(served):
    routes, calls, _ = served
    routes["/statistics-v2/stats-info/teams_summary"] = _Resp(
        _full("statistics-v2__stats-info__teams_summary__qzw34da6jg.json")
    )
    for _ in range(3):
        rt._get(f"{B}/statistics-v2/stats-info/teams_summary", {"season": 2026})
    assert [u for u, _ in calls].count(f"{B}/statistics-v2/layout-info") == 1


def test_schedule_gets_all_three_uuids_and_passes_the_full_season(served):
    routes, calls, _ = served
    routes["/sports-v2/game-schedule"] = _Resp(_full("sports-v2__game-schedule__qzw34da6jg.json"))
    out = rt._get(f"{B}/sports-v2/game-schedule", {"season": 2026, "gameType": "regular"})
    assert calls[-1][1] == {"seasonUuid": "ggp2aapun2", "seriesUuid": "m1ay3mpa4t", "gameTypeUuid": "aprlbjgxlp"}
    assert len(out["gameInfo"]) == 192


def test_schedule_of_another_season_is_refused(served):
    """The 2024/25 schedule served for a 2025/26 request: a wrong answer -> AssetFetchError."""
    routes, _, _ = served
    other = _full("sports-v2__game-schedule__qzw34da6jg.json")
    other["ssgtUuid"] = "m8izm7whkk"
    for g in other["gameInfo"]:
        g["ssgtUuid"] = "m8izm7whkk"
    routes["/sports-v2/game-schedule"] = _Resp(other)
    with pytest.raises(AssetFetchError, match="not the data asked for"):
        rt._get(f"{B}/sports-v2/game-schedule", {"season": 2026})


def test_schedule_with_games_outside_the_season_window_is_refused(served):
    routes, _, _ = served
    body = _full("sports-v2__game-schedule__qzw34da6jg.json")
    body["gameInfo"][0]["rawStartDateTime"] = "2019-10-01T17:00:00.000Z"
    routes["/sports-v2/game-schedule"] = _Resp(body)
    with pytest.raises(AssetFetchError, match="outside the season window"):
        rt._get(f"{B}/sports-v2/game-schedule", {"season": 2026})


def test_schedule_with_a_cut_team_list_is_refused(served):
    routes, _, _ = served
    routes["/sports-v2/game-schedule"] = _Resp(_load("sports-v2__game-schedule@qzw34da6jg.json"))  # teamList cut to 3
    with pytest.raises(AssetFetchError, match="team list"):
        rt._get(f"{B}/sports-v2/game-schedule", {"season": 2026})


def test_match_day_without_a_season_uses_the_active_ssgt(served):
    routes, calls, _ = served
    routes["/sports-v2/last-game-day"] = _Resp(_load("sports-v2__last-game-day.json"))
    rt._get(f"{B}/sports-v2/last-game-day", {"season": None, "gameType": "regular"})
    assert calls[-1][1] == {"seasonSeriesGameType": "xs07bd3fmp"}


def test_match_day_of_another_ssgt_is_refused(served):
    routes, _, _ = served
    routes["/sports-v2/next-game-day"] = _Resp(_load("sports-v2__next-game-day.json"))
    with pytest.raises(AssetFetchError, match="not the data asked for"):
        rt._get(f"{B}/sports-v2/next-game-day", {"season": 2026})  # the capture is the active 2026/27 ssgt


@pytest.mark.parametrize(("season", "game_type"), [(2028, "regular"), (2002, "regular"), (2026, "allstar")])
def test_unknown_season_or_game_type_is_no_data(served, season, game_type):
    with pytest.raises(NoDataError, match="layout-info covers 2003-2027"):
        rt.sportality_ssgt_uuid(season, game_type)


def test_ssgt_helper_and_bad_season(served):
    assert rt.sportality_ssgt_uuid(2026) == SSGT_2026
    with pytest.raises(ValueError, match="END year"):
        rt.sportality_ssgt_uuid("twenty")


def test_a_short_layout_directory_is_refused(served):
    routes, _, _ = served
    short = copy.deepcopy(LAYOUT)
    short[0]["seasonSeriesGameTypeTeams"] = short[0]["seasonSeriesGameTypeTeams"][:5]
    routes["/statistics-v2/layout-info"] = _Resp(short)
    with pytest.raises(AssetFetchError, match="expected the 40"):
        rt.sportality_ssgt_uuid(2026)


# --------------------------------------------------------------------------- stats-info refusals


def test_truncated_stats_page_is_refused(served):
    routes, _, _ = served
    body = _full("statistics-v2__stats-info__players_summary__qzw34da6jg.json")
    body[0]["stats"] = body[0]["stats"][:1] * rt.STATS_COUNT
    routes["/statistics-v2/stats-info/players_summary"] = _Resp(body)
    with pytest.raises(AssetFetchError, match="may be cut"):
        rt._get(f"{B}/statistics-v2/stats-info/players_summary", {"season": 2026})


def test_stats_page_short_of_its_total_count_is_refused(served):
    """count=1000 sent, 3 rows back while totalCount says 164: a cut answer, not a short table."""
    routes, _, _ = served
    routes["/statistics-v2/stats-info/players_summary"] = _Resp(
        _load("statistics-v2__stats-info__module__players_summary.json")
    )
    with pytest.raises(AssetFetchError, match="3 of totalCount=164"):
        rt._get(f"{B}/statistics-v2/stats-info/players_summary", {"season": 2026})


def test_teams_table_with_a_null_total_is_checked_on_the_count_alone(served):
    routes, _, _ = served
    routes["/statistics-v2/stats-info/teams_summary"] = _Resp(
        _full("statistics-v2__stats-info__teams_summary__qzw34da6jg.json")
    )
    assert len(rt._get(f"{B}/statistics-v2/stats-info/teams_summary", {"season": 2026})[0]["stats"]) == 12


def test_unknown_module_that_answers_the_games_table_is_refused(served):
    """An unknown stats-info module answers the games table with HTTP 200."""
    routes, _, _ = served
    routes["/statistics-v2/stats-info/players_bogus"] = _Resp(
        _full("statistics-v2__stats-info__games_games__qzw34da6jg.json")
    )
    with pytest.raises(AssetFetchError, match="unknown module answers the games table"):
        rt._get(f"{B}/statistics-v2/stats-info/players_bogus", {"season": 2026})


def test_table_of_teams_outside_the_ssgt_is_refused(served):
    routes, _, _ = served
    routes["/statistics-v2/stats-info/teams_summary"] = _Resp(
        _full("statistics-v2__stats-info__teams_summary__qzw34da6jg.json")
    )
    with pytest.raises(AssetFetchError, match="not teams of the ssgt"):
        rt._get(f"{B}/statistics-v2/stats-info/teams_summary", {"season": 2027})  # a different ssgt, other team set


def test_player_crosswalk_is_one_stats_info_call(served):
    routes, calls, _ = served
    routes["/statistics-v2/stats-info/players_summary"] = _Resp(
        _full("statistics-v2__stats-info__players_summary__qzw34da6jg.json")
    )
    xw = rt.sportality_player_crosswalk(2026)
    assert [u.rsplit("/", 1)[-1] for u, _ in calls] == ["layout-info", "players_summary"]
    assert xw.schema["player_id"] == pl.Int64 and xw.height == 206


# --------------------------------------------------------------------------- scope and shape refusals


def test_another_team_page_is_a_failed_fetch(served):
    """A team page for another uuid is a wrong answer (AssetFetchError), not "nothing here"."""
    routes, _, _ = served
    routes["/sports-v2/teams/zzzzzzzzzz"] = _Resp(_load("sports-v2__teams__team_uuid.json"))
    with pytest.raises(AssetFetchError, match="not the data asked for"):
        rt._get(f"{B}/sports-v2/teams/zzzzzzzzzz")


def test_empty_gameday_body_is_no_data(served):
    """Games before 2023/24, and unknown game uuids, answer 200 with an empty body."""
    routes, _, _ = served
    routes["/gameday/play-by-play/qqqqqqqqqq"] = _Resp(text="")
    with pytest.raises(NoDataError, match="2023/24"):
        rt._get(f"{B}/gameday/play-by-play/qqqqqqqqqq")


def test_unknown_game_header_with_null_fields_is_no_data(served):
    routes, _, _ = served
    routes["/sports-v2/game-info/qqqqqqqqqq"] = _Resp({"gameInfo": {"gameUuid": None, "extId": None, "state": None}})
    with pytest.raises(NoDataError, match="no rows"):
        rt._get(f"{B}/sports-v2/game-info/qqqqqqqqqq")


@pytest.mark.parametrize("body", [{}, []])
def test_stub_body_is_no_data(served, body):
    routes, _, _ = served
    routes["/sports-v2/teams/rfo82vr830"] = _Resp(body)
    with pytest.raises(NoDataError, match="empty body"):
        rt._get(f"{B}/sports-v2/teams/rfo82vr830")


def test_zero_row_table_is_no_data(served):
    routes, _, _ = served
    routes["/sports-v2/upcoming-games/rfo82vr830"] = _Resp({"upcomingGames": []})
    with pytest.raises(NoDataError, match="no rows"):
        rt._get(f"{B}/sports-v2/upcoming-games/rfo82vr830")


def test_error_envelope_is_a_failed_fetch(served):
    routes, _, _ = served
    routes[f"/gameday/boxscore/{GAME}"] = _Resp({"statusCode": 500, "message": "Internal server error"})
    with pytest.raises(AssetFetchError, match="error envelope"):
        rt._get(f"{B}/gameday/boxscore/{GAME}")


def test_non_json_body_is_a_failed_fetch(served):
    routes, _, _ = served
    routes[f"/gameday/boxscore/{GAME}"] = _Resp(text="<!doctype html><html></html>")
    with pytest.raises(AssetFetchError, match="non-JSON"):
        rt._get(f"{B}/gameday/boxscore/{GAME}")


def test_body_without_its_scope_field_is_unverifiable(served):
    routes, _, _ = served
    body = _load("gameday__game-overview__game_uuid.json")
    del body["gameUuid"]
    routes[f"/gameday/game-overview/{GAME}"] = _Resp(body)
    with pytest.raises(AssetFetchError, match="cannot be verified"):
        rt._get(f"{B}/gameday/game-overview/{GAME}")


def test_played_games_of_another_team_are_refused(served):
    routes, _, _ = served
    routes["/sports-v2/played-games/zzzzzzzzzz"] = _Resp(_load("sports-v2__played-games__team_uuid.json"))
    with pytest.raises(AssetFetchError, match="does not involve team"):
        rt._get(f"{B}/sports-v2/played-games/zzzzzzzzzz")


def test_career_of_another_athlete_is_refused(served):
    routes, _, _ = served
    routes["/statistics-v2/athlete/playerProfile_gameLog"] = _Resp(
        _load("statistics-v2__athlete__playerProfile_gameLog.json")
    )
    with pytest.raises(AssetFetchError, match="not (only) the requested athlete|requested athlete"):
        rt._get(f"{B}/statistics-v2/athlete/playerProfile_gameLog", {"playerUuid": "zzzzzzzzzz"})


def test_profile_of_another_athlete_is_refused(served):
    routes, _, _ = served
    routes["/statistics-v2/athlete/profile-page"] = _Resp(_load("statistics-v2__athlete__profile-page.json"))
    with pytest.raises(AssetFetchError, match="not the data asked for"):
        rt._get(f"{B}/statistics-v2/athlete/profile-page", {"playerUuid": "zzzzzzzzzz"})


def test_sites_must_be_the_clubs_of_the_ssgt(served):
    routes, _, _ = served
    routes[f"/sports-v2/all-sites/{SSGT_2026}"] = _Resp(
        _load("sports-v2__all-sites__ssgt_uuid.json")
    )  # the 2026/27 clubs
    with pytest.raises(AssetFetchError, match="not layout-info's"):
        rt._get(f"{B}/sports-v2/all-sites/{SSGT_2026}")


def test_head_to_head_names_the_teams_asked_for(served):
    routes, _, _ = served
    url = "/statistics-v2/player-h2h-v2/xs07bd3fmp/rfo82vr830/gvvuxe9ysw"
    routes[url] = _Resp(_load("statistics-v2__player-h2h-v2__ssgt_uuid__home_team_uuid__away_team_uuid.json"))
    assert rt._get(f"{B}{url}")["homeTeamUuid"] == "rfo82vr830"
    swapped = "/statistics-v2/player-h2h-v2/xs07bd3fmp/gvvuxe9ysw/rfo82vr830"
    routes[swapped] = routes[url]
    with pytest.raises(AssetFetchError, match="not the data asked for"):
        rt._get(f"{B}{swapped}")


def test_league_is_hard_to_fool_with_personal_data(served):
    """The runtime removes personal keys from every body before it is returned."""
    routes, _, _ = served
    body = _load("sports-v2__athlete-details__athlete_uuid.json")
    body["athleteData"]["dateOfBirth"] = "2007-03-01"
    body["athleteData"]["contactInfo"] = {"email": "x@y.z"}
    routes["/sports-v2/athlete-details/dpu0901c0t"] = _Resp(body)
    out = rt._get(f"{B}/sports-v2/athlete-details/dpu0901c0t")
    assert "dateOfBirth" not in out["athleteData"] and "contactInfo" not in out["athleteData"]
    assert out["athleteData"]["uuid"] == "dpu0901c0t"


# --------------------------------------------------------------------------- game bodies: wrong game, unfinished, truncated


def test_a_whole_finished_feed_passes(served):
    routes, _, _ = served
    routes[f"/gameday/play-by-play/{GAME}"] = _Resp(_pbp())
    assert len(rt._get(f"{B}/gameday/play-by-play/{GAME}")) == 129


def test_another_games_feed_is_refused(served):
    routes, _, _ = served
    routes["/gameday/play-by-play/zzzzzzzzzz"] = _Resp(_pbp())
    with pytest.raises(AssetFetchError, match=f"gameUuid='{GAME}'"):
        rt._get(f"{B}/gameday/play-by-play/zzzzzzzzzz")


@pytest.mark.parametrize("keep", [100, 60, 5])
def test_a_truncated_feed_is_refused(served, keep):
    """The source has games whose feed stops early (e.g. at 89-91 of a 92-93 game): the game-end line is missing."""
    routes, _, _ = served
    events = [e for e in _pbp() if e["type"] != "game" or e.get("subType") != "end"][-keep:]
    routes[f"/gameday/play-by-play/{GAME}"] = _Resp(events)
    with pytest.raises(AssetFetchError, match="truncated feed"):
        rt._get(f"{B}/gameday/play-by-play/{GAME}")


def test_an_unfinished_game_feed_is_refused(served):
    routes, _, _ = served
    events = _pbp()
    for e in events:
        if e["type"] == "game" and e.get("subType") == "end":
            e["gameState"] = "ongoing"
    routes[f"/gameday/play-by-play/{GAME}"] = _Resp(events)
    with pytest.raises(AssetFetchError, match="unfinished game"):
        rt._get(f"{B}/gameday/play-by-play/{GAME}")


def test_a_feed_that_puts_one_player_on_both_teams_is_refused(served):
    routes, _, _ = served
    events = _pbp()
    shot = next(e for e in events if e.get("eventTeam", {}).get("place") == "home" and e.get("player"))
    other = next(e for e in events if e.get("eventTeam", {}).get("place") == "away" and e.get("player"))
    other["player"]["playerId"] = shot["player"]["playerId"]
    routes[f"/gameday/play-by-play/{GAME}"] = _Resp(events)
    with pytest.raises(AssetFetchError, match="plays for both teams"):
        rt._get(f"{B}/gameday/play-by-play/{GAME}")


def test_a_feed_whose_event_team_disagrees_with_its_side_block_is_refused(served):
    routes, _, _ = served
    events = _pbp()
    next(e for e in events if e.get("eventTeam", {}).get("place") == "home")["eventTeam"]["teamId"] = "999"
    routes[f"/gameday/play-by-play/{GAME}"] = _Resp(events)
    with pytest.raises(AssetFetchError, match="eventTeam 'home'"):
        rt._get(f"{B}/gameday/play-by-play/{GAME}")


def test_a_feed_naming_two_genius_matches_is_refused(served):
    routes, _, _ = served
    events = _pbp()
    events[3]["gameSourceId"] = "999"
    routes[f"/gameday/play-by-play/{GAME}"] = _Resp(events)
    with pytest.raises(AssetFetchError, match="Genius match ids"):
        rt._get(f"{B}/gameday/play-by-play/{GAME}")


def test_a_box_score_passes_and_a_foreign_player_row_is_refused(served):
    routes, _, _ = served
    routes[f"/gameday/boxscore/{GAME}"] = _Resp(_box())
    assert rt._get(f"{B}/gameday/boxscore/{GAME}")["provider"] == "genius-basketball"
    body = _box()
    next(r for r in body["stats"]["homeTeamValue"] if r["info"]["playerId"] != "")["info"]["playerId"] = 1
    routes[f"/gameday/boxscore/{GAME}"] = _Resp(body)
    with pytest.raises(AssetFetchError, match="not in the homeTeamValue players map"):
        rt._get(f"{B}/gameday/boxscore/{GAME}")


def test_a_box_score_without_a_totals_row_is_refused(served):
    routes, _, _ = served
    body = _box()
    body["stats"]["awayTeamValue"] = [
        r for r in body["stats"]["awayTeamValue"] if r["info"].get("label") != "sports.stats.team_totals"
    ]
    routes[f"/gameday/boxscore/{GAME}"] = _Resp(body)
    with pytest.raises(AssetFetchError, match="0 team_totals rows"):
        rt._get(f"{B}/gameday/boxscore/{GAME}")


def test_a_box_score_with_a_player_on_both_sides_is_refused(served):
    routes, _, _ = served
    body = _box()
    pid = next(iter(body["players"]["homeTeamValue"]))
    body["players"]["awayTeamValue"][pid] = body["players"]["homeTeamValue"][pid]
    routes[f"/gameday/boxscore/{GAME}"] = _Resp(body)
    with pytest.raises(AssetFetchError, match="both sides"):
        rt._get(f"{B}/gameday/boxscore/{GAME}")


def test_player_stats_share_the_box_checks(served):
    routes, _, _ = served
    body = _box()  # player-stats has the box score's shape (its rows just carry no team id)
    for rows in body["stats"].values():
        for r in rows:
            r["info"].pop("teamId", None)
    routes[f"/gameday/player-stats/{GAME}"] = _Resp(body)
    assert len(rt._get(f"{B}/gameday/player-stats/{GAME}")["stats"]["homeTeamValue"]) > 3
    # the committed capture is cut to 3 rows per side and its name maps to 3 keys: its rows do not match them
    routes[f"/gameday/player-stats/{GAME}"] = _Resp(_load("gameday__player-stats__game_uuid.json"))
    with pytest.raises(AssetFetchError):
        rt._get(f"{B}/gameday/player-stats/{GAME}")


def test_overview_of_a_finished_game_passes_and_an_unfinished_one_is_refused(served):
    routes, _, _ = served
    routes[f"/gameday/game-overview/{GAME}"] = _Resp(_load("gameday__game-overview__game_uuid.json"))
    assert rt._get(f"{B}/gameday/game-overview/{GAME}")["state"] == "GameEnded"
    live = _load("gameday__game-overview__game_uuid.json")
    live["state"] = "GameOngoing"
    routes[f"/gameday/game-overview/{GAME}"] = _Resp(live)
    with pytest.raises(AssetFetchError, match="unfinished game"):
        rt._get(f"{B}/gameday/game-overview/{GAME}")


def test_team_stats_of_the_game_pass_and_a_foreign_block_is_refused(served):
    routes, _, _ = served
    routes[f"/gameday/team-stats/{GAME}"] = _Resp(_full("gameday__team-stats__c4hh5nig6t.json"))
    assert rt._get(f"{B}/gameday/team-stats/{GAME}")["home"]["gameUuid"] == GAME
    body = _full("gameday__team-stats__c4hh5nig6t.json")
    body["away"]["gameUuid"] = "zzzzzzzzzz"
    routes[f"/gameday/team-stats/{GAME}"] = _Resp(body)
    with pytest.raises(AssetFetchError, match="not the data asked for"):
        rt._get(f"{B}/gameday/team-stats/{GAME}")


def test_game_header_of_an_upcoming_game_is_not_refused(served):
    """A header describes the game at schedule time; only the gameday bodies need a finished game."""
    routes, _, _ = served
    body = _load("sports-v2__game-info__game_uuid.json")
    body["gameInfo"]["state"] = "pre_game"
    routes[f"/sports-v2/game-info/{GAME}"] = _Resp(body)
    assert rt._get(f"{B}/sports-v2/game-info/{GAME}")["gameInfo"]["state"] == "pre_game"


# --------------------------------------------------------------------------- vocabulary and politeness


@pytest.mark.parametrize(("status", "error"), [(404, NoDataError), (400, ValueError), (500, AssetFetchError)])
def test_non_200_follows_the_error_vocabulary(served, status, error):
    routes, _, _ = served
    routes["/sports-v2/teams/rfo82vr830"] = _Resp({"message": "x"}, status=status)
    with pytest.raises(error):
        rt._get(f"{B}/sports-v2/teams/rfo82vr830")


def test_provider_text_route(served):
    routes, _, waits = served
    routes[f"/sports-v2/ssgt/{SSGT_2026}/provider"] = _Resp(text="genius-basketball")
    assert rt._get(f"{B}/sports-v2/ssgt/{SSGT_2026}/provider") == "genius-basketball"
    routes[f"/sports-v2/ssgt/{SSGT_2026}/provider"] = _Resp(text="<!doctype html><html>app shell</html>")
    with pytest.raises(AssetFetchError, match="not a provider name"):
        rt._get(f"{B}/sports-v2/ssgt/{SSGT_2026}/provider")
    assert len(waits) == 2


class _Wire:
    def __init__(self, status: int, body: bytes, headers: Dict[str, str] = None) -> None:
        self.status_code, self.content, self.text = status, body, body.decode()
        self.headers = {"Content-Type": "application/json", **(headers or {})}
        self.url = f"{B}/sports-v2/teams/rfo82vr830"

    def json(self) -> Any:
        return json.loads(self.content)


@pytest.fixture
def wire(monkeypatch: pytest.MonkeyPatch):
    """Real ``download()`` over ``SESSION`` with the socket and the sleeps replaced; yields (answers, waits, sleeps)."""
    from sportsdataverse import dl_utils
    from sportsdataverse.basketball import basketball_runtime as br

    waits: List[str] = []
    sleeps: List[float] = []
    answers: List[_Wire] = []
    monkeypatch.setattr(br, "_polite_wait", waits.append)
    monkeypatch.setattr(dl_utils.time, "sleep", sleeps.append)
    monkeypatch.setattr(br.requests.Session, "request", lambda self, method, url, *a, **k: answers.pop(0))
    return answers, waits, sleeps


def test_every_attempt_retries_included_waits_its_turn(wire):
    """A 503 then a 200 is two polite waits, one per attempt, and the 503 Retry-After is honoured."""
    answers, waits, sleeps = wire
    team = json.dumps(_load("sports-v2__teams__team_uuid.json")).encode()
    answers += [_Wire(503, b"{}", {"Retry-After": "7"}), _Wire(200, team)]
    assert rt._get(f"{B}/sports-v2/teams/rfo82vr830")["uuid"] == "rfo82vr830"
    assert len(waits) == 2
    assert sleeps and sleeps[0] >= 7


def test_a_500_unknown_uuid_is_not_retried(wire):
    """The API answers 500 for an unknown uuid; retrying it would triple the cost of every wrong id."""
    answers, waits, _ = wire
    answers += [_Wire(500, b'{"message":"Internal Server Error"}'), _Wire(200, b"{}")]
    with pytest.raises(AssetFetchError):
        rt._get(f"{B}/sports-v2/teams/rfo82vr830")
    assert len(waits) == 1 and len(answers) == 1


def test_the_attempt_budget_is_three(wire):
    answers, waits, _ = wire
    answers += [_Wire(503, b"{}") for _ in range(5)]
    with pytest.raises(AssetFetchError):
        rt._get(f"{B}/sports-v2/teams/rfo82vr830")
    assert len(waits) == rt.ATTEMPTS + 1 or len(waits) == rt.ATTEMPTS
