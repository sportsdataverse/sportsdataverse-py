"""Offline tests for ``tpbl_runtime``: the error vocabulary, wrong-scope refusals, the season mapping and throttling.

The runtime is driven through the real generated wrappers over a scripted ``requests`` adapter mounted on a
``PoliteSession``, answering with the committed fixtures, so every refusal is exercised end to end and every attempt
is counted. No network, and nothing reads the private sdv-internal-refs checkout.
"""

from __future__ import annotations

import copy
import json
from pathlib import Path
from typing import Any, Callable, Dict, List, Tuple
from urllib.parse import parse_qs, urlsplit

import polars as pl
import pytest
import requests
from requests.adapters import HTTPAdapter

from sportsdataverse import dl_utils
from sportsdataverse.basketball import basketball_runtime
from sportsdataverse.basketball.tpbl import tpbl as W
from sportsdataverse.basketball.tpbl import tpbl_runtime as R
from sportsdataverse.errors import AssetFetchError, NoDataError

FIXTURES = Path(__file__).parents[3] / "tests" / "fixtures" / "tpbl"
JSON = {"content-type": "application/json"}
HTML = {"content-type": "text/html; charset=UTF-8"}


def _full(name: str) -> Any:
    return json.loads((FIXTURES / "full" / f"{name}.json").read_text(encoding="utf-8"))


def _cap(name: str) -> Any:
    return json.loads((FIXTURES / "captures" / f"{name}.json").read_text(encoding="utf-8"))


class Api(HTTPAdapter):
    """A scripted api.tpbl.basketball: ``handler(request)`` returns ``(status, headers, body)``; requests are recorded."""

    def __init__(self, handler: Callable[[requests.PreparedRequest], Tuple[int, Dict[str, str], Any]]) -> None:
        super().__init__()
        self.handler = handler
        self.calls: List[requests.PreparedRequest] = []

    def send(self, request: requests.PreparedRequest, **kwargs: Any) -> requests.Response:  # type: ignore[override]
        self.calls.append(request)
        status, headers, body = self.handler(request)
        resp = requests.Response()
        resp.status_code = status
        resp.headers.update(headers)
        resp._content = body if isinstance(body, bytes) else str(body).encode("utf-8")
        resp.encoding = "utf-8"
        resp.url = request.url
        resp.request = request
        return resp

    def paths(self) -> List[str]:
        return [urlsplit(c.url).path for c in self.calls]

    def queries(self) -> List[Dict[str, List[str]]]:
        return [parse_qs(urlsplit(c.url).query) for c in self.calls]


@pytest.fixture
def waits(monkeypatch):
    """Count one polite wait per attempt; sleeping is free."""
    seen: List[str] = []
    monkeypatch.setattr(basketball_runtime, "_polite_wait", lambda url, *a, **k: seen.append(url))
    slept: List[float] = []
    monkeypatch.setattr(dl_utils.time, "sleep", lambda s: slept.append(s))
    return type("W", (), {"urls": seen, "slept": slept})


@pytest.fixture
def site(monkeypatch, waits):
    """Mount an Api on a fresh polite session; the season list and finished-game caches start empty."""

    def install(handler):
        adapter = Api(handler)
        session = basketball_runtime.PoliteSession()
        session.mount("https://", adapter)
        monkeypatch.setattr(R, "SESSION", session)
        monkeypatch.setattr(R, "_GAMES", {})
        monkeypatch.setattr(R, "_SEASONS", [])
        return adapter

    return install


def _shots(rounds: List[str] | None) -> List[Dict[str, Any]]:
    return _full("team_shots_2_4_r1")


def _routes(**over: Any) -> Dict[str, Any]:
    """Path (after the host) -> body. A value may be a callable ``f(request) -> (status, headers, body)``."""
    base: Dict[str, Any] = {
        "/api/seasons": _full("seasons"),
        "/api/seasons/3/games": _full("season_games_2027"),
        "/api/events/3/divisions": _cap("events__event_id__divisions@3"),
        "/api/events/2/teams": _cap("events__event_id__teams@2"),
        "/api/games/6": _full("game_6"),
        "/api/games/6/stats": _full("game_stats_6"),
        "/api/divisions/2/games/standings": _full("standings_2"),
        "/api/divisions/2/teams/4/players": _full("roster_2_4"),
        "/api/divisions/2/teams/4/shooting-spots": _full("team_shots_2_4_r1"),
        "/api/divisions/2/players/75/shooting-spots": _full("team_shots_2_4_r1"),
        "/api/players/75/games/stats": _full("player_games_75_div2"),
    }
    base.update(over)
    return base


def _api(routes: Dict[str, Any] | None = None, *, status: int = 200, headers=None):
    table = routes if routes is not None else _routes()

    def handler(req):
        path = urlsplit(req.url).path
        if path not in table:
            return 404, HTML, "<html>Not Found</html>"
        val = table[path]
        if callable(val):
            return val(req)
        return status, headers or JSON, val if isinstance(val, (str, bytes)) else json.dumps(val)

    return handler


def _edit(body: Any, *path: Any, value: Any) -> Any:
    """A deep copy of ``body`` with ``body[path...] = value``."""
    out = copy.deepcopy(body)
    node = out
    for p in path[:-1]:
        node = node[p]
    node[path[-1]] = value
    return out


# --- end to end ----------------------------------------------------------------------------------


def test_wrappers_end_to_end_each_attempt_waits_and_the_season_list_is_read_once(site, waits):
    adapter = site(_api())
    games = W.tpbl_games(season=2027)
    assert games.height == 133 and games["season"].unique().to_list() == [2027]
    assert W.tpbl_divisions(season=2027)["division_id"].to_list() == [655, 656]
    assert adapter.paths() == ["/api/seasons", "/api/seasons/3/games", "/api/events/3/divisions"]  # mapped once
    assert len(waits.urls) == len(adapter.calls)  # one throttled wait per request


def test_the_box_score_fetches_the_game_detail_once_and_checks_the_stats_against_it(site, waits):
    adapter = site(_api())
    players = W.tpbl_box_players(game_id=6)
    assert players.height == 35 and players["game_id"].unique().to_list() == [6]
    assert adapter.paths() == ["/api/games/6", "/api/games/6/stats"]
    assert W.tpbl_box_teams(game_id=6).height == 2 and W.tpbl_box_team_periods(game_id=6).height == 8
    assert W.tpbl_game(game_id=6).height == 1 and W.tpbl_referees(game_id=6).height == 3  # served from the cache
    assert adapter.paths() == ["/api/games/6", "/api/games/6/stats", "/api/games/6/stats", "/api/games/6/stats"]
    assert len(waits.urls) == len(adapter.calls)


def test_division_and_player_wrappers_stamp_what_they_asked_for(site):
    adapter = site(_api())
    roster = W.tpbl_roster(division_id=2, team_id=4, return_parsed=False)
    assert roster["_request"] == {"division_id": 2, "team_id": 4} and len(roster["data"]) == 13
    log = W.tpbl_player_games(player_id=75, division_id=2)
    assert log.height == 20 and log["division_id"].unique().to_list() == [2]
    assert adapter.queries()[-1] == {"division_id": ["2"]}
    table = W.tpbl_standings(division_id=2)
    assert table.height == 7 and W.tpbl_standings_against(division_id=2).height == 42
    assert W.tpbl_player_shots(division_id=2, player_id=75)["player_id"].unique().to_list() == [75]
    raw = W.tpbl_game(game_id=6, return_parsed=False)
    assert raw["_request"] == {"game_id": 6} and raw["id"] == 6


def test_the_rounds_filter_is_sent_as_the_repeatable_rounds_array(site):
    adapter = site(_api())
    shots = W.tpbl_team_shots(division_id=2, team_id=4, rounds=[2, 1, 2])
    assert adapter.queries() == [{"rounds[]": ["1", "2"]}] and shots["rounds"].unique().to_list() == ["1,2"]
    W.tpbl_team_shots(division_id=2, team_id=4, rounds=1)
    assert adapter.queries()[-1] == {"rounds[]": ["1"]}
    W.tpbl_team_shots(division_id=2, team_id=4)
    assert adapter.queries()[-1] == {} and len(adapter.calls) == 3
    for bad in (0, "x", [1, "y"], [], -1, True):
        with pytest.raises(ValueError, match="rounds"):
            W.tpbl_team_shots(division_id=2, team_id=4, rounds=bad)
    assert len(adapter.calls) == 3  # refused before any request


def test_the_team_filter_reaches_the_api_and_is_checked(site):
    games = _full("season_games_2027")
    mine = [g for g in games if 4 in (g["home_team"]["id"], g["away_team"]["id"])]

    def by_team(req):
        q = parse_qs(urlsplit(req.url).query)
        return 200, JSON, json.dumps(mine if q.get("team_id") == ["4"] else games)

    adapter = site(_api(_routes(**{"/api/seasons/3/games": by_team})))
    assert W.tpbl_games(season=2027, team_id=4).height == len(mine) < 133
    assert adapter.queries()[-1] == {"team_id": ["4"]}
    site(_api(_routes(**{"/api/seasons/3/games": lambda req: (200, JSON, json.dumps(games))})))
    with pytest.raises(AssetFetchError, match="does not involve team 4"):  # the API ignored the filter
        W.tpbl_games(season=2027, team_id=4)


# --- the season mapping --------------------------------------------------------------------------


def test_the_end_year_is_mapped_through_the_season_list(site):
    adapter = site(_api())
    assert [int(R._season_row(y)["id"]) for y in (2025, 2026, 2027)] == [1, 2, 3]
    assert [R._season_row(y)["gohoops_id"] for y in (2025, 2026, 2027)] == [101, 213, 275]
    assert adapter.paths() == ["/api/seasons"]
    with pytest.raises(NoDataError, match="no TPBL season ends in 2030"):
        R._season_row(2030)
    assert adapter.paths() == ["/api/seasons", "/api/seasons"]  # a missing year re-reads the list once


def test_a_season_added_later_is_found_without_a_code_change(site):
    first = _full("seasons")
    later = first + [
        {
            "id": 4,
            "gohoops_id": 340,
            "name": "2027-2028 賽季",
            "status": "IN_PROGRESS",
            "started_at": "x",
            "ended_at": "y",
        }
    ]
    state = {"n": 0}

    def seasons(req):
        state["n"] += 1
        return 200, JSON, json.dumps(first if state["n"] == 1 else later)

    site(_api(_routes(**{"/api/seasons": seasons, "/api/events/4/teams": [{"id": 2, "name": "x", "gohoops_id": 940}]})))
    assert W.tpbl_divisions(season=2027).height == 2  # first read: three seasons
    assert R._season_row(2028)["id"] == 4  # not listed -> the list is re-read once and now has it
    assert W.tpbl_teams(season=2028, return_parsed=False)["_request"]["season_id"] == 4


@pytest.mark.parametrize(
    "mutate",
    [
        lambda s: [{**s[0], "name": "Season One"}, *s[1:]],  # a label that is not 'YYYY-YYYY ...'
        lambda s: [{**s[0], "name": "2024-2026 賽季"}, *s[1:]],  # not consecutive years
        lambda s: [s[0], s[0]],  # duplicate ids
        lambda s: [{k: v for k, v in s[0].items() if k != "gohoops_id"}, *s[1:]],
        lambda s: [],
    ],
)
def test_a_season_list_that_changed_shape_is_refused(site, mutate):
    site(_api(_routes(**{"/api/seasons": mutate(_full("seasons"))})))
    with pytest.raises(AssetFetchError):
        W.tpbl_games(season=2027)


# --- error vocabulary --------------------------------------------------------------------------------


def test_an_unknown_id_is_no_data_not_an_empty_frame(site):
    adapter = site(_api())
    with pytest.raises(NoDataError, match="No data found"):
        W.tpbl_game(game_id=999999)
    with pytest.raises(NoDataError, match="No data found"):
        W.tpbl_roster(division_id=99999, team_id=4)
    assert len(adapter.calls) == 2  # a 404 is not retried


def test_400_is_a_value_error_and_other_statuses_are_fetch_failures(site):
    site(_api(status=400, headers=JSON))
    with pytest.raises(ValueError, match="HTTP 400"):
        W.tpbl_seasons()
    site(_api(status=422, headers=HTML))
    with pytest.raises(ValueError, match="HTTP 422"):
        W.tpbl_seasons()
    site(_api(status=403, headers=HTML))
    with pytest.raises(AssetFetchError, match="HTTP 403"):
        W.tpbl_seasons()


@pytest.mark.parametrize(
    ("body", "headers", "match"),
    [
        ({"message": "Server Error", "exception": "ErrorException"}, JSON, "expected a JSON list"),
        ("<html>maintenance</html>", HTML, "not JSON"),
        (b"", JSON, "empty body"),
        ("not json {", JSON, "not JSON"),
        ("{}", JSON, "expected a JSON list"),
    ],
)
def test_error_envelopes_stubs_and_non_json_bodies_are_refused(site, body, headers, match):
    site(
        _api(_routes(**{"/api/seasons": body if isinstance(body, (str, bytes)) else json.dumps(body)}), headers=headers)
    )
    with pytest.raises(AssetFetchError, match=match):
        W.tpbl_seasons()


@pytest.mark.parametrize(
    "short, args, path",
    [
        ("roster", {"division_id": 2, "team_id": 4}, "/api/divisions/2/teams/4/players"),
        ("team_shots", {"division_id": 2, "team_id": 4}, "/api/divisions/2/teams/4/shooting-spots"),
        ("player_shots", {"division_id": 2, "player_id": 75}, "/api/divisions/2/players/75/shooting-spots"),
        ("player_games", {"player_id": 75, "division_id": 2}, "/api/players/75/games/stats"),
        ("standings", {"division_id": 2}, "/api/divisions/2/games/standings"),
        ("games", {"season": 2027}, "/api/seasons/3/games"),
        ("divisions", {"season": 2027}, "/api/events/3/divisions"),
        ("teams", {"season": 2026}, "/api/events/2/teams"),
    ],
)
def test_an_empty_list_is_no_data(site, short, args, path):
    site(_api(_routes(**{path: []})))
    with pytest.raises(NoDataError):
        getattr(W, f"tpbl_{short}")(**args)


def test_the_standings_of_a_bracket_division_say_to_use_the_game_list(site):
    site(_api(_routes(**{"/api/divisions/2/games/standings": []})))
    with pytest.raises(NoDataError, match="no standings"):
        W.tpbl_standings(division_id=2)


def test_a_deterministic_500_is_retried_except_on_the_player_game_log(site, waits):
    adapter = site(_api(_routes(**{"/api/divisions/2/teams/4/shooting-spots": lambda req: (500, HTML, "boom")})))
    with pytest.raises(AssetFetchError, match="HTTP 500"):
        W.tpbl_team_shots(division_id=2, team_id=4)
    assert len(adapter.calls) == R.ATTEMPTS and len(waits.urls) == R.ATTEMPTS

    waits.urls.clear()
    adapter = site(_api(_routes(**{"/api/players/75/games/stats": lambda req: (500, HTML, "boom")})))
    with pytest.raises(AssetFetchError, match="not retried"):  # an unknown division_id answers 500, one request only
        W.tpbl_player_games(player_id=75, division_id=99999)
    assert len(adapter.calls) == 1 and len(waits.urls) == 1

    adapter = site(_api(_routes(**{"/api/players/75/games/stats": lambda req: (503, HTML, "later")})))
    waits.urls.clear()
    with pytest.raises(AssetFetchError, match="HTTP 503"):  # other 5xx still retry on that route
        W.tpbl_player_games(player_id=75, division_id=2)
    assert len(adapter.calls) == R.ATTEMPTS


def test_retries_wait_their_turn_and_honour_retry_after(site, waits):
    state = {"n": 0}

    def flaky(req):
        state["n"] += 1
        if state["n"] == 1:
            return 429, {"Retry-After": "7"}, b""
        if state["n"] == 2:
            return 503, {}, b""
        return 200, JSON, json.dumps(_full("seasons"))

    adapter = site(_api(_routes(**{"/api/seasons": flaky})))
    assert W.tpbl_seasons().height == 3
    assert len(adapter.calls) == 3 and len(waits.urls) == 3  # each attempt is throttled
    assert max(waits.slept) >= 7.0  # Retry-After honoured, not just the exponential delay


def test_a_connection_failure_names_the_exception_class_only(site, monkeypatch):
    def boom(*a, **k):
        raise requests.ConnectionError("secret-host-detail")

    site(_api())
    monkeypatch.setattr(R, "download", boom)
    with pytest.raises(AssetFetchError, match="ConnectionError") as e:
        W.tpbl_seasons()
    assert "secret-host-detail" not in str(e.value)


@pytest.mark.parametrize("bad", ["abc", None, True, 1.5, -3, ""])
def test_a_bad_id_or_season_is_a_value_error_before_any_request(site, bad):
    adapter = site(_api())
    for short, key in (("game", "game_id"), ("standings", "division_id"), ("games", "season")):
        with pytest.raises(ValueError):
            getattr(W, f"tpbl_{short}")(**{key: bad})
    with pytest.raises(ValueError):
        W.tpbl_player_games(player_id=bad, division_id=2)
    assert adapter.calls == []


def test_the_player_game_log_needs_a_division(site):
    adapter = site(_api())
    with pytest.raises(ValueError, match="division_id"):
        R._get("https://api.tpbl.basketball/api/players/75/games/stats")
    with pytest.raises(ValueError, match="division_id"):
        W.tpbl_player_games(player_id=75, division_id="x")
    assert adapter.calls == []


def test_unsupported_kwargs_and_foreign_urls_are_refused(site):
    adapter = site(_api())
    with pytest.raises(TypeError, match="unsupported"):
        W.tpbl_seasons(timeout=3)
    for url in (
        "https://example.com/api/seasons",
        "https://api.tpbl.basketball/other/seasons",
        "https://api.tpbl.basketball/api/news",
        "https://api.tpbl.basketball/api/games/6/broadcasts",
    ):
        with pytest.raises(ValueError, match="not a TPBL route"):
            R._get(url)
    assert adapter.calls == []


# --- the finished-game guard and the box-score scope checks --------------------------------------------


@pytest.mark.parametrize(
    "game",
    [
        _edit(_full("game_6"), "status", value="NOT_STARTED"),
        _edit(_full("game_6"), "is_live", value=True),
        _edit(_full("game_6"), "status", value="IN_PROGRESS"),
    ],
)
def test_an_unfinished_game_is_a_fetch_failure_and_no_stats_are_asked_for(site, game):
    adapter = site(_api(_routes(**{"/api/games/6": game})))
    for short in ("game", "box_players", "box_teams", "referees"):
        with pytest.raises(AssetFetchError, match="not finished"):
            getattr(W, f"tpbl_{short}")(game_id=6)
    assert set(adapter.paths()) == {"/api/games/6"}  # never the stats route; a failure is not cached
    assert len(adapter.calls) == 4


def _swap_sides(stats: Dict[str, Any]) -> Dict[str, Any]:
    return {"home_team": stats["away_team"], "away_team": stats["home_team"]}


def _bump_player(stats: Dict[str, Any]) -> Dict[str, Any]:
    out = copy.deepcopy(stats)
    line = next(v for v in out["home_team"]["players"]["total"].values() if v.get("score") is not None)
    line["score"] += 1
    return out


def _drop_quarter(stats: Dict[str, Any]) -> Dict[str, Any]:
    out = copy.deepcopy(stats)
    del out["away_team"]["teams"]["rounds"]["4"]
    return out


@pytest.mark.parametrize(
    "routes, match",
    [
        ({"/api/games/6/stats": _swap_sides(_full("game_stats_6"))}, "home/away order"),
        ({"/api/games/6/stats": _bump_player(_full("game_stats_6"))}, "players score"),
        ({"/api/games/6/stats": _drop_quarter(_full("game_stats_6"))}, "quarters|final score"),
        ({"/api/games/6/stats": _edit(_full("game_stats_6"), "home_team", "id", value=999)}, "home/away order"),
        ({"/api/games/6/stats": {"message": "x"}}, "no home_team"),
        ({"/api/games/6/stats": []}, "no home_team"),
        ({"/api/games/6": _edit(_full("game_6"), "home_team", "won_score", value=112)}, "differs from the game detail"),
        ({"/api/games/6": _edit(_full("game_6"), "round", value=5)}, "quarters"),
        ({"/api/games/6": _edit(_full("game_6"), "id", value=7)}, "game id 7 is not 6"),
        ({"/api/games/6": _edit(_full("game_6"), "round", value=None)}, "quarter count"),
    ],
)
def test_a_box_score_that_is_not_this_games_is_refused_not_returned_empty(site, routes, match):
    site(_api(_routes(**routes)))
    with pytest.raises(AssetFetchError, match=match):
        W.tpbl_box_players(game_id=6)


def test_the_known_source_defect_game_is_refused(site):
    """Game 7 (2024-25 preseason): a phantom 5th quarter and away players summing to 87 against a team score of 92."""
    stats = _full("game_stats_6")
    bad = _bump_player(_bump_player(stats))
    site(_api(_routes(**{"/api/games/6/stats": bad})))
    with pytest.raises(AssetFetchError, match="players score"):
        W.tpbl_box_teams(game_id=6)


# --- list scope checks -------------------------------------------------------------------------------


def _games_with(mutator: Callable[[List[Dict[str, Any]]], None]) -> List[Dict[str, Any]]:
    games = copy.deepcopy(_full("season_games_2027"))
    mutator(games)
    return games


@pytest.mark.parametrize(
    "mutator, match",
    [
        (lambda g: g[0]["home_team"].__setitem__("gohoops_season_id", 213), "not in gohoops season 275"),
        (lambda g: g[1].__setitem__("id", g[0]["id"]), "duplicate game ids"),
        (lambda g: g[3].__setitem__("gohoops_division_id", 1), "maps to two GoHoops divisions"),
        (lambda g: g[0]["away_team"].__setitem__("gohoops_event_id", 490), "more than one GoHoops event"),
        (lambda g: g.append("not an object"), "non-object rows"),
        (lambda g: g[0].__setitem__("division_id", None), "no integer division id"),
    ],
)
def test_a_game_list_of_the_wrong_scope_is_refused(site, mutator, match):
    site(_api(_routes(**{"/api/seasons/3/games": _games_with(mutator)})))
    with pytest.raises(AssetFetchError, match=match):
        W.tpbl_games(season=2027)


@pytest.mark.parametrize(
    "path, mutate, match, call",
    [
        (
            "/api/divisions/2/teams/4/players",
            lambda b: [{**b[0], "team": {**b[0]["team"], "id": 5}}, *b[1:]],
            "not 4",
            lambda: W.tpbl_roster(division_id=2, team_id=4),
        ),
        (
            "/api/divisions/2/teams/4/players",
            lambda b: [{**b[0], "gohoops_event_id": 490}, *b[1:]],
            "more than one GoHoops event|not in the row's GoHoops event",
            lambda: W.tpbl_roster(division_id=2, team_id=4),
        ),
        (
            "/api/players/75/games/stats",
            lambda b: [_edit(b[0], "player", "id", value=76), *b[1:]],
            "not 75",
            lambda: W.tpbl_player_games(player_id=75, division_id=2),
        ),
        (
            "/api/players/75/games/stats",
            lambda b: [_edit(b[0], "game", "gohoops_division_id", value=64), *b[1:]],
            "same GoHoops division",
            lambda: W.tpbl_player_games(player_id=75, division_id=2),
        ),
        (
            "/api/players/75/games/stats",
            lambda b: [b[0], b[0]],
            "appears twice",
            lambda: W.tpbl_player_games(player_id=75, division_id=2),
        ),
        (
            "/api/divisions/2/games/standings",
            lambda b: [b[0], b[0]],
            "unique integer ids",
            lambda: W.tpbl_standings(division_id=2),
        ),
        (
            "/api/events/3/divisions",
            lambda b: [b[0], {**b[1], "gohoops_event_id": 490}],
            "more than one GoHoops event",
            lambda: W.tpbl_divisions(season=2027),
        ),
        (
            "/api/events/2/teams",
            lambda b: [b[0], b[0]],
            "unique objects",
            lambda: W.tpbl_teams(season=2026),
        ),
    ],
)
def test_division_player_and_event_bodies_of_the_wrong_scope_are_refused(site, path, mutate, match, call):
    base = _routes()[path]
    site(_api(_routes(**{path: mutate(base)})))
    with pytest.raises(AssetFetchError, match=match):
        call()


@pytest.mark.parametrize(
    "shots, match",
    [
        ([{"is_made": True, "point_x": 100, "point_y": 100}], "not {is_made"),
        ([{"is_made": "yes", "point_x": 100, "point_y": 100, "offense_basket": 0}], "non-boolean"),
        ([{"is_made": True, "point_x": 100, "point_y": 100, "offense_basket": 2}], "offense_basket"),
        ([{"is_made": True, "point_x": 28001, "point_y": 100, "offense_basket": 1}], "outside the 28000 x 15000"),
        ([{"is_made": True, "point_x": 10.5, "point_y": 100, "offense_basket": 1}], "outside"),
        ([[1, 2]], "not {is_made"),
    ],
)
def test_a_shot_list_must_have_the_documented_shape_and_stay_on_the_court(site, shots, match):
    site(_api(_routes(**{"/api/divisions/2/teams/4/shooting-spots": shots})))
    with pytest.raises(AssetFetchError, match=match):
        W.tpbl_team_shots(division_id=2, team_id=4)


# --- completeness check (verify=True) ------------------------------------------------------------------


def _standings_from(games: List[Dict[str, Any]], div: int) -> List[Dict[str, Any]]:
    rows = []
    teams = sorted({g[s]["id"] for g in games if g["division_id"] == div for s in ("home_team", "away_team")})
    done = [g for g in games if g["division_id"] == div and g["status"] == "COMPLETED"]
    for rank, team in enumerate(teams, start=1):
        won = sum(
            1
            for g in done
            for me, other in (("home_team", "away_team"), ("away_team", "home_team"))
            if g[me]["id"] == team and g[me]["won_score"] > g[other]["won_score"]
        )
        lost = sum(1 for g in done if team in (g["home_team"]["id"], g["away_team"]["id"])) - won
        rows.append(
            {
                "team": {"id": team},
                "rank": rank,
                "score_won_matches": won,
                "score_lost_matches": lost,
                "score_draw_matches": 0,
                "against_result": [],
            }
        )
    return rows


def _verify_routes(games, **over):
    def team_list(req):
        q = parse_qs(urlsplit(req.url).query)
        team = int(q["team_id"][0])
        return 200, JSON, json.dumps([g for g in games if team in (g["home_team"]["id"], g["away_team"]["id"])])

    def seasons_games(req):
        return team_list(req) if "team_id" in urlsplit(req.url).query else (200, JSON, json.dumps(games))

    base = {
        "/api/seasons/3/games": seasons_games,
        "/api/divisions/655/games/standings": _standings_from(games, 655),
        "/api/divisions/656/games/standings": _standings_from(games, 656),
    }
    base.update(over)
    return _routes(**base)


def test_verify_checks_every_division_against_its_own_standings(site):
    games = _full("season_games_2027")
    adapter = site(_api(_verify_routes(games)))
    out = W.tpbl_games(season=2027, verify=True)
    assert out.height == 133
    assert adapter.paths() == [
        "/api/seasons",
        "/api/seasons/3/games",
        "/api/events/3/divisions",
        "/api/divisions/655/games/standings",
        "/api/divisions/656/games/standings",
    ]


def test_verify_refuses_a_list_that_is_short_of_a_completed_game(site):
    games = _full("season_games_2027")
    short = [g for g in games if g is not next(x for x in games if x["status"] == "COMPLETED")]
    site(_api(_verify_routes(short, **{"/api/divisions/655/games/standings": _standings_from(games, 655)})))
    with pytest.raises(AssetFetchError, match="division 655: 6 completed games in the list but the standings count 7"):
        W.tpbl_games(season=2027, verify=True)


def test_verify_refuses_a_division_the_season_does_not_have(site):
    games = _full("season_games_2027")
    other = _full("season_games_2027")
    for g in other:
        if g["division_id"] == 656:
            g["division_id"], g["gohoops_division_id"] = 700, 999
    site(_api(_verify_routes(other, **{"/api/divisions/700/games/standings": []})))
    with pytest.raises(AssetFetchError, match="not a division of season 3"):
        W.tpbl_games(season=2027, verify=True)
    assert games  # the fixture itself is untouched


def test_verify_falls_back_to_the_team_lists_when_the_standings_are_empty(site):
    games = _full("season_games_2027")
    teams = sorted({g[s]["id"] for g in games if g["division_id"] == 655 for s in ("home_team", "away_team")})
    adapter = site(_api(_verify_routes(games, **{"/api/divisions/655/games/standings": []})))
    assert W.tpbl_games(season=2027, verify=True).height == 133
    team_lists = [q for q in adapter.queries() if "team_id" in q]
    assert sorted(int(q["team_id"][0]) for q in team_lists) == teams  # one team-filtered list per team in the division


def test_verify_fallback_refuses_a_team_list_that_disagrees(site):
    games = _full("season_games_2027")

    def lying(req):
        q = parse_qs(urlsplit(req.url).query)
        if "team_id" in q:
            rows = [g for g in games if int(q["team_id"][0]) in (g["home_team"]["id"], g["away_team"]["id"])]
            return 200, JSON, json.dumps([g for g in rows if g["division_id"] != 655])  # drops the preseason games
        return 200, JSON, json.dumps(games)

    site(_api(_verify_routes(games, **{"/api/divisions/655/games/standings": [], "/api/seasons/3/games": lying})))
    with pytest.raises(AssetFetchError, match="own game list differs"):
        W.tpbl_games(season=2027, verify=True)


def test_verify_arguments_are_validated_before_any_request(site):
    adapter = site(_api())
    with pytest.raises(ValueError, match="cannot be combined with team_id"):
        W.tpbl_games(season=2027, team_id=4, verify=True)
    with pytest.raises(ValueError, match="verify must be a bool"):
        W.tpbl_games(season=2027, verify="yes")
    assert adapter.calls == []


# --- personal data -------------------------------------------------------------------------------------


def test_personal_fields_are_redacted_by_key_name_in_raw_bodies(site):
    roster = _full("roster_2_4")
    roster[0]["meta"]["birthday"] = "1994-10-24"
    roster[0]["contact"] = {"email": "a@b.c", "phone": "0912"}
    game = _full("game_6")
    game["referees"][0]["birthday"] = "1980-01-01"
    game["referees"][0]["mobile_phone"] = "0900"
    site(_api(_routes(**{"/api/divisions/2/teams/4/players": roster, "/api/games/6": game})))
    raw = W.tpbl_roster(division_id=2, team_id=4, return_parsed=False)["data"][0]
    assert raw["meta"]["birthday"] == "<redacted>" and raw["contact"] == "<redacted>"
    out = W.tpbl_game(game_id=6, return_parsed=False)["referees"][0]
    assert out["birthday"] == "<redacted>" and out["mobile_phone"] == "<redacted>"
    frame = W.tpbl_referees(game_id=6)
    assert "birthday" not in frame.columns and frame.height == 3
    assert isinstance(R._redact_personal({"a": [{"Birth_Date": 1, "x": 2}]}), dict)
    assert R._redact_personal({"a": [{"Birth_Date": 1, "x": 2}]}) == {"a": [{"Birth_Date": "<redacted>", "x": 2}]}


def test_the_stamped_body_round_trips_through_the_parsers(site):
    site(_api())
    raw = W.tpbl_box_players(game_id=6, return_parsed=False)
    assert raw["_request"] == {"game_id": 6, "home_team_id": 4, "away_team_id": 8, "periods": 4}
    from sportsdataverse.basketball.tpbl import tpbl_parsers as P

    assert P.parse_tpbl_box_players(raw).height == 35 and isinstance(P.parse_tpbl_box_teams(raw), pl.DataFrame)
