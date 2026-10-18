"""The SHL runtime maps END-year seasons to ssgts and refuses every answer the refs capture refuses.

Transport is mocked at ``shl_runtime.download``; bodies are the committed real captures
(``tests/fixtures/shl/``), cut or altered only where a refusal case needs it. No network.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List, Tuple
from urllib.parse import urlsplit

import polars as pl
import pytest

from sportsdataverse.errors import AssetFetchError, NoDataError
from sportsdataverse.hockey.shl import shl_runtime as rt

FIXTURES = Path(__file__).parents[2] / "fixtures" / "shl"
LAYOUT = json.loads((FIXTURES / "full" / "statistics-v2__layout-info__directory.json").read_text(encoding="utf-8"))
B = rt.BASE


def _load(name: str) -> Any:
    return json.loads((FIXTURES / name).read_text(encoding="utf-8"))


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
    routes["/statistics-v2/stats-info/standings_standings"] = _Resp(
        _load("statistics-v2__stats-info__module__standings_standings.json")
    )
    rt._get(f"{B}/statistics-v2/stats-info/standings_standings", {"season": 2026, "gameType": "regular", "count": 50})
    assert calls[-1][1] == {"ssgtUuid": "iuzqg7dqk9", "count": 1000}
    assert len(waits) == len(calls) == 2  # layout-info + the table, each over the polite session


def test_layout_info_is_fetched_once_per_process(served):
    routes, calls, _ = served
    routes["/statistics-v2/stats-info/teams_result"] = _Resp(
        _load("full/statistics-v2__stats-info__teams_result__iuzqg7dqk9.json")
    )
    for _ in range(3):
        rt._get(f"{B}/statistics-v2/stats-info/teams_result", {"season": 2026})
    assert [u for u, _ in calls].count(f"{B}/statistics-v2/layout-info") == 1


def test_schedule_gets_all_three_uuids_and_checks_the_ssgt(served):
    routes, calls, _ = served
    routes["/sports-v2/game-schedule"] = _Resp(_load("sports-v2__game-schedule.json"))
    rt._get(f"{B}/sports-v2/game-schedule", {"season": 2026, "gameType": "regular"})
    assert calls[-1][1] == {"seasonUuid": "xs4m9qupsi", "seriesUuid": "qQ9-bb0bzEWUk", "gameTypeUuid": "qQ9-af37Ti40B"}


def test_schedule_of_another_season_is_refused(served):
    """The 2024/25 schedule served for a 2025/26 request: a wrong answer -> AssetFetchError."""
    routes, _, _ = served
    other = _load("sports-v2__game-schedule.json")
    for g in other["gameInfo"]:
        g["ssgtUuid"] = "qeb-73qtP8d19"
    routes["/sports-v2/game-schedule"] = _Resp(other)
    with pytest.raises(AssetFetchError, match="not the data asked for"):
        rt._get(f"{B}/sports-v2/game-schedule", {"season": 2026})


def test_match_day_without_a_season_uses_the_active_ssgt(served):
    routes, calls, _ = served
    routes["/sports-v2/last-game-day"] = _Resp(_load("sports-v2__last-game-day.json"))
    rt._get(f"{B}/sports-v2/last-game-day", {"season": None, "gameType": "regular"})
    assert calls[-1][1] == {"seasonSeriesGameType": "qa98unlbd6"}


def test_season_game_log_sends_the_start_year(served):
    """The wrapper declares ``transform: {offset: -1}`` (END -> START); ``_get`` sends the season as given."""
    from sportsdataverse.hockey.shl import shl_athlete_game_log

    routes, calls, _ = served
    routes["/statistics-v2/athlete/playerProfile_seasonGameLog"] = _Resp(
        _load("statistics-v2__athlete__playerProfile_seasonGameLog.json")
    )
    shl_athlete_game_log(athlete_uuid="qQ9-349c7WDv6", season=2026, return_parsed=False)
    assert calls[-1][1]["season"] == 2025
    shl_athlete_game_log(athlete_uuid="qQ9-349c7WDv6", return_parsed=False)  # None passes through
    assert "season" not in calls[-1][1]
    rt._get(f"{B}/statistics-v2/athlete/playerProfile_seasonGameLog", {"playerUuid": "qQ9-349c7WDv6", "season": 2025})
    assert calls[-1][1]["season"] == 2025  # the runtime no longer subtracts


def test_filter_route_season_uuid_passes_through(served):
    routes, calls, _ = served
    routes["/sports-v2/season-series-game-types-filter"] = _Resp(
        _load("sports-v2__season-series-game-types-filter.json")
    )
    rt._get(f"{B}/sports-v2/season-series-game-types-filter", {"season": "xs4m9qupsi"})
    assert calls[-1][1] == {"season": "xs4m9qupsi"}


@pytest.mark.parametrize(("season", "game_type"), [(2028, "regular"), (1975, "regular"), (2026, "allstar")])
def test_unknown_season_or_game_type_is_no_data(served, season, game_type):
    with pytest.raises(NoDataError, match="layout-info covers 1976-2027"):
        rt.shl_ssgt_uuid(season, game_type)


def test_ssgt_helper_and_bad_season(served):
    assert rt.shl_ssgt_uuid(2026) == "iuzqg7dqk9"
    assert rt.shl_ssgt_uuid(2025) == "qeb-73qtP8d19"
    with pytest.raises(ValueError, match="END year"):
        rt.shl_ssgt_uuid("twenty")


# --------------------------------------------------------------------------- refusals


def test_truncated_stats_page_is_refused(served):
    routes, _, _ = served
    body = _load("statistics-v2__stats-info__module__players_summary.json")
    body[0]["stats"] = body[0]["stats"][:1] * rt.STATS_COUNT
    routes["/statistics-v2/stats-info/players_summary"] = _Resp(body)
    with pytest.raises(AssetFetchError, match="may be cut"):
        rt._get(f"{B}/statistics-v2/stats-info/players_summary", {"season": 2026})


def test_stats_page_short_of_its_total_count_is_refused(served):
    """count=1000 sent, 3 rows back while totalCount says 473: a cut answer, not a short table."""
    routes, _, _ = served
    routes["/statistics-v2/stats-info/players_summary"] = _Resp(
        _load("statistics-v2__stats-info__module__players_summary.json")
    )
    with pytest.raises(AssetFetchError, match="3 of totalCount=473"):
        rt._get(f"{B}/statistics-v2/stats-info/players_summary", {"season": 2026})


def test_stats_page_matching_its_total_count_passes(served):
    routes, _, _ = served
    routes["/statistics-v2/stats-info/goalkeepers_summary"] = _Resp(
        _load("full/statistics-v2__stats-info__goalkeepers_summary__iuzqg7dqk9.json")
    )
    assert len(rt._get(f"{B}/statistics-v2/stats-info/goalkeepers_summary", {"season": 2026})[0]["stats"]) == 36


def test_another_team_page_is_a_failed_fetch(served):
    """A team page for another uuid is a wrong answer (AssetFetchError), not "nothing here"."""
    routes, _, _ = served
    routes["/sports-v2/teams/zzzz-zzzzzzzzz"] = _Resp(_load("sports-v2__teams__team_uuid.json"))
    with pytest.raises(AssetFetchError, match="not the data asked for"):
        rt._get(f"{B}/sports-v2/teams/zzzz-zzzzzzzzz")


def test_empty_gameday_body_is_no_data(served):
    """Games before 2023/24 answer 200 with an empty body."""
    routes, _, _ = served
    routes["/gameday/play-by-play/qeb-old0game"] = _Resp(text="")
    with pytest.raises(NoDataError, match="2023/24"):
        rt._get(f"{B}/gameday/play-by-play/qeb-old0game")


@pytest.mark.parametrize("body", [{}, []])
def test_stub_body_is_no_data(served, body):
    routes, _, _ = served
    routes["/sports-v2/teams/087a-087aTQv9u"] = _Resp(body)
    with pytest.raises(NoDataError, match="empty body"):
        rt._get(f"{B}/sports-v2/teams/087a-087aTQv9u")


def test_zero_row_table_is_no_data(served):
    routes, _, _ = served
    routes["/sports-v2/upcoming-games/087a-087aTQv9u"] = _Resp({"upcomingGames": []})
    with pytest.raises(NoDataError, match="no rows"):
        rt._get(f"{B}/sports-v2/upcoming-games/087a-087aTQv9u")


def test_error_envelope_is_a_failed_fetch(served):
    routes, _, _ = served
    routes["/gameday/boxscore/bdhvuc5tex"] = _Resp({"statusCode": 500, "message": "Internal server error"})
    with pytest.raises(AssetFetchError, match="error envelope"):
        rt._get(f"{B}/gameday/boxscore/bdhvuc5tex")


def test_non_json_body_is_a_failed_fetch(served):
    routes, _, _ = served
    routes["/gameday/boxscore/bdhvuc5tex"] = _Resp(text="<!doctype html><html></html>")
    with pytest.raises(AssetFetchError, match="non-JSON"):
        rt._get(f"{B}/gameday/boxscore/bdhvuc5tex")


def test_another_game_is_refused(served):
    routes, _, _ = served
    routes["/gameday/play-by-play/zzzzzzzzzz"] = _Resp(_load("gameday__play-by-play__game_uuid.json"))
    with pytest.raises(AssetFetchError, match="gameUuid='bdhvuc5tex'"):
        rt._get(f"{B}/gameday/play-by-play/zzzzzzzzzz")


def test_body_without_its_scope_field_is_unverifiable(served):
    routes, _, _ = served
    body = _load("gameday__game-overview__game_uuid.json")
    del body["gameUuid"]
    routes["/gameday/game-overview/bdhvuc5tex"] = _Resp(body)
    with pytest.raises(AssetFetchError, match="cannot be verified"):
        rt._get(f"{B}/gameday/game-overview/bdhvuc5tex")


def test_matching_scope_passes(served):
    routes, _, _ = served
    routes["/gameday/play-by-play/bdhvuc5tex"] = _Resp(_load("gameday__play-by-play__game_uuid.json"))
    assert len(rt._get(f"{B}/gameday/play-by-play/bdhvuc5tex")) == 6


@pytest.mark.parametrize(("status", "error"), [(404, NoDataError), (400, ValueError), (500, AssetFetchError)])
def test_non_200_follows_the_error_vocabulary(served, status, error):
    routes, _, _ = served
    routes["/sports-v2/teams/087a-087aTQv9u"] = _Resp({"message": "x"}, status=status)
    with pytest.raises(error):
        rt._get(f"{B}/sports-v2/teams/087a-087aTQv9u")


def test_provider_text_route(served):
    routes, _, waits = served
    routes["/sports-v2/ssgt/iuzqg7dqk9/provider"] = _Resp(text="statnet")
    assert rt._get(f"{B}/sports-v2/ssgt/iuzqg7dqk9/provider") == "statnet"
    routes["/sports-v2/ssgt/iuzqg7dqk9/provider"] = _Resp(text="<!doctype html><html>app shell</html>")
    with pytest.raises(AssetFetchError, match="not a provider name"):
        rt._get(f"{B}/sports-v2/ssgt/iuzqg7dqk9/provider")
    assert len(waits) == 2


def test_player_crosswalk_is_one_stats_info_call(served):
    routes, calls, _ = served
    body = _load("statistics-v2__stats-info__module__players_summary.json")
    body[0]["totalCount"] = len(body[0]["stats"])  # the committed capture is cut to 3 rows
    routes["/statistics-v2/stats-info/players_summary"] = _Resp(body)
    xw = rt.shl_player_crosswalk(2026)
    assert [u.rsplit("/", 1)[-1] for u, _ in calls] == ["layout-info", "players_summary"]
    assert xw.schema["player_id"] == pl.Int64 and xw.height == 3


def test_every_attempt_retries_included_waits_its_turn(monkeypatch: pytest.MonkeyPatch):
    """Real ``download()`` over ``SESSION``: a 503 then a 200 is two polite waits, one per attempt."""
    from sportsdataverse import dl_utils
    from sportsdataverse.hockey import euro_hockey_runtime as ehr

    class _Wire:
        def __init__(self, status: int, body: bytes) -> None:
            self.status_code, self.content, self.text = status, body, body.decode()
            self.headers = {"Content-Type": "application/json"}
            self.url = f"{B}/sports-v2/teams/087a-087aTQv9u"

        def json(self) -> Any:
            return json.loads(self.content)

    team = json.dumps(_load("sports-v2__teams__team_uuid.json")).encode()
    answers = iter([_Wire(503, b"{}"), _Wire(200, team)])
    waits: List[str] = []
    monkeypatch.setattr(ehr, "_polite_wait", waits.append)
    monkeypatch.setattr(dl_utils.time, "sleep", lambda s: None)
    monkeypatch.setattr(ehr.requests.Session, "request", lambda self, method, url, *a, **k: next(answers))
    assert rt._get(f"{B}/sports-v2/teams/087a-087aTQv9u")["uuid"] == "087a-087aTQv9u"
    assert len(waits) == 2
