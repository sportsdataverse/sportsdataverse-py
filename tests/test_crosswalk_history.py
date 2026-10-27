"""Offline tests for the Fox / Yahoo / CBS history harvester (real captured payloads)."""

from __future__ import annotations

import gzip
import json
from datetime import date
from pathlib import Path
from typing import Any, Dict, List

import pytest

from sportsdataverse import _crosswalk_history as xh
from sportsdataverse.errors import AssetFetchError

FIX = Path(__file__).parent / "fixtures" / "crosswalk_history"


def _load(name: str) -> Any:
    return json.loads((FIX / name).read_text())


@pytest.fixture(autouse=True)
def _fast(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("XWALK_RPS", "1000")
    monkeypatch.setattr(xh, "_today", lambda: date(2026, 10, 11))


def test_season_windows_follow_sdv_season_labels() -> None:
    assert xh.season_window("nba", 2025) == (date(2024, 10, 15), date(2025, 6, 30))
    assert xh.season_window("nba", 2021)[0] == date(2020, 12, 10)  # the late COVID start
    assert xh.season_window("mbb", 2025) == (date(2024, 11, 1), date(2025, 4, 12))
    assert xh.season_window("wnba", 2025) == (date(2025, 5, 1), date(2025, 10, 31))
    assert xh.season_window("nfl", 2024)[1] == date(2025, 2, 20)


def test_et_date_converts_utc_to_eastern() -> None:
    assert xh._et_date("2024-11-15T00:30:00Z") == date(2024, 11, 14)  # 7:30 pm ET the day before
    assert xh._et_date("2024-10-22T18:30:00-05:00") == date(2024, 10, 22)
    assert xh._et_date("20250617") == date(2025, 6, 17)
    assert xh._et_date(None) is None


def test_store_is_atomic_refuses_empty_and_remembers_misses(tmp_path: Path) -> None:
    store = xh.Store(tmp_path, "fox", "nba")
    with pytest.raises(ValueError):
        store.put("event_data", 2025, "1", {}, gz=True)
    assert not store.has("event_data", 2025, "1")
    p = store.put("event_data", 2025, "1", {"header": {"id": 1}}, gz=True)
    assert p.name == "1.json.gz" and json.loads(gzip.decompress(p.read_bytes())) == {"header": {"id": 1}}
    assert store.has("event_data", 2025, "1") and store.load("event_data", 2025, "1") == {"header": {"id": 1}}
    assert not list(p.parent.glob(".*tmp*"))
    store.miss("event_data", 2025, "2", "404")
    assert xh.Store(tmp_path, "fox", "nba").has("event_data", 2025, "2")  # read back from _missing.tsv


def test_fox_events_keep_only_in_window_games() -> None:
    raw = _load("fox_nba_scores_segment_20241114.json")
    games, dropped = xh._fox_events(raw, xh.season_window("nba", 2025))
    assert games and dropped == 0
    assert all(g["date"] == "2024-11-14" and g["final"] for g in games)
    # the same answer offered for another season is the current-season trap: all dropped
    games, dropped = xh._fox_events(raw, xh.season_window("nba", 2019))
    assert games == [] and dropped > 0


def test_fox_listing_resumes_without_requests(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    import sportsdataverse.providers.fox.fox_api as fox_api

    raw = _load("fox_nba_scores_segment_20241114.json")
    calls: List[str] = []

    def fake(sport: str, segment_id: str, **kwargs: Any) -> Dict[str, Any]:
        calls.append(segment_id)
        return raw if segment_id == "20241114" else {}

    monkeypatch.setattr(fox_api, "fox_api_league_scores_segment", fake)
    monkeypatch.setattr(xh, "_sweep_dates", lambda window: [date(2024, 11, 13), date(2024, 11, 14)])
    store = xh.Store(tmp_path, "fox", "nba")
    games = xh.fox_listing(store, xh.Pacer("fox"), "nba", 2025, refresh=False)
    assert calls == ["20241113", "20241114"] and games
    assert store.missing("scores_segment", 2025) == {"20241113": "no-events"}
    pacer = xh.Pacer("fox")
    assert xh.fox_listing(xh.Store(tmp_path, "fox", "nba"), pacer, "nba", 2025, refresh=False) == games
    assert pacer.n == 0 and calls == ["20241113", "20241114"]


def test_yahoo_listing_games_and_box_checks() -> None:
    listed = xh._yahoo_games(_load("yahoo_nba_game_ids_by_date_20250617_20250623.json"))
    assert listed and all(g["gameId"].startswith("nba.g.2025") for g in listed)
    box = _load("yahoo_nba_playbook_boxscore_2024102202_trimmed.json")
    window = xh.season_window("nba", 2025)
    assert xh._payload_problem("yahoo", "nba", "playbook_boxscore", box, "nba.g.2024102202", 2025, window) is None
    # Yahoo keys NBA 2024-25 as season 2024; the same payload filed under 2026 is a mismatch
    problem = xh._payload_problem("yahoo", "nba", "playbook_boxscore", box, "nba.g.2024102202", 2026, window)
    assert problem == "season-mismatch:2024"
    assert xh.box_players("yahoo", box) == 5


def test_yahoo_cfb_lists_one_date_at_a_time(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    import sportsdataverse.providers.yahoo.yahoo_shangrila as shangrila

    calls: List[Dict[str, Any]] = []

    def fake(**kwargs: Any) -> Dict[str, Any]:
        calls.append(kwargs)
        day = kwargs["dates"]
        game = {"gameId": f"ncaaf.g.{day.replace('-', '')}01", "startTime": f"{day}T12:00:00-05:00", "status": "FINAL"}
        return {"data": {"leagues": [{"games": [game]}]}}

    monkeypatch.setattr(shangrila, "yahoo_league_game_ids_by_date", fake)
    monkeypatch.setattr(xh, "_sweep_dates", lambda window: [date(2024, 11, 29), date(2024, 11, 30)])
    store = xh.Store(tmp_path, "yahoo", "cfb")
    games = xh.yahoo_listing(store, xh.Pacer("yahoo"), "cfb", 2024, refresh=False)
    # ncaaf ignores startRange/endRange: one dates= request per day, no range parameters
    assert [c["dates"] for c in calls] == ["2024-11-29", "2024-11-30"]
    assert not any("start_range" in c or "end_range" in c for c in calls)
    assert sorted(g["date"] for g in games) == ["2024-11-29", "2024-11-30"]
    assert store.has("game_ids_by_date", 2024, "20241130")


def test_yahoo_cfb_by_date_ignores_a_stored_range_listing(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """A range page stored by the old code is never read for a by-date league: both days are fetched."""
    import sportsdataverse.providers.yahoo.yahoo_shangrila as shangrila

    calls: List[str] = []

    def fake(**kwargs: Any) -> Dict[str, Any]:
        calls.append(kwargs["dates"])
        day = kwargs["dates"]
        game = {"gameId": f"ncaaf.g.{day.replace('-', '')}01", "startTime": f"{day}T12:00:00-05:00", "status": "FINAL"}
        return {"data": {"leagues": [{"games": [game]}]}}

    monkeypatch.setattr(shangrila, "yahoo_league_game_ids_by_date", fake)
    monkeypatch.setattr(xh, "_sweep_dates", lambda window: [date(2024, 11, 29), date(2024, 11, 30)])
    store = xh.Store(tmp_path, "yahoo", "cfb")
    stale = {"gameId": "ncaaf.g.2024082401", "startTime": "2024-08-24T12:00:00-05:00", "status": "FINAL"}
    store.put("game_ids_by_date", 2024, "20241129_20241130", {"data": {"leagues": [{"games": [stale]}]}}, gz=False)
    xh.yahoo_listing(store, xh.Pacer("yahoo"), "cfb", 2024, refresh=False)
    assert calls == ["2024-11-29", "2024-11-30"]


def test_yahoo_cfb_off_date_listing_is_not_stored(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """If Yahoo ignores ``dates=`` and answers with another day's games, nothing is filed under that day."""
    import sportsdataverse.providers.yahoo.yahoo_shangrila as shangrila

    def fake(**kwargs: Any) -> Dict[str, Any]:  # always the season opener, whatever date was asked for
        game = {"gameId": "ncaaf.g.2024082401", "startTime": "2024-08-24T12:00:00-05:00", "status": "FINAL"}
        return {"data": {"leagues": [{"games": [game]}]}}

    monkeypatch.setattr(shangrila, "yahoo_league_game_ids_by_date", fake)
    monkeypatch.setattr(xh, "_sweep_dates", lambda window: [date(2024, 11, 30)])
    store = xh.Store(tmp_path, "yahoo", "cfb")
    games = xh.yahoo_listing(store, xh.Pacer("yahoo"), "cfb", 2024, refresh=False)
    assert games == []
    assert not store.has("game_ids_by_date", 2024, "20241130")


def test_cbs_envelope_and_box_players() -> None:
    window = xh.season_window("nfl", 2024)
    envelope = _load("cbs_cfb_game_boxscore_2168097_404_envelope.json")
    assert xh._payload_problem("cbs", "cfb", "game_boxscore", envelope, "2168097", 2015, window) == "cbs-404-envelope"
    box = _load("cbs_nfl_game_boxscore_29375848_trimmed.json")
    assert xh._payload_problem("cbs", "nfl", "game_boxscore", box, "29375848", 2024, window) is None
    assert xh.box_players("cbs", box) >= 4


def test_pacer_stops_on_403_and_keeps_going_on_404(monkeypatch: pytest.MonkeyPatch) -> None:
    from sportsdataverse.errors import NoDataError

    pacer = xh.Pacer("cbs")

    def missing() -> Any:
        raise NoDataError("gone")

    assert pacer.call("x", missing) == ("none", None)

    def blocked() -> Any:
        raise AssetFetchError("api.cbssports.com/napi answered HTTP 403 with a non-JSON body")

    with pytest.raises(xh.HarvestBlocked):
        pacer.call("y", blocked)


def test_pacer_rate_comes_from_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("XWALK_RPS")
    assert xh.Pacer("cbs").rps == 0.5 and xh.Pacer("fox").rps == 1.0
    monkeypatch.setenv("XWALK_YAHOO_RPS", "0.8")
    assert xh.Pacer("yahoo").rps == 0.8


def test_fox_box_season_check_and_players() -> None:
    box = _load("fox_nba_event_data_42022_trimmed.json")
    assert xh._payload_problem("fox", "nba", "event_data", box, "42022", 2025, xh.season_window("nba", 2025)) is None
    problem = xh._payload_problem("fox", "nba", "event_data", box, "42022", 2019, xh.season_window("nba", 2019))
    assert problem == "season-mismatch:2024-10-15"
    assert xh.box_players("fox", box) == 25


def test_yahoo_cfb_season_page_with_the_day_is_still_rejected(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """A season page that also holds the asked-for day's games is not a listing of that day, nor is a capped one."""
    import sportsdataverse.providers.yahoo.yahoo_shangrila as shangrila

    def season_page(**kwargs: Any) -> Dict[str, Any]:
        days = ["2024-08-31", "2024-09-07", "2024-09-14"]  # the asked-for day plus later ones
        games = [
            {"gameId": f"ncaaf.g.{d.replace('-', '')}01", "startTime": f"{d}T12:00:00-05:00", "status": "FINAL"}
            for d in days
        ]
        return {"data": {"leagues": [{"games": games}]}}

    monkeypatch.setattr(shangrila, "yahoo_league_game_ids_by_date", season_page)
    monkeypatch.setattr(xh, "_sweep_dates", lambda window: [date(2024, 8, 31)])
    store = xh.Store(tmp_path, "yahoo", "cfb")
    assert xh.yahoo_listing(store, xh.Pacer("yahoo"), "cfb", 2024, refresh=False) == []
    assert not store.has("game_ids_by_date", 2024, "20240831")

    capped = {"gameId": "ncaaf.g.2024083101", "startTime": "2024-08-31T12:00:00-05:00", "status": "FINAL"}
    assert not xh._yahoo_answers_date(
        {"data": {"leagues": [{"games": [capped] * xh._YAHOO_ROW_CAP}]}}, date(2024, 8, 31)
    )
