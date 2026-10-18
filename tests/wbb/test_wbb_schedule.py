from pathlib import Path

from sportsdataverse.wbb.wbb_schedule import espn_wbb_calendar, most_recent_wbb_season
from tests.codegen.test_runtime_errors import _route

CAPTURE = Path(__file__).resolve().parents[1] / "fixtures" / "espn" / "scoreboard_wbb.json"


def test_wbb_calendar_season_is_an_int(monkeypatch):
    """B-L9 / R855-7: ``season=None`` means the current season and ``2024.0`` is requested as ``2024``."""
    body = CAPTURE.read_text(encoding="utf-8")
    calls = _route(monkeypatch, lambda url: (200, body, "application/json"))
    cal = espn_wbb_calendar(season=2024.0)
    espn_wbb_calendar()
    assert calls[0].endswith("dates=2024")
    assert calls[1].endswith(f"dates={most_recent_wbb_season()}")
    assert cal.height == 116  # the capture's leagues[0].calendar
