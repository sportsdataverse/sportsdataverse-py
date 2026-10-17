"""espn_wbb_calendar: ESPN serves a women's college basketball calendar from the 2000-01 season (2001)."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from sportsdataverse.errors import SeasonNotFoundError
from sportsdataverse.wbb import wbb_schedule

FIX = Path(__file__).parent / "fixtures" / "espn" / "wbb_calendar_2001.json"


def test_season_2001_is_served(monkeypatch):
    monkeypatch.setattr(wbb_schedule, "_download_json", lambda *a, **k: json.loads(FIX.read_text()))
    cal = wbb_schedule.espn_wbb_calendar(season=2001)
    assert cal.height == 125
    assert (cal["date"].min(), cal["date"].max()) == ("2000-11-10", "2001-04-01")


def test_season_2000_is_refused_before_any_request(monkeypatch):
    def boom(*a, **k):
        raise AssertionError("no request expected")

    monkeypatch.setattr(wbb_schedule, "_download_json", boom)
    with pytest.raises(SeasonNotFoundError, match="2001"):
        wbb_schedule.espn_wbb_calendar(season=2000)
