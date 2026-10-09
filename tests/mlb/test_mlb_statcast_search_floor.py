"""mlb_statcast_search clamps pre-tracking windows (rules/mlb.yaml#mlb-2008-pitchfx-pitch-tracking)."""

import polars as pl
import pytest

from sportsdataverse.mlb import mlb_statcast_extra as m


def test_start_before_floor_is_clamped_with_warning(monkeypatch: pytest.MonkeyPatch) -> None:
    seen: dict = {}

    def fake_core(start_dt, end_dt, base_url, label, **kw):
        seen.update(start_dt=start_dt, end_dt=end_dt)
        return pl.DataFrame({"game_pk": []})

    monkeypatch.setattr(m, "_search_core", fake_core)
    with pytest.warns(UserWarning, match="Statcast pitch tracking starts 2008-03-25; start_dt='2005-04-01' clamped"):
        m.mlb_statcast_search("2005-04-01", "2008-04-10")
    assert seen == {"start_dt": m.STATCAST_SEARCH_FLOOR, "end_dt": "2008-04-10"}


def test_start_at_or_after_floor_passes_through(monkeypatch: pytest.MonkeyPatch) -> None:
    seen: dict = {}

    def fake_core(start_dt, end_dt, base_url, label, **kw):
        seen.update(start_dt=start_dt)
        return pl.DataFrame({"game_pk": []})

    monkeypatch.setattr(m, "_search_core", fake_core)
    m.mlb_statcast_search("2015-04-05", "2015-04-06")  # no warning (warnings are errors in pytest.ini)
    assert seen["start_dt"] == "2015-04-05"
