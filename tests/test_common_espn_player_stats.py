"""Offline tests for the core-v2 season stat pivot behind the 8 ``espn_<lg>_player_stats`` wrappers (B-M33).

Driven through the real ``dl_utils.download`` (only ``requests.Session.get`` is replaced) on the
committed capture ``tests/fixtures/espn/athlete_statistics_core_nba_1966_2024.json`` (LeBron
James, 2023-24 regular season). Every expected value is read from that payload.
"""

from __future__ import annotations

import json
from pathlib import Path

import polars as pl
import pytest
import requests

from sportsdataverse.dl_utils import underscore
from sportsdataverse.errors import AssetFetchError
from tests.codegen.test_runtime_errors import _route

FIX = Path(__file__).resolve().parent / "fixtures"
STATS = (FIX / "espn" / "athlete_statistics_core_nba_1966_2024.json").read_text(encoding="utf-8")
NOT_FOUND = (FIX / "runtime_errors" / "espn_core_event_404.json").read_text(encoding="utf-8")


@pytest.fixture(autouse=True)
def _two_retries(monkeypatch):
    monkeypatch.setenv("SDV_PY_HTTP_RETRIES", "2")


def _serve(monkeypatch, athlete_status: int = 404, athlete_body: str = NOT_FOUND, stats: str = STATS):
    def answer(url: str):
        if "/statistics" in url:
            return (200, stats, "application/json")
        return (athlete_status, athlete_body, "application/json")

    return _route(monkeypatch, answer)


def _lebron() -> pl.DataFrame:
    from sportsdataverse.nba import espn_nba_player_stats

    return espn_nba_player_stats(athlete_id=1966, season=2024)


def test_stat_pivot_matches_every_stat_in_the_capture(monkeypatch):
    calls = _serve(monkeypatch)
    df = _lebron()
    assert df.height == 1
    assert calls[0].endswith("/seasons/2024/types/2/athletes/1966/statistics/")
    stats = [(c["name"], s) for c in json.loads(STATS)["splits"]["categories"] for s in c["stats"]]
    assert len(stats) == 112
    for cat, stat in stats:
        col = underscore(f"{cat}_{stat['name']}")
        assert df.schema[col] == pl.Float64, col
        assert df[col][0] == pytest.approx(float(stat["value"])), col


def test_stat_pivot_takes_value_not_display_value(monkeypatch):
    # pinned from the capture: displayValue "685/1269" and "59.9" must not replace the numbers
    _serve(monkeypatch)
    row = _lebron().row(0, named=True)
    assert row["offensive_field_goals"] == pytest.approx(0.539795)
    assert row["offensive_effective_fg_pct"] == pytest.approx(59.85)
    assert row["offensive_assists"] == 589.0
    assert row["defensive_blocks"] == 38.0
    assert row["defensive_steals"] == 89.0
    assert row["general_fouls"] == 78.0


def test_identity_columns_on_a_404_athlete_deref(monkeypatch):
    # the statistics node is authoritative; a 404 on the athlete node leaves identity null
    _serve(monkeypatch)
    row = _lebron().row(0, named=True)
    assert row["athlete_id"] == 1966
    assert (row["season"], row["season_type"], row["total"]) == (2024, "regular", False)
    assert row["full_name"] is None and row["team_id"] is None


@pytest.mark.parametrize("status", [403, 429, 503, "connection"])
def test_safe_json_raises_on_a_failed_athlete_deref(monkeypatch, status):
    """B-M1: only a 404 on the athlete node may leave identity null; a failed fetch raises."""
    if status == "connection":
        _route(
            monkeypatch,
            lambda url: (200, STATS, "application/json") if "/statistics" in url else requests.ConnectionError("reset"),
        )
    else:
        _serve(monkeypatch, athlete_status=status, athlete_body='{"message": "unavailable"}')
    with pytest.raises(AssetFetchError):
        _lebron()


@pytest.mark.parametrize("value", [float("nan"), "nan", " NaN "])
def test_coerce_float_maps_nan_to_null(value):
    """B-GB-20: a NaN is a missing value, not a float that stringifies as "nan"."""
    from sportsdataverse._common_espn_player_stats import _coerce_float

    assert _coerce_float(value) is None
    assert _coerce_float(" 7.8 ") == 7.8 and _coerce_float(3) == 3.0 and _coerce_float("7.8-15.7") is None


def test_stat_without_a_number_is_a_null_float_not_its_display_value(monkeypatch):
    """B-L20: the real capture with one stat's value withheld: the column stays Float64 (null), not "--"."""
    payload = json.loads(STATS)
    stat = payload["splits"]["categories"][0]["stats"][0]
    stat["value"], stat["displayValue"] = None, "--"
    _serve(monkeypatch, stats=json.dumps(payload))
    col = underscore(f"{payload['splits']['categories'][0]['name']}_{stat['name']}")
    df = _lebron()
    assert df.schema[col] == pl.Float64 and df[col][0] is None
