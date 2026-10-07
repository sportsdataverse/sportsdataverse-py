"""``sportsdataverse.soccer.soccer_events`` -- kloppy behind the optional ``soccer`` extra.

Offline tests load the trimmed real StatsBomb match under ``tests/fixtures/kloppy/`` through
kloppy itself; the one live test reads StatsBomb open data over the network.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any

import pandas as pd
import polars as pl
import pytest

statsbomb = pytest.importorskip("kloppy.statsbomb")

import sportsdataverse
from sportsdataverse import soccer
from sportsdataverse.dl_utils import underscore
from sportsdataverse.soccer import soccer_events_to_frame, soccer_open_dataset, soccer_open_events
from tests.conftest import skip_if_no_live

FIXTURES = Path(__file__).resolve().parents[1] / "fixtures" / "kloppy"
EVENTS = FIXTURES / "statsbomb_8658_events.json"
LINEUPS = FIXTURES / "statsbomb_8658_lineups.json"
RAW_EVENT_COUNT = 57  # trim_statsbomb_8658.py: 50 head + 4 half boundaries + the first shot (2 overlap)
CORE_COLUMNS = {
    "event_id",
    "event_type",
    "period_id",
    "timestamp",
    "team_id",
    "player_id",
    "coordinates_x",
    "coordinates_y",
}


def _dataset(**kwargs: Any) -> Any:
    return statsbomb.load(event_data=str(EVENTS), lineup_data=str(LINEUPS), **kwargs)


@pytest.fixture(scope="module")
def raw_events() -> list:
    return json.loads(EVENTS.read_text(encoding="utf-8"))


def test_exported_from_soccer_and_the_top_level_package() -> None:
    assert soccer.soccer_open_events is soccer_open_events
    assert soccer.soccer_events_to_frame is soccer_events_to_frame
    assert soccer.soccer_open_dataset is soccer_open_dataset
    assert sportsdataverse.soccer_open_dataset is soccer_open_dataset
    assert sportsdataverse.soccer_open_events is soccer_open_events
    assert sportsdataverse.soccer_events_to_frame is soccer_events_to_frame


def test_missing_kloppy_is_a_clear_import_error(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setitem(sys.modules, "kloppy", None)
    with pytest.raises(ImportError, match=r"sportsdataverse\[soccer\]"):
        soccer_open_events("statsbomb", 8658)


def test_unknown_provider_lists_the_supported_keys() -> None:
    with pytest.raises(ValueError, match=r"'opta'.*statsbomb"):
        soccer_open_events("opta", 1)


def test_frame_is_polars_snake_case_one_row_per_event(raw_events: list) -> None:
    assert len(raw_events) == RAW_EVENT_COUNT
    ds = _dataset()
    df = soccer_events_to_frame(ds)
    assert isinstance(df, pl.DataFrame)
    assert df.height == len(ds.events) >= RAW_EVENT_COUNT
    assert {e["id"] for e in raw_events} <= set(df["event_id"].to_list())
    assert CORE_COLUMNS <= set(df.columns)
    assert all(c == underscore(c) for c in df.columns), df.columns
    assert df.schema["coordinates_x"] == pl.Float64
    assert df.schema["period_id"] == pl.Int64
    assert df.schema["player_id"] == pl.String


def test_first_shot_coordinates_match_the_raw_json(raw_events: list) -> None:
    shot = next(e for e in raw_events if e["type"]["name"] == "Shot")
    raw_x, raw_y = shot["location"]
    # Provider units: kloppy centres StatsBomb's 1-yard cells, so raw [112, 49] reads (111.5, 48.5).
    row = (
        soccer_events_to_frame(_dataset(coordinates="statsbomb"))
        .filter(pl.col("event_id") == shot["id"])
        .row(0, named=True)
    )
    assert row["event_type"] == "SHOT"
    assert (row["coordinates_x"], row["coordinates_y"]) == (raw_x - 0.5, raw_y - 0.5)
    assert row["player_id"] == str(shot["player"]["id"])
    assert row["team_id"] == str(shot["team"]["id"])
    # kloppy's default system is a 0-1 pitch, but its StatsBomb mapping is piecewise (penalty-area
    # anchored, not x/120), so only the provider-unit path above is checked against the raw JSON.
    default = soccer_events_to_frame(_dataset()).filter(pl.col("event_id") == shot["id"]).row(0, named=True)
    assert 0.0 <= default["coordinates_x"] <= 1.0 and 0.0 <= default["coordinates_y"] <= 1.0
    assert default["coordinates_x"] > 0.9  # still the attacking end


def test_return_as_pandas() -> None:
    ds = _dataset()
    pdf = soccer_events_to_frame(ds, return_as_pandas=True)
    assert isinstance(pdf, pd.DataFrame)
    assert len(pdf) == len(ds.events)
    assert list(pdf.columns) == soccer_events_to_frame(ds).columns


def test_non_snake_case_columns_are_renamed() -> None:
    class _Dataset:
        def to_df(self, engine: str) -> pl.DataFrame:
            assert engine == "polars"
            return pl.DataFrame({"eventId": ["a"], "coordinates_x": [1.0]})

    assert soccer_events_to_frame(_Dataset()).columns == ["event_id", "coordinates_x"]


def test_open_events_dispatches_to_kloppy_and_forwards_kwargs(monkeypatch: pytest.MonkeyPatch) -> None:
    seen: dict = {}

    def fake_load_open_data(**kwargs: Any) -> Any:
        seen.update(kwargs)
        return _dataset(coordinates=kwargs["coordinates"])

    monkeypatch.setattr(statsbomb, "load_open_data", fake_load_open_data)
    df = soccer_open_events("StatsBomb", 8658)  # coordinates default to the provider's units
    assert seen == {"match_id": 8658, "coordinates": "statsbomb"}
    assert isinstance(df, pl.DataFrame)
    # kloppy synthesizes rows (a BALL_OUT after the pass that went out, ...); count what it loaded
    assert df.height == len(_dataset(coordinates="statsbomb").events)
    assert df["coordinates_x"].max() > 1.0  # provider units survived the kwarg


@skip_if_no_live
def test_live_statsbomb_open_data_match_8658() -> None:
    df = soccer_open_events("statsbomb", 8658)
    assert isinstance(df, pl.DataFrame)
    assert df.height > 1000
    assert CORE_COLUMNS <= set(df.columns)
    assert df.filter(pl.col("event_type") == "SHOT").height > 10
