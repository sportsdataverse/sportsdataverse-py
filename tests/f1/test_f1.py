"""Offline tests for the Jolpica F1 (``f1``) flat-API family, plus one gated live smoke.

Asserts the parser against the real trimmed captures in ``tests/fixtures/f1/`` (byte
copies of the recon captures, never synthetic payloads), the generated endpoint YAML +
returns-schemas, the empty/malformed contract, the generated wrappers' URL building and
the hand-written ``f1_laps`` paging loop through a fake transport. No network except the
``@skip_if_no_live`` smoke (2 requests against a 500/hour shared budget).
"""

from __future__ import annotations

import json
import math
import re
from pathlib import Path
from typing import Any, Dict, List

import polars as pl
import pytest
import yaml

from sportsdataverse.f1 import f1 as f1_mod
from sportsdataverse.f1 import f1_extra as laps_mod
from sportsdataverse.f1.f1_parsers import parse_f1_mrdata
from tests.conftest import skip_if_no_live

FIXTURES = Path(__file__).parents[1] / "fixtures" / "f1"
YAML_PATH = Path(__file__).parents[2] / "tools" / "codegen" / "endpoints" / "f1.yaml"
SCHEMA_DIR = Path(__file__).parents[2] / "tools" / "codegen" / "schemas" / "native" / "f1"

_STEMS = sorted(p.stem for p in FIXTURES.glob("*.json"))

# fixture stem -> endpoint short (the schema whose documented columns it must parse to)
_SHORT_OF = {
    "seasons": "seasons",
    "2024": "schedule",
    "current": "schedule",
    "2024__1": "race",
    "2024__1__results": "results",
    "2024__1__qualifying": "qualifying",
    "2024__5__sprint": "sprint",
    "2024__1__laps": "laps_page",
    "2024__1__pitstops": "pitstops",
    "2024__driverStandings": "driver_standings",
    "2024__constructorStandings": "constructor_standings",
    "2024__drivers": "drivers",
    "2024__constructors": "constructors",
    "circuits": "circuits",
    "status": "status",
    "drivers__max_verstappen": "driver",
}

_ID = re.compile(r"(^|_)id$")
_SNAKE = re.compile(r"^[a-z0-9_]+$")


def _load(stem: str) -> Any:
    return json.loads((FIXTURES / f"{stem}.json").read_text(encoding="utf-8"))


def _yaml() -> Dict[str, Any]:
    return yaml.safe_load(YAML_PATH.read_text(encoding="utf-8"))


def _schema_columns(short: str) -> List[str]:
    doc = yaml.safe_load((SCHEMA_DIR / f"{short}.yaml").read_text(encoding="utf-8"))
    return [c["name"] for c in doc["columns"]]


class _Recorder:
    """Stand-in for the runtime ``_get`` that records every URL + params and answers a payload."""

    def __init__(self, payload: Any = None) -> None:
        self.payload = payload if payload is not None else {"MRData": {"total": "0", "RaceTable": {"Races": []}}}
        self.calls: List[Dict[str, Any]] = []

    def __call__(self, url: str, params: Any = None, **kwargs: Any) -> Any:
        self.calls.append({"url": url, "params": dict(params or {})})
        return self.payload


@pytest.fixture()
def recorder(monkeypatch: pytest.MonkeyPatch) -> _Recorder:
    rec = _Recorder()
    monkeypatch.setattr(f1_mod, "_get", rec)
    return rec


# ---------------------------------------------------------------------------
# parse_f1_mrdata -- every committed capture
# ---------------------------------------------------------------------------


def test_every_fixture_is_mapped_to_a_schema() -> None:
    assert set(_STEMS) == set(_SHORT_OF)


@pytest.mark.parametrize("stem", _STEMS)
def test_every_fixture_parses_to_the_documented_columns(stem: str) -> None:
    raw = _load(stem)
    assert set(raw) == {"MRData"}, "capture no longer carries the envelope"
    df = parse_f1_mrdata(raw)
    assert isinstance(df, pl.DataFrame)
    assert df.height > 0
    documented = _schema_columns(_SHORT_OF[stem])
    # the 3-row schedule sample has no sprint weekend; the schema unions it with ``current``
    assert set(df.columns) <= set(documented)
    assert [c for c in documented if c in df.columns] == df.columns, "column order drifted from the schema"
    for col in df.columns:
        assert _SNAKE.match(col), f"{stem}.{col} is not snake_case"
        if _ID.search(col):
            assert df.schema[col] == pl.String, f"{stem}.{col} must stay Utf8"
    # the envelope's paging metadata never leaks into the rows
    assert not {"limit", "offset", "total", "xmlns", "series", "url_1"} & set(df.columns)


def test_results_row_shape_and_casts() -> None:
    df = parse_f1_mrdata(_load("2024__1__results"))
    assert df.height == 3  # trimmed capture; MRData.total still says 20
    assert df["race_name"][0] == "Bahrain Grand Prix"
    assert df.schema["season"] == pl.Int64 and df["season"][0] == 2024
    assert df.schema["position"] == pl.Int64 and df["position"].to_list() == [1, 2, 3]
    assert df.schema["points"] == pl.Float64 and df["points"][0] == 26.0
    assert df.schema["time_millis"] == pl.Int64 and df["time_millis"][0] == 5504742
    assert df.schema["fastest_lap"] == pl.Int64 and df["fastest_lap"][0] == 39
    assert df.schema["race_circuit_location_lat"] == pl.Float64
    assert df["driver_id"][0] == "max_verstappen" and df["constructor_id"][0] == "red_bull"
    assert df["fastest_lap_time"][0] == "1:32.608" and df["time"][0] == "1:31:44.742"


def test_laps_carry_the_lap_number_onto_each_timing() -> None:
    df = parse_f1_mrdata(_load("2024__1__laps"))
    assert df.height == 9  # 3 laps x 3 timings
    assert df["lap_number"].to_list() == [1, 1, 1, 2, 2, 2, 3, 3, 3]
    assert df["driver_id"][0] == "max_verstappen" and df["time"][0] == "1:37.284"
    assert df["race_circuit_id"].unique().to_list() == ["bahrain"]


def test_driver_standings_keep_constructors_as_a_json_cell() -> None:
    df = parse_f1_mrdata(_load("2024__driverStandings"))
    assert df.height == 3
    assert df["round"][0] == 24
    cell = json.loads(df["constructors"][0])
    assert cell[0]["constructorId"] == "red_bull"
    assert df.schema["wins"] == pl.Int64 and df["wins"][0] == 9


def test_single_driver_route_carries_the_table_level_id_first() -> None:
    df = parse_f1_mrdata(_load("drivers__max_verstappen"))
    assert df.columns[0] == "driver_id" and df["driver_id"][0] == "max_verstappen"
    assert df.schema["permanent_number"] == pl.Int64


def test_status_id_is_a_utf8_join_key_not_an_integer() -> None:
    df = parse_f1_mrdata(_load("status"))
    assert df.schema["status_id"] == pl.String and df["status_id"][0] == "1"
    assert df.schema["count"] == pl.Int64


def test_table_argument_stops_the_descent() -> None:
    df = parse_f1_mrdata(_load("2024__1__results"), table="Races")
    assert df.height == 1
    assert {"race_name", "circuit_id", "results"} <= set(df.columns)
    assert json.loads(df["results"][0])[0]["Driver"]["driverId"] == "max_verstappen"


@pytest.mark.parametrize(
    "raw",
    [
        None,
        {},
        [],
        "x",
        {"MRData": {}},
        {"MRData": {"total": "0", "RaceTable": {"season": "2024", "round": "3", "Races": []}}},  # non-sprint weekend
        {"MRData": {"RaceTable": {"Races": [None]}}},
    ],
)
def test_empty_payload_is_zero_row_frame(raw: Any) -> None:
    df = parse_f1_mrdata(raw)
    assert isinstance(df, pl.DataFrame)
    assert df.height == 0


def test_return_as_pandas() -> None:
    pdf = parse_f1_mrdata(_load("circuits"), return_as_pandas=True)
    assert type(pdf).__module__.startswith("pandas")
    assert len(pdf) == 3


# ---------------------------------------------------------------------------
# generated endpoint YAML + schemas (gen_f1.py output)
# ---------------------------------------------------------------------------


def test_yaml_lists_every_spec_route() -> None:
    doc = _yaml()
    assert doc["host"] == "https://api.jolpi.ca/ergast/f1"
    assert sorted(e["short"] for e in doc["endpoints"]) == sorted(set(_SHORT_OF.values()))
    for ep in doc["endpoints"]:
        assert ep["parser"] == "parse_f1_mrdata"
        assert ep["path"].endswith(".json"), ep["path"]
        assert {p["name"] for p in ep.get("extra_params", [])} == {"limit", "offset"}
        tokens = set(re.findall(r"\{(\w+)\}", ep["path"]))
        names = {p["name"] for p in ep.get("path_params", [])}
        assert not {"league", "sport"} & (tokens | names), ep["path"]
        assert tokens == names, f"{ep['short']}: path tokens {tokens} != path_params {names}"
    notes = " ".join(doc["docstring"]["notes"])
    assert "CC BY-NC-SA 4.0" in notes and "500 requests/hour" in notes


def test_every_endpoint_schema_has_described_columns() -> None:
    for ep in _yaml()["endpoints"]:
        schema = yaml.safe_load((SCHEMA_DIR / f"{ep['short']}.yaml").read_text(encoding="utf-8"))
        assert schema["columns"], ep["short"]
        blank = [c["name"] for c in schema["columns"] if not c["description"]]
        assert not blank, (ep["short"], blank)


# ---------------------------------------------------------------------------
# generated wrappers + the hand-written pager (fake transport)
# ---------------------------------------------------------------------------


def test_wrapper_builds_the_url_and_passes_paging(recorder: _Recorder) -> None:
    f1_mod.f1_results(2024, 1)
    assert recorder.calls[-1]["url"] == "https://api.jolpi.ca/ergast/f1/2024/1/results.json"
    f1_mod.f1_driver_standings(season="current", limit="100", offset="30")
    assert recorder.calls[-1]["url"] == "https://api.jolpi.ca/ergast/f1/current/driverStandings.json"
    assert recorder.calls[-1]["params"] == {"limit": "100", "offset": "30"}
    f1_mod.f1_driver(driver_id="max_verstappen")
    assert recorder.calls[-1]["url"] == "https://api.jolpi.ca/ergast/f1/drivers/max_verstappen.json"


def test_wrapper_docstrings_carry_terms_and_rate_limit() -> None:
    for name in f1_mod.__all__:
        doc = getattr(f1_mod, name).__doc__ or ""
        assert "Notes:" in doc and "CC BY-NC-SA 4.0" in doc and "500 requests/hour" in doc, name
    doc = laps_mod.f1_laps.__doc__ or ""
    assert "CC BY-NC-SA 4.0" in doc and "500 requests/hour" in doc


def test_f1_laps_pages_over_offset_until_total(monkeypatch: pytest.MonkeyPatch) -> None:
    page = _load("2024__1__laps")  # total 1129, limit echoed as 100
    rec = _Recorder(page)
    monkeypatch.setattr(f1_mod, "_get", rec)
    monkeypatch.setattr(laps_mod, "_PAGE_PAUSE_S", 0)
    df = laps_mod.f1_laps(2024, 1)
    expected_pages = math.ceil(int(page["MRData"]["total"]) / 100)
    assert len(rec.calls) == expected_pages == 12
    assert [c["params"]["offset"] for c in rec.calls] == [str(100 * i) for i in range(expected_pages)]
    assert all(c["params"]["limit"] == "100" for c in rec.calls)
    assert all(c["url"] == "https://api.jolpi.ca/ergast/f1/2024/1/laps.json" for c in rec.calls)
    assert df.height == expected_pages * 9
    assert df.columns == _schema_columns("laps_page")


def test_f1_laps_stops_on_an_empty_page(monkeypatch: pytest.MonkeyPatch) -> None:
    rec = _Recorder()  # Races: [] with total 0 (a round not yet run)
    monkeypatch.setattr(f1_mod, "_get", rec)
    monkeypatch.setattr(laps_mod, "_PAGE_PAUSE_S", 0)
    df = laps_mod.f1_laps(2024, "next")
    assert len(rec.calls) == 1
    assert df.height == 0


# ---------------------------------------------------------------------------
# live smoke -- 2 requests against the shared 500/hour budget
# ---------------------------------------------------------------------------


@skip_if_no_live
def test_live_2024_round_1_results_and_schedule() -> None:
    df = f1_mod.f1_results(2024, 1)
    assert df.height == 20
    assert df.schema["driver_id"] == pl.String
    assert df.filter(pl.col("position") == 1)["driver_id"].item() == "max_verstappen"
    sched = f1_mod.f1_schedule(2024)
    assert sched.height == 24
    assert sched["round"].to_list() == list(range(1, 25))
