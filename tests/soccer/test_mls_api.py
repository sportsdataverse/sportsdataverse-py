"""Offline tests for the official MLS web API (``mls_api``) stem.

Asserts the four parsers against the real trimmed captures in
``tests/fixtures/mls_api/`` (never synthetic payloads), plus the three-host URL
construction of the generated wrappers and the empty/malformed contract.
No network.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict

import polars as pl
import pytest

from sportsdataverse.soccer.mls import mls_api
from sportsdataverse.soccer.mls.mls_api_parsers import (
    parse_mls_api,
    parse_mls_entity,
    parse_mls_match,
    parse_mls_standings,
)

FIXTURES = Path(__file__).parents[1] / "fixtures" / "mls_api"


def _load(stem: str) -> Any:
    return json.loads((FIXTURES / f"{stem}.json").read_text(encoding="utf-8"))


class _Recorder:
    def __init__(self, payload: Any = None) -> None:
        self.payload = payload if payload is not None else {}
        self.url: str = ""
        self.params: Dict[str, Any] = {}

    def __call__(self, url: str, params: Any = None, **kwargs: Any) -> Any:
        self.url = url
        self.params = params or {}
        return self.payload


@pytest.fixture()
def recorder(monkeypatch: pytest.MonkeyPatch) -> _Recorder:
    rec = _Recorder()
    monkeypatch.setattr(mls_api, "_get", rec)
    return rec


# ---------------------------------------------------------------------------
# parse_mls_api -- envelopes the SPEC says are bare arrays
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("stem", "expected"),
    [
        ("statsapi_competitions", {"competition_id", "competition_name", "competition_type"}),
        ("statsapi_competitions_seasons", {"season_id", "season"}),
        ("statsapi_matches_by_season", {"match_id", "home_team_id", "away_team_id", "season_id"}),
        ("sportapi_players_byclub", {"sportec_id", "club_sportec_id", "position"}),
        ("dapi_seasons_query", {"slug", "fields_sportec_id"}),
    ],
)
def test_parse_mls_api_unwraps_the_rows_key(stem: str, expected: set) -> None:
    """The spec declares bare arrays for several of these; the capture is truth."""
    df = parse_mls_api(_load(stem))
    assert df.height >= 1
    assert expected <= set(df.columns)


def test_parse_mls_api_ids_are_utf8() -> None:
    df = parse_mls_api(_load("statsapi_matches_by_season"))
    for col in ("match_id", "season_id", "competition_id", "home_team_id", "away_team_id", "stadium_id"):
        assert df.schema[col] == pl.String, col


def test_parse_mls_api_accepts_a_bare_array() -> None:
    df = parse_mls_api(_load("sportapi_players_byclub"))
    assert df.height == 3


@pytest.mark.parametrize("raw", [None, [], {}, "nope", 3])
def test_parse_mls_api_empty_or_malformed_is_zero_rows(raw: Any) -> None:
    df = parse_mls_api(raw)
    assert isinstance(df, pl.DataFrame)
    assert df.height == 0


# ---------------------------------------------------------------------------
# parse_mls_entity -- single record whose nested arrays are NOT rows
# ---------------------------------------------------------------------------


def test_parse_mls_entity_club_is_one_row() -> None:
    df = parse_mls_entity(_load("statsapi_club_single"))
    assert df.height == 1
    assert df.schema["club_id"] == pl.String
    assert "club_color_one_club_color" in df.columns  # nested object flattened


def test_parse_mls_entity_does_not_mistake_broadcasters_for_rows() -> None:
    """``/api/matches/{id}`` nests a ``broadcasters`` array that is not the table."""
    raw = _load("sportapi_match_single")
    assert len(raw["broadcasters"]) > 1
    df = parse_mls_entity(raw)
    assert df.height == 1
    assert df.schema["sportec_id"] == pl.String
    # the parallel Opta integer namespace never stringifies as "123.0"
    assert not df["opta_id"].cast(pl.String).str.ends_with(".0").any()


def test_parse_mls_entity_return_as_pandas() -> None:
    pdf = parse_mls_entity(_load("statsapi_club_single"), return_as_pandas=True)
    assert type(pdf).__module__.startswith("pandas")


@pytest.mark.parametrize("raw", [None, {}, "nope"])
def test_parse_mls_entity_empty_is_zero_rows(raw: Any) -> None:
    assert parse_mls_entity(raw).height == 0


# ---------------------------------------------------------------------------
# parse_mls_standings
# ---------------------------------------------------------------------------


def test_parse_mls_standings_splits_tables_and_entries() -> None:
    tables = parse_mls_standings(_load("statsapi_standings_conference"))
    assert set(tables) == {"tables", "entries"}
    assert tables["tables"].height == 2
    assert "entries" not in tables["tables"].columns
    entries = tables["entries"]
    assert entries.height == 6  # 2 conferences x 3 trimmed clubs
    assert {"competition_id", "season_id", "group", "position", "team_id", "points"} <= set(entries.columns)
    assert entries.schema["team_id"] == pl.String
    assert entries.schema["position"].is_integer()


def test_parse_mls_standings_entries_carry_the_parent_group() -> None:
    entries = parse_mls_standings(_load("statsapi_standings_conference"))["entries"]
    assert entries["group"].n_unique() == 2


@pytest.mark.parametrize("raw", [None, {}, {"tables": []}, "nope"])
def test_parse_mls_standings_empty_keeps_both_keys(raw: Any) -> None:
    tables = parse_mls_standings(raw)
    assert set(tables) == {"tables", "entries"}
    assert all(t.height == 0 for t in tables.values())


# ---------------------------------------------------------------------------
# parse_mls_match
# ---------------------------------------------------------------------------

_MATCH_KEYS = {"match_information", "environment", "teams", "players", "staff", "referees", "last_matches"}


def test_parse_mls_match_splits_every_block() -> None:
    tables = parse_mls_match(_load("statsapi_match_single"))
    assert set(tables) == _MATCH_KEYS
    assert tables["match_information"].height == 1
    assert tables["environment"].height == 1
    assert tables["teams"].height == 2
    assert set(tables["teams"]["side"]) == {"home", "away"}
    assert tables["players"].height == 6  # capture is trimmed to 3 per side
    assert {"side", "team_id", "person_id", "playing_position"} <= set(tables["players"].columns)
    assert tables["referees"].height >= 1
    assert "role" in tables["referees"].columns


def test_parse_mls_match_person_ids_are_utf8() -> None:
    tables = parse_mls_match(_load("statsapi_match_single"))
    assert tables["players"].schema["person_id"] == pl.String
    assert tables["players"].schema["team_id"] == pl.String
    assert tables["referees"].schema["person_id"] == pl.String


def test_parse_mls_match_staff_is_grouped() -> None:
    staff = parse_mls_match(_load("statsapi_match_single"))["staff"]
    assert "staff_group" in staff.columns
    assert set(staff["staff_group"]) <= {"trainer_staff", "official_staff"}


@pytest.mark.parametrize("raw", [None, {}, "nope", []])
def test_parse_mls_match_empty_keeps_every_key(raw: Any) -> None:
    """An unplayed match 404s, which reaches the parser as an empty body."""
    tables = parse_mls_match(raw)
    assert set(tables) == _MATCH_KEYS
    assert all(t.height == 0 for t in tables.values())


# ---------------------------------------------------------------------------
# generated wrappers -- three hosts
# ---------------------------------------------------------------------------


def test_stats_api_host(recorder: _Recorder) -> None:
    mls_api.mls_competitions()
    assert recorder.url == "https://stats-api.mlssoccer.com/competitions"


def test_sportapi_host_override(recorder: _Recorder) -> None:
    mls_api.mls_sportapi_club_players(club_id="MLS-CLU-000001")
    assert recorder.url == "https://sportapi.mlssoccer.com/api/players/byClub/MLS-CLU-000001"


def test_dapi_host_override(recorder: _Recorder) -> None:
    mls_api.mls_content_season(slug="mls-regular-season-2026")
    assert recorder.url == "https://dapi.mlssoccer.com/v2/content/en-us/seasons/mls-regular-season-2026"


def test_standings_query_keys_are_the_wire_names(recorder: _Recorder) -> None:
    mls_api.mls_standings(
        competition_id="MLS-COM-000001",
        season_id="MLS-SEA-0001KA",
        category="conference",
        standings_type="home",
        is_live=True,
    )
    assert recorder.url == (
        "https://stats-api.mlssoccer.com/competitions/MLS-COM-000001/seasons/MLS-SEA-0001KA/standings"
    )
    assert recorder.params["category"] == "conference"
    assert recorder.params["type"] == "home"  # ``standings_type`` -> wire key ``type``
    assert recorder.params["is_live"] == "true"  # bool_str, not Python's "True"


def test_season_matches_bracketed_date_window(recorder: _Recorder) -> None:
    mls_api.mls_season_matches(
        season_id="MLS-SEA-0001KA",
        match_date_gte="2026-03-01",
        match_date_lte="2026-12-01",
        per_page=100,
    )
    assert recorder.params["match_date[gte]"] == "2026-03-01"
    assert recorder.params["match_date[lte]"] == "2026-12-01"
    assert recorder.params["per_page"] == 100


def test_wrapper_returns_raw_when_not_parsed(recorder: _Recorder) -> None:
    recorder.payload = _load("statsapi_competitions")
    assert mls_api.mls_competitions(return_parsed=False) == recorder.payload


def test_wrapper_count() -> None:
    assert len(mls_api.__all__) == 12


# ---------------------------------------------------------------------------
# runtime getter -- the site Referer the three hosts expect
# ---------------------------------------------------------------------------


def test_runtime_sends_the_site_referer(monkeypatch: pytest.MonkeyPatch) -> None:
    from sportsdataverse.soccer.mls import mls_api_runtime

    seen: Dict[str, Any] = {}

    class _Resp:
        def json(self) -> Any:
            return {"competitions": []}

    def _fake_download(url: str, params: Any = None, headers: Any = None, **kwargs: Any) -> Any:
        seen.update(url=url, params=params, headers=headers)
        return _Resp()

    monkeypatch.setattr(mls_api_runtime, "download", _fake_download)
    body = mls_api_runtime._get("https://stats-api.mlssoccer.com/competitions", params={"a": None, "b": 1})
    assert body == {"competitions": []}
    assert seen["headers"]["Referer"] == "https://www.mlssoccer.com/"
    assert "Mozilla" in seen["headers"]["User-Agent"]
    assert seen["params"] == {"b": 1}  # None-valued params stripped


def test_runtime_caller_headers_win(monkeypatch: pytest.MonkeyPatch) -> None:
    from sportsdataverse.soccer.mls import mls_api_runtime

    seen: Dict[str, Any] = {}
    monkeypatch.setattr(
        mls_api_runtime,
        "download",
        lambda url, params=None, headers=None, **kw: seen.update(headers=headers) or None,
    )
    assert mls_api_runtime._get("https://stats-api.mlssoccer.com/competitions", headers={"Referer": "x"}) == {}
    assert seen["headers"]["Referer"] == "x"
