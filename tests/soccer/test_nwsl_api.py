"""Offline tests for the official NWSL (StatsPerform SDP) ``nwsl_api`` stem.

Asserts the four parsers against the real trimmed captures in
``tests/fixtures/nwsl_api/`` (never synthetic payloads), plus the generated
wrappers' URL construction -- including the literal ``::`` in a composite id --
and the empty/malformed contract. No network.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict

import polars as pl
import pytest

from sportsdataverse.soccer.nwsl import nwsl_api
from sportsdataverse.soccer.nwsl.nwsl_api_parsers import (
    parse_nwsl_lineups,
    parse_nwsl_sdp,
    parse_nwsl_standings,
    parse_nwsl_stats,
)

FIXTURES = Path(__file__).parents[1] / "fixtures" / "nwsl_api"

SEASON_ID = "nwsl::Football_Season::0b6761e4701749f593690c0f338da74c"
MATCH_ID = "nwsl::Football_Match::0b6761e4701749f593690c0f338da74c"


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
    monkeypatch.setattr(nwsl_api, "_get", rec)
    return rec


# ---------------------------------------------------------------------------
# parse_nwsl_sdp -- named-key envelopes
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("stem", "expected"),
    [
        ("sdp_competitions", {"competition_id", "provider_id", "official_name"}),
        ("sdp_teams", {"team_id", "official_name", "stadium_id"}),
        ("sdp_multipleSeasonMatches", {"match_id", "season_id", "status", "match_date_utc"}),
    ],
)
def test_parse_nwsl_sdp_unwraps_the_rows_key(stem: str, expected: set) -> None:
    df = parse_nwsl_sdp(_load(stem))
    assert df.height == 3
    assert expected <= set(df.columns)


def test_parse_nwsl_sdp_prefers_matches_over_the_competitions_context() -> None:
    """``multipleSeasonMatches`` returns BOTH arrays -- the matches are the rows."""
    raw = _load("sdp_multipleSeasonMatches")
    assert "competitions" in raw and "matches" in raw
    df = parse_nwsl_sdp(raw)
    assert "match_id" in df.columns


def test_parse_nwsl_sdp_composite_ids_are_utf8() -> None:
    df = parse_nwsl_sdp(_load("sdp_teams"))
    assert df.schema["team_id"] == pl.String
    assert df["team_id"].str.starts_with("nwsl::Football_Team::").all()


def test_parse_nwsl_sdp_null_rows_key_is_zero_rows() -> None:
    """``sdp_stages.json`` is a real ``{"stages": null}`` league-season body."""
    raw = _load("sdp_stages")
    assert raw["stages"] is None
    assert parse_nwsl_sdp(raw).height == 0


@pytest.mark.parametrize("raw", [None, [], {}, "nope", 5])
def test_parse_nwsl_sdp_empty_or_malformed_is_zero_rows(raw: Any) -> None:
    df = parse_nwsl_sdp(raw)
    assert isinstance(df, pl.DataFrame)
    assert df.height == 0


def test_parse_nwsl_sdp_return_as_pandas() -> None:
    pdf = parse_nwsl_sdp(_load("sdp_teams"), return_as_pandas=True)
    assert type(pdf).__module__.startswith("pandas")


# ---------------------------------------------------------------------------
# parse_nwsl_standings -- stats[] pivoted wide
# ---------------------------------------------------------------------------


def test_parse_nwsl_standings_pivots_the_fixed_stat_set() -> None:
    df = parse_nwsl_standings(_load("sdp_standings_overall"))
    assert df.height == 9  # 3 splits x 3 trimmed clubs
    assert set(df["split_type"]) == {"table", "home", "away"}
    assert {"rank", "team", "points"} <= set(df.columns)  # trimmed capture ships 3 of 12 stat cells
    assert "stats" not in df.columns
    assert df.schema["team_id"] == pl.String


def test_parse_nwsl_standings_numeric_stats_keep_native_dtypes() -> None:
    df = parse_nwsl_standings(_load("sdp_standings_overall"))
    assert df.schema["points"].is_integer()
    assert df.schema["rank"].is_integer()


@pytest.mark.parametrize("raw", [None, {}, {"standings": []}, "nope"])
def test_parse_nwsl_standings_empty_is_zero_rows(raw: Any) -> None:
    assert parse_nwsl_standings(raw).height == 0


# ---------------------------------------------------------------------------
# parse_nwsl_stats -- stats[] kept long
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(("stem", "entity"), [("sdp_stats_players", "player_id"), ("sdp_stats_teams", "team_id")])
def test_parse_nwsl_stats_is_long(stem: str, entity: str) -> None:
    df = parse_nwsl_stats(_load(stem))
    assert df.height == 9  # 3 entities x 3 trimmed stat cells
    assert {"stats_id", "stats_label", "stats_value", entity} <= set(df.columns)
    assert df.schema[entity] == pl.String
    assert df[entity].n_unique() == 3


def test_parse_nwsl_stats_value_is_utf8() -> None:
    """The API mixes ints, strings and arrays in ``statsValue``."""
    df = parse_nwsl_stats(_load("sdp_stats_players"))
    assert df.schema["stats_value"] == pl.String


@pytest.mark.parametrize("raw", [None, {}, {"players": []}, "nope"])
def test_parse_nwsl_stats_empty_is_zero_rows(raw: Any) -> None:
    assert parse_nwsl_stats(raw).height == 0


# ---------------------------------------------------------------------------
# parse_nwsl_lineups
# ---------------------------------------------------------------------------

_LINEUP_KEYS = {"teams", "players", "staff"}


def test_parse_nwsl_lineups_splits_by_role() -> None:
    tables = parse_nwsl_lineups(_load("sdp_match_lineups"))
    assert set(tables) == _LINEUP_KEYS
    assert tables["teams"].height == 2
    assert set(tables["teams"]["side"]) == {"home", "away"}
    players = tables["players"]
    assert players.height == 12  # 2 sides x (3 fielded + 3 benched)
    assert set(players["selection"]) == {"fielded", "benched"}
    assert {"match_id", "side", "team_id", "player_id", "display_name"} <= set(players.columns)
    assert players.schema["player_id"] == pl.String
    assert players.schema["team_id"] == pl.String
    assert tables["staff"].height >= 1


def test_parse_nwsl_lineups_match_id_is_propagated() -> None:
    tables = parse_nwsl_lineups(_load("sdp_match_lineups"))
    assert tables["players"]["match_id"].str.starts_with("nwsl::Football_Match::").all()


@pytest.mark.parametrize("raw", [None, {}, "nope", []])
def test_parse_nwsl_lineups_empty_keeps_every_key(raw: Any) -> None:
    tables = parse_nwsl_lineups(raw)
    assert set(tables) == _LINEUP_KEYS
    assert all(t.height == 0 for t in tables.values())


# ---------------------------------------------------------------------------
# generated wrappers
# ---------------------------------------------------------------------------


def test_wrapper_url_keeps_the_composite_id_literal(recorder: _Recorder) -> None:
    """``::`` is passed literally in the path, never percent-encoded."""
    nwsl_api.nwsl_teams(season_id=SEASON_ID)
    assert recorder.url == f"https://api-sdp.nwslsoccer.com/v1/nwsl/football/seasons/{SEASON_ID}/teams"
    assert "::" in recorder.url


def test_wrapper_defaults_locale(recorder: _Recorder) -> None:
    nwsl_api.nwsl_competitions()
    assert recorder.url == "https://api-sdp.nwslsoccer.com/v1/nwsl/football/competitions"
    assert recorder.params["locale"] == "en-US"


def test_lineups_url_has_both_path_params(recorder: _Recorder) -> None:
    nwsl_api.nwsl_match_lineups(season_id=SEASON_ID, match_id=MATCH_ID)
    assert recorder.url == (
        f"https://api-sdp.nwslsoccer.com/v1/nwsl/football/seasons/{SEASON_ID}/matches/{MATCH_ID}/lineups"
    )


def test_player_stats_passes_pagination(recorder: _Recorder) -> None:
    nwsl_api.nwsl_player_stats(season_id=SEASON_ID, category="general", page=1, page_num_element=400)
    assert recorder.params["category"] == "general"
    assert recorder.params["page"] == 1
    assert recorder.params["pageNumElement"] == 400


def test_wrapper_returns_raw_when_not_parsed(recorder: _Recorder) -> None:
    recorder.payload = _load("sdp_teams")
    assert nwsl_api.nwsl_teams(season_id=SEASON_ID, return_parsed=False) == recorder.payload


def test_wrapper_count() -> None:
    assert len(nwsl_api.__all__) == 9


# ---------------------------------------------------------------------------
# runtime getter -- the site Referer the SDP host expects
# ---------------------------------------------------------------------------


def test_runtime_sends_the_site_referer(monkeypatch: pytest.MonkeyPatch) -> None:
    from sportsdataverse.soccer.nwsl import nwsl_api_runtime

    seen: Dict[str, Any] = {}

    class _Resp:
        def json(self) -> Any:
            return {"competitions": []}

    def _fake_download(url: str, params: Any = None, headers: Any = None, **kwargs: Any) -> Any:
        seen.update(url=url, params=params, headers=headers)
        return _Resp()

    monkeypatch.setattr(nwsl_api_runtime, "download", _fake_download)
    url = f"https://api-sdp.nwslsoccer.com/v1/nwsl/football/seasons/{SEASON_ID}/teams"
    body = nwsl_api_runtime._get(url, params={"locale": "en-US", "orderBy": None})
    assert body == {"competitions": []}
    assert seen["headers"]["Referer"] == "https://www.nwslsoccer.com/"
    assert seen["params"] == {"locale": "en-US"}
    assert "::" in seen["url"]  # composite id reaches the host literally


def test_runtime_non_json_body_is_empty_dict(monkeypatch: pytest.MonkeyPatch) -> None:
    from sportsdataverse.soccer.nwsl import nwsl_api_runtime

    class _Bad:
        def json(self) -> Any:
            raise ValueError("not json")

    monkeypatch.setattr(nwsl_api_runtime, "download", lambda **kw: _Bad())
    assert nwsl_api_runtime._get("https://api-sdp.nwslsoccer.com/v1/nwsl/football/competitions") == {}
