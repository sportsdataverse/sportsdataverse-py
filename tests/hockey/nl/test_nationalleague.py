"""Offline tests for the National League app API family (``nationalleague``, prefix ``nl``).

Parsers are asserted against the real committed captures in ``tests/fixtures/nationalleague/``
(truncated, 3 rows per array) and the full bodies under ``full/`` (the dtype source). The runtime's
refusals are asserted with a mocked transport. No network.
"""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

import pandas as pd
import polars as pl
import pytest
import yaml

from sportsdataverse import _codegen_runtime as _rt
from sportsdataverse.errors import AssetFetchError, NoDataError
from sportsdataverse.hockey import euro_hockey_runtime as _euro
from sportsdataverse.hockey.nl import nationalleague_parsers as P
from sportsdataverse.hockey.nl import nationalleague_runtime as rt
from sportsdataverse.hockey.sihf.sihf_api_web_parsers import parse_sihf_game_lineups

ROOT = Path(__file__).parents[3]
FIXTURES = ROOT / "tests" / "fixtures" / "nationalleague"
ENDPOINTS = ROOT / "tools" / "codegen" / "endpoints" / "nationalleague.yaml"
SCHEMAS = ROOT / "tools" / "codegen" / "schemas" / "native" / "nationalleague"
HOST = "https://www.nationalleague.ch/api"
GAME = "game_20261105000325.json"
_TYPES = {pl.Int64: "integer", pl.Float64: "numeric", pl.String: "character", pl.Boolean: "logical"}


def _load(name: str) -> Any:
    return json.loads((FIXTURES / name).read_text(encoding="utf-8"))


def _yaml() -> Dict[str, Any]:
    return yaml.safe_load(ENDPOINTS.read_text(encoding="utf-8"))


def _parser(short: str) -> Callable[..., pl.DataFrame]:
    ep = next(e for e in _yaml()["endpoints"] if e["short"] == short)
    return getattr(P, ep["parser"])


def _schema(short: str) -> List[tuple]:
    doc = yaml.safe_load((SCHEMAS / f"{short}.yaml").read_text(encoding="utf-8"))
    return [(c["name"], c["type"]) for c in doc["columns"]]


def _documented(df: pl.DataFrame) -> List[tuple]:
    return [(n, _TYPES[type(t)]) for n, t in df.schema.items()]


# short -> (truncated refs capture, full body or None)
CASES: Dict[str, tuple] = {
    "schedule": ("games.json", None),
    "current_games": ("games__current.json", None),
    "team_schedule": ("games__team__teamId.json", None),
    "playoffs": ("games__playoffs.json", None),
    "playouts": ("games__playouts.json", None),
    "game_overview": ("games__gameId.json", GAME),
    "pbp": ("games__gameId.json", GAME),
    "lineups": ("games__gameId.json", GAME),
    "player_box": ("games__gameId.json", GAME),
    "team_stats": ("games__gameId.json", GAME),
    "shots": ("games__gameId.json", GAME),
    "player_stats": ("player.json", "player_2026_4940.json"),
    "top_scorers": ("player__topscorer.json", "player_2026_4940.json"),
    "team_player_stats": ("player__team__teamId.json", "player_2026_4940.json"),
    "player_season": ("player__playerId.json", "player_2026_4940.json"),
    "standings": ("teams.json", "teams_2026_4940.json"),
    "team_info": ("teams__teamId__info.json", None),
}


# ---------------------------------------------------------------------------
# Parsers
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("short", sorted(CASES))
def test_every_capture_parses_to_a_tidy_frame(short: str) -> None:
    df = _parser(short)(_load(CASES[short][0]))
    assert isinstance(df, pl.DataFrame) and df.height > 0
    assert all(re.fullmatch(r"[a-z][a-z0-9_]*", c) for c in df.columns), df.columns
    ids = [c for c in df.columns if c == "id" or c.endswith("_id")]
    assert ids and all(df.schema[c] == pl.Int64 for c in ids), {c: df.schema[c] for c in ids}


@pytest.mark.parametrize("short", sorted(CASES))
def test_schema_yaml_matches_the_parser(short: str) -> None:
    # The committed returns-schema is exactly what the parser emits on its documented sample.
    capture, full = CASES[short]
    assert _documented(_parser(short)(_load(f"full/{full}" if full else capture))) == _schema(short)


@pytest.mark.parametrize("short", sorted(s for s, (_, full) in CASES.items() if full))
def test_truncated_capture_has_the_full_body_dtypes(short: str) -> None:
    # Dtypes come from the full body: a 3-row capture must not retype a rate as an integer.
    capture, full = CASES[short]
    assert _parser(short)(_load(capture)).schema == _parser(short)(_load(f"full/{full}")).schema


def test_rates_are_float_even_when_every_sampled_value_is_whole() -> None:
    raw = _load("games__gameId.json")
    assert raw["playerStatsAway"][0]["savePercentage"] == 90  # an int in the capture ...
    box = P.parse_nationalleague_player_box(raw)
    assert box.schema["save_percentage"] == pl.Float64  # ... a Float64 column all the same
    full = P.parse_nationalleague_player_stats(_load("full/player_2026_4940.json"))
    assert full.schema["ps_percentage"] == pl.Float64  # whole in all 502 rows of the 2026 body


def test_a_value_the_documented_dtype_cannot_hold_is_kept() -> None:
    df = P.parse_nationalleague_games([{"gameId": "20261105000325", "gameTime": 12.5}])
    assert df.schema["game_time"] == pl.Float64 and df["game_time"][0] == 12.5


def test_an_id_column_stays_int64_when_a_value_does_not_fit() -> None:
    # Join keys never flip dtype: a non-digit id is nulled, not a reason to retype the column.
    rows = [{"gameId": "20261105000325", "homeTeamId": "101151"}, {"gameId": "x1", "homeTeamId": "abc"}]
    df = P.parse_nationalleague_games(rows)
    assert df.schema["game_id"] == pl.Int64 and df["game_id"].to_list() == [20261105000325, None]
    assert df.schema["home_team_id"] == pl.Int64 and df["home_team_id"].to_list() == [101151, None]
    extra = P.parse_nationalleague_games([{"gameId": "20261105000325", "newThingId": "n/a"}])
    assert extra.schema["new_thing_id"] == pl.Int64 and extra["new_thing_id"].to_list() == [None]


@pytest.mark.parametrize("games", ["x", 3, {"a": 1}, None])
def test_bracket_tolerates_a_wrong_type_games_value(games: Any) -> None:
    raw = [{"round": "final", "games": games}, {"round": "semifinal", "games": [{"games": games}]}]
    df = P.parse_nationalleague_bracket(raw)
    assert df.height == 0 and df.schema == P.parse_nationalleague_bracket([]).schema


@pytest.mark.parametrize("short", sorted(CASES))
@pytest.mark.parametrize("raw", [{}, [], None, "x", 3, {"overview": "x"}])
def test_empty_or_malformed_payload_is_a_zero_row_documented_frame(short: str, raw: Any) -> None:
    if raw == {"overview": "x"} and short.startswith(("player_season", "player_stats", "top_", "team_p", "team_info")):
        pytest.skip("an object is a valid one-row body for this parser")
    df = _parser(short)(raw)
    assert df.height == 0
    assert _documented(df) == _schema(short)


def test_generic_row_rule() -> None:
    assert P.parse_nationalleague([{"gameId": "1", "x": {"y": 2}}]).to_dicts() == [{"game_id": 1, "x_y": 2}]
    assert P.parse_nationalleague({"playerId": "305519"}).height == 1
    assert P.parse_nationalleague(None).height == 0


def test_return_as_pandas() -> None:
    df = P.parse_nationalleague_shots(_load(f"full/{GAME}"), return_as_pandas=True)
    assert isinstance(df, pd.DataFrame) and len(df) == 105


def test_player_rows_cast_the_one_integer_position_to_a_string() -> None:
    df = P.parse_nationalleague_player_stats(_load("full/player_2026_4940.json"))
    assert df.schema["position"] == pl.String and "0" in df["position"].to_list()


def test_bracket_rows_carry_round_and_series() -> None:
    df = P.parse_nationalleague_bracket(_load("games__playoffs.json"))
    assert set(df["round"]) == {"final", "semifinal", "quarterfinal"}
    final = df.filter(pl.col("round") == "final")
    assert final["series_best_of"].unique().to_list() == [7]
    assert final["series_home_team_id"].unique().to_list() == [101151]
    assert df.select(pl.col("game_id").cast(pl.String).str.slice(0, 4)).to_series().unique().to_list() == ["2026"]


# ---------------------------------------------------------------------------
# The game page
# ---------------------------------------------------------------------------


def test_fo_is_faceoffs_in_the_game_box_and_penalties_in_season_rows() -> None:
    game = _load(f"full/{GAME}")
    box = P.parse_nationalleague_player_box(game)
    team = P.parse_nationalleague_team_stats(game)
    for df in (box, team):
        assert "fo" not in df.columns and "penalties" not in df.columns
        assert df.filter(pl.col("faceoffs") != pl.col("fow") + pl.col("fol")).height == 0
    season = P.parse_nationalleague_player_stats(_load("full/player_2026_4940.json"))
    assert "fo" not in season.columns and "faceoffs" not in season.columns
    total = pl.col("fo2") + pl.col("fo5") + pl.col("fo10") + pl.col("gm") + pl.col("mp")
    assert season.filter(pl.col("penalties") != total).height == 0
    assert "penalties" in P.parse_nationalleague_standings(_load("full/teams_2026_4940.json")).columns


def test_shots_coordinates_and_xg() -> None:
    shots = P.parse_nationalleague_shots(_load(f"full/{GAME}"))
    assert shots.height == 105 and set(shots["team_side"]) == {"home", "away"}
    assert set(shots["shot_type"]) <= {"SOG", "MISS", "POST", "BLOCK", "GOAL"}
    assert shots.filter((pl.col("pos_x") < 0) | (pl.col("pos_x") > 30)).height == 0
    assert shots.filter((pl.col("pos_y") < 0) | (pl.col("pos_y") > 60)).height == 0
    assert shots.filter((pl.col("pos_y_percentage") - pl.col("pos_y") / 24).abs() > 1e-9).height == 0
    assert shots.filter((pl.col("pos_x_percentage") - pl.col("pos_x") / 30).abs() > 1e-9).height == 0
    assert shots.filter((pl.col("shot_type") == "BLOCK") & (pl.col("xg") != 0)).height == 0
    assert shots.filter((pl.col("shot_type") != "BLOCK") & (pl.col("xg") > 0)).height > 0  # a 2026 game: xG on
    assert shots["time"].is_sorted()
    assert shots.filter(pl.col("player_last_name").is_null()).height == 0  # every shooter is in the lineup


def test_box_score_rows_get_their_player_id_from_the_lineup() -> None:
    game = _load(f"full/{GAME}")
    box, lineups = P.parse_nationalleague_player_box(game), P.parse_nationalleague_lineups(game)
    assert box["player_id"].null_count() == 0
    key = ["team_side", "number", "player_id"]
    assert box.select(key).join(lineups.select(key), on=key, how="anti").height == 0


def test_pbp_clock_and_order() -> None:
    pbp = P.parse_nationalleague_pbp(_load(f"full/{GAME}"))
    assert pbp["event_index"].to_list() == list(range(1, pbp.height + 1))
    first = pbp.row(0, named=True)
    assert (first["time"], first["game_seconds"], first["period"], first["action"]) == ("822", 502, 1, "goal")
    assert pbp["game_seconds"].is_sorted()


def test_player_id_joins_the_sihf_family_on_the_same_game() -> None:
    # The licence number is Int64 on both sides, and the same game's lineups share it.
    ours = P.parse_nationalleague_lineups(_load(f"full/{GAME}"))
    sihf_path = ROOT / "tests" / "fixtures" / "sihf_api_web" / "gameoverview__20261105000325.json"
    theirs = parse_sihf_game_lineups(json.loads(sihf_path.read_text(encoding="utf-8")))
    assert ours.schema["player_id"] == theirs.schema["player_id"] == pl.Int64
    assert ours.schema["game_id"] == theirs.schema["game_id"] == pl.Int64
    assert set(ours["game_id"]) == set(theirs["game_id"]) == {20261105000325}
    joined = ours.join(theirs.select("player_id").unique(), on="player_id", how="inner")
    assert joined.height >= 40  # 44 dressed players here


# ---------------------------------------------------------------------------
# Redaction
# ---------------------------------------------------------------------------


def _with_key(raw: Dict[str, Any]) -> Dict[str, Any]:
    raw = json.loads(json.dumps(raw))
    raw["fields"]["apiKey"] = "s3cr3t-club-key"
    raw["fields"]["website"] = "https://example.ch/?apiKey=s3cr3t-club-key&x=1"
    return raw


def test_team_info_never_returns_the_api_key() -> None:
    df = P.parse_nationalleague_team_info(_with_key(_load("teams__teamId__info.json")))
    assert not [c for c in df.columns if "api_key" in c]
    assert "s3cr3t" not in json.dumps(df.to_dicts())
    assert df["team_id"][0] == 101144 and df["cms_node_id"][0] == 1627


def test_runtime_drops_the_api_key_from_the_raw_body(monkeypatch: pytest.MonkeyPatch) -> None:
    body = _with_key(_load("teams__teamId__info.json"))
    monkeypatch.setattr(_euro, "_get", lambda url, params=None, **kw: body)
    raw = rt._get(f"{HOST}/teams/101144/info")
    assert "apiKey" not in raw["fields"] and "s3cr3t" not in json.dumps(raw)


# ---------------------------------------------------------------------------
# Runtime: polite wait, END-year season, refusals
# ---------------------------------------------------------------------------


class _Resp:
    def __init__(self, status: int, text: str) -> None:
        self.status_code, self.text = status, text

    def json(self) -> Any:
        return json.loads(self.text)


@pytest.fixture()
def transport(monkeypatch: pytest.MonkeyPatch) -> Dict[str, Any]:
    """Mock the HTTP transport under the real getter chain; record requests and throttled attempts."""
    state: Dict[str, Any] = {"waits": [], "calls": [], "status": 200, "text": "[]"}
    monkeypatch.setattr(_rt.time, "sleep", lambda s: None)

    def download(url: str, params: Optional[dict] = None, **kw: Any) -> _Resp:
        state["calls"].append((url, dict(params or {})))
        if kw.get("session") is _euro.SESSION:  # SESSION waits for the host's slot before every attempt
            state["waits"].append(url)
        return _Resp(state["status"], state["text"])

    monkeypatch.setattr(_rt, "download", download)
    return state


def _answer(state: Dict[str, Any], body: Any, status: int = 200) -> None:
    state["status"], state["text"] = status, body if isinstance(body, str) else json.dumps(body)


# (url, params, capture) exactly as the capture was requested (fixtures README).
REQUESTS = [
    ("/games", {"season": 2026}, "games.json"),
    ("/games/current", {"season": 2027}, "games__current.json"),
    ("/games/playoffs", {"season": 2026}, "games__playoffs.json"),
    ("/games/playouts", {"season": 2026}, "games__playouts.json"),
    ("/games/team/101144", {"season": 2026}, "games__team__teamId.json"),
    ("/games/20261105000325", {}, "games__gameId.json"),
    ("/player", {"season": 2026, "phase": 4940}, "player.json"),
    ("/player/topscorer", {"take": 3, "season": 2026, "phase": 4940}, "player__topscorer.json"),
    ("/player/team/101144", {"season": 2026, "phase": 4940}, "player__team__teamId.json"),
    ("/player/305519", {"season": 2026, "phase": 4940}, "player__playerId.json"),
    ("/teams", {"season": 2026, "phase": 4940}, "teams.json"),
    ("/teams/101144/info", {}, "teams__teamId__info.json"),
]


@pytest.mark.parametrize(("path", "params", "capture"), REQUESTS)
def test_every_real_capture_passes_the_runtime_checks(transport, path, params, capture) -> None:
    _answer(transport, _load(capture))
    assert rt._get(HOST + path, params) is not None
    assert transport["waits"] == [HOST + path]  # sent over the throttled session
    assert transport["calls"] == [(HOST + path, params)]


def test_season_is_sent_as_the_end_year_unchanged(transport) -> None:
    # The API keys seasons by END year (2026 = 2025/26), the package convention: no mapping.
    _answer(transport, _load("games.json"))
    rt._get(f"{HOST}/games", {"season": 2026})
    assert transport["calls"][0][1] == {"season": 2026}
    _answer(transport, _load("games.json"))
    with pytest.raises(AssetFetchError, match="asked season 2025"):  # 2026 games for a 2025 request: a wrong answer
        rt._get(f"{HOST}/games", {"season": 2025})


@pytest.mark.parametrize(
    ("status", "error"),
    [(404, NoDataError), (400, ValueError), (403, AssetFetchError), (500, AssetFetchError)],
)
def test_non_200_follows_the_error_vocabulary(transport, status, error) -> None:
    _answer(transport, "upstream says no", status)
    with pytest.raises(error):
        rt._get(f"{HOST}/games", {"season": 2026})


@pytest.mark.parametrize("text", ["", "<html>blocked</html>", '[{"gameId": "2026110'])
def test_empty_html_or_truncated_body_is_a_failed_fetch(transport, text) -> None:
    _answer(transport, text)
    with pytest.raises(AssetFetchError):
        rt._get(f"{HOST}/games", {"season": 2026})


def _game(**overview: Any) -> Dict[str, Any]:
    body = _load("games__gameId.json")
    body["overview"].update(overview)
    return body


@pytest.mark.parametrize(
    ("path", "params", "body", "error", "match"),
    [
        # error envelopes and stub bodies
        ("/games", {"season": 2026}, {"error": "NotFound", "status": 500}, AssetFetchError, "error object"),
        ("/games/20261105000325", {}, {"message": "oops"}, AssetFetchError, "error object"),
        ("/games", {"season": 2016}, [], NoDataError, "empty list"),
        ("/player", {"season": 2026}, [], NoDataError, "empty list"),
        ("/games/20261105000325", {}, {}, NoDataError, "empty object"),
        ("/games", {"season": 2026}, {"games": []}, AssetFetchError, "not a list"),
        ("/player/305519", {}, [], AssetFetchError, "not an object"),
        ("/games/playouts", {"season": 2026}, [{"round": "playoutFinal", "games": []}], NoDataError, "no games"),
        # wrong scope
        ("/games/team/101151", {"season": 2026}, "games__team__teamId.json", AssetFetchError, "asked team 101151"),
        ("/player/team/101151", {"season": 2026}, "player__team__teamId.json", AssetFetchError, "another team"),
        ("/games/20261105000326", {}, "games__gameId.json", AssetFetchError, "asked 20261105000326"),
        ("/player/305520", {}, "player__playerId.json", AssetFetchError, "asked 305520"),
        ("/teams/101151/info", {}, "teams__teamId__info.json", AssetFetchError, "asked 101151"),
        ("/games/playoffs", {"season": 2027}, "games__playoffs.json", AssetFetchError, "asked season 2027"),
        # malformed rows / ids (type-strict: an integer id is refused)
        ("/games/20261105000325", {}, {"result": {}}, AssetFetchError, "no digit-string id"),
        ("/games/20261105000325", {}, _game(gameId=20261105000325), AssetFetchError, "no digit-string id"),
        ("/games", {"season": 2026}, [{"gameId": 20261105000325}], AssetFetchError, "digit-string gameId"),
        ("/games/playoffs", {"season": 2026}, ["final"], AssetFetchError, "a str where"),
        ("/player", {"season": 2026}, [{"playerId": 305519}], AssetFetchError, "digit-string playerId"),
        ("/teams", {"season": 2026}, [{"name": "EV Zug"}], AssetFetchError, "digit-string teamId"),
    ],
)
def test_runtime_refusals(transport, path, params, body, error, match) -> None:
    _answer(transport, _load(body) if isinstance(body, str) and body.endswith(".json") else body)
    with pytest.raises(error, match=match):
        rt._get(HOST + path, params)
    assert transport["waits"] == [HOST + path]


# ---------------------------------------------------------------------------
# Endpoint YAML
# ---------------------------------------------------------------------------


def test_yaml_lists_every_captured_route() -> None:
    doc = _yaml()
    assert doc["name_pattern"] == "nl_{short}" and doc["getter_module"] == rt.__name__
    assert sorted(e["short"] for e in doc["endpoints"]) == sorted(CASES)
    # 12 captured routes, one per committed capture; the game page feeds six wrappers.
    captures = sorted(p.name for p in FIXTURES.glob("*.json"))
    assert len({e["path"] for e in doc["endpoints"]}) == len(captures) == 12
    assert sum(e["path"] == "/games/{game_id}" for e in doc["endpoints"]) == 6


def test_no_path_token_named_league_or_sport() -> None:
    for ep in _yaml()["endpoints"]:
        assert not {"league", "sport"} & set(re.findall(r"\{([^}]+)\}", ep["path"])), ep["path"]


def test_docstring_carries_the_robots_ruling() -> None:
    notes = " ".join(_yaml()["docstring"]["notes"])
    assert "robots.txt" in notes and "2026-10-12" in notes and "1 s" in notes


def test_generated_wrapper_builds_the_url(monkeypatch: pytest.MonkeyPatch) -> None:
    mod = pytest.importorskip("sportsdataverse.hockey.nl.nationalleague")  # rendered at integration
    seen: Dict[str, Any] = {}
    monkeypatch.setattr(mod, "_get", lambda url, params=None, **kw: seen.update(url=url, params=params) or [])
    mod.nl_shots(game_id=20261105000325)
    assert seen["url"] == f"{HOST}/games/20261105000325"
    mod.nl_player_stats(season=2026, phase=4940)
    assert (seen["url"], seen["params"]) == (f"{HOST}/player", {"season": 2026, "phase": 4940})
