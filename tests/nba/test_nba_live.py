import datetime as dt
import json
from pathlib import Path

import polars as pl
import pytest
import yaml

from sportsdataverse.dl_utils import underscore
from sportsdataverse.errors import AssetFetchError, NoDataError
from sportsdataverse.nba import nba_live as mod
from sportsdataverse.nba.nba_live import (
    NBA_LIVE_GAME_CORE_SCHEMA,
    NBA_LIVE_OFFICIALS_CORE_SCHEMA,
    NBA_LIVE_PBP_CORE_SCHEMA,
    NBA_LIVE_PLAYERS_CORE_SCHEMA,
    NBA_LIVE_TEAM_CORE_SCHEMA,
    nba_live_boxscore,
    nba_live_pbp,
    parse_nba_live_boxscore,
    parse_nba_live_pbp,
)
from sportsdataverse.wnba.wnba_live import wnba_live_boxscore, wnba_live_pbp

FIX = Path(__file__).parent / "fixtures" / "nba_live"
PBP_PAYLOAD = json.loads((FIX / "playbyplay_0022500001.json").read_text())
BOX_PAYLOAD = json.loads((FIX / "boxscore_0022500001.json").read_text())


def _fake_transport(status, text):
    seen = {}

    def transport(url, params, headers, proxy_url):
        seen["url"], seen["headers"], seen["proxy_url"] = url, headers, proxy_url
        return status, text

    return transport, seen


def _sequence_transport(*responses):
    """Transport yielding *responses* in order (status/text tuple or an Exception instance);
    extra calls beyond the given sequence repeat the last entry. Returns (transport, calls)."""
    responses = list(responses)
    calls = {"n": 0}

    def transport(url, params, headers, proxy_url):
        calls["n"] += 1
        item = responses[min(calls["n"] - 1, len(responses) - 1)]
        if isinstance(item, Exception):
            raise item
        return item

    return transport, calls


@pytest.fixture(autouse=True)
def _no_real_sleep(monkeypatch):
    """H1: retry backoff must never actually sleep in tests -- monkeypatch the
    module-level reference, not the global ``time`` module."""
    monkeypatch.setattr(mod, "_sleep", lambda *_: None)


# ---------------------------------------------------------------------------
# parse_nba_live_pbp
# ---------------------------------------------------------------------------


def test_pbp_row_count_matches_actions():
    df = parse_nba_live_pbp(PBP_PAYLOAD)
    assert df.height == len(PBP_PAYLOAD["game"]["actions"])


def test_pbp_every_foul_has_official_id():
    df = parse_nba_live_pbp(PBP_PAYLOAD)
    fouls = df.filter(df["action_type"] == "foul")
    assert fouls.height > 0
    assert fouls["official_id"].null_count() == 0
    assert fouls["official_id"].dtype.__str__() == "Int64"


def test_pbp_time_actual_parses_as_datetime():
    df = parse_nba_live_pbp(PBP_PAYLOAD)
    sample = df["time_actual"][0]
    # ISO-8601 with a trailing Z (e.g. "2025-10-21T23:43:24.7Z")
    parsed = dt.datetime.fromisoformat(sample.replace("Z", "+00:00"))
    assert isinstance(parsed, dt.datetime)


def test_pbp_game_id_is_zero_padded_string():
    df = parse_nba_live_pbp(PBP_PAYLOAD)
    assert set(df["game_id"].unique().to_list()) == {"0022500001"}
    assert df.schema["game_id"].__str__() == "String"


def test_pbp_empty_payload_is_zero_rows():
    df = parse_nba_live_pbp({})
    assert df.height == 0


def test_pbp_empty_actions_has_core_schema():
    df = parse_nba_live_pbp({"game": {"gameId": "0022500001", "actions": []}})
    assert df.height == 0
    for name, dtype in NBA_LIVE_PBP_CORE_SCHEMA.items():
        assert name in df.columns
        assert df.schema[name] == dtype


# ---------------------------------------------------------------------------
# Ruling: infer_schema_length=None -- fields first seen after row 100 must survive
# ---------------------------------------------------------------------------


def _expected_flat_columns(actions):
    """Every column ``pl.json_normalize(..., separator="_")`` + ``underscore()`` produces
    for the full action list, with no row cap -- the ground truth ``_normalize`` must match."""
    flat = pl.json_normalize(actions, separator="_", infer_schema_length=None)
    return {underscore(c) for c in flat.columns}


@pytest.mark.parametrize(
    "fixture_path",
    [
        FIX / "playbyplay_0022500001.json",
        Path(__file__).parent.parent / "wnba" / "fixtures" / "wnba_live" / "playbyplay_1022600097.json",
    ],
    ids=["nba", "wnba"],
)
def test_pbp_every_flattened_key_survives_on_both_fixtures(fixture_path):
    payload = json.loads(fixture_path.read_text())
    actions = payload["game"]["actions"]
    expected = _expected_flat_columns(actions)
    df = parse_nba_live_pbp(payload)
    missing = expected - set(df.columns)
    assert not missing, f"columns dropped by schema inference: {sorted(missing)}"


def test_pbp_block_person_id_present_and_int64_on_both_fixtures():
    for fixture_path in (
        FIX / "playbyplay_0022500001.json",
        Path(__file__).parent.parent / "wnba" / "fixtures" / "wnba_live" / "playbyplay_1022600097.json",
    ):
        payload = json.loads(fixture_path.read_text())
        df = parse_nba_live_pbp(payload)
        assert "block_person_id" in df.columns, fixture_path
        assert df.schema["block_person_id"] == pl.Int64, fixture_path
        assert df["block_person_id"].null_count() < df.height, fixture_path


_AUTODOC = Path(__file__).resolve().parents[2] / "tools" / "codegen" / "schemas" / "autodoc"
_DOC_TYPE = {"Int64": "integer", "Float64": "double", "String": "character", "Boolean": "logical"}


@pytest.mark.parametrize(
    ("fixture_path", "schema_path"),
    [
        (FIX / "playbyplay_0022500001.json", _AUTODOC / "nba" / "nba_live_pbp.yaml"),
        (
            Path(__file__).parent.parent / "wnba" / "fixtures" / "wnba_live" / "playbyplay_1022600097.json",
            _AUTODOC / "wnba" / "wnba_live_pbp.yaml",
        ),
    ],
    ids=["nba", "wnba"],
)
def test_pbp_returns_table_documents_every_capture_column(fixture_path, schema_path):
    """The hand-authored Returns table lists the core columns first, then every other
    column a real capture produces, at the captured dtype -- none left undocumented."""
    df = parse_nba_live_pbp(json.loads(fixture_path.read_text()))
    doc = {c["name"]: c["type"] for c in yaml.safe_load(schema_path.read_text(encoding="utf-8"))["columns"]}
    assert list(doc)[: len(NBA_LIVE_PBP_CORE_SCHEMA)] == NBA_LIVE_PBP_CORE_SCHEMA.names()
    assert set(doc) == set(df.columns)
    for name, dtype in df.schema.items():
        assert doc[name] == ("list" if isinstance(dtype, pl.List) else _DOC_TYPE[str(dtype)]), name


def test_pbp_official_id_survives_when_first_occurrence_moves_past_row_100():
    """Reorder a copy of the real fixture so the first officialId-bearing action lands
    well past the old 100-row inference cap, and assert no official_id is lost."""
    payload = json.loads((FIX / "playbyplay_0022500001.json").read_text())
    actions = payload["game"]["actions"]
    before_count = parse_nba_live_pbp(payload)["official_id"].null_count()

    without_official = [a for a in actions if "officialId" not in a]
    with_official = [a for a in actions if "officialId" in a]
    assert len(without_official) > 100  # sanity: the reorder actually pushes past the old cap
    reordered = dict(payload)
    reordered["game"] = dict(payload["game"])
    reordered["game"]["actions"] = without_official + with_official

    after_count = parse_nba_live_pbp(reordered)["official_id"].null_count()
    assert after_count == before_count


# ---------------------------------------------------------------------------
# parse_nba_live_boxscore
# ---------------------------------------------------------------------------


def test_boxscore_officials_shape():
    result = parse_nba_live_boxscore(BOX_PAYLOAD)
    officials = result["officials"]
    assert officials.height >= 3
    assert officials.schema["person_id"].__str__() == "Int64"
    assert officials.schema["assignment"].__str__() == "String"


def test_boxscore_keys():
    result = parse_nba_live_boxscore(BOX_PAYLOAD)
    assert set(result.keys()) == {"game", "officials", "home_players", "away_players", "home_team", "away_team"}
    assert result["home_players"].height > 0
    assert result["away_players"].height > 0
    assert result["home_players"]["team_id"].dtype.__str__() == "Int64"


def test_boxscore_empty_payload_is_zero_rows_everywhere():
    result = parse_nba_live_boxscore({})
    assert all(v.height == 0 for v in result.values())


def test_boxscore_one_side_zero_players_stays_concat_safe():
    box2 = json.loads(json.dumps(BOX_PAYLOAD))
    box2["game"]["homeTeam"]["players"] = []
    result = parse_nba_live_boxscore(box2)
    home, away = result["home_players"], result["away_players"]
    assert home.height == 0
    assert away.height > 0
    for df in (home, away):
        for name, dtype in NBA_LIVE_PLAYERS_CORE_SCHEMA.items():
            assert name in df.columns
            assert df.schema[name] == dtype
    combined = pl.concat([home, away], how="diagonal_relaxed")
    assert combined.height == away.height


# ---------------------------------------------------------------------------
# nba_live_pbp / nba_live_boxscore -- transport + error classification
# ---------------------------------------------------------------------------


def test_nba_live_pbp_uses_curl_transport_and_nba_cdn_headers(monkeypatch):
    transport, seen = _fake_transport(200, json.dumps(PBP_PAYLOAD))
    monkeypatch.setattr(mod, "_curl_transport", transport)
    df = nba_live_pbp("0022500001")
    assert "cdn.nba.com" in seen["url"]
    # curl_cffi's impersonation supplies a User-Agent that matches its client hints.
    assert "User-Agent" not in seen["headers"]
    assert seen["headers"]["Origin"] == "https://www.nba.com"
    assert df.height == len(PBP_PAYLOAD["game"]["actions"])


def test_nba_live_boxscore_raw_returns_dict(monkeypatch):
    transport, _ = _fake_transport(200, json.dumps(BOX_PAYLOAD))
    monkeypatch.setattr(mod, "_curl_transport", transport)
    payload = nba_live_boxscore("0022500001", raw=True)
    assert payload == BOX_PAYLOAD


def test_404_is_no_data(monkeypatch):
    transport, _ = _fake_transport(404, "")
    monkeypatch.setattr(mod, "_curl_transport", transport)
    with pytest.raises(NoDataError):
        nba_live_pbp("0022500001")


def test_blocked_403_is_fetch_error(monkeypatch):
    transport, _ = _fake_transport(403, "<html>captcha</html>")
    monkeypatch.setattr(mod, "_curl_transport", transport)
    with pytest.raises(AssetFetchError):
        nba_live_pbp("0022500001")


def test_s3_access_denied_403_is_no_data(monkeypatch):
    body = (FIX / "playbyplay_0029999999_no_object_s3_403.xml").read_text()
    transport, _ = _fake_transport(403, body)
    monkeypatch.setattr(mod, "_curl_transport", transport)
    with pytest.raises(NoDataError):
        nba_live_boxscore("0022500001")


def test_transport_exception_is_reclassified_as_fetch_error(monkeypatch):
    def raising_transport(url, params, headers, proxy_url):
        raise TimeoutError("curl_cffi timed out")

    monkeypatch.setattr(mod, "_curl_transport", raising_transport)
    with pytest.raises(AssetFetchError):
        nba_live_pbp("0022500001")


def test_missing_curl_cffi_import_error_is_not_masked(monkeypatch):
    """A missing curl_cffi must surface as ImportError, not be reclassified as AssetFetchError."""

    def raising_transport(url, params, headers, proxy_url):
        raise ImportError("curl_cffi is required: pip install curl_cffi")

    monkeypatch.setattr(mod, "_curl_transport", raising_transport)
    with pytest.raises(ImportError):
        nba_live_pbp("0022500001")


# ---------------------------------------------------------------------------
# _fetch_live retry/backoff (H1) + 200-body validation (H2)
# ---------------------------------------------------------------------------


def test_503_then_200_recovers(monkeypatch):
    monkeypatch.setenv("SDV_PY_NBA_STATS_RETRIES", "2")
    transport, calls = _sequence_transport((503, ""), (200, json.dumps(PBP_PAYLOAD)))
    monkeypatch.setattr(mod, "_curl_transport", transport)
    df = nba_live_pbp("0022500001")
    assert df.height == len(PBP_PAYLOAD["game"]["actions"])
    assert calls["n"] == 2


def test_always_503_raises_after_configured_attempts(monkeypatch):
    monkeypatch.setenv("SDV_PY_NBA_STATS_RETRIES", "2")
    transport, calls = _sequence_transport((503, ""))
    monkeypatch.setattr(mod, "_curl_transport", transport)
    with pytest.raises(AssetFetchError):
        nba_live_pbp("0022500001")
    assert calls["n"] == 3  # retries(2) + the first attempt


def test_timeout_then_200_recovers(monkeypatch):
    monkeypatch.setenv("SDV_PY_NBA_STATS_RETRIES", "1")
    transport, calls = _sequence_transport(TimeoutError("curl_cffi timed out"), (200, json.dumps(PBP_PAYLOAD)))
    monkeypatch.setattr(mod, "_curl_transport", transport)
    df = nba_live_pbp("0022500001")
    assert df.height == len(PBP_PAYLOAD["game"]["actions"])
    assert calls["n"] == 2


def test_s3_access_denied_never_retries(monkeypatch):
    monkeypatch.setenv("SDV_PY_NBA_STATS_RETRIES", "3")
    body = (FIX / "playbyplay_0029999999_no_object_s3_403.xml").read_text()
    transport, calls = _sequence_transport((403, body))
    monkeypatch.setattr(mod, "_curl_transport", transport)
    with pytest.raises(NoDataError):
        nba_live_pbp("0022500001")
    assert calls["n"] == 1


def test_import_error_never_retries(monkeypatch):
    monkeypatch.setenv("SDV_PY_NBA_STATS_RETRIES", "3")
    transport, calls = _sequence_transport(ImportError("curl_cffi is required"))
    monkeypatch.setattr(mod, "_curl_transport", transport)
    with pytest.raises(ImportError):
        nba_live_pbp("0022500001")
    assert calls["n"] == 1


def test_200_html_body_always_raises_asset_fetch_error(monkeypatch):
    transport, _ = _fake_transport(200, "<html>akamai interstitial</html>")
    monkeypatch.setattr(mod, "_curl_transport", transport)
    with pytest.raises(AssetFetchError):
        nba_live_pbp("0022500001")


def test_200_empty_body_always_raises_asset_fetch_error(monkeypatch):
    transport, _ = _fake_transport(200, "")
    monkeypatch.setattr(mod, "_curl_transport", transport)
    with pytest.raises(AssetFetchError):
        nba_live_pbp("0022500001")


def test_200_null_body_always_raises_asset_fetch_error(monkeypatch):
    transport, _ = _fake_transport(200, "null")
    monkeypatch.setattr(mod, "_curl_transport", transport)
    with pytest.raises(AssetFetchError):
        nba_live_pbp("0022500001")


def test_200_html_then_valid_json_recovers(monkeypatch):
    monkeypatch.setenv("SDV_PY_NBA_STATS_RETRIES", "1")
    transport, calls = _sequence_transport((200, "<html>akamai interstitial</html>"), (200, json.dumps(PBP_PAYLOAD)))
    monkeypatch.setattr(mod, "_curl_transport", transport)
    df = nba_live_pbp("0022500001")
    assert df.height == len(PBP_PAYLOAD["game"]["actions"])
    assert calls["n"] == 2


_MALFORMED_LIVE = [
    None,
    [],
    "x",
    {"game": []},
    {"game": "x"},
    {"game": {"actions": {}}},
    {"game": {"actions": {"a": 1}}},
    {"game": {"actions": [1, "x", None]}},
    {"game": {"gameId": "abc", "actions": [{"actionNumber": 1}]}},
    {"game": {"homeTeam": [], "awayTeam": "x", "officials": {}}},
    {"game": {"homeTeam": {"teamId": "abc", "players": [1, {"personId": 5}]}, "officials": [1]}},
    # Wrong-typed cells inside well-formed rows (self-review finding 3).
    {"game": {"gameId": "0022500001", "actions": [{"personId": [1, 2]}]}},
    {"game": {"gameId": "0022500001", "actions": [{"clock": [1], "description": ["a"], "period": {"q": 4}}]}},
    {"game": {"gameId": "0022500001", "actions": [{"personId": 1, "person_id": 2}]}},
    {"game": {"gameId": "0022500001", "actions": [{"personId": 1}, {"personId": [1]}]}},
    {
        "game": {
            "gameId": "0022500001",
            "gameStatus": [3],
            "officials": [{"personId": [1]}],
            "homeTeam": {"teamId": [1], "score": {"q": 1}, "players": [{"personId": [5], "name": ["x"]}]},
            "awayTeam": {"teamId": {"id": 2}, "players": [{"personId": 6, "person_id": 7}]},
        }
    },
]
_BOX_CORE = {
    "game": NBA_LIVE_GAME_CORE_SCHEMA,
    "officials": NBA_LIVE_OFFICIALS_CORE_SCHEMA,
    "home_players": NBA_LIVE_PLAYERS_CORE_SCHEMA,
    "away_players": NBA_LIVE_PLAYERS_CORE_SCHEMA,
    "home_team": NBA_LIVE_TEAM_CORE_SCHEMA,
    "away_team": NBA_LIVE_TEAM_CORE_SCHEMA,
}


def _core(df, schema):
    return {name: df.schema.get(name) for name in schema}


@pytest.mark.parametrize("payload", _MALFORMED_LIVE)
def test_malformed_live_payloads_never_raise(payload):
    # The parsers document that malformed envelopes and wrong-typed cells produce
    # frames carrying every core column at its declared dtype.
    pbp = parse_nba_live_pbp(payload)
    assert _core(pbp, NBA_LIVE_PBP_CORE_SCHEMA) == dict(NBA_LIVE_PBP_CORE_SCHEMA)
    box = parse_nba_live_boxscore(payload)
    assert set(box) == set(_BOX_CORE)
    for key, schema in _BOX_CORE.items():
        assert _core(box[key], schema) == dict(schema), key


@pytest.mark.parametrize(
    ("fixture_path", "schema_path"),
    [
        (FIX / "boxscore_0022500001.json", _AUTODOC / "nba" / "nba_live_boxscore.yaml"),
        (
            Path(__file__).parent.parent / "wnba" / "fixtures" / "wnba_live" / "boxscore_1022600097.json",
            _AUTODOC / "wnba" / "wnba_live_boxscore.yaml",
        ),
    ],
    ids=["nba", "wnba"],
)
def test_boxscore_returns_table_documents_every_capture_column(fixture_path, schema_path):
    """Per frame, the hand-authored Returns table lists the core columns first, then
    every other column a real capture produces, at the captured dtype."""
    box = parse_nba_live_boxscore(json.loads(fixture_path.read_text()))
    columns = yaml.safe_load(schema_path.read_text(encoding="utf-8"))["columns"]
    assert [c["name"].split(".", 1)[0] for c in columns] == [k for k, df in box.items() for _ in df.columns]
    for key, df in box.items():
        doc = {c["name"].split(".", 1)[1]: c["type"] for c in columns if c["name"].startswith(f"{key}.")}
        assert list(doc)[: len(_BOX_CORE[key])] == _BOX_CORE[key].names(), key
        assert set(doc) == set(df.columns), key
        for name, dtype in df.schema.items():
            assert doc[name] == _DOC_TYPE[str(dtype)], f"{key}.{name}"


@pytest.mark.parametrize(
    ("action", "col"),
    [({"personId": [1, 2]}, "person_id"), ({"clock": [1]}, "clock"), ({"description": ["a"]}, "description")],
)
def test_wrong_typed_live_cell_is_null(action, col):
    pbp = parse_nba_live_pbp({"game": {"gameId": "0022500001", "actions": [{"actionNumber": 3, **action}]}})
    assert pbp.select("action_number", col).row(0) == (3, None)


def test_colliding_and_list_ids_keep_the_rows():
    # personId and person_id snake-case to one column: the first key wins.
    pbp = parse_nba_live_pbp({"game": {"gameId": "0022500001", "actions": [{"personId": 7, "person_id": 8}]}})
    assert pbp["person_id"].to_list() == [7]
    box = parse_nba_live_boxscore(
        {"game": {"gameId": "0022500001", "homeTeam": {"teamId": [1], "players": [{"personId": 5}]}}}
    )
    assert box["home_players"].select("team_id", "person_id").row(0) == (None, 5)
    assert box["game"]["home_team_id"].to_list() == [None]


def test_boxscore_game_table_game_id_is_zero_padded_like_the_rest():
    box = parse_nba_live_boxscore({"game": {"gameId": 22500001, "gameStatus": 3, "officials": [{"personId": 1}]}})
    assert box["game"]["game_id"].to_list() == ["0022500001"] == box["officials"]["game_id"].to_list()


# ---------------------------------------------------------------------------
# Fetch contract: bad ids, bodies without `game`, and the "fetch failed" marker
# ---------------------------------------------------------------------------

_LIVE_FETCHERS = [nba_live_pbp, nba_live_boxscore, wnba_live_pbp, wnba_live_boxscore]


@pytest.mark.parametrize("gid", [True, -1, "-1", "4_2500405", " 22500001", "+22500001", "abc", 22500001.5])
@pytest.mark.parametrize("fetch", _LIVE_FETCHERS)
def test_bad_game_id_is_value_error_before_any_request(monkeypatch, fetch, gid):
    transport, calls = _sequence_transport((200, json.dumps(PBP_PAYLOAD)))
    monkeypatch.setattr(mod, "_curl_transport", transport)
    with pytest.raises(ValueError, match="integer id"):
        fetch(gid)
    assert calls["n"] == 0


@pytest.mark.parametrize("body", ['{"code": "rest_forbidden"}', '{"game": []}', '{"game": "x"}', '{"meta": {"v": 1}}'])
@pytest.mark.parametrize("fetch", _LIVE_FETCHERS)
def test_200_without_the_game_object_is_asset_fetch_error(monkeypatch, fetch, body):
    transport, _ = _fake_transport(200, body)
    monkeypatch.setattr(mod, "_curl_transport", transport)
    with pytest.raises(AssetFetchError, match="no `game` object"):
        fetch("0022500001")
    with pytest.raises(AssetFetchError, match="no `game` object"):
        fetch("0022500001", raw=True)


@pytest.mark.parametrize(
    ("response", "blocked"),
    [
        (TimeoutError("curl_cffi timed out"), True),
        ((403, "<html>captcha</html>"), True),
        ((503, ""), True),
        ((200, "<html>akamai interstitial</html>"), False),
        ((200, '{"code": "rest_forbidden"}'), False),
    ],
)
def test_only_failed_fetches_say_fetch_failed(monkeypatch, response, blocked):
    # tests/nba/test_nba_officiating_live.py skips only an AssetFetchError that says
    # "fetch failed"; a 200 with the wrong body is drift and must fail the live run.
    transport, _ = _sequence_transport(response)
    monkeypatch.setattr(mod, "_curl_transport", transport)
    with pytest.raises(AssetFetchError) as err:
        nba_live_pbp("0022500001")
    assert ("fetch failed" in str(err.value)) is blocked


def test_fractional_ids_are_null_not_truncated():
    df = mod._normalize([{"personId": 1628983.5}, {"personId": 1628983.0}, {"personId": "1628983"}])
    assert df["person_id"].to_list() == [None, 1628983, 1628983]
    box = parse_nba_live_boxscore(
        {"game": {"gameId": "0022500001", "homeTeam": {"teamId": 1610612737.5, "players": [{"personId": 1}]}}}
    )
    assert box["home_players"]["team_id"].to_list() == [None]


def test_fractional_core_integers_are_null_not_truncated():
    df = mod._ensure_core_schema(
        pl.DataFrame({"period": [4.5, 4.0], "clock": ["PT00M01.00S"] * 2}), NBA_LIVE_PBP_CORE_SCHEMA
    )
    assert df["period"].to_list() == [None, 4]
    assert df.schema["period"] == pl.Int64
