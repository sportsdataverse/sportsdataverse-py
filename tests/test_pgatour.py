"""Offline tests for the PGA TOUR GraphQL family (``pgatour_*``): runtime getter and parsers."""

from __future__ import annotations

import base64
import gzip
import json
from pathlib import Path

import pandas as pd
import polars as pl
import pytest

from sportsdataverse.errors import AssetFetchError, NoDataError
from sportsdataverse.golf import pgatour_parsers as P
from sportsdataverse.golf import pgatour_runtime as rt

FIXTURES = Path(__file__).parent / "fixtures" / "pgatour"

CASES = [
    ("schedule", "schedule_R_2026.json", P.SCHEDULE_SCHEMA, 40),
    ("leaderboard", "leaderboard_R2026014.json", P.LEADERBOARD_SCHEMA, 91),
    ("scorecard", "scorecard_R2026014_28237.json", P.SCORECARD_SCHEMA, 72),
    ("tee_times", "tee_times_R2026014.json", P.TEE_TIMES_SCHEMA, 250),
    ("player", "player_28237.json", P.PLAYER_SCHEMA, 1),
    ("player_directory", "player_directory_R.json", P.PLAYER_DIRECTORY_SCHEMA, 200),
    ("stat_catalog", "stat_catalog_R_2026.json", P.STAT_CATALOG_SCHEMA, 300),
    ("stat_details", "stat_details_120_R_2026.json", P.STAT_DETAILS_SCHEMA, 500),
    ("tour_cup", "tour_cup_02671_R_2025.json", P.TOUR_CUP_SCHEMA, 200),
    ("shot_details", "shot_details_R2026006_59095_r4.json", P.SHOT_DETAILS_SCHEMA, 50),
]


def _load(name: str) -> dict:
    return json.loads((FIXTURES / name).read_text(encoding="utf-8"))


@pytest.mark.parametrize("short, fixture, schema, min_rows", CASES)
def test_parser_rows_and_schema(short, fixture, schema, min_rows):
    parse = getattr(P, f"parse_pgatour_{short}")
    df = parse(_load(fixture))
    assert df.schema == pl.Schema(schema)
    assert df.height >= min_rows
    assert isinstance(parse(_load(fixture), return_as_pandas=True), pd.DataFrame)


@pytest.mark.parametrize("short, fixture, schema, min_rows", CASES)
@pytest.mark.parametrize("payload", [None, {}, [], "x", {"payload": {}}, {"players": "x", "rounds": [1]}])
def test_empty_or_malformed_is_zero_rows_with_schema(short, fixture, schema, min_rows, payload):
    df = getattr(P, f"parse_pgatour_{short}")(payload)
    assert df.height == 0
    assert df.schema == pl.Schema(schema)


def test_ids_are_strings_and_join():
    lb = P.parse_pgatour_leaderboard(_load("leaderboard_R2026014.json"))
    sc = P.parse_pgatour_scorecard(_load("scorecard_R2026014_28237.json"))
    tt = P.parse_pgatour_tee_times(_load("tee_times_R2026014.json"))
    for df in (lb, sc, tt):
        assert df.schema["player_id"] == pl.String and df.schema["tournament_id"] == pl.String
    assert sc["player_id"].unique().to_list() == ["28237"]
    assert set(sc["player_id"]) <= set(lb["player_id"]) and set(tt["player_id"]) >= set(lb["player_id"])


def test_leaderboard_winner_and_cut_rows():
    lb = P.parse_pgatour_leaderboard(_load("leaderboard_R2026014.json"))
    top = lb.row(0, named=True)
    assert (top["player_id"], top["position"], top["to_par"], top["total_strokes"]) == ("28237", "1", -12, 276)
    assert top["total_strokes"] == sum(top[f"round_{i}"] for i in range(1, 5))
    cut = lb.filter(pl.col("position") == "CUT")
    assert cut.height > 0 and cut["round_3"].null_count() == cut.height
    assert lb["sort_order"].is_sorted()


def test_scorecard_sums_to_leaderboard_total():
    sc = P.parse_pgatour_scorecard(_load("scorecard_R2026014_28237.json"))
    assert sc.group_by("round_number").len()["len"].to_list() == [18] * 4
    assert sc["score"].sum() == 276


def test_stat_details_long_form():
    sd = P.parse_pgatour_stat_details(_load("stat_details_120_R_2026.json"))
    assert set(sd["stat_name"]) == {"Avg", "Total Strokes", "Total Adjustment", "Total Rounds"}
    assert sd.group_by("player_id").len()["len"].max() == 4
    leader = sd.filter((pl.col("rank") == 1) & (pl.col("stat_name") == "Avg"))
    assert leader["player_id"].to_list() == ["46046"]


def test_shot_details_strokes_and_coordinates():
    shots = P.parse_pgatour_shot_details(_load("shot_details_R2026006_59095_r4.json"))
    assert shots.group_by("hole_number").len().height == 18
    per_hole = shots.group_by("hole_number").agg(pl.len().alias("n"), pl.col("hole_score").first())
    assert (per_hole["n"] >= per_hole["hole_score"]).all()  # penalty / drop rows can add lines
    assert shots["from_x"].null_count() < shots.height


def _compressed(value: dict) -> dict:
    blob = base64.b64encode(gzip.compress(json.dumps(value).encode())).decode()
    return {"id": "x", "payload": blob}


def test_decode_payload_round_trip_and_bad_payload():
    body = _load("leaderboard_R2026014.json")["payload"]
    decoded = rt.decode_payload(_compressed(body))
    assert decoded["payload"] == body
    assert rt.decode_payload({"id": "x"}) == {"id": "x"}
    with pytest.raises(AssetFetchError):
        rt.decode_payload({"id": "x", "payload": "not-base64-gzip"})


def test_every_wrapper_has_a_query_document():
    import inspect
    import re

    from sportsdataverse.golf import pgatour

    names = [n for n in pgatour.__all__ if n.startswith("pgatour_")]
    assert len(names) == 10
    for name in names:
        src = inspect.getsource(getattr(pgatour, name))
        assert '"https://orchestrator.pgatour.com/graphql"' in src
        operation = re.search(r'"operation": "(\w+)"', src).group(1)
        doc = rt.query_document(operation)
        assert doc.startswith(f"query {operation[0].upper()}{operation[1:]}")
        assert f"  {operation}(" in doc


def test_variables_take_the_declared_type():
    assert rt._variables("schedule", {"tourCode": "R", "year": 2026, "x": None}) == {"tourCode": "R", "year": "2026"}
    assert rt._variables("statDetails", {"year": "2026", "statId": 120}) == {"year": 2026, "statId": "120"}
    assert rt._variables("shotDetailsV4Compressed", {"round": 4.0, "playerId": 59095}) == {
        "round": 4,
        "playerId": "59095",
    }
    for bad in ("last", 2026.7, True):
        with pytest.raises(ValueError):
            rt._variables("statDetails", {"year": bad})


def test_graphql_body_wraps_the_document():
    body = rt.graphql_body({"operation": "statDetails", "tourCode": "R", "statId": "120", "year": None})
    assert body["operationName"] == "StatDetails"
    assert body["variables"] == {"tourCode": "R", "statId": "120"}
    assert body["query"].startswith("query StatDetails(")
    with pytest.raises(ValueError):
        rt.graphql_body({"operation": "noSuchQuery"})


def test_get_posts_the_document_with_the_key(monkeypatch):
    seen: dict = {}

    def fake_http(url, json=None, headers=None, **kwargs):
        seen.update(url=url, body=json, headers=headers, kwargs=kwargs)
        return {"data": {"leaderboardCompressedV3": _compressed({"tournamentId": "R1", "players": []})}}

    monkeypatch.setattr(rt, "_http_get", fake_http)
    monkeypatch.setenv(rt.KEY_ENV, "test-key")
    body = rt.graphql_body({"operation": "leaderboardCompressedV3", "id": "R1"})
    out = rt._get(rt.GRAPHQL_URL, params={}, json=body)
    assert seen["url"] == rt.GRAPHQL_URL
    assert seen["headers"]["x-api-key"] == "test-key"
    assert seen["body"]["variables"] == {"id": "R1"}
    assert "leaderboardCompressedV3(id: $id)" in seen["body"]["query"]
    assert 403 not in seen["kwargs"]["retry_statuses"]
    assert out["payload"] == {"tournamentId": "R1", "players": []}


def test_wrapper_end_to_end_through_the_getter(monkeypatch):
    from sportsdataverse.golf import pgatour

    def fake_http(url, json=None, headers=None, **kwargs):
        assert json["operationName"] == "StatDetails" and json["variables"]["statId"] == "120"
        return {"data": {"statDetails": _load("stat_details_120_R_2026.json")}}

    monkeypatch.setattr(rt, "_http_get", fake_http)
    monkeypatch.setenv(rt.KEY_ENV, "test-key")
    df = pgatour.pgatour_stat_details(stat_id=120, year=2026)
    assert df.height > 500 and df.schema["player_id"] == pl.String


def test_refused_site_key_is_read_again_once(monkeypatch):
    keys = iter(["old-key", "new-key"])
    calls: list = []

    def fake_http(url, json=None, headers=None, **kwargs):
        calls.append(headers["x-api-key"])
        if headers["x-api-key"] == "old-key":
            raise AssetFetchError("https://orchestrator.pgatour.com/graphql answered HTTP 403")
        return {"data": {"player": {"id": "1"}}}

    monkeypatch.delenv(rt.KEY_ENV, raising=False)
    monkeypatch.setattr(rt, "_site_api_key", _Cached(lambda: next(keys)))
    monkeypatch.setattr(rt, "_http_get", fake_http)
    assert rt._get(rt.GRAPHQL_URL, json=rt.graphql_body({"operation": "player", "id": "1"})) == {"id": "1"}
    assert calls == ["old-key", "new-key"]


class _Cached:
    """A stand-in for the lru_cache'd key reader: ``cache_clear`` makes the next call read anew."""

    def __init__(self, read):
        self.read, self.value = read, None

    def __call__(self):
        if self.value is None:
            self.value = self.read()
        return self.value

    def cache_clear(self):
        self.value = None


def test_malformed_field_values_never_raise():
    body = {
        "tournamentId": {"x": 1},
        "tournamentStatus": [1, 2],
        "players": [{"__typename": "PlayerRowV3", "id": 7, "player": {"amateur": "yes", "country": {"a": 1}},
                     "scoringData": {"totalStrokes": float("nan"), "rounds": "x", "total": ["-1"]}}],
    }  # fmt: skip
    df = P.parse_pgatour_leaderboard({"payload": body})
    row = df.row(0, named=True)
    assert (row["player_id"], row["tournament_id"], row["amateur"], row["country"]) == ("7", None, None, None)
    assert row["total_strokes"] is None and row["round_1"] is None


@pytest.mark.parametrize(
    "answer, error",
    [
        ({"data": {"player": None}}, NoDataError),
        ({"data": None, "errors": [{"message": "Validation error of type WrongType"}]}, ValueError),
        ({"data": None, "errors": [{"message": "Internal failure"}]}, AssetFetchError),
    ],
)
def test_get_maps_graphql_errors(monkeypatch, answer, error):
    monkeypatch.setattr(rt, "_http_get", lambda *a, **k: answer)
    monkeypatch.setenv(rt.KEY_ENV, "test-key")
    with pytest.raises(error):
        rt._get(rt.GRAPHQL_URL, json=rt.graphql_body({"operation": "player", "id": "1"}))


def test_site_key_is_read_from_the_app_bundle(monkeypatch):
    pages = {
        rt.SITE_URL: '<script src="/_next/static/chunks/pages/_app-0123abcd.js"></script>',
        "https://www.pgatour.com/_next/static/chunks/pages/_app-0123abcd.js": (
            '{"api":{"dev":{"apiKey":"dev-key"},"prod":{"comment":"live","apiKey":"prod-key","queryEndpoint":"x"}}}'
        ),
    }
    monkeypatch.setattr(rt, "_text", lambda url: pages[url])
    monkeypatch.delenv(rt.KEY_ENV, raising=False)
    rt._site_api_key.cache_clear()
    try:
        assert rt.api_key() == "prod-key"
    finally:
        rt._site_api_key.cache_clear()


def test_site_key_missing_raises(monkeypatch):
    monkeypatch.setattr(rt, "_text", lambda url: "<html></html>")
    rt._site_api_key.cache_clear()
    try:
        with pytest.raises(AssetFetchError, match="PGA_API_KEY"):
            rt._site_api_key()
    finally:
        rt._site_api_key.cache_clear()
