import datetime as dt
import json
from pathlib import Path

import pandas as pd
import polars as pl
import pytest
import requests

from sportsdataverse.errors import AssetFetchError, NoDataError
from sportsdataverse.nba import nba_officiating as mod
from sportsdataverse.nba.nba_officiating import (
    L2M_CALLS_SCHEMA,
    L2M_GAME_SCHEMA,
    L2M_STATS_SCHEMA,
    NBA_REFEREE_ASSIGN_SCHEMA,
    NBA_REFEREE_REPLAY_SCHEMA,
    nba_l2m,
    nba_referee_assignments,
    parse_nba_l2m,
    parse_nba_l2m_games,
    parse_nba_referee_assignments,
)

FIX = Path(__file__).parent / "fixtures" / "official_nba"


class _Resp:
    def __init__(self, status, body, ctype):
        self.status_code, self.text, self.headers = status, body, {"Content-Type": ctype}
        self.content = body.encode()

    def json(self):
        return json.loads(self.text)


def _patch(monkeypatch, resp):
    seen = {}

    def fake_download(url, **kw):
        seen.update(kw, url=url)
        return resp

    monkeypatch.setattr(mod, "download", fake_download)
    return seen


def test_browser_ua_and_no_403_retry(monkeypatch):
    seen = _patch(monkeypatch, _Resp(200, "{}", "application/json"))
    mod._official_get("https://official.nba.com/l2m/json/0042500405.json")
    assert "Mozilla/5.0" in seen["headers"]["User-Agent"]
    assert 403 not in seen["retry_statuses"]


def test_s3_xml_403_is_no_data(monkeypatch):
    body = (FIX / "l2m_json_0022500002_no_report_s3_403.xml").read_text()
    _patch(monkeypatch, _Resp(403, body, "application/xml"))
    with pytest.raises(NoDataError):
        mod._official_get("https://official.nba.com/l2m/json/0022500002.json")


def test_akamai_html_403_is_fetch_error(monkeypatch):
    body = (FIX / "akamai_403_blocked_ua.html").read_text()
    _patch(monkeypatch, _Resp(403, body, "text/html"))
    with pytest.raises(AssetFetchError):
        mod._official_get("https://official.nba.com/l2m/json/0042500405.json")


def test_errors_are_disjoint():
    assert not issubclass(NoDataError, AssetFetchError)
    assert not issubclass(AssetFetchError, NoDataError)


def _payload():
    return json.loads((FIX / "l2m_json_0042500405.json").read_text(encoding="utf-8"))


def test_parse_real_capture():
    out = parse_nba_l2m(_payload())
    calls, game, stats = out["calls"], out["game"], out["stats"]
    assert calls.height == 21 and stats.height == 3 and game.height == 1
    assert calls["decision"].value_counts(sort=True).rows() == [("CNC", 13), ("CC", 7), ("INC", 1)]
    first = calls.row(0, named=True)
    assert first["game_id"] == "0042500405"
    assert first["period"] == 4 and first["pc_time"] == "01:54.0"
    assert first["seconds_remaining"] == 114.0
    assert (first["call"], first["type"]) == ("FOUL", "SHOOTING")
    assert (first["committing"], first["disadvantaged"]) == ("Knicks", "Victor Wembanyama")
    assert game.row(0, named=True)["season_type"] == "playoffs"
    assert game["game_date"].to_list()[0].isoformat() == "2026-06-13"


def test_schema_is_stable_on_empty():
    out = parse_nba_l2m({})
    assert out["calls"].height == 0
    assert out["calls"].schema == L2M_CALLS_SCHEMA


def test_decision_normalization():
    p = _payload()
    for row, raw in zip(p["l2m"][:5], ["NCC", "NCI", "Undetectable", "", "CC*"]):
        row["CallRatingName"] = raw
    got = parse_nba_l2m(p)["calls"]["decision"].to_list()[:5]
    assert got == ["CNC", "INC", None, None, "CC"]


def test_names_kept_verbatim():
    p = _payload()
    p["l2m"][0]["DP"] = "Nikola Jokić"
    assert parse_nba_l2m(p)["calls"]["disadvantaged"][0] == "Nikola Jokić"


def test_game_id_zero_padded_from_int(monkeypatch):
    seen = {}

    def fake_get(url, **kw):
        seen["url"] = url
        return _Resp(200, json.dumps(_payload()), "application/json")

    monkeypatch.setattr(mod, "_official_get", fake_get)
    nba_l2m(42500405)
    assert seen["url"].endswith("/l2m/json/0042500405.json")


def test_listing_2025_26():
    html = (FIX / "l2m_listing_2025-26.html").read_text(encoding="utf-8")
    df = parse_nba_l2m_games(html, 2026)
    assert df.height == 415  # 416 links, one duplicate
    assert df["game_id"].n_unique() == 415
    assert df["season_type"].value_counts(sort=True).rows() == [("regular", 386), ("playoffs", 26), ("play-in", 3)]
    assert df.row(0, named=True) == {
        "game_id": "0042500405",
        "season": 2026,
        "season_type": "playoffs",
        "label": "Knicks 94, Spurs 90",
    }


def test_listing_url_uses_span(monkeypatch):
    seen = {}

    def fake_get(url, **kw):
        seen["url"] = url
        return _Resp(200, "<html><title>2025-26 NBA Officiating Last Two Minute Reports</title></html>", "text/html")

    monkeypatch.setattr(mod, "_official_get", fake_get)
    mod.nba_l2m_games(2026)
    assert seen["url"] == "https://official.nba.com/2025-26-nba-officiating-last-two-minute-reports/"


def test_listing_without_marker_raises_asset_fetch_error(monkeypatch):
    """H3: a 200 body missing the 'Last Two Minute' marker (Akamai interstitial, blank
    body, or a redesigned page) must not silently parse to zero games."""

    def fake_get(url, **kw):
        return _Resp(200, "<html><body>please verify you are a human</body></html>", "text/html")

    monkeypatch.setattr(mod, "_official_get", fake_get)
    with pytest.raises(AssetFetchError):
        mod.nba_l2m_games(2026)


def test_listing_via_wrapper_real_fixture_still_yields_415(monkeypatch):
    """H3 regression guard: the marker check must not reject the real capture."""
    html = (FIX / "l2m_listing_2025-26.html").read_text(encoding="utf-8")

    def fake_get(url, **kw):
        return _Resp(200, html, "text/html")

    monkeypatch.setattr(mod, "_official_get", fake_get)
    df = mod.nba_l2m_games(2026)
    assert df.height == 415


def _assign():
    return json.loads((FIX / "referee_assignments_2026-06-13.json").read_text(encoding="utf-8"))


def test_assignments_nba_long_format():

    out = parse_nba_referee_assignments(_assign(), "nba")
    o = out["officials"]
    assert o.height == 4  # 1 game x 4 slots
    chief = o.filter(pl.col("crew_position") == 1).row(0, named=True)
    assert (chief["official_id"], chief["official_name"], chief["jersey_num"]) == (1162, "Scott Foster", "48")
    assert (chief["season"], chief["season_type"], chief["game_id"]) == (2026, "playoffs", "0042500405")
    assert out["replay_center"].height == 1


def test_assignments_empty_league_keeps_schema():

    out = parse_nba_referee_assignments(_assign(), "gl")
    assert out["officials"].height == 0 and out["officials"].schema == NBA_REFEREE_ASSIGN_SCHEMA


def test_assignments_wnba_rows():

    assert parse_nba_referee_assignments(_assign(), "wnba")["officials"]["game_id"].n_unique() == 4


def test_assignments_invalid_league_raises():

    with pytest.raises(ValueError, match="league must be"):
        nba_referee_assignments("2026-06-13", league="bogus")


def test_assignments_wnba_season_no_increment():

    out = parse_nba_referee_assignments(_assign(), "wnba")
    wnba_officials = out["officials"]
    assert (wnba_officials["season"] == 2026).all()
    assert wnba_officials["season_type"].unique().to_list() == ["regular"]


def test_parse_assignments_invalid_league_raises():
    with pytest.raises(ValueError, match="league must be"):
        parse_nba_referee_assignments(_assign(), "bogus")


# ---------------------------------------------------------------------------
# _official_get -- requests.exceptions.* reclassified as AssetFetchError
# ---------------------------------------------------------------------------


def test_connection_error_is_asset_fetch_error(monkeypatch):
    def raising_download(url, **kw):
        raise requests.exceptions.ConnectionError("connection reset")

    monkeypatch.setattr(mod, "download", raising_download)
    with pytest.raises(AssetFetchError, match="fetch failed"):
        mod._official_get("https://official.nba.com/l2m/json/0042500405.json")


@pytest.mark.parametrize(
    ("status", "body", "call", "blocked"),
    [
        (403, "<html>Access Denied</html>", lambda: nba_l2m("0042500405"), True),
        (503, "", lambda: nba_referee_assignments("2026-06-13"), True),
        (200, '{"message": "error"}', lambda: nba_referee_assignments("2026-06-13"), False),
        (200, '{"code": "rest_forbidden"}', lambda: nba_l2m("0042500405"), False),
        (200, "<html>not json</html>", lambda: nba_l2m("0042500405"), False),
        (200, "<html>verify you are human</html>", lambda: mod.nba_l2m_games(2026), False),
    ],
)
def test_only_failed_fetches_say_fetch_failed(monkeypatch, status, body, call, blocked):
    # tests/nba/test_nba_officiating_live.py skips an AssetFetchError only when it says
    # "fetch failed" (a transport error or a non-200 status); schema drift must not say
    # it, so drift fails the live run instead of reading as an IP block.
    _patch(monkeypatch, _Resp(status, body, "text/html"))
    with pytest.raises(AssetFetchError) as err:
        call()
    assert ("fetch failed" in str(err.value)) is blocked


def test_l2m_non_json_200_body_is_asset_fetch_error(monkeypatch):
    _patch(monkeypatch, _Resp(200, "<html>not json</html>", "text/html"))
    with pytest.raises(AssetFetchError):
        nba_l2m("0042500405")


def test_referee_assignments_non_json_200_body_is_asset_fetch_error(monkeypatch):
    _patch(monkeypatch, _Resp(200, "<html>not json</html>", "text/html"))
    with pytest.raises(AssetFetchError):
        nba_referee_assignments("2026-06-13")


# ---------------------------------------------------------------------------
# nba_referee_assignments -- datetime normalization (Minor 4)
# ---------------------------------------------------------------------------


def test_referee_assignments_datetime_with_time_normalizes_to_date(monkeypatch):
    seen = {}

    def fake_get(url, **kw):
        seen["url"], seen["params"] = url, kw.get("params")
        return _Resp(200, json.dumps(_assign()), "application/json")

    monkeypatch.setattr(mod, "_official_get", fake_get)
    nba_referee_assignments(dt.datetime(2026, 6, 13, 19, 30))
    assert seen["url"] == mod._ASSIGN_URL  # H4: date travels as params=, not hand-built into the URL
    assert seen["params"] == {"date": "2026-06-13"}


def test_referee_assignments_plain_date_still_works(monkeypatch):
    seen = {}

    def fake_get(url, **kw):
        seen["url"], seen["params"] = url, kw.get("params")
        return _Resp(200, json.dumps(_assign()), "application/json")

    monkeypatch.setattr(mod, "_official_get", fake_get)
    nba_referee_assignments(dt.date(2026, 6, 13))
    assert seen["url"] == mod._ASSIGN_URL
    assert seen["params"] == {"date": "2026-06-13"}


@pytest.mark.parametrize("bad", ["2026-02-31", "06/13/2026", "2026-6-13", "20260613", ""])
def test_referee_assignments_invalid_date_raises_before_request(monkeypatch, bad):
    def fake_get(url, **kw):
        raise AssertionError("no request expected")

    monkeypatch.setattr(mod, "_official_get", fake_get)
    with pytest.raises(ValueError, match="YYYY-MM-DD"):
        nba_referee_assignments(bad)


@pytest.mark.parametrize(
    "payload",
    [
        {"nba": {"Table": {"rows": []}, "Table1": {"rows": []}}},  # no wnba block
        {"wnba": {"Table": {"rows": []}}},  # block without Table1
        {"message": "error"},  # error envelope
        {"wnba": {"Table": None, "Table1": {"rows": []}}},  # null table
        {"wnba": {"Table": {}, "Table1": {"rows": []}}},  # table without rows
        {"wnba": {"Table": {"rows": None}, "Table1": {"rows": []}}},  # null rows
        {"wnba": {"Table": {"rows": {"a": 1}}, "Table1": {"rows": []}}},  # rows is an object
        {"wnba": {"Table": {"rows": [1, 2]}, "Table1": {"rows": []}}},  # rows are not objects
        # A replay-center row must name its official, or it parses to a row of nulls.
        {"wnba": {"Table": {"rows": []}, "Table1": {"rows": [{}]}}},
        {"wnba": {"Table": {"rows": []}, "Table1": {"rows": [{"game_date": "06/13/2026", "official": "A Ref"}]}}},
        {"wnba": {"Table": {"rows": []}, "Table1": {"rows": [{"replaycenter_official": " "}]}}},
    ],
)
def test_referee_assignments_missing_league_block_is_asset_fetch_error(monkeypatch, payload):
    # The live feed carries every league's Table/Table1 block, each with a rows list,
    # on every date (zero rows on a day without games), so anything else is a failed
    # fetch, not an empty day -- including under raw=True, which a capture job stores.
    # nba and gl are well-formed, so the wnba block is the only defect: the raw=True
    # half fails if wnba is dropped from the three-league check.
    ok = {"Table": {"rows": []}, "Table1": {"rows": []}}
    payload = {"nba": ok, "gl": ok, **payload}
    monkeypatch.setattr(mod, "_official_get", lambda url, **kw: _Resp(200, json.dumps(payload), "application/json"))
    with pytest.raises(AssetFetchError):
        nba_referee_assignments("2026-06-13", league="wnba")
    with pytest.raises(AssetFetchError):
        nba_referee_assignments("2026-06-13", league="wnba", raw=True)


def test_referee_assignments_empty_block_is_an_empty_day(monkeypatch):
    payload = {"wnba": {"Table": {"rows": []}, "Table1": {"rows": []}}}
    monkeypatch.setattr(mod, "_official_get", lambda url, **kw: _Resp(200, json.dumps(payload), "application/json"))
    out = nba_referee_assignments("2026-06-13", league="wnba")
    assert out["officials"].height == 0
    assert out["replay_center"].height == 0


# ---------------------------------------------------------------------------
# L2M calls.period dtype -- Ruling R7 (Minor 9)
# ---------------------------------------------------------------------------


def test_calls_period_dtype_is_int64():
    out = parse_nba_l2m(_payload())
    assert out["calls"].schema["period"] == pl.Int64
    assert L2M_CALLS_SCHEMA["period"] == pl.Int64


# ---------------------------------------------------------------------------
# parse_nba_l2m -- D2: None payload / non-numeric GameId must not raise
# ---------------------------------------------------------------------------


def test_none_payload_is_zero_rows():
    out = parse_nba_l2m(None)
    assert out["calls"].height == 0
    assert out["game"].height == 0
    assert out["stats"].height == 0


def test_non_numeric_game_id_is_kept_raw_not_raised():
    p = _payload()
    p["game"][0]["GameId"] = "ABC123"
    out = parse_nba_l2m(p)
    assert out["game"]["game_id"][0] == "ABC123"
    assert out["calls"]["game_id"][0] == "ABC123"


@pytest.mark.parametrize("gid", ["X", "AB"])
def test_short_non_numeric_game_id_gives_null_season_type_not_index_error(gid):
    p = _payload()
    p["game"][0]["GameId"] = gid
    out = parse_nba_l2m(p)
    assert out["game"]["game_id"][0] == gid
    assert out["game"]["season_type"][0] is None


def test_malformed_game_date_is_null_not_raised():
    p = _payload()
    p["game"][0]["GameDate"] = "not-a-date"
    out = parse_nba_l2m(p)
    assert out["game"].height == 1
    assert out["game"]["game_date"][0] is None


@pytest.mark.parametrize("body", ["null", "[]", '"error"', "42"])
def test_non_object_json_200_is_asset_fetch_error(monkeypatch, body):
    # Valid JSON that is not an object must not reach .get() (AttributeError) or be
    # handed back from raw=True as a non-dict.
    _patch(monkeypatch, _Resp(200, body, "application/json"))
    with pytest.raises(AssetFetchError):
        nba_l2m("0042500405", raw=True)
    with pytest.raises(AssetFetchError):
        nba_referee_assignments("2026-06-13")


@pytest.mark.parametrize("gid", [42500405.5, 1.25])
def test_fractional_game_id_is_rejected_not_truncated(monkeypatch, gid):
    # int() would truncate 42500405.5 to another game's id; both helpers must refuse.
    monkeypatch.setattr(
        mod, "_official_get", lambda url, **kw: (_ for _ in ()).throw(AssertionError("no request expected"))
    )
    with pytest.raises(ValueError, match="integer id"):
        nba_l2m(gid)
    from sportsdataverse.nba import nba_live

    with pytest.raises(ValueError, match="integer id"):
        nba_live._gid(gid)
    assert nba_live._gid(42500405.0) == "0042500405"


def test_referee_assignments_row_without_game_id_is_asset_fetch_error(monkeypatch):
    payload = {"nba": {"Table": {"rows": [{"official1": "A Ref", "season": "22025"}]}, "Table1": {"rows": []}}}
    monkeypatch.setattr(mod, "_official_get", lambda url, **kw: _Resp(200, json.dumps(payload), "application/json"))
    with pytest.raises(AssetFetchError):
        nba_referee_assignments("2026-06-13")


def test_referee_assignments_raw_requires_all_three_leagues(monkeypatch):
    ok = {"Table": {"rows": []}, "Table1": {"rows": []}}
    payload = {"wnba": ok}  # well-formed for the requested league only
    monkeypatch.setattr(mod, "_official_get", lambda url, **kw: _Resp(200, json.dumps(payload), "application/json"))
    assert nba_referee_assignments("2026-06-13", league="wnba")["officials"].height == 0
    with pytest.raises(AssetFetchError):
        nba_referee_assignments("2026-06-13", league="wnba", raw=True)


def test_parse_referee_assignments_tolerates_malformed_rows():
    # The parser itself never raises: no game_id -> null, odd season code -> null,
    # non-dict rows and a non-dict payload are skipped.
    payload = {"nba": {"Table": {"rows": [{"official1": "A Ref", "season": "2X025"}, 7]}, "Table1": {"rows": ["x"]}}}
    out = parse_nba_referee_assignments(payload, "nba")
    assert out["officials"].height == 1
    assert out["officials"]["game_id"][0] is None
    assert out["officials"]["season"][0] is None
    assert out["replay_center"].height == 0
    assert parse_nba_referee_assignments(["not", "a", "dict"], "nba")["officials"].height == 0


# ---------------------------------------------------------------------------
# Parser contract: malformed input never raises (self-review findings 1-2)
# ---------------------------------------------------------------------------

_L2M_SCHEMAS = {"calls": L2M_CALLS_SCHEMA, "game": L2M_GAME_SCHEMA, "stats": L2M_STATS_SCHEMA}
_MALFORMED_L2M = [
    ["x"],
    "x",
    {"game": "x"},
    {"game": [1]},
    {"game": {"GameId": "0042500405"}},
    {"l2m": [1]},
    {"l2m": {"a": 1}},
    {"stats": ["x"]},
    {"l2m": [{"posTeamId": ""}]},
    {"l2m": [{"posID": "abc"}]},
    {"l2m": [{"Comment": {"a": 1}}]},
    {"l2m": [{"Comment": ["a"]}]},
    {"l2m": [{"PeriodName": "Q99999999999999999999"}]},
    {"game": [{"GameId": "0042500405", "HomeTeamScore": "", "HomeTeamId": 10**30}]},
    {"stats": [{"stats_name": "Calls", "home": "", "away": "-"}]},
]


@pytest.mark.parametrize("payload", _MALFORMED_L2M)
def test_malformed_l2m_payloads_never_raise(payload):
    out = parse_nba_l2m(payload)
    assert {k: v.schema for k, v in out.items()} == _L2M_SCHEMAS


def test_l2m_bad_cells_become_null_and_good_cells_survive():
    p = _payload()
    p["l2m"][0].update(posTeamId="", Comment={"note": "x"})
    p["game"][0]["HomeTeamScore"] = ""
    out = parse_nba_l2m(p)
    first = out["calls"].row(0, named=True)
    assert first["pos_team_id"] is None and first["comment"] == '{"note": "x"}'
    assert (first["pos_id"], first["period"], first["game_id"]) == (p["l2m"][0]["posID"], 4, "0042500405")
    game = out["game"].row(0, named=True)
    assert game["home_score"] is None and game["away_score"] == p["game"][0]["VisitorTeamScore"]
    assert game["game_date"].isoformat() == "2026-06-13"


def test_l2m_fetch_with_a_bad_cell_parses_instead_of_raising(monkeypatch):
    p = _payload()
    p["l2m"][0]["posTeamId"] = ""
    _patch(monkeypatch, _Resp(200, json.dumps(p), "application/json"))
    assert nba_l2m("0042500405")["calls"]["pos_team_id"][0] is None


_REF_ROW = {
    "game_id": "0042500405",
    "game_date": "06/13/2026",
    "season": "42025",
    "official1": "Scott Foster",
    "official1_code": 1162,
    "home_team_id": 1610612759,
}
_REPLAY_ROW = {"game_date": "06/13/2026", "official_code": 1627541, "replaycenter_official": "John Goble"}


@pytest.mark.parametrize(
    ("row", "replay", "table", "col", "expected"),
    [
        ({"game_date": 20260613}, {}, "officials", "game_date", None),
        ({}, {"game_date": 6132026}, "replay_center", "game_date", None),
        ({"official1_code": ""}, {}, "officials", "official_id", None),
        ({"home_team_id": ""}, {}, "officials", "home_team_id", None),
        ({}, {"official_code": ""}, "replay_center", "official_id", None),
        ({"official1": {"name": "A Ref"}}, {}, "officials", "official_name", '{"name": "A Ref"}'),
    ],
)
def test_referee_bad_cells_become_null_in_parser_and_fetcher(monkeypatch, row, replay, table, col, expected):
    ok = {"Table": {"rows": []}, "Table1": {"rows": []}}
    block = {"Table": {"rows": [{**_REF_ROW, **row}]}, "Table1": {"rows": [{**_REPLAY_ROW, **replay}]}}
    payload = {"nba": block, "gl": ok, "wnba": ok}
    monkeypatch.setattr(mod, "_official_get", lambda url, **kw: _Resp(200, json.dumps(payload), "application/json"))
    for out in (parse_nba_referee_assignments(payload, "nba"), nba_referee_assignments("2026-06-13")):
        assert out["officials"].schema == NBA_REFEREE_ASSIGN_SCHEMA
        assert out["replay_center"].schema == NBA_REFEREE_REPLAY_SCHEMA
        assert out[table][col][0] == expected
        assert out["officials"]["game_id"][0] == "0042500405"  # the good cells survive


# ---------------------------------------------------------------------------
# Caller arguments are checked before any request (findings 7, 11)
# ---------------------------------------------------------------------------


def _no_request(url, **kw):
    raise AssertionError(f"no request expected, got {url}")


@pytest.mark.parametrize(
    "gid", [True, False, -1, "-1", -42500405.0, "4_2500405", " 42500405", "+42500405", "", "abc", "٤٢٥٠٠٤٠٥"]
)
def test_bad_game_id_is_value_error_before_request(monkeypatch, gid):
    # int() would read "4_2500405", " 42500405" and "+42500405" as game 42500405,
    # True as game 1, and -1 as the URL "-000000001" whose 403 reads as "no report".
    monkeypatch.setattr(mod, "_official_get", _no_request)
    with pytest.raises(ValueError, match="integer id"):
        nba_l2m(gid)


@pytest.mark.parametrize("season", [26, "26", 2026.0, "2026.0", True, " 2026", "2026 ", "20266", "abcd", None])
def test_l2m_games_bad_season_is_value_error_before_request(monkeypatch, season):
    monkeypatch.setattr(mod, "_official_get", _no_request)
    with pytest.raises(ValueError, match="4-digit"):
        mod.nba_l2m_games(season)


def test_l2m_games_accepts_a_digit_string_season(monkeypatch):
    seen = {}

    def fake_get(url, **kw):
        seen["url"] = url
        return _Resp(200, "<html><title>2025-26 NBA Officiating Last Two Minute Reports</title></html>", "text/html")

    monkeypatch.setattr(mod, "_official_get", fake_get)
    df = mod.nba_l2m_games("2026")
    assert seen["url"] == "https://official.nba.com/2025-26-nba-officiating-last-two-minute-reports/"
    assert df.height == 0 and df.schema["season"] == pl.Int32


# ---------------------------------------------------------------------------
# A 200 without the expected shape is a failed fetch (finding 8, listing NIT)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "body",
    [
        {"code": "rest_forbidden", "message": "Sorry, you are not allowed", "data": {"status": 401}},
        {"game": [], "l2m": [], "stats": []},
        {"game": {"GameId": "0042500405"}},
        {"game": [1]},
        {"l2m": [{"PeriodName": "Q4"}]},
        # Two game rows: the parser reads the first, so the second would vanish silently.
        {"game": [{"GameId": "0042500405"}, {"GameId": "0042500404"}], "l2m": [], "stats": []},
    ],
)
def test_l2m_body_without_its_game_row_is_asset_fetch_error(monkeypatch, body):
    _patch(monkeypatch, _Resp(200, json.dumps(body), "application/json"))
    with pytest.raises(AssetFetchError, match="game row"):
        nba_l2m("0042500405")
    with pytest.raises(AssetFetchError, match="game row"):
        nba_l2m("0042500405", raw=True)


_L2M_GAME = [{"GameId": "0042500405"}]


@pytest.mark.parametrize(
    "tables",
    [
        {"l2m": "abc"},
        {"l2m": {"PCTime": "01:00"}},
        {"l2m": [{"PCTime": "01:00"}, 5]},
        # An empty record would parse to a made-up all-null row (hoopR's twin check).
        {"l2m": [{}]},
        {"stats": [{}]},
        {"stats": "abc"},
    ],
)
def test_l2m_malformed_l2m_or_stats_table_is_asset_fetch_error(monkeypatch, tables):
    _patch(monkeypatch, _Resp(200, json.dumps({"game": _L2M_GAME, **tables}), "application/json"))
    with pytest.raises(AssetFetchError, match="malformed L2M"):
        nba_l2m("0042500405")
    with pytest.raises(AssetFetchError, match="malformed L2M"):
        nba_l2m("0042500405", raw=True)


@pytest.mark.parametrize("tables", [{"l2m": [], "stats": []}, {}, {"l2m": {}, "stats": {}}])
def test_l2m_empty_or_absent_tables_are_still_a_report(monkeypatch, tables):
    _patch(monkeypatch, _Resp(200, json.dumps({"game": _L2M_GAME, **tables}), "application/json"))
    out = nba_l2m("0042500405")
    assert out["game"].height == 1 and out["calls"].height == 0 and out["stats"].height == 0


def test_listing_with_unreadable_report_links_is_asset_fetch_error(monkeypatch):
    # The page keeps its title but links reports in a new shape: a redesign, not an empty season.
    html = (
        "<html><title>2025-26 NBA Officiating Last Two Minute Reports</title>"
        "<a href='/L2MReport.html?game=0042500405'>Knicks 94, Spurs 90</a></html>"
    )
    monkeypatch.setattr(mod, "_official_get", lambda url, **kw: _Resp(200, html, "text/html"))
    with pytest.raises(AssetFetchError, match="unrecognized shape"):
        mod.nba_l2m_games(2026)


# ---------------------------------------------------------------------------
# Parser NITs: bytes/None listing input, pandas output
# ---------------------------------------------------------------------------


def test_parse_listing_accepts_bytes_and_none():
    html = (FIX / "l2m_listing_2025-26.html").read_text(encoding="utf-8")
    assert parse_nba_l2m_games(html.encode("utf-8"), 2026).height == 415
    empty = parse_nba_l2m_games(None, 2026)
    assert empty.height == 0 and empty.columns == ["game_id", "season", "season_type", "label"]


def test_parsers_return_pandas_on_request():
    html = (FIX / "l2m_listing_2025-26.html").read_text(encoding="utf-8")
    assert isinstance(parse_nba_l2m_games(html, 2026, return_as_pandas=True), pd.DataFrame)
    out = parse_nba_referee_assignments(_assign(), "nba", return_as_pandas=True)
    assert all(isinstance(v, pd.DataFrame) for v in out.values())
    assert out["officials"].shape == (4, 14)


@pytest.mark.parametrize(
    "game",
    [[{"GameId": "0042500406"}], [{}], [{"GameId": ""}], [{"GameId": None}]],
)
def test_l2m_report_for_another_game_or_without_id_is_asset_fetch_error(monkeypatch, game):
    payload = {"game": game, "l2m": [], "stats": []}
    _patch(monkeypatch, _Resp(200, json.dumps(payload), "application/json"))
    with pytest.raises(AssetFetchError):
        nba_l2m("0042500405")
    with pytest.raises(AssetFetchError):
        nba_l2m("0042500405", raw=True)


def test_l2m_numeric_game_id_matches_after_padding(monkeypatch):
    payload = {"game": [{"GameId": 42500405}], "l2m": [], "stats": []}
    _patch(monkeypatch, _Resp(200, json.dumps(payload), "application/json"))
    assert nba_l2m("0042500405", raw=True)["game"][0]["GameId"] == 42500405


_OK_BLOCK = {"Table": {"rows": []}, "Table1": {"rows": []}}


@pytest.mark.parametrize("bad", ["not-an-id", "12345678901", "4.2e7", 42500405.5, True, ""])
def test_referee_row_game_id_must_be_ten_digits(monkeypatch, bad):
    wnba = {"Table": {"rows": [{"game_id": bad, "official1": "A Ref"}]}, "Table1": {"rows": []}}
    payload = {"nba": _OK_BLOCK, "gl": _OK_BLOCK, "wnba": wnba}
    monkeypatch.setattr(mod, "_official_get", lambda url, **kw: _Resp(200, json.dumps(payload), "application/json"))
    with pytest.raises(AssetFetchError):
        nba_referee_assignments("2026-06-13", league="wnba")


def test_referee_row_numeric_game_id_is_padded(monkeypatch):
    wnba = {"Table": {"rows": [{"game_id": 1022600097, "official1": "A Ref"}]}, "Table1": {"rows": []}}
    payload = {"nba": _OK_BLOCK, "gl": _OK_BLOCK, "wnba": wnba}
    monkeypatch.setattr(mod, "_official_get", lambda url, **kw: _Resp(200, json.dumps(payload), "application/json"))
    assert nba_referee_assignments("2026-06-13", league="wnba")["officials"]["game_id"].to_list() == ["1022600097"]
