import datetime as dt
import json
from pathlib import Path

import polars as pl
import pytest
import requests

from sportsdataverse.errors import AssetFetchError, NoDataError
from sportsdataverse.nba import nba_officiating as mod
from sportsdataverse.nba.nba_officiating import (
    L2M_CALLS_SCHEMA,
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
        return _Resp(200, "", "text/html")

    monkeypatch.setattr(mod, "_official_get", fake_get)
    mod.nba_l2m_games(2026)
    assert seen["url"] == "https://official.nba.com/2025-26-nba-officiating-last-two-minute-reports/"


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
    assert out["officials"].height == 0 and "official_id" in out["officials"].columns


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
    with pytest.raises(AssetFetchError):
        mod._official_get("https://official.nba.com/l2m/json/0042500405.json")


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
        seen["url"] = url
        return _Resp(200, json.dumps(_assign()), "application/json")

    monkeypatch.setattr(mod, "_official_get", fake_get)
    nba_referee_assignments(dt.datetime(2026, 6, 13, 19, 30))
    assert seen["url"].endswith("date=2026-06-13")


def test_referee_assignments_plain_date_still_works(monkeypatch):
    seen = {}

    def fake_get(url, **kw):
        seen["url"] = url
        return _Resp(200, json.dumps(_assign()), "application/json")

    monkeypatch.setattr(mod, "_official_get", fake_get)
    nba_referee_assignments(dt.date(2026, 6, 13))
    assert seen["url"].endswith("date=2026-06-13")


# ---------------------------------------------------------------------------
# L2M calls.period dtype -- Ruling R7 (Minor 9)
# ---------------------------------------------------------------------------


def test_calls_period_dtype_is_int64():
    out = parse_nba_l2m(_payload())
    assert out["calls"].schema["period"] == pl.Int64
    assert L2M_CALLS_SCHEMA["period"] == pl.Int64
