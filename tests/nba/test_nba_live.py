import datetime as dt
import json
from pathlib import Path

import pytest

from sportsdataverse.errors import AssetFetchError, NoDataError
from sportsdataverse.nba import nba_live as mod
from sportsdataverse.nba.nba_live import (
    nba_live_boxscore,
    nba_live_pbp,
    parse_nba_live_boxscore,
    parse_nba_live_pbp,
)

FIX = Path(__file__).parent / "fixtures" / "nba_live"
PBP_PAYLOAD = json.loads((FIX / "playbyplay_0022500001.json").read_text())
BOX_PAYLOAD = json.loads((FIX / "boxscore_0022500001.json").read_text())


def _fake_transport(status, text):
    seen = {}

    def transport(url, params, headers, proxy_url):
        seen["url"], seen["headers"], seen["proxy_url"] = url, headers, proxy_url
        return status, text

    return transport, seen


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


# ---------------------------------------------------------------------------
# nba_live_pbp / nba_live_boxscore -- transport + error classification
# ---------------------------------------------------------------------------


def test_nba_live_pbp_uses_curl_transport_and_nba_cdn_headers(monkeypatch):
    transport, seen = _fake_transport(200, json.dumps(PBP_PAYLOAD))
    monkeypatch.setattr(mod, "_curl_transport", transport)
    df = nba_live_pbp("0022500001")
    assert "cdn.nba.com" in seen["url"]
    assert "Mozilla/5.0" in seen["headers"]["User-Agent"]
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
    transport, _ = _fake_transport(403, "<Error><Code>AccessDenied</Code></Error>")
    monkeypatch.setattr(mod, "_curl_transport", transport)
    with pytest.raises(NoDataError):
        nba_live_boxscore("0022500001")
