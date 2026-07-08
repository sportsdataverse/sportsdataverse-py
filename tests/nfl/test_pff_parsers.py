"""Offline tests for the PFF Premium Stats runtime + parsers (transport injected, no network)."""

import json
from pathlib import Path

import polars as pl
import pytest

from sportsdataverse.nfl import pff_runtime
from sportsdataverse.nfl.pff_parsers import parse_pff_player_detail, parse_pff_report

FIX = Path(__file__).resolve().parents[1] / "fixtures" / "pff"


def _load(name: str) -> dict:
    return json.loads((FIX / f"{name}.json").read_text(encoding="utf-8"))


# --------------------------------------------------------------------------- runtime
def test_pff_get_injected_transport():
    def fake(url, params, headers, cookies):
        assert cookies.get("_premium_key") == "PK"
        return 200, json.dumps({"passing_summary": []})

    out = pff_runtime._get(
        "https://premium.pff.com/api/v1/facet/passing/summary",
        {"league": "nfl"},
        cookies={"_premium_key": "PK"},
        transport=fake,
    )
    assert out == {"passing_summary": []}


def test_pff_get_requires_cookies(monkeypatch):
    for k in ("SDV_PY_PFF_PREMIUM_KEY", "SDV_PY_PFF_SESSION", "SDV_PY_PFF_COOKIES"):
        monkeypatch.delenv(k, raising=False)
    with pytest.raises(RuntimeError, match="log in and supply cookies"):
        pff_runtime._get("https://premium.pff.com/api/v1/leagues", {})


def test_pff_get_reads_env_cookies(monkeypatch):
    for k in ("SDV_PY_PFF_SESSION", "SDV_PY_PFF_COOKIES"):
        monkeypatch.delenv(k, raising=False)
    monkeypatch.setenv("SDV_PY_PFF_PREMIUM_KEY", "envPK")
    seen: dict = {}

    def fake(url, params, headers, cookies):
        seen.update(cookies)
        return 200, "{}"

    pff_runtime._get("https://premium.pff.com/api/v1/leagues", {}, transport=fake)
    assert seen["_premium_key"] == "envPK"


def test_pff_login_not_implemented():
    # T0 auth spike was deferred (no live PFF session): pff_login ships as an
    # experimental stub pointing at the supported cookie-supply path.
    with pytest.raises(NotImplementedError, match="cookie"):
        pff_runtime.pff_login("a@b.com", "pw")


# --------------------------------------------------------------------- parse_pff_report
def test_facet_passing_summary_tidy():
    df = parse_pff_report(_load("facet_passing_summary"))
    assert isinstance(df, pl.DataFrame)
    assert df.height == 3
    assert "grades_offense" in df.columns and "player_id" in df.columns
    assert df.schema["player_id"] in (pl.Int64, pl.Int32)
    assert df.schema["jersey_number"] == pl.Utf8


def test_facet_team_and_game_filter_same_shape():
    for fx in ("facet_team_filter", "facet_game_filter"):
        df = parse_pff_report(_load(fx))
        assert df.height == 3 and "passing_summary" not in df.columns  # unwrapped


def test_singletons_leagues_games_teams_overview():
    assert parse_pff_report(_load("leagues")).height == 4
    assert parse_pff_report(_load("games")).height == 3
    df_to = parse_pff_report(_load("teams_overview"))
    assert df_to.height == 3
    assert "franchise_id" in df_to.columns


def test_teams_multikey_returns_dict():
    out = parse_pff_report(_load("teams"))  # {franchise_groups, games, teams}
    assert isinstance(out, dict)
    assert set(out) == {"franchise_groups", "games", "teams"}


def test_empty_payload_zero_rows():
    df = parse_pff_report({"passing_summary": []})
    assert isinstance(df, pl.DataFrame) and df.height == 0
    df2 = parse_pff_report({})
    assert isinstance(df2, pl.DataFrame) and df2.height == 0


# ------------------------------------------------- parse_pff_player_detail + matrix
def test_player_detail_weeks_long_frame():
    df = parse_pff_player_detail(_load("player_passing_summary"))
    assert df.height == 3  # 3 weeks in the fixture
    assert df.schema["player_id"] in (pl.Int64, pl.Int32)
    assert df.schema["league_id"] in (pl.Int64, pl.Int32)
    assert "game_id" in df.columns
    assert any(c.startswith("game_") for c in df.columns)
    assert "grades_offense" in df.columns


def test_matrix_report_three_subframes():
    out = parse_pff_report(_load("facet_receiving_coverage_matrix"))
    assert isinstance(out, dict)
    assert set(out) == {"defenders", "receivers", "versus"}


def test_player_seasons_no_raise():
    df = parse_pff_report(_load("player_seasons"))
    assert isinstance(df, pl.DataFrame)
    assert df.height == 9


# --------------------------------------------------------- end-to-end wrapper (Task 12)
def test_facet_wrapper_end_to_end(monkeypatch):
    monkeypatch.setenv("SDV_PY_PFF_PREMIUM_KEY", "PK")
    from sportsdataverse.nfl import pff as nflpff

    payload = _load("facet_passing_summary")
    captured: dict = {}

    def fake(url, params, headers, cookies):
        captured["url"] = url
        captured["league"] = params.get("league")
        return 200, json.dumps(payload)

    df = nflpff.pff_facet_passing_summary(season=2024, transport=fake)
    assert df.height == 3
    assert captured["league"] == "nfl"  # shim pre-bound the league
    assert "facet/passing/summary" in captured["url"]


def test_cfb_shim_binds_ncaa(monkeypatch):
    monkeypatch.setenv("SDV_PY_PFF_PREMIUM_KEY", "PK")
    from sportsdataverse.cfb import pff as cfbpff

    seen: dict = {}

    def fake(url, params, headers, cookies):
        seen["league"] = params.get("league")
        return 200, "{}"

    cfbpff.pff_facet_passing_summary(transport=fake)
    assert seen["league"] == "ncaa"  # cfb shim pre-bound league=ncaa
