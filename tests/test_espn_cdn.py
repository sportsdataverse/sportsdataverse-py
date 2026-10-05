"""ESPN CDN family (``cdn.espn.com/core/{league}/{page}?xhr=1``): parsers on real
captured payloads, the generated wrappers' URL + query, and the probe-verified
per-league scope. Fixture provenance: ``tests/fixtures/espn/cdn/README.md``."""

from __future__ import annotations

import importlib
import json
from pathlib import Path

import polars as pl
import pytest

from sportsdataverse._common_espn_parsers import (
    ENDPOINT_PARSERS,
    SUMMARY_SECTION_PARSERS,
    parse_cdn_game,
    parse_cdn_rankings,
    parse_cdn_schedule,
    parse_cdn_scoreboard,
)
from tests.conftest import skip_if_no_live

FIX = Path(__file__).parent / "fixtures" / "espn" / "cdn"
CDN_YAML = Path(__file__).parents[1] / "tools" / "codegen" / "endpoints" / "espn_cdn.yaml"


def _load(name: str) -> dict:
    return json.loads((FIX / name).read_text(encoding="utf-8"))


# ---------------------------------------------------------------- parsers


def test_game_page_parses_like_a_summary():
    frames = parse_cdn_game(_load("playbyplay_nba.json"))
    assert set(frames) == set(SUMMARY_SECTION_PARSERS)
    assert frames["header"]["id"].to_list() == ["401705127"]
    assert frames["plays"].height == 441
    assert frames["boxscore_player"].height > 0


def test_football_playbyplay_page_carries_drives():
    frames = parse_cdn_game(_load("playbyplay_cfb.json"))
    assert frames["drives"].height == 23
    assert frames["drive_plays"].height == 179
    assert frames["scoring_plays"].height == 7


def test_boxscore_page_and_single_section():
    raw = _load("boxscore_mlb.json")
    assert parse_cdn_game(raw)["boxscore_player"].height == 63
    plays = parse_cdn_game(raw, section="plays", return_as_pandas=True)
    assert len(plays) == 618
    with pytest.raises(ValueError):
        parse_cdn_game(raw, section="not_a_section")


def test_schedule_flattens_every_day():
    df = parse_cdn_schedule(_load("schedule_nba.json"))
    # 2025-01-15 .. 2025-01-21 local, 51 games, each once
    assert df.height == 51
    assert df["game_id"].n_unique() == 51
    assert "401705127" in df["game_id"].to_list()


def test_scoreboard_unwraps_sbdata_nba_and_soccer():
    nba = parse_cdn_scoreboard(_load("scoreboard_nba.json"))
    assert nba.height == 11
    assert "401705127" in nba["game_id"].to_list()
    epl = parse_cdn_scoreboard(_load("scoreboard_epl.json"))
    assert epl.height == 6
    assert "704518" in epl["game_id"].to_list()


def test_rankings_one_row_per_poll_entry():
    df = parse_cdn_rankings(_load("rankings_cfb.json"))
    assert df.columns[:5] == ["poll_id", "poll_name", "poll_short_name", "ranked", "team_id"]
    ranked = df.filter(pl.col("ranked") == True)
    assert ranked.group_by("poll_name").len()["len"].to_list() == [25] * 5
    assert df.schema["rank"] == pl.Int64
    assert df.schema["team_id"] == pl.Utf8
    ap1 = ranked.filter((pl.col("poll_id") == 1) & (pl.col("rank") == 1))
    assert ap1.select("team_display_name", "team_id").row(0) == ("Texas", "251")
    votes = df.filter(pl.col("ranked") == False)
    assert votes.height > 0
    assert votes["rank"].null_count() == votes.height


def test_rankings_team_id_joins_the_family_ids():
    # Join-key discipline: the rankings team_id must share a dtype with the ids the
    # rest of the family emits (scoreboard home_id / away_id, summary team_id).
    rk = parse_cdn_rankings(_load("rankings_cfb.json"))
    sb = parse_cdn_scoreboard(_load("scoreboard_nba.json"))
    box = parse_cdn_game(_load("playbyplay_nba.json"))["boxscore_player"]
    assert rk.schema["team_id"] == sb.schema["home_id"] == sb.schema["away_id"] == box.schema["team_id"]


def test_rankings_all_null_team_url_does_not_raise():
    # A page whose team_url is present but null on every row types the column Null.
    raw = _load("rankings_cfb.json")
    for poll in raw["content"]["data"]["rankings"]:
        for entry in poll["ranks"]:
            entry["team_url"] = None
    df = parse_cdn_rankings(raw)
    assert df.height == 217
    assert df.schema["team_id"] == pl.Utf8
    assert df["team_id"].null_count() == df.height
    # and a page with no team_url key at all keeps the same column set
    for poll in raw["content"]["data"]["rankings"]:
        for entry in poll["ranks"]:
            del entry["team_url"]
    assert parse_cdn_rankings(raw).columns[:5] == ["poll_id", "poll_name", "poll_short_name", "ranked", "team_id"]


@pytest.mark.parametrize(
    "payload",
    [None, {}, [], "x", {"content": None}, {"content": {"data": {"rankings": "x"}}}, {"gamepackageJSON": []}],
)
def test_parsers_never_raise_on_empty_or_malformed(payload):
    assert parse_cdn_rankings(payload).height == 0
    assert parse_cdn_schedule(payload).height == 0
    assert parse_cdn_scoreboard(payload).height == 0
    assert all(f.height == 0 for f in parse_cdn_game(payload).values())


def test_every_cdn_short_is_registered_with_its_yaml_parser():
    import yaml

    eps = yaml.safe_load(CDN_YAML.read_text(encoding="utf-8"))["endpoints"]
    for ep in eps:
        assert ENDPOINT_PARSERS[ep["short"]].__name__ == ep["parser"]


# ------------------------------------------------- generated wrappers / scope

# Probe matrix (2026-10-05) -> which league modules expose each page.
EXPECTED = {
    "cdn_playbyplay": {"nba", "wnba", "mbb", "wbb", "cfb", "nfl", "mlb", "college_baseball", "college_softball"},
    "cdn_boxscore": {"nba", "wnba", "mbb", "wbb", "cfb", "nfl", "mlb", "college_baseball", "college_softball"},
    "cdn_schedule": {
        "nba",
        "wnba",
        "mbb",
        "wbb",
        "cfb",
        "nfl",
        "mlb",
        "nhl",
        "college_baseball",
        "college_softball",
        "ufl",
    },
    "cdn_scoreboard": {"nba", "wnba", "mbb", "wbb", "cfb", "nfl", "mlb", "college_baseball", "epl", "mls", "ucl"},
    "cdn_rankings": {"cfb"},
}


def test_wrappers_exist_only_for_probe_verified_leagues():
    from tools.codegen import generate, spec

    cfg = spec.load_leagues(generate.ENDPOINTS / "leagues.yaml")
    params = spec.load_parameters(generate.ENDPOINTS / "parameters.yaml")
    apis = [spec.load_espn_api(generate.ENDPOINTS / f"{a}.yaml", params) for a in generate.ESPN_APIS]
    got: dict[str, set[str]] = {short: set() for short in EXPECTED}
    for lg in cfg.leagues:
        for v in generate._espn_league_views(lg, apis, cfg.hosts):
            if v.short in got:
                got[v.short].add(lg.prefix)
                assert "?xhr=1" in v.example_url  # the documented URL must be one that serves JSON
    assert got == EXPECTED  # the soccer param-mode catch-all (slug eng.1) is not swept in with epl


class _Resp:
    def __init__(self, payload):
        self._payload = payload

    def json(self):
        return self._payload


def _capture(monkeypatch, payload):
    import sportsdataverse._codegen_runtime as rt

    calls = []

    def fake_download(url, params=None, **kwargs):
        calls.append((url, params))
        return _Resp(payload)

    monkeypatch.setattr(rt, "download", fake_download)
    return calls


def _wrapper(module: str, name: str):
    return getattr(importlib.import_module(module), name)


def test_playbyplay_wrapper_sends_xhr_and_parses(monkeypatch):
    calls = _capture(monkeypatch, _load("playbyplay_nba.json"))
    fn = _wrapper("sportsdataverse.nba.nba_espn_ext", "espn_nba_cdn_playbyplay")
    frames = fn(game_id=401705127)
    assert calls == [("https://cdn.espn.com/core/nba/playbyplay", {"xhr": 1, "gameId": 401705127})]
    assert frames["plays"].height == 441
    assert fn(401705127, return_parsed=False)["gameId"] == 401705127


def test_schedule_wrapper_drops_unset_params(monkeypatch):
    calls = _capture(monkeypatch, _load("schedule_nba.json"))
    fn = _wrapper("sportsdataverse.nba.nba_espn_ext", "espn_nba_cdn_schedule")
    assert fn(date="20250115").height == 51
    assert calls == [("https://cdn.espn.com/core/nba/schedule", {"xhr": 1, "date": "20250115"})]


def test_rankings_wrapper_maps_season_to_year(monkeypatch):
    calls = _capture(monkeypatch, _load("rankings_cfb.json"))
    fn = _wrapper("sportsdataverse.cfb.cfb_espn_ext", "espn_cfb_cdn_rankings")
    assert fn(season=2024, week=5, season_type=2).height > 0
    assert calls == [
        ("https://cdn.espn.com/core/college-football/rankings", {"xhr": 1, "week": 5, "year": 2024, "seasontype": 2})
    ]


def test_soccer_scoreboard_wrapper_uses_league_slug(monkeypatch):
    calls = _capture(monkeypatch, _load("scoreboard_epl.json"))
    fn = _wrapper("sportsdataverse.soccer.epl.epl_espn_ext", "espn_epl_cdn_scoreboard")
    assert fn(date="20250201").height == 6
    assert calls == [("https://cdn.espn.com/core/eng.1/scoreboard", {"xhr": 1, "date": "20250201"})]


@skip_if_no_live
def test_live_nba_cdn_playbyplay():
    from sportsdataverse.nba import espn_nba_cdn_playbyplay

    frames = espn_nba_cdn_playbyplay(game_id=401705127)
    assert frames["plays"].height > 400
