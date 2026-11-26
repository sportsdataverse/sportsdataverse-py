"""Offline tests for the ESPN lacrosse family (``espn_lacrosse_*``) and the league ``endpoints`` allowlist."""

from __future__ import annotations

import json
from pathlib import Path

import polars as pl
import pytest

from tools.codegen import spec

FIXTURES = Path(__file__).parent / "fixtures" / "espn" / "lacrosse"
ENDPOINTS = Path(__file__).parent.parent / "tools" / "codegen" / "endpoints"


def _load(league: str, endpoint: str) -> dict:
    return json.loads((FIXTURES / league / "site-v2" / f"{endpoint}.json").read_text(encoding="utf-8"))


def test_league_endpoints_allowlist_filters_in_scope_endpoints():
    lg = spec.League(prefix="x", sport="s", league="l", scopes=["universal"], endpoints=["scoreboard"])
    assert lg.emits(spec.Endpoint(short="scoreboard", path="/p"))
    assert not lg.emits(spec.Endpoint(short="team_roster", path="/p"))
    # the allowlist narrows scope; it never widens it
    assert not lg.emits(spec.Endpoint(short="scoreboard", path="/p", scope="ncaa"))
    open_lg = spec.League(prefix="x", sport="s", league="l", scopes=["universal"])
    assert open_lg.emits(spec.Endpoint(short="team_roster", path="/p"))


def test_league_endpoints_allowlist_rejects_unknown_short():
    api = spec.EspnApi(api="a", host="h", name_pattern="{short}", endpoints=[spec.Endpoint(short="x", path="/x")])
    bad = spec.LeaguesConfig(
        hosts={}, leagues=[spec.League(prefix="p", sport="s", league="l", scopes=["universal"], endpoints=["nope"])]
    )
    with pytest.raises(spec.SpecError, match="nope"):
        spec.validate_league_endpoints([api], bad)


def test_lacrosse_family_exposes_only_allowlisted_wrappers():
    import sportsdataverse.lacrosse as lax

    cfg = spec.load_leagues(ENDPOINTS / "leagues.yaml")
    row = next(lg for lg in cfg.leagues if lg.prefix == "lacrosse")
    names = {n for n in dir(lax) if n.startswith("espn_lacrosse_")}
    assert len(names) == len(row.endpoints)
    assert {"espn_lacrosse_scoreboard", "espn_lacrosse_summary", "espn_lacrosse_standings"} <= names
    # ESPN carries no lacrosse athletes, rosters or play-by-play: no wrapper is generated for them
    assert not names & {"espn_lacrosse_team_roster", "espn_lacrosse_athletes_index", "espn_lacrosse_game_plays"}


@pytest.mark.parametrize("league", ["pll", "mens-college-lacrosse", "womens-college-lacrosse"])
def test_lacrosse_scoreboard_parses(league):
    from sportsdataverse._common_espn_parsers import parse_scoreboard

    df = parse_scoreboard(_load(league, "scoreboard"))
    assert isinstance(df, pl.DataFrame)
    assert df.height >= 1
    assert {"game_id", "home_id", "away_id", "home_score", "away_score"} <= set(df.columns)
    assert df.schema["game_id"] == pl.Int64  # the ESPN scoreboard id convention


def test_lacrosse_teams_and_standings_parse():
    from sportsdataverse._common_espn_parsers import parse_standings, parse_teams

    teams = parse_teams(_load("pll", "teams"))
    assert teams.height >= 8
    standings = parse_standings(_load("pll", "standings"))
    assert standings.height >= 8
    pdf = parse_standings(_load("pll", "standings"), return_as_pandas=True)
    assert len(pdf) == standings.height


def test_lacrosse_examples_carry_no_shared_placeholder_ids():
    # Offline guard for the live example test: a lacrosse Quick start that falls back to the shared
    # placeholder ids (an NBA event, group 80) would run but come back empty against ESPN.
    import ast
    import inspect
    import re

    from sportsdataverse.lacrosse import lacrosse_espn_ext as ext

    calls = [
        re.search(r"Quick start::\s+(espn_lacrosse_\w+\(.*\))", fn.__doc__ or "")
        for name, fn in inspect.getmembers(ext, inspect.isfunction)
        if name.startswith("espn_lacrosse_")
    ]
    assert calls and all(calls)
    for call in calls:
        values = {str(ast.literal_eval(k.value)) for k in ast.parse(call.group(1), mode="eval").body.keywords}
        assert not values & {"401584793", "80"}, call.group(1)
