"""Offline tests for the CBS Sports NAPI stem (``api.cbssports.com/napi``):
the ``parse_cbs_napi`` / ``parse_cbs_napi_standings`` envelope parsers and the
generated wrapper wiring. No network.

Fixtures in ``tests/fixtures/cbs/`` are **trimmed copies of the real committed
captures** in ``sdv-internal-refs/cbs/captures/`` (rows sliced, nothing edited).
When that refs checkout is present the sweep tests additionally run the parsers
over the full 17-league capture set; they skip when it is not.
"""

from __future__ import annotations

import json
import os
import re
from pathlib import Path
from typing import Any, Dict, List

import polars as pl
import pytest
import yaml

from sportsdataverse.cbs.cbs_napi_parsers import (
    parse_cbs_napi,
    parse_cbs_napi_scoring_drives,
    parse_cbs_napi_scoring_plays,
    parse_cbs_napi_standings,
)

FIXTURES = Path(__file__).resolve().parents[1] / "fixtures" / "cbs"
REPO_ROOT = Path(__file__).resolve().parents[2]
ENDPOINT_YAML = REPO_ROOT / "tools" / "codegen" / "endpoints" / "cbs_napi.yaml"

# A float that was silently stringified back into an "id" (the "123.0" bug).
_FLOAT_ID = re.compile(r"^-?\d+\.0$")


def _load(stem: str) -> Any:
    return json.loads((FIXTURES / f"{stem}.json").read_text(encoding="utf-8"))


def _refs_cbs() -> Path | None:
    """The ``sdv-internal-refs/cbs`` dir, or None when the checkout is absent."""
    env = os.environ.get("SDV_INTERNAL_REFS_REPO")
    candidates = [Path(env)] if env else []
    candidates += [REPO_ROOT.parent / "sdv-internal-refs", Path("C:/Users/saiem/Documents/sdv-internal-refs")]
    for base in candidates:
        if (base / "cbs" / "captures" / "_sample").is_dir():
            return base / "cbs"
    return None


def _id_columns(frame: pl.DataFrame) -> List[str]:
    return [c for c in frame.columns if c == "id" or c.endswith("_id") or c.endswith("_ids")]


def _assert_ids_are_clean(frame: pl.DataFrame, where: str) -> None:
    """Every id column holds one dtype, never a float, never a ``"123.0"`` string."""
    for col in _id_columns(frame):
        dtype = frame.schema[col]
        assert dtype not in (pl.Float32, pl.Float64), f"{where}: {col} is a float id ({dtype})"
        if dtype == pl.String:
            offenders = [v for v in frame[col].drop_nulls().to_list() if _FLOAT_ID.match(v)]
            assert not offenders, f"{where}: {col} holds float-origin string ids {offenders[:3]}"


# ===========================================================================
# Envelope shapes -- one assertion set per shape NAPI actually serves
# ===========================================================================


def test_data_list_envelope_season_teams():
    frame = parse_cbs_napi(_load("season_teams_nfl"))
    assert frame.height == 3
    assert frame.schema["team_id"] == pl.Int64
    assert frame["team_id"].to_list() == [404, 405, 406]
    assert frame["abbrev"].to_list()[0] == "ARI"
    # nullable id columns survive as Int64 rather than widening to Float64
    assert frame.schema["division_id"] in (pl.Int64, pl.Null)
    _assert_ids_are_clean(frame, "season_teams")


def test_data_list_envelope_team_players():
    frame = parse_cbs_napi(_load("team_players_nfl"))
    assert frame.height == 3
    assert frame.schema["player_id"] == pl.Int64
    assert {"first_name", "last_name", "weight"} <= set(frame.columns)
    _assert_ids_are_clean(frame, "team_players")


def test_plain_object_envelope_league_meta():
    frame = parse_cbs_napi(_load("league_meta_nfl"))
    assert frame.height == 1
    assert frame["league_id"].to_list() == [59]
    assert frame["league_abbr"].to_list() == ["NFL"]
    assert frame.schema["league_id"] == pl.Int64
    # the empty `teams` array is JSON-encoded rather than exploding the frame
    assert frame["teams"].to_list() == ["[]"]


def test_keyed_collection_envelope_registry():
    frame = parse_cbs_napi(_load("endpoint_registry"))
    assert frame.height == 4
    assert "key" in frame.columns
    assert "BoxscoreResource" in frame["key"].to_list()
    row = frame.filter(pl.col("key") == "BoxscoreResource")
    assert row["path"].to_list() == ["/resource/game/boxscore/{gameId}"]


def test_single_data_object_envelope():
    frame = parse_cbs_napi({"data": {"teamId": 404, "abbrev": "ARI"}})
    assert frame.height == 1
    assert frame.schema["team_id"] == pl.Int64


def test_bare_list_envelope():
    frame = parse_cbs_napi([{"playerId": 1}, {"playerId": 2}])
    assert frame.height == 2
    assert frame.schema["player_id"] == pl.Int64


# ===========================================================================
# Standings -- the one payload that is not a record list
# ===========================================================================


def test_standings_nfl_capture():
    frame = parse_cbs_napi_standings(_load("team_standings_nfl"))
    assert frame.height == 4  # 2 years x {regular, pre}
    assert frame.schema["season_year"] == pl.Int64
    assert set(frame["season_type"].to_list()) == {"regular", "pre"}
    assert "wins_number" in frame.columns
    assert "points_for_number" in frame.columns  # football-shaped stat block
    # list-valued stat blocks are JSON-encoded, not exploded
    assert frame["win_loss_record"].to_list()[0].startswith("[")
    _assert_ids_are_clean(frame, "standings_nfl")


def test_standings_is_sport_shaped():
    """NHL and NFL standings carry different stat keys -- the parser must not
    assume one league's column set."""
    nfl = parse_cbs_napi_standings(_load("team_standings_nfl"))
    nhl = parse_cbs_napi_standings(_load("team_standings_nhl"))
    assert "goals_for_goals" in nhl.columns
    assert "goals_for_goals" not in nfl.columns
    assert {"season_year", "season_type"} <= set(nhl.columns)


def test_standings_year_keys_stay_out_of_float_space():
    frame = parse_cbs_napi_standings({"2025": {"regular": {"wins": {"number": 12}}}})
    assert frame["season_year"].to_list() == [2025]
    assert frame.schema["season_year"] == pl.Int64


def test_standings_non_numeric_year_keys_stay_strings():
    frame = parse_cbs_napi_standings({"career": {"regular": {"wins": {"number": 1}}}})
    assert frame["season_year"].to_list() == ["career"]


# ===========================================================================
# Empty / malformed -> zero-row frame, never an exception
# ===========================================================================


@pytest.mark.parametrize(
    "payload",
    [
        None,
        {},
        [],
        "not json",
        123,
        {"error": "Not found"},
        {"errors": [{"code": 404}]},
        {"warnings": ["no such id"]},
        {"data": []},
        {"data": None},
    ],
)
def test_generic_parser_never_raises(payload):
    frame = parse_cbs_napi(payload)
    assert isinstance(frame, pl.DataFrame)
    assert frame.height == 0


@pytest.mark.parametrize("payload", [None, {}, [], "nope", {"error": "x"}, {"errors": []}, {"2025": "junk"}])
def test_standings_parser_never_raises(payload):
    frame = parse_cbs_napi_standings(payload)
    assert isinstance(frame, pl.DataFrame)
    assert frame.height == 0


def test_return_as_pandas():
    import pandas as pd

    assert isinstance(parse_cbs_napi(_load("season_teams_nfl"), return_as_pandas=True), pd.DataFrame)
    assert isinstance(parse_cbs_napi_standings(_load("team_standings_nfl"), return_as_pandas=True), pd.DataFrame)
    assert isinstance(parse_cbs_napi(None, return_as_pandas=True), pd.DataFrame)


# ===========================================================================
# game/scoring plays + drives -- real NCAAF (OHIOST @ TEXAS, 50027666) and NFL
# (CLE @ JAX, 50029216) bodies, 10 plays / 3 drives each, rows sliced not edited
# ===========================================================================

_PLAY_INTS = ("id", "game_id", "drive_id", "quarter", "down", "yardline", "team_in_possession")


@pytest.mark.parametrize("league", ["ncaaf", "nfl"])
def test_scoring_plays_one_row_per_play(league):
    raw = _load(f"game_scoring_plays_{league}")
    frame = parse_cbs_napi_scoring_plays(raw)
    assert frame.height == len(raw["plays"]) == 10
    assert frame["id"].to_list() == [int(r["id"]) for r in raw["plays"]]
    for col in (*_PLAY_INTS, "home_timeouts_remaining", "away_timeouts_remaining"):
        assert frame.schema[col] == pl.Int64, col
    assert frame.schema["distance"] == pl.String  # CBS sends the literal "Goal" on goal-to-go
    assert "Goal" in frame["distance"].to_list()
    assert frame.schema["score_on_play"] == pl.Boolean and frame.schema["under_review"] == pl.Boolean
    assert frame["score_on_play"].to_list() == [r["score_on_play"] == "Yes" for r in raw["plays"]]
    _assert_ids_are_clean(frame, f"scoring_plays_{league}")


@pytest.mark.parametrize("league", ["ncaaf", "nfl"])
def test_scoring_plays_keep_subplays_as_list_of_structs(league):
    raw = _load(f"game_scoring_plays_{league}")
    frame = parse_cbs_napi_scoring_plays(raw)
    assert isinstance(frame.schema["subplays"], pl.List)
    first = frame["subplays"][0].to_list()
    src = raw["plays"][0]["subplays"]["subplay"]
    assert [sp["type"] for sp in first] == [sp["type"] for sp in src] == ["Kickoff", "KickReturn"]
    assert first[0]["kicker"] == src[0]["kickoff"]["kicker"]
    assert first[1]["returned_by_name"] == src[1]["kick_return"]["returned_by_name"]
    assert frame["subplays"].list.len().to_list() == [len(r["subplays"]["subplay"]) for r in raw["plays"]]


@pytest.mark.parametrize("league", ["ncaaf", "nfl"])
def test_scoring_drives_one_row_per_drive(league):
    raw = _load(f"game_scoring_drives_{league}")
    frame = parse_cbs_napi_scoring_drives(raw)
    assert frame.height == len(raw["drives"]) == 3
    for col in ("id", "team_id", "quarter", "drive_plays", "starting_play_id", "ending_play_id", "yards_on_drive"):
        assert frame.schema[col] == pl.Int64, col
    assert frame.schema["score_on_drive"] == pl.Boolean and frame.schema["inside_the_20"] == pl.Boolean
    assert frame["result"].to_list() == [d["result"] for d in raw["drives"]]
    _assert_ids_are_clean(frame, f"scoring_drives_{league}")


def test_scoring_frames_from_two_leagues_stack_vertically():
    """CBS key order differs between games; the pinned column order must not. The
    ``subplays`` struct carries only the event fields a game used, so stacking
    games takes ``vertical_relaxed`` (polars unions the struct fields)."""
    plays = [parse_cbs_napi_scoring_plays(_load(f"game_scoring_plays_{lg}")) for lg in ("ncaaf", "nfl")]
    drives = [parse_cbs_napi_scoring_drives(_load(f"game_scoring_drives_{lg}")) for lg in ("ncaaf", "nfl")]
    assert plays[0].columns[:3] == ["id", "game_id", "drive_id"]
    assert pl.concat(plays, how="vertical_relaxed").height == 20
    assert pl.concat(drives, how="vertical").height == 6


def test_scoring_plays_and_drives_join_on_ids():
    """drive_id on a play and id on a drive share one dtype, so a join matches."""
    plays = parse_cbs_napi_scoring_plays(_load("game_scoring_plays_nfl"))
    drives = parse_cbs_napi_scoring_drives(_load("game_scoring_drives_nfl"))
    assert plays.schema["drive_id"] == drives.schema["id"]
    joined = plays.join(drives.select("id", "result"), left_on="drive_id", right_on="id", how="inner")
    assert joined.height == plays.filter(pl.col("drive_id") <= 3).height > 0


@pytest.mark.parametrize(
    "payload",
    [
        None,
        {},
        [],
        "nope",
        {"plays": []},
        {"drives": None},
        # the real HTTP-200 not-found envelopes NAPI returns for these two routes
        {"warnings": [{"code": 404, "type": "NotFoundException", "message": "No scoring plays data for that game."}]},
        {"errors": [{"code": 404, "type": "NotFoundException", "message": "No scoring plays data for that game."}]},
    ],
)
def test_scoring_parsers_never_raise(payload):
    """A miss is a zero-row frame with the documented columns and dtypes, so a
    season loop that hits one not-found envelope still stacks (a 0x0 frame makes
    ``pl.concat`` raise ``schema lengths differ``)."""
    for parser, fixture in (
        (parse_cbs_napi_scoring_plays, "game_scoring_plays_nfl"),
        (parse_cbs_napi_scoring_drives, "game_scoring_drives_nfl"),
    ):
        frame = parser(payload)
        full = parser(_load(fixture))
        assert isinstance(frame, pl.DataFrame) and frame.height == 0
        assert frame.columns == full.columns
        assert {c: t for c, t in frame.schema.items() if c != "subplays"} == {
            c: t for c, t in full.schema.items() if c != "subplays"
        }
        assert pl.concat([full, frame], how="vertical_relaxed").height == full.height


def test_scoring_plays_older_game_keeps_documented_columns():
    """CBS omits ``game_id``, ``real_clock`` and the timeout columns on older
    games; they come back typed null so eras stack."""
    omitted = ("game_id", "real_clock", "home_timeouts_remaining", "away_timeouts_remaining")
    raw = _load("game_scoring_plays_ncaaf")
    older = {"plays": [{k: v for k, v in play.items() if k not in omitted} for play in raw["plays"]]}
    frame = parse_cbs_napi_scoring_plays(older)
    full = parse_cbs_napi_scoring_plays(_load("game_scoring_plays_nfl"))
    assert frame.columns == full.columns
    assert all(frame[c].null_count() == frame.height == 10 for c in omitted)
    assert frame.schema["game_id"] == pl.Int64 and frame.schema["real_clock"] == pl.String
    assert pl.concat([full, frame], how="vertical_relaxed").height == 20


def test_scoring_parsers_return_as_pandas():
    import pandas as pd

    df = parse_cbs_napi_scoring_plays(_load("game_scoring_plays_nfl"), return_as_pandas=True)
    assert isinstance(df, pd.DataFrame) and len(df) == 10
    assert isinstance(parse_cbs_napi_scoring_drives(None, return_as_pandas=True), pd.DataFrame)


def test_scoring_wrappers_use_row_parsers(monkeypatch):
    import sportsdataverse.cbs.cbs_napi as mod

    bodies = {"plays": _load("game_scoring_plays_nfl"), "drives": _load("game_scoring_drives_nfl")}
    monkeypatch.setattr(mod, "_get", lambda url, params=None, **kw: bodies[url.rsplit("/", 2)[-2]])
    assert mod.cbs_game_scoring_plays(game_id=50029216).height == 10
    assert mod.cbs_game_scoring_drives(game_id=50029216).height == 3


# ===========================================================================
# Generated wrappers -- URL construction across several leagues' resources
# ===========================================================================


@pytest.fixture()
def calls(monkeypatch):
    import sportsdataverse.cbs.cbs_napi as mod

    seen: List[Dict[str, Any]] = []

    def fake_get(url, params=None, **kwargs):
        seen.append({"url": url, "params": params, "kwargs": kwargs})
        return {}

    monkeypatch.setattr(mod, "_get", fake_get)
    return seen


@pytest.mark.parametrize(
    ("fn_name", "kwargs", "expected"),
    [
        # NFL (leagueId/seasonId 59, team 404) -- ids from the committed captures
        ("cbs_league", {"league_id": 59}, "/resource/league/59"),
        ("cbs_season_teams", {"season_id": 59}, "/resource/season/teams/59"),
        ("cbs_team_players", {"team_id": 404}, "/resource/team/players/404"),
        ("cbs_team_standings", {"team_id": 404}, "/resource/team/standings/404"),
        # other leagues in the same 17-league surface: MLB 52, NBA 54, NCAAB-M 55, NHL 60
        ("cbs_league", {"league_id": 52}, "/resource/league/52"),
        ("cbs_league_teams", {"league_id": 54}, "/resource/league/teams/54"),
        ("cbs_season", {"season_id": 55}, "/resource/season/55"),
        ("cbs_sport_leagues", {"sport_id": 1}, "/resource/sport/leagues/1"),
        # game/scoring family (gameId comes from the torq feed, not from REST)
        ("cbs_game_scoring_plays", {"game_id": 1234}, "/resource/game/scoring/plays/1234"),
        ("cbs_game_boxscore", {"game_id": 1234}, "/resource/game/boxscore/1234"),
        # no-path-param + string-path-param endpoints
        ("cbs_endpoint_registry", {}, "/resource/endpoint/registry"),
        ("cbs_bulk", {}, "/resource/bulk"),
        ("cbs_client_config", {"client_name": "cbs"}, "/resource/client/config/cbs"),
    ],
)
def test_wrapper_builds_url(calls, fn_name, kwargs, expected):
    import sportsdataverse.cbs.cbs_napi as mod

    getattr(mod, fn_name)(**kwargs)
    assert calls[0]["url"] == f"https://api.cbssports.com/napi{expected}"


def test_wrapper_maps_python_args_to_wire_query_keys(calls):
    from sportsdataverse.cbs.cbs_napi import cbs_team_standings

    cbs_team_standings(team_id=404, season_type="regular", season_id="59", year=2025)
    assert calls[0]["params"] == {"seasonType": "regular", "seasonId": "59", "year": 2025}


def test_wrapper_returns_raw_when_return_parsed_false(calls, monkeypatch):
    import sportsdataverse.cbs.cbs_napi as mod

    monkeypatch.setattr(mod, "_get", lambda url, params=None, **kw: {"data": [{"teamId": 404}]})
    assert mod.cbs_team_players(team_id=404, return_parsed=False) == {"data": [{"teamId": 404}]}
    assert mod.cbs_team_players(team_id=404).schema["team_id"] == pl.Int64


def test_error_envelope_from_wrapper_is_zero_rows(monkeypatch):
    import sportsdataverse.cbs.cbs_napi as mod

    monkeypatch.setattr(mod, "_get", lambda url, params=None, **kw: {"errors": [{"code": 404}]})
    assert mod.cbs_team_players(team_id=-1).height == 0


# ===========================================================================
# Spec coverage -- the committed YAML must wrap every spec path
# ===========================================================================


def test_every_yaml_endpoint_is_exported():
    doc = yaml.safe_load(ENDPOINT_YAML.read_text(encoding="utf-8"))
    from sportsdataverse.cbs import cbs_napi as mod

    assert len(doc["endpoints"]) == 82
    assert sorted(mod.__all__) == sorted(f"cbs_{e['short']}" for e in doc["endpoints"])
    for name in mod.__all__:
        assert callable(getattr(mod, name))


def test_yaml_paths_match_the_frozen_spec():
    refs = _refs_cbs()
    if refs is None:
        pytest.skip("sdv-internal-refs checkout not available")
    spec = yaml.safe_load((refs / "cbssports-napi.openapi.yaml").read_text(encoding="utf-8"))
    doc = yaml.safe_load(ENDPOINT_YAML.read_text(encoding="utf-8"))
    # compare shapes with path tokens erased -- the YAML snake_cases them
    strip = re.compile(r"\{[^}]+\}")
    assert {strip.sub("{}", p) for p in spec["paths"]} == {strip.sub("{}", e["path"]) for e in doc["endpoints"]}


# ===========================================================================
# Full-capture sweep (17 leagues) -- runs only with the refs checkout present
# ===========================================================================


def test_every_committed_capture_parses_with_clean_ids():
    refs = _refs_cbs()
    if refs is None:
        pytest.skip("sdv-internal-refs checkout not available")
    sample = refs / "captures" / "_sample"
    seen = 0
    for src in sorted(sample.glob("*.json")):
        raw = json.loads(src.read_text(encoding="utf-8"))
        parser = parse_cbs_napi_standings if src.name.endswith("_team_standings.json") else parse_cbs_napi
        frame = parser(raw)
        assert isinstance(frame, pl.DataFrame)
        assert frame.height > 0, f"{src.name} parsed to zero rows"
        _assert_ids_are_clean(frame, src.name)
        seen += 1
    assert seen >= 60  # 17 leagues x 4 resource families (a few leagues lack one)


def test_capture_id_dtype_is_consistent_across_leagues():
    """One dtype per id, fixed at the parser boundary -- a join across two
    leagues' frames must not silently match nothing."""
    refs = _refs_cbs()
    if refs is None:
        pytest.skip("sdv-internal-refs checkout not available")
    sample = refs / "captures" / "_sample"
    dtypes: Dict[str, Dict[str, set]] = {}
    for src in sorted(sample.glob("*.json")):
        if src.name.endswith("_team_standings.json"):
            continue  # sport-shaped stat blocks, no cross-league id contract
        family = src.name.split("_", 1)[1]
        frame = parse_cbs_napi(json.loads(src.read_text(encoding="utf-8")))
        for col in _id_columns(frame):
            dtypes.setdefault(family, {}).setdefault(col, set()).add(str(frame.schema[col]))
    assert dtypes, "no id columns found in the captures"
    for family, cols in dtypes.items():
        for col, seen in cols.items():
            assert seen <= {"Int64", "Null"}, f"{family}.{col} has mixed dtypes across leagues: {sorted(seen)}"
