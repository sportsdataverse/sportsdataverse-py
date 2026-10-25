"""Offline contract of the 7 Core v2 ``espn_<lg>_game_rosters`` chains + mlb (task 2.10).

Every payload is a real capture (``tests/fixtures/espn_game_rosters/README.md``; nba reuses
``tests/fixtures/espn``) served through the real ``dl_utils.download`` (only
``requests.Session.get`` is replaced); expected values are read from the payload itself.
"""

from __future__ import annotations

import copy
import importlib
import json
import re
import warnings
from pathlib import Path
from typing import Any, Dict

import polars as pl
import pytest

from sportsdataverse.errors import NoDataError
from tests.codegen.test_runtime_errors import _route

FIX = Path(__file__).parent / "fixtures"
LEAGUES = ["mbb", "wbb", "nba", "wnba", "nfl", "nhl", "cfb"]
_404 = (FIX / "espn" / "error_404_body.json").read_text(encoding="utf-8")


def _payload(kind: str, league: str) -> Dict[str, Any]:
    path = FIX / "espn" / f"{kind}_nba.json" if league == "nba" else FIX / "espn_game_rosters" / f"{kind}_{league}.json"
    return json.loads(path.read_text(encoding="utf-8"))


def _mod(league: str):
    return importlib.import_module(f"sportsdataverse.{league}.{league}_game_rosters")


def _serve(monkeypatch, routes: Dict[str, Any]):
    """Answer each URL whose path (query dropped) is a key of ``routes``; every other URL is ESPN's real 404."""

    def answer(url: str):
        body = routes.get(url.split("?", 1)[0].replace("https://", "http://"))
        return (200, json.dumps(body), "application/json") if body is not None else (404, _404, "application/json")

    return _route(monkeypatch, answer)


def _key(href: str) -> str:
    return href.split("?", 1)[0].replace("https://", "http://")


# --------------------------------------------------------------------------------------
# athletes: position kept (B-M26), one 404 athlete dropped alone (B-L5)
# --------------------------------------------------------------------------------------


@pytest.mark.parametrize("league", LEAGUES)
def test_game_rosters_keep_position(league, monkeypatch):
    athlete = _payload("athlete_core", league)
    _serve(monkeypatch, {_key(athlete["$ref"]): athlete})
    helper = getattr(_mod(league), f"helper_{league}_athlete_items")
    df = helper(teams_rosters=pl.DataFrame({"athlete_href": [athlete["$ref"]]}))
    pos = athlete["position"]
    assert df["position_id"].to_list() == [int(pos["id"])] and df.schema["position_id"] == pl.Int32
    assert df["position_name"].to_list() == [pos["name"]]
    assert df["position_display_name"].to_list() == [pos["displayName"]]
    assert df["position_abbreviation"].to_list() == [pos["abbreviation"]]
    assert df["position_leaf"].to_list() == [pos["leaf"]]
    assert "position_href" not in df.columns  # only the position's own $ref is dropped (hoopR / wehoop)
    if "parent" in pos:
        assert df["position_parent_href"].to_list() == [pos["parent"]["$ref"]]


@pytest.mark.parametrize("league", LEAGUES)
def test_game_rosters_ids_int64(league, monkeypatch):
    """B-M15: the finished frame's ids are Int64 (the chain reads team_id / position_id Int32, status_id String)."""
    from sportsdataverse._espn_game_rosters import roster_ids

    athlete = _payload("athlete_core", league)
    _serve(monkeypatch, {_key(athlete["$ref"]): athlete})
    df = getattr(_mod(league), f"helper_{league}_athlete_items")(
        teams_rosters=pl.DataFrame({"athlete_href": [athlete["$ref"]]})
    )
    df = roster_ids(df.with_columns(team_id=pl.lit(1, dtype=pl.Int32), game_id=pl.lit(401, dtype=pl.Int64)))
    ids = {c: df.schema[c] for c in df.columns if c == "id" or c.endswith("_id")}
    assert {"athlete_id", "position_id", "team_id", "game_id"} <= set(ids)
    assert set(ids.values()) == {pl.Int64}, ids
    assert df["position_id"].to_list() == [int(athlete["position"]["id"])]


@pytest.mark.parametrize("league", LEAGUES)
def test_game_rosters_athlete_404_drops_only_that_athlete(league, monkeypatch):
    athlete = _payload("athlete_core", league)
    missing = re.sub(r"/athletes/\d+", "/athletes/1", athlete["$ref"])
    _serve(monkeypatch, {_key(athlete["$ref"]): athlete})
    helper = getattr(_mod(league), f"helper_{league}_athlete_items")
    with pytest.warns(UserWarning, match="no athlete resource"):
        df = helper(teams_rosters=pl.DataFrame({"athlete_href": [missing, athlete["$ref"]]}))
    assert df["athlete_id"].to_list() == [int(athlete["id"])]
    with pytest.warns(UserWarning, match="no athlete resource"), pytest.raises(NoDataError):
        helper(teams_rosters=pl.DataFrame({"athlete_href": [missing]}))


# --------------------------------------------------------------------------------------
# raw= honoured (B-GB-06); mlb's empty competitors (B-L25)
# --------------------------------------------------------------------------------------


@pytest.mark.parametrize("league", LEAGUES)
def test_game_rosters_raw_payload(league, monkeypatch):
    body = _payload("event_competitors", league)
    game_id = int(re.search(r"/events/(\d+)/", body["items"][0]["$ref"]).group(1))
    url = re.sub(r"/competitors/.*", "/competitors", body["items"][0]["$ref"])
    calls = _serve(monkeypatch, {_key(url): body})
    out = getattr(_mod(league), f"espn_{league}_game_rosters")(game_id=game_id, raw=True)
    assert out == body
    assert len(calls) == 1  # the roster, team and athlete $refs are not followed


def test_mlb_game_rosters_empty_competitors_is_a_schemad_frame(monkeypatch):
    body = _payload("event_competitors", "mlb")
    game_id = int(re.search(r"/events/(\d+)/", body["items"][0]["$ref"]).group(1))
    url = re.sub(r"/competitors/.*", "/competitors", body["items"][0]["$ref"])
    _serve(monkeypatch, {_key(url): {**body, "items": [], "count": 0}})
    mod = _mod("mlb")
    df = mod.espn_mlb_game_rosters(game_id=game_id)
    assert isinstance(df, pl.DataFrame) and df.height == 0
    assert df.columns == list(mod._COLUMNS)
    assert df.schema["athlete_starter"] == pl.Boolean
    pdf = mod.espn_mlb_game_rosters(game_id=game_id, return_as_pandas=True)
    assert pdf.columns.tolist() == list(mod._COLUMNS)
    ids = [c for c in mod._COLUMNS if c.endswith("_id")]
    assert {c: str(pdf[c].dtype) for c in ids} == dict.fromkeys(ids, "Int64")  # R981-8: as the full frame's


# --------------------------------------------------------------------------------------
# teams: one pop list (B-GB-32), logo block (B-GB-19)
# --------------------------------------------------------------------------------------


def _team_frame(monkeypatch, league: str, team: Dict[str, Any]) -> pl.DataFrame:
    href = team["$ref"]
    _serve(monkeypatch, {_key(href): team})
    return getattr(_mod(league), f"helper_{league}_team_items")(items=pl.DataFrame({"team_href": [href]}))


def test_game_rosters_college_team_frame_is_the_canonical_team_columns(monkeypatch):
    team = _payload("team_core", "mbb")
    assert {"awards", "coaches"} <= set(team)  # the college keys B-GB-32 found leaking
    df = _team_frame(monkeypatch, "mbb", team)
    assert not [c for c in df.columns if c.endswith("_href") and not c.startswith("logo")], df.columns
    assert df["team_id"].to_list() == [int(team["id"])]


@pytest.mark.parametrize("league", ["nba", "mbb"])
def test_game_rosters_team_with_one_logo_keeps_it(league, monkeypatch):
    team = copy.deepcopy(_payload("team_core", league))
    team["logos"] = team["logos"][:1]
    df = _team_frame(monkeypatch, league, team)
    assert df["logo_href"].to_list() == [team["logos"][0]["href"]]
    assert df["logo_dark_href"].to_list() == [""]


def test_game_rosters_unreadable_logos_warn(monkeypatch):
    team = copy.deepcopy(_payload("team_core", "nba"))
    for logo in team["logos"]:
        logo.pop("href")
    with pytest.warns(UserWarning, match="logos have a shape"):
        df = _team_frame(monkeypatch, "nba", team)
    assert df["logo_href"].to_list() == [""] and df["logo_dark_href"].to_list() == [""]


@pytest.mark.parametrize("failure", [403, 429, 500, 503, "connection"])
@pytest.mark.parametrize("league", ["mbb", "nfl"])
def test_game_rosters_athlete_failed_fetch_raises(league, failure, monkeypatch):
    """A FAILED athlete fetch is unknown, not absent: it raises AssetFetchError and is never a dropped athlete."""
    import requests

    from sportsdataverse.errors import AssetFetchError

    monkeypatch.setenv("SDV_PY_HTTP_RETRIES", "2")
    athlete = _payload("athlete_core", league)
    if failure == "connection":
        _route(monkeypatch, lambda url: requests.exceptions.ConnectionError("connection reset"))
    else:
        _route(monkeypatch, lambda url: (failure, json.dumps({"message": "upstream failure"}), "application/json"))
    helper = getattr(_mod(league), f"helper_{league}_athlete_items")
    with warnings.catch_warnings(record=True) as seen, pytest.raises(AssetFetchError):
        warnings.simplefilter("always")
        helper(teams_rosters=pl.DataFrame({"athlete_href": [athlete["$ref"]]}))
    assert not [w for w in seen if "no athlete resource" in str(w.message)]


@pytest.mark.parametrize("league", LEAGUES)
def test_game_rosters_pandas_ids_are_nullable_int64(league, monkeypatch):
    """R981-8: the finished frame's pandas exit, per chain. The real competitors capture feeds the chain; the roster /
    team / athlete fetches are stubbed so one athlete is off the roster, which leaves its ``team_id`` null -- a plain
    ``to_pandas()`` floated it (``52.0``)."""
    body = _payload("event_competitors", league)
    game_id = int(re.search(r"/events/(\d+)/", body["items"][0]["$ref"]).group(1))
    url = re.sub(r"/competitors/.*", "/competitors", body["items"][0]["$ref"])
    _serve(monkeypatch, {_key(url): body})
    mod = _mod(league)
    on_roster = []

    def rosters(items, summary_url, **kw):
        out = pl.DataFrame({"team_id": items["team_id"]}).with_columns(player_id=pl.int_range(1, pl.len() + 1))
        on_roster.extend(out["player_id"].to_list())
        return out

    monkeypatch.setattr(mod, f"helper_{league}_roster_items", rosters)
    monkeypatch.setattr(mod, f"helper_{league}_team_items", lambda items, **kw: items.select("team_id"))
    monkeypatch.setattr(
        mod,
        f"helper_{league}_athlete_items",
        lambda teams_rosters, **kw: pl.DataFrame({"athlete_id": [on_roster[0], 999999]}),
    )
    df = getattr(mod, f"espn_{league}_game_rosters")(game_id=game_id)
    pdf = getattr(mod, f"espn_{league}_game_rosters")(game_id=game_id, return_as_pandas=True)
    assert df["team_id"].null_count() == 1
    ids = [c for c, t in df.schema.items() if t == pl.Int64 and (c == "id" or c.endswith("_id"))]
    assert {"team_id", "athlete_id", "game_id"} <= set(ids)
    assert {c: str(pdf[c].dtype) for c in ids} == dict.fromkeys(ids, "Int64")


def _mlb_athlete(player_id: int) -> Dict[str, Any]:
    return json.loads((FIX / "espn_game_rosters" / "mlb_athletes" / f"athlete_{player_id}.json").read_text("utf-8"))


def _serve_mlb_rosters(monkeypatch, missing: tuple = ()) -> tuple[int, dict]:
    """Serve the real MLB competitors capture, each side's real Core v2 roster capture and each rostered athlete's real
    Core v2 record (an athlete id in ``missing`` answers ESPN's real 404); return (game_id, rosters)."""
    body = _payload("event_competitors", "mlb")
    rosters, athletes = {}, {}
    for c in body["items"]:
        team = re.search(r"/competitors/(\d+)/roster", c["roster"]["$ref"]).group(1)
        roster = json.loads(
            (FIX / "espn_game_rosters" / f"event_competitor_roster_mlb_{team}.json").read_text(encoding="utf-8")
        )
        rosters[_key(c["roster"]["$ref"])] = roster
        for e in roster["entries"]:
            if e["playerId"] not in missing:
                athletes[_key(e["athlete"]["$ref"])] = _mlb_athlete(e["playerId"])
    game_id = int(re.search(r"/events/(\d+)/", body["items"][0]["$ref"]).group(1))
    url = re.sub(r"/competitors/.*", "/competitors", body["items"][0]["$ref"])
    _serve(monkeypatch, {_key(url): body, **rosters, **athletes})
    return game_id, rosters


def test_mlb_game_rosters_read_each_athlete_from_its_ref(monkeypatch):
    """R1002-1/2: the real Core v2 roster entry carries the athlete and the position as a bare ``$ref``, and its
    ``displayName`` is ESPN's SHORT name ("T. Hernández", "E. Hernández" on one roster). Each athlete is read from its
    ``$ref`` (the step the other Core v2 chains use), so the id, full name, jersey and position are the athlete
    record's, one row per entry, ids Int64."""
    game_id, rosters = _serve_mlb_rosters(monkeypatch)
    df = _mod("mlb").espn_mlb_game_rosters(game_id=game_id)
    entries = [e for payload in rosters.values() for e in payload["entries"]]
    records = [_mlb_athlete(e["playerId"]) for e in entries]
    assert df.height == len(entries)
    assert df.schema["athlete_id"] == df.schema["athlete_position_id"] == pl.Int64
    assert df["athlete_id"].to_list() == [int(r["id"]) for r in records] == [e["playerId"] for e in entries]
    assert df["athlete_full_name"].to_list() == [r["fullName"] for r in records]
    assert df["athlete_jersey"].to_list() == [r.get("jersey") for r in records]
    assert df["athlete_position_id"].to_list() == [int(r["position"]["id"]) for r in records]
    assert df["athlete_position_abbreviation"].to_list() == [r["position"]["abbreviation"] for r in records]
    assert df["athlete_position_name"].to_list() == [r["position"]["name"] for r in records]
    assert df["athlete_starter"].to_list() == [e["starter"] for e in entries]
    hernandez = df.filter(pl.col("athlete_full_name").str.contains("Hernandez"))["athlete_full_name"].to_list()
    assert len(set(hernandez)) == len(hernandez) == 2  # the entries' short names were ambiguous


def test_mlb_game_rosters_leave_out_an_athlete_whose_ref_404s(monkeypatch):
    """R1002-1: the siblings' rule -- an athlete ``$ref`` that 404s drops that athlete with a warning, not the game."""
    _, rosters = _serve_mlb_rosters(monkeypatch)
    gone = next(iter(rosters.values()))["entries"][0]["playerId"]
    game_id, rosters = _serve_mlb_rosters(monkeypatch, missing=(gone,))
    with pytest.warns(UserWarning, match=rf"no athlete resource at .*/athletes/{gone}\?.*left out"):
        df = _mod("mlb").espn_mlb_game_rosters(game_id=game_id)
    entries = [e for payload in rosters.values() for e in payload["entries"]]
    assert df.height == len(entries) - 1 and gone not in df["athlete_id"].to_list()


def test_mlb_game_rosters_pandas_ids_are_nullable_int64(monkeypatch):
    """R981-8, mlb's own chain on the real captures: the pandas ids are pandas' nullable Int64."""
    game_id, _ = _serve_mlb_rosters(monkeypatch)
    mod = _mod("mlb")
    df = mod.espn_mlb_game_rosters(game_id=game_id)
    pdf = mod.espn_mlb_game_rosters(game_id=game_id, return_as_pandas=True)
    assert df.height and df["athlete_id"].null_count() == 0
    ids = [c for c in df.columns if c == "id" or c.endswith("_id")]
    assert {c: str(pdf[c].dtype) for c in ids} == dict.fromkeys(ids, "Int64")


def test_athlete_step_keeps_the_top_level_key_whatever_the_json_order(monkeypatch):
    """R1002-6: an MLB athlete record ships ``debutYear`` beside ``debut.year`` (both snake-case to ``debut_year``).
    The top-level key wins by rule, not because ESPN happened to serialise it first: on the real record with the
    nested ``debut`` moved ahead of ``debutYear`` (and its year changed so the two differ) ``debut_year`` is still
    the top-level value."""
    from sportsdataverse import _espn_game_rosters

    record = _mlb_athlete(33804)
    reordered = {"debut": {**record["debut"], "year": record["debutYear"] + 1}}
    reordered.update({k: v for k, v in record.items() if k != "debut"})
    href = "http://sports.core.api.espn.com/v2/sports/baseball/leagues/mlb/seasons/2024/athletes/33804"
    _serve(monkeypatch, {_key(href): reordered})
    df = _espn_game_rosters.athlete_items(pl.DataFrame({"athlete_href": [href]}))
    assert df.columns.count("debut_year") == 1
    assert df["debut_year"].to_list() == [record["debutYear"]]


def test_snake_collision_keeps_the_top_level_key_in_either_column_order():
    """R1002-6: pandas puts top-level keys first today, so pin the rule on the order it does not produce too."""
    from sportsdataverse._espn_game_rosters import _NEST, _snake_flattened

    for cols in (("debutYear", f"debut{_NEST}year"), (f"debut{_NEST}year", "debutYear")):
        frame = pl.DataFrame({c: [2018 if c == "debutYear" else 2019] for c in cols})
        assert _snake_flattened(frame).to_dicts() == [{"debut_year": 2018}], cols


def test_mlb_game_rosters_with_every_athlete_ref_404_raises_like_the_siblings(monkeypatch):
    """PRE-1008 D-3: when every athlete ``$ref`` 404s each is warned about and left out, and the game raises
    ``NoDataError`` as the six sibling Core v2 chains do (it used to return an empty frame, so a loop that skips on
    ``NoDataError`` wrote an empty MLB file)."""
    from sportsdataverse.errors import NoDataError

    _, rosters = _serve_mlb_rosters(monkeypatch)
    every = tuple(e["playerId"] for payload in rosters.values() for e in payload["entries"])
    game_id, _ = _serve_mlb_rosters(monkeypatch, missing=every)
    with pytest.warns(UserWarning, match=r"no athlete resource at .*left out") as rec:
        with pytest.raises(NoDataError, match="no athlete resource found for any rostered MLB player"):
            _mod("mlb").espn_mlb_game_rosters(game_id=game_id)
    assert len(rec) == len(every)


def test_mlb_game_rosters_game_position_is_the_entrys_not_the_athletes(monkeypatch):
    """PRE-1008 M-1: ``athlete_position_*`` is the athlete record's (season) position; the position the player was
    entered at in this game is ``game_position_id``, from the entry's own position ``$ref``. On 401701038 Judge's
    record says RF while his entry lists position 8 (CF)."""
    game_id, rosters = _serve_mlb_rosters(monkeypatch)
    df = _mod("mlb").espn_mlb_game_rosters(game_id=game_id)
    entries = [e for payload in rosters.values() for e in payload["entries"]]
    assert df.schema["game_position_id"] == pl.Int64
    want = [int(re.search(r"/positions/(\d+)", e["position"]["$ref"]).group(1)) for e in entries]
    assert df["game_position_id"].to_list() == want
    judge = df.filter(pl.col("athlete_full_name") == "Aaron Judge").row(0, named=True)
    assert (judge["game_position_id"], judge["athlete_position_abbreviation"]) == (8, "RF")
    assert (df["game_position_id"] != df["athlete_position_id"]).sum() > 0  # the two are different things


def test_mlb_game_rosters_take_the_id_from_the_athlete_ref_when_playerid_is_missing(monkeypatch):
    """PRE-1008 M-2: an entry with an athlete ``$ref`` but no ``playerId`` had a null join key, so every athlete was
    fetched and every row silently dropped. The id is read from the ``$ref`` path; the real rosters without any
    ``playerId`` keep every row."""
    game_id, rosters = _serve_mlb_rosters(monkeypatch)
    entries = [e for payload in rosters.values() for e in payload["entries"]]
    ids = [e.pop("playerId") for e in entries]
    df = _mod("mlb").espn_mlb_game_rosters(game_id=game_id)
    assert df["athlete_id"].to_list() == ids


def test_mlb_game_rosters_warn_on_an_entry_that_matches_no_athlete_read(monkeypatch):
    """PRE-1008 M-2: an entry whose athlete record was read but whose ``playerId`` differs from it no longer drops in
    silence; it is left out with a warning, the other entries keep their rows."""
    game_id, rosters = _serve_mlb_rosters(monkeypatch)
    entries = [e for payload in rosters.values() for e in payload["entries"]]
    entries[0]["playerId"] = 1
    with pytest.warns(UserWarning, match=r"1 ESPN MLB roster entries match no athlete record read"):
        df = _mod("mlb").espn_mlb_game_rosters(game_id=game_id)
    assert df.height == len(entries) - 1


def test_mlb_game_rosters_repeated_athlete_gives_one_row_per_entry(monkeypatch):
    """PRE-1008 minor / bodies B1: two entries naming one athlete give one row each; his record is deduplicated on
    athlete_id before the join, so the join cannot multiply rows."""
    game_id, rosters = _serve_mlb_rosters(monkeypatch)
    payload = next(iter(rosters.values()))
    payload["entries"].append(dict(payload["entries"][0]))
    entries = [e for p in rosters.values() for e in p["entries"]]
    df = _mod("mlb").espn_mlb_game_rosters(game_id=game_id)
    assert df.height == len(entries)
    assert df["athlete_id"].to_list() == [e["playerId"] for e in entries]


def test_mlb_game_rosters_drop_an_entry_with_no_athlete(monkeypatch):
    """WB-consolidation-1: a real roster entry with neither an inline athlete nor a ``$ref`` is left out with a
    warning; the other entries keep their rows."""
    game_id, rosters = _serve_mlb_rosters(monkeypatch)
    first = next(iter(rosters.values()))["entries"][0]
    gone = first["playerId"]
    first.pop("athlete")
    with pytest.warns(UserWarning, match=r"1 ESPN MLB roster entries carry no athlete record or \$ref"):
        df = _mod("mlb").espn_mlb_game_rosters(game_id=game_id)
    entries = [e for payload in rosters.values() for e in payload["entries"]]
    assert df.height == len(entries) - 1 and gone not in df["athlete_id"].to_list()


def test_mlb_game_rosters_raw_rows_keep_the_documented_keys(monkeypatch):
    """WB-consolidation-1: ``raw=True`` has its pre-#1002 shape again, one dict per entry with the documented columns
    read from the entry alone (no athlete fetch); ``athlete_id`` is the entry's ``playerId``."""
    game_id, rosters = _serve_mlb_rosters(monkeypatch)
    mod = _mod("mlb")
    out = mod.espn_mlb_game_rosters(game_id=game_id, raw=True)
    entries = [e for payload in rosters.values() for e in payload["entries"]]
    assert [list(r) for r in out["rows"]] == [list(mod._COLUMNS)] * len(entries)
    assert [r["athlete_id"] for r in out["rows"]] == [e["playerId"] for e in entries]
