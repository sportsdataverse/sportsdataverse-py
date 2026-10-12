"""Offline tests for the Ligue Magnus (``magnus``) flat-API family.

Asserts the parsers against the real captures in ``tests/fixtures/magnus/`` (never synthetic
payloads), the dtypes against the FULL game sheet, the committed returns-schemas against what
the parsers emit, the id key spaces, the endpoint YAML, and the empty / malformed contract.
The wrapper tests run once integration has rendered ``sportsdataverse/hockey/magnus/magnus.py``.
"""

from __future__ import annotations

import json
import math
from pathlib import Path
from typing import Any, Callable, Dict, List

import pandas as pd
import polars as pl
import pytest
import yaml

from sportsdataverse.dl_utils import underscore
from sportsdataverse.hockey.magnus import magnus_runtime
from sportsdataverse.hockey.magnus.magnus_parsers import (
    EVENT_TYPES,
    parse_magnus_competitions,
    parse_magnus_game,
    parse_magnus_lineups,
    parse_magnus_pbp,
    parse_magnus_phases,
    parse_magnus_rounds,
    parse_magnus_schedule,
    parse_magnus_shootout,
    parse_magnus_standings,
    parse_magnus_teams,
)

ROOT = Path(__file__).parents[3]
FIXTURES = ROOT / "tests" / "fixtures" / "magnus"
SCHEMA_DIR = ROOT / "tools" / "codegen" / "schemas" / "native" / "magnus"
ENDPOINTS_YAML = ROOT / "tools" / "codegen" / "endpoints" / "magnus.yaml"
FULL = "full/rencontre__77779.json"
GAMES = ["rencontre__id.json", "rencontre__id@68838.json", "rencontre__id@865.json", FULL]

# short -> (parser, the fixtures it parses)
ROUTES: Dict[str, tuple] = {
    "competitions": (parse_magnus_competitions, ["ajax__get_competition_by_type.json"]),
    "phases": (parse_magnus_phases, ["ajax__get_phases.json"]),
    "teams": (parse_magnus_teams, ["ajax__get_equipes.json"]),
    "schedule": (
        parse_magnus_schedule,
        ["ajax__get_rencontres.json", "ajax__get_rencontres@round2.json", "ajax__get_rencontres@round44.json"],
    ),
    "rounds": (parse_magnus_rounds, ["ajax__get_liste_tours.json"]),
    "round_games": (parse_magnus_schedule, ["ajax__get_rencontrestours.json"]),
    "standings": (parse_magnus_standings, ["ajax__get_classementsphase.json"]),
    "game": (parse_magnus_game, GAMES),
    "pbp": (parse_magnus_pbp, GAMES),
    "lineups": (parse_magnus_lineups, GAMES),
    "shootout": (parse_magnus_shootout, ["rencontre__id.json", "rencontre__id@865.json", FULL]),
}
_DOC_TYPES = {pl.Int64: "integer", pl.Boolean: "logical", pl.String: "character", pl.Date: "date"}


def _load(name: str) -> Any:
    return json.loads((FIXTURES / name).read_text(encoding="utf-8"))


def _doc_type(dtype: Any) -> str:
    if isinstance(dtype, pl.List):
        return "list"
    if isinstance(dtype, pl.Datetime):
        return "datetime"
    return _DOC_TYPES[dtype]


def _schema(short: str) -> List[Dict[str, str]]:
    return yaml.safe_load((SCHEMA_DIR / f"{short}.yaml").read_text(encoding="utf-8"))["columns"]


_CASES = [(short, name) for short, (_, names) in ROUTES.items() for name in names]


# --- parser contract ---------------------------------------------------------------


@pytest.mark.parametrize(("short", "name"), _CASES, ids=[f"{s}:{n}" for s, n in _CASES])
def test_fixture_parses_to_snake_case_rows_with_integer_ids(short, name):
    df = ROUTES[short][0](_load(name))
    assert isinstance(df, pl.DataFrame) and df.height > 0
    assert all(underscore(c) == c for c in df.columns)
    for col, dtype in df.schema.items():
        if col.endswith("_id") and col != "event_id":
            assert dtype == pl.Int64, col  # one dtype per id, never a float-origin string
        elif col.endswith("_ids"):
            assert dtype == pl.List(pl.Int64), col
    assert "ddn" not in " ".join(df.columns) and "birth" not in " ".join(df.columns)


@pytest.mark.parametrize("short", ROUTES)
def test_committed_schema_matches_the_parser(short):
    parser, names = ROUTES[short]
    committed = [(c["name"], c["type"]) for c in _schema(short)]
    for name in names:
        df = parser(_load(name))
        assert [(c, _doc_type(t)) for c, t in df.schema.items()] == committed, name


@pytest.mark.parametrize("short", ROUTES)
@pytest.mark.parametrize(
    "payload", [{}, [], None, "x", 3, {"success": False, "data": ""}, {"success": True, "data": {}}]
)
def test_empty_or_malformed_payload_is_a_zero_row_frame_with_the_documented_columns(short, payload):
    df = ROUTES[short][0](payload)
    assert df.height == 0
    assert [(c, _doc_type(t)) for c, t in df.schema.items()] == [(c["name"], c["type"]) for c in _schema(short)]


@pytest.mark.parametrize("short", ROUTES)
def test_return_as_pandas(short):
    parser, names = ROUTES[short]
    pdf = parser(_load(names[0]), return_as_pandas=True)
    assert isinstance(pdf, pd.DataFrame) and len(pdf) > 0
    assert list(pdf.columns) == [c["name"] for c in _schema(short)]


@pytest.mark.parametrize("short", ["pbp", "lineups", "shootout", "game"])
def test_wrong_typed_leaves_never_raise(short):
    sheet = _load(FULL)
    sheet["evenements"][0]["temps"] = {"x": 1}
    sheet["joueurs"][0]["position"] = "A"
    sheet["tirs_aux_but"][0]["numero"] = [1]
    sheet["receveur"] = "ANG"
    assert ROUTES[short][0](sheet).height > 0


def test_ajax_parsers_accept_the_inner_data_object():
    body = _load("ajax__get_classementsphase.json")
    assert parse_magnus_standings(body["data"]).equals(parse_magnus_standings(body))


# --- dtypes from the FULL body ------------------------------------------------------


def _leaves(obj: Any) -> List[Any]:
    if isinstance(obj, dict):
        return [x for v in obj.values() for x in _leaves(v)]
    if isinstance(obj, list):
        return [x for v in obj for x in _leaves(v)]
    return [obj]


@pytest.mark.parametrize("name", sorted(p.name for p in FIXTURES.glob("*.json")) + [FULL])
def test_no_number_in_any_body_is_a_float(name):
    # The integer dtypes of every frame are exact only while the source never sends a rate as a float.
    assert not [v for v in _leaves(_load(name)) if isinstance(v, float)]


def test_full_game_sheet_integer_columns_lose_nothing():
    # Every integer the full body carries for an Int64 column survives the coercion (a float or a
    # stringified value would come back null).
    sheet = _load(FULL)
    pbp = parse_magnus_pbp(sheet)
    events = sheet["evenements"]
    assert pbp["game_seconds"].null_count() == 0 and pbp.height == len(events) == 159
    assert sorted(pbp["game_seconds"].to_list()) == sorted(e["temps"] for e in events)
    penalties = [e for e in events if e["type"] == "SANCTION"]
    pen = pbp.filter(pl.col("event_type") == "penalty")
    assert sorted(pen["penalty_minutes"].to_list()) == sorted(e["temps_penalite"] for e in penalties)
    assert pen["penalty_end_seconds"].null_count() == 0
    lineups = parse_magnus_lineups(sheet)
    assert lineups.height == 41 and lineups["player_id"].null_count() == 0
    assert lineups["player_participant_id"].null_count() == 0
    so = parse_magnus_shootout(sheet)
    assert so.height == 10 and so["shootout_round"].to_list() == [1, 1, 2, 2, 3, 3, 4, 4, 5, 5]


# --- endpoint YAML -------------------------------------------------------------------


def _yaml() -> Dict[str, Any]:
    return yaml.safe_load(ENDPOINTS_YAML.read_text(encoding="utf-8"))


def test_yaml_lists_every_captured_route():
    eps = {e["short"]: e for e in _yaml()["endpoints"]}
    assert set(eps) == set(ROUTES)
    captured = {
        n.split("__", 1)[1].split("@")[0].removesuffix(".json") for n in (p.name for p in FIXTURES.glob("ajax__*.json"))
    }
    assert {e["fixed_params"]["action"] for e in eps.values() if "fixed_params" in e} == captured
    game = [s for s, e in eps.items() if e["path"] == "/rencontre/{game_id}/"]
    assert sorted(game) == ["game", "lineups", "pbp", "shootout"]
    assert all(eps[s]["returns_schema"] == f"native/magnus/{s}" for s in eps)
    assert {p.stem for p in SCHEMA_DIR.glob("*.yaml")} == set(ROUTES)


def test_yaml_dispatches_by_action_on_the_bare_path():
    doc = _yaml()
    assert doc["getter_module"] == "sportsdataverse.hockey.magnus.magnus_runtime"
    assert doc["schema_prefix"] == "magnus_"
    for ep in doc["endpoints"]:
        assert "#" not in ep["path"]
        if ep["path"] == "/wp-admin/admin-ajax.php":
            assert ep["fixed_params"]["action"].startswith("get_")


def test_season_is_the_end_year_sent_as_competition_id():
    eps = {e["short"]: e for e in _yaml()["endpoints"]}
    for short in ("phases", "schedule"):
        (season,) = [p for p in eps[short]["extra_params"] if p["name"] == "season"]
        assert (season["query_key"], season["transform"], season["required"]) == (
            "competition_id",
            "_competition_id",
            True,
        )
    assert eps["schedule"]["fixed_params"]["par_page"] == 700


def test_no_path_token_named_league_or_sport():
    for ep in _yaml()["endpoints"]:
        assert not {"league", "sport"} & {p["name"] for p in ep.get("path_params", [])}


# --- game sheet: pbp ------------------------------------------------------------------


def test_pbp_translates_event_types_and_keeps_the_raw_value():
    pbp = parse_magnus_pbp(_load(FULL))
    counts = dict(zip(*pbp.group_by("event_type_raw").len().sort("event_type_raw").to_dict(as_series=False).values()))
    assert counts == {"BUT": 6, "ENGA": 71, "SANCTION": 11, "TIR": 71}
    assert pbp.select((pl.col("event_type_raw").replace_strict(EVENT_TYPES) == pl.col("event_type")).all()).item()
    timeouts = parse_magnus_pbp(_load("rencontre__id@865.json")).filter(pl.col("event_type_raw") == "TEMPS_MORT")
    assert timeouts["event_type"].to_list() == ["timeout"]
    changes = parse_magnus_pbp(_load("rencontre__id@68838.json")).filter(pl.col("event_type") == "goalie_change")
    assert changes["goalie_out_player_id"].to_list() == [136880] and changes["player_id"].to_list() == [None]


def test_pbp_rows_are_in_game_order_not_grouped_by_type():
    pbp = parse_magnus_pbp(_load(FULL))
    assert pbp["event_type_raw"].to_list() != sorted(pbp["event_type_raw"].to_list())
    keys = list(zip(pbp["period"].to_list(), pbp["game_seconds"].to_list()))
    assert keys == sorted(keys)
    assert pbp["game_seconds"].to_list() == sorted(pbp["game_seconds"].to_list())


def test_pbp_period_rule_at_the_boundaries():
    pbp = parse_magnus_pbp(_load(FULL))
    at = pbp.filter(pl.col("game_seconds").is_in([1200, 2400, 3600]))
    faceoffs = at.filter(pl.col("event_type") == "faceoff")
    assert faceoffs.select((pl.col("period") == pl.col("game_seconds") // 1200 + 1).all()).item()
    others = at.filter(pl.col("event_type") != "faceoff")
    assert others.height > 0  # game 77779 has shots at exactly 1200
    assert others.select((pl.col("period") == pl.col("game_seconds") // 1200).all()).item()
    for row in pbp.filter(pl.col("event_type") != "faceoff").iter_rows(named=True):
        assert row["period"] == max(1, math.ceil(row["game_seconds"] / 1200))
    assert pbp["period"].max() == 4  # overtime
    assert pbp.select(pl.col("period_seconds").is_between(0, 1200).all()).item()


def test_2020_21_sheets_carry_no_faceoffs():
    pbp = parse_magnus_pbp(_load("rencontre__id@865.json"))
    assert pbp.height > 0 and "faceoff" not in pbp["event_type"].to_list()


def test_pbp_player_columns_are_person_ids_from_the_roster():
    sheet = _load(FULL)
    pbp, lineups = parse_magnus_pbp(sheet), parse_magnus_lineups(sheet)
    people = set(lineups["player_id"].to_list())
    for col in [c for c in pbp.columns if c.endswith("player_id")]:
        assert pbp.schema[col] == lineups.schema["player_id"]  # one dtype per key space before any join
        assert set(pbp[col].drop_nulls().to_list()) <= people, col
    goals = pbp.filter(pl.col("event_type") == "goal")
    assert goals["player_id"].null_count() == 0 and goals["goalie_player_id"].null_count() == 0
    on_ice = goals.select(
        pl.concat_list("on_ice_plus_player_ids", "on_ice_minus_player_ids").explode(empty_as_null=True)
    )
    assert on_ice.to_series().null_count() == 0 and set(on_ice.to_series().to_list()) <= people
    participants = set(lineups["player_participant_id"].to_list())
    assert not participants & people  # the per-game participant ids are a separate key space


def test_pbp_goal_details():
    goals = parse_magnus_pbp(_load(FULL)).filter(pl.col("event_type") == "goal")
    first = goals.row(0, named=True)
    assert (first["period"], first["period_seconds"], first["team_abbreviation"]) == (1, 641, "ANG")
    assert (first["player_name"], first["assist1_player_name"]) == ("HERVE Jordan", "GABORIT Robin")
    assert first["strength_code"] == "EG" and len(first["on_ice_plus_player_ids"]) == 6
    assert goals["strength_code"].to_list()[-1] == "SUP1"


# --- game sheet: header, lineups, shootout ----------------------------------------------


def test_game_header_is_one_row_with_scores_matched_by_team():
    sheet = _load(FULL)
    row = parse_magnus_game(sheet).row(0, named=True)
    assert row["game_id"] == 77779 and row["status"] == "final" and row["has_shootout"] is True
    assert (row["home_team_id"], row["home_score"], row["away_team_id"], row["away_score"]) == (553, 3, 3409, 4)
    assert (row["home_team_participant_id"], row["away_team_participant_id"]) == (9130, 9131)
    assert str(row["game_datetime"]) == "2026-09-29 20:30:00+02:00"
    assert (row["n_periods"], row["period_minutes"], row["attendance"]) == (3, 20, 3586)


def test_lineups_player_id_is_the_person_id_and_mvps_join_by_participant():
    sheet = _load(FULL)
    lineups = parse_magnus_lineups(sheet)
    assert lineups["player_id"].to_list() == [p["personne"]["id"] for p in sheet["joueurs"]]
    assert lineups["player_id"].to_list() == [p["id"] for p in sheet["joueurs"]]
    mvps = lineups.filter(pl.col("is_mvp"))
    assert set(mvps["player_participant_id"].to_list()) == {m["participant_joueur_id"] for m in sheet["mvps"]}
    assert sorted(mvps["home_away"].to_list()) == ["away", "home"]
    assert not {m["id"] for m in sheet["mvps"]} & set(lineups["player_id"].to_list())  # mvps[].id is a record id
    assert lineups["licence_number"].str.len_chars().min() == 6  # licence numbers keep leading zeros
    assert set(lineups["position"].to_list()) == {"forward", "defence", "goalie"}
    assert set(lineups["captaincy"].drop_nulls().to_list()) == {"C", "A"}


def test_shootout_rounds_and_untaken_attempts():
    so = parse_magnus_shootout(_load(FULL))
    assert so.filter(pl.col("scored")).group_by("team_abbreviation").len().to_dicts() == [
        {"team_abbreviation": "BRI", "len": 2}
    ]
    tail = so.filter(pl.col("shootout_round") == 5)
    assert tail["scored"].null_count() == 2 and tail["shooter_player_id"].null_count() == 2
    assert parse_magnus_shootout(_load("rencontre__id@68838.json")).height == 0  # no shootout


# --- admin-ajax frames ------------------------------------------------------------------


def test_schedule_scores_are_matched_by_team_id_not_position():
    body = _load("ajax__get_rencontres@round2.json")
    df = parse_magnus_schedule(body)
    for game in body["data"]["data"]:
        by_team = {s["equipe_id"]: s["score"] for s in game["score"]}
        row = df.filter(pl.col("game_id") == game["id"]).row(0, named=True)
        assert (row["home_score"], row["away_score"]) == (
            by_team[game["receveur"]["id"]],
            by_team[game["visiteur"]["id"]],
        )
    assert {len(g["score"]) for g in body["data"]["data"]} > {2}  # the trap is in the fixture
    assert df["status"].unique().to_list() == ["final"]


def test_unplayed_games_have_no_score_and_no_decision():
    df = parse_magnus_schedule(_load("ajax__get_rencontres@round44.json"))
    assert df["status"].unique().to_list() == ["scheduled"]
    assert df["home_score"].null_count() == df.height and df["decision"].null_count() == df.height
    assert df["home_team_participant_id"].null_count() == 0


def test_round_games_inherit_the_round_scope_and_share_the_local_time():
    rnd = parse_magnus_schedule(_load("ajax__get_rencontrestours.json"))
    assert rnd.select("season", "competition_id", "phase_id", "round").unique().rows() == [(2027, 240, 714, 1)]
    sched = parse_magnus_schedule(_load("ajax__get_rencontres.json"))
    both = rnd.select("game_id", "game_datetime").join(sched.select("game_id", "game_datetime"), on="game_id")
    assert both.height > 0 and both.select((pl.col("game_datetime") == pl.col("game_datetime_right")).all()).item()
    assert str(rnd.schema["game_datetime"]) == "Datetime(time_unit='us', time_zone='Europe/Paris')"


def test_schedule_decision_and_participant_ids():
    df = parse_magnus_schedule(_load("ajax__get_rencontres.json"))
    assert df["decision"].to_list() == ["regulation", "regulation", "overtime"]
    assert df["home_team_participant_id"].to_list() == [9124, 9131, 9120]


def test_standings_record_and_form():
    df = parse_magnus_standings(_load("ajax__get_classementsphase.json"))
    assert df["rank"].to_list() == [1, 2, 3]
    first = df.row(0, named=True)
    assert (first["team_id"], first["wins"], first["ot_wins"], first["points"]) == (3095, 6, 2, 22)
    assert set(first["last_results"]) <= {"W", "L", "T"} and first["standings_id"] is not None


def test_teams_competitions_phases_rounds():
    teams = parse_magnus_teams(_load("ajax__get_equipes.json"))
    assert teams.schema["competition_ids"] == pl.List(pl.Int64) and teams["competition_ids"][0].to_list()[:1] == [1]
    comps = parse_magnus_competitions(_load("ajax__get_competition_by_type.json"))
    assert all(magnus_runtime._SEASONS[s] == c for s, c in comps.select("season", "competition_id").rows())
    assert parse_magnus_phases(_load("ajax__get_phases.json"))["phase_id"].to_list() == [714]
    rounds = parse_magnus_rounds(_load("ajax__get_liste_tours.json"))
    assert rounds.schema["start_date"] == pl.Date and str(rounds["start_date"][0]) == "2026-09-15"


# --- generated wrappers (rendered by integration) ----------------------------------------


@pytest.fixture
def magnus_module() -> Any:
    return pytest.importorskip("sportsdataverse.hockey.magnus.magnus")


def _capture(monkeypatch: pytest.MonkeyPatch, module: Any, body: Any) -> List[Dict[str, Any]]:
    calls: List[Dict[str, Any]] = []

    def fake(url: str, params: Any = None, **kw: Any) -> Any:
        calls.append({"url": url, "params": dict(params or {})})
        return body

    monkeypatch.setattr(module, "_get", fake)
    return calls


_WRAPPER_CALLS: List[tuple] = [
    ("competitions", {}, "ajax__get_competition_by_type.json", {"action": "get_competition_by_type", "id_type": 1}),
    ("phases", {"season": 2027}, "ajax__get_phases.json", {"action": "get_phases", "competition_id": 240}),
    ("teams", {"phase_id": 714}, "ajax__get_equipes.json", {"action": "get_equipes", "phase_id": 714}),
    (
        "schedule",
        {"season": 2027, "phase_id": 714, "round": 2},
        "ajax__get_rencontres@round2.json",
        {"action": "get_rencontres", "par_page": 700, "competition_id": 240, "phase_id": 714, "journee": 2},
    ),
    ("rounds", {"phase_id": 714}, "ajax__get_liste_tours.json", {"action": "get_liste_tours", "phase_id": 714}),
    (
        "round_games",
        {"phase_id": 714, "round": 1},
        "ajax__get_rencontrestours.json",
        {"action": "get_rencontrestours", "phase_id": 714, "journee": 1},
    ),
    (
        "standings",
        {"phase_id": 714},
        "ajax__get_classementsphase.json",
        {"action": "get_classementsphase", "phase_id": 714},
    ),
]


@pytest.mark.parametrize(("short", "kwargs", "fixture", "sent"), _WRAPPER_CALLS, ids=[c[0] for c in _WRAPPER_CALLS])
def test_ajax_wrappers_send_the_action_and_the_mapped_season(monkeypatch, magnus_module, short, kwargs, fixture, sent):
    calls = _capture(monkeypatch, magnus_module, _load(fixture))
    df = getattr(magnus_module, f"magnus_{short}")(**kwargs)
    assert calls[0]["url"] == "https://liguemagnus.com/wp-admin/admin-ajax.php"
    assert {k: v for k, v in calls[0]["params"].items() if v is not None} == sent
    assert df.height > 0


@pytest.mark.parametrize("short", ["game", "pbp", "lineups", "shootout"])
def test_game_wrappers_request_the_game_page(monkeypatch, magnus_module, short):
    calls = _capture(monkeypatch, magnus_module, _load(FULL))
    fn: Callable[..., Any] = getattr(magnus_module, f"magnus_{short}")
    assert fn(game_id=77779).height > 0
    assert calls[0]["url"] == "https://liguemagnus.com/rencontre/77779/"
    assert fn(game_id=77779, return_parsed=False)["scores"][0]["sportif_rencontre_id"] == 77779


def test_a_season_before_coverage_raises_before_any_request(monkeypatch, magnus_module):
    from sportsdataverse.errors import NoDataError

    calls = _capture(monkeypatch, magnus_module, {})
    with pytest.raises(NoDataError):
        magnus_module.magnus_schedule(season=2019)
    assert calls == []


@pytest.mark.parametrize(
    "ep", yaml.safe_load(ENDPOINTS_YAML.read_text(encoding="utf-8"))["endpoints"], ids=lambda e: e["short"]
)
def test_the_documented_example_url_is_the_request_the_wrapper_sends(monkeypatch, magnus_module, ep):
    from urllib.parse import urlencode

    class _Stop(Exception):
        pass

    sent: List[str] = []

    def fake(url: str, params: Any = None, **kw: Any) -> Any:
        clean = {k: v for k, v in (params or {}).items() if v is not None}
        sent.append(url + (f"?{urlencode(clean, doseq=True)}" if clean else ""))
        raise _Stop

    monkeypatch.setattr(magnus_module, "_get", fake)
    fn = getattr(magnus_module, f"magnus_{ep['short']}")
    with pytest.raises(_Stop):
        fn(**ep["example_args"])
    documented = next(
        line.split("Example URL:", 1)[1].strip() for line in (fn.__doc__ or "").splitlines() if "Example URL:" in line
    )
    assert sent == [documented]
