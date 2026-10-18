"""Offline tests for the FIBA flat-API families (``fiba_gdap``, ``fiba_gamepage``).

Asserts the parsers against the real captures in ``tests/fixtures/fiba_gdap`` and ``tests/fixtures/fiba_gamepage``
(never synthetic payloads), the dtypes against the FULL bodies under their ``full/`` dirs, the committed
returns-schemas against what the parsers emit, the id key spaces, the endpoint YAMLs against the snapshot of the
spec's routes (``routes.json``: these tests never read the private refs checkout), and the empty / malformed
contract (a zero-row frame typed from the documented schema, concat-safe).
"""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any, Dict, List

import pandas as pd
import polars as pl
import pytest
import yaml

from sportsdataverse.basketball.fiba_hub import fiba_frames
from sportsdataverse.basketball.fiba_hub import fiba_gamepage_parsers as pp
from sportsdataverse.basketball.fiba_hub import fiba_gdap_parsers as gp
from sportsdataverse.dl_utils import underscore

ROOT = Path(__file__).parents[3]
GDAP_FX = ROOT / "tests" / "fixtures" / "fiba_gdap"
PAGE_FX = ROOT / "tests" / "fixtures" / "fiba_gamepage"
ROUTES = json.loads((GDAP_FX / "routes.json").read_text(encoding="utf-8"))
SCHEMAS = ROOT / "tools" / "codegen" / "schemas" / "native"
ENDPOINTS = ROOT / "tools" / "codegen" / "endpoints"
_DOC = {pl.Int64: "integer", pl.Float64: "numeric", pl.Boolean: "logical", pl.String: "character"}
_ID_COL = re.compile(
    r"(^|_)(team|organisation|player|person|game|competition|round|venue|club|federation|zone|country)_id$"
)
_PAGES = ROUTES["gamepage"]
_BAD_PAGE = [{}, [], None, "x", 3, {"game": {}}, {"gameDetails": "x", "playByPlay": []}]


def _load(stem_dir: Path, name: str) -> Any:
    return json.loads((stem_dir / name).read_text(encoding="utf-8"))


def _cases() -> List[tuple]:
    out = []
    for short, cfg in ROUTES["gdap"].items():
        out += [("gdap", short, name) for name in cfg["fixtures"]]
    for short, cfg in ROUTES["gamepage"].items():
        out += [("gamepage", short, name) for name in cfg["fixtures"]]
    return out


def _parser(kind: str, short: str):
    return getattr(gp if kind == "gdap" else pp, f"parse_fiba_{kind}_{short}")


def _fx(kind: str) -> Path:
    return GDAP_FX if kind == "gdap" else PAGE_FX


def _schema(kind: str, short: str) -> List[Dict[str, str]]:
    path = SCHEMAS / f"fiba_{kind}" / f"{short}.yaml"
    return yaml.safe_load(path.read_text(encoding="utf-8"))["columns"]


CASES = _cases()
SHORTS = [(k, s) for k in ("gdap", "gamepage") for s in ROUTES[k]]


# --- parser contract ---------------------------------------------------------------------------------------------------


@pytest.mark.parametrize(("kind", "short", "name"), CASES, ids=[f"{k}:{s}:{n}" for k, s, n in CASES])
def test_fixture_parses_to_snake_case_rows_with_integer_ids(kind, short, name):
    df = _parser(kind, short)(_load(_fx(kind), name))
    assert isinstance(df, pl.DataFrame) and df.height > 0
    assert all(underscore(c) == c for c in df.columns)
    for col, dtype in df.schema.items():
        if _ID_COL.search(col):
            assert dtype == pl.Int64, col  # one dtype per id, never a float-origin string
        if dtype == pl.String:
            if col.endswith("_id") or col.endswith("player_id"):
                continue
            bad = [v for v in df[col].drop_nulls().to_list() if re.fullmatch(r"\d+\.0", v)]
            assert not bad, f"{col} holds a float-origin id string"


@pytest.mark.parametrize(("kind", "short"), SHORTS)
def test_committed_schema_matches_the_parser(kind, short):
    committed = [(c["name"], c["type"]) for c in _schema(kind, short)]
    for name in ROUTES[kind][short]["fixtures"]:
        df = _parser(kind, short)(_load(_fx(kind), name))
        assert [(c, _DOC[t]) for c, t in df.schema.items()] == committed, name


@pytest.mark.parametrize(("kind", "short"), SHORTS)
@pytest.mark.parametrize("payload", [{}, [], None, "x", 3, [1, "a"], {"data": []}, _BAD_PAGE[5], _BAD_PAGE[6]])
def test_empty_or_malformed_payload_is_a_zero_row_frame_with_the_documented_schema(kind, short, payload):
    df = _parser(kind, short)(payload)
    assert df.height == 0
    assert [(c, _DOC[t]) for c, t in df.schema.items()] == [(c["name"], c["type"]) for c in _schema(kind, short)]


@pytest.mark.parametrize(("kind", "short"), SHORTS)
def test_two_empty_frames_stack_and_an_empty_frame_stacks_onto_a_populated_one(kind, short):
    parse = _parser(kind, short)
    empty = parse({})
    assert pl.concat([empty, parse(None)]).height == 0
    populated = parse(_load(_fx(kind), ROUTES[kind][short]["fixtures"][-1]))
    assert pl.concat([empty, populated]).height == populated.height  # vertical concat needs equal dtypes


@pytest.mark.parametrize(("kind", "short"), SHORTS)
def test_return_as_pandas(kind, short):
    name = ROUTES[kind][short]["fixtures"][0]
    pdf = _parser(kind, short)(_load(_fx(kind), name), return_as_pandas=True)
    assert isinstance(pdf, pd.DataFrame) and len(pdf) > 0
    assert list(pdf.columns) == [c["name"] for c in _schema(kind, short)]


@pytest.mark.parametrize(("kind", "short"), SHORTS)
def test_columns_argument_types_an_empty_frame(kind, short):
    cols = [c["name"] for c in _schema(kind, short)]
    df = _parser(kind, short)({}, columns=cols[:3])
    assert df.columns == cols[:3] and df.height == 0


def test_every_public_parser_has_an_example_block():
    for mod in (gp, pp):
        for name in mod.__all__:
            doc = getattr(mod, name).__doc__ or ""
            assert all(h in doc for h in ("Args:", "Returns:", "Raises:", "Example:")), name


# --- dtypes come from the FULL bodies -----------------------------------------------------------------------------------


def test_a_rate_is_float_even_where_a_body_sends_a_whole_number():
    # Germany's pointsPerGame is the integer 20 in the capture body: a rate must still be Float64
    df = gp.parse_fiba_gdap_team_stats(
        _load(GDAP_FX / "full", "getgdapcompetitionteamstatisticsbyteamid__wc2023-ger.json")
    )
    rates = [c for c in df.columns if fiba_frames._RATE.search(c)]
    assert rates and all(df.schema[c] == pl.Float64 for c in rates), {c: df.schema[c] for c in rates}
    cap = gp.parse_fiba_gdap_team_stats(
        _load(GDAP_FX, "gdap__getgdapcompetitionteamstatisticsbyteamid__wc2023-ger.json")
    )
    assert cap.schema == df.schema


@pytest.mark.parametrize("name", sorted(p.name for p in (PAGE_FX / "full").glob("*.json")))
def test_blocks_received_is_int64_in_both_box_tables(name):
    # the team lines carry no BSR, so box_teams.blocks_received is all null: Int64 like box_players, never String
    page = _load(PAGE_FX / "full", name)
    players, teams = pp.parse_fiba_gamepage_box_players(page), pp.parse_fiba_gamepage_box_teams(page)
    assert players.schema["blocks_received"] == teams.schema["blocks_received"] == pl.Int64
    shared = set(players.columns) & set(teams.columns)
    assert {c: (players.schema[c], teams.schema[c]) for c in shared if players.schema[c] != teams.schema[c]} == {}


@pytest.mark.parametrize("kind", ["gdap", "gamepage"])
def test_one_column_name_has_one_type_across_the_family(kind):
    types: Dict[str, set] = {}
    for short in ROUTES[kind]:
        for col in _schema(kind, short):
            types.setdefault(col["name"], set()).add(col["type"])
    assert {c: t for c, t in types.items() if len(t) > 1} == {}


def test_full_bodies_are_committed_and_under_the_size_budget():
    full = sorted((GDAP_FX / "full").glob("*.json")) + sorted((PAGE_FX / "full").glob("*.json"))
    assert len(full) >= 7
    assert all(p.stat().st_size < 300_000 for p in full)


def test_full_schedule_keeps_every_game():
    raw = _load(GDAP_FX / "full", "getgdapgamesbycompetitionid__paris2024-men.json")
    df = gp.parse_fiba_gdap_schedule(raw)
    assert df.height == len(raw) == 26
    assert df["game_id"].n_unique() == 26 and df["game_id"].dtype == pl.Int64


def test_gdap_ids_are_integers_and_group_id_is_a_string():
    df = gp.parse_fiba_gdap_schedule(_load(GDAP_FX / "full", "getgdapgamesbycompetitionid__paris2024-men.json"))
    for col in ("team_a_team_id", "team_a_organisation_id", "competition_competition_id", "venue_id", "round_round_id"):
        if col in df.columns:
            assert df.schema[col] == pl.Int64, col
    assert df.schema["group_id"] == pl.String  # 'A'-style group code, not an integer id


def test_team_id_is_per_competition_and_organisation_id_is_stable():
    ger23 = gp.parse_fiba_gdap_roster(_load(GDAP_FX / "full", "getgdapcompetitionteamrosterbyteamid__wc2023-ger.json"))
    assert set(ger23["team_id"]) == {279569} and set(ger23["organisation_id"]) == {64}
    sched = gp.parse_fiba_gdap_schedule(_load(GDAP_FX / "full", "getgdapgamesbycompetitionid__paris2024-men.json"))
    ids = {
        t
        for side in ("a", "b")
        for o, t in zip(sched[f"team_{side}_organisation_id"], sched[f"team_{side}_team_id"])
        if o == 64
    }
    assert len(ids) == 1 and 279569 not in ids  # Germany at Paris 2024: one team id, not the World Cup's


def test_competition_ids_from_the_gateway_are_integers_not_numeric_strings():
    raw = _load(GDAP_FX, "gdap__getcustomgateway__competitionIdsBetweenTwoDates-2025-01-14.json")
    assert all(isinstance(i, str) for i in raw["competitionIds"])  # the API sends strings
    df = gp.parse_fiba_gdap_gateway_competition_ids(raw)
    assert df.schema["competition_id"] == pl.Int64 and df["competition_id"].to_list() == [208898, 208884, 208850]


def test_id_helper_never_stringifies_or_accepts_a_float():
    ident = fiba_frames._id
    assert ident("P_204689") == 204689 and ident("T_64") == 64 and ident("O_15177") == 15177
    assert ident(123) == 123 and ident("456") == 456
    assert ident(123.0) is None and ident("123.0") is None and ident(True) is None and ident(None) is None


# --- personal data -------------------------------------------------------------------------------------------------------


@pytest.mark.parametrize("short", ["team_staff", "roster_coaches", "game_officials", "nominations"])
def test_officials_and_coaches_carry_no_date_of_birth_or_contact_column(short):
    cfg = ROUTES["gdap"][short]
    for name in cfg["fixtures"]:
        df = _parser("gdap", short)(_load(GDAP_FX, name))
        cols = " ".join(df.columns)
        assert not re.search(r"birth|email|phone|mobile|skype|contact|airport", cols), (short, name)


def test_players_dates_of_birth_are_public_and_kept():
    df = gp.parse_fiba_gdap_roster(_load(GDAP_FX / "full", "getgdapcompetitionteamrosterbyteamid__wc2023-ger.json"))
    assert "date_of_birth" in df.columns and df["date_of_birth"].null_count() == 0


@pytest.mark.parametrize(("kind", "short", "name"), CASES, ids=[f"{k}:{s}:{n}" for k, s, n in CASES])
def test_no_parsed_cell_is_an_email_or_a_redaction_marker(kind, short, name):
    df = _parser(kind, short)(_load(_fx(kind), name))
    for col in df.columns:
        if df.schema[col] == pl.String:
            values = df[col].drop_nulls().to_list()
            assert not [v for v in values if "@" in v or v == "REDACTED"], (short, name, col)


# --- endpoint YAML and route snapshot --------------------------------------------------------------------------------------


def _yaml(stem: str) -> Dict[str, Any]:
    return yaml.safe_load((ENDPOINTS / f"{stem}.yaml").read_text(encoding="utf-8"))


def test_yaml_lists_every_route_of_the_spec():
    eps = {e["short"]: e for e in _yaml("fiba_gdap")["endpoints"]}
    assert set(eps) == set(ROUTES["gdap"])
    assert {e["path"] for e in eps.values()} == {f"/{m}" for m in ROUTES["spec_methods"]}  # every method wrapped
    assert all(eps[s]["path"] == f"/{ROUTES['gdap'][s]['method']}" for s in eps)
    assert all(e["returns_schema"] == f"native/fiba_gdap/{s}" for s, e in eps.items())
    assert {p.stem for p in (SCHEMAS / "fiba_gdap").glob("*.yaml")} == set(ROUTES["gdap"])
    assert ROUTES["unwrapped"] == []
    assert len(ROUTES["page_templates"]) == 3
    pages = {e["short"]: e for e in _yaml("fiba_gamepage")["endpoints"]}
    assert set(pages) == set(ROUTES["gamepage"]) and all(e["path"] == "/en/games/{game_id}" for e in pages.values())
    assert {p.stem for p in (SCHEMAS / "fiba_gamepage").glob("*.yaml")} == set(ROUTES["gamepage"])


def test_every_required_spec_parameter_is_sent():
    for ep in _yaml("fiba_gdap")["endpoints"]:
        method = ep["path"][1:]
        sent = {p["query_key"] for p in ep.get("extra_params", [])} | set(ep.get("fixed_params", {}))
        assert set(ROUTES["spec_methods"][method]) <= sent, ep["short"]
        for p in ep.get("extra_params", []):
            if p["query_key"] in ROUTES["spec_methods"][method]:
                assert p.get("required"), (ep["short"], p["name"])  # a required wire key is a required argument


def test_competition_is_the_comp_argument_not_a_family_per_event():
    eps = {e["short"]: e for e in _yaml("fiba_gdap")["endpoints"]}
    for short in ("schedule", "competition", "teams", "rounds", "awards", "nominations", "team_staff", "player_stats"):
        (comp,) = [p for p in eps[short]["extra_params"] if p["name"] == "comp"]
        assert comp["query_key"] == "gdapCompetitionId" and comp["transform"] == "_int_id" and comp["required"]
    for ep in _yaml("fiba_gamepage")["endpoints"]:
        assert [p["name"] for p in ep["extra_params"]] == ["comp"] and ep["extra_params"][0]["required"]


def test_season_is_sent_unchanged_and_documented_by_competition_kind():
    (season,) = [
        p
        for p in {e["short"]: e for e in _yaml("fiba_gdap")["endpoints"]}["editions"]["extra_params"]
        if p["name"] == "season"
    ]
    assert (season["query_key"], season["transform"]) == ("fromSeason", "_season")
    assert (
        "END year" in season["description"]
        and "event year" in season["description"]
        and "TARGET" in season["description"]
    )


def test_yaml_runtime_and_docstring_flags():
    for stem in ("fiba_gdap", "fiba_gamepage"):
        doc = _yaml(stem)
        assert (
            doc["getter_module"] == "sportsdataverse.basketball.fiba_hub.fiba_runtime" and doc["parser_columns"] is True
        )
        assert doc["schema_prefix"] == f"{stem}_"
        notes = " ".join(doc["docstring"]["notes"])
        assert "Authorized Users" in notes and "accepted them" in notes  # the owner-accepted ToS caveat
        assert "public" in notes and "never logs" in notes


def test_no_path_token_named_league_or_sport():
    for stem in ("fiba_gdap", "fiba_gamepage"):
        for ep in _yaml(stem)["endpoints"]:
            assert not {"league", "sport"} & {p["name"] for p in ep.get("path_params", [])}


def test_descriptions_are_complete_in_the_notes_files():
    for stem in ("fiba_gdap", "fiba_gamepage"):
        notes = yaml.safe_load(
            (ROOT / "tools" / "codegen" / "notes" / f"{stem}_descriptions.yaml").read_text(encoding="utf-8")
        )
        for short in ROUTES[stem.removeprefix("fiba_")]:
            cols = [c["name"] for c in _schema(stem.removeprefix("fiba_"), short)]
            assert set(notes[f"{stem}_{short}"]) == set(cols)
            for col, text in notes[f"{stem}_{short}"].items():
                assert len(text) >= 15 and text.lower() != col.lower(), (stem, short, col)


def test_schema_files_carry_no_descriptions():
    for stem in ("fiba_gdap", "fiba_gamepage"):
        for path in (SCHEMAS / stem).glob("*.yaml"):
            cols = yaml.safe_load(path.read_text(encoding="utf-8"))["columns"]
            assert all(c["description"] == "" for c in cols), path.name  # descriptions live in the notes / manual file


def test_dtypes_file_covers_every_parser_in_schema_order():
    from sportsdataverse.basketball.fiba_hub import fiba_dtypes

    for kind, short in SHORTS:
        assert list(fiba_dtypes.DTYPES[f"parse_fiba_{kind}_{short}"]) == [c["name"] for c in _schema(kind, short)]


# --- game page ------------------------------------------------------------------------------------------------------------

OVR = _load(PAGE_FX / "full", "page__wc2023-final.json")
FLS = _load(PAGE_FX / "full", "page__elw2025.json")


def _pbp_actions(page: Dict[str, Any]) -> int:
    return sum(len(p["items"]) for p in page["playByPlay"]["items"].values())


@pytest.mark.parametrize("page", [OVR, FLS], ids=["OVR", "FLS"])
def test_pbp_keeps_every_action_in_game_order(page):
    pbp = pp.parse_fiba_gamepage_pbp(page)
    assert pbp.height == _pbp_actions(page)
    assert pbp["game_id"].unique().to_list() == [page["game"]["gameId"]]
    assert pbp["competition_id"].unique().to_list() == [page["game"]["competition"]["competitionId"]]
    assert pbp["period"].unique(maintain_order=True).to_list()[0] == "Q1"
    assert pbp["period_number"].is_sorted()  # periods come out in game order
    assert pbp["wall_time_ms"].dtype == pl.Int64 and pbp["clock_seconds"].dtype == pl.Int64
    assert pbp["action_id"].dtype == pl.String  # '3:0015' in OVR, a number-as-string in FLS


def test_two_live_stats_systems_differ_where_the_refs_say():
    ovr, fls = pp.parse_fiba_gamepage_pbp(OVR), pp.parse_fiba_gamepage_pbp(FLS)
    assert set(ovr["statistic_system"]) == {"OVR"} and set(fls["statistic_system"]) == {"FLS"}
    assert ovr["action_id"].str.contains(":").all() and not fls["action_id"].str.contains(":").any()
    assert ovr["order"].min() < 1000 <= fls["order"].min()  # OVR 1, 2, ... ; FLS 1000, 2000, ...
    paris = pp.parse_fiba_gamepage_pbp(_load(PAGE_FX, "page__events__paris2024-final.json"))
    assert paris["player2_id"].null_count() < paris.height  # the refs say FLS only; this OVR game carries it
    assert paris["player2_id"].dtype == pl.Int64
    assert ovr["description"].null_count() > fls["description"].null_count()  # FLS carries English text


@pytest.mark.parametrize("page", [OVR, FLS], ids=["OVR", "FLS"])
def test_shots_have_pixel_coordinates_and_free_throws_are_not_located(page):
    shots = pp.parse_fiba_gamepage_shots(page)
    pbp = pp.parse_fiba_gamepage_pbp(page)
    assert shots.height == pbp.filter(pl.col("action_code").is_in(["P2", "P3", "FT"])).height > 0
    assert set(shots["shot_type"]) <= {"2PT", "3PT", "FT"}
    located = shots.filter(pl.col("has_location") == True)  # noqa: E712
    assert located.height and located["shot_x"].null_count() == 0 and located["shot_y"].is_between(0, 300).all()
    ft = shots.filter(pl.col("shot_type") == "FT")
    assert ft.height and (ft["has_location"] == False).all()  # noqa: E712
    assert ((ft["shot_x"] == 0) & (ft["shot_y"] == 0)).all()  # free throws are always 0, 0
    assert shots["made"].null_count() == 0 and shots["points_attempted"].is_in([1, 2, 3]).all()


@pytest.mark.parametrize("page", [OVR, FLS], ids=["OVR", "FLS"])
def test_teams_are_organisation_ids_and_sides_resolve(page):
    pbp = pp.parse_fiba_gamepage_pbp(page)
    orgs = {page["game"]["teamA"]["organisationId"]: "A", page["game"]["teamB"]["organisationId"]: "B"}
    acting = pbp.filter(pl.col("organisation_id").is_not_null())
    assert set(acting["organisation_id"].to_list()) <= set(orgs)
    assert all(orgs[o] == s for o, s in zip(acting["organisation_id"], acting["team_side"]))
    assert pbp.schema["organisation_id"] == pl.Int64


@pytest.mark.parametrize("page", [OVR, FLS], ids=["OVR", "FLS"])
def test_box_players_sum_to_the_team_box_and_the_final_score(page):
    players, teams = pp.parse_fiba_gamepage_box_players(page), pp.parse_fiba_gamepage_box_teams(page)
    assert teams.height == 2 and set(teams["team_side"]) == {"A", "B"}
    by_team = players.group_by("organisation_id").agg(
        pl.col("points").sum().alias("p"), pl.col("assists").sum().alias("a")
    )
    merged = teams.join(by_team, on="organisation_id", how="inner")
    assert merged.height == 2
    assert (merged["points"] == merged["p"]).all() and (merged["assists"] == merged["a"]).all()
    assert teams["score"].to_list() == [page["game"]["teamAScore"], page["game"]["teamBScore"]]
    assert players.filter(pl.col("has_played") == True)["seconds_played"].min() > 0  # noqa: E712


@pytest.mark.parametrize("page", [OVR, FLS], ids=["OVR", "FLS"])
def test_box_players_join_the_lineup_and_the_play_by_play_on_one_integer_person_id(page):
    box, lineup = pp.parse_fiba_gamepage_box_players(page), pp.parse_fiba_gamepage_lineup(page)
    pbp = pp.parse_fiba_gamepage_pbp(page)
    for left, right in ((box, lineup), (box, pbp)):
        assert left.schema["player_id"] == right.schema["player_id"] == pl.Int64  # asserted before joining
    assert box["last_name"].null_count() == 0  # every box row found its lineup-card name
    assert set(pbp["player_id"].drop_nulls()) <= set(box["player_id"])
    assert lineup.height == box.height == 24 and lineup["jersey"].dtype == pl.String


@pytest.mark.parametrize("page", [OVR, FLS], ids=["OVR", "FLS"])
def test_period_scores_add_up_to_the_final_score(page):
    per = pp.parse_fiba_gamepage_period_scores(page)
    totals = dict(per.group_by("team_side").agg(pl.col("points").sum()).iter_rows())
    assert totals == {"A": page["game"]["teamAScore"], "B": page["game"]["teamBScore"]}


def test_team_box_keeps_the_ids_across_pbp_box_and_schedule():
    box, pbp = pp.parse_fiba_gamepage_box_teams(OVR), pp.parse_fiba_gamepage_pbp(OVR)
    sched = gp.parse_fiba_gdap_schedule(_load(GDAP_FX, "gdap__getgdapgamesbycompetitionid__wc2023.json"))
    assert box.schema["organisation_id"] == pbp.schema["organisation_id"] == sched.schema["team_a_organisation_id"]


@pytest.mark.parametrize("name", ROUTES["gamepage"]["pbp"]["fixtures"])
def test_every_page_fixture_parses_all_six_views(name):
    page = _load(PAGE_FX, name)
    for short in ROUTES["gamepage"]:
        assert _parser("gamepage", short)(page).height > 0, (name, short)


def test_a_box_only_page_has_an_empty_pbp_but_a_box_score():
    page = {k: v for k, v in OVR.items() if k != "playByPlay"}
    assert pp.parse_fiba_gamepage_pbp(page).height == 0 and pp.parse_fiba_gamepage_shots(page).height == 0
    assert pp.parse_fiba_gamepage_box_players(page).height == 24


# --- generated wrappers ------------------------------------------------------------------------------------------------------


class _Recorder:
    def __init__(self, monkeypatch, module, body):
        self.calls: List[tuple] = []
        monkeypatch.setattr(module, "_get", lambda url, params=None, **kw: self.calls.append((url, params, kw)) or body)


def test_wrappers_build_the_gdap_urls_and_coerce_ids(monkeypatch):
    from sportsdataverse.basketball.fiba_hub import fiba_gdap as mod

    rec = _Recorder(monkeypatch, mod, _load(GDAP_FX, "gdap__getgdapgamesbycompetitionid__wc2023.json"))
    df = mod.fiba_gdap_schedule(comp="208182")
    url, params, _ = rec.calls[0]
    assert url == "https://digital-api.fiba.basketball/hapi/getgdapgamesbycompetitionid"
    assert params == {"gdapCompetitionId": 208182} and isinstance(params["gdapCompetitionId"], int)
    assert df.height == 3 and mod.fiba_gdap_schedule(comp=208182, return_parsed=False)[0]["gameId"]
    with pytest.raises(ValueError, match="integer"):
        mod.fiba_gdap_schedule(comp=208182.0)


def test_editions_sends_the_season_unchanged_and_fixed_params_ride_along(monkeypatch):
    from sportsdataverse.basketball.fiba_hub import fiba_gdap as mod

    rec = _Recorder(
        monkeypatch, mod, _load(GDAP_FX, "gdap__getgdapcompetitionsbyseasonsandcompetitionmarketingname__bcl-2025.json")
    )
    mod.fiba_gdap_editions(family_id=112, season=2025)
    sent = {k: v for k, v in rec.calls[0][1].items() if v is not None}  # the shared getter drops None
    assert sent == {"competitionMarketingNameId": 112, "fromSeason": 2025}
    rec2 = _Recorder(monkeypatch, mod, _load(GDAP_FX, "gdap__getgdapcompetitionteamsbycompetitionid__wc2023.json"))
    mod.fiba_gdap_teams(comp=208182)
    assert rec2.calls[0][1] == {"gdapCompetitionId": 208182, "profile": "true"}  # the API 500s without profile
    rec3 = _Recorder(monkeypatch, mod, _load(GDAP_FX, "gdap__getcustomgateway__federationRanking-men-world.json"))
    mod.fiba_gdap_gateway_federation_ranking(as_of_date="2025-09-15")
    assert rec3.calls[0][1]["fibaFunction"] == "federationRanking" and rec3.calls[0][1]["gender"] == "men"


def test_gamepage_wrappers_route_through_the_pseudo_path_with_comp(monkeypatch):
    from sportsdataverse.basketball.fiba_hub import fiba_gamepage as mod

    rec = _Recorder(monkeypatch, mod, OVR)
    df = mod.fiba_gamepage_pbp(game_id="100720", comp=208182)
    url, params, _ = rec.calls[0]
    assert url == "https://www.fiba.basketball/en/games/100720" and params == {"comp": 208182}
    assert df.height == _pbp_actions(OVR)
    assert mod.fiba_gamepage_box_teams(game_id=100720, comp=208182, return_parsed=False) is OVR
