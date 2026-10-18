"""Genius Sports LiveStats family: parsers over committed bodies, the YAML, and the runtime's refusals.

Every body is a committed fixture (``tests/fixtures/genius``): a FULL ``data.json`` (the numeric dtypes come from
it, never from the 3-row refs captures), the refs captures, and real saved hosted pages. Nothing reads the private
refs checkout, and no test touches the network or a proxy.
"""

from __future__ import annotations

import gzip
import json
import re
from pathlib import Path
from typing import Any, Dict

import polars as pl
import pytest
import yaml

from sportsdataverse.basketball import basketball_runtime as bb
from sportsdataverse.basketball.genius import genius_parsers as P
from sportsdataverse.basketball.genius import genius_runtime as rt
from sportsdataverse.errors import AssetFetchError, NoDataError

ROOT = Path(__file__).resolve().parents[2]
FIX = ROOT / "tests" / "fixtures" / "genius"
DATA_FRAMES = {
    "game": P.parse_genius_game,
    "team_box": P.parse_genius_team_box,
    "player_box": P.parse_genius_player_box,
    "pbp": P.parse_genius_pbp,
    "shots": P.parse_genius_shots,
}
ALL_PARSERS = {
    **DATA_FRAMES,
    "competition_games": P.parse_genius_competition_games,
    "match_competition": P.parse_genius_match_competition,
    "competitions": P.parse_genius_competitions,
    "schedule": P.parse_genius_schedule,
    "widget_games": P.parse_genius_widget_games,
}
SNAKE = re.compile(r"^[a-z][a-z0-9]*(_[a-z0-9]+)*$")


def _full(name: str = "data_2902690.json.gz", match_id: int = 2902690) -> Dict[str, Any]:
    path = FIX / "full" / name
    with gzip.open(path, "rt", encoding="utf-8") if path.suffix == ".gz" else open(path, encoding="utf-8") as fh:
        body = json.load(fh)
    body["_request"] = {"match_id": match_id}
    return body


def _capture(name: str) -> Any:
    return json.loads((FIX / "captures" / name).read_text(encoding="utf-8"))


def _html(name: str) -> str:
    return (FIX / "html" / name).read_text(encoding="utf-8")


@pytest.fixture(scope="module")
def game() -> Dict[str, Any]:
    return _full()


# ------------------------------------------------------------------------------ data.json frames
@pytest.mark.parametrize("name", list(DATA_FRAMES))
def test_full_body_frames_are_non_empty_snake_case_with_pinned_dtypes(game, name):
    df = DATA_FRAMES[name](game)
    assert df.height > 0
    assert all(SNAKE.match(c) for c in df.columns), df.columns
    assert dict(df.schema) == P.SCHEMAS[name]
    assert df["match_id"].to_list() == [2902690] * df.height


def test_game_row_reads_state_scores_and_officials(game):
    row = P.parse_genius_game(game).to_dicts()[0]
    assert (row["team1_score"], row["team2_score"], row["in_ot"], row["period_type"]) == (91, 89, 1, "OVERTIME")
    assert row["team1_code"] == "PER" and row["periods_max"] == 4 and row["period_length_overtime"] == 5
    assert row["referee1"] and row["commissioner"]  # the current feed nests officials.<role>


def test_team_box_totals_have_float_rates_and_in_game_team_numbers(game):
    box = P.parse_genius_team_box(game)
    assert box["tno"].to_list() == [1, 2]
    assert box["score"].to_list() == [91, 89] and box["ot_score"].to_list() == [12, box["ot_score"][1]]
    assert box.schema["field_goals_percentage"] == pl.Float64 and box.schema["time_leading"] == pl.Float64
    assert box.schema["field_goals_made"] == pl.Int64 and box.schema["eff_3"] == pl.Float64
    p = [box[f"p{i}_score"][0] for i in range(1, 5)] + [box["ot_score"][0]]
    assert sum(p) == box["score"][0]  # period scores plus overtime add up to the final score


def test_player_box_rows_key_on_in_game_numbers_not_ids(game):
    pb = P.parse_genius_player_box(game)
    assert pb.height == sum(len(t["pl"]) for t in game["tm"].values())
    assert pb.select(["tno", "pno"]).is_duplicated().sum() == 0
    assert pb.schema["tno"] == pb.schema["pno"] == pl.Int64 and pb.schema["shirt_number"] == pl.String
    assert pb.schema["minutes"] == pl.String and pb.schema["seconds_played"] == pl.Int64
    assert pb.filter(pl.col("tno") == 1)["points"].sum() == P.parse_genius_team_box(game)["points"][0]


def test_pbp_is_newest_first_with_integer_scores_and_a_game_end_row(game):
    pbp = P.parse_genius_pbp(game)
    assert pbp.height == len(game["pbp"])
    nums = pbp["action_number"].to_list()
    assert nums[0] == max(nums) and nums[-1] == min(nums) and len(set(nums)) == len(nums)
    assert nums != sorted(nums, reverse=True)  # newest first by game time, NOT strictly by action number
    first = pbp.row(0, named=True)
    assert (first["action_type"], first["sub_type"], first["score_1"], first["score_2"]) == ("game", "end", 91, 89)
    assert pbp.schema["score_1"] == pbp.schema["lead"] == pl.Int64
    assert pbp["row_index"].to_list() == list(range(pbp.height))


def test_every_shot_joins_one_pbp_row_on_action_number(game):
    shots, pbp = P.parse_genius_shots(game), P.parse_genius_pbp(game)
    assert shots.schema["action_number"] == pbp.schema["action_number"] == pl.Int64  # asserted before the join
    joined = shots.join(pbp.select("action_number", "action_type"), on="action_number", how="left")
    assert joined["action_type_right"].null_count() == 0 and joined.height == shots.height
    assert shots["x"].min() >= 0 and shots["x"].max() <= 100 and set(shots["result"].unique()) <= {0, 1}


def test_old_era_body_casts_string_scores_and_has_no_action_numbers():
    old = _full("data_12248.json", 12248)
    assert isinstance(old["tm"]["1"]["score"], str)  # the 2014-15 feed sends numeric strings
    box, pbp = P.parse_genius_team_box(old), P.parse_genius_pbp(old)
    assert box.schema["score"] == box.schema["p1_score"] == pl.Int64 and box["score"].null_count() == 0
    assert pbp["action_number"].null_count() == pbp.height and pbp["clock"].null_count() == pbp.height
    assert pbp.schema == P.parse_genius_pbp(_full()).schema  # the era does not change the dtypes


def test_integer_pbp_scores_and_a_truncated_capture_parse():
    body = _capture("data_BSN_2080988.json")
    assert isinstance(body["pbp"][0]["s1"], int)
    pbp = P.parse_genius_pbp(body)
    assert pbp.height == 3 and pbp.schema["score_1"] == pl.Int64 and pbp["match_id"].null_count() == 3


def test_minutes_text_and_seconds():
    assert P._seconds("35:08") == 2108 and P._seconds("224:60") == 13500
    assert P._seconds(0) is None and P._seconds("n/a") is None and P._seconds(None) is None


# ------------------------------------------------------------------------------ the other routes
def test_competition_window_ids_and_scores():
    df = P.parse_genius_competition_games(_capture("ESAKE__49915.json"))
    assert df.schema == P.SCHEMAS["competition_games"]
    row = df.row(0, named=True)
    assert row["competition_id"] == 49915 and row["home_score"] == 99 and row["away_score"] == 83  # strings cast
    assert row["home_team_id"] == 23954 and df.schema["match_id"] == pl.Int64


def test_match_competition_page_is_one_row():
    df = P.parse_genius_match_competition({**_capture("u_ESAKE_2902690.json"), "_request": {"org": "ESAKE"}})
    assert df.height == 1 and df["comp_id"][0] == 49915 and df["org"][0] == "ESAKE"


@pytest.mark.parametrize(
    "kind,name,n",
    [
        ("u", "u_ESAKE_2902690.html", None),
        ("chooser", "hosted_ESAKE_schedule.html", 47),
        ("schedule", "hosted_ESAKE_34541_first3.html", 3),
        ("embednf", "embednf_HKS_49946_first3.json", 3),
        ("widget", "widget_ESAKE.js", 7),
    ],
)
def test_real_pages_reduce_to_records(kind, name, n):
    rec = P._page_record(kind, _html(name))
    key = {"chooser": "competitions"}.get(kind, "matches")
    if n is not None:
        assert len(rec[key]) == n
    else:
        assert rec["match_id"] == 2902690 and rec["comp_id"] == 49915


def test_page_records_match_the_refs_captures():
    assert P._page_record("u", _html("u_ESAKE_2902690.html")) == _capture("u_ESAKE_2902690.json")
    assert P._page_record("embednf", _html("embednf_HKS_49946_first3.json")) == _capture(
        "embednf_schedule_HKS_49946.json"
    )
    assert P._page_record("widget", _html("widget_ESAKE.js"))["matches"][:3] == _capture("widget_ESAKE.json")["matches"]


def test_schedule_scores_are_null_when_the_page_prints_none():
    rec = _capture("hosted_schedule_CEBL_47852.json")
    df = P.parse_genius_schedule(rec)
    # one game in progress, and a COMPLETE game whose score the page does not print: both are null, not 0
    assert df["status"].to_list() == ["IN_PROGRESS", "COMPLETE", "COMPLETE"]
    assert df.height == 3 and df["home_score"].null_count() == 2 and df.schema["home_score"] == pl.Int64
    assert df["match_id"].to_list() == [2845983, 2798717, 2845984]


def test_a_schedule_page_with_no_matches_is_empty_not_a_parse_failure():
    with pytest.raises(P._NotParsed) as err:
        P._page_record("schedule", _html("hosted_ESAKE_28567_schedule.html"))
    assert err.value.empty is True


def test_widget_and_chooser_frames():
    w = P.parse_genius_widget_games(P._page_record("widget", _html("widget_ESAKE.js")))
    assert w.height == 7 and set(w["webcast_org"].to_list()) == {"ESAKE"} and w.schema["home_team_id"] == pl.Int64
    c = P.parse_genius_competitions(
        {**P._page_record("chooser", _html("hosted_ESAKE_schedule.html")), "_request": {"org": "ESAKE"}}
    )
    assert c.height == 47 and c["selected"].sum() == 1 and c["comp_id"].is_unique().all()


# ------------------------------------------------------------------------------ season mapping
@pytest.mark.parametrize(
    "label,year,span,want",
    [
        ("BASKET LEAGUE 2019 - 20", 2019, "split", (2020, "label span")),
        ("2023-2024", 2023, "split", (2024, "label span")),
        ("26-27", None, "split", (2027, "label span")),
        ("Stoiximan GBL", 2026, "split", (2027, "genius year + 1 (split)")),  # START year -> END year
        ("2026", 2026, "calendar", (2026, "genius year (calendar)")),  # CEBL, BSN, LNBP, NZNBL, BAL: END year
        ("CEBL Summer 2022", None, "calendar", (2022, "label year (calendar)")),
        ("Cup", None, "split", (None, "unknown")),
        ("Cup", 2026, None, (None, "unknown")),  # an unknown org gets no guessed season
    ],
)
def test_season_end_rules(label, year, span, want):
    assert P.season_end(label, year, span) == want


def test_competitions_map_start_year_orgs_up_and_calendar_orgs_straight():
    split = P.parse_genius_competitions({**_capture("hosted_schedule_ESAKE.json"), "_request": {"org": "ESAKE"}})
    assert split.select((pl.col("season") == pl.col("year") + 1).all())[0, 0]  # ESAKE labels carry no span here
    cal = P.parse_genius_competitions(
        {"competitions": [{"comp_id": 1, "label": "2026", "league_id": 5, "year": 2026, "org": "BAL"}]}
    )
    assert cal["season"].to_list() == [2026] and cal["season_rule"].to_list() == ["genius year (calendar)"]


def test_org_span_covers_the_registry_orgs():
    assert P.ORG_SPAN["ESAKE"] == "split" and P.ORG_SPAN["CEBL"] == P.ORG_SPAN["NZNBL"] == "calendar"
    assert {"BNXT", "BAL", "BSN", "LNBP", "NZN", "SPB"} <= set(P.ORG_SPAN)


# ------------------------------------------------------------------------------ parser contract
@pytest.mark.parametrize("name", list(ALL_PARSERS))
@pytest.mark.parametrize("payload", [{}, [], None, "x", 5, {"tm": "x", "pbp": 3}])
def test_empty_or_malformed_payload_is_a_typed_zero_row_frame(name, payload):
    df = ALL_PARSERS[name](payload)
    assert df.height == 0 and dict(df.schema) == P.SCHEMAS[name]


@pytest.mark.parametrize("name", list(ALL_PARSERS))
def test_two_empty_frames_stack_and_stack_onto_a_populated_one(name, game):
    sources = {
        **{k: game for k in DATA_FRAMES},
        "competition_games": _capture("ESAKE__49915.json"),
        "match_competition": {**_capture("u_ESAKE_2902690.json"), "_request": {"org": "ESAKE"}},
        "competitions": {**_capture("hosted_schedule_ESAKE.json"), "_request": {"org": "ESAKE"}},
        "schedule": _capture("hosted_schedule_CEBL_47852.json"),
        "widget_games": _capture("widget_ESAKE.json"),
    }
    empty, full = ALL_PARSERS[name]({}), ALL_PARSERS[name](sources[name])
    assert full.height > 0
    assert pl.concat([empty, empty]).height == 0
    assert pl.concat([empty, full]).height == full.height  # strict vertical: identical schemas


@pytest.mark.parametrize("name", list(ALL_PARSERS))
def test_return_as_pandas(name, game):
    pd = pytest.importorskip("pandas")
    assert isinstance(ALL_PARSERS[name]({}, return_as_pandas=True), pd.DataFrame)
    assert isinstance(DATA_FRAMES["game"](game, return_as_pandas=True), pd.DataFrame)


def test_columns_argument_is_accepted_and_the_pinned_schema_wins(game):
    assert P.parse_genius_game(game, columns=["x"]).columns == list(P.SCHEMAS["game"])


def test_every_public_parser_has_an_example_block():
    for name in P.__all__:
        doc = getattr(P, name).__doc__ or ""
        assert all(h in doc for h in ("Args:", "Returns:", "Example:")), name
        assert not any(ln.strip().startswith("None:") for ln in doc.splitlines()), name  # a parser raises nothing
        assert ">>>" not in doc, name


# ------------------------------------------------------------------------------ YAML and schemas
def _yaml() -> Dict[str, Any]:
    return yaml.safe_load((ROOT / "tools" / "codegen" / "endpoints" / "genius.yaml").read_text(encoding="utf-8"))


def test_yaml_lists_every_spec_route():
    routes = json.loads((FIX / "spec_routes.json").read_text(encoding="utf-8"))
    assert len(routes) == 7
    wanted = {re.sub(r"\{(\w+)\}", "{}", p) for p in routes}
    got = {re.sub(r"\{(\w+)\}", "{}", e["path"].split("?")[0]) for e in _yaml()["endpoints"]}
    # the spec says compId / matchId; the wrappers say comp_id / match_id
    assert wanted == got
    assert len(_yaml()["endpoints"]) == 11


def test_no_path_token_named_league_or_sport_and_org_and_competition_are_parameters():
    names = {p["name"] for e in _yaml()["endpoints"] for p in e.get("path_params", [])}
    assert not {"league", "sport"} & names
    assert {"org", "comp_id", "match_id"} <= names


def test_every_endpoint_schema_matches_its_pinned_parser_schema():
    for e in _yaml()["endpoints"]:
        doc = yaml.safe_load(
            (ROOT / "tools" / "codegen" / "schemas" / f"{e['returns_schema']}.yaml").read_text(encoding="utf-8")
        )
        frame = e["short"] if e["short"] != "schedule_embed" else "schedule"
        assert [c["name"] for c in doc["columns"]] == list(P.SCHEMAS[frame]), e["short"]
        assert all(c["description"] == "" for c in doc["columns"])  # descriptions live in manual_column_descriptions
        kinds = {pl.Int64: "integer", pl.Float64: "numeric", pl.String: "character", pl.Boolean: "logical"}
        assert [c["type"] for c in doc["columns"]] == [kinds[t] for t in P.SCHEMAS[frame].values()], e["short"]


def test_the_owner_flags_are_in_the_docstring_notes():
    notes = " ".join(_yaml()["docstring"]["notes"])
    assert "GENIUS_PROXY" in notes and "robots.txt" in notes and "data.json" in notes


# ------------------------------------------------------------------------------ runtime
class _Resp:
    def __init__(self, status: int, text: str = "", headers: Dict[str, str] | None = None):
        self.status_code, self.text, self.reason = status, text, "x"
        self.headers = headers or {}
        self.url = "https://example.invalid"

    def json(self) -> Any:
        return json.loads(self.text)


CLOUDFRONT = (
    "<html>ERROR: The request could not be satisfied. Request blocked. Generated by cloudfront (CloudFront)</html>"
)
ACCESS_DENIED = "<Error><Code>AccessDenied</Code><Message>Access Denied</Message></Error>"
DATA_URL = f"https://{rt.LIVE}/data/2902690/data.json"
WINDOW_URL = f"https://{rt.LIVE}/data/competition/49915.json"
U_URL = f"https://{rt.LIVE}/u/ESAKE/2902690/"
CHOOSER_URL = f"https://{rt.HOSTED}/ESAKE/en/schedule"
SCHEDULE_URL = f"https://{rt.HOSTED}/ESAKE/en/competition/34541/schedule"
WIDGET_KEY = "RFJIN037I07QV520B7WEXKV1GYNA31"
WIDGET_URL = f"https://{rt.WIDGET}/widget/?{WIDGET_KEY}"


@pytest.fixture
def answer(monkeypatch):
    """Stub the one transport call; returns a setter and the list of recorded call kwargs."""
    calls = []
    state = {"resp": _Resp(200)}

    def fake(url, **kw):
        calls.append((url, kw))
        r = state["resp"]
        if isinstance(r, BaseException):
            raise r
        return r

    monkeypatch.setattr(rt, "download", fake)
    monkeypatch.delenv(rt.PROXY_ENV, raising=False)

    def set_(status=200, text="", **kw):
        state["resp"] = status if isinstance(status, BaseException) else _Resp(status, text, **kw)

    set_.calls = calls  # type: ignore[attr-defined]
    return set_


def _game_text(**edit: Any) -> str:
    body = _full()
    body.pop("_request")
    body.update(edit)
    return json.dumps(body)


def test_data_json_is_returned_with_the_match_id_stamped(answer):
    answer(200, _game_text())
    body = rt._get(DATA_URL)
    assert body["_request"] == {"match_id": 2902690} and body["tm"]["1"]["score"] == 91
    assert P.parse_genius_game(body)["match_id"][0] == 2902690


def test_every_attempt_goes_through_the_polite_session_and_never_retries_a_403(answer):
    answer(200, _game_text())
    rt._get(DATA_URL)
    ((_, kw),) = answer.calls
    assert kw["session"] is bb.SESSION and 403 not in kw["retry_statuses"] and {429, 503} <= set(kw["retry_statuses"])
    assert kw["proxy"] is None  # no GENIUS_PROXY set


def test_the_proxy_comes_from_the_environment_and_never_leaks_into_an_error(answer, monkeypatch):
    secret = "http://user:hunter2@proxy.example:8080"
    monkeypatch.setenv(rt.PROXY_ENV, secret)
    answer(200, _game_text())
    rt._get(DATA_URL)
    assert answer.calls[-1][1]["proxy"] == {"http": secret, "https": secret}
    answer(ConnectionError(f"cannot reach {secret}"))
    with pytest.raises(AssetFetchError) as err:
        rt._get(DATA_URL)
    assert "hunter2" not in str(err.value) and "proxy.example" not in str(err.value)
    assert "ConnectionError" in str(err.value) and rt.PROXY_ENV in str(err.value) and err.value.__cause__ is None
    answer(403, CLOUDFRONT)
    with pytest.raises(AssetFetchError) as err:
        rt._get(DATA_URL)
    assert "hunter2" not in str(err.value) and "proxy.example" not in str(err.value)


def test_cloudfront_block_is_an_unknown_answer_that_names_the_variable(answer):
    answer(403, CLOUDFRONT)
    with pytest.raises(AssetFetchError, match="GENIUS_PROXY"):
        rt._get(DATA_URL)


def test_s3_access_denied_is_no_such_object(answer):
    answer(403, ACCESS_DENIED)
    with pytest.raises(NoDataError, match="no such object"):
        rt._get(DATA_URL)


@pytest.mark.parametrize(
    "status,text,exc",
    [
        (404, "nope", NoDataError),
        (400, "bad", ValueError),
        (422, "bad", ValueError),
        (429, "slow down", AssetFetchError),
        (503, "down", AssetFetchError),
        (403, "<html>forbidden</html>", AssetFetchError),
    ],
)
def test_status_vocabulary(answer, status, text, exc):
    answer(status, text)
    with pytest.raises(exc):
        rt._get(DATA_URL)


def test_a_hosted_500_website_error_page_points_at_the_embed_route(answer):
    answer(500, "<html>A website error has occurred. Contact the administrator</html>")
    with pytest.raises(AssetFetchError, match="genius_schedule_embed"):
        rt._get(SCHEDULE_URL)


@pytest.mark.parametrize("text", ["", "   ", "<html>not data</html>", '{"tm": {"1": {}', "[1]"])
def test_data_json_stubs_and_bad_bodies_are_refused(answer, text):
    answer(200, text)
    with pytest.raises(AssetFetchError):
        rt._get(DATA_URL)


def test_data_json_without_both_teams_or_pbp_is_refused(answer):
    answer(200, json.dumps({"error": "no such game"}))
    with pytest.raises(AssetFetchError, match="tm.1, tm.2 and pbp"):
        rt._get(DATA_URL)
    answer(200, json.dumps({"tm": {"1": {}}, "pbp": []}))
    with pytest.raises(AssetFetchError):
        rt._get(DATA_URL)


def test_an_unfinished_game_is_refused(answer):
    body = _full()
    body.pop("_request")
    body["pbp"] = [r for r in body["pbp"] if not (r["actionType"] == "game" and r["subType"] == "end")]
    answer(200, json.dumps(body))
    with pytest.raises(AssetFetchError, match="not finished"):
        rt._get(DATA_URL)


def test_competition_window(answer):
    answer(200, json.dumps(_capture("ESAKE__49915.json")))
    assert len(rt._get(WINDOW_URL)) == 1
    answer(200, json.dumps(_capture("POL__50175.json")))  # a window of another competition
    with pytest.raises(AssetFetchError, match="not the competition asked for"):
        rt._get(WINDOW_URL)
    answer(200, "[]")
    with pytest.raises(NoDataError):
        rt._get(WINDOW_URL)
    row = dict(_capture("ESAKE__49915.json")[0], competitionId="49915")  # a string id is another type
    answer(200, json.dumps([row]))
    with pytest.raises(AssetFetchError):
        rt._get(WINDOW_URL)


def test_u_shell_must_be_the_match_asked_for(answer):
    answer(200, _html("u_ESAKE_2902690.html"))
    rec = rt._get(U_URL)
    assert rec["comp_id"] == 49915 and rec["_request"] == {"org": "ESAKE"}
    answer(200, _html("u_ESAKE_2902690.html").replace('value = "2902690"', 'value = "2902691"'))
    with pytest.raises(AssetFetchError, match="not the match asked for"):
        rt._get(U_URL)


def test_chooser_must_be_the_org_asked_for_and_an_empty_200_is_no_hosted_site(answer):
    answer(200, _html("hosted_ESAKE_schedule.html"))
    assert rt._get(CHOOSER_URL)["_request"] == {"org": "ESAKE"}
    with pytest.raises(AssetFetchError, match="not the org asked for"):
        rt._get(f"https://{rt.HOSTED}/FRB/en/schedule")
    answer(200, "")
    with pytest.raises(NoDataError, match="without hosted pages"):
        rt._get(CHOOSER_URL)
    answer(200, "<html><body>maintenance</body></html>")  # a page that is not the chooser
    with pytest.raises(NoDataError):
        rt._get(CHOOSER_URL)


def test_schedule_scope_and_truncation(answer):
    answer(200, _html("hosted_ESAKE_34541_first3.html"))
    rec = rt._get(SCHEDULE_URL)
    assert len(rec["matches"]) == 3 and rec["_request"] == {"org": "ESAKE", "comp_id": 34541}
    with pytest.raises(AssetFetchError, match="not the competition asked for"):
        rt._get(f"https://{rt.HOSTED}/ESAKE/en/competition/99999/schedule")
    with pytest.raises(AssetFetchError, match="not the org asked for"):
        rt._get(f"https://{rt.HOSTED}/FRB/en/competition/34541/schedule")
    answer(200, _html("hosted_ESAKE_34541_first3.html").replace("</html>", ""))  # cut off
    with pytest.raises(AssetFetchError, match="cut off"):
        rt._get(SCHEDULE_URL)
    answer(200, _html("hosted_ESAKE_28567_schedule.html"))  # a real page with no matches
    with pytest.raises(NoDataError, match="no matches"):
        rt._get(f"https://{rt.HOSTED}/ESAKE/en/competition/28567/schedule")


def test_a_schedule_block_linking_another_match_is_refused(answer):
    text = _html("hosted_ESAKE_34541_first3.html").replace("/match/2147860/", "/match/2147861/", 1)
    answer(200, text)
    with pytest.raises(AssetFetchError, match="links to"):
        rt._get(SCHEDULE_URL)


FFBB_URL = f"https://{rt.HOSTED}/FFBB/en/competition/21849/schedule"


def test_a_to_be_determined_block_takes_the_scope_the_other_blocks_prove(answer):
    # Block 1 of the real FFBB 21849 page is an unfilled play-off slot: no team links, no match link.
    text = _html("hosted_FFBB_21849_first3.html")
    assert P._page_record("schedule", text)["matches"][0]["comp_id"] is None
    answer(200, text)
    rec = rt._get(FFBB_URL)
    assert [(m["match_id"], m["comp_id"], m["org"]) for m in rec["matches"]] == [
        (1145701, 21849, "FFBB"), (909156, 21849, "FFBB"), (909159, 21849, "FFBB")
    ]  # fmt: skip
    with pytest.raises(AssetFetchError, match="not the competition asked for"):  # still refused under another comp
        rt._get(f"https://{rt.HOSTED}/FFBB/en/competition/18540/schedule")


def test_a_schedule_of_only_placeholder_blocks_is_refused(answer):
    text = _html("hosted_FFBB_21849_first3.html")
    first, second = text.find('<div class="match-wrap '), text.find('<div class="match-wrap ', 100)
    answer(200, text[:second] + "</body></html>")
    assert text[first:second].count("extfix_1145701") == 1
    with pytest.raises(AssetFetchError, match="no block names"):
        rt._get(FFBB_URL)


def test_embed_envelope(answer):
    answer(200, _html("embednf_HKS_49946_first3.json"))
    rec = rt._get(f"https://{rt.HOSTED}/embednf/HKS/en/competition/49946/schedule", {"roundNumber": -1})
    assert rec["_request"] == {"org": "HKS", "comp_id": 49946} and answer.calls[-1][1]["params"] == {"roundNumber": -1}
    answer(200, "<html>plain page</html>")
    with pytest.raises(AssetFetchError, match="not JSON|envelope"):
        rt._get(f"https://{rt.HOSTED}/embednf/HKS/en/competition/49946/schedule")


def test_widget_scope(answer):
    answer(200, _html("widget_ESAKE.js"))
    assert len(rt._get(WIDGET_URL)["matches"]) == 7
    assert answer.calls[-1][1]["params"] is None  # the key rides in the URL's query string
    with pytest.raises(AssetFetchError, match="not the widget asked for"):  # the FRB key must not return ESAKE games
        rt._get(f"https://{rt.WIDGET}/widget/?TXMFPAVMWV2N5ML0BJYODXOEWWIQKS")
    answer(200, "var nothing = 1;")
    with pytest.raises(AssetFetchError, match="var html"):
        rt._get(WIDGET_URL)


def test_unknown_routes_and_arguments(answer):
    with pytest.raises(ValueError, match="not a Genius route"):
        rt._get("https://example.com/data/1/data.json")
    with pytest.raises(ValueError, match="not a Genius route"):
        rt._get(f"https://{rt.LIVE}/data/abc/data.json")
    with pytest.raises(TypeError):
        rt._get(DATA_URL, timeout=3)
    assert answer.calls == []


# ------------------------------------------------------------------------------ throttling (real download())
@pytest.fixture
def clock(monkeypatch):
    state = {"now": 1000.0, "slept": [], "sent": []}
    monkeypatch.setattr(bb.time, "monotonic", lambda: state["now"])

    def sleep(s):
        state["slept"].append(s)
        state["now"] += s

    monkeypatch.setattr(bb.time, "sleep", sleep)
    monkeypatch.setattr(bb, "_last", {})
    return state


def _wire(monkeypatch, clock, answers):
    it = iter(answers)

    def fake(self, method, url, *a, **k):
        clock["sent"].append(clock["now"])
        return next(it)

    monkeypatch.setattr(bb.requests.Session, "request", fake)


def test_each_attempt_of_a_retried_request_waits_its_turn(clock, monkeypatch):
    monkeypatch.delenv(rt.PROXY_ENV, raising=False)
    _wire(monkeypatch, clock, [_Resp(503, "down"), _Resp(200, _game_text())])
    assert rt._get(DATA_URL)["_request"]["match_id"] == 2902690
    assert len(clock["sent"]) == 2 and clock["sent"][1] - clock["sent"][0] >= bb.MIN_INTERVAL
    assert bb._last[rt.LIVE] == pytest.approx(clock["sent"][1])


def test_retry_after_on_429_is_honoured(clock, monkeypatch):
    monkeypatch.delenv(rt.PROXY_ENV, raising=False)
    _wire(monkeypatch, clock, [_Resp(429, "slow", {"Retry-After": "7"}), _Resp(200, _game_text())])
    rt._get(DATA_URL)
    assert clock["sent"][1] - clock["sent"][0] >= 7


def test_403_is_answered_once_without_a_retry(clock, monkeypatch):
    monkeypatch.delenv(rt.PROXY_ENV, raising=False)
    _wire(monkeypatch, clock, [_Resp(403, ACCESS_DENIED)])
    with pytest.raises(NoDataError):
        rt._get(DATA_URL)
    assert len(clock["sent"]) == 1


# ------------------------------------------------------------------------------ generated wrappers (after integration)
def test_wrappers_build_the_documented_urls(monkeypatch):
    api = pytest.importorskip("sportsdataverse.basketball.genius.genius")
    seen = []
    monkeypatch.setattr(api, "_get", lambda url, params=None, **kw: seen.append((url, params)) or {})
    api.genius_game(match_id=2902690)
    api.genius_competition_games(comp_id=49915)
    api.genius_match_competition(org="NZN", match_id=2785478)
    api.genius_competitions(org="ESAKE")
    api.genius_schedule(org="CEBL", comp_id=47852)
    api.genius_schedule_embed(org="HKS", comp_id=49946, round_number=3)
    api.genius_widget_games(widget_key=WIDGET_KEY)
    urls = [u for u, _ in seen]
    assert urls == [
        f"https://{rt.LIVE}/data/2902690/data.json",
        f"https://{rt.LIVE}/data/competition/49915.json",
        f"https://{rt.LIVE}/u/NZN/2785478/",
        f"https://{rt.HOSTED}/ESAKE/en/schedule",
        f"https://{rt.HOSTED}/CEBL/en/competition/47852/schedule",
        f"https://{rt.HOSTED}/embednf/HKS/en/competition/49946/schedule",
        WIDGET_URL,
    ]
    assert seen[4][1] == {"roundNumber": -1} and seen[5][1] == {"roundNumber": 3}
