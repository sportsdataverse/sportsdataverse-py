"""Offline tests for the ABA League (``aba``) flat-API family: parsers, runtime and YAML.

Parsers are asserted against real data: the FULL match record of one game (ABA 2025-26 game 6, with
overtime) in ``tests/fixtures/aba/full/`` (dtypes come from it), the refs capture records and the whole shot and
minute-chart bodies in ``tests/fixtures/aba/``, and the refs' cut-down HTML pages in ``tests/fixtures/aba/*.html``
(the HTML parser's input). The runtime is driven through a scripted ``requests`` adapter mounted on a
``PoliteSession``, so every refusal is exercised end to end and every attempt is counted. No network, and nothing
reads the private sdv-internal-refs checkout.
"""

from __future__ import annotations

import json
import re
import sys
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple

import pandas as pd
import polars as pl
import pytest
import requests
import yaml
from requests.adapters import HTTPAdapter

import sportsdataverse.basketball.aba as aba_pkg
from sportsdataverse import dl_utils
from sportsdataverse.basketball import basketball_runtime
from sportsdataverse.basketball.aba import aba_parsers as P
from sportsdataverse.basketball.aba import aba_runtime as R
from sportsdataverse.errors import AssetFetchError, NoDataError

ROOT = Path(__file__).parents[3]
FIXTURES = ROOT / "tests" / "fixtures" / "aba"
YAML_PATH = ROOT / "tools" / "codegen" / "endpoints" / "aba.yaml"
SCHEMA_DIR = ROOT / "tools" / "codegen" / "schemas" / "native" / "aba"

sys.path.insert(0, str(ROOT / "tools" / "codegen"))
import gen_aba  # noqa: E402

SNAKE = re.compile(r"^[a-z0-9]+(_[a-z0-9]+)*$")
ALL_PARSERS = {n: getattr(P, f"parse_aba_{n}") for n in P.SPECS}
WWW, DRUGA = "https://www.aba-liga.com", "https://druga.aba-liga.com"
M6 = {"game_no": 6, "sez": 25, "lea": 1}


def _full() -> Dict[str, Any]:
    return json.loads((FIXTURES / "full" / "match_6_25_1.json").read_text(encoding="utf-8"))


def _json(name: str) -> Any:
    return json.loads((FIXTURES / name).read_text(encoding="utf-8"))


def _html(name: str) -> str:
    return (FIXTURES / name).read_text(encoding="utf-8")


def _stamped(rows: Any) -> Dict[str, Any]:
    return {"rows": rows, R.REQUEST_KEY: {"game_no": 6, "season": 2026, "league_id": 1, "league": "aba"}}


# --- parsers over every committed body ----------------------------------------------------------


@pytest.mark.parametrize("short", sorted(gen_aba.ENDPOINTS))
def test_every_wrapper_parses_its_fixture(short):
    df = gen_aba._frame(gen_aba.ENDPOINTS[short])
    assert isinstance(df, pl.DataFrame) and df.height > 0, short
    assert all(SNAKE.match(c) for c in df.columns), [c for c in df.columns if not SNAKE.match(c)]
    assert dict(df.schema) == dict(P.SPECS[short]), short
    ids = [c for c in df.columns if c.endswith("_id") or c in ("game_no", "season")]
    assert {c: df.schema[c] for c in ids} == {c: pl.Int64 for c in ids}


def test_numeric_dtypes_come_from_the_full_body():
    box = P.parse_aba_box_players(_full())
    assert box.schema["fg_pct"] == pl.Float64 and box.schema["ft_pct"] == pl.Float64
    assert box.schema["minutes"] == pl.Utf8 and box.schema["points"] == pl.Int64 and box.schema["valuation"] == pl.Int64
    assert box.schema["starter"] == pl.Boolean and box.schema["played"] == pl.Boolean
    team = P.parse_aba_box_teams(_full())
    assert team.schema["fg3_pct"] == pl.Float64 and team["minutes"].to_list() == ["225:00", "225:00"]
    game = P.parse_aba_game(_full())
    assert game.schema["date"] == pl.Date and game.schema["attendance"] == pl.Int64
    pbp = P.parse_aba_pbp(_full())
    assert pbp.schema["seq"] == pl.Int64 and pbp.schema["clock"] == pl.Utf8 and pbp.schema["home_score"] == pl.Int64


def test_game_header_and_periods():
    g = P.parse_aba_game(_full()).row(0, named=True)
    assert (g["game_no"], g["season"], g["league_id"], g["league"]) == (6, 2026, 1, "aba")
    assert (g["home_team_id"], g["home_score"], g["away_team_id"], g["away_score"]) == (
        g["home_team_id"],
        93,
        g["away_team_id"],
        97,
    )
    assert str(g["date"]) == "2025-10-05" and g["time"] == "19:30" and g["time_zone"] == "CET"
    assert (g["home_team_id"], g["away_team_id"], g["attendance"], g["venue"]) == (
        12,
        91,
        1200,
        "Sportski centar Morača",
    )
    assert g["season_label"] == "2025/26" and g["group"] == "B"
    assert g["referees"].count(";") == 2 and g["commissioner"]
    periods = P.parse_aba_periods(_full())
    assert periods["period"].to_list() == ["Q1", "Q2", "Q3", "Q4", "OT1"]
    assert periods["period_no"].to_list() == [1, 2, 3, 4, 5]
    assert (periods["home_points"].sum(), periods["away_points"].sum()) == (93, 97)  # overtime included


def test_box_score_adds_up_to_the_header_and_the_team_totals():
    box = P.parse_aba_box_players(_full())
    teams = P.parse_aba_box_teams(_full())
    g = P.parse_aba_game(_full()).row(0, named=True)
    pts = dict(box.group_by("side").agg(pl.col("points").sum()).iter_rows())
    assert pts == {"home": 93, "away": 97}
    assert teams["side"].to_list() == ["home", "away"] and teams["points"].to_list() == [93, 97]
    assert teams["team_id"].to_list() == [g["home_team_id"], g["away_team_id"]]
    assert box.schema["team_id"] == teams.schema["team_id"] == pl.Int64
    reb = dict(box.group_by("side").agg(pl.col("reb_tot").sum()).iter_rows())
    assert reb == dict(zip(teams["side"], teams["reb_tot"]))  # team rebounds are not added on top


def test_a_did_not_play_row_keeps_the_schema_and_nulls_the_stats():
    box = P.parse_aba_box_players(_full())
    dnp = box.filter(pl.col("played") == False)  # noqa: E712
    assert dnp.height == 2  # two did-not-play rows in this game
    row = dnp.row(0, named=True)
    assert row["minutes"] == "00:00" and row["points"] is None and row["fg_pct"] is None and row["valuation"] is None
    assert box.filter(pl.col("played") == True)["minutes"].str.contains(":").all()  # noqa: E712


def test_pbp_is_in_game_order_and_ends_on_the_final_score():
    pbp = P.parse_aba_pbp(_full())
    assert pbp.height == 659 and pbp["seq"].to_list() == list(range(1, 660))
    scored = pbp.filter(pl.col("home_score").is_not_null()).tail(1).row(0, named=True)
    assert (scored["home_score"], scored["away_score"]) == (93, 97)
    assert pbp["period"].unique().sort().to_list() == [1, 2, 3, 4, 5]  # every overtime is period 5
    assert pbp["text"].to_list()[-1] == "game finished"
    ticks = pbp.filter(pl.col("clock") == "08:34")
    assert ticks.height > 0 and ticks["seconds_left"].unique().to_list() == [514]
    made = pbp.filter(pl.col("action") == "made 3 points")
    assert made.height > 0 and made["qualifier"].null_count() == 0
    team_rows = pbp.filter(pl.col("action").is_null() & pl.col("qualifier").is_not_null())
    assert (
        set(team_rows["qualifier"]) >= {"defensive", "offensive"}
        and team_rows["player_id"].null_count() == team_rows.height
    )
    ids = pbp["player_id"].drop_nulls()
    box = P.parse_aba_box_players(_full())
    assert ids.is_in(box["player_id"]).all()  # play-by-play and box score share one id space
    assert pbp.schema["player_id"] == box.schema["player_id"]


def test_box_only_season_has_no_pbp_starters_or_advanced_columns():
    rec = P.record_for("match", _html("match_1_8_1.html"), {"game_no": 1, "sez": 8, "lea": 1})
    assert rec["pbp"] == []
    assert P.parse_aba_pbp(rec).height == 0 and P.parse_aba_pbp(rec).schema == pl.Schema(P.SPECS["pbp"])
    box = P.parse_aba_box_players(rec)
    assert box["starter"].null_count() == box.height and box["on_court_at_end"].null_count() == box.height
    assert box["pts_paint"].null_count() == box.height  # the 'Points from' trio did not exist
    g = P.parse_aba_game(rec).row(0, named=True)
    assert g["season"] == 2009 and g["venue"] is None and g["season_label"] == "2008/09"


def test_cut_down_page_equals_the_refs_capture_of_the_same_game():
    rec = P.record_for("match", _html("match_6_25_1.html"), M6)
    cap = _json("captures/match__gameNo.json")
    assert rec["game"] == cap["game"] and rec["periods"][:2] == cap["periods"][:2]
    assert rec["players"][0] == cap["players"][0] and rec["teams"][0] == cap["teams"][0]


def test_shots_ids_are_cast_and_the_chart_joins_the_box_score():
    shots = P.parse_aba_shots(_stamped(_json("create_shooting_chart_6_25_1.json")))
    assert shots.height == 152 and set(shots["side"]) == {"home", "away"}
    assert shots.schema["x"] == shots.schema["y"] == pl.Float64 and shots.schema["made"] == pl.Boolean
    assert shots.schema["player_id"] == pl.Int64 and shots["player_id"].null_count() == 1  # id "0": not recorded
    assert shots.filter(pl.col("player_id").is_null())["player_name"].null_count() == 1
    box = P.parse_aba_box_players(_full())
    assert shots["player_id"].drop_nulls().is_in(box["player_id"]).all()
    assert shots.schema["player_id"] == box.schema["player_id"]
    assert shots.filter(pl.col("side") == "home")["x"].max() < 50 < shots.filter(pl.col("side") == "away")["x"].min()


def test_shot_x_is_a_float_whether_the_site_sent_a_string_or_a_number():
    rows = [
        {
            "ekipa": "1",
            "koordinata_x": "8.92",
            "koordinata_y": "26.1",
            "koordinata_uspeh": "0",
            "player_id": "7",
            "player_name": "A B",
        },
        {
            "ekipa": "2",
            "koordinata_x": 91.14000000000001,
            "koordinata_y": "35.14",
            "koordinata_uspeh": "1",
            "player_id": "8",
            "player_name": "C D",
        },
    ]
    df = P.parse_aba_shots(_stamped(rows))
    assert df["x"].to_list() == [8.92, 91.14000000000001] and df["made"].to_list() == [False, True]
    assert df["game_no"].to_list() == [6, 6] and df["league"].to_list() == ["aba", "aba"]


def test_minute_charts_cast_the_string_scores_and_end_on_the_final_score():
    flow = P.parse_aba_score_flow(_stamped(_json("create_json_6_25_1.json")))
    assert flow["minute"].to_list() == list(range(46))  # 0..40 plus five overtime minutes
    assert flow.schema["home_score"] == pl.Int64
    assert flow.tail(1).row(0, named=True)["home_score"] == 93 and flow["away_score"][-1] == 97
    assert flow["home_score"].is_sorted()  # cumulative
    diff = P.parse_aba_score_difference(_stamped(_json("create_json_6_25_1.json")))
    assert diff.equals(flow)


def test_calendar_standings_and_player_pages():
    cal = P.parse_aba_calendar(_json("captures/calendar.json"))
    assert set(cal["season"]) == {2026} and set(cal["league"]) == {"aba"} and cal["game_no"].is_unique().all()
    first = cal.row(0, named=True)
    assert first["round"] == "ROUND 1" and first["home_abbr"] and str(first["date"]) == "2025-10-06"
    assert first["time_zone"] == "CET" and first["season_label"] == "Season 2025/26"
    old = P.parse_aba_calendar(_json("captures/calendar@8_1.json"))
    assert set(old["season"]) == {2009} and old["group"].null_count() == old.height  # no Group column then
    local = P.parse_aba_calendar(_json("captures/calendar@25_2.json"))
    assert set(local["league_id"]) == {2} and set(local["league"]) == {"aba2"} and set(local["time_zone"]) == {"local"}
    st = P.parse_aba_standings(P.record_for("standings", _html("standings_25_1.html"), {"sez": 25, "lea": 1}))
    assert st.schema["team_id"] == pl.Int64 and set(st["table"]) == {
        "Top 8 Season 2025/26",
        "Play-out Season 2025/26",
        "Regular Season 2025/26",
    }
    assert (st["games"] == st["wins"] + st["losses"]).all()
    assert (st["points_for"] - st["points_against"] == st["point_diff"]).all()
    assert (st["standing_points"] == st["wins"] * 2 + st["losses"]).all()  # 2 per win, 1 per loss
    reg = st.filter(pl.col("table").str.starts_with("Regular"))
    assert set(reg["group"]) == {"Group A", "Group B"} and reg["points_for"].sum() == reg["points_against"].sum()


def test_player_bio_and_game_log():
    rec = P.record_for("player", _html("player_4807_25_1.html"), {"player_id": 4807, "sez": 25, "lea": 1})
    bio = P.parse_aba_player(rec).row(0, named=True)
    assert (bio["player_id"], bio["team_id"], bio["season"], bio["height_cm"]) == (4807, 22, 2026, 185)
    assert str(bio["birth_date"]) == "1997-12-23" and bio["nationality"] == "SSD"
    assert bio["career"].endswith("2024-27 - Partizan Mozzart Bet") and bio["career"].count("; ") == 9
    log = P.parse_aba_player_games(rec)
    assert log.height == 2 and log["player_id"].unique().to_list() == [4807]  # the Average row is dropped
    assert log.schema["round_no"] == pl.Int64 and log["pts_paint"].null_count() == log.height
    assert log["game_no"].to_list() == [1, 9] and log["round_no"].to_list() == [1, 2]


def test_html_records_equal_the_refs_captures():
    cal = P.record_for("calendar", _html("calendar_25_1.html"), {"sez": 25, "lea": 1})
    assert cal["season"] == _json("captures/calendar.json")["season"] and len(cal["games"]) == 5
    pl_ = P.record_for("player", _html("player_4807_25_1.html"), {"player_id": 4807, "sez": 25, "lea": 1})
    cap = _json("captures/player__playerId.json")
    cut = ("games", "seasons", "career")  # the refs captures cut every list to three rows
    assert {k: pl_[k] for k in cap if k not in cut} == {k: cap[k] for k in cap if k not in cut}
    assert pl_["career"][:3] == cap["career"]


# --- the parser contract: empty, malformed, pandas, columns ---------------------------------------


@pytest.mark.parametrize("short", sorted(P.SPECS))
@pytest.mark.parametrize("bad", [{}, [], None, "x", 7, {"game": None}, {"rows": "x"}, {"games": []}, {"rows": [1, 2]}])
def test_empty_or_malformed_payload_is_a_typed_zero_row_frame(short, bad):
    df = ALL_PARSERS[short](bad)
    assert isinstance(df, pl.DataFrame) and df.height == 0
    assert dict(df.schema) == dict(P.SPECS[short])


@pytest.mark.parametrize("short", sorted(gen_aba.ENDPOINTS))
def test_two_empty_frames_stack_and_stack_with_a_populated_one(short):
    empty = ALL_PARSERS[short]({})
    full = gen_aba._frame(gen_aba.ENDPOINTS[short])
    assert pl.concat([empty, empty]).height == 0
    assert pl.concat([empty, full]).height == full.height
    assert pl.concat([full, empty]).schema == full.schema


@pytest.mark.parametrize("short", sorted(gen_aba.ENDPOINTS))
def test_return_as_pandas(short):
    entry = gen_aba.ENDPOINTS[short]
    pdf = entry[2](entry[3](), return_as_pandas=True)
    assert isinstance(pdf, pd.DataFrame) and len(pdf) > 0
    assert list(pdf.columns) == list(P.SPECS[short])


def test_columns_argument_adds_missing_documented_columns_as_nulls():
    df = P.parse_aba_pbp({}, columns=["game_no", "brand_new_column"])
    assert df.height == 0 and df.schema["brand_new_column"] == pl.Utf8


def test_every_id_column_is_int64_and_no_float_origin_strings():
    for short in P.SPECS:
        df = gen_aba._frame(gen_aba.ENDPOINTS[short])
        for c, dt in df.schema.items():
            if c.endswith("_id"):
                assert dt == pl.Int64, (short, c)
            if dt == pl.Utf8:
                assert not df[c].drop_nulls().str.contains(r"^\d+\.0$").any(), (short, c)


def test_every_public_parser_has_a_documented_example_and_helpers_are_private():
    assert set(P.__all__) == {f"parse_aba_{n}" for n in P.SPECS}
    for name in P.__all__:
        doc = getattr(P, name).__doc__ or ""
        for part in ("Args:", "Returns:", "Raises:", "Example:", "Quick start::"):
            assert part in doc, (name, part)
        assert ">>>" not in doc
    assert set(P.__all__) <= set(aba_pkg.__all__)
    assert all(n.startswith("aba_") or n.startswith("parse_aba_") for n in aba_pkg.__all__)


# --- the YAML, schemas and generator --------------------------------------------------------------


def _yaml() -> Dict[str, Any]:
    return yaml.safe_load(YAML_PATH.read_text(encoding="utf-8"))


def test_yaml_lists_every_spec_route():
    spec_paths = set(json.loads((FIXTURES / "spec_paths.json").read_text(encoding="utf-8")))
    assert len(spec_paths) == 7 and spec_paths == {e[0] for e in gen_aba.ENDPOINTS.values()}
    doc = _yaml()
    got = {(e["short"], e["path"]) for e in doc["endpoints"]}
    assert got == {(s, e[1]) for s, e in gen_aba.ENDPOINTS.items()}
    assert len(doc["endpoints"]) == 12 and set(P.SPECS) == {e["short"] for e in doc["endpoints"]}
    assert [e["name"] for e in doc["endpoints"][0]["extra_params"]] == ["league"]


def test_no_path_token_named_league_or_sport():
    for e in _yaml()["endpoints"]:
        assert not {"league", "sport"} & set(re.findall(r"\{([^}]+)\}", e["path"])), e["path"]


def test_yaml_carries_the_access_and_coverage_notes():
    notes = " ".join(_yaml()["docstring"]["notes"])
    assert "robots.txt" in notes and "Crawl-delay" in notes and "at least 1 s" in notes
    assert "2017-18" in notes and "game_no" in notes and "AssetFetchError" in notes


@pytest.mark.parametrize("short", sorted(gen_aba.ENDPOINTS))
def test_schema_matches_the_parser_and_carries_no_descriptions(short):
    schema = yaml.safe_load((SCHEMA_DIR / f"{short}.yaml").read_text(encoding="utf-8"))
    assert [c["name"] for c in schema["columns"]] == list(P.SPECS[short])
    assert {c["description"] for c in schema["columns"]} == {""}  # descriptions live in manual_column_descriptions.yaml


def test_the_committed_descriptions_cover_every_column_without_filler():
    notes = yaml.safe_load(gen_aba.NOTES.read_text(encoding="utf-8"))
    assert set(notes) == {f"aba_{s}" for s in gen_aba.ENDPOINTS}
    for key, cols in notes.items():
        assert set(cols) == set(P.SPECS[key[4:]])
        for col, text in cols.items():
            assert len(text) >= 15 and text.lower() != col, (key, col)


def test_unverified_caveats_stay_in_the_descriptions():
    assert "unverified" in gen_aba.LOCAL_BY["shots"]["y"]


# --- season mapping and argument checks ---------------------------------------------------------


@pytest.mark.parametrize("season,sez", [(2026, 25), (2009, 8), (2018, 17), (2002, 1), ("2026", 25)])
def test_season_end_year_maps_to_the_sites_sez(season, sez):
    assert R.sez_for_season(season) == sez


def test_season_before_the_first_season_is_nodata_and_garbage_is_valueerror():
    with pytest.raises(NoDataError):
        R.sez_for_season(2001)
    for bad in ("x", None, 2.5, True, 2101):
        with pytest.raises(ValueError):
            R.sez_for_season(bad)  # type: ignore[arg-type]


# --- runtime: a scripted transport -------------------------------------------------------------------

HTML = {"content-type": "text/html; charset=UTF-8"}


class Site(HTTPAdapter):
    """A scripted ABA site: ``handler(request)`` returns ``(status, headers, body)``; every request is recorded."""

    def __init__(self, handler: Callable[[requests.PreparedRequest], Tuple[int, Dict[str, str], Any]]) -> None:
        super().__init__()
        self.handler = handler
        self.calls: List[requests.PreparedRequest] = []

    def send(self, request: requests.PreparedRequest, **kwargs: Any) -> requests.Response:  # type: ignore[override]
        self.calls.append(request)
        status, headers, body = self.handler(request)
        resp = requests.Response()
        resp.status_code = status
        resp.headers.update(headers)
        resp._content = body if isinstance(body, bytes) else str(body).encode("utf-8")
        resp.encoding = "utf-8"
        resp.url = headers.get("x-final-url") or request.url
        resp.request = request
        return resp

    def urls(self) -> List[str]:
        return [c.url for c in self.calls]


@pytest.fixture
def waits(monkeypatch):
    """Count one polite wait per attempt; sleeping is free."""
    seen: List[str] = []
    monkeypatch.setattr(basketball_runtime, "_polite_wait", lambda url, *a, **k: seen.append(url))
    slept: List[float] = []
    monkeypatch.setattr(dl_utils.time, "sleep", lambda s: slept.append(s))
    return type("W", (), {"urls": seen, "slept": slept})


@pytest.fixture
def site(monkeypatch, waits):
    """Mount a Site on a fresh polite session."""

    def install(handler):
        adapter = Site(handler)
        session = basketball_runtime.PoliteSession()
        session.mount("https://", adapter)
        monkeypatch.setattr(R, "SESSION", session)
        return adapter

    return install


def _pages(
    overrides: Optional[Dict[str, Any]] = None,
) -> Callable[[requests.PreparedRequest], Tuple[int, Dict[str, str], Any]]:
    """A site that serves the committed pages and helper bodies, keyed by path; ``overrides`` replaces a path."""
    table: Dict[str, Any] = {
        "/match/6/25/1/Overview": _html("match_6_25_1.html"),
        "/match/1/8/1/Overview": _html("match_1_8_1.html"),
        "/calendar/25/1/": _html("calendar_25_1.html"),
        "/standings/25/1/": _html("standings_25_1.html"),
        "/player/4807/25/1/": _html("player_4807_25_1.html"),
        f"{R.LIVE}/create_shooting_chart.php": (FIXTURES / "create_shooting_chart_6_25_1.json").read_text(
            encoding="utf-8"
        ),
        f"{R.LIVE}/create_json.php": (FIXTURES / "create_json_6_25_1.json").read_text(encoding="utf-8"),
        f"{R.LIVE}/create_scoredifference.php": (FIXTURES / "create_json_6_25_1.json").read_text(encoding="utf-8"),
    }
    table.update(overrides or {})

    def handler(req):
        path = req.url.split("aba-liga.com", 1)[1].split("?")[0]
        body = table.get(path)
        if body is None:
            return 404, HTML, "not found"
        return body if isinstance(body, tuple) else (200, HTML, body)

    return handler


def _mutate_match(fn: Callable[[str], str]) -> str:
    out = fn(_html("match_6_25_1.html"))
    assert out != _html("match_6_25_1.html")
    return out


def test_wrappers_build_the_sites_url_and_every_request_waits(site, waits):
    adapter = site(_pages())
    game = aba_pkg.aba_game(game_no=6, season=2026)
    assert game.height == 1 and game["season"].to_list() == [2026]
    assert aba_pkg.aba_pbp(game_no=6, season=2026).height == 95
    assert adapter.urls() == [f"{WWW}/match/6/25/1/Overview"] * 2  # sez = season - 2001, lea = 1
    assert len(waits.urls) == len(adapter.calls)  # one throttled wait per request
    assert aba_pkg.aba_calendar(season=2026).height == 5
    assert adapter.urls()[-1] == f"{WWW}/calendar/25/1/"


def test_the_league_argument_picks_the_host_and_lea(site):
    adapter = site(lambda req: (_ for _ in ()).throw(AssertionError("no fetch expected")))
    with pytest.raises(ValueError, match="league"):
        aba_pkg.aba_game(game_no=1, season=2026, league="nba")
    assert adapter.calls == []

    def druga(req):
        assert req.url == f"{DRUGA}/match/1/25/2/Overview"
        return 200, HTML, _html("match_6_25_1.html")

    site(druga)
    with pytest.raises(AssetFetchError, match="out of scope"):  # the echo says league 1 / game 6, not 2 / game 1
        aba_pkg.aba_game(game_no=1, season=2026, league="aba2")


def test_return_parsed_false_hands_back_the_stamped_record(site):
    site(_pages())
    raw = aba_pkg.aba_box_teams(game_no=6, season=2026, return_parsed=False)
    assert raw[R.REQUEST_KEY] == {
        "league": "aba",
        "league_id": 1,
        "kind": "match",
        "season": 2026,
        "sez": 25,
        "game_no": 6,
    }
    assert set(raw) >= {"game", "periods", "players", "teams", "pbp"}
    assert aba_pkg.parse_aba_periods(raw).height == 5


def test_player_page_may_redirect_to_its_slug_url(site):
    site(
        _pages(
            {
                "/player/4807/25/1/": (
                    200,
                    {**HTML, "x-final-url": f"{WWW}/player/4807/25/1/carlik-jones/"},
                    _html("player_4807_25_1.html"),
                )
            }
        )
    )
    assert aba_pkg.aba_player(player_id=4807, season=2026).row(0, named=True)["name"] == "Carlik Jones"
    assert aba_pkg.aba_player_games(player_id=4807, season=2026).height == 2


def test_unknown_game_redirect_is_nodata(site):
    site(
        _pages(
            {
                "/match/9999/25/1/Overview": (
                    200,
                    {**HTML, "x-final-url": f"{WWW}/match-not-defined/"},
                    "<html>Error</html>",
                )
            }
        )
    )
    with pytest.raises(NoDataError, match="no such game"):
        aba_pkg.aba_game(game_no=9999, season=2026)


def test_status_codes_follow_the_error_vocabulary(site, waits):
    site(_pages({"/match/6/25/1/Overview": (404, HTML, "x")}))
    with pytest.raises(NoDataError):
        aba_pkg.aba_game(game_no=6, season=2026)
    site(_pages({"/match/6/25/1/Overview": (422, HTML, "x")}))
    with pytest.raises(ValueError, match="422"):
        aba_pkg.aba_game(game_no=6, season=2026)
    site(_pages({"/match/6/25/1/Overview": (403, HTML, "x")}))
    with pytest.raises(AssetFetchError, match="403"):
        aba_pkg.aba_game(game_no=6, season=2026)


def test_every_retry_attempt_waits_its_turn_and_5xx_exhausts_to_assetfetcherror(site, waits):
    adapter = site(_pages({"/match/6/25/1/Overview": (503, {**HTML, "Retry-After": "2"}, "busy")}))
    with pytest.raises(AssetFetchError, match="503"):
        aba_pkg.aba_game(game_no=6, season=2026)
    assert len(adapter.calls) == R.ATTEMPTS == len(waits.urls)  # each attempt went through the polite session
    assert waits.slept and max(waits.slept) >= 2  # Retry-After honoured


def test_a_transient_429_then_200_succeeds(site, waits):
    state = {"n": 0}
    inner = _pages()

    def handler(req):
        state["n"] += 1
        return (429, {**HTML, "Retry-After": "1"}, "slow down") if state["n"] == 1 else inner(req)

    adapter = site(handler)
    assert aba_pkg.aba_game(game_no=6, season=2026).height == 1
    assert len(adapter.calls) == 2 == len(waits.urls)


def test_a_connection_error_is_assetfetcherror_without_the_url_query(site, monkeypatch):
    def boom(req):
        raise requests.ConnectionError("secret-host-detail")

    site(boom)
    with pytest.raises(AssetFetchError) as exc:
        aba_pkg.aba_game(game_no=6, season=2026)
    assert "ConnectionError" in str(exc.value) and "secret-host-detail" not in str(exc.value)


def test_redirect_off_the_aba_sites_or_to_another_path_is_refused(site):
    site(
        _pages(
            {
                "/match/6/25/1/Overview": (
                    200,
                    {**HTML, "x-final-url": "https://example.com/match/6/25/1/Overview"},
                    _html("match_6_25_1.html"),
                )
            }
        )
    )
    with pytest.raises(AssetFetchError, match="redirected off"):
        aba_pkg.aba_game(game_no=6, season=2026)
    site(_pages({"/match/6/25/1/Overview": (200, {**HTML, "x-final-url": f"{WWW}/news/"}, _html("match_6_25_1.html"))}))
    with pytest.raises(AssetFetchError, match="not the page asked for"):
        aba_pkg.aba_game(game_no=6, season=2026)


def test_a_match_page_for_another_game_season_or_league_is_refused(site):
    site(_pages({"/match/6/25/1/Overview": _html("match_1_8_1.html")}))  # the site served another game
    with pytest.raises(AssetFetchError, match="out of scope"):
        aba_pkg.aba_game(game_no=6, season=2026)
    wrong_lea = _mutate_match(lambda h: h.replace('value="1" id="match_data_lea"', 'value="2" id="match_data_lea"'))
    site(_pages({"/match/6/25/1/Overview": wrong_lea}))
    with pytest.raises(AssetFetchError, match="match_data_lea"):
        aba_pkg.aba_game(game_no=6, season=2026)
    wrong_sez = _mutate_match(lambda h: h.replace('value="25" id="match_data_sez"', 'value="24" id="match_data_sez"'))
    site(_pages({"/match/6/25/1/Overview": wrong_sez}))
    with pytest.raises(AssetFetchError, match="match_data_sez"):
        aba_pkg.aba_game(game_no=6, season=2026)


def test_a_player_link_outside_the_requested_season_is_refused(site):
    foreign = _mutate_match(lambda h: h.replace("/player/", "/player/", 1).replace("/25/1/", "/24/1/", 3))
    site(_pages({"/match/6/25/1/Overview": foreign}))
    with pytest.raises(AssetFetchError):
        aba_pkg.aba_box_players(game_no=6, season=2026)


def test_an_unfinished_game_is_assetfetcherror(site):
    unplayed = _mutate_match(
        lambda h: re.sub(r'(<td class="gameScore">).*?(</td>)', r"\1 : \2", h, count=1, flags=re.S)
    )
    site(_pages({"/match/6/25/1/Overview": unplayed}))
    with pytest.raises(AssetFetchError, match="not finished"):
        aba_pkg.aba_game(game_no=6, season=2026)


def test_period_scores_that_do_not_add_up_are_refused(site):
    broken = _mutate_match(lambda h: h.replace("<td>31:21</td>", "<td>30:21</td>"))
    site(_pages({"/match/6/25/1/Overview": broken}))
    with pytest.raises(AssetFetchError, match="not final"):
        aba_pkg.aba_periods(game_no=6, season=2026)


def test_a_pbp_without_game_finished_is_refused(site):
    live = _mutate_match(lambda h: h.replace("game finished", "game running"))
    site(_pages({"/match/6/25/1/Overview": live}))
    with pytest.raises(AssetFetchError, match="game finished"):
        aba_pkg.aba_pbp(game_no=6, season=2026)


def test_layout_drift_and_garbage_bodies_are_assetfetcherror(site):
    for body in ("", "   ", "<html><body>maintenance</body></html>", "not html at all"):
        site(_pages({"/match/6/25/1/Overview": body}))
        with pytest.raises(AssetFetchError):
            aba_pkg.aba_game(game_no=6, season=2026)
    drift = _mutate_match(lambda h: h.replace(">Rebs<", ">Rebz<"))
    site(_pages({"/match/6/25/1/Overview": drift}))
    with pytest.raises(AssetFetchError, match="header drift"):
        aba_pkg.aba_box_players(game_no=6, season=2026)


def test_calendar_and_standings_scope_and_unknown_season(site):
    site(_pages({"/calendar/8/1/": _html("calendar_25_1.html")}))  # served the 2025-26 page for 2008-09
    with pytest.raises(AssetFetchError, match="outside the season"):
        aba_pkg.aba_calendar(season=2009)
    site(_pages({"/standings/8/1/": _html("standings_25_1.html")}))
    with pytest.raises(AssetFetchError, match="outside"):
        aba_pkg.aba_standings(season=2009)
    empty = '<html><select id="season"><option value="25/1/">Season 2025/26</option></select><section id="main_content"></section></html>'
    site(_pages({"/calendar/99/1/": empty, "/standings/99/1/": empty}))
    with pytest.raises(NoDataError, match="no season"):
        aba_pkg.aba_calendar(season=2100)
    with pytest.raises(NoDataError, match="no season"):
        aba_pkg.aba_standings(season=2100)
    with pytest.raises(NoDataError, match="first season"):
        aba_pkg.aba_calendar(season=2001)


def test_a_standings_table_that_lost_a_row_is_refused(site):
    html = _html("standings_25_1.html")
    start = html.index("Regular Season")  # a closed round robin: Top 8 and Play-out tables are exempt
    row = re.search(r"<tr class=\"standings[^>]*>(?:(?!</tr>).)*</tr>", html[html.index("<tbody>", start) :], re.S)
    assert row
    site(_pages({"/standings/25/1/": html[:start] + html[start:].replace(row.group(0), "", 1)}))
    with pytest.raises(AssetFetchError, match="balance|games != wins"):
        aba_pkg.aba_standings(season=2026)


def test_a_player_page_for_another_player_or_season_is_refused(site):
    other = _html("player_4807_25_1.html").replace("4807/25/1/carlik-jones/", "4808/25/1/carlik-jones/")
    site(_pages({"/player/4807/25/1/": other}))
    with pytest.raises(AssetFetchError, match="out of scope"):
        aba_pkg.aba_player(player_id=4807, season=2026)


def test_helpers_fetch_the_match_page_first_then_the_helper(site, waits):
    adapter = site(_pages())
    shots = aba_pkg.aba_shots(game_no=6, season=2026)
    assert shots.height == 152
    assert adapter.urls()[0] == f"{WWW}/match/6/25/1/Overview"
    assert adapter.urls()[1].startswith(f"{WWW}{R.LIVE}/create_shooting_chart.php?") and "id=6" in adapter.urls()[1]
    assert "sez=25" in adapter.urls()[1] and "lea=1" in adapter.urls()[1]
    assert len(waits.urls) == len(adapter.calls) == 2
    flow = aba_pkg.aba_score_flow(game_no=6, season=2026)
    diff = aba_pkg.aba_score_difference(game_no=6, season=2026)
    assert flow.height == diff.height == 46
    raw = aba_pkg.aba_score_flow(game_no=6, season=2026, return_parsed=False)
    assert set(raw) == {"rows", R.REQUEST_KEY} and raw[R.REQUEST_KEY]["kind"] == "flow"


def test_a_helper_with_no_data_is_nodata(site):
    for body in ("", "\n\n  \n", "[]\n\n"):
        site(_pages({f"{R.LIVE}/create_shooting_chart.php": body}))
        with pytest.raises(NoDataError, match="2017-18"):
            aba_pkg.aba_shots(game_no=6, season=2026)


def test_helper_bodies_that_belong_to_another_game_are_refused(site):
    # the unknown-id placeholder: 40 zero rows (minutes 0..39)
    zeros = json.dumps([{"minuta": i, "domaci_rez": 0, "tuji_rez": 0} for i in range(40)])
    site(_pages({f"{R.LIVE}/create_scoredifference.php": zeros}))
    with pytest.raises(AssetFetchError, match="out of scope"):
        aba_pkg.aba_score_difference(game_no=6, season=2026)
    wrong_final = json.loads((FIXTURES / "create_json_6_25_1.json").read_text(encoding="utf-8"))
    wrong_final[-1]["domaci_rez"] = "99"
    site(_pages({f"{R.LIVE}/create_json.php": json.dumps(wrong_final)}))
    with pytest.raises(AssetFetchError, match="final"):
        aba_pkg.aba_score_flow(game_no=6, season=2026)
    shots = json.loads((FIXTURES / "create_shooting_chart_6_25_1.json").read_text(encoding="utf-8"))
    for s in shots:  # a rematch's chart: shooters that are not in this game's box score
        if s["player_id"] != "0":
            s["player_id"] = "999999"
    site(_pages({f"{R.LIVE}/create_shooting_chart.php": json.dumps(shots)}))
    with pytest.raises(AssetFetchError, match="not in the"):
        aba_pkg.aba_shots(game_no=6, season=2026)
    thin = shots[:100]
    thin_real = json.loads((FIXTURES / "create_shooting_chart_6_25_1.json").read_text(encoding="utf-8"))[:100]
    assert thin and thin_real
    site(_pages({f"{R.LIVE}/create_shooting_chart.php": json.dumps(thin_real)}))
    with pytest.raises(AssetFetchError, match="shots / "):
        aba_pkg.aba_shots(game_no=6, season=2026)


def test_a_helper_that_is_not_json_or_not_a_list_is_assetfetcherror(site):
    for body in ("<html>error</html>", json.dumps({"error": "x"}), json.dumps([1, 2])):
        site(_pages({f"{R.LIVE}/create_json.php": body}))
        with pytest.raises(AssetFetchError):
            aba_pkg.aba_score_flow(game_no=6, season=2026)


def test_the_shot_chart_is_the_only_unscoped_route_and_is_checked_against_the_box_score():
    # README "Routes that cannot be fully scoped": the chart echoes no game id; the runtime tallies it per shooter
    match = P.record_for("match", _html("match_6_25_1.html"), M6)
    ok = _json("create_shooting_chart_6_25_1.json")
    assert P.helper_scope_error("shots", ok, match) is None
    assert P.helper_scope_error("flow", _json("create_json_6_25_1.json"), match) is None
    assert P.helper_scope_error("shots", {"x": 1}, match) is not None


def test_unsupported_keyword_arguments_and_bad_route_are_refused(site):
    site(_pages())
    with pytest.raises(TypeError, match="unsupported"):
        aba_pkg.aba_game(game_no=6, season=2026, timeout=5)
    with pytest.raises(ValueError, match="not an ABA route"):
        R._get(f"{WWW}/news/")
    with pytest.raises(ValueError):
        aba_pkg.aba_game(game_no="six", season=2026)


def test_a_selected_season_option_that_is_not_the_request_is_refused(site):
    # the page's links are all 25/1 but its selected season says otherwise
    for kind, path, fn, args in (
        ("calendar", "/calendar/25/1/", aba_pkg.aba_calendar, {"season": 2026}),
        ("standings", "/standings/25/1/", aba_pkg.aba_standings, {"season": 2026}),
    ):
        html = _html(f"{kind}_25_1.html").replace('value="25/1/" selected', 'value="24/1/" selected')
        site(_pages({path: html}))
        with pytest.raises(AssetFetchError, match="out of scope: season"):
            fn(**args)
