"""Offline tests for the LKL (``lkl``) flat-API family: parsers, HTML readers, runtime and YAML.

Parsers are asserted against real data: the FULL bodies of one overtime game (11446) in ``tests/fixtures/lkl/full/``
(dtypes come from those), the refs capture samples of five games and the season records in
``tests/fixtures/lkl/captures/``, and the refs' cut-down HTML pages in ``tests/fixtures/lkl/html/`` (the HTML readers'
input). The runtime is driven through a scripted ``requests`` adapter mounted on a ``PoliteSession``, so every refusal
is exercised end to end and every attempt is counted. No network, and nothing reads the private sdv-internal-refs
checkout.
"""

from __future__ import annotations

import html as htmllib
import json
import re
import sys
from pathlib import Path
from typing import Any, Callable, Dict, List, Tuple
from urllib.parse import parse_qs, urlsplit

import pandas as pd
import polars as pl
import pytest
import requests
import yaml
from requests.adapters import HTTPAdapter

import sportsdataverse.basketball.lkl as lkl_pkg
from sportsdataverse import dl_utils
from sportsdataverse.basketball import basketball_runtime
from sportsdataverse.basketball.lkl import lkl_parsers as P
from sportsdataverse.basketball.lkl import lkl_runtime as R
from sportsdataverse.errors import AssetFetchError, NoDataError

ROOT = Path(__file__).parents[3]
FIXTURES = ROOT / "tests" / "fixtures" / "lkl"
YAML_PATH = ROOT / "tools" / "codegen" / "endpoints" / "lkl.yaml"
SCHEMA_DIR = ROOT / "tools" / "codegen" / "schemas" / "native" / "lkl"

sys.path.insert(0, str(ROOT / "tools" / "codegen"))
import gen_lkl  # noqa: E402

SNAKE = re.compile(r"^[a-z0-9]+(_[a-z0-9]+)*$")
GAME = 11446
ALL_PARSERS = {n: getattr(P, f"parse_lkl_{n}") for n in P.SPECS}


def _full(name: str) -> Dict[str, Any]:
    return json.loads((FIXTURES / "full" / f"{name}.json").read_text(encoding="utf-8"))


def _cap(name: str) -> Dict[str, Any]:
    return json.loads((FIXTURES / "captures" / f"{name}.json").read_text(encoding="utf-8"))


def _html(name: str) -> str:
    return (FIXTURES / "html" / name).read_text(encoding="utf-8")


def _stamped(name: str) -> Dict[str, Any]:
    body = _full(name)
    body[R.REQUEST_KEY] = R._request(_full("rungtynes"))
    return body


# --- parsers over every committed body ----------------------------------------------------------


@pytest.mark.parametrize("short", sorted(gen_lkl.ENDPOINTS))
def test_every_wrapper_parses_its_fixture(short):
    df = gen_lkl._frame(gen_lkl.ENDPOINTS[short])
    assert isinstance(df, pl.DataFrame) and df.height > 0, short
    assert all(SNAKE.match(c) for c in df.columns), [c for c in df.columns if not SNAKE.match(c)]
    assert dict(df.schema) == dict(P.SPECS[short]), short
    ids = [c for c in df.columns if c.endswith("_id") or c in ("season_id", "rank", "position")]
    assert {c: df.schema[c] for c in ids} == {c: pl.Int64 for c in ids}


def test_numeric_dtypes_come_from_the_full_bodies():
    box = P.parse_lkl_player_box(_stamped("boxscore"))
    assert box.schema["fg_pct"] == pl.Float64 and box.schema["ft_pct"] == pl.Float64  # "44.4": a string on the site
    assert box.schema["seconds_played"] == pl.Int64 and box.schema["jersey"] == pl.Utf8
    assert box.schema["is_starter"] == pl.Boolean and box.schema["plus_minus"] == pl.Int64
    team = P.parse_lkl_team_box(_stamped("boxscore"))
    assert team.schema["field_goals_percentage"] == pl.Float64 and team.schema["time_leading"] == pl.Int64
    shots = P.parse_lkl_shots(_stamped("shot-chart"))
    assert shots.schema["x"] == pl.Float64 and shots.schema["y"] == pl.Float64  # "93.00": a string on the site
    stats = P.parse_lkl_game_stats(_stamped("game-stats"))
    assert stats.schema["home_percentage"] == pl.Float64 and stats.schema["home_value"] == pl.Utf8
    leads = P.parse_lkl_lead_chart(_stamped("game-stats"))
    assert leads.schema["score_home"] == pl.Int64  # "23": a string on the site


def test_box_score_adds_up_to_the_team_totals_and_the_periods():
    box, team = P.parse_lkl_player_box(_stamped("boxscore")), P.parse_lkl_team_box(_stamped("boxscore"))
    per = P.parse_lkl_period_scores(_stamped("boxscore"))
    assert team["team_side"].to_list() == ["home", "away"] and team["team_id"].to_list() == [11, 41]
    for side, tid, pts in (("home", 11, 88), ("away", 41, 85)):
        mine = box.filter(pl.col("team_side") == side)
        assert mine["team_id"].unique().to_list() == [tid]
        assert mine["points"].sum() == pts == team.filter(pl.col("team_id") == tid)["points"][0]
        assert per.filter(pl.col("team_id") == tid)["points"].sum() == pts
        assert mine["fg_made"].sum() == team.filter(pl.col("team_id") == tid)["field_goals_made_total"][0]
    assert per["period"].max() == 5 and per.filter(pl.col("period") == 5)["period_type"].unique().to_list() == [
        "overtime"
    ]
    assert box["player_id"].null_count() == 0  # every slug resolved through the stamped roster
    row = box.filter(pl.col("player_slug") == "justinas-marcinkevicius").row(0, named=True)
    assert (row["fg_made"], row["fg_attempted"], row["fg_pct"], row["seconds_played"]) == (4, 9, 44.4, 1974)
    assert row["player_id"] == 1762 and row["player"] == "J. Marcinkevičius"


def test_game_info_and_players_come_from_the_game_page():
    info = P.parse_lkl_game_info(_full("rungtynes"))
    assert info.height == 1
    r = info.row(0, named=True)
    assert (r["home_team_id"], r["away_team_id"], r["home_winner"], r["away_winner"]) == (11, 41, True, False)
    assert str(r["start_date"]) == "2026-02-14" and r["start_time"] == "16:50" and r["attendance_count"] == 1097
    assert r["game_status"] == "Pasibaigė" and r["has_actions"] is True
    players = P.parse_lkl_game_players(_full("rungtynes"))
    assert players.height == len(_full("rungtynes")["players"]["home"]) + len(_full("rungtynes")["players"]["away"])
    assert players["player_id"].is_unique().all() and players["slug"].is_unique().all()
    assert players.filter(pl.col("team_side") == "home")["team_id"].unique().to_list() == [11]


def test_pbp_is_oldest_first_and_ends_on_the_final_score():
    pbp = P.parse_lkl_pbp(_stamped("play-by-play"))
    assert pbp["event_num"].to_list() == list(range(1, pbp.height + 1))
    assert pbp["period"].to_list() == sorted(pbp["period"].to_list())
    assert pbp["period"].unique().sort().to_list() == [1, 2, 3, 4, 5]
    last = pbp.row(pbp.height - 1, named=True)
    assert (last["type"], last["subtype"], last["period"]) == ("game", "end", 5)  # the game-end line keeps period 5
    assert (last["score_home"], last["score_away"]) == (88, 85)
    first = pbp.row(0, named=True)
    assert (first["type"], first["subtype"], first["clock"], first["period"]) == ("game", "start", "10:00", 1)
    assert pbp.filter(pl.col("period_type") == "overtime")["period"].unique().to_list() == [5]
    named = pbp.filter(pl.col("player_slug").is_not_null())
    assert named["player_id"].null_count() == 0 and named.height > 400
    assert set(pbp["team_side"].drop_nulls().unique()) == {"home", "away"}


def test_overtime_periods_are_derived_from_the_period_end_lines():
    def line(t, st, clock, home=True):
        return {
            "is_home": home,
            "player": {"name": None, "slug": None},
            "clock": clock,
            "type": t,
            "subtype": st,
            "description": "x",
            "success": True,
            "is_points_info": False,
            "score": {"home": 1, "away": 1},
        }

    # newest first, as the site lists them: OT2 (end, game end, a play, its start) then OT1 (end, a play, start)
    ot = [
        line("game", "end", "00:00"), line("period", "end", "00:00"), line("2pt", "layup", "01:00"),
        line("period", "start", "05:00"), line("substitution", "in", "05:00"),  # OT2's start-of-period subs
        line("period", "end", "00:00"), line("2pt", "layup", "02:00"), line("period", "start", "05:00"),
    ]  # fmt: skip
    got = P._chronological({"regular": {}, "overtime": ot})
    assert [p for p, _t, _l in got] == [5, 5, 5, 6, 6, 6, 6, 6]
    assert [(ln["type"], ln["subtype"]) for _p, _t, ln in got][-1] == ("game", "end")


def test_shots_match_the_box_attempts_and_the_roster():
    shots, box = P.parse_lkl_shots(_stamped("shot-chart")), P.parse_lkl_team_box(_stamped("boxscore"))
    roster = P.parse_lkl_game_players(_full("rungtynes"))
    for side in ("home", "away"):
        fga = box.filter(pl.col("team_side") == side)["field_goals_attempted_total"][0]
        assert shots.filter(pl.col("team_side") == side).height == fga
    assert shots.schema["player_id"] == roster.schema["player_id"] == pl.Int64  # one dtype for the join key
    joined = shots.join(roster.select("player_id", "slug"), on="player_id", how="left")
    assert joined["slug"].null_count() == 0 and (joined["slug"] == joined["player_slug"]).all()
    assert shots["period"].max() == 5 and shots.filter(pl.col("period") == 5)["period_type"].unique().to_list() == [
        "overtime"
    ]
    assert (
        shots["x"].min() >= 0 and shots["x"].max() <= 100 and shots["type"].unique().sort().to_list() == ["2pt", "3pt"]
    )


def test_leaders_summary_and_game_stats():
    leaders = P.parse_lkl_leaders(_stamped("leaders"))
    assert leaders["category"].unique().sort().to_list() == [
        "assists",
        "blocks",
        "efficiency",
        "points",
        "steals",
        "total_rebounds",
    ]
    top = leaders.filter((pl.col("category") == "points") & (pl.col("rank") == 1)).sort("team_side")
    assert top["value"].to_list() == [20, 22] and top["team_side"].to_list() == ["away", "home"]
    assert leaders["player_id"].null_count() == 0 and leaders["rank"].min() == 1
    summary, stats = P.parse_lkl_summary(_stamped("summary")), P.parse_lkl_game_stats(_stamped("game-stats"))
    fg = summary.filter(pl.col("stat") == "field_goals").row(0, named=True)
    assert (fg["home_value"], fg["home_percentage"], fg["home_winning"]) == ("35/75", 46.7, True)
    assert summary["group"].null_count() == summary.height
    assert set(stats["group"].drop_nulls()) == {"shooting", "main"}
    assert stats.filter(pl.col("stat") == "rebounds")["home_value"][0] == "51"
    assert summary["stat"].null_count() == 0 and stats["stat"].null_count() == 0  # every label is in STAT_CODES
    score = P.parse_lkl_game_score(_stamped("game-score")).row(0, named=True)
    assert (score["home_score"], score["away_score"], score["game_id"]) == (88, 85, GAME)
    leads = P.parse_lkl_lead_chart(_stamped("game-stats"))
    assert leads["period"].unique().sort().to_list() == [1, 2, 3, 4, 5]
    assert (leads["diff"] == leads["score_home"] - leads["score_away"]).all()


def test_a_bare_body_without_the_stamp_keeps_the_schema_and_nulls_what_it_cannot_know():
    box = P.parse_lkl_player_box(_full("boxscore"))
    assert box.height == 22 and box["player_id"].null_count() == 22 and box["team_id"].null_count() == 22
    assert dict(box.schema) == dict(P.SPECS["player_box"])
    team = P.parse_lkl_team_box(_full("boxscore"))
    assert team["team_side"].null_count() == 2 and sorted(team["team_id"].to_list()) == [11, 41]


def test_results_parse_the_html_pages_into_one_record():
    idx = _html("rezultatai_selects.html")
    pages = [_html(f"results_49436_p{n}.html") for n in (1, 2, 3)]
    rec = P._results_record(49436, idx, pages)
    cap = _cap("loadResults")
    assert (rec["season_label"], rec["season_end_year"], rec["pages"], rec["game_count"]) == (
        "2026-2027 m. sezonas",
        2027,
        3,
        22,
    )
    assert rec["games"][:3] == cap["games"] and cap["game_count"] == 22
    df = P.parse_lkl_results(rec)
    assert df.height == 22 and df["game_id"].is_unique().all()
    assert df.schema["date"] == pl.Date and df.schema["home_score"] == pl.Int64
    assert df["season_id"].unique().to_list() == [49436] and df["season_end_year"].unique().to_list() == [2027]
    assert set(df["winner_highlighted"].drop_nulls()) <= {"home", "away"}


def test_results_refusals():
    idx = _html("rezultatai_selects.html")
    p1, p2, p3 = (_html(f"results_49436_p{n}.html") for n in (1, 2, 3))
    with pytest.raises(P._NotParsed, match="not in the /rezultatai season select"):
        P._results_record(1, idx, [p1, p2, p3])
    with pytest.raises(P._NotParsed, match="outside 2025-2026"):  # 2026-27 dates under the 2025-26 label
        P._results_record(41936, idx, [p1, p2, p3])
    with pytest.raises(P._NotParsed, match="walked 2 pages"):
        P._results_record(49436, idx, [p1, p2])
    with pytest.raises(P._NotParsed, match="has 2 games"):  # a short middle page
        P._results_record(49436, idx, [p1, p3, p3])
    with pytest.raises(P._NotParsed, match="no paginator"):  # a full page 1 without page links: drift
        P._results_record(49436, idx, [re.sub(r"loadResults\?page=\d+", "x", p1)])
    with pytest.raises(P._NotParsed, match="duplicate games"):
        P._results_record(49436, idx, [p1, p1, p3])
    one_page = re.sub(r"loadResults\?page=\d+", "loadResults?page=1", p1)  # paginator claims one (full) page
    with pytest.raises(P._NotParsed, match="was not checked"):
        P._results_record(49436, idx, [one_page])
    with pytest.raises(P._NotParsed, match="after the paginator's last page"):
        P._results_record(49436, idx, [one_page], after=p2)
    rec = P._results_record(49436, idx, [one_page], after="<div>Informacija ruošiama</div>")
    assert rec["game_count"] == 10 and rec["pages"] == 1  # confirmed: nothing follows
    with pytest.raises(P._NotParsed) as e:
        P._results_record(49436, idx, ["<div>Informacija ruošiama</div>"])
    assert e.value.empty
    with pytest.raises(P._NotParsed, match="drift"):
        P._results_record(49436, idx, [p3.replace("rungtynes/", "rungtynes-x/")])


def test_standings_parse_the_html_tables():
    sel = _html("turnyrine_select.html")
    rec = P._standings_record(_html("standings_35.html"), 35, sel)
    cap = _cap("loadStandings__seasonId")
    assert (rec["season_label"], rec["season_end_year"]) == ("2026-2027 m. sezonas", 2027)
    # the fixture page keeps a header, 3 rows and the last row of each table: its leader is the capture's leader
    assert rec["full"][0]["team_slug"] == cap["full"][0]["team_slug"] and rec["full"][0]["position"] == 1
    for t in ("full", "home", "away"):
        pos = [r["position"] for r in rec[t]]
        assert pos == sorted(pos) and len(rec[t]) >= 3, t
        assert all(r["won"] + r["lost"] == r["played"] for r in rec[t]), t
    df = P.parse_lkl_standings(rec)
    assert set(df["table"]) == {"full", "home", "away"} and df["season_id"].unique().to_list() == [35]
    full = df.filter(pl.col("table") == "full")
    assert (
        full["win_pct"].null_count() == 0
        and full.schema["last5"] == pl.Utf8
        and set("".join(full["last5"])) <= {"W", "L"}
    )
    old = P._standings_record(_html("standings_3.html"), 3, sel)
    assert old["final"] == [] and old["full"][0]["win_pct"] is None  # the placeholder row is skipped; '-' is null
    done = P.parse_lkl_standings(P._standings_record(_html("standings_34.html"), 34, sel))
    assert "final" in set(done["table"]) and done.filter(pl.col("table") == "final")["played"].null_count() > 0


def test_standings_refusals():
    sel, t35 = _html("turnyrine_select.html"), _html("standings_35.html")
    with pytest.raises(P._NotParsed, match="not in the /turnyrine-lentele"):
        P._standings_record(t35, 99, sel)
    with pytest.raises(P._NotParsed, match="header drift"):
        P._standings_record(t35.replace("Laim.%", "Win%"), 35, sel)
    with pytest.raises(P._NotParsed, match="tabs drift"):
        P._standings_record(t35.replace('data-id="away"', 'data-id="neutral"'), 35, sel)
    with pytest.raises(P._NotParsed, match="colour drift"):
        P._standings_record(t35.replace("bg-green", "bg-purple", 1), 35, sel)
    with pytest.raises(P._NotParsed, match="no select"):
        P._standings_record(t35, 35, "<html></html>")


def test_game_page_reader():
    rec = P._game_record(_html("game_11590.html"), 11590)
    cap = _cap("rungtynes__gameId")
    for key in (
        "game_id",
        "game_status",
        "referees",
        "teams_info",
        "game_info",
        "start_date",
        "start_time",
        "attendance_count",
    ):
        assert rec[key] == cap[key], key
    assert "players_stats_columns" not in rec and "main_tabs" not in rec  # UI config is dropped
    with pytest.raises(P._NotParsed, match="out of scope"):
        P._game_record(_html("game_11590.html"), 11591)
    with pytest.raises(P._NotParsed, match="no <game-info-component>"):
        P._game_record("<html><body>preview</body></html>", 11590)
    with pytest.raises(P._NotParsed, match="not JSON"):
        P._game_record(_html("game_11590.html").replace(':teams-info="{', ':teams-info="{{'), 11590)


# --- the parser contract ------------------------------------------------------------------------


@pytest.mark.parametrize("short", sorted(P.SPECS))
@pytest.mark.parametrize(
    "bad", [{}, [], None, "x", 7, {"quarters": None}, {"boxscore": {}}, {"games": []}, {"gameStatistics": []}]
)
def test_empty_or_malformed_payload_is_a_typed_zero_row_frame(short, bad):
    df = ALL_PARSERS[short](bad)
    assert isinstance(df, pl.DataFrame) and df.height == 0
    assert dict(df.schema) == dict(P.SPECS[short])


@pytest.mark.parametrize("short", sorted(gen_lkl.ENDPOINTS))
def test_two_empty_frames_stack_and_stack_with_a_populated_one(short):
    empty = ALL_PARSERS[short]({})
    full = gen_lkl._frame(gen_lkl.ENDPOINTS[short])
    assert pl.concat([empty, empty]).height == 0
    assert pl.concat([empty, full]).height == full.height


@pytest.mark.parametrize("short", sorted(gen_lkl.ENDPOINTS))
def test_return_as_pandas(short):
    entry = gen_lkl.ENDPOINTS[short]
    df = entry[2](entry[3](), return_as_pandas=True)
    assert isinstance(df, pd.DataFrame) and list(df.columns) == list(P.SPECS[short]) and len(df) > 0


def test_columns_argument_adds_missing_documented_columns_as_nulls():
    df = P.parse_lkl_game_score({}, columns=["game_id", "brand_new_column"])
    assert df.height == 0 and df.schema["brand_new_column"] == pl.Utf8


def test_no_float_origin_id_strings():
    for short in P.SPECS:
        df = gen_lkl._frame(gen_lkl.ENDPOINTS[short])
        for c, dt in df.schema.items():
            if dt == pl.Utf8 and (c.endswith("_id") or c in ("jersey", "slug", "player_slug", "team_slug")):
                assert not df[c].drop_nulls().str.contains(r"\.0$").any(), (short, c)
            if c.endswith("_id"):
                assert dt == pl.Int64, (short, c)


# --- the YAML, schemas and generator --------------------------------------------------------------


def _yaml() -> Dict[str, Any]:
    return yaml.safe_load(YAML_PATH.read_text(encoding="utf-8"))


def test_yaml_lists_every_spec_route():
    spec_paths = set(json.loads((FIXTURES / "spec_paths.json").read_text(encoding="utf-8")))
    assert len(spec_paths) == 10  # 7 JSON + 3 HTML routes
    assert spec_paths == {e[0] for e in gen_lkl.ENDPOINTS.values()}
    doc = _yaml()
    assert {(e["short"], e["path"]) for e in doc["endpoints"]} == {(s, e[1]) for s, e in gen_lkl.ENDPOINTS.items()}
    assert len(doc["endpoints"]) == 14 and set(P.SPECS) == {e["short"] for e in doc["endpoints"]}
    html_routes = {"/rungtynes/{game_id}", "/loadResults", "/loadStandings/{season}"}
    assert {e["path"] for e in doc["endpoints"] if e.get("host")} == html_routes
    assert doc["host"] == "https://lkl.lt/api/livestream" and {e["host"] for e in doc["endpoints"] if "host" in e} == {
        "https://lkl.lt"
    }


def test_no_path_token_named_league_or_sport():
    for e in _yaml()["endpoints"]:
        assert not {"league", "sport"} & set(re.findall(r"\{([^}]+)\}", e["path"])), e["path"]


def test_yaml_carries_the_unofficial_and_coverage_notes():
    notes = " ".join(_yaml()["docstring"]["notes"])
    for needle in (
        "Unofficial",
        "robots.txt",
        "not redistributed",
        "FINISHED",
        "2024-25",
        "Genius",
        "King Mindaugas",
        "Lithuanian",
    ):
        assert needle in notes, needle


@pytest.mark.parametrize("short", sorted(gen_lkl.ENDPOINTS))
def test_schema_matches_the_parser_and_carries_no_descriptions(short):
    schema = yaml.safe_load((SCHEMA_DIR / f"{short}.yaml").read_text(encoding="utf-8"))
    assert [c["name"] for c in schema["columns"]] == list(P.SPECS[short])
    assert {c["description"] for c in schema["columns"]} == {""}  # descriptions live in the notes file


def test_the_description_notes_cover_every_column_without_filler():
    notes = yaml.safe_load(gen_lkl.NOTES.read_text(encoding="utf-8"))
    assert set(notes) == {f"lkl_{s}" for s in gen_lkl.ENDPOINTS}
    for key, cols in notes.items():
        assert set(cols) == set(P.SPECS[key[4:]]), key
        for col, text in cols.items():
            assert len(text) >= 15 and text.lower() != col and not gen_lkl._BANNED.match(text), (key, col)


def test_unverified_caveats_stay_in_the_descriptions():
    assert "unverified" in gen_lkl.LOCAL["y"] and "inference" in gen_lkl.LOCAL["x"]
    assert "unverified" in gen_lkl.LOCAL_BY["pbp"]["period"]
    assert "unverified" in P.parse_lkl_pbp.__doc__
    assert "CURRENT" in gen_lkl.LOCAL_BY["game_info"]["home_coach"]
    assert "NOT a state flag" in gen_lkl.LOCAL_BY["game_score"]["clock"]


def test_every_public_parser_has_a_documented_example_and_helpers_are_private():
    for name in P.__all__:
        doc = getattr(P, name).__doc__ or ""
        for part in ("Args:", "Returns:", "Example:", "Quick start::"):
            assert part in doc, (name, part)
        assert not any(ln.strip().startswith("None:") for ln in doc.splitlines()), name  # a parser raises nothing
        assert ">>>" not in doc
    assert set(P.__all__) <= set(lkl_pkg.__all__ if hasattr(lkl_pkg, "__all__") else dir(lkl_pkg))
    assert len(P.__all__) == 14


# --- runtime: a scripted transport -------------------------------------------------------------------

JSON = {"content-type": "application/json"}
HTML = {"content-type": "text/html; charset=utf-8"}


def _page_html(rec: Dict[str, Any], **override: Any) -> str:
    """A game page whose ``<game-info-component>`` carries ``rec``'s props (``:`` props as escaped JSON)."""
    props = {**{k: v for k, v in rec.items() if k not in ("start_date", "start_time", "attendance_count")}, **override}
    attrs = []
    for k, v in props.items():
        name = k.replace("_", "-")
        if k == "game_id":  # a plain string attribute on the real page
            v = str(v)
        if isinstance(v, str):
            attrs.append(f'{name}="{htmllib.escape(v)}"')
        else:
            attrs.append(f':{name}="{htmllib.escape(json.dumps(v, ensure_ascii=False))}"')
    return "<html><body><game-info-component " + " ".join(attrs) + "></game-info-component></body></html>"


class Site(HTTPAdapter):
    """A scripted lkl.lt: ``handler(request)`` returns ``(status, headers, body)``; every request is recorded."""

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
        resp.url = request.url
        resp.request = request
        return resp

    def paths(self) -> List[str]:
        return [urlsplit(c.url).path for c in self.calls]


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
    """Mount a Site on a fresh polite session; the finished-game cache starts empty."""

    def install(handler):
        adapter = Site(handler)
        session = basketball_runtime.PoliteSession()
        session.mount("https://", adapter)
        monkeypatch.setattr(R, "SESSION", session)
        monkeypatch.setattr(R, "_GAMES", {})
        return adapter

    return install


def _lkl(routes: Dict[str, Any] | None = None, *, page: str | None = None, status: int = 200, headers=None):
    """Handler: the game page, then ``/api/livestream/{route}/{id}`` bodies from the full fixtures (``routes`` overrides)."""
    page_text = page if page is not None else _page_html(_full("rungtynes"))
    over = routes or {}

    def handler(req):
        path = urlsplit(req.url).path
        if path.startswith("/rungtynes/"):
            return 200, HTML, page_text
        route = path.split("/")[3]
        if route in over:
            val = over[route]
            if callable(val):
                return val(req)
            return status, headers or JSON, val if isinstance(val, (str, bytes)) else json.dumps(val)
        return status, headers or JSON, json.dumps(_full(route))

    return handler


def _call(short: str, **args: Any) -> Any:
    """What a generated wrapper does: build the URL from the YAML, call the runtime, parse."""
    ep = next(e for e in _yaml()["endpoints"] if e["short"] == short)
    url = (ep.get("host") or _yaml()["host"]) + ep["path"].format(**args)
    params = {p["query_key"]: args[p["name"]] for p in ep.get("extra_params", []) if p["name"] in args}
    raw = R._get(url, params=params)
    return getattr(P, ep["parser"])(raw)


def test_game_wrappers_end_to_end_and_the_game_page_is_fetched_once(site, waits):
    adapter = site(_lkl())
    box = _call("player_box", game_id=GAME)
    assert box.height == 22 and box["game_id"].unique().to_list() == [GAME] and box["player_id"].null_count() == 0
    assert _call("pbp", game_id=GAME).height == 668
    assert _call("shots", game_id=GAME).height == 148
    assert _call("game_info", game_id=GAME).height == 1  # served from the cached page: no second request
    assert adapter.paths() == [
        f"/rungtynes/{GAME}", f"/api/livestream/boxscore/{GAME}", f"/api/livestream/play-by-play/{GAME}",
        f"/api/livestream/shot-chart/{GAME}",
    ]  # fmt: skip
    assert len(waits.urls) == len(adapter.calls)  # one throttled wait per request


def test_return_parsed_false_hands_back_the_stamped_body(site):
    site(_lkl())
    raw = R._get(f"https://lkl.lt/api/livestream/boxscore/{GAME}")
    req = raw[R.REQUEST_KEY]
    assert req["game_id"] == GAME and (req["home_team_id"], req["away_team_id"]) == (11, 41)
    assert req["players"]["justinas-marcinkevicius"] == 1762 and set(raw) >= {"gameStatistics", "boxscore"}
    assert P.parse_lkl_period_scores(raw).height == 10
    page = R._get(f"https://lkl.lt/rungtynes/{GAME}")
    assert page[R.REQUEST_KEY] == {"game_id": GAME} and page["teams_info"]["home"]["id"] == 11


def test_an_unfinished_game_is_a_fetch_failure_and_no_route_is_asked_for(site):
    for status in ("Vyksta", "Nepradėta", ""):
        adapter = site(_lkl(page=_page_html(_full("rungtynes"), game_status=status)))
        with pytest.raises(AssetFetchError, match="not finished"):
            _call("player_box", game_id=GAME)
        with pytest.raises(AssetFetchError, match="not finished"):
            _call("game_info", game_id=GAME)
        assert set(adapter.paths()) == {f"/rungtynes/{GAME}"}


def test_an_unplayed_games_page_has_no_component_and_is_a_fetch_failure(site):
    site(_lkl(page="<html><body>preview</body></html>"))
    with pytest.raises(AssetFetchError, match="no <game-info-component>"):
        _call("game_score", game_id=GAME)


def test_a_page_for_another_game_is_a_wrong_scope_failure(site):
    site(_lkl(page=_page_html(_full("rungtynes"), game_id="11447")))
    with pytest.raises(AssetFetchError, match="out of scope: game-id"):
        _call("player_box", game_id=GAME)


def test_404_is_no_data_for_the_page_and_for_a_route(site):
    def gone(req):
        return 404, HTML, "<html>nope</html>"

    site(gone)
    with pytest.raises(NoDataError, match="No data found"):
        _call("player_box", game_id=6000)
    site(_lkl({"leaders": lambda req: (404, HTML, "x")}))
    with pytest.raises(NoDataError, match="No data found"):
        _call("leaders", game_id=GAME)


@pytest.mark.parametrize(
    ("route", "short", "body", "match"),
    [
        ("boxscore", "player_box", {"message": "Server Error"}, "error envelope"),
        ("boxscore", "player_box", {}, "empty or not an object"),
        ("boxscore", "player_box", [1], "empty or not an object"),
        ("boxscore", "player_box", b"<html>oops</html>", "not JSON"),
        ("boxscore", "player_box", b"", "empty body"),
        ("game-score", "game_score", {"home": None, "away": None, "clock": "x"}, "game not final"),
        ("game-stats", "game_stats", {"comparison": {}}, "layout drift"),
        ("play-by-play", "pbp", {"quarters": {"regular": {"1": []}}}, "layout drift"),
    ],
)
def test_error_envelopes_stubs_and_changed_bodies_are_refused(site, route, short, body, match):
    site(_lkl({route: body}))
    with pytest.raises(AssetFetchError, match=match):
        _call(short, game_id=GAME)


def test_non_json_content_type_is_refused(site):
    site(_lkl({"boxscore": _full("boxscore")}, headers={"content-type": "text/html"}))
    with pytest.raises(AssetFetchError, match="not JSON"):
        _call("player_box", game_id=GAME)


def _swap(body: Dict[str, Any], *path: Any, value: Any) -> Dict[str, Any]:
    out = json.loads(json.dumps(body))
    node = out
    for k in path[:-1]:
        node = node[k]
    node[path[-1]] = value
    return out


def test_wrong_scope_bodies_are_refused_not_returned_empty(site):
    box = _full("boxscore")
    other = json.loads(
        json.dumps(box)
        .replace('"game_id": ' + str(GAME), '"game_id": 11447')
        .replace(f'"game_id":{GAME}', '"game_id":11447')
    )
    cases = {
        "boxscore": [
            (other, "is not game 11446's row"),
            (
                _swap(
                    box, "gameStatistics", value={"99": box["gameStatistics"]["11"], "41": box["gameStatistics"]["41"]}
                ),
                "gameStatistics teams",
            ),
            (_swap(box, "boxscore", "home", "players", 0, "slug", value="somebody-else"), "not a home player"),
            (_swap(box, "boxscore", "away", "team", "points", "value", value=1), "!= gameStatistics"),
            (_swap(box, "gameStatistics", "11", "points", value=None), "game not final"),
        ],
        "leaders": [
            (_swap(_full("leaders"), "leaders", "points", "home", "slug", value="somebody-else"), "not a home player")
        ],
        "play-by-play": [
            (
                _swap(_full("play-by-play"), "quarters", "regular", "1", 5, "player", "slug", value="somebody-else"),
                "not a",
            ),
        ],
        "shot-chart": [(_swap(_full("shot-chart"), "quarters", "regular", "1", 0, "player_id", value=99999), "not a")],
    }
    short = {"boxscore": "player_box", "leaders": "leaders", "play-by-play": "pbp", "shot-chart": "shots"}
    for route, items in cases.items():
        for body, match in items:
            site(_lkl({route: body}))
            with pytest.raises(AssetFetchError, match=match):
                _call(short[route], game_id=GAME)


def test_a_play_by_play_without_a_game_end_line_is_unfinished(site):
    body = _full("play-by-play")
    body["quarters"]["overtime"] = [ln for ln in body["quarters"]["overtime"] if ln["type"] != "game"]
    site(_lkl({"play-by-play": body}))
    with pytest.raises(AssetFetchError, match="game-end line"):
        _call("pbp", game_id=GAME)


@pytest.mark.parametrize("short", ["pbp", "shots"])
@pytest.mark.parametrize("empty", [{"quarters": {"regular": [], "overtime": []}}, {"regular": [], "overtime": []}])
def test_a_finished_game_before_2024_25_has_no_plays_or_shots(site, short, empty):
    route = {"pbp": "play-by-play", "shots": "shot-chart"}[short]
    site(_lkl({route: empty}))
    with pytest.raises(NoDataError, match="2024-25"):
        _call(short, game_id=GAME)


def test_400_is_a_value_error_and_other_statuses_are_fetch_failures(site, waits):
    site(_lkl({"boxscore": b""}, status=400))
    with pytest.raises(ValueError, match="HTTP 400"):
        _call("player_box", game_id=GAME)
    site(_lkl({"boxscore": b""}, status=403))
    with pytest.raises(AssetFetchError, match="HTTP 403"):
        _call("player_box", game_id=GAME)


def test_retries_wait_their_turn_and_honour_retry_after(site, waits):
    state = {"n": 0}

    def flaky(req):
        state["n"] += 1
        if state["n"] == 1:
            return 429, {"Retry-After": "7"}, b""
        if state["n"] == 2:
            return 503, {}, b""
        return 200, JSON, json.dumps(_full("leaders"))

    adapter = site(_lkl({"leaders": flaky}))
    assert _call("leaders", game_id=GAME).height == 60
    assert len(adapter.calls) == 4 and len(waits.urls) == 4  # the page + three attempts, each throttled
    assert max(waits.slept) >= 7.0  # Retry-After honoured, not just the exponential delay


def test_a_server_that_never_recovers_is_a_fetch_failure_after_three_attempts(site, waits):
    adapter = site(_lkl({"shot-chart": b""}, status=500))
    with pytest.raises(AssetFetchError, match="HTTP 500"):
        _call("shots", game_id=GAME)
    assert len(adapter.calls) == 1 + R.ATTEMPTS and len(waits.urls) == 1 + R.ATTEMPTS


def test_a_connection_failure_names_the_exception_class_only(site, monkeypatch):
    def boom(*a, **k):
        raise requests.ConnectionError("secret-host-detail")

    site(_lkl())
    monkeypatch.setattr(R, "download", boom)
    with pytest.raises(AssetFetchError, match="ConnectionError") as e:
        _call("player_box", game_id=GAME)
    assert "secret-host-detail" not in str(e.value)


@pytest.mark.parametrize("bad", ["abc", None, True, 1.5, -3])
def test_a_bad_game_id_is_a_value_error_before_any_request(site, bad):
    adapter = site(_lkl())
    with pytest.raises(ValueError):
        R._get(f"https://lkl.lt/api/livestream/boxscore/{bad}")
    assert adapter.calls == []


def test_unsupported_kwargs_and_foreign_urls_are_refused(site):
    adapter = site(_lkl())
    with pytest.raises(TypeError, match="unsupported"):
        R._get(f"https://lkl.lt/rungtynes/{GAME}", timeout=3)
    for url in ("https://example.com/rungtynes/1", "https://lkl.lt/zaidejai/x", "https://lkl.lt/api/livestream/boxscore",
                "https://lkl.lt/api/livestream/other/1", "https://lkl.lt/loadStandings"):  # fmt: skip
        with pytest.raises(ValueError, match="LKL"):
            R._get(url)
    assert adapter.calls == []


def test_personal_fields_are_redacted_by_key_name_in_raw_bodies(site):
    body = _full("leaders")
    body["leaders"]["points"]["home"]["birth_date"] = "1990-01-01"
    body["leaders"]["points"]["home"]["others"][0]["Email"] = "x@y.z"
    site(_lkl({"leaders": body}))
    raw = R._get(f"https://lkl.lt/api/livestream/leaders/{GAME}")
    assert raw["leaders"]["points"]["home"]["birth_date"] == "<redacted>"
    assert raw["leaders"]["points"]["home"]["others"][0]["Email"] == "<redacted>"
    assert "1990-01-01" not in json.dumps(raw)


# --- season pages: the END-year -> opaque id mapping ----------------------------------------------------

#: Results ids and standings ids by sdv END year (lkl/README.md "Season keys").
RESULTS_IDS = {2027: 49436, 2026: 41936, 2025: 39252, 2024: 34698, 2023: 34514, 2022: 30527, 2021: 28293, 2020: 25195,
               2019: 21837, 2018: 18743, 2017: 9225, 2016: 3120, 2015: 3118, 2014: 3116, 2013: 3094, 2012: 3095,
               2011: 3093, 2010: 3092, 2009: 3091, 2008: 3090, 2007: 3096, 2006: 3107, 2005: 3106, 2004: 3105,
               2003: 3104, 2002: 3103, 2001: 3112, 2000: 3111, 1999: 3110, 1998: 3109, 1997: 3108, 1996: 3115,
               1995: 3117, 1994: 3114}  # fmt: skip
STANDINGS_IDS = {2027: 35, 2026: 34, 2025: 33, 2024: 32, 2023: 31, 2022: 30, 2021: 29, 2020: 28, 2019: 27, 2018: 2,
                 2017: 1, 2016: 26, 2015: 10, 2014: 9, 2013: 7, 2012: 8, 2011: 6, 2010: 5, 2009: 4, 2008: 3,
                 2007: 11, 2006: 12, 2005: 13, 2004: 14, 2003: 15, 1994: 24}  # fmt: skip


def test_the_season_end_year_is_mapped_through_the_sites_selects():
    results, standings = _html("rezultatai_selects.html"), _html("turnyrine_select.html")
    for year, sid in RESULTS_IDS.items():
        assert R._season_id(results, "season-lkl", year, "x") == sid, year
    for year, sid in STANDINGS_IDS.items():
        assert R._season_id(standings, "season", year, "x") == sid, year
    assert len(RESULTS_IDS) == 34  # 1993-94 .. 2026-27
    with pytest.raises(NoDataError, match="no season ending in 2099"):
        R._season_id(results, "season-lkl", 2099, "x")
    dup = results.replace("2007-2008 m. sezonas", "2008-2009 m. sezonas")
    with pytest.raises(AssetFetchError, match="ambiguous"):
        R._season_id(dup, "season-lkl", 2009, "x")
    with pytest.raises(AssetFetchError, match="no select"):
        R._season_id("<html></html>", "season-lkl", 2026, "x")


def _season_site(results_pages: List[str], standings: str | None = None, results_select: str | None = None):
    def handler(req):
        u = urlsplit(req.url)
        q = {k: v[0] for k, v in parse_qs(u.query).items()}
        if u.path == "/rezultatai":
            return 200, HTML, results_select or _html("rezultatai_selects.html")
        if u.path == "/turnyrine-lentele":
            return 200, HTML, _html("turnyrine_select.html")
        if u.path == "/loadResults":
            n = int(q["page"])
            return 200, HTML, results_pages[n - 1] if n <= len(results_pages) else "<div>Informacija ruošiama</div>"
        if u.path.startswith("/loadStandings/"):
            return 200, HTML, standings or ""
        return 404, HTML, ""

    return handler


def test_results_wrapper_walks_every_page_for_the_mapped_season_id(site, waits):
    pages = [_html(f"results_49436_p{n}.html") for n in (1, 2, 3)]
    adapter = site(_season_site(pages))
    df = _call("results", season=2027)
    assert df.height == 22 and df["season_end_year"].unique().to_list() == [2027]
    assert adapter.paths() == ["/rezultatai"] + ["/loadResults"] * 3  # the last page holds 2 games: no extra page
    queries = [parse_qs(urlsplit(c.url).query) for c in adapter.calls[1:]]
    assert [q["page"] for q in queries] == [["1"], ["2"], ["3"]] and {q["season"][0] for q in queries} == {"49436"}
    assert len(waits.urls) == 4
    raw = R._get("https://lkl.lt/loadResults", params={"season": 2027})
    assert raw[R.REQUEST_KEY] == {"season": 2027, "season_id": 49436}


def test_results_wrapper_confirms_a_full_last_page_and_refuses_the_rest(site):
    p1, p2, p3 = (_html(f"results_49436_p{n}.html") for n in (1, 2, 3))
    one_page = re.sub(r"loadResults\?page=\d+", "loadResults?page=1", p1)
    adapter = site(_season_site([one_page]))  # page 2 answers "Informacija ruošiama"
    assert _call("results", season=2027).height == 10
    assert adapter.paths() == ["/rezultatai", "/loadResults", "/loadResults"]  # page 2 fetched to confirm
    site(_season_site([one_page, p2, p3]))
    with pytest.raises(AssetFetchError, match="after the paginator's last page"):
        _call("results", season=2027)
    site(_season_site([p1, p2, p3]))
    with pytest.raises(AssetFetchError, match="outside 2025-2026"):  # the id maps to 2025-26; the dates are 2026-27
        _call("results", season=2026)
    site(_season_site([p1, p3, p3]))
    with pytest.raises(AssetFetchError, match="expected 10 on all but the last page"):
        _call("results", season=2027)


def test_a_season_without_played_games_or_in_the_selects_is_no_data(site):
    site(_season_site(["<div>Informacija ruošiama</div>"]))
    with pytest.raises(NoDataError, match="no played games"):
        _call("results", season=2027)
    with pytest.raises(NoDataError, match="no season ending in 2099"):
        _call("results", season=2099)
    with pytest.raises(NoDataError, match="no season ending in 2099"):
        _call("standings", season=2099)


def test_standings_wrapper_maps_the_season_and_reads_the_fragment(site, waits):
    adapter = site(_season_site([], standings=_html("standings_35.html")))
    df = _call("standings", season=2027)
    assert df["season_id"].unique().to_list() == [35] and df["season_end_year"].unique().to_list() == [2027]
    assert adapter.paths() == ["/turnyrine-lentele", "/loadStandings/35"] and len(waits.urls) == 2
    adapter = site(_season_site([], standings=_html("standings_3.html")))
    assert _call("standings", season=2008)["season_id"].unique().to_list() == [3]
    assert adapter.paths()[-1] == "/loadStandings/3"
    site(_season_site([], standings=_html("standings_35.html").replace("Poz.", "Pos.")))
    with pytest.raises(AssetFetchError, match="header drift"):
        _call("standings", season=2027)


def test_standings_404_is_no_data_and_a_changed_select_page_is_a_fetch_failure(site):
    def handler(req):
        path = urlsplit(req.url).path
        if path == "/turnyrine-lentele":
            return 200, HTML, _html("turnyrine_select.html")
        return 404, HTML, "x"

    site(handler)
    with pytest.raises(NoDataError, match="No data found"):
        _call("standings", season=2027)
    site(lambda req: (200, HTML, "<html>no select</html>"))
    with pytest.raises(AssetFetchError, match="no select"):
        _call("standings", season=2027)
    site(lambda req: (200, {"content-type": "application/json"}, "{}"))
    with pytest.raises(AssetFetchError, match="not HTML"):
        _call("results", season=2027)


@pytest.mark.parametrize("bad", ["abc", None, True, 20.5])
def test_season_arguments_are_validated_before_any_request(site, bad):
    adapter = site(_season_site([]))
    with pytest.raises(ValueError):
        R._get("https://lkl.lt/loadResults", params={"season": bad})
    with pytest.raises(ValueError):
        R._get(f"https://lkl.lt/loadStandings/{bad}")
    with pytest.raises(ValueError, match="needs season"):
        R._get("https://lkl.lt/loadResults")
    assert adapter.calls == []


def test_no_fixture_holds_personal_contact_or_birth_fields():
    for path in list((FIXTURES / "full").glob("*.json")) + list((FIXTURES / "captures").glob("*.json")):
        text = path.read_text(encoding="utf-8")
        assert not R._PERSONAL.search("".join(re.findall(r'"([A-Za-z_]+)":', text))), path.name
