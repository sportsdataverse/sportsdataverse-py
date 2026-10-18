"""Offline tests for the VHL (``vhl``) flat-API family: HTML parsers, runtime and YAML.

Parsers are asserted against real pages: the cut-down pages in ``tests/fixtures/vhl/`` (the
sdv-internal-refs parser fixtures) and the full report page in ``tests/fixtures/vhl/full/``
(dtypes come from that one). The refs capture records in ``tests/fixtures/vhl/captures/`` are
the runtime's raw-record shape. The runtime is driven through a fake transport
(``vhl_runtime._transport``), so every refusal is exercised end to end. No network.
"""

from __future__ import annotations

import functools
import json
import re
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd
import polars as pl
import pytest
import yaml

import sportsdataverse.hockey.vhl as vhl_pkg
from sportsdataverse.errors import AssetFetchError, NoDataError
from sportsdataverse.hockey import euro_hockey_runtime
from sportsdataverse.hockey.vhl import vhl_parsers as P
from sportsdataverse.hockey.vhl import vhl_runtime as R

ROOT = Path(__file__).parents[3]
FIXTURES = ROOT / "tests" / "fixtures" / "vhl"
YAML_PATH = ROOT / "tools" / "codegen" / "endpoints" / "vhl.yaml"
SCHEMA_DIR = ROOT / "tools" / "codegen" / "schemas" / "native" / "vhl"
MANUAL = ROOT / "tools" / "codegen" / "manual_column_descriptions.yaml"
HOST = "https://www.vhlru.ru"

sys.path.insert(0, str(ROOT / "tools" / "codegen"))
import gen_vhl  # noqa: E402

SNAKE = re.compile(r"^[a-z0-9]+(_[a-z0-9]+)*$")
ALL_PARSERS = [getattr(P, n) for n in P.__all__]
REPORT = "full/report_899223.html"


def _html(name: str) -> str:
    return (FIXTURES / name).read_text(encoding="utf-8")


def _capture(name: str) -> Dict[str, Any]:
    return json.loads((FIXTURES / "captures" / name).read_text(encoding="utf-8"))


@functools.lru_cache(maxsize=None)
def _descriptions(key: str) -> Dict[str, str]:
    """The ``vhl_<short>`` description block: manual_column_descriptions.yaml, else the gen snippet."""
    loader = getattr(yaml, "CSafeLoader", yaml.SafeLoader)
    block = (yaml.load(MANUAL.read_text(encoding="utf-8"), Loader=loader) or {}).get(key)
    if block is None and gen_vhl.SNIPPET.exists():
        block = (yaml.safe_load(gen_vhl.SNIPPET.read_text(encoding="utf-8")) or {}).get(key)
    return block or {}


def _id_cols(df: pl.DataFrame) -> List[str]:
    return [c for c in df.columns if c == "tn" or c.endswith("_id") or c == "calendar_tn"]


# --- parsers over every committed page ---------------------------------------------------------


@pytest.mark.parametrize("short", sorted(gen_vhl.ENDPOINTS))
def test_every_wrapper_parses_its_fixture(short):
    df = gen_vhl._frame(gen_vhl.ENDPOINTS[short])
    assert isinstance(df, pl.DataFrame) and df.height > 0, short
    assert all(SNAKE.match(c) for c in df.columns), [c for c in df.columns if not SNAKE.match(c)]
    assert {c: df.schema[c] for c in _id_cols(df)} == {c: pl.Int64 for c in _id_cols(df)}
    if "season" in df.columns:
        assert df.schema["season"] == pl.Int64 and df["season"].null_count() == 0


@pytest.mark.parametrize("short", sorted(gen_vhl.ENDPOINTS))
def test_committed_schema_matches_the_parser(short):
    committed = yaml.safe_load((SCHEMA_DIR / f"{short}.yaml").read_text(encoding="utf-8"))
    df = gen_vhl._frame(gen_vhl.ENDPOINTS[short])
    assert committed["schema"] == f"vhl_{short}"
    assert [c["name"] for c in committed["columns"]] == df.columns
    typed = {"integer": [pl.Int64], "numeric": [pl.Float64], "logical": [pl.Boolean]}
    described = _descriptions(f"vhl_{short}")
    for col in committed["columns"]:
        assert col["description"] == "", col["name"]  # descriptions never live in schemas/**
        assert len(described.get(col["name"]) or "") >= 15, col["name"]
        if col["type"] in typed:
            assert df.schema[col["name"]] in typed[col["type"]], col


@pytest.mark.parametrize(
    ("kind", "page"),
    [
        ("calendar", "calendar_1374.html"),
        ("report", "report_899223.html"),
        ("report", REPORT),
        ("online", "online_899223.html"),
        ("standings", "standings_1374.html"),
        ("stats_players", "stats_players_1374.html"),
        ("player", "player_34418.html"),
        ("team", "team_460279.html"),
    ],
)
def test_every_page_parses_to_its_record(kind, page):
    rec = P._page_record(kind, _html(page))
    assert isinstance(rec, dict) and rec


@pytest.mark.parametrize(
    ("parser", "capture"),
    [
        (P.parse_vhl_schedule, "en__calendar__tn__0.json"),
        (P.parse_vhl_game_info, "en__report__tn.json"),
        (P.parse_vhl_goals, "en__report__tn.json"),
        (P.parse_vhl_penalties, "en__report__tn.json"),
        (P.parse_vhl_player_box, "en__report__tn.json"),
        (P.parse_vhl_goalie_box, "en__report__tn.json"),
        (P.parse_vhl_pbp, "online__idgame.json"),
        (P.parse_vhl_standings, "en__standings__league__tn.json"),
        (P.parse_vhl_skater_stats, "en__stats__players__tn.json"),
        (P.parse_vhl_goalie_stats, "en__stats__players__tn.json"),
        (P.parse_vhl_player, "en__players__player_id.json"),
        (P.parse_vhl_player_career, "en__players__player_id@40370.json"),
        (P.parse_vhl_roster, "en__teams__team_id.json"),
    ],
)
def test_parsers_take_the_record_the_runtime_returns(parser, capture):
    df = parser(_capture(capture))
    assert df.height > 0 and all(SNAKE.match(c) for c in df.columns)


def test_html_and_record_inputs_give_the_same_frame():
    page = _html(REPORT)
    assert P.parse_vhl_goals(page).equals(P.parse_vhl_goals(P._page_record("report", page)))


@pytest.mark.parametrize("parser", ALL_PARSERS, ids=lambda f: f.__name__)
@pytest.mark.parametrize("payload", [{}, [], None, "x", 3, b"\xff", "<html><body><h1>Game summary</h1></body></html>"])
def test_empty_or_malformed_payload_is_a_zero_row_frame(parser, payload):
    df = parser(payload)
    assert isinstance(df, pl.DataFrame) and df.height == 0 and df.width > 0


@pytest.mark.parametrize("parser", ALL_PARSERS, ids=lambda f: f.__name__)
def test_empty_payload_keeps_the_documented_columns(parser):
    df = parser(None, columns=["season", "game_id", "save_pct", "home_on_ice", "never_sent"])
    assert df.height == 0
    assert df.schema["season"] == pl.Int64 and df.schema["game_id"] == pl.Int64
    assert df.schema["save_pct"] == pl.Float64 and df.schema["home_on_ice"] == pl.List(pl.Int64)
    assert df.schema["never_sent"] == pl.String


@pytest.mark.parametrize("short", ["schedule", "goals", "player_career"])
def test_return_as_pandas(short):
    entry = gen_vhl.ENDPOINTS[short]
    out = entry[2](_html(entry[3]), return_as_pandas=True)
    assert isinstance(out, pd.DataFrame) and len(out) > 0


# --- dtypes from the full page -----------------------------------------------------------------


def test_rates_are_float_even_when_the_page_prints_them_whole():
    skaters = P.parse_vhl_player_box(_html(REPORT))
    # a game report prints Sft/G as a whole number ('22'); the season tables print '22.8'
    assert skaters.schema["shifts_per_game"] == pl.Float64
    assert 22.0 in skaters["shifts_per_game"].to_list()
    assert all(v == int(v) for v in skaters["shifts_per_game"].drop_nulls().to_list())
    for col in ("faceoff_pct", "shot_pct"):
        assert skaters.schema[col] == pl.Float64
    assert skaters.schema["toi_per_game"] == pl.String  # mm:ss, never a number
    goalies = P.parse_vhl_goalie_box(_html(REPORT))
    assert goalies.schema["save_pct"] == pl.Float64 and goalies.schema["gaa"] == pl.Float64
    assert goalies["save_pct"].to_list() == [100.0, 60.0, 93.8, None]  # '-' for the goalie who did not play


def test_every_rate_column_of_every_frame_is_float():
    for short, entry in gen_vhl.ENDPOINTS.items():
        df = gen_vhl._frame(entry)
        for col in df.columns:
            if col.endswith("_pct") or col == "gaa" or (col.endswith("_per_game") and not col.startswith("toi")):
                assert df.schema[col] == pl.Float64, (short, col)


def test_full_report_is_complete():
    assert P.parse_vhl_player_box(_html(REPORT)).height == 34
    assert P.parse_vhl_goalie_box(_html(REPORT))["games_played"].to_list() == [1, 1, 1, 0]


# --- shapes and id key spaces --------------------------------------------------------------------


def test_goals_carry_on_ice_jerseys_and_the_scoring_side():
    goals = P.parse_vhl_goals(_html(REPORT))
    assert goals.schema["home_on_ice"] == pl.List(pl.Int64) and goals.schema["away_on_ice"] == pl.List(pl.Int64)
    assert goals["team_side"].to_list() == ["away", "away", "home", "home", "away"]
    assert goals["team_page_id"].to_list() == [505612, 505612, 390, 390, 505612]
    shootout = goals.row(-1, named=True)  # the shootout winner: the goalie alone on ice, period 'РБ'
    assert shootout["period"] == "РБ" and shootout["away_on_ice"] == [] and len(shootout["home_on_ice"]) == 1


def test_report_frames_share_the_game_key_from_the_request():
    rec = P._page_record("report", _html(REPORT))
    rec[R.REQUEST_KEY] = {"season": 2026, "stage": "regular", "tn": 1374, "game_id": 899223}
    for parser in (P.parse_vhl_game_info, P.parse_vhl_goals, P.parse_vhl_penalties, P.parse_vhl_player_box):
        df = parser(rec)
        assert df.select("game_id", "season", "stage", "game_number").unique().rows() == [
            (899223, 2026, "regular", 984)
        ]


def test_bare_report_html_has_no_game_id_but_keeps_its_season():
    info = P.parse_vhl_game_info(_html(REPORT))
    assert info["game_id"].to_list() == [None] and info["season"].to_list() == [2026]


def test_club_and_team_page_ids_bridge_only_through_game_id():
    sched = P.parse_vhl_schedule(_html("calendar_1374.html"))
    rec = P._page_record("report", _html(REPORT))
    rec[R.REQUEST_KEY] = {"season": 2026, "stage": "regular", "tn": 1374, "game_id": 899223}
    info = P.parse_vhl_game_info(rec)
    assert sched.schema["game_id"] == info.schema["game_id"] == pl.Int64
    row = sched.join(info.select("game_id", "home_team_page_id", "away_team_page_id"), on="game_id").row(0, named=True)
    # Toros: club id 192 (logo) and team page id 390 are different spaces of the same team
    assert (row["home_club_id"], row["home_team_page_id"]) == (192, 390)
    assert (row["away_club_id"], row["away_team_page_id"]) == (892, 505612)


def test_schedule_dates_take_the_season_year():
    sched = P.parse_vhl_schedule(_html("calendar_1374.html"))
    assert set(sched["game_date"].to_list()) == {"2026-03-18"}
    assert sched.select("season", "stage", "tn").unique().rows() == [(2026, "regular", 1374)]


def test_career_labels_become_end_year_and_stage():
    career = P.parse_vhl_player_career(_html("player_34418.html"))
    assert career.select("season", "stage", "tn", "team_season_id").rows() == [
        (2027, "regular", 1430, 15524),
        (2026, "playoff", 1375, 15429),
    ]
    goalie = P.parse_vhl_player_career(_capture("en__players__player_id@40370.json"))
    assert goalie.columns == career.columns and goalie["save_pct"].null_count() < goalie.height


def test_standings_rows_carry_team_season_ids():
    st = P.parse_vhl_standings(_html("standings_1374.html"))
    assert st.schema["team_season_id"] == pl.Int64 and st["team_season_id"].null_count() == 0
    assert st.schema["goals_for"] == pl.Int64 and st["rank"].to_list() == [1, 2, 3]


def test_skater_stats_tag_position_groups():
    st = P.parse_vhl_skater_stats(_html("stats_players_1374.html"))
    assert st["position_group"].unique().sort().to_list() == ["defense", "forward"]
    assert P.parse_vhl_goalie_stats(_html("stats_players_1374.html")).schema["save_pct"] == pl.Float64


def test_pbp_is_russian_with_player_id_lists():
    pbp = P.parse_vhl_pbp(_html("online_899223.html"))
    assert pbp.schema["player_ids"] == pl.List(pl.Int64)
    assert pbp["event_index"].to_list() == list(range(pbp.height))
    assert set(pbp["kind"].drop_nulls().to_list()) >= {"goal", "penalty", "shootout_attempt"}
    assert pbp["home_team"][0] == "Торос"


def test_roster_is_cyrillic_and_fixture_birth_dates_are_masked():
    roster = P.parse_vhl_roster(_html("team_460279.html"))
    assert set(roster["born"].to_list()) == {"01.01.2000"}
    assert all(re.search(r"[А-Яа-я]", n) for n in roster["player"].to_list())
    assert P.parse_vhl_player(_html("player_34418.html"))["born"].to_list() == ["01.01.2000"]
    assert all(r["born"] == "DD.MM.YYYY" for r in _capture("en__teams__team_id.json")["roster"])


def test_tournament_table_lists_every_season_stage():
    table = P._tournament_table(_html("tournaments.html"))
    assert len(table) == 33
    pairs = {(r["season"], r["stage"]): r["tn"] for r in table}
    assert pairs[(2011, "regular")] == 189 and pairs[(2011, "playoff")] == 190 and pairs[(2027, "regular")] == 1430
    # the KHL rule "playoffs = regular + 1" does not hold for the VHL
    assert (pairs[(2020, "regular")], pairs[(2020, "playoff")]) == (872, 875)
    assert (pairs[(2018, "regular")], pairs[(2018, "playoff")]) == (492, 496)


# --- the HTML layer's refusals ---------------------------------------------------------------------


def test_table_reader_refuses_header_drift_short_rows_and_zero_rows():
    good = (
        "<table><thead><tr><th>Time</th><th>Player</th><th></th><th>Penalty</th></tr></thead><tbody>{}</tbody></table>"
    )
    row = "<tr><td>01:00</td><td>1. A B</td><td>2</td><td>hooking</td></tr>"
    table = P._parse_html(good.format(row)).find("table")
    assert P._read_table(table, P.REPORT_PENALTIES, "p")[0]["minutes"] == 2
    with pytest.raises(P._NotParsed, match="spans"):
        P._read_table(
            P._parse_html(good.format("<tr><td>01:00</td><td>x</td><td>2</td></tr>")).find("table"),
            P.REPORT_PENALTIES,
            "p",
        )
    with pytest.raises(P._NotParsed, match="zero rows"):
        P._read_table(P._parse_html(good.format("")).find("table"), P.REPORT_PENALTIES, "p")
    with pytest.raises(P._NotParsed, match="headers"):
        P._read_table(
            P._parse_html(good.replace("Penalty", "Infraction").format(row)).find("table"), P.REPORT_PENALTIES, "p"
        )


def test_hidden_cells_are_dropped_and_deep_nesting_does_not_recurse():
    html = "<table><tr><th>#</th><th style='display: none'>x</th></tr><tr><td>7</td><td style='display:none'>9</td></tr></table>"
    head, body = P._table_parts(P._parse_html(html).find("table"))
    assert [c.text() for c in head] == ["#"] and [c.text() for c in body[0]] == ["7"]
    deep = "<div>" * 5000 + "x" + "</div>" * 5000
    assert P._parse_html(deep).text() == "x"


def test_record_identity_checks():
    with pytest.raises(P._NotParsed, match="tournament"):
        P._page_record("calendar", _html("calendar_1374.html"), {"tn": 1430})
    with pytest.raises(P._NotParsed, match="game number"):
        P._page_record("report", _html(REPORT), {"season": "2025/2026", "stage": "Regular Season", "game_number": 985})
    with pytest.raises(P._NotParsed, match="season"):
        P._page_record(
            "report", _html(REPORT), {"season": 2026, "stage": "Regular Season", "game_number": 984}
        )  # strict types
    with pytest.raises(P._NotParsed, match="player"):
        P._page_record("player", _html("player_34418.html"), {"player_id": 34526})
    shell = P._NotParsed
    with pytest.raises(shell) as exc:
        P._page_record("report", "<html><body><h1>Game summary</h1></body></html>")
    assert exc.value.empty


# --- YAML -------------------------------------------------------------------------------------------


def _yaml() -> Dict[str, Any]:
    return yaml.safe_load(YAML_PATH.read_text(encoding="utf-8"))


def test_yaml_lists_every_route():
    doc = _yaml()
    assert [e["short"] for e in doc["endpoints"]] == list(gen_vhl.ENDPOINTS)
    assert len(doc["endpoints"]) == 13
    assert {e[0] for e in gen_vhl.ENDPOINTS.values()} == {
        "/en/calendar/{tn}/0/",
        "/en/report/{tn}/",
        "/online/{idgame}.html",
        "/en/standings/league/{tn}/",
        "/en/stats/players/{tn}/",
        "/en/players/{player_id}/",
        "/en/teams/{team_id}/",
    }
    assert doc["getter_module"] == "sportsdataverse.hockey.vhl.vhl_runtime" and doc["schema_prefix"] == "vhl_"
    assert all((SCHEMA_DIR / f"{e['short']}.yaml").exists() for e in doc["endpoints"])


def test_every_wrapper_carries_the_sanctions_flag():
    notes = _yaml()["docstring"]["notes"]
    assert notes == [gen_vhl.SANCTIONS] and "sanctioned" in notes[0] and "sign-off" in notes[0]


def test_no_path_token_named_league_or_sport():
    for ep in _yaml()["endpoints"]:
        assert not {"league", "sport"} & set(re.findall(r"\{([^}]+)\}", ep["path"])), ep["path"]


def test_season_routes_take_the_end_year_and_a_stage():
    eps = {e["short"]: e for e in _yaml()["endpoints"]}
    for short in ("schedule", "game_info", "goals", "skater_stats", "goalie_stats", "standings"):
        assert "{season}" in eps[short]["path"] and eps[short]["example_args"]["season"] == 2026
    for short in ("schedule", "goals", "pbp", "skater_stats"):
        stage = next(p for p in eps[short]["extra_params"] if p["name"] == "stage")
        assert stage["choices"] == ["regular", "playoff"] and stage["default"] == "regular"
    assert "extra_params" not in eps["standings"]  # regular season only
    assert eps["pbp"]["host"] == "https://online.vhlru.ru"
    assert eps["roster"]["path"] == "/en/teams/{team_page_id}/"


def test_family_docstring_carries_the_sanctions_flag():
    assert "sanction" in (vhl_pkg.__doc__ or "").lower()
    assert {"vhl_tn", "parse_vhl_goals"} <= set(vhl_pkg.__all__)


# --- runtime through a fake transport -------------------------------------------------------------


class _Resp:
    def __init__(
        self, status: int = 200, body: Any = "", ctype: str = "text/html; charset=UTF-8", url: Optional[str] = None
    ):
        self.status_code = status
        self.content = body.encode("utf-8") if isinstance(body, str) else body
        self.headers = {"content-type": ctype}
        self.url = url


class _Site:
    """URL -> response (a page name, a ``_Resp`` or a list of them, served in turn)."""

    def __init__(self) -> None:
        self.pages: Dict[str, Any] = {R.CALENDAR_ROOT: "tournaments.html"}
        self.calls: List[str] = []
        self.waited: List[str] = []

    def __call__(self, url: str) -> Any:
        self.calls.append(url)
        page = self.pages.get(url)
        if isinstance(page, list):
            page = page.pop(0) if len(page) > 1 else page[0]
        if isinstance(page, BaseException):
            raise page
        if page is None:
            return _Resp(404, "Not found", url=url)
        if isinstance(page, _Resp):
            page.url = page.url or url
            return page
        return _Resp(200, _html(page), url=url)


@pytest.fixture()
def site(monkeypatch):
    s = _Site()
    monkeypatch.setattr(R, "_transport", s)
    monkeypatch.setattr(euro_hockey_runtime.time, "sleep", lambda secs: None)
    monkeypatch.setattr(euro_hockey_runtime, "_polite_wait", lambda url, *a: s.waited.append(url))
    R._tournaments.cache_clear()
    R._games.clear()
    yield s
    R._tournaments.cache_clear()
    R._games.clear()


CAL = f"{HOST}/en/calendar/1374/0/"
REP = f"{HOST}/en/report/1374/?idgame=899223"


def test_season_maps_to_tn_once_and_rewrites_the_path(site):
    site.pages[CAL] = "calendar_1374.html"
    rec = R._get(f"{HOST}/en/calendar/2026/0/", {"stage": "regular"})
    assert rec[R.REQUEST_KEY] == {"season": 2026, "stage": "regular", "tn": 1374}
    assert site.calls == [R.CALENDAR_ROOT, CAL] and site.waited == [R.CALENDAR_ROOT, CAL]
    assert R.vhl_tn(2026, "playoff") == 1375 and R.vhl_tn(2011) == 189
    assert site.calls.count(R.CALENDAR_ROOT) == 1  # cached


def test_vhl_tn_refuses_unknown_seasons_and_stages(site):
    with pytest.raises(NoDataError, match="no playoff tournament for season 2027"):
        R.vhl_tn(2027, "playoff")
    with pytest.raises(ValueError, match="stage"):
        R.vhl_tn(2026, "playoffs")
    with pytest.raises(ValueError, match="season"):
        R.vhl_tn(True)


def test_report_is_checked_against_the_calendar(site):
    site.pages.update({CAL: "calendar_1374.html", REP: REPORT})
    rec = R._get(f"{HOST}/en/report/2026/", {"idgame": 899223, "stage": "regular"})
    assert rec[R.REQUEST_KEY] == {"season": 2026, "stage": "regular", "tn": 1374, "game_id": 899223}
    assert P.parse_vhl_goals(rec)["game_id"].unique().to_list() == [899223]
    # a second report of the same tn reads the cached calendar
    R._get(f"{HOST}/en/report/2026/", {"idgame": 899223})
    assert site.calls.count(CAL) == 1


def test_report_of_another_game_number_is_refused(site):
    # the calendar says 899225 is game 986; the page served is game 984
    site.pages.update({CAL: "calendar_1374.html", f"{HOST}/en/report/1374/?idgame=899225": REPORT})
    with pytest.raises(AssetFetchError, match="game number 984 != 986"):
        R._get(f"{HOST}/en/report/2026/", {"idgame": 899225})


def test_a_game_outside_the_calendar_is_nodata_without_fetching_the_report(site):
    site.pages[CAL] = "calendar_1374.html"
    with pytest.raises(NoDataError, match="not in the calendar"):
        R._get(f"{HOST}/en/report/2026/", {"idgame": 1})
    assert not any("/report/" in u for u in site.calls)


def test_the_empty_report_shell_is_nodata(site):
    shell = "<html><head><title>Supreme Hockey League - Game summary</title></head><body><h1>Game summary</h1></body></html>"
    site.pages.update({CAL: "calendar_1374.html", REP: _Resp(200, shell)})
    with pytest.raises(NoDataError, match="match-card"):
        R._get(f"{HOST}/en/report/2026/", {"idgame": 899223})


def test_an_unknown_tn_serving_the_current_season_is_refused(site):
    # season 2027 maps to tn 1430; the site answers with another season's calendar
    site.pages[f"{HOST}/en/calendar/1430/0/"] = "calendar_1374.html"
    with pytest.raises(AssetFetchError, match="tournament 1374, asked 1430"):
        R._get(f"{HOST}/en/calendar/2027/0/", {})


def test_standings_read_league_route_and_check_the_tn(site):
    site.pages[f"{HOST}/en/standings/league/1374/"] = "standings_1374.html"
    assert R._get(f"{HOST}/en/standings/league/2026/", {})["rows"]
    site.pages[f"{HOST}/en/standings/league/1430/"] = "standings_1374.html"
    with pytest.raises(AssetFetchError, match="asked 1430"):
        R._get(f"{HOST}/en/standings/league/2027/", {})
    with pytest.raises(ValueError, match="regular season only"):
        R._get(f"{HOST}/en/standings/league/2026/", {"stage": "playoff"})


def test_broadcast_needs_its_season_and_its_game_number(site):
    online = "https://online.vhlru.ru/online/{}.html"
    site.pages.update({CAL: "calendar_1374.html", online.format(899223): "online_899223.html"})
    rec = R._get(online.format(899223), {"season": 2026})
    assert rec[R.REQUEST_KEY]["game_id"] == 899223 and P.parse_vhl_pbp(rec)["game_id"][0] == 899223
    site.pages[online.format(899225)] = "online_899223.html"
    with pytest.raises(AssetFetchError, match="game number 984 != 986"):
        R._get(online.format(899225), {"season": 2026})
    with pytest.raises(ValueError, match="season"):
        R._get(online.format(899223), {})


def test_player_page_must_link_its_own_id(site):
    site.pages[f"{HOST}/en/players/34418/"] = "player_34418.html"
    assert R._get(f"{HOST}/en/players/34418/", {})[R.REQUEST_KEY] == {"player_id": 34418}
    site.pages[f"{HOST}/en/players/34526/"] = "player_34418.html"
    with pytest.raises(AssetFetchError, match="page is player 34418"):
        R._get(f"{HOST}/en/players/34526/", {})


def test_team_shell_is_nodata_and_a_team_page_is_stamped(site):
    site.pages[f"{HOST}/en/teams/460279/"] = "team_460279.html"
    rec = R._get(f"{HOST}/en/teams/460279/", {})
    assert P.parse_vhl_roster(rec)["team_page_id"].unique().to_list() == [460279]
    site.pages[f"{HOST}/en/teams/1/"] = _Resp(200, "<html><body><h1>Teams</h1></body></html>")
    with pytest.raises(NoDataError, match="not a team page"):
        R._get(f"{HOST}/en/teams/1/", {})


@pytest.mark.parametrize(
    ("resp", "error", "match"),
    [
        (_Resp(404, "nope"), NoDataError, "404"),
        (_Resp(400, "bad"), ValueError, "400"),
        (_Resp(403, "denied"), AssetFetchError, "403"),
        (_Resp(204, ""), AssetFetchError, "not 200"),
        (_Resp(200, '{"status": "error"}', ctype="application/json"), AssetFetchError, "non-HTML"),
        (_Resp(200, "   "), AssetFetchError, "empty body"),
        (_Resp(200, "Торос".encode("cp1251")), AssetFetchError, "not valid 'UTF-8'"),
        (_Resp(200, "x", ctype="text/html; charset=nonsense"), AssetFetchError, "not valid"),
        (_Resp(200, "ok \ufffd"), AssetFetchError, "replacement"),
        (_Resp(200, "<html></html>", url=f"{HOST}/404.php"), NoDataError, "redirected"),
        (_Resp(200, "<html></html>", url=f"{HOST}/en/"), AssetFetchError, "redirected"),
        (_Resp(200, "<html><body><table></table></body></html>"), AssetFetchError, "not the page asked for"),
    ],
)
def test_runtime_refusals(site, resp, error, match):
    site.pages[f"{HOST}/en/players/34418/"] = resp
    with pytest.raises(error, match=match):
        R._get(f"{HOST}/en/players/34418/", {})


def test_broadcast_redirect_to_the_online_index_is_nodata(site):
    url = "https://online.vhlru.ru/online/899223.html"
    site.pages.update({CAL: "calendar_1374.html", url: _Resp(200, "<html></html>", url="https://online.vhlru.ru/")})
    with pytest.raises(NoDataError, match="redirected"):
        R._get(url, {"season": 2026})


def test_a_truncated_page_is_refused(site):
    page = _html(REPORT)
    site.pages.update({CAL: "calendar_1374.html", REP: _Resp(200, page[: len(page) // 2])})
    with pytest.raises(AssetFetchError, match="not the page asked for"):
        R._get(f"{HOST}/en/report/2026/", {"idgame": 899223})


def test_retries_5xx_and_transport_errors_then_gives_up(site):
    url = f"{HOST}/en/players/34418/"
    site.pages[url] = [_Resp(503, "busy"), ConnectionError("reset"), "player_34418.html"]
    assert R._get(url, {})["player_id"] == 34418
    assert site.calls.count(url) == 3 and site.waited.count(url) == 3
    site.pages[url] = [ConnectionError("reset")]
    with pytest.raises(AssetFetchError, match="after 3 attempts"):
        R._get(url, {})
    site.pages[url] = [_Resp(502, "bad gateway")]
    with pytest.raises(AssetFetchError, match="502"):
        R._get(url, {})


@pytest.mark.parametrize("status", [429, 503])
def test_retry_after_is_honoured_on_429_and_503(site, monkeypatch, status):
    slept: List[float] = []
    monkeypatch.setattr(euro_hockey_runtime.time, "sleep", slept.append)
    url = f"{HOST}/en/players/34418/"
    busy = _Resp(status, "busy")
    busy.headers["retry-after"] = "30"
    site.pages[url] = [busy, "player_34418.html"]
    assert R._get(url, {})["player_id"] == 34418
    assert slept == [30.0]  # Retry-After over the 2 s backoff
    huge = _Resp(status, "busy")
    huge.headers["retry-after"] = "86400"
    site.pages[url] = [huge, "player_34418.html"]
    slept.clear()
    R._get(url, {})
    assert slept == [euro_hockey_runtime._MAX_RETRY_AFTER]


def test_bad_arguments_are_refused(site):
    with pytest.raises(TypeError, match="unsupported"):
        R._get(f"{HOST}/en/players/1/", {}, timeout=5)
    with pytest.raises(ValueError, match="idgame"):
        R._get(f"{HOST}/en/report/2026/", {})
    with pytest.raises(ValueError, match="not a VHL route"):
        R._get(f"{HOST}/en/standings/regular/2026/", {})
    assert site.calls == []


def test_transport_pins_only_the_www_host(monkeypatch):
    curl_cffi = pytest.importorskip("curl_cffi")
    seen: List[Any] = []
    monkeypatch.setattr(curl_cffi.requests, "get", lambda url, **kw: seen.append((url, kw)))
    R._transport(f"{HOST}/en/players/1/")
    R._transport("https://online.vhlru.ru/online/1.html")
    (www, www_kw), (online, online_kw) = seen
    assert www_kw["curl_options"] == {curl_cffi.CurlOpt.RESOLVE: ["www.vhlru.ru:443:188.128.49.29"]}
    assert online_kw["curl_options"] is None and www_kw["impersonate"] == "chrome"
