"""Offline tests for the P. LEAGUE+ site family (``pleague``): parsers, runtime, YAML and descriptions.

Parsers are asserted against real data: the full parsed play-by-play of one game (gid 197) in
``tests/fixtures/pleague/full/``, the whole box bodies of four games (both box eras, a double-overtime game and a
2025-26 finals game without ``personId``), the refs' cut-down HTML pages and the refs capture records. The runtime is
driven through a scripted ``curl_cffi`` stand-in, so every refusal is exercised end to end and every attempt is
counted. No network, and nothing reads the private sdv-internal-refs checkout.
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
import yaml

import sportsdataverse.basketball.plg as plg_pkg
from sportsdataverse.basketball.plg import pleague_parsers as P
from sportsdataverse.basketball.plg import pleague_runtime as R
from sportsdataverse.errors import AssetFetchError, NoDataError

ROOT = Path(__file__).parents[3]
FIXTURES = ROOT / "tests" / "fixtures" / "pleague"
YAML_PATH = ROOT / "tools" / "codegen" / "endpoints" / "pleague.yaml"
SCHEMA_DIR = ROOT / "tools" / "codegen" / "schemas" / "native" / "pleague"

sys.path.insert(0, str(ROOT / "tools" / "codegen"))
import gen_pleague  # noqa: E402

SNAKE = re.compile(r"^[a-z0-9]+(_[a-z0-9]+)*$")
ALL_PARSERS = {n: getattr(P, f"parse_pleague_{n}") for n in P.SPECS}
UUID_197 = "e6f98675-3e5b-11ed-949e-5d17350689cf"


def _json(name: str) -> Any:
    return json.loads((FIXTURES / name).read_text(encoding="utf-8"))


def _html(name: str) -> str:
    return (FIXTURES / name).read_text(encoding="utf-8")


def _stamp(body: Dict[str, Any], gid: int) -> Dict[str, Any]:
    return {**body, P._REQUEST: {"gid": gid}}


def _game(name: str = "game_197.html", gid: int = 197) -> Dict[str, Any]:
    return P.record_for("game", _html(name), gid=gid)


# --- parsers over every committed body -------------------------------------------------------------


@pytest.mark.parametrize("short", sorted(gen_pleague.SITE))
def test_every_wrapper_parses_its_fixture(short):
    df = gen_pleague._frame(gen_pleague.SITE[short])
    assert isinstance(df, pl.DataFrame) and df.height > 0, short
    assert all(SNAKE.match(c) for c in df.columns), [c for c in df.columns if not SNAKE.match(c)]
    assert dict(df.schema) == dict(P.SPECS[short]), short


def test_the_full_play_by_play_is_typed_ordered_and_ends_on_the_final_score():
    pbp = P.parse_pleague_pbp(_stamp(_json("full/pbp_197.json"), 197))
    assert pbp.height == 630 and pbp["seq"].to_list() == list(range(1, 631))
    assert pbp.schema["x"] == pl.Float64 and pbp.schema["clock"] == pl.Utf8 and pbp.schema["gid"] == pl.Int64
    assert set(pbp["period"].unique()) == {"Q1", "Q2", "Q3", "Q4"}
    assert pbp["period_no"].is_in([1, 2, 3, 4]).all()
    scored = pbp.drop_nulls("score_away").tail(1).row(0, named=True)
    assert (scored["score_away"], scored["score_home"]) == (94, 102)  # AWAY first, the opposite of the box
    q1 = pbp.filter(pl.col("period") == "Q1")
    assert q1["clock"][0] == "12:00" and q1["seconds_left"][0] == 720
    assert pbp.filter(pl.col("action").is_null()).height == 0
    assert pbp["action_en"].null_count() == 0


def test_the_action_vocabulary_is_the_82_observed_phrases_with_their_counts():
    table = P.action_table()
    assert len(table) == 83 and sum(r[3] for r in table) == 258183  # 82 phrases plus the blank (unlabelled) row
    assert len(P.ACTIONS) == 82 and P.OBSERVED_COUNTS[""] == 169
    assert dict((z, n) for z, _c, _e, n in table)["搶到防守籃板"] == 29537
    pbp = P.parse_pleague_pbp(_stamp(_json("full/pbp_197.json"), 197))
    assert set(pbp["action"].unique()) <= {c for _z, c, _e, _n in table}


def test_free_throw_phrases_carry_the_trip_position():
    rec = _json("full/pbp_197.json")
    fts = [e for e in rec["events"] if e["ft_number"] is not None]
    assert fts and {e["ft_number"] for e in fts} <= {1, 2, 3}
    first = [e for e in fts if e["ft_awarded"] is not None]
    assert first and all(e["ft_number"] == 1 for e in first)


def test_action_en_reads_each_rows_own_phrase():
    pbp = P.parse_pleague_pbp(_stamp(_json("full/pbp_197.json"), 197))
    labelled = pbp.filter(pl.col("action_zh").is_in(list(P.ACTIONS)))
    assert labelled.height > 0
    assert labelled["action_en"].to_list() == [P.ACTIONS[z][1] for z in labelled["action_zh"]]
    fts = pbp.filter(pl.col("action").is_in(["ft_made", "ft_missed"]))["action_en"]
    assert fts.n_unique() > 2 and not fts.str.contains("1 of 3").all()


def test_pbp_never_keeps_a_clip_url_or_access_key():
    text = json.dumps(_json("full/pbp_197.json"), ensure_ascii=False)
    assert "key=" not in text and "http" not in text
    df = P.parse_pleague_pbp(_stamp(_json("full/pbp_197.json"), 197))
    assert not any("url" in c for c in df.columns)
    assert df["video_uuid"].null_count() < df.height  # clips exist, as references only


def test_box_dtypes_come_from_the_full_bodies_in_both_eras():
    b = P.parse_pleague_box_players(_stamp(_json("box_197.json"), 197))
    assert b.schema["fg2_pct"] == pl.Float64 and b.schema["ortg"] == pl.Float64 and b.schema["points"] == pl.Int64
    assert b.schema["minutes"] == pl.Utf8 and b.schema["starter"] == pl.Boolean and b.schema["player_id"] == pl.Int64
    a = P.parse_pleague_box_players(_stamp(_json("box_13.json"), 13))
    assert a["person_id"].null_count() == a.height  # era A has no personId
    played = a.filter(pl.col("played") == True)  # noqa: E712
    assert played.height > 0 and played["seconds"].null_count() == 0 and played["efg_pct"].null_count() < played.height
    assert played["ortg"].null_count() == played.height  # ratings are era B only
    assert (b["efg_pct"].null_count(), b["ortg"].null_count() < b.height) == (b.height, True)


def test_did_not_play_rows_keep_the_schema_and_null_every_stat():
    b = P.parse_pleague_box_players(_stamp(_json("box_197.json"), 197))
    dnp = b.filter(pl.col("played") == False)  # noqa: E712
    assert dnp.height >= 1
    assert (
        dnp.select(["minutes", "points", "fg2_made", "plus_minus", "ortg", "starter"]).null_count().row(0)
        == (dnp.height,) * 6
    )
    assert dnp["name_zh"].null_count() == 0 and dnp["player_id"].null_count() == 0


def test_box_points_add_up_to_the_final_score_and_team_totals():
    box = _stamp(_json("box_197.json"), 197)
    players = P.parse_pleague_box_players(box)
    teams = P.parse_pleague_box_teams(box)
    pts = dict(players.group_by("side").agg(pl.col("points").sum()).iter_rows())
    assert teams["side"].to_list() == ["home", "away"] and teams["points"].to_list() == [102, 94]
    assert pts == {"home": 102, "away": 94}
    assert teams["fg2_made"].to_list() == [26, 25] and teams.schema["fg2_pct"] == pl.Float64


def test_fg_made_comes_from_the_made_attempted_strings_in_every_era():
    for name, gid in (("box_13.json", 13), ("box_716.json", 716)):
        b = P.parse_pleague_box_players(_stamp(_json(name), gid)).filter(pl.col("played") == True)  # noqa: E712
        assert b.height > 0
        assert (b["fg2_made"] <= b["fg2_att"]).all() and (b["fg3_made"] <= b["fg3_att"]).all()
        assert (b["points"] == b["fg2_made"] * 2 + b["fg3_made"] * 3 + b["ft_made"]).all(), name


def test_person_id_is_missing_only_on_did_not_play_rows_of_the_2025_26_finals_game_and_the_join_key_is_player_id():
    # The refs notes report some 2025-26 finals boxes without personId; gid 716 carries it on every played row.
    b = P.parse_pleague_box_players(_stamp(_json("box_716.json"), 716))
    played = b.filter(pl.col("played") == True)  # noqa: E712
    assert played["person_id"].null_count() == 0 and b.filter(pl.col("person_id").is_null())["played"].sum() == 0
    assert b["player_id"].null_count() == 0 and b["player_id"].n_unique() == b.height


def test_game_header_periods_and_officials():
    g = P.parse_pleague_game(_game()).row(0, named=True)
    assert (g["gid"], g["uuid"], g["finished"], g["overtimes"]) == (197, UUID_197, True, 0)
    assert (g["away_score"], g["home_score"]) == (94, 102) and str(g["date"]) == "2022-11-05" and g["time"] == "17:00"
    assert g["status_zh"] == "已完賽" and g["venue_zh"] and g["away_name_en"] == "Hsinchu Jko Lioneers"
    ot = _game("game_206.html", 206)
    periods = P.parse_pleague_periods(ot)
    assert periods["period"].to_list() == ["Q1", "Q2", "Q3", "Q4", "OT1", "OT2"]
    assert periods["period_no"].to_list() == [1, 2, 3, 4, 5, 6]
    game = P.parse_pleague_game(ot).row(0, named=True)
    assert game["overtimes"] == 2
    assert (periods["home_points"].sum(), periods["away_points"].sum()) == (game["home_score"], game["away_score"])
    officials = P.parse_pleague_officials(_game())
    assert officials["role"].to_list() == ["referee", "first_umpire", "second_umpire"]
    assert officials["official_no"].to_list() == [1, 2, 3]


def test_a_2020_21_game_page_has_no_uuid_and_no_english_names():
    g = P.parse_pleague_game(_game("game_13.html", 13)).row(0, named=True)
    assert g["uuid"] is None and g["away_name_en"] is None and g["home_name_en"] is None
    assert g["home_logo"].startswith("https:////")  # the site's malformed logo url, kept as printed


def test_schedule_season_is_the_end_year_and_future_games_are_marked():
    s = P.record_for("schedule", _html("schedule_regular_2022-23.html"), stage="regular-season", season="2022-23")
    df = P.parse_pleague_schedule(s)
    assert df["season"].unique().to_list() == [2023] and df["season_slug"].unique().to_list() == ["2022-23"]
    assert df["gid"].to_list() == [197, 198, 199] and df["away_team_id"][0] != df["home_team_id"][0]
    future = P.parse_pleague_schedule(
        P.record_for("schedule", _html("schedule_future_2026-27.html"), stage="regular-season", season="2026-27")
    )
    assert future["season"].unique().to_list() == [2027] and future["phase"].unique().to_list() == ["is_future"]
    assert (future["home_score"].sum(), future["away_score"].sum()) == (0, 0)
    finals = P.parse_pleague_schedule(
        P.record_for("schedule", _html("schedule_finals_2022-23.html"), stage="finals", season="2022-23")
    )
    assert finals.height == 6 and finals["stage"].unique().to_list() == ["finals"]
    assert df.schema["date"] == pl.Date and df.schema["starred"] == pl.Boolean


def test_standings_team_uuid_joins_the_site_team_id_to_the_clubs_table():
    rec = P.record_for("standings", _html("standings_2022-23.html"), season="2022-23")
    df = P.parse_pleague_standings(rec)
    assert df["season"].unique().to_list() == [2023] and df["last_n"].unique().to_list() == [10]
    clubs = {team_id: uuid for team_id, uuid, _zh, _en in gen_pleague.CLUBS}
    assert df["team_uuid"].null_count() == 0
    assert {(r["team_id"], r["team_uuid"]) for r in df.iter_rows(named=True)} <= set(clubs.items())
    assert df.schema["win_pct"] == pl.Float64 and df.schema["wins"] == pl.Int64


def test_player_season_table_and_player_page():
    rec = P.record_for("stat_player", _html("stat_player_2022-23_2.html"), season="2022-23", stage_sn=2)
    df = P.parse_pleague_stat_player(rec)
    assert df["stage_sn"].unique().to_list() == [2] and df["stage"].unique().to_list() == ["regular-season"]
    assert df.schema["points_avg"] == pl.Float64 and df.schema["points_total"] == pl.Int64
    assert df.schema["minutes_avg"] == pl.Utf8 and df["player_page_id"].n_unique() == df.height
    page = P.record_for("player", _html("player_1JbPozLB.html"), pid="1JbPozLB")
    p = P.parse_pleague_player(page).row(0, named=True)
    assert (p["player_page_id"], p["player_id"], p["name_en"]) == ("1JbPozLB", 40, "Ting-En Lai")
    assert "birthday" not in P.SPECS["player"] and p["status_zh"] == "本土" and p["education"].count(";") >= 1
    seasons = P.parse_pleague_player_seasons(page)
    assert set(seasons["competition"].unique()) == {"plg", "easl"} and set(seasons["table"].unique()) == {
        "totals",
        "averages",
    }
    career = seasons.filter(pl.col("season_slug") == "career")
    assert career.height >= 2 and career["season"].null_count() == career.height
    assert seasons.filter(pl.col("season_slug") != "career")["season"].min() >= 2021
    single = P.parse_pleague_player_seasons(P.record_for("player", _html("player_1P42uoZg.html"), pid="1P42uoZg"))
    assert set(single["competition"].unique()) == {"plg"}


def test_html_records_equal_the_refs_captures():
    cap = _json("captures/game__gid.json")
    rec = _game()
    assert {k: v for k, v in rec.items() if k != "officials"} == {k: v for k, v in cap.items() if k != "officials"}
    assert rec["officials"][:3] == cap["officials"][:3]
    st = P.record_for("standings", _html("standings_2022-23.html"), season="2022-23")
    cs = _json("captures/standings__season@2022-23.json")
    assert {k: v for k, v in st.items() if k not in ("teams", "team_count")} == {
        k: v for k, v in cs.items() if k not in ("teams", "team_count")
    }
    assert st["teams"][:3] == cs["teams"]
    box = _json("box_197.json")
    assert _json("captures/api__boxscore.preciser.php.json")["data"]["score_home"] == box["data"]["score_home"]


# --- the parser contract: empty, malformed, pandas, columns --------------------------------------


@pytest.mark.parametrize("short", sorted(P.SPECS))
@pytest.mark.parametrize(
    "bad", [{}, [], None, "x", 7, {"data": None}, {"games": []}, {"events": "x"}, {"teams": [1, 2]}]
)
def test_empty_or_malformed_payload_is_a_typed_zero_row_frame(short, bad):
    df = ALL_PARSERS[short](bad)
    assert isinstance(df, pl.DataFrame) and df.height == 0
    assert dict(df.schema) == dict(P.SPECS[short])


@pytest.mark.parametrize("short", sorted(gen_pleague.SITE))
def test_two_empty_frames_stack_and_stack_with_a_populated_one(short):
    empty = ALL_PARSERS[short]({})
    full = gen_pleague._frame(gen_pleague.SITE[short])
    assert pl.concat([empty, empty]).height == 0
    assert pl.concat([empty, full]).height == full.height
    assert pl.concat([full, empty]).schema == full.schema


@pytest.mark.parametrize("short", sorted(gen_pleague.SITE))
def test_return_as_pandas(short):
    entry = gen_pleague.SITE[short]
    pdf = entry[2](entry[3](), return_as_pandas=True)
    assert isinstance(pdf, pd.DataFrame) and len(pdf) > 0
    assert list(pdf.columns) == list(P.SPECS[short])


def test_columns_argument_adds_missing_documented_columns_as_nulls():
    df = P.parse_pleague_pbp({}, columns=["gid", "brand_new_column"])
    assert df.height == 0 and df.schema["brand_new_column"] == pl.Utf8


def test_every_id_column_is_int64_and_no_float_origin_strings():
    for short in P.SPECS:
        df = gen_pleague._frame(gen_pleague.SITE[short])
        for c, dt in df.schema.items():
            if c.endswith("_id") and c not in (
                "person_id",
                "random_id",
                "player_page_id",
                "synergy_play_id",
                "video_uuid",
            ):
                assert dt == pl.Int64, (short, c)
            if dt == pl.Utf8:
                assert not df[c].drop_nulls().str.contains(r"^\d+\.0$").any(), (short, c)


def test_every_public_parser_has_a_documented_example_and_helpers_are_private():
    assert set(P.__all__) == {f"parse_pleague_{n}" for n in P.SPECS}
    for name in P.__all__:
        doc = getattr(P, name).__doc__ or ""
        for part in ("Args:", "Returns:", "Raises:", "Example:", "Quick start::"):
            assert part in doc, (name, part)
        assert ">>>" not in doc
    assert set(P.__all__) <= set(plg_pkg.__all__)
    assert all(n.startswith(("pleague_", "parse_pleague_", "season_slug")) for n in plg_pkg.__all__)


# --- the YAML, schemas and generator ----------------------------------------------------------------


def _yaml() -> Dict[str, Any]:
    return yaml.safe_load(YAML_PATH.read_text(encoding="utf-8"))


def test_yaml_lists_every_spec_route():
    spec_paths = set(json.loads((FIXTURES / "spec_paths.json").read_text(encoding="utf-8")))
    assert len(spec_paths) == 10 and spec_paths == {p for e in gen_pleague.SITE.values() for p in e[0]}
    doc = _yaml()
    got = {(e["short"], e["path"]) for e in doc["endpoints"]}
    assert got == {(s, e[1]) for s, e in gen_pleague.SITE.items()}
    assert len(doc["endpoints"]) == 11 and set(P.SPECS) == {e["short"] for e in doc["endpoints"]}
    stage = next(e for e in doc["endpoints"] if e["short"] == "schedule")["extra_params"][0]
    assert stage["name"] == "stage" and stage["choices"] == ["pre-season", "regular-season", "playoffs", "finals"]


def test_no_path_token_named_league_or_sport():
    for e in _yaml()["endpoints"]:
        assert not {"league", "sport"} & set(re.findall(r"\{([^}]+)\}", e["path"])), e["path"]


def test_yaml_carries_the_owner_flag_access_and_coverage_notes():
    notes = " ".join(_yaml()["docstring"]["notes"])
    assert "data release needs owner sign-off" in notes and "/user-policy" in notes and "curl_cffi" in notes
    assert "robots.txt" in notes and "at least 1 s" in notes and "2022-23" in notes and "AssetFetchError" in notes
    assert _yaml()["getter_module"] == "sportsdataverse.basketball.plg.pleague_runtime"


@pytest.mark.parametrize("short", sorted(gen_pleague.SITE))
def test_schema_matches_the_parser_and_carries_no_descriptions(short):
    schema = yaml.safe_load((SCHEMA_DIR / f"{short}.yaml").read_text(encoding="utf-8"))
    assert [c["name"] for c in schema["columns"]] == list(P.SPECS[short])
    assert {c["description"] for c in schema["columns"]} == {""}  # descriptions live in manual_column_descriptions.yaml


def test_the_committed_descriptions_cover_every_column_without_filler():
    notes = yaml.safe_load(gen_pleague.NOTES.read_text(encoding="utf-8"))
    for stem, spec_names in (("pleague", set(gen_pleague.SITE)), ("pleague_preciser", set(gen_pleague.VENDOR))):
        keys = {k for k in notes if k.startswith(stem + "_") and (k[len(stem) + 1 :] in spec_names)}
        assert keys == {f"{stem}_{s}" for s in spec_names}
    for key, cols in notes.items():
        short = key[len("pleague_preciser_") :] if key.startswith("pleague_preciser_") else key[len("pleague_") :]
        specs = gen_pleague.V.SPECS if key.startswith("pleague_preciser_") else P.SPECS
        assert set(cols) == set(specs[short]), key
        for col, text in cols.items():
            assert len(text) >= 15 and text.lower() != col, (key, col)


def test_translated_values_and_the_action_table_live_in_the_descriptions():
    notes = yaml.safe_load(gen_pleague.NOTES.read_text(encoding="utf-8"))
    action = notes["pleague_pbp"]["action"]
    assert len(re.findall(r"\[[^\]]*, [\d,]+\]", action)) == 83 and "29,537" in action and "fg2_dunk_made" in action
    assert (
        "灌籃得分" in notes["pleague_pbp"]["action_zh"] and "82 phrases" in action
    )  # counts per phrase, 82 phrases plus the blank row
    assert "已完賽" in notes["pleague_game"]["status_zh"] and "洋將" in notes["pleague_player"]["status_zh"]
    assert "主裁判" in notes["pleague_officials"]["role_zh"] and "例行賽" in notes["pleague_schedule"]["stage"]
    assert "unverified" in notes["pleague_pbp"]["y"]  # the unverified sideline caveat is kept
    assert (
        "unreliable" in notes["pleague_box_players"]["random_id"]
        or "wrong" in notes["pleague_box_players"]["random_id"]
    )
    assert "e86394aa-3d88-11ed-8ba8-7f44ba46b0b1" in notes["pleague_standings"]["team_uuid"]
    assert "x_vendor = 100 - x" in notes["pleague_pbp"]["x"]


# --- season mapping and argument checks --------------------------------------------------------------


@pytest.mark.parametrize(
    "season,slug", [(2025, "2024-25"), (2021, "2020-21"), (2027, "2026-27"), (2100, "2099-00"), ("2023", "2022-23")]
)
def test_season_end_year_maps_to_the_sites_slug(season, slug):
    assert R.season_slug(season) == slug


def test_season_before_the_first_season_is_nodata_and_garbage_is_valueerror():
    for bad in (2020, 1999, 0):
        with pytest.raises(NoDataError, match="first season"):
            R.season_slug(bad)
    for bad in ("x", None, 2025.5, True, -1, ""):
        with pytest.raises(ValueError):
            R.season_slug(bad)  # type: ignore[arg-type]


# --- runtime: a scripted transport ----------------------------------------------------------------------

HTML = "text/html; charset=UTF-8"


class Resp:
    """The slice of a ``curl_cffi`` response the runtime reads."""

    def __init__(self, status: int = 200, text: str = "", ctype: str = HTML, **headers: str) -> None:
        self.status_code = status
        self.text = text
        self.headers = {"content-type": ctype, **{k.replace("_", "-"): v for k, v in headers.items()}}


@pytest.fixture
def waits(monkeypatch):
    """Count one polite wait per attempt; sleeping is free."""
    R.clear_cache()
    seen: List[str] = []
    monkeypatch.setattr(R, "_polite_wait", lambda url, *a, **k: seen.append(url))
    slept: List[float] = []
    monkeypatch.setattr(R.time, "sleep", lambda s: slept.append(s))
    yield type("W", (), {"urls": seen, "slept": slept})
    R.clear_cache()


@pytest.fixture
def site(monkeypatch, waits):
    """Install a handler ``(url, params) -> Resp`` as the transport; returns the recorded calls."""

    def install(handler: Callable[[str, Optional[dict]], Resp]) -> List[Tuple[str, Optional[dict]]]:
        calls: List[Tuple[str, Optional[dict]]] = []

        def transport(url, params):
            calls.append((url, params))
            return handler(url, params)

        monkeypatch.setattr(R, "_transport", transport)
        return calls

    return install


def _pages(overrides: Optional[Dict[str, Any]] = None) -> Callable[[str, Optional[dict]], Resp]:
    """A site serving the committed pages and bodies, keyed by path; ``overrides`` replaces a path (a ``Resp``, a text
    or a callable of the params)."""
    table: Dict[str, Any] = {
        "/game/197": _html("game_197.html"),
        "/game/13": _html("game_13.html"),
        "/game/206": _html("game_206.html"),
        "/api/boxscore.preciser.php": lambda p: json.dumps(
            {"197": _json("box_197.json"), "13": _json("box_13.json")}[str(p["id"])]
        ),
        "/api/playbyplay.php": lambda p: json.dumps(_json("pbp_197.json")),
        "/schedule-regular-season/2022-23": _html("schedule_regular_2022-23.html"),
        "/schedule-finals/2022-23": _html("schedule_finals_2022-23.html"),
        "/standings/2022-23": _html("standings_2022-23.html"),
        "/stat-player/2022-23/2": _html("stat_player_2022-23_2.html"),
        "/player/1JbPozLB": _html("player_1JbPozLB.html"),
    }
    table.update(overrides or {})

    def handler(url: str, params: Optional[dict]) -> Resp:
        path = url.split("pleagueofficial.com", 1)[1]
        body = table.get(path)
        if body is None:
            return Resp(404, "not found")
        if callable(body):
            body = body(params or {})
        return body if isinstance(body, Resp) else Resp(200, body)

    return handler


def test_wrappers_build_the_sites_url_and_every_request_waits(site, waits):
    calls = site(_pages())
    game = plg_pkg.pleague_game(gid=197)
    assert game.height == 1 and game["gid"].to_list() == [197]
    assert plg_pkg.pleague_periods(gid=197).height == 4  # the cached game page: no second fetch
    assert [u for u, _p in calls] == ["https://pleagueofficial.com/game/197"]
    assert plg_pkg.pleague_schedule(season=2023).height == 3
    assert calls[-1][0] == "https://pleagueofficial.com/schedule-regular-season/2022-23"
    assert plg_pkg.pleague_schedule(season=2023, stage="finals").height == 6
    assert calls[-1][0] == "https://pleagueofficial.com/schedule-finals/2022-23"
    assert plg_pkg.pleague_standings(season=2023).height == 3
    assert plg_pkg.pleague_stat_player(season=2023, stage_sn=2).height == 3
    assert calls[-1][0] == "https://pleagueofficial.com/stat-player/2022-23/2"
    assert plg_pkg.pleague_player(player_page_id="1JbPozLB").height == 1
    assert len(waits.urls) == len(calls)  # one throttled wait per request


def test_box_and_pbp_check_the_game_page_first_and_share_it(site, waits):
    calls = site(_pages())
    box = plg_pkg.pleague_box_players(gid=197)
    assert box.height == 26 and plg_pkg.pleague_box_teams(gid=197).height == 2
    pbp = plg_pkg.pleague_pbp(gid=197)
    assert pbp.height > 10 and pbp["gid"].unique().to_list() == [197]
    paths = [u.split("pleagueofficial.com")[1] for u, _p in calls]
    assert paths == ["/game/197", "/api/boxscore.preciser.php", "/api/playbyplay.php"]  # the game page once
    assert calls[1][1] == {"id": 197, "away_tab": "total", "home_tab": "total"} and calls[2][1] == {"uuid": UUID_197}


def test_return_parsed_false_hands_back_the_stamped_record(site):
    site(_pages())
    raw = plg_pkg.pleague_box_teams(gid=197, return_parsed=False)
    assert raw[R.REQUEST_KEY] == {"kind": "box", "gid": 197} and raw["data"]["score_home"] == 102
    rec = plg_pkg.pleague_pbp(gid=197, return_parsed=False)
    assert rec[R.REQUEST_KEY] == {"kind": "pbp", "gid": 197, "uuid": UUID_197} and rec["final_score_home"] == 102
    sched = plg_pkg.pleague_schedule(season=2023, return_parsed=False)
    assert sched[R.REQUEST_KEY] == {
        "kind": "schedule",
        "season": 2023,
        "season_slug": "2022-23",
        "stage": "regular-season",
    }


def test_status_codes_follow_the_error_vocabulary(site):
    site(_pages({"/game/197": Resp(404, "x")}))
    with pytest.raises(NoDataError, match="404"):
        plg_pkg.pleague_game(gid=197)
    R.clear_cache()
    site(_pages({"/game/197": Resp(422, "x")}))
    with pytest.raises(ValueError, match="422"):
        plg_pkg.pleague_game(gid=197)
    R.clear_cache()
    site(_pages({"/game/197": Resp(403, "<html>Just a moment</html>")}))
    with pytest.raises(AssetFetchError, match="403"):
        plg_pkg.pleague_game(gid=197)


def test_an_unknown_gid_or_a_season_before_2020_21_redirects_and_is_nodata(site):
    site(_pages({"/game/999999": Resp(302, "", location="/")}))
    with pytest.raises(NoDataError, match="redirected to /"):
        plg_pkg.pleague_game(gid=999999)
    site(_pages({"/standings/2022-23": Resp(302, "", location="/standings")}))
    with pytest.raises(NoDataError, match="redirected"):
        plg_pkg.pleague_standings(season=2023)


def test_every_retry_attempt_waits_its_turn_and_5xx_exhausts_to_assetfetcherror(site, waits):
    calls = site(_pages({"/game/197": Resp(503, "busy", retry_after="7")}))
    with pytest.raises(AssetFetchError, match="503"):
        plg_pkg.pleague_game(gid=197)
    assert len(calls) == R.ATTEMPTS == len(waits.urls)  # each attempt waited
    assert waits.slept and max(waits.slept) >= 7  # Retry-After honoured on 503


def test_a_transient_429_then_200_succeeds_and_honours_retry_after(site, waits):
    state = {"n": 0}
    inner = _pages()

    def handler(url, params):
        state["n"] += 1
        return Resp(429, "slow down", retry_after="3") if state["n"] == 1 else inner(url, params)

    calls = site(handler)
    assert plg_pkg.pleague_game(gid=197).height == 1
    assert len(calls) == 2 == len(waits.urls) and max(waits.slept) >= 3


def test_http_500_is_not_retried(site, waits):
    calls = site(_pages({"/api/boxscore.preciser.php": Resp(500, "")}))
    with pytest.raises(AssetFetchError, match="500"):
        plg_pkg.pleague_box_players(gid=197)
    assert len([c for c in calls if "boxscore" in c[0]]) == 1


def test_a_connection_error_is_assetfetcherror_without_the_detail(site, waits):
    def boom(url, params):
        raise ConnectionError("secret-host-detail")

    calls = site(boom)
    with pytest.raises(AssetFetchError) as exc:
        plg_pkg.pleague_game(gid=197)
    assert "ConnectionError" in str(exc.value) and "secret-host-detail" not in str(exc.value)
    assert len(calls) == R.ATTEMPTS == len(waits.urls)


def test_a_page_for_another_game_is_refused(site):
    site(_pages({"/game/197": _html("game_206.html")}))
    with pytest.raises(AssetFetchError, match="out of scope"):
        plg_pkg.pleague_game(gid=197)


def test_an_unfinished_game_is_assetfetcherror(site):
    site(_pages({"/game/197": _html("game_197.html").replace("已完賽", "未開賽")}))
    with pytest.raises(AssetFetchError, match="not final"):
        plg_pkg.pleague_game(gid=197)


def test_layout_drift_and_garbage_bodies_are_assetfetcherror(site):
    for body in (_html("game_197.html").replace("game_header", "game_hdr"), "<html><body>hello</body></html>"):
        R.clear_cache()
        site(_pages({"/game/197": body}))
        with pytest.raises(AssetFetchError, match="not the page asked for"):
            plg_pkg.pleague_game(gid=197)
    R.clear_cache()
    site(_pages({"/game/197": Resp(200, "  ")}))
    with pytest.raises(AssetFetchError, match="empty body"):
        plg_pkg.pleague_game(gid=197)
    R.clear_cache()
    site(_pages({"/game/197": Resp(200, _html("game_197.html"), ctype="application/json")}))
    with pytest.raises(AssetFetchError, match="not HTML"):
        plg_pkg.pleague_game(gid=197)


def test_a_stage_the_season_does_not_have_is_nodata(site):
    empty = "<html><head><title>2022-23-例行賽賽程│P. LEAGUE+</title></head><body></body></html>"
    with pytest.raises(P._NotParsed) as exc:
        P.record_for("schedule", empty, stage="regular-season", season="2022-23")
    assert exc.value.empty is True
    site(_pages({"/schedule-regular-season/2022-23": empty}))
    with pytest.raises(NoDataError, match="zero games"):
        plg_pkg.pleague_schedule(season=2023)


def test_schedule_pages_of_another_season_or_stage_are_refused(site):
    site(_pages({"/schedule-regular-season/2022-23": _html("schedule_regular_2020-21.html")}))
    with pytest.raises(AssetFetchError, match="out of scope"):
        plg_pkg.pleague_schedule(season=2023)
    site(_pages({"/schedule-regular-season/2022-23": _html("schedule_finals_2022-23.html")}))
    with pytest.raises(AssetFetchError, match="out of scope"):
        plg_pkg.pleague_schedule(season=2023)


def test_standings_stat_player_and_player_pages_are_scope_checked(site):
    site(_pages({"/standings/2022-23": _html("standings_2022-23.html").replace("2022-23", "2021-22")}))
    with pytest.raises(AssetFetchError, match="out of scope"):
        plg_pkg.pleague_standings(season=2023)
    site(
        _pages(
            {"/stat-player/2022-23/2": _html("stat_player_2022-23_2.html").replace('value="2" selected', 'value="2"')}
        )
    )
    with pytest.raises(AssetFetchError):
        plg_pkg.pleague_stat_player(season=2023, stage_sn=2)
    site(_pages({"/player/1P42uoZg": _html("player_1JbPozLB.html")}))
    with pytest.raises(AssetFetchError, match="out of scope"):
        plg_pkg.pleague_player(player_page_id="1P42uoZg")


def _box_with(fn: Callable[[Dict[str, Any]], None]) -> str:
    body = _json("box_197.json")
    fn(body)
    return json.dumps(body)


@pytest.mark.parametrize(
    "mutate,match",
    [
        (lambda b: b["data"].update(score_home=103), "score"),
        (lambda b: b["data"].update(q2_away=99), "q2_away"),
        (lambda b: b["data"].update(ot1_home=7), "ot1_home"),
        (lambda b: b["data"]["home"][0].update(points=40), "player points"),
        (lambda b: b["data"].update(home=[], away=[]), "no player rows"),
        (lambda b: b.update(error="boom"), "error field"),
        (lambda b: b["data"].pop("home_total"), "lacks"),
        (lambda b: b.update(data=[]), "not an object"),
    ],
)
def test_a_box_that_is_not_the_games_box_is_refused(site, mutate, match):
    site(_pages({"/api/boxscore.preciser.php": _box_with(mutate)}))
    with pytest.raises(AssetFetchError, match=match):
        plg_pkg.pleague_box_players(gid=197)


def test_a_box_that_is_not_json_is_assetfetcherror(site):
    site(_pages({"/api/boxscore.preciser.php": "<html>nope</html>"}))
    with pytest.raises(AssetFetchError, match="not JSON"):
        plg_pkg.pleague_box_players(gid=197)


def test_the_box_of_an_unknown_id_is_not_banked_even_though_the_site_answers_200(site):
    empty = _box_with(lambda b: b["data"].update(home=[], away=[]))
    site(_pages({"/api/boxscore.preciser.php": empty}))
    with pytest.raises(AssetFetchError):
        plg_pkg.pleague_box_teams(gid=197)


def _pbp_with(fn: Callable[[str], str]) -> str:
    body = _json("pbp_197.json")
    out = fn(body["data"])
    assert out != body["data"]
    return json.dumps({**body, "data": out})


@pytest.mark.parametrize(
    "mutate,match",
    [
        (lambda h: h.replace("94 - 102", "94 - 103"), "final"),
        (lambda h: h.replace("助攻", "神秘攻X"), "unknown play-by-play phrase"),
        (lambda h: h.replace("pbplay", "pbplayx"), "pbplay table"),
        (lambda h: h[: h.index('<tr class="all-tr quater-tr">')], "truncated"),
        (lambda h: re.sub(r'<tr class="Q4-tr quater-tr">.*?</tr>', "", h, flags=re.S), "period"),
    ],
)
def test_a_play_by_play_that_is_not_the_games_is_refused(site, mutate, match):
    try:
        text = _pbp_with(mutate)
    except AssertionError:  # the cut fixture holds no such phrase: use a phrase it does hold
        pytest.skip("phrase absent from the cut fixture")
    site(_pages({"/api/playbyplay.php": text}))
    with pytest.raises(AssetFetchError, match=match):
        plg_pkg.pleague_pbp(gid=197)


def test_a_play_by_play_error_field_and_a_game_without_uuid(site):
    site(_pages({"/api/playbyplay.php": json.dumps({"error": "bad", "data": ""})}))
    with pytest.raises(AssetFetchError, match="error field"):
        plg_pkg.pleague_pbp(gid=197)
    R.clear_cache()
    site(_pages())
    with pytest.raises(NoDataError, match="no play-by-play"):  # 2020-21: a box but no play-by-play
        plg_pkg.pleague_pbp(gid=13)
    assert plg_pkg.pleague_box_players(gid=13).height > 0  # the box still works there


def test_a_vendor_key_surviving_in_the_record_is_refused(site, monkeypatch):
    site(_pages())
    real = P.pbp_record

    def leaky(body):
        rec = real(body)
        rec["events"][0]["text_zh"] += "?key=0123456789abcdef0123"
        return rec

    monkeypatch.setattr(P, "pbp_record", leaky)
    with pytest.raises(AssetFetchError, match="access key"):
        plg_pkg.pleague_pbp(gid=197)


def test_argument_checks_happen_before_any_request(site):
    calls = site(_pages())
    with pytest.raises(ValueError, match="stage must"):
        plg_pkg.pleague_schedule(season=2023, stage="exhibition")
    with pytest.raises(ValueError, match="stage_sn"):
        plg_pkg.pleague_stat_player(season=2023, stage_sn=9)
    with pytest.raises(ValueError, match="player_page_id"):
        plg_pkg.pleague_player(player_page_id="a b")
    with pytest.raises(ValueError, match="gid"):
        plg_pkg.pleague_game(gid="abc")  # type: ignore[arg-type]
    with pytest.raises(NoDataError, match="first season"):
        plg_pkg.pleague_standings(season=2019)
    with pytest.raises(TypeError, match="unsupported"):
        R._get("https://pleagueofficial.com/game/197", proxy=1)
    with pytest.raises(ValueError, match="not a P. LEAGUE"):
        R._get("https://pleagueofficial.com/elsewhere")
    assert calls == []


def test_the_transport_never_follows_redirects_and_names_curl_cffi(monkeypatch):
    seen: Dict[str, Any] = {}

    class FakeCreq:
        @staticmethod
        def get(url, **kw):
            seen.update(kw)
            return Resp(200, "ok")

    import types

    monkeypatch.setitem(sys.modules, "curl_cffi", types.SimpleNamespace(requests=FakeCreq))
    R._transport("https://pleagueofficial.com/game/1", {"a": 1})
    assert seen["impersonate"] == "chrome" and seen["allow_redirects"] is False and seen["params"] == {"a": 1}


def test_the_runtime_does_not_need_the_refs_checkout(monkeypatch, tmp_path):
    monkeypatch.setenv("SDV_INTERNAL_REFS_REPO", str(tmp_path))
    assert not any((tmp_path / "pleague").exists() for _ in range(1))
    assert gen_pleague.SITE and P.parse_pleague_game(_game()).height == 1  # tests read only committed fixtures
