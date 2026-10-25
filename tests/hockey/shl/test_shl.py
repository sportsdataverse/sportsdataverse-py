"""Offline tests for the SHL (``shl``) parsers, endpoint YAML and returns-schemas.

Every parser runs on the real captures in ``tests/fixtures/shl/`` (3-row samples copied from
sdv-internal-refs) and, for the dtypes, on the untruncated bodies in ``tests/fixtures/shl/full/``.
No network.
"""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

import pandas as pd
import polars as pl
import pytest
import yaml

from sportsdataverse.hockey.shl import shl_parsers as P

ROOT = Path(__file__).parents[3]
FIXTURES = ROOT / "tests" / "fixtures" / "shl"
FULL = FIXTURES / "full"
YAML = ROOT / "tools" / "codegen" / "endpoints" / "shl.yaml"
SCHEMAS = ROOT / "tools" / "codegen" / "schemas" / "native" / "shl"

_SNAKE = re.compile(r"^[a-z0-9]+(_[a-z0-9]+)*$")
# isa / StatNet ids are Int64 in every frame; platform uuids and team codes are Utf8.
_INT_ID = re.compile(r"(^|_)(player_id|game_id|event_id|ext_id)$")


def _load(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def _endpoints() -> list:
    return yaml.safe_load(YAML.read_text(encoding="utf-8"))["endpoints"]


def _capture_for(ep: dict) -> list:
    slug = ep["path"].strip("/").replace("/", "__").replace("{", "").replace("}", "")
    if ep["short"] == "stats_info":
        return sorted(FIXTURES.glob(f"{slug}__*.json"))
    return [FIXTURES / f"{slug}.json"]


_CASES = [(ep["short"], ep["parser"], f) for ep in _endpoints() for f in _capture_for(ep)]


@pytest.mark.parametrize(("short", "parser", "fixture"), _CASES, ids=[f"{s}:{f.stem[-30:]}" for s, _, f in _CASES])
def test_every_capture_parses_to_a_typed_frame(short, parser, fixture):
    df = getattr(P, parser)(_load(fixture))
    assert isinstance(df, pl.DataFrame)
    assert df.height > 0, short
    assert all(_SNAKE.match(c) for c in df.columns), [c for c in df.columns if not _SNAKE.match(c)]
    for col in df.columns:
        if _INT_ID.search(col):
            assert df.schema[col] == pl.Int64, (short, col, df.schema[col])
        if col.endswith("_uuid"):
            assert df.schema[col] == pl.Utf8, (short, col)


@pytest.mark.parametrize("payload", [{}, [], None, "<html>", 3, [None], {"stats": None}])
@pytest.mark.parametrize("parser", P.__all__)
def test_empty_or_malformed_payload_is_a_zero_row_frame(parser, payload):
    df = getattr(P, parser)(payload)
    assert isinstance(df, pl.DataFrame)
    assert df.height == 0


def test_zero_row_frame_carries_the_documented_columns():
    df = P.parse_shl_pbp([], columns=["game_uuid", "player_id", "pop_player_ids"])
    assert df.schema == {"game_uuid": pl.Utf8, "player_id": pl.Int64, "pop_player_ids": pl.List(pl.Int64)}


@pytest.mark.parametrize("parser", P.__all__)
def test_return_as_pandas(parser):
    ep = next(e for e in _endpoints() if e["parser"] == parser) if parser != "parse_shl_player_crosswalk" else None
    fixture = _capture_for(ep)[0] if ep else FIXTURES / "statistics-v2__stats-info__module__players_summary.json"
    out = getattr(P, parser)(_load(fixture), return_as_pandas=True)
    assert isinstance(out, pd.DataFrame) and len(out) > 0


# --------------------------------------------------------------------------- dtypes from FULL bodies


def test_pbp_full_body_ids_coordinates_and_on_ice_lists():
    df = P.parse_shl_pbp(_load(FULL / "gameday__play-by-play__bdhvuc5tex.json"))
    assert df.height == 131
    assert df.schema["player_id"] == pl.Int64  # a STRING in this body, an integer elsewhere
    assert df.schema["game_id"] == pl.Int64 and df.schema["event_id"] == pl.Int64
    assert df.schema["location_x"] == pl.Int64 and df.schema["location_y"] == pl.Int64
    assert df.schema["pop_player_ids"] == pl.List(pl.Int64)
    goals = df.filter(pl.col("type") == "goal")
    assert goals.height == 3
    assert goals["goal_section"].null_count() == 0
    assert goals["pop_player_ids"].list.len().min() >= 5
    assert goals["nep_player_ids"].list.len().min() >= 4
    # The scorer is on the ice for his own goal.
    assert goals.select(pl.col("pop_player_ids").list.contains(pl.col("player_id")).all()).item()
    assert set(goals["assist1_player_id"].drop_nulls().to_list()) <= set(df["player_id"].drop_nulls().to_list())


def test_player_box_full_body_joins_names_and_types_rates():
    df = P.parse_shl_player_box(_load(FULL / "gameday__boxscore__bdhvuc5tex.json"))
    assert set(df["team_side"]) == {"home", "away"}
    assert set(df["position_group"]) == {"skater", "goalie"}
    assert df.schema["player_id"] == pl.Int64
    assert df["full_name"].null_count() == 0  # every row found its isa key in the name map
    assert df.schema["svs_perc"] == pl.Float64 and df.schema["fo_perc"] == pl.Float64
    assert df.schema["toi"] == pl.Utf8
    lars = df.filter(pl.col("player_id") == 2427)
    assert lars["position_group"].item() == "goalie"
    assert lars["svs_perc"].item() == pytest.approx(28 / 29)


def test_stats_info_full_bodies_rates_are_float_and_isa_is_joined():
    gk = P.parse_shl_stats_info(_load(FULL / "statistics-v2__stats-info__goalkeepers_summary__iuzqg7dqk9.json"))
    assert gk.height == 36
    assert gk.schema["svs_perc"] == pl.Float64 and gk.schema["gaa"] == pl.Float64
    assert gk.schema["mip"] == pl.Utf8  # minutes played, mm:ss
    assert gk.schema["player_id"] == pl.Int64 and gk["player_id"].null_count() == 0
    assert "player" not in gk.columns and "team" not in gk.columns  # constant-0 display slots dropped
    teams = P.parse_shl_stats_info(_load(FULL / "statistics-v2__stats-info__teams_result__iuzqg7dqk9.json"))
    assert teams.height == 14
    assert teams.schema["gpg"] == pl.Float64
    # teams_result W / L are totals: every game is a win or a loss.
    assert teams.select((pl.col("w") + pl.col("l") == pl.col("gp")).all()).item()


def test_schema_types_come_from_the_full_bodies():
    """A 3-row sample can type a rate as an integer; the committed schema must not."""
    for short, body, parser in [
        ("stats_info", "statistics-v2__stats-info__goalkeepers_summary__iuzqg7dqk9.json", P.parse_shl_stats_info),
        ("boxscore", "gameday__boxscore__bdhvuc5tex.json", P.parse_shl_player_box),
        ("pbp", "gameday__play-by-play__bdhvuc5tex.json", P.parse_shl_pbp),
    ]:
        schema = {
            c["name"]: c["type"]
            for c in yaml.safe_load((SCHEMAS / f"{short}.yaml").read_text(encoding="utf-8"))["columns"]
        }
        df = parser(_load(FULL / body))
        for col, dtype in df.schema.items():
            want = "numeric" if dtype == pl.Float64 else "integer" if dtype.is_integer() else None
            if want:
                assert schema[col] == want, (short, col)


# --------------------------------------------------------------------------- dedicated shapers


def test_standings_season_is_the_end_year_and_t_balances():
    df = P.parse_shl_standings(_load(FIXTURES / "gameday__team-standings__ssgt_uuid.json"))
    assert df["season"].unique().to_list() == [2026]  # body season 2025 = 2025/26
    assert df.select((pl.col("w") + pl.col("t") + pl.col("l") == pl.col("gp")).all()).item()
    assert df.select((pl.col("otw") + pl.col("otl") == pl.col("t")).all()).item()
    assert df["team_code"].to_list()[0] == "SAIK"


def test_schedule_rows_are_games_with_side_prefixes():
    df = P.parse_shl_schedule(_load(FIXTURES / "sports-v2__game-schedule.json"))
    assert df["game_uuid"][0] == "bdhvuc5tex"
    assert {"home_team_uuid", "away_team_code", "home_team_score", "venue_name"} <= set(df.columns)


def test_team_stats_one_row_per_side_and_period():
    df = P.parse_shl_team_stats(_load(FIXTURES / "gameday__team-stats__game_uuid.json"))
    assert df.group_by("team_side").len().sort("team_side")["len"].to_list() == [3, 3]
    assert df.filter((pl.col("team_side") == "home") & (pl.col("period") == 0))["g"].item() == 2


def test_layout_maps_start_year_codes_to_end_year_seasons():
    df = P.parse_shl_layout(_load(FULL / "statistics-v2__layout-info__directory.json"))
    assert df.height == 110
    assert df.select(pl.struct("season", "game_type").is_unique().all()).item()
    row = df.filter((pl.col("season") == 2026) & (pl.col("game_type") == "regular"))
    assert row["ssgt_uuid"].item() == "iuzqg7dqk9" and row["season_code"].item() == "2025"


def test_player_crosswalk_isa_to_athlete_uuid():
    df = P.parse_shl_player_crosswalk(_load(FIXTURES / "statistics-v2__stats-info__module__players_summary.json"))
    assert df.schema["player_id"] == pl.Int64 and df.schema["athlete_uuid"] == pl.Utf8
    assert df.height == 3 and df["athlete_uuid"].null_count() == 0


def test_page_object_is_one_row():
    """A team page holds one incidental list (contact); it must not become the rows."""
    team = _load(FIXTURES / "sports-v2__teams__team_uuid.json")
    assert P.parse_shl(team).height != 1  # the generic rule would take the contact list
    df = P.parse_shl_page(team)
    assert df.height == 1 and df["uuid"].item() == "087a-087aTQv9u"


def test_provider_bare_word_is_one_row():
    assert P.parse_shl("statnet").to_dicts() == [{"provider": "statnet"}]


# --------------------------------------------------------------------------- YAML + schemas


def test_yaml_lists_every_spec_route():
    shorts = [e["short"] for e in _endpoints()]
    assert len(shorts) == len(set(shorts)) == 27
    for ep in _endpoints():
        assert all(f.exists() for f in _capture_for(ep)), ep["short"]
        assert (SCHEMAS / f"{ep['short']}.yaml").exists()


def test_no_path_token_named_league_or_sport():
    for ep in _endpoints():
        assert not {"league", "sport"} & set(re.findall(r"\{(\w+)\}", ep["path"]))


def test_season_routes_take_the_end_year_and_stats_info_always_sends_count_1000():
    by = {e["short"]: e for e in _endpoints()}
    for short in ("stats_info", "schedule", "last_game_day", "next_game_day"):
        args = {p["name"]: p for p in by[short]["extra_params"]}
        assert args["season"]["type"] == "int" and "END year" in args["season"]["description"]
        assert args["game_type"]["choices"] == ["regular", "playoff", "qualdown", "playin"]
        assert not {"ssgt_uuid", "season_uuid", "count"} & set(args)
    assert by["stats_info"]["fixed_params"] == {"count": 1000}
    modules = next(p for p in by["stats_info"]["path_params"] if p["name"] == "module")["choices"]
    assert len(modules) == 34


def test_schemas_match_the_parsers_on_the_captures():
    for ep in _endpoints():
        cols = [
            c["name"] for c in yaml.safe_load((SCHEMAS / f"{ep['short']}.yaml").read_text(encoding="utf-8"))["columns"]
        ]
        for f in _capture_for(ep):
            parsed = getattr(P, ep["parser"])(_load(f)).columns
            assert set(parsed) <= set(cols), (ep["short"], f.name, sorted(set(parsed) - set(cols)))


# --------------------------------------------------------------------------- empty frames = populated dtypes


def _documented(short: str) -> list:
    return [c["name"] for c in yaml.safe_load((SCHEMAS / f"{short}.yaml").read_text(encoding="utf-8"))["columns"]]


@pytest.mark.parametrize(("short", "parser", "fixture"), _CASES, ids=[f"{s}:{f.stem[-30:]}" for s, _, f in _CASES])
def test_empty_frame_concatenates_with_a_populated_one(short, parser, fixture):
    """What the generated wrapper does: ``columns=`` -> documented columns and dtypes, empty or not."""
    cols = _documented(short)
    empty = getattr(P, parser)({}, columns=cols)
    full = getattr(P, parser)(_load(fixture), columns=cols)
    both = pl.concat([empty, full], how="vertical")
    assert both.columns == cols and both.height == full.height > 0
    assert empty.schema == full.schema


@pytest.mark.parametrize(
    ("short", "parser", "body"),
    [
        ("pbp", "parse_shl_pbp", "gameday__play-by-play__bdhvuc5tex.json"),
        ("boxscore", "parse_shl_player_box", "gameday__boxscore__bdhvuc5tex.json"),
        ("stats_info", "parse_shl_stats_info", "statistics-v2__stats-info__teams_result__iuzqg7dqk9.json"),
        ("stats_info", "parse_shl_stats_info", "statistics-v2__stats-info__goalkeepers_summary__iuzqg7dqk9.json"),
        ("layout_info", "parse_shl_layout", "statistics-v2__layout-info__directory.json"),
    ],
)
def test_empty_frame_matches_the_full_bodies(short, parser, body):
    cols = _documented(short)
    empty = getattr(P, parser)([], columns=cols)
    pl.concat([empty, getattr(P, parser)(_load(FULL / body), columns=cols)], how="vertical")


def test_schema_module_lists_the_returns_schema_columns():
    """``shl_schemas.SCHEMAS`` is written by gen_shl.py with the schemas; they must not drift apart."""
    from sportsdataverse.hockey.shl.shl_schemas import SCHEMAS as DTYPES

    assert set(DTYPES) == {e["short"] for e in _endpoints()}
    r_type = {"str": "character", "int": "integer", "float": "numeric", "bool": "logical", "list[int]": "character"}
    for short, types in DTYPES.items():
        doc = yaml.safe_load((SCHEMAS / f"{short}.yaml").read_text(encoding="utf-8"))["columns"]
        assert [c["name"] for c in doc] == list(types), short
        assert [c["type"] for c in doc] == [r_type[t] for t in types.values()], short
        assert all(c["description"] == "" for c in doc), short  # text lives in the descriptions snippet


def test_unplayed_games_type_the_score_like_played_ones():
    cols = _documented("team_upcoming_games")
    df = P.parse_shl_schedule(_load(FIXTURES / "sports-v2__upcoming-games__team_uuid.json"), columns=cols)
    assert df.schema["home_team_score"] == pl.Int64 == df.schema["away_team_score"]


def test_missing_documented_columns_are_backfilled_with_their_dtype():
    cols = _documented("standings")
    df = P.parse_shl_standings({"season": 2025, "leagueStandings": [{"id": "SAIK"}]}, columns=cols)
    assert df.columns == cols and df.height == 1
    assert df.schema["gp"] == pl.Int64 and df["gp"].null_count() == 1


@pytest.mark.parametrize(
    ("parser", "payload"),
    [
        ("parse_shl_team_stats", {"home": {"teamId": "FHC", "statistics": 5}}),
        ("parse_shl_team_stats", {"home": {"statistics": [{"period": 0, "parsedTotalStatistics": "x"}]}}),
        ("parse_shl_standings", {"leagueStandings": "x"}),
        ("parse_shl_stats_info", [{"stats": 7}]),
        ("parse_shl_filter", {"season": [{"code": "2025", "names": 3}], "series": True}),
        ("parse_shl_roster", [{"positionCode": "GK", "players": {"a": 1}}]),
        ("parse_shl_h2h", {"players": "x"}),
    ],
)
def test_wrong_typed_containers_read_as_absent(parser, payload):
    assert isinstance(getattr(P, parser)(payload), pl.DataFrame)


def test_h2h_face_off_percentage_is_float():
    cols = _documented("player_h2h")
    body = _load(FIXTURES / "statistics-v2__player-h2h-v2__ssgt_uuid__home_team_uuid__away_team_uuid.json")
    assert P.parse_shl_h2h(body).schema["match_stats_fo_perc"] == pl.Float64
    assert P.parse_shl_h2h(body, columns=cols).schema["match_stats_fo_perc"] == pl.Float64
