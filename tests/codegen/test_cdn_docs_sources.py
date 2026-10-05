"""Source metadata for the ESPN CDN reference pages (review follow-ups to #681)."""

from __future__ import annotations

import json
from pathlib import Path

import yaml

from sportsdataverse._common_espn_parsers import parse_cdn_schedule, parse_cdn_scoreboard
from tools.codegen import generate, spec

ROOT = Path(__file__).resolve().parents[2]
FIX = ROOT / "tests" / "fixtures" / "espn" / "cdn"
_CDN = yaml.safe_load((ROOT / "tools/codegen/endpoints/espn_cdn.yaml").read_text("utf-8"))["endpoints"]
_BY_SHORT = {e["short"]: e for e in _CDN}


def test_scoreboard_schema_matches_parser_columns():
    schema = yaml.safe_load((ROOT / "tools/codegen/schemas/cdn_scoreboard.yaml").read_text("utf-8"))
    cols = [c["name"] for c in schema["columns"]]
    for short, parse, fixture in (
        ("cdn_scoreboard", parse_cdn_scoreboard, "scoreboard_epl"),
        ("cdn_schedule", parse_cdn_schedule, "schedule_nba"),
    ):
        assert _BY_SHORT[short]["returns_schema"] == "cdn_scoreboard"
        df = parse(json.loads((FIX / f"{fixture}.json").read_text("utf-8")))
        assert df.columns == cols, short


def test_football_examples_use_week_params_not_date():
    for short in ("cdn_schedule", "cdn_scoreboard"):
        lea = _BY_SHORT[short]["league_example_args"]
        for lg in ("cfb", "nfl"):
            assert set(lea[lg]) == {"season", "week", "season_type"}


def test_game_examples_are_per_league_ids():
    for short in ("cdn_playbyplay", "cdn_boxscore"):
        ids = {lg: a["game_id"] for lg, a in _BY_SHORT[short]["league_example_args"].items()}
        assert "wnba" in ids and "wbb" in ids
        assert "401705127" not in ids.values()  # the NBA id belongs to nba only
        assert len(set(ids.values())) == len(ids)


def test_league_example_args_replace_defaults_in_view():
    registry = spec.load_parameters(generate.ENDPOINTS / "parameters.yaml")
    api = spec.load_espn_api(generate.ENDPOINTS / "espn_cdn.yaml", registry)
    ep = next(e for e in api.endpoints if e.short == "cdn_schedule")
    lg = spec.League(prefix="nfl", sport="football", league="nfl", scopes=["universal"])
    view = generate._EndpointView(ep, "espn_nfl_cdn_schedule", "https://cdn.espn.com/core", lg)
    assert "date" not in view.example_args and view.example_args["week"] == 5
    assert "week=5" in view.example_url and "date=" not in view.example_url
