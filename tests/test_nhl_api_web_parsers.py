"""Offline parser tests for the NHL api-web parser layer in
``sportsdataverse.nhl.nhl_api_web_parsers``.

Captured fixtures live in ``tests/fixtures/nhl_api_web/``. See that
directory's README.md for provenance.

The 16 parsers + 2 dispatchers cover all 26 ``nhl_web_*`` wrappers
in :mod:`sportsdataverse.nhl.nhl_api_web` (game-center, schedule /
score, standings, team / player, leaders, draft).
"""

from __future__ import annotations

import json
from pathlib import Path

import polars as pl
import pytest


FIXTURE_DIR = Path(__file__).parent / "fixtures" / "nhl_api_web"


def _load(stem: str) -> dict:
    return json.loads((FIXTURE_DIR / f"{stem}.json").read_text(encoding="utf-8"))


# ===========================================================================
# Game-center
# ===========================================================================


def test_parse_nhl_web_pbp_returns_one_row_per_play():
    from sportsdataverse.nhl import parse_nhl_web_pbp

    df = parse_nhl_web_pbp(_load("pbp_2024_scf_g7"))
    assert isinstance(df, pl.DataFrame)
    # 2024 SCF G7 has 331 plays in the capture
    assert df.height >= 100, f"expected >=100 plays, got {df.height}"
    for col in ("event_id", "type_code", "type_desc_key",
                "time_in_period", "sort_order"):
        assert col in df.columns, f"missing column {col!r}"


def test_parse_nhl_web_boxscore_unrolls_team_x_position_groups():
    """playerByGameStats ships 6 buckets (away/home × forwards/defense/
    goalies). The parser must merge all 6 into one frame and tag each
    row with home_away + position_group."""
    from sportsdataverse.nhl import parse_nhl_web_boxscore

    df = parse_nhl_web_boxscore(_load("boxscore_2024_scf_g7"))
    assert df.height >= 30, f"expected >=30 players, got {df.height}"
    assert "home_away" in df.columns
    assert "position_group" in df.columns
    assert "player_id" in df.columns
    # Verify both teams are present
    home_aways = set(df["home_away"].to_list())
    assert home_aways == {"home", "away"}, (
        f"expected both home and away rows, got {home_aways}"
    )
    # Verify all 3 position groups present
    pos_groups = set(df["position_group"].to_list())
    assert pos_groups == {"forwards", "defense", "goalies"}, (
        f"expected all 3 position groups, got {pos_groups}"
    )


def test_parse_nhl_web_landing_returns_single_row():
    from sportsdataverse.nhl import parse_nhl_web_landing

    df = parse_nhl_web_landing(_load("landing_2024_scf_g7"))
    assert df.height == 1
    assert "id" in df.columns
    assert "game_date" in df.columns


# ===========================================================================
# right_rail dispatcher
# ===========================================================================


def test_parse_nhl_web_right_rail_returns_six_section_dict():
    from sportsdataverse.nhl import parse_nhl_web_right_rail

    payload = _load("right_rail_2024_scf_g7")
    out = parse_nhl_web_right_rail(payload)
    assert isinstance(out, dict)
    assert set(out) == {
        "season_series", "shots_by_period", "team_game_stats",
        "game_info", "linescore_by_period", "season_series_wins",
    }
    # Each sub-frame should be a polars DataFrame, several should have rows
    for name, frame in out.items():
        assert isinstance(frame, pl.DataFrame), (
            f"{name}: returned {type(frame)}"
        )
    # Specific row-count checks for sections that should always have data
    assert out["shots_by_period"].height == 3, "expected 3 periods"
    assert out["linescore_by_period"].height >= 3
    assert out["season_series"].height >= 1


def test_parse_nhl_web_right_rail_with_section_returns_single_frame():
    from sportsdataverse.nhl import parse_nhl_web_right_rail

    payload = _load("right_rail_2024_scf_g7")
    df = parse_nhl_web_right_rail(payload, section="team_game_stats")
    assert isinstance(df, pl.DataFrame)
    assert df.height >= 5, f"expected >=5 stat categories, got {df.height}"


def test_parse_nhl_web_right_rail_raises_on_unknown_section():
    from sportsdataverse.nhl import parse_nhl_web_right_rail

    with pytest.raises(ValueError, match="Unknown right_rail section"):
        parse_nhl_web_right_rail({}, section="nope")


# ===========================================================================
# Schedule / score / scoreboard
# ===========================================================================


def test_parse_nhl_web_schedule_prefixes_date_per_game():
    from sportsdataverse.nhl import parse_nhl_web_schedule

    df = parse_nhl_web_schedule(_load("schedule_2024_06_24"))
    assert df.height >= 1
    assert "schedule_date" in df.columns


def test_parse_nhl_web_score_returns_games_for_date():
    from sportsdataverse.nhl import parse_nhl_web_score

    df = parse_nhl_web_score(_load("score_2024_06_24"))
    assert df.height >= 1
    assert "id" in df.columns


def test_parse_nhl_web_scoreboard_unrolls_games_by_date():
    from sportsdataverse.nhl import parse_nhl_web_scoreboard

    df = parse_nhl_web_scoreboard(_load("scoreboard_now"))
    assert df.height >= 1
    assert "scoreboard_date" in df.columns


def test_parse_nhl_web_club_schedule_includes_club_context_columns():
    """club_schedule prefixes context columns (club_timezone,
    club_current_season, etc.) onto each game row."""
    from sportsdataverse.nhl import parse_nhl_web_club_schedule

    df = parse_nhl_web_club_schedule(_load("club_schedule_edm_2024"))
    # EDM 2023-24: 82 regular + 25 playoff = 115 games
    assert df.height >= 80, f"expected full club schedule, got {df.height}"
    for col in ("club_timezone", "club_current_season", "id", "game_date"):
        assert col in df.columns


# ===========================================================================
# Standings
# ===========================================================================


def test_parse_nhl_web_standings_returns_32_teams():
    from sportsdataverse.nhl import parse_nhl_web_standings

    df = parse_nhl_web_standings(_load("standings_now"))
    assert df.height == 32, f"expected 32 NHL teams, got {df.height}"


def test_parse_nhl_web_standings_season_returns_one_row_per_season():
    from sportsdataverse.nhl import parse_nhl_web_standings_season

    df = parse_nhl_web_standings_season(_load("standings_season"))
    # 108 NHL seasons since 1917-18 — historic depth check
    assert df.height >= 100, f"expected ~108 seasons, got {df.height}"


# ===========================================================================
# Team / player
# ===========================================================================


def test_parse_nhl_web_club_stats_dispatcher_returns_skaters_and_goalies():
    from sportsdataverse.nhl import parse_nhl_web_club_stats

    out = parse_nhl_web_club_stats(_load("club_stats_edm_2024"))
    assert isinstance(out, dict)
    assert set(out) == {"skaters", "goalies"}
    assert out["skaters"].height >= 20, "expected full skater roster"
    assert out["goalies"].height >= 2, "expected at least 2 goalies"


def test_parse_nhl_web_club_stats_with_section_returns_single_frame():
    from sportsdataverse.nhl import parse_nhl_web_club_stats

    df = parse_nhl_web_club_stats(_load("club_stats_edm_2024"), section="skaters")
    assert isinstance(df, pl.DataFrame)
    assert df.height >= 20


def test_parse_nhl_web_club_stats_raises_on_unknown_section():
    from sportsdataverse.nhl import parse_nhl_web_club_stats

    with pytest.raises(ValueError, match="Unknown club_stats section"):
        parse_nhl_web_club_stats({}, section="nope")


def test_parse_nhl_web_roster_merges_three_position_groups():
    from sportsdataverse.nhl import parse_nhl_web_roster

    df = parse_nhl_web_roster(_load("roster_edm_2024"))
    # 11 forwards + 9 defensemen + 4 goalies = 24 in the EDM capture
    assert df.height >= 20
    assert "position_group" in df.columns
    pg = set(df["position_group"].to_list())
    assert pg == {"forwards", "defensemen", "goalies"}, (
        f"expected all 3 position groups, got {pg}"
    )


def test_parse_nhl_web_player_landing_returns_rich_profile():
    from sportsdataverse.nhl import parse_nhl_web_player_landing

    df = parse_nhl_web_player_landing(_load("player_mcdavid_landing"))
    assert df.height == 1
    assert "player_id" in df.columns
    assert "is_active" in df.columns


def test_parse_nhl_web_player_game_log_returns_one_row_per_game():
    from sportsdataverse.nhl import parse_nhl_web_player_game_log

    df = parse_nhl_web_player_game_log(_load("player_mcdavid_gamelog"))
    # McDavid played 76 games in 2023-24 regular season
    assert df.height >= 70
    for col in ("game_id", "game_date", "goals", "assists", "points"):
        assert col in df.columns


# ===========================================================================
# Leaders + draft
# ===========================================================================


def test_parse_nhl_web_leaders_unrolls_category_keyed_payload():
    """Leaders payloads are keyed by stat category at the top level —
    each value is a list of player rows. The parser walks all list-
    valued keys and tags each row with the category it came from."""
    from sportsdataverse.nhl import parse_nhl_web_leaders

    df_skaters = parse_nhl_web_leaders(_load("skater_leaders_now"))
    assert df_skaters.height >= 10
    assert "category" in df_skaters.columns
    assert "id" in df_skaters.columns

    df_goalies = parse_nhl_web_leaders(_load("goalie_leaders_now"))
    assert df_goalies.height >= 10
    assert "category" in df_goalies.columns


def test_parse_nhl_web_draft_picks_returns_one_row_per_pick():
    from sportsdataverse.nhl import parse_nhl_web_draft_picks

    df = parse_nhl_web_draft_picks(_load("draft_picks_2024_r1"))
    assert df.height == 32, f"expected 32 first-round picks, got {df.height}"
    for col in ("round", "pick_in_round", "overall_pick", "team_id"):
        assert col in df.columns


# ===========================================================================
# Empty payload + pandas opt-in + registry
# ===========================================================================


@pytest.mark.parametrize("parser_name", [
    "parse_nhl_web_pbp",
    "parse_nhl_web_boxscore",
    "parse_nhl_web_landing",
    "parse_nhl_web_schedule",
    "parse_nhl_web_score",
    "parse_nhl_web_scoreboard",
    "parse_nhl_web_club_schedule",
    "parse_nhl_web_standings",
    "parse_nhl_web_standings_season",
    "parse_nhl_web_roster",
    "parse_nhl_web_player_landing",
    "parse_nhl_web_player_game_log",
    "parse_nhl_web_leaders",
    "parse_nhl_web_draft_picks",
])
def test_parser_handles_empty_payload(parser_name):
    from sportsdataverse import nhl

    parser = getattr(nhl, parser_name)
    df = parser({})
    assert isinstance(df, pl.DataFrame)
    assert df.height == 0


def test_parse_nhl_web_pbp_returns_pandas_when_requested():
    import pandas as pd

    from sportsdataverse.nhl import parse_nhl_web_pbp

    df = parse_nhl_web_pbp(_load("pbp_2024_scf_g7"), return_as_pandas=True)
    assert isinstance(df, pd.DataFrame)
    assert len(df) >= 100


def test_nhl_api_web_endpoint_parsers_registry_references_real_wrappers():
    from sportsdataverse.nhl import NHL_API_WEB_ENDPOINT_PARSERS, nhl_api_web

    for fn_name in NHL_API_WEB_ENDPOINT_PARSERS:
        assert hasattr(nhl_api_web, fn_name), (
            f"NHL_API_WEB_ENDPOINT_PARSERS references missing wrapper {fn_name!r}"
        )


def test_parser_for_nhl_api_web_returns_callable_or_none():
    from sportsdataverse.nhl import parse_nhl_web_pbp, parser_for_nhl_api_web

    assert parser_for_nhl_api_web("nhl_web_pbp") is parse_nhl_web_pbp
    # Unregistered endpoint (e.g. playoff_series, player_spotlight,
    # draft_rankings) — returns None, caller null-checks
    assert parser_for_nhl_api_web("nhl_web_unregistered_endpoint") is None
