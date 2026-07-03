"""Tests for :func:`build_nfl_player_stats_def` / :func:`build_nfl_player_stats_kicking`.

Faithful polars ports of nflfastR's deprecated ``calculate_player_stats_def`` /
``calculate_player_stats_kicking`` (``aggregate_game_stats_def.R`` /
``aggregate_game_stats_kicking.R``). These builders consume a caller-supplied
play-by-play frame directly (unlike :func:`build_nfl_player_stats`, which loads
its own PBP) -- so the tests build a small synthetic PBP frame in-process,
no ``load_nfl_pbp`` monkeypatching required.
"""

from __future__ import annotations

import polars as pl
import pytest

from sportsdataverse.nfl import (
    build_nfl_player_stats_def,
    build_nfl_player_stats_kicking,
)


def _synthetic_players() -> pl.DataFrame:
    ids = ["DE1", "DE2", "LB1", "LB2", "CB1", "K1"]
    return pl.DataFrame(
        {
            "gsis_id": ids,
            "display_name": [f"{i} Display" for i in ids],
            "short_name": [f"{i[0]}.{i[1:]}" for i in ids],
            "position": ["DE", "DE", "LB", "LB", "CB", "K"],
            "position_group": ["DL", "DL", "LB", "LB", "DB", "SPEC"],
            "headshot": [f"u_{i}" for i in ids],
        }
    )


@pytest.fixture(autouse=True)
def _patch_players(monkeypatch):
    import sportsdataverse.nfl.nfl_loaders as loaders

    monkeypatch.setattr(loaders, "load_nfl_players", lambda *a, **k: _synthetic_players())


def _base_row(**kw):  # type: ignore[no-untyped-def]
    base = {
        "game_id": "2023_01_AAA_BBB",
        "season": 2023,
        "week": 1,
        "season_type": "REG",
        "posteam": "AAA",
        "defteam": "BBB",
        "home_team": "AAA",
        "away_team": "BBB",
        "down": 1,
        "play_type": "pass",
        "yards_gained": 0,
        "touchdown": 0,
        "td_team": None,
        "td_player_id": None,
        "safety": 0,
        "safety_player_id": None,
        "fumble": 0,
        "fumble_lost": 0,
        "penalty": 0,
        "penalty_team": None,
        "penalty_player_id": None,
        "penalty_yards": 0,
        "interception": 0,
        "interception_player_id": None,
        "return_yards": 0,
        "return_touchdown": 0,
        "sack": 0,
        "sack_player_id": None,
        "half_sack_1_player_id": None,
        "half_sack_2_player_id": None,
        "qb_hit_1_player_id": None,
        "qb_hit_2_player_id": None,
        "pass_defense_1_player_id": None,
        "pass_defense_2_player_id": None,
        "solo_tackle_1_player_id": None,
        "solo_tackle_2_player_id": None,
        "assist_tackle_1_player_id": None,
        "assist_tackle_2_player_id": None,
        "tackle_with_assist_1_player_id": None,
        "tackle_for_loss_1_player_id": None,
        "tackle_for_loss_2_player_id": None,
        "tackled_for_loss": 0,
        "forced_fumble_player_1_player_id": None,
        "forced_fumble_player_2_player_id": None,
        "fumbled_1_team": None,
        "fumbled_1_player_id": None,
        "fumbled_2_team": None,
        "fumbled_2_player_id": None,
        "fumble_recovery_1_team": None,
        "fumble_recovery_1_player_id": None,
        "fumble_recovery_1_yards": 0,
        "fumble_recovery_2_team": None,
        "fumble_recovery_2_player_id": None,
        "fumble_recovery_2_yards": 0,
        "kicker_player_id": None,
        "kicker_player_name": None,
        "kick_distance": None,
        "field_goal_attempt": 0,
        "field_goal_result": None,
        "extra_point_attempt": 0,
        "extra_point_result": None,
        "fixed_drive": 1,
        "score_differential": 0,
    }
    base.update(kw)
    return base


def _def_pbp() -> pl.DataFrame:
    """One week: split sack, solo+assist tackle, INT-return TD."""
    rows = [
        # AAA dropback, BBB splits a sack (DE1 + DE2 half each), -8 yards
        _base_row(
            play_type="pass",
            down=1,
            sack=1,
            yards_gained=-8,
            half_sack_1_player_id="DE1",
            half_sack_2_player_id="DE2",
        ),
        # AAA run, BBB solo tackle (LB1) + assist (LB2)
        _base_row(
            play_type="run",
            down=2,
            yards_gained=3,
            solo_tackle_1_player_id="LB1",
            assist_tackle_1_player_id="LB2",
        ),
        # AAA pass intercepted by CB1, returned 45 yards for a TD (defteam scores)
        _base_row(
            play_type="pass",
            down=3,
            week=1,
            interception=1,
            interception_player_id="CB1",
            return_yards=45,
            return_touchdown=1,
            touchdown=1,
            td_team="BBB",
            td_player_id="CB1",
        ),
        # A tackle-for-loss (separate from the sack play): LB1 stops the run for -2
        _base_row(
            play_type="run",
            down=1,
            yards_gained=-2,
            tackled_for_loss=1,
            tackle_for_loss_1_player_id="LB1",
        ),
        # A second week for LB1 (season-collapse exercise): another solo tackle
        _base_row(
            game_id="2023_02_AAA_BBB",
            week=2,
            play_type="run",
            down=1,
            yards_gained=4,
            solo_tackle_1_player_id="LB1",
        ),
    ]
    return pl.DataFrame(rows, infer_schema_length=None)


def _kicking_pbp() -> pl.DataFrame:
    """One week: 47-yard made FG, 52-yard missed FG, blocked PAT."""
    rows = [
        _base_row(
            play_type="field_goal",
            down=None,
            field_goal_attempt=1,
            field_goal_result="made",
            kick_distance=47,
            kicker_player_id="K1",
            kicker_player_name="K.One",
            fixed_drive=1,
        ),
        _base_row(
            play_type="field_goal",
            down=None,
            field_goal_attempt=1,
            field_goal_result="missed",
            kick_distance=52,
            kicker_player_id="K1",
            kicker_player_name="K.One",
            fixed_drive=2,
        ),
        _base_row(
            play_type="extra_point",
            down=None,
            extra_point_attempt=1,
            extra_point_result="blocked",
            kicker_player_id="K1",
            kicker_player_name="K.One",
            fixed_drive=3,
        ),
        # A second week for K1 (season-collapse exercise): one made FG
        _base_row(
            game_id="2023_02_AAA_BBB",
            week=2,
            play_type="field_goal",
            down=None,
            field_goal_attempt=1,
            field_goal_result="made",
            kick_distance=30,
            kicker_player_id="K1",
            kicker_player_name="K.One",
            fixed_drive=1,
        ),
    ]
    return pl.DataFrame(rows, infer_schema_length=None)


# ---------------------------------------------------------------------------
# defense
# ---------------------------------------------------------------------------


def test_def_split_sack_half_credit():
    df = build_nfl_player_stats_def(_def_pbp(), weekly=True)
    de1 = df.filter(pl.col("player_id") == "DE1").to_dicts()[0]
    de2 = df.filter(pl.col("player_id") == "DE2").to_dicts()[0]
    assert de1["def_sacks"] == 0.5
    assert de2["def_sacks"] == 0.5
    assert de1["def_sack_yards"] == 4.0  # 0.5 * 8
    assert de2["def_sack_yards"] == 4.0


def test_def_solo_and_assist_tackle():
    df = build_nfl_player_stats_def(_def_pbp(), weekly=True)
    lb1_wk1 = df.filter((pl.col("player_id") == "LB1") & (pl.col("week") == 1)).to_dicts()[0]
    lb2 = df.filter(pl.col("player_id") == "LB2").to_dicts()[0]
    assert lb1_wk1["def_tackles_solo"] == 1
    assert lb1_wk1["def_tackles"] == 1  # solo + tackle_with_assist (0 here)
    assert lb2["def_tackle_assists"] == 1
    assert lb1_wk1["def_tackles_for_loss"] == 1
    assert lb1_wk1["def_tackles_for_loss_yards"] == 2


def test_def_interception_return_td():
    df = build_nfl_player_stats_def(_def_pbp(), weekly=True)
    cb1 = df.filter(pl.col("player_id") == "CB1").to_dicts()[0]
    assert cb1["def_interceptions"] == 1
    assert cb1["def_interception_yards"] == 45
    assert cb1["def_tds"] == 1
    assert cb1["team"] == "BBB"


def test_def_weekly_vs_season_collapse():
    weekly = build_nfl_player_stats_def(_def_pbp(), weekly=True)
    season = build_nfl_player_stats_def(_def_pbp(), weekly=False)

    lb1_weekly_solo = weekly.filter(pl.col("player_id") == "LB1")["def_tackles_solo"].sum()
    lb1_season = season.filter(pl.col("player_id") == "LB1").to_dicts()[0]
    assert lb1_season["def_tackles_solo"] == lb1_weekly_solo == 2
    assert lb1_season["games"] == 2
    # R's season grain groups on (player_id, team) only -- no season column.
    assert "season" not in season.columns
    assert "season" in weekly.columns
    assert "week" not in season.columns
    assert "season_type" not in season.columns
    assert "games" in season.columns


def test_def_empty_frame_schema():
    empty_weekly = build_nfl_player_stats_def(pl.DataFrame(), weekly=True)
    empty_season = build_nfl_player_stats_def(pl.DataFrame(), weekly=False)
    assert empty_weekly.height == 0
    assert empty_season.height == 0
    assert "week" in empty_weekly.columns
    assert "season" in empty_weekly.columns
    assert "season" not in empty_season.columns
    assert "games" in empty_season.columns
    assert "def_sacks" in empty_weekly.columns
    assert "def_sacks" in empty_season.columns


# ---------------------------------------------------------------------------
# kicking
# ---------------------------------------------------------------------------


def test_kicking_made_missed_blocked_buckets():
    df = build_nfl_player_stats_kicking(_kicking_pbp(), weekly=True)
    wk1 = df.filter(pl.col("week") == 1).to_dicts()[0]
    assert wk1["fg_made"] == 1
    assert wk1["fg_missed"] == 1
    assert wk1["fg_att"] == 2
    assert wk1["fg_made_40_49"] == 1
    assert wk1["fg_missed_50_59"] == 1
    assert wk1["fg_long"] == 47
    assert wk1["fg_pct"] == 0.5
    assert wk1["pat_blocked"] == 1
    assert wk1["pat_att"] == 1
    assert wk1["fg_made_list"] == "47"
    assert wk1["fg_missed_list"] == "52"


def test_kicking_weekly_vs_season_collapse():
    weekly = build_nfl_player_stats_kicking(_kicking_pbp(), weekly=True)
    season = build_nfl_player_stats_kicking(_kicking_pbp(), weekly=False)

    assert weekly.filter(pl.col("week") == 1)["fg_made"].sum() == 1
    assert weekly.filter(pl.col("week") == 2)["fg_made"].sum() == 1

    k1 = season.filter(pl.col("player_id") == "K1").to_dicts()[0]
    assert k1["fg_made"] == 2
    assert k1["fg_att"] == 3
    assert k1["games"] == 2
    # R's season grain groups on (player_id, team) only -- no season column.
    assert "season" not in season.columns
    assert "season" in weekly.columns
    assert "week" not in season.columns
    assert "gwfg_distance" not in season.columns
    assert "gwfg_distance_list" in season.columns
    assert "gwfg_distance_list" not in weekly.columns
    assert "gwfg_distance" in weekly.columns

    # Positive GWFG: week 2's lone made 30-yarder is the only FG on the
    # team's final drive with score_differential in [-2, 0] (week 1's final
    # drive ends on the PAT, so it never qualifies).
    wk2 = weekly.filter(pl.col("week") == 2).to_dicts()[0]
    assert wk2["gwfg_att"] == 1
    assert wk2["gwfg_made"] == 1
    assert wk2["gwfg_distance"] == [30.0]
    assert weekly.filter(pl.col("week") == 1).to_dicts()[0]["gwfg_att"] == 0
    assert k1["gwfg_att"] == 1
    assert k1["gwfg_made"] == 1
    assert k1["gwfg_distance_list"] == "30"


def test_kicking_empty_frame_schema():
    empty_weekly = build_nfl_player_stats_kicking(pl.DataFrame(), weekly=True)
    empty_season = build_nfl_player_stats_kicking(pl.DataFrame(), weekly=False)
    assert empty_weekly.height == 0
    assert empty_season.height == 0
    assert "fg_made" in empty_weekly.columns
    assert "season" in empty_weekly.columns
    assert "season" not in empty_season.columns
    assert "games" in empty_season.columns
    assert "gwfg_distance" in empty_weekly.columns
    assert "gwfg_distance_list" in empty_season.columns
