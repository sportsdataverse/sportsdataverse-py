from __future__ import annotations

import polars as pl

from sportsdataverse.nhl.nhl_expected_assists import nhl_expected_assists
from sportsdataverse.nhl.nhl_faceoff_value import nhl_faceoff_value
from sportsdataverse.nhl.nhl_penalty_value import nhl_penalty_value
from sportsdataverse.nhl.nhl_zone_transitions import nhl_zone_transitions
from sportsdataverse.nhl.pwhl_microstat import (
    pwhl_edge_skating_value,
    pwhl_expected_assists,
    pwhl_faceoff_value,
    pwhl_penalty_value,
    pwhl_zone_transitions,
)


def _row(tdk: str, **overrides: object) -> dict:
    base = {
        "game_id": "P1",
        "season": 2024,
        "event_idx": 0,
        "period": 1,
        "time_in_period": "10:00",
        "type_desc_key": tdk,
        "event_owner_team_id": "10",
        "zone_code": "O",
        "x_coord": 0.0,
        "y_coord": 0.0,
        "situation_code": "1551",
        "home_team_id": "10",
        "home_team_defending_side": "left",
        "winning_player_id": None,
        "losing_player_id": None,
        "scoring_player_id": None,
        "assist1_player_id": None,
        "assist2_player_id": None,
        "shooting_player_id": None,
        "committed_player_id": None,
        "drawn_player_id": None,
        "penalty_type_code": None,
        "shot_type": None,
    }
    base.update(overrides)
    return base


def test_shims_bind_league_and_edge_zero_row() -> None:
    assert pwhl_faceoff_value.func is nhl_faceoff_value
    assert pwhl_faceoff_value.keywords["league"] == "pwhl"
    assert pwhl_penalty_value.func is nhl_penalty_value
    assert pwhl_expected_assists.func is nhl_expected_assists
    assert pwhl_zone_transitions.func is nhl_zone_transitions
    assert pwhl_edge_skating_value.keywords["league"] == "pwhl"
    assert pwhl_edge_skating_value(season=2024).height == 0


def test_shims_produce_same_schema_as_nhl_cores() -> None:
    faceoffs = pl.DataFrame(
        [_row("faceoff", winning_player_id="A", losing_player_id="B", zone_code="O") for _ in range(4)]
    )
    assert pwhl_faceoff_value(faceoffs).columns == nhl_faceoff_value(faceoffs).columns

    pens = pl.DataFrame([_row("penalty", committed_player_id="A", drawn_player_id="B", penalty_type_code="MIN")])
    assert pwhl_penalty_value(pens).columns == nhl_penalty_value(pens).columns

    goal = _row(
        "goal", scoring_player_id="S", assist1_player_id="A", shooting_player_id="S", x_coord=80.0, shot_type="wrist"
    )
    goals = pl.DataFrame([goal])
    assert pwhl_expected_assists(goals).columns == nhl_expected_assists(goals).columns

    zones = pl.DataFrame(
        [
            _row("faceoff", zone_code="N", winning_player_id="A", time_in_period="10:00"),
            _row("shot-on-goal", zone_code="O", shooting_player_id="A", time_in_period="10:02"),
        ]
    )
    assert pwhl_zone_transitions(zones).columns == nhl_zone_transitions(zones).columns


def test_pwhl_penalty_uses_pwhl_constants() -> None:
    from sportsdataverse.nhl.nhl_microstat_constants import get_constants

    pens = pl.DataFrame([_row("penalty", committed_player_id="A", drawn_player_id="B", penalty_type_code="MIN")])
    b = pwhl_penalty_value(pens).filter(pl.col("player_id") == "B").row(0, named=True)
    assert abs(b["net_penalty_value"] - get_constants("pwhl").pp_goal_value) < 1e-9
