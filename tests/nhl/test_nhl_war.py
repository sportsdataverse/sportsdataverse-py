"""Offline tests for faceoff/penalty GAR components + the GAR/WAR composite."""

from __future__ import annotations

from pathlib import Path

import polars as pl

from sportsdataverse.nhl.nhl_player_impact_constants import get_constants
from sportsdataverse.nhl.nhl_war import _faceoff_penalty_components, nhl_skater_war

FIX = Path(__file__).parent.parent / "fixtures" / "nhl_player_impact"
MODELS = FIX / "xg_models"

_CFG = get_constants("nhl")


def _synthetic_faceoff_penalty_pbp() -> pl.DataFrame:
    # Player 1 wins 3 faceoffs, loses 1 (fo_total=4, fo_won=3).
    # Player 1 draws 2 penalties, takes 1 (pens_drawn=2, pens_taken=1).
    faceoff_rows = [
        {
            "event_type": "FACEOFF",
            "event_player_1_id": 1,
            "event_player_1_type": "Winner",
            "event_player_2_id": 2,
            "event_player_2_type": "Loser",
        },
        {
            "event_type": "FACEOFF",
            "event_player_1_id": 1,
            "event_player_1_type": "Winner",
            "event_player_2_id": 2,
            "event_player_2_type": "Loser",
        },
        {
            "event_type": "FACEOFF",
            "event_player_1_id": 1,
            "event_player_1_type": "Winner",
            "event_player_2_id": 2,
            "event_player_2_type": "Loser",
        },
        {
            "event_type": "FACEOFF",
            "event_player_1_id": 2,
            "event_player_1_type": "Winner",
            "event_player_2_id": 1,
            "event_player_2_type": "Loser",
        },
    ]
    penalty_rows = [
        {
            "event_type": "PENALTY",
            "event_player_1_id": 3,
            "event_player_1_type": "PenaltyOn",
            "event_player_2_id": 1,
            "event_player_2_type": "DrewBy",
        },
        {
            "event_type": "PENALTY",
            "event_player_1_id": 3,
            "event_player_1_type": "PenaltyOn",
            "event_player_2_id": 1,
            "event_player_2_type": "DrewBy",
        },
        {
            "event_type": "PENALTY",
            "event_player_1_id": 1,
            "event_player_1_type": "PenaltyOn",
            "event_player_2_id": 3,
            "event_player_2_type": "DrewBy",
        },
    ]
    rows = faceoff_rows + penalty_rows
    return pl.DataFrame(rows).with_columns(
        pl.col("event_player_1_id").cast(pl.Int64), pl.col("event_player_2_id").cast(pl.Int64)
    )


def test_faceoff_penalty_components_exact_on_synthetic_events():
    out = _faceoff_penalty_components(_synthetic_faceoff_penalty_pbp())
    row = out.filter(pl.col("player_id") == 1)
    assert row.height == 1
    expected_fo = (3 - 0.5 * 4) * _CFG.faceoff_goal_weight
    expected_pens = (2 - 1) * _CFG.penalty_goal_weight
    assert abs(row["faceoffs_goals"][0] - expected_fo) < 1e-9
    assert abs(row["pens_goals"][0] - expected_pens) < 1e-9


def test_faceoff_penalty_components_empty_input_returns_zero_row_frame():
    out = _faceoff_penalty_components(pl.DataFrame())
    assert out.height == 0
    assert set(out.columns) == {"player_id", "faceoffs_goals", "pens_goals"}


def test_nhl_skater_war_assembles_components_into_gar_and_war(monkeypatch):
    import sportsdataverse.nhl.nhl_war as nhl_war_mod

    rapm = pl.DataFrame(
        {"player_id": [1, 2], "xg_rapm_off": [1.0, -0.5], "xg_rapm_def": [0.2, 0.1], "toi_minutes": [600.0, 500.0]}
    )
    st = pl.DataFrame(
        {
            "player_id": [1, 2],
            "pp_toi_minutes": [30.0, 10.0],
            "pk_toi_minutes": [5.0, 40.0],
            "pp_value": [1.5, 0.2],
            "pk_value": [0.1, 0.8],
        }
    )
    fp = pl.DataFrame({"player_id": [1, 2], "faceoffs_goals": [0.3, -0.1], "pens_goals": [0.2, -0.2]})

    monkeypatch.setattr(nhl_war_mod, "nhl_skater_rapm", lambda *a, **k: rapm)
    monkeypatch.setattr(nhl_war_mod, "nhl_special_teams_value", lambda *a, **k: st)
    monkeypatch.setattr(nhl_war_mod, "_faceoff_penalty_components", lambda *a, **k: fp)

    war = nhl_skater_war(pl.DataFrame({"a": [1]}), pl.DataFrame({"a": [1]}))
    assert war.height == 2
    row1 = war.filter(pl.col("player_id") == 1)
    cfg = get_constants("nhl")
    ev_off1 = (1.0 - cfg.replacement_ev_off) * 600.0 / 60.0
    ev_def1 = (0.2 - cfg.replacement_ev_def) * 600.0 / 60.0
    gar1 = ev_off1 + ev_def1 + 1.5 + 0.1 + 0.2 + 0.3
    assert abs(row1["gar"][0] - gar1) < 1e-6
    assert abs(row1["war"][0] - gar1 / cfg.goals_per_win) < 1e-6

    war_pd = nhl_skater_war(pl.DataFrame({"a": [1]}), pl.DataFrame({"a": [1]}), return_as_pandas=True)
    assert hasattr(war_pd, "to_dict")


def test_nhl_skater_war_empty_input_returns_documented_schema():
    out = nhl_skater_war(pl.DataFrame(), pl.DataFrame())
    assert out.height == 0
    assert set(out.columns) == {
        "player_id",
        "ev_off",
        "ev_def",
        "pp",
        "pk",
        "pens",
        "faceoffs",
        "gar",
        "war",
    }
