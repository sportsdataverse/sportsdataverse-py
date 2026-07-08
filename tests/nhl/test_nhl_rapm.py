"""Offline tests for the shift-stint builder + skater xG RAPM (weighted sparse ridge)."""

from __future__ import annotations

from pathlib import Path

import polars as pl

from sportsdataverse.nhl.nhl_rapm import build_design, build_stints, nhl_skater_rapm

FIX = Path(__file__).parent.parent / "fixtures" / "nhl_player_impact"
MODELS = FIX / "xg_models"


def _synthetic_shifts() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "game_id": [999, 999, 999, 999],
            "period": [1, 1, 1, 1],
            "game_seconds": [0, 0, 100, 200],
            "event_team": ["Buffalo Sabres", "New Jersey Devils", "Buffalo Sabres", "New Jersey Devils"],
            "ids_on": ["1, 2, 3", "4, 5, 6", "7", "8"],
            "ids_off": ["0", "0", "1", "4"],
        }
    ).with_columns(pl.col("game_id").cast(pl.Int64))


def _synthetic_scored() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "game_id": [999],
            "event_id": [1],
            "event_type": ["SHOT"],
            "game_seconds": [250],
            "event_team_abbr": ["BUF"],
            "home_abbr": ["BUF"],
            "away_abbr": ["NJD"],
            "strength_state": ["5v5"],
            "home_goalie_id": [111],
            "away_goalie_id": [222],
            "xg": [0.7],
        }
    ).with_columns(pl.col("game_id").cast(pl.Int64), pl.col("event_id").cast(pl.Int64))


def test_build_stints_folds_two_changes_into_three_intervals():
    stints = build_stints(_synthetic_shifts(), _synthetic_scored())
    stints = stints.sort("start_s")
    assert stints.height == 3
    durations = stints["duration"].to_list()
    assert durations == [100, 100, 51]

    first, second, third = stints.to_dicts()
    assert sorted(first["home_ids"]) == [1, 2, 3]
    assert sorted(first["away_ids"]) == [4, 5, 6]
    assert sorted(second["home_ids"]) == [2, 3, 7]
    assert sorted(second["away_ids"]) == [4, 5, 6]
    assert sorted(third["home_ids"]) == [2, 3, 7]
    assert sorted(third["away_ids"]) == [5, 6, 8]

    # The single xG event (game_seconds=250, BUF/home) lands in the 3rd interval only.
    assert first["xgf_home"] in (0.0, None)
    assert second["xgf_home"] in (0.0, None)
    assert abs(third["xgf_home"] - 0.7) < 1e-9


def test_build_stints_as_of_truncates_later_intervals():
    stints = build_stints(_synthetic_shifts(), _synthetic_scored(), as_of=200)
    assert stints.height == 2
    assert stints["start_s"].max() < 200


def test_build_stints_empty_shifts_returns_zero_row_frame():
    out = build_stints(pl.DataFrame(), _synthetic_scored())
    assert out.height == 0


def test_build_design_two_rows_per_stint_weighted_by_duration():
    stints = pl.DataFrame(
        {
            "game_id": [1, 1],
            "period": [1, 1],
            "start_s": [0, 100],
            "end_s": [100, 220],
            "duration": [100, 120],
            "home_ids": [[10, 11], [10, 12]],
            "away_ids": [[20, 21], [20, 21]],
            "home_goalie": [None, None],
            "away_goalie": [None, None],
            "strength_state": ["5v5", "5v5"],
            "xgf_home": [1.0, 1.2],
            "xgf_away": [0.5, 0.6],
        }
    )
    X, y, w, player_index = build_design(stints)
    assert X.shape[0] == 4  # two rows (home-attacking, away-attacking) per stint
    assert len(y) == 4
    assert w.tolist() == [100, 100, 120, 120]
    assert set(player_index) == {10, 11, 12, 20, 21}


def test_nhl_skater_rapm_recovers_dominant_skater_ordering():
    # Skater 999 is always on the ice (both teams' stints) whenever xGF is high,
    # regardless of teammates -- the ridge should rank them highest offensively.
    rng_ids_a = [1, 2, 999]
    rng_ids_b = [3, 4, 999]
    stints = pl.DataFrame(
        {
            "game_id": [1, 1, 1, 1],
            "period": [1, 1, 1, 1],
            "start_s": [0, 100, 200, 300],
            "end_s": [100, 200, 300, 400],
            "duration": [100, 100, 100, 100],
            "home_ids": [rng_ids_a, rng_ids_b, [5, 6, 7], [8, 9, 10]],
            "away_ids": [[50, 51, 52], [50, 51, 52], [50, 51, 52], [50, 51, 52]],
            "home_goalie": [None, None, None, None],
            "away_goalie": [None, None, None, None],
            "strength_state": ["5v5", "5v5", "5v5", "5v5"],
            "xgf_home": [3.0, 3.0, 0.2, 0.2],
            "xgf_away": [0.1, 0.1, 0.1, 0.1],
        }
    )
    rapm = nhl_skater_rapm(pl.DataFrame(), pl.DataFrame(), _stints=stints, lam=10.0)
    top = rapm.sort("xg_rapm_off", descending=True).head(1)
    assert top["player_id"][0] == 999


def test_nhl_skater_rapm_empty_input_returns_zero_row_frame():
    out = nhl_skater_rapm(pl.DataFrame(), pl.DataFrame())
    assert out.height == 0
    assert set(out.columns) == {"player_id", "xg_rapm_off", "xg_rapm_def", "xg_rapm", "toi_minutes"}
