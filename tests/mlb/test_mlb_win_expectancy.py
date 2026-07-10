"""Tests for the empirical win-expectancy table, per-play WE, WPA, and leverage."""

import polars as pl

from sportsdataverse.mlb.mlb_run_expectancy import pbp_base_out_states
from sportsdataverse.mlb.mlb_win_expectancy import build_we_table, mlb_win_expectancy, mlb_win_probability_added


def _blowout_pbp():
    # One game, a single PA in the bottom of the 8th (not excluded -- only
    # bottom-9th+ is excluded from RE24, and WE has no such exclusion), home
    # team up 6, bases empty. A single-PA half-inning keeps outs_start pinned
    # to 0 (the shift fill-value) so every synthetic copy lands in the same
    # (inning_capped, half, base_state, outs_start, score_diff_bucket) group.
    return pl.DataFrame(
        {
            "game_id": ["G1"],
            "about_inning": [8],
            "about_half_inning": ["bottom"],
            "about_at_bat_index": [0],
            "count_outs": [2],
            "result_home_score": [6],
            "result_away_score": [0],
            "matchup_post_on_first_id": [None],
            "matchup_post_on_second_id": [None],
            "matchup_post_on_third_id": [None],
        }
    )


def _results_blowout():
    return pl.DataFrame(
        {
            "game_id": ["G1"],
            "home_score": [8],
            "away_score": [1],
        }
    )


def test_we_table_and_lopsided_state_near_extremes():
    # Build a table from many repeated blowout observations plus one tie-state
    # observation, so both extremes are populated with n > 1.
    states = pl.concat(
        [pbp_base_out_states(_blowout_pbp()) for _ in range(20)],
        how="vertical",
    ).with_columns(pl.Series("game_id", [f"G{i}" for i in range(20)]))
    results = pl.DataFrame({"game_id": [f"G{i}" for i in range(20)], "home_score": [8] * 20, "away_score": [1] * 20})
    table = build_we_table(states, results)
    assert set(table.columns) == {
        "inning_capped",
        "half",
        "base_state",
        "outs_start",
        "score_diff_bucket",
        "home_win_exp",
        "n",
    }
    row = table.filter(pl.col("score_diff_bucket") == 6)
    assert row.height == 1
    assert row["home_win_exp"][0] > 0.9

    we = mlb_win_expectancy(_blowout_pbp(), _results_blowout())
    assert we["home_win_exp"].min() >= 0.0 and we["home_win_exp"].max() <= 1.0
    assert set(we.columns) == {"game_id", "at_bat_index", "half", "home_win_exp"}


def test_wpa_sums_to_half_for_winner():
    # synthetic one-game WE path: 0.5 -> 0.6 -> 0.8 -> 1.0 (home won)
    we = pl.DataFrame(
        {
            "game_id": ["G"] * 4,
            "at_bat_index": [0, 1, 2, 3],
            "half": ["top"] * 4,
            "home_win_exp": [0.6, 0.8, 0.9, 1.0],
        }
    )
    wpa = mlb_win_probability_added(we)
    # first PA measured vs 0.5 baseline; total = 1.0 - 0.5 = 0.5
    assert abs(wpa["wpa"].sum() - 0.5) < 1e-9
