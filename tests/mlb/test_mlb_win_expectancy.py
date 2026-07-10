"""Tests for the empirical win-expectancy table, per-play WE, WPA, and leverage."""

import polars as pl

from sportsdataverse.mlb.mlb_run_expectancy import pbp_base_out_states
from sportsdataverse.mlb.mlb_win_expectancy import build_we_table, mlb_win_expectancy, mlb_win_probability_added


def _blowout_pbp():
    # One game, two PAs in the bottom of the 8th (not excluded -- only
    # bottom-9th+ is excluded from RE24, and WE has no such exclusion). PA0
    # brings the score to 6-0; PA1 then has a genuine START-OF-PA (pre-play)
    # score_diff of +6, bases empty, 0 outs -- the lopsided state the table
    # test keys on. (score_diff is pre-play, so a single-PA game would show a
    # 0-0 pre-play diff -- the leading PA0 is what establishes the +6.)
    return pl.DataFrame(
        {
            "game_id": ["G1", "G1"],
            "about_inning": [8, 8],
            "about_half_inning": ["bottom", "bottom"],
            "about_at_bat_index": [0, 1],
            "count_outs": [0, 1],
            "result_home_score": [6, 6],
            "result_away_score": [0, 0],
            "matchup_post_on_first_id": [None, None],
            "matchup_post_on_second_id": [None, None],
            "matchup_post_on_third_id": [None, None],
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
    # Each _blowout_pbp() is a 2-PA game -> 2 state rows; relabel each pair as
    # its own game G0..G19 (40 rows, 2 per game).
    states = pl.concat(
        [pbp_base_out_states(_blowout_pbp()) for _ in range(20)],
        how="vertical",
    ).with_columns(pl.Series("game_id", [f"G{i}" for i in range(20) for _ in range(2)]))
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
