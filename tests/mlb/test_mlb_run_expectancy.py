"""Tests for the RE24 base-out-state substrate + run-expectancy matrix."""

import polars as pl

from sportsdataverse.mlb.mlb_run_expectancy import mlb_run_expectancy_matrix, pbp_base_out_states, run_value


def _half():
    # One (game G1, inning 1, top) half-inning, 3 PAs:
    #  PA0: bases empty, 0 out -> single (runner to 1st), 0 runs, 0 out after
    #  PA1: runner on 1st, 0 out -> HR (2 runs), bases empty after, 0 out after
    #  PA2: bases empty, 0 out -> strikeout, 0 runs, 1 out after
    return pl.DataFrame(
        {
            "game_id": ["G1", "G1", "G1"],
            "about_inning": [1, 1, 1],
            "about_half_inning": ["top", "top", "top"],
            "about_at_bat_index": [0, 1, 2],
            "count_outs": [0, 0, 1],
            "result_home_score": [0, 0, 0],
            "result_away_score": [0, 2, 2],
            "matchup_post_on_first_id": ["10", None, None],
            "matchup_post_on_second_id": [None, None, None],
            "matchup_post_on_third_id": [None, None, None],
        }
    )


def test_base_out_reconstruction():
    out = pbp_base_out_states(_half()).sort("at_bat_index")
    rows = out.to_dicts()
    assert [r["base_state"] for r in rows] == ["___", "1__", "___"]
    assert [r["outs_start"] for r in rows] == [0, 0, 0]
    assert [r["runs_on_play"] for r in rows] == [0, 2, 0]
    # runs rest of inning from each PA (suffix sum): 2, 2, 0
    assert [r["runs_rest_of_inning"] for r in rows] == [2, 2, 0]


def test_run_value_uses_matrix():
    m = pl.DataFrame({"base_state": ["___", "1__"], "outs": [0, 0], "re": [0.48, 0.86], "n": [1, 1]})
    # single with nobody on, 0 out: state ___/0 -> 1__/0, 0 runs => 0.86 - 0.48 + 0 = 0.38
    assert abs(run_value("___", 0, "1__", 0, 0, m) - 0.38) < 1e-9


def test_matrix_from_synthetic_pbp_has_24_states():
    pbp = pl.read_parquet("tests/fixtures/mlb_game_state/pbp_corpus.parquet")
    m = mlb_run_expectancy_matrix(pbp=pbp)
    assert set(m.columns) == {"base_state", "outs", "re", "n"}
    assert m.height <= 24 and m["re"].min() >= 0.0
