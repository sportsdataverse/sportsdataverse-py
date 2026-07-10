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


def _two_halves():
    # Game G1, top 1st then bottom 1st -- the single-half fixture above can't
    # exercise the cross-half-inning score carry (over("game_id") vs
    # over(half_grp) are identical within one half).
    #  top    PA0: away HR (away 1-0), 0 out after
    #  top    PA1: strikeout, 1 out after
    #  bottom PA2: home HR (tie 1-1), 0 out after   <-- FIRST PA of a new half
    #  bottom PA3: strikeout, 1 out after
    return pl.DataFrame(
        {
            "game_id": ["G1", "G1", "G1", "G1"],
            "about_inning": [1, 1, 1, 1],
            "about_half_inning": ["top", "top", "bottom", "bottom"],
            "about_at_bat_index": [0, 1, 2, 3],
            "count_outs": [0, 1, 0, 1],
            "result_home_score": [0, 0, 1, 1],
            "result_away_score": [1, 1, 1, 1],
            "matchup_post_on_first_id": [None, None, None, None],
            "matchup_post_on_second_id": [None, None, None, None],
            "matchup_post_on_third_id": [None, None, None, None],
        }
    )


def test_cross_half_inning_score_carry_and_pre_play_score_diff():
    rows = pbp_base_out_states(_two_halves()).sort("at_bat_index").to_dicts()
    # runs_on_play must carry the score across the half boundary: PA2 (bottom
    # 1st, first PA of the half) scored 1, NOT 2. With over(half_grp) + fill 0
    # it would read 2 (total - 0); over("game_id") gives the correct 2 - 1 = 1.
    assert [r["runs_on_play"] for r in rows] == [1, 0, 1, 0]
    # score_diff is START-OF-PA (pre-play): away leads 1-0 going into PA1 and
    # PA2; the tie has not yet happened at the start of PA2 (post-play of PA2).
    assert [r["score_diff"] for r in rows] == [0, -1, -1, 0]
    # base/outs reset at the half boundary (PA2 starts fresh at ___/0).
    assert [r["base_state"] for r in rows] == ["___", "___", "___", "___"]
    assert [r["outs_start"] for r in rows] == [0, 0, 0, 0]


def test_run_sum_invariant_over_corpus():
    # The load-bearing invariant the README + capture script claim: within a
    # game, sum(runs_on_play) equals the game's final total. Exercises the
    # cross-half-inning carry over thousands of real multi-half games (with
    # over(half_grp) instead of over("game_id"), this fails wholesale).
    pbp = pl.read_parquet("tests/fixtures/mlb_game_state/pbp_corpus.parquet")
    results = pl.read_parquet("tests/fixtures/mlb_game_state/results_corpus.parquet")
    states = pbp_base_out_states(pbp)
    per_game = states.group_by("game_id").agg(pl.col("runs_on_play").sum().alias("runs_sum"))
    assert per_game.schema["game_id"] == results.schema["game_id"]
    chk = per_game.join(
        results.with_columns((pl.col("home_score") + pl.col("away_score")).alias("final_total")),
        on="game_id",
        how="inner",
    )
    assert chk.height >= 4600, f"only {chk.height} games joined -- corpus/results regression"
    mismatches = chk.filter(pl.col("runs_sum") != pl.col("final_total"))
    assert mismatches.height == 0, f"{mismatches.height} games where sum(runs_on_play) != final total"


def test_run_value_uses_matrix():
    m = pl.DataFrame({"base_state": ["___", "1__"], "outs": [0, 0], "re": [0.48, 0.86], "n": [1, 1]})
    # single with nobody on, 0 out: state ___/0 -> 1__/0, 0 runs => 0.86 - 0.48 + 0 = 0.38
    assert abs(run_value("___", 0, "1__", 0, 0, m) - 0.38) < 1e-9


def test_matrix_from_synthetic_pbp_has_24_states():
    pbp = pl.read_parquet("tests/fixtures/mlb_game_state/pbp_corpus.parquet")
    m = mlb_run_expectancy_matrix(pbp=pbp)
    assert set(m.columns) == {"base_state", "outs", "re", "n"}
    assert m.height <= 24 and m["re"].min() >= 0.0
