"""Markov-chain RE24 (``mlb_base_out_transitions`` + ``mlb_markov_run_expectancy``) on real pbp.

Corpus: tests/fixtures/mlb_game_state/pbp_corpus.parquet (statsapi, April-June 1999-2002;
4,697 games, 352,346 counted plate appearances -- see the fixture README). Observed on it
when the gates were set:

  * Markov vs empirical RE24 (``mlb_run_expectancy_matrix``, same games): max per-state
    |diff| 0.0546 (``_23``/0, n = 1,081), mean diff -0.0004, Pearson r 0.99962.
  * Markov vs Tango's published table: max |diff| 0.0438, r 0.99960.

Gates (set from those values, never loosened -- a breach means re-derive, not widen):
|diff| <= 0.06, r >= 0.999, |mean diff| <= 0.005, and Tango <= 0.05 (the floor the
empirical oracle gate already holds). The two estimates differ by sampling, not by
method: the empirical cell averages only the PAs that start in it, the Markov cell pools
every transition downstream of it.
"""

import numpy as np
import polars as pl
import pytest
from polars.testing import assert_frame_equal

from sportsdataverse.mlb.mlb_run_expectancy import (
    MLB_PA_EVENT_MAP,
    MLB_PA_OUTCOMES,
    mlb_base_out_transitions,
    mlb_markov_run_expectancy,
    mlb_run_expectancy_matrix,
    run_value,
)

FIXTURE_DIR = "tests/fixtures/mlb_game_state"
EXTRA_INNINGS_FIXTURE = f"{FIXTURE_DIR}/pbp_extra_innings_2023.parquet"


@pytest.fixture(scope="module")
def corpus():
    return pl.read_parquet(f"{FIXTURE_DIR}/pbp_corpus.parquet")


@pytest.fixture(scope="module")
def corpus_transitions(corpus):
    return mlb_base_out_transitions(corpus)


def _halves(pbp):
    """Independent per-half summary from the raw pbp: PA count, final outs, late-bottom flag."""
    return (
        pbp.unique(subset=["game_id", "about_at_bat_index"], keep="first")
        .sort("game_id", "about_at_bat_index")
        .group_by("game_id", "about_inning", "about_half_inning", maintain_order=True)
        .agg(pl.len().alias("pa"), pl.col("count_outs").last().alias("outs_end"))
        .with_columns(((pl.col("about_half_inning") == "bottom") & (pl.col("about_inning") >= 9)).alias("late_bottom"))
    )


def _n(transitions, base_state, outs):
    rows = transitions.filter((pl.col("from_base_state") == base_state) & (pl.col("from_outs") == outs))
    return rows["n"].sum()


def test_markov_re24_matches_empirical_re24(corpus, corpus_transitions):
    markov = mlb_markov_run_expectancy(corpus_transitions)
    empirical = mlb_run_expectancy_matrix(pbp=corpus)
    assert markov.schema["base_state"] == empirical.schema["base_state"]
    assert markov.schema["outs"] == empirical.schema["outs"]
    j = markov.join(empirical, on=["base_state", "outs"], how="inner", validate="1:1", suffix="_emp")
    assert j.height == 24
    diff = j["re"] - j["re_emp"]
    max_diff = diff.abs().max()
    corr = np.corrcoef(j["re"].to_numpy(), j["re_emp"].to_numpy())[0, 1]
    assert max_diff <= 0.06, f"max per-state |markov - empirical| = {max_diff:.4f} (gate 0.06, observed 0.0546)"
    assert abs(diff.mean()) <= 0.005, f"mean markov - empirical = {diff.mean():.4f} (gate 0.005)"
    assert corr >= 0.999, f"markov/empirical correlation {corr:.5f} (gate 0.999, observed 0.99962)"


def test_markov_re24_keeps_known_ordering(corpus_transitions):
    markov = mlb_markov_run_expectancy(corpus_transitions).sort("re", descending=True)
    assert (markov["base_state"][0], markov["outs"][0]) == ("123", 0)
    assert (markov["base_state"][-1], markov["outs"][-1]) == ("___", 2)
    for bs in markov["base_state"].unique().to_list():
        r = markov.filter(pl.col("base_state") == bs).sort("outs")["re"].to_list()
        assert all(a > b for a, b in zip(r, r[1:])), f"RE not decreasing in outs for {bs!r}: {r}"


def test_markov_re24_matches_tango(corpus_transitions):
    tango = pl.read_parquet(f"{FIXTURE_DIR}/re24_tango_book.parquet")
    j = mlb_markov_run_expectancy(corpus_transitions).join(tango, on=["base_state", "outs"], validate="1:1")
    assert j.height == 24
    max_diff = (j["re"] - j["re_right"]).abs().max()
    assert max_diff <= 0.05, f"max per-state |markov - tango| = {max_diff:.4f} (gate 0.05, observed 0.0438)"


def test_transition_counts_conserve_plate_appearances(corpus, corpus_transitions):
    halves = _halves(corpus)
    counted = halves.filter(~pl.col("late_bottom") & (pl.col("outs_end") == 3))
    # Every counted PA departs exactly once; every counted half is absorbed exactly once.
    assert corpus_transitions["n"].sum() == counted["pa"].sum()
    absorbed = corpus_transitions.filter(pl.col("to_outs") == 3)
    assert absorbed["n"].sum() == counted.height
    assert absorbed["to_base_state"].unique().to_list() == ["___"]
    assert (corpus_transitions["to_outs"] >= corpus_transitions["from_outs"]).all()
    sums = corpus_transitions.group_by("from_base_state", "from_outs").agg(pl.col("prob").sum())
    assert sums.height == 24
    assert ((sums["prob"] - 1.0).abs() < 1e-12).all()


def test_walk_offs_and_incomplete_halves_are_excluded(corpus, corpus_transitions):
    halves = _halves(corpus)
    walk_offs = halves.filter(pl.col("late_bottom") & (pl.col("outs_end") < 3))
    assert walk_offs.height > 0  # the corpus does hold walk-offs (413)
    # Outside the late bottom halves the only difference from the empirical matrix's PA count
    # is the incomplete halves (suspended/shortened games), which never reach 3 outs.
    incomplete = halves.filter(~pl.col("late_bottom") & (pl.col("outs_end") != 3))
    empirical = mlb_run_expectancy_matrix(pbp=corpus.unique(subset=["game_id", "about_at_bat_index"], keep="first"))
    assert empirical["n"].sum() - corpus_transitions["n"].sum() == incomplete["pa"].sum()


def test_truncated_half_drops_out_whole():
    pbp = pl.read_parquet(EXTRA_INNINGS_FIXTURE)
    full = mlb_base_out_transitions(pbp)
    half = pbp.filter(
        (pl.col("game_id") == "718735") & (pl.col("about_inning") == 3) & (pl.col("about_half_inning") == "top")
    )
    last = half["about_at_bat_index"].max()
    cut = mlb_base_out_transitions(
        pbp.filter(~((pl.col("game_id") == "718735") & (pl.col("about_at_bat_index") == last)))
    )
    assert full["n"].sum() - cut["n"].sum() == half.height
    assert full.filter(pl.col("to_outs") == 3)["n"].sum() - cut.filter(pl.col("to_outs") == 3)["n"].sum() == 1


def test_extra_inning_halves_open_with_the_automatic_runner():
    # 718735 top 10 and 718511 top 10/11 are counted (bottom 10/11 are late bottom halves).
    pbp = pl.read_parquet(EXTRA_INNINGS_FIXTURE)
    seeded = mlb_base_out_transitions(pbp)
    unseeded = mlb_base_out_transitions(pbp.drop("matchup_batter_id", "runners"))
    assert _n(seeded, "_2_", 0) - _n(unseeded, "_2_", 0) == 3
    assert _n(unseeded, "___", 0) - _n(seeded, "___", 0) == 3
    # 718511 top 10 is followed (once the late bottom halves are dropped) by top 11, which opens
    # on "_2_": the 3-out rows must not read that next half's runner.
    assert seeded.filter(pl.col("to_outs") == 3)["to_base_state"].unique().to_list() == ["___"]


def test_sparse_rows_kept_and_unobserved_states_null(corpus_transitions):
    # A triple seen once is kept, with its share of the from-state's departures.
    singles = corpus_transitions.filter(pl.col("n") == 1)
    assert singles.height > 0
    totals = corpus_transitions.group_by("from_base_state", "from_outs").agg(pl.col("n").sum().alias("n_from"))
    chk = singles.join(totals, on=["from_base_state", "from_outs"])
    assert ((chk["prob"] - 1.0 / chk["n_from"]).abs() < 1e-15).all()

    # Two real 2023 games never reach bases loaded with 0 outs: null RE, not 0 runs.
    tiny = mlb_markov_run_expectancy(mlb_base_out_transitions(pl.read_parquet(EXTRA_INNINGS_FIXTURE)))
    assert tiny.height == 24
    loaded0 = tiny.filter((pl.col("base_state") == "123") & (pl.col("outs") == 0))
    assert loaded0["n"].to_list() == [0]
    assert loaded0["re"].to_list() == [None]
    assert tiny.filter(pl.col("n") > 0)["re"].null_count() == 0


def test_transition_into_unobserved_state_raises(corpus_transitions):
    no_departures = corpus_transitions.filter(~((pl.col("from_base_state") == "_23") & (pl.col("from_outs") == 1)))
    with pytest.raises(ValueError, match="no observed departures"):
        mlb_markov_run_expectancy(no_departures)


def test_empty_input():
    empty = mlb_base_out_transitions(pl.DataFrame())
    assert empty.height == 0
    out = mlb_markov_run_expectancy(empty)
    assert out.height == 24
    assert out["re"].null_count() == 24
    assert out["n"].sum() == 0


# Real statsapi capture, 2023 opening day and day two (20 games, 1,508 PAs) with result_event_type.
OPENING_FIXTURE = f"{FIXTURE_DIR}/pbp_opening_2023.parquet"


def test_by_event_sums_to_the_default_frame():
    pbp = pl.read_parquet(OPENING_FIXTURE)
    default = mlb_base_out_transitions(pbp)
    by_event = mlb_base_out_transitions(pbp, by_event=True)
    assert by_event.columns == ["from_base_state", "from_outs", "event", *default.columns[2:]]
    triple = ["from_base_state", "from_outs", "to_base_state", "to_outs", "runs"]
    summed = (
        by_event.group_by(triple)
        .agg(pl.col("n").sum())
        .with_columns((pl.col("n") / pl.col("n").sum().over("from_base_state", "from_outs")).alias("prob"))
        .sort("from_outs", "from_base_state", "to_outs", "to_base_state", "runs")
    )
    assert_frame_equal(summed, default)
    # The chain does not care how it is keyed.
    assert_frame_equal(mlb_markov_run_expectancy(by_event), mlb_markov_run_expectancy(default))


def test_by_event_prob_and_event_classes():
    by_event = mlb_base_out_transitions(pl.read_parquet(OPENING_FIXTURE), by_event=True)
    sums = by_event.group_by("from_base_state", "from_outs", "event").agg(pl.col("prob").sum())
    assert ((sums["prob"] - 1.0).abs() < 1e-12).all()
    assert set(by_event["event"].unique()) <= set(MLB_PA_OUTCOMES)
    assert set(MLB_PA_EVENT_MAP.values()) <= set(MLB_PA_OUTCOMES)
    # field_out / force_out / sac_bunt / fielders_choice_out are not in the map: outs in play.
    in_play = by_event.filter(pl.col("event") == "out_inplay")
    assert in_play["n"].sum() == 562  # of 580 such PAs, the rest in late bottom halves
    assert (in_play["to_outs"] > in_play["from_outs"]).all()
    # A home run always clears the bases and scores the batter.
    hr = by_event.filter(pl.col("event") == "hr")
    assert (hr["to_base_state"] == "___").all() and (hr["runs"] >= 1).all()


def test_by_event_needs_the_event_column(corpus):
    with pytest.raises(ValueError, match="result_event_type"):
        mlb_base_out_transitions(corpus, by_event=True)
    assert "event" in mlb_base_out_transitions(pl.DataFrame(), by_event=True).columns


def test_run_value_on_a_markov_matrix_with_an_unobserved_state():
    # Two real 2023 games never reach bases loaded with 0 outs, so that row's RE is null.
    tiny = mlb_markov_run_expectancy(mlb_base_out_transitions(pl.read_parquet(EXTRA_INNINGS_FIXTURE)))
    assert np.isnan(run_value("123", 0, "___", 3, 0, tiny))
    assert np.isnan(run_value("12_", 0, "123", 0, 0, tiny))
    re = {(r["base_state"], r["outs"]): r["re"] for r in tiny.to_dicts()}
    assert run_value("___", 0, "1__", 0, 0, tiny) == pytest.approx(re[("1__", 0)] - re[("___", 0)])


def test_run_value_on_an_empirical_matrix_missing_a_state():
    # The empirical matrix emits only observed states: a missing row is NaN, as a null Markov row is, never 0.0.
    pbp = pl.read_parquet(EXTRA_INNINGS_FIXTURE)
    emp = mlb_run_expectancy_matrix(pbp=pbp)
    assert emp.filter((pl.col("base_state") == "123") & (pl.col("outs") == 0)).height == 0
    assert np.isnan(run_value("12_", 0, "123", 0, 0, emp))
    re = {(r["base_state"], r["outs"]): r["re"] for r in emp.to_dicts()}
    assert run_value("___", 0, "1__", 0, 0, emp) == pytest.approx(re[("1__", 0)] - re[("___", 0)])
