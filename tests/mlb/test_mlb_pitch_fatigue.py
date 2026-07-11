"""Tests for times-through-order / fatigue (model ④)."""

from __future__ import annotations

import polars as pl

from sportsdataverse.mlb.mlb_pitch_fatigue import add_velo_drop_from_start, mlb_times_through_order, tto_penalty_table


def _synthetic_tto_frame() -> pl.DataFrame:
    # TTO=1: run_value 0.0 (10 pitches); TTO=2: run_value 0.05 fixed higher gap (10 pitches).
    return pl.DataFrame(
        {
            "times_through_order": [1] * 10 + [2] * 10,
            "run_value": [0.0] * 10 + [0.05] * 10,
        }
    )


def test_tto_penalty_table_first_is_zero_second_matches_injected_gap():
    out = tto_penalty_table(_synthetic_tto_frame())
    first = out.filter(pl.col("times_through_order") == 1).row(0, named=True)
    second = out.filter(pl.col("times_through_order") == 2).row(0, named=True)
    assert abs(first["penalty_vs_first"] - 0.0) < 1e-9
    assert abs(second["penalty_vs_first"] - 0.05) < 1e-9
    assert first["n"] == 10 and second["n"] == 10


def test_tto_penalty_table_empty_input():
    out = tto_penalty_table(pl.DataFrame())
    assert out.height == 0 and "penalty_vs_first" in out.columns


def test_velo_drop_from_start_within_game():
    df = pl.DataFrame(
        {
            "pitcher": [1, 1, 1],
            "game_pk": [100, 100, 100],
            "at_bat_number": [1, 2, 3],
            "pitch_number": [1, 1, 1],
            "release_speed": [96.0, 94.0, 92.0],
        }
    )
    out = add_velo_drop_from_start(df)
    assert out["velo_drop_from_start"].to_list() == [0.0, 2.0, 4.0]


def test_mlb_times_through_order_monotone_on_real_fixture():
    from sportsdataverse.mlb.mlb_pitch_features import add_sequence_features, pitch_features

    fixture = pl.read_parquet("tests/fixtures/mlb_pitching/pitches_2024-06-15.parquet")
    feats = add_sequence_features(pitch_features(fixture))
    out = mlb_times_through_order(feats, season=2024)
    penalties = out.group_by("times_through_order").agg(pl.col("fatigue_rv_adj").first()).sort("times_through_order")
    p = penalties["fatigue_rv_adj"].to_list()
    assert len(p) >= 2
    assert all(p[i] <= p[i + 1] for i in range(len(p) - 1))


def test_mlb_times_through_order_empty_input():
    out = mlb_times_through_order(pl.DataFrame())
    assert out.height == 0 and "fatigue_rv_adj" in out.columns


def test_tto_penalty_table_on_real_fixture_monotone_and_magnitude_band():
    """Task 5.2 gate: the observed real-data ``tto_penalty_table`` on the
    2023 pitcher-season sample (raw, unconditional per-TTO run-value means --
    Task 5.1's aggregation, distinct from the OLS-fitted, confound-adjusted
    ``tto_penalty`` coefficients ``mlb_times_through_order`` applies) must be
    monotone increasing 1->2->3, with each marginal within a documented,
    observed-value-derived band.

    Observed on this fixture: penalty_vs_first = [0.0, 0.00293, 0.00447]
    (marginals 0.00293 and 0.00154), which lands close to the plan's cited
    published ~0.003-0.008 wOBA/PA reference band for a TTO penalty -- the
    raw per-pitch ``delta_run_exp`` scale and the wOBA/PA scale are not
    identical units, but are the same order of magnitude here, so the band
    below is set from the observed values with headroom, not from the
    published band verbatim. If a future run falls outside this band, debug
    the TTO derivation / velo-drop control -- do not widen the band to pass.
    """
    from sportsdataverse.mlb.mlb_pitch_features import add_sequence_features, pitch_features

    fixture = pl.read_parquet("tests/fixtures/mlb_pitching/pitcher_season_pitches_2023_sample.parquet")
    feats = add_sequence_features(pitch_features(fixture))
    tbl = tto_penalty_table(feats).sort("times_through_order")
    p = tbl["penalty_vs_first"].to_list()
    assert len(p) == 3
    assert p[0] == 0.0
    assert p[1] > p[0]
    assert p[2] > p[1]
    assert 0.001 <= p[1] <= 0.01  # TTO2-TTO1 marginal (run-value units; documented above)
    assert 0.0005 <= (p[2] - p[1]) <= 0.01  # TTO3-TTO2 marginal
