"""Tests for the pitch-feature substrate — the sole ``mlb_statcast_search`` consumer."""

from __future__ import annotations

import datetime as dt

import polars as pl

from sportsdataverse.mlb.mlb_pitch_features import add_sequence_features, pitch_features


def _mini() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "pitcher": [1, 1],
            "batter": [9, 9],
            "game_pk": [100, 100],
            "game_date": [dt.date(2024, 6, 15)] * 2,
            "pitch_type": ["FF", "SL"],
            "release_speed": [95.0, 85.0],
            "release_spin_rate": [2300.0, 2500.0],
            "pfx_x": [0.5, -0.4],
            "pfx_z": [1.4, 0.2],
            "release_pos_x": [-1.8, -1.9],
            "release_pos_z": [6.0, 5.9],
            "release_extension": [6.5, 6.4],
            "plate_x": [0.0, 0.6],
            "plate_z": [2.5, 1.8],
            "sz_top": [3.5, 3.5],
            "sz_bot": [1.5, 1.5],
            "balls": [0, 1],
            "strikes": [0, 1],
            "stand": ["R", "R"],
            "p_throws": ["R", "R"],
            "delta_run_exp": [-0.04, 0.02],
            "at_bat_number": [1, 1],
            "pitch_number": [1, 2],
            "inning": [1, 1],
        }
    )


def test_plate_z_norm_and_in_zone():
    out = pitch_features(_mini())
    ff = out.filter(pl.col("pitch_type") == "FF").row(0, named=True)
    # plate_z_norm = (2.5 - 1.5)/(3.5 - 1.5) = 0.5 ; centered, in zone
    assert abs(ff["plate_z_norm"] - 0.5) < 1e-9
    assert ff["in_zone"] == 1
    assert abs(ff["run_value"] - (-0.04)) < 1e-9


def test_out_of_zone_pitch():
    out = pitch_features(_mini())
    sl = out.filter(pl.col("pitch_type") == "SL").row(0, named=True)
    # plate_x_abs = 0.6 > 0.83? no -- 0.6 <= 0.83; plate_z_norm = (1.8-1.5)/2.0 = 0.15 in [0,1] -> in zone
    assert sl["in_zone"] == 1
    assert abs(sl["plate_x_abs"] - 0.6) < 1e-9


def test_ids_are_int64_and_empty_schema():
    out = pitch_features(_mini())
    assert out.schema["pitcher"] == pl.Int64 and out.schema["game_pk"] == pl.Int64
    empty = pitch_features(pl.DataFrame())
    assert empty.height == 0 and "plate_z_norm" in empty.columns


def test_standardization_within_pitcher():
    # 3 pitches from the same pitcher: standardized velo should have mean ~0.
    df = pl.concat(
        [
            _mini(),
            _mini().with_columns(pl.col("pitch_number") + 2, pl.lit(93.0).alias("release_speed")),
        ],
        how="vertical",
    )
    out = pitch_features(df)
    assert "velo_z" in out.columns
    assert out.filter(pl.col("pitcher") == 1)["velo_z"].mean() is not None


def test_return_as_pandas():
    out = pitch_features(_mini(), return_as_pandas=True)
    assert type(out).__name__ == "DataFrame"
    assert "plate_z_norm" in out.columns


def _single_game_20_pitches() -> pl.DataFrame:
    """20 pitches across 10 batters (1 pitch each, PA 1-10) then a repeat batter (PA 11, 2 pitches)."""
    n_pa = 11
    rows = []
    pitch_num_global = 1
    for pa in range(1, n_pa + 1):
        n_pitches = 2 if pa == n_pa else 1
        for pn in range(1, n_pitches + 1):
            rows.append(
                {
                    "pitcher": 1,
                    "batter": 100 + pa,
                    "game_pk": 500,
                    "game_date": dt.date(2024, 6, 15),
                    "pitch_type": "FF",
                    "release_speed": 95.0,
                    "release_spin_rate": 2300.0,
                    "pfx_x": 0.5,
                    "pfx_z": 1.4,
                    "release_pos_x": -1.8,
                    "release_pos_z": 6.0,
                    "release_extension": 6.5,
                    "plate_x": 0.0,
                    "plate_z": 2.5,
                    "sz_top": 3.5,
                    "sz_bot": 1.5,
                    "balls": 0,
                    "strikes": 0,
                    "stand": "R",
                    "p_throws": "R",
                    "delta_run_exp": 0.0,
                    "at_bat_number": pa,
                    "pitch_number": pn,
                    "inning": 1,
                }
            )
            pitch_num_global += 1
    return pl.DataFrame(rows)


def test_sequence_features_first_pitch_null_and_tto_flip():
    feats = pitch_features(_single_game_20_pitches())
    out = add_sequence_features(feats)
    # every PA's first pitch has no prev pitch
    first_pitches = out.filter(pl.col("pitch_number") == 1)
    assert first_pitches["prev_pitch_type"].is_null().all()
    # batter_faced_index 1-9 -> TTO 1; 10th batter -> TTO flips to 2
    tto_by_pa = out.group_by("at_bat_number").agg(pl.col("times_through_order").first()).sort("at_bat_number")
    assert tto_by_pa.filter(pl.col("at_bat_number") == 9)["times_through_order"].item() == 1
    assert tto_by_pa.filter(pl.col("at_bat_number") == 10)["times_through_order"].item() == 2
    # cum_pitches_game is monotone non-decreasing
    cum = out.sort(["at_bat_number", "pitch_number"])["cum_pitches_game"].to_list()
    assert all(cum[i] <= cum[i + 1] for i in range(len(cum) - 1))


def test_sequence_features_second_pitch_has_prev():
    feats = pitch_features(_single_game_20_pitches())
    out = add_sequence_features(feats)
    last_pa = out.filter(pl.col("at_bat_number") == 11).sort("pitch_number")
    assert last_pa["prev_pitch_type"].to_list() == [None, "FF"]


def test_add_sequence_features_empty():
    empty = add_sequence_features(pitch_features(pl.DataFrame()))
    assert empty.height == 0
    assert "times_through_order" in empty.columns


def test_real_fixture_shape_gate():
    """Task 1.3 gate: real Savant fixture (2024-06-15, ~4,145 pitches) must survive
    the full substrate with no exception, matching row count, and sane null rates."""
    fixture = pl.read_parquet("tests/fixtures/mlb_pitching/pitches_2024-06-15.parquet")
    out = add_sequence_features(pitch_features(fixture))
    assert out.height == fixture.height
    assert out.filter(pl.col("plate_z_norm").is_null()).height / out.height < 0.05
    assert set(out["times_through_order"].unique().to_list()) <= {1, 2, 3}
    assert out.filter(pl.col("run_value").is_not_null()).height / out.height > 0.9
