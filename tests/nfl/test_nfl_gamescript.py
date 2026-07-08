"""Unit tests for game-script pace / PROE (Tasks 2.1/2.3)."""

import polars as pl

from sportsdataverse.nfl.nfl_gamescript import team_game_pace


def _pbp() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "game_id": ["G"] * 4,
            "season": [2023] * 4,
            "week": [1] * 4,
            "posteam": ["A"] * 4,
            "drive": [1.0, 1.0, 2.0, 2.0],
            "play_type": ["pass", "run", "pass", "pass"],
            "qb_dropback": [1, 0, 1, 1],
            "pass": [1, 0, 1, 1],
            "pass_oe": [10.0, None, -5.0, 5.0],
            "game_seconds_remaining": [3600.0, 3560.0, 3000.0, 2960.0],
            "wp": [0.5] * 4,
            "half_seconds_remaining": [1800.0] * 4,
        }
    )


def test_pace_and_proe():
    out = team_game_pace(_pbp()).row(0, named=True)
    assert out["off_plays"] == 4
    # drive1: (3600-3560)/2=20 ; drive2: (3000-2960)/2=20 -> mean 20 sec/play
    assert abs(out["sec_per_play"] - 20.0) < 1e-6
    # proe = mean(10, -5, 5) over dropbacks = 3.333...
    assert abs(out["proe"] - (10.0 - 5.0 + 5.0) / 3) < 1e-6


def test_neutral_filter():
    df = _pbp().with_columns(
        pl.Series("wp", [0.5, 0.5, 0.95, 0.95]),
    )
    out = team_game_pace(df).row(0, named=True)
    assert out["neutral_plays"] == 2
    assert out["off_plays"] == 4


def test_kneels_excluded():
    df = _pbp().with_columns(pl.Series("play_type", ["pass", "qb_kneel", "pass", "pass"]))
    out = team_game_pace(df).row(0, named=True)
    assert out["off_plays"] == 3


def test_empty_zero_row():
    out = team_game_pace(_pbp().head(0))
    assert out.height == 0
    assert "sec_per_play" in out.columns


# --------------------------------------------------------------------------- #
# Task 2.3 oracle gates (committed fixture; provenance tests/fixtures/nfl_scheme)
# --------------------------------------------------------------------------- #

from pathlib import Path  # noqa: E402

import numpy as np  # noqa: E402
import pytest  # noqa: E402

FIXTURES = Path(__file__).resolve().parents[1] / "fixtures" / "nfl_scheme"


@pytest.fixture(scope="module")
def oracle_pbp() -> pl.DataFrame:
    return pl.read_parquet(FIXTURES / "pbp_2021_2023_slice.parquet")


def test_proe_reconciles_with_pass_oe(oracle_pbp):
    """Gate: season PROE equals the direct pass_oe aggregate over dropbacks.

    Observed at gate time: max abs diff 0.0 (dropback-weighted mean is exact).
    """
    from sportsdataverse.nfl.nfl_gamescript import _game_script_from

    gs = _game_script_from(oracle_pbp.filter(pl.col("season") == 2023))
    direct = (
        oracle_pbp.filter((pl.col("season") == 2023) & (pl.col("qb_dropback") == 1))
        .group_by("posteam")
        .agg(pl.col("pass_oe").mean().alias("proe_direct"))
    )
    assert gs.schema["team"] == direct.schema["posteam"]
    j = gs.join(direct, left_on="team", right_on="posteam")
    assert j.height == 32
    assert np.allclose(j["proe"].to_numpy(), j["proe_direct"].to_numpy(), atol=1e-6)


def test_expected_plays_mae_floor(oracle_pbp):
    """Gate: team-season expected-plays MAE vs realized <= 2.5 plays/game.

    Floor from the observed value at gate time (2.19 on the 2021-2023
    fixture, PACE_CONSTANTS fit 2026-07-08); never raised to pass.
    """
    from sportsdataverse.nfl.nfl_gamescript import _game_script_from
    from sportsdataverse.nfl.nfl_scheme_constants import mae

    gs = _game_script_from(oracle_pbp)
    got = mae(gs["exp_plays_pg"].to_numpy(), gs["off_plays_pg"].to_numpy())
    assert got <= 2.5, f"expected-plays MAE {got}"
