"""Tests for the RAPM replacement-pool collapse + lambda_to_alpha."""

from __future__ import annotations

import numpy as np
import polars as pl
import pytest

from sportsdataverse.nba.nba_rapm import (
    REPLACEMENT_PLAYER_ID,
    build_rapm_design,
    lambda_to_alpha,
    nba_rapm,
)


def _poss(n: int = 30, fringe_id: int = 999) -> pl.DataFrame:
    """n possessions of two stable lineups, plus ONE with a fringe player."""
    base = {
        "off_player_1": [1] * n,
        "off_player_2": [2] * n,
        "off_player_3": [3] * n,
        "off_player_4": [4] * n,
        "off_player_5": [5] * n,
        "def_player_1": [6] * n,
        "def_player_2": [7] * n,
        "def_player_3": [8] * n,
        "def_player_4": [9] * n,
        "def_player_5": [10] * n,
        "points": [2, 0] * (n // 2),
    }
    df = pl.DataFrame(base)
    fringe = df.head(1).with_columns(pl.lit(fringe_id).cast(pl.Int64).alias("off_player_5"))
    return pl.concat([df, fringe])


def test_lambda_to_alpha_units() -> None:
    assert lambda_to_alpha(3000.0) == 3000.0
    assert lambda_to_alpha(3000.0, mean_row_weight=2.0) == 6000.0


def test_default_keeps_every_player() -> None:
    _, _, pids = build_rapm_design(_poss())
    assert 999 in pids
    assert REPLACEMENT_PLAYER_ID not in pids


def test_collapse_pools_fringe_player() -> None:
    X, y, pids = build_rapm_design(_poss(), replacement_min_obs=5)
    assert 999 not in pids
    assert REPLACEMENT_PLAYER_ID in pids
    # regulars keep their own columns
    assert set(range(1, 11)) <= set(pids)
    # possessions are RETAINED, not dropped (the whole point vs a min_poss drop)
    assert X.shape[0] == y.shape[0] == 31


def test_two_pooled_players_sum_in_replacement_column() -> None:
    df = _poss()
    both = df.head(1).with_columns(
        pl.lit(998).cast(pl.Int64).alias("off_player_4"),
        pl.lit(999).cast(pl.Int64).alias("off_player_5"),
    )
    X, _, pids = build_rapm_design(pl.concat([df, both]), replacement_min_obs=5)
    rep_col = pids.index(REPLACEMENT_PLAYER_ID)
    dense = np.asarray(X.todense())
    # the row with two fringe players carries weight 2 on the replacement column
    assert dense[:, rep_col].max() == pytest.approx(2.0)


def test_nba_rapm_threads_replacement() -> None:
    out = nba_rapm(_poss(), replacement_min_obs=5, alphas=np.array([lambda_to_alpha(3000.0)]))
    assert REPLACEMENT_PLAYER_ID in out["player_id"].to_list()
    assert 999 not in out["player_id"].to_list()
    # schema unchanged
    assert out.columns == ["player_id", "o_rapm", "d_rapm", "rapm", "off_poss", "def_poss"]
