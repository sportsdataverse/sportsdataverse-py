"""Smoke tests for sportsdataverse.nfl.nfl_play_participants (live-gated)."""

from __future__ import annotations

import polars as pl

from sportsdataverse.nfl import espn_nfl_play_participants
from tests.conftest import skip_if_no_live


@skip_if_no_live
def test_nfl_play_participants_wide_frame():
    parts = espn_nfl_play_participants(401872922)  # CLE @ JAX, 2026 week 1
    assert isinstance(parts, pl.DataFrame)
    assert parts.height > 100
    assert {"play_id", "passer_player_name", "passer_player_id", "rusher_player_name", "rusher_player_id"} <= set(
        parts.columns
    )
    assert parts["passer_player_id"].is_not_null().sum() > 30
    assert "Deshaun Watson" in parts["passer_player_name"].drop_nulls().unique().to_list()
