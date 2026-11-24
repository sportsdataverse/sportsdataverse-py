"""The nflverse play_by_play columns ``enrich_nfl_pbp`` adds, checked against nflverse's own rows.

Each step runs on nflverse's inputs and must give nflverse's output, which isolates the port from the
upstream differences of a Shield-built frame. Oracles: ``tests/fixtures/nfl_shield/2024_01_BAL_KC_nflverse.parquet``
(a whole game) and ``tests/fixtures/nfl_ep_wp/try_rows_2002_2005_2024.parquet`` (every week-1 try of three seasons).
"""

from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest
from sportsdataverse.nfl import ep_wp

FIXTURES = Path(__file__).resolve().parents[1] / "fixtures"
GAME = FIXTURES / "nfl_shield" / "2024_01_BAL_KC_nflverse.parquet"
TRIES = FIXTURES / "nfl_ep_wp" / "try_rows_2002_2005_2024.parquet"


@pytest.fixture(scope="module")
def game() -> pl.DataFrame:
    if not GAME.exists():
        pytest.skip("2024_01_BAL_KC nflverse fixture not present")
    return pl.read_parquet(GAME)


def _max_diff(out: pl.DataFrame, oracle: pl.DataFrame, col: str) -> float:
    return (out[col] - oracle[col]).abs().max() or 0.0


def test_try_ep_is_nflfastr_try_value():
    if not TRIES.exists():
        pytest.skip("try-row fixture not present")
    tries = pl.read_parquet(TRIES)
    # what the EP model left on a try: its touchback 1st-and-10 proxy, about 0.45, and non-zero score classes
    src = tries.drop("extra_point_prob", "two_point_conversion_prob").with_columns(ep=pl.lit(0.45), td_prob=pl.lit(0.3))
    out = ep_wp._apply_try_values(src)
    assert out["two_point_conversion_prob"].to_list() == tries["two_point_conversion_prob"].to_list()
    # the bundled fg_model against nflverse's FG table: within 0.02 on every kick try of 2002, 2005 and 2024
    assert _max_diff(out, tries, "extra_point_prob") < 0.02
    for season in (2002, 2005, 2024):
        rows = out.with_columns(nv_ep=tries["ep"]).filter(pl.col("season") == season)
        assert (rows["ep"] - rows["nv_ep"]).abs().max() < 0.02, season
    assert out["td_prob"].max() == 0.0


def test_wp_post_matches_nflverse(game):
    out = ep_wp._add_wp_post(game.drop("home_wp_post", "away_wp_post"))
    for col in ("home_wp_post", "away_wp_post"):
        assert out[col].null_count() == game[col].null_count() == 13
        assert _max_diff(out, game, col) < 1e-9


def test_vegas_home_wp_is_published(game):
    out = ep_wp._derive_wpa(game.drop("home_wp", "away_wp", "wpa", "vegas_home_wp"))
    assert _max_diff(out, game, "vegas_home_wp") < 1e-9


def test_play_flag_and_player_aliases_match_nflverse(game):
    cols = ["play", "name", "id", "fantasy", "fantasy_id", "fantasy_player_name", "fantasy_player_id"]
    out = ep_wp._add_clean_pbp_columns(game.drop(cols))
    for col in cols:
        assert out[col].cast(pl.Utf8).to_list() == game[col].cast(pl.Utf8).to_list(), col
    assert out["play"].sum() == 136
