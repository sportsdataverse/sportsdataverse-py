"""The nflverse play_by_play columns ``enrich_nfl_pbp`` adds, checked against nflverse's own rows.

Each step runs on nflverse's inputs and must give nflverse's output, which isolates the port from the
upstream differences of a Shield-built frame. Oracles: ``tests/fixtures/nfl_shield/2024_01_BAL_KC_nflverse.parquet``
(a whole game), ``tests/fixtures/nfl_ep_wp/try_rows_2002_2005_2024.parquet`` (every week-1 try of three seasons)
and ``tests/fixtures/nfl_ep_wp/overtime_tries_2025.parquet`` (three 2025 overtimes with an answering try).
"""

from __future__ import annotations

from pathlib import Path

import numpy as np
import polars as pl
import pytest
from sportsdataverse.nfl import ep_wp, nfl_fourth_down

FIXTURES = Path(__file__).resolve().parents[1] / "fixtures"
GAME = FIXTURES / "nfl_shield" / "2024_01_BAL_KC_nflverse.parquet"
TRIES = FIXTURES / "nfl_ep_wp" / "try_rows_2002_2005_2024.parquet"
OVERTIME = FIXTURES / "nfl_ep_wp" / "overtime_tries_2025.parquet"


def _read(path: Path) -> pl.DataFrame:
    if not path.exists():
        pytest.skip(f"{path.name} fixture not present")
    return pl.read_parquet(path)


@pytest.fixture(scope="module")
def game() -> pl.DataFrame:
    return _read(GAME)


@pytest.fixture(scope="module")
def tries() -> pl.DataFrame:
    return _read(TRIES)


def _max_diff(out: pl.Series, oracle: pl.Series) -> float:
    """Largest absolute difference, after the two null masks agree and every value is finite."""
    assert out.is_null().to_list() == oracle.is_null().to_list(), out.name
    a, b = out.drop_nulls().to_numpy(), oracle.drop_nulls().to_numpy()
    assert len(a) and np.isfinite(a).all() and np.isfinite(b).all(), out.name
    return float(np.abs(a - b).max())


def _proxy(tries: pl.DataFrame) -> pl.DataFrame:
    """What the EP model leaves on a try: its touchback 1st-and-10 proxy, about 0.45, and non-zero classes."""
    return tries.drop("extra_point_prob", "two_point_conversion_prob").with_columns(
        ep=pl.lit(0.45), td_prob=pl.lit(0.3)
    )


def test_try_ep_is_nflfastr_try_value(tries):
    out = ep_wp._apply_try_values(_proxy(tries))
    assert out["two_point_conversion_prob"].to_list() == tries["two_point_conversion_prob"].to_list()
    # the bundled fg_model against nflverse's FG table: within 0.02 on every kick try of 2002, 2005 and 2024
    assert _max_diff(out["extra_point_prob"], tries["extra_point_prob"]) < 0.02
    # nflverse's ep on the flagged tries; 4 of its 6 unflagged 2024 kick tries carry 0.98 against its own
    # extra_point_prob of 0.949
    flagged = (pl.col("extra_point_attempt") == 1) | (pl.col("two_point_attempt") == 1)
    for season in (2002, 2005, 2024):
        rows = out.with_columns(nv_ep=tries["ep"]).filter((pl.col("season") == season) & flagged)
        assert _max_diff(rows["ep"], rows["nv_ep"]) < 0.02, season
    assert out["td_prob"].max() == 0.0


def test_a_kick_try_without_its_flag_keeps_its_try_value(tries):
    # nflverse types these 2024 tries extra_point without extra_point_attempt; the EP / WP fill must not treat
    # them as marker rows and overwrite the try's ep with the next play's
    unflagged = tries.filter((pl.col("extra_point_attempt") == 0) & (pl.col("two_point_attempt") == 0))
    assert unflagged.height == 6 and unflagged["down"].null_count() == 6
    assert unflagged.select(ep_wp._is_real_play_expr(unflagged).all()).item()


def test_try_values_without_the_fg_model(tries, monkeypatch):
    monkeypatch.setattr(nfl_fourth_down, "fg_make_probability", lambda *a, **k: None)
    with pytest.raises(FileNotFoundError):
        ep_wp._apply_try_values(_proxy(tries), strict=True)
    out = ep_wp._apply_try_values(_proxy(tries))
    kick = out.filter(pl.col("two_point_attempt") == 0)
    # no make probability: the kick try keeps the model's ep instead of a null the fill would overwrite
    assert kick["extra_point_prob"].null_count() == kick.height
    assert kick["ep"].to_list() == [0.45] * kick.height


def test_overtime_answering_try_reads_its_make_probability():
    ot = _read(OVERTIME)
    src = ot.drop("wp", "vegas_wp").with_columns(wp=pl.lit(0.5), vegas_wp=pl.lit(0.5))
    out = ep_wp._apply_ot_wp_overlay(ep_wp._apply_try_values(src)).with_columns(nv_wp=ot["wp"])
    # 2025 both-possess overtime: the answering team's two-point try down one wins if made (nflverse 0.49);
    # its zeroed score classes would make it 0
    answer = out.filter(pl.col("two_point_attempt") == 1)
    assert answer["score_differential"].to_list() == [-1.0, -1.0, -1.0]
    assert _max_diff(answer["wp"], answer["nv_wp"]) < 1e-9


def test_wp_post_matches_nflverse(game):
    out = ep_wp._add_wp_post(game.drop("home_wp_post", "away_wp_post"))
    for col in ("home_wp_post", "away_wp_post"):
        assert game[col].null_count() == 13
        assert _max_diff(out[col], game[col]) < 1e-9


def test_vegas_home_wp_is_published(game):
    out = ep_wp._derive_wpa(game.drop("home_wp", "away_wp", "wpa", "vegas_home_wp"))
    assert _max_diff(out["vegas_home_wp"], game["vegas_home_wp"]) < 1e-9


def test_play_flag_and_player_aliases_match_nflverse(game):
    cols = ["play", "name", "id", "fantasy", "fantasy_id", "fantasy_player_name", "fantasy_player_id"]
    out = ep_wp._add_clean_pbp_columns(game.drop(cols))
    for col in cols:
        assert out[col].cast(pl.Utf8).to_list() == game[col].cast(pl.Utf8).to_list(), col
    assert out["play"].sum() == 136
