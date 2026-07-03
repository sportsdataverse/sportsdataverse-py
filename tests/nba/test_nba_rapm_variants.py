"""Tests for nba_rapm_variants (WP2: LA / four-factor / decay RAPM)."""

from __future__ import annotations

import datetime

import numpy as np
import polars as pl

from sportsdataverse.nba.nba_model_validation import _synthetic_possessions
from sportsdataverse.nba.nba_rapm import nba_rapm
from sportsdataverse.nba.nba_rapm_variants import (
    _fit_weighted,
    _prepare,
    decay_weights,
)

_OFF = [f"off_player_{i}" for i in range(1, 6)]
_DEF = [f"def_player_{i}" for i in range(1, 6)]


def _synth(seed: int = 1, n_games: int = 20) -> pl.DataFrame:
    o = {p: 0.03 for p in list(range(100, 108)) + list(range(200, 208))}
    d = {p: 0.01 for p in o}
    return _synthetic_possessions(o, d, n_games=n_games, poss_per_game=40, noise_sd=0.3, seed=seed)


def test_decay_weights_halflife_math():
    dates = pl.Series("game_date", [datetime.date(2023, 1, 1), datetime.date(2023, 1, 31)])
    w = decay_weights(dates, datetime.date(2023, 1, 31), half_life_days=30.0)
    # 30 days ago -> 0.5 ; 0 days ago -> 1.0
    assert np.isclose(w[0], 0.5, atol=1e-9)
    assert np.isclose(w[1], 1.0, atol=1e-9)


def test_decay_weights_asof_none_all_ones():
    dates = pl.Series("game_date", [datetime.date(2023, 1, 1), datetime.date(2023, 6, 1)])
    w = decay_weights(dates, None, half_life_days=30.0)
    assert np.allclose(w, 1.0)


def test_decay_weights_future_games_clamped_not_amplified():
    # a game AFTER asof must not receive weight > 1
    dates = pl.Series("game_date", [datetime.date(2023, 12, 31)])
    w = decay_weights(dates, datetime.date(2023, 1, 1), half_life_days=30.0)
    assert w[0] <= 1.0 + 1e-9


def test_prepare_row_alignment_matches_design():
    poss = _synth()
    X, y, w, pids = _prepare(poss, "points", weight_col=None)
    assert X.shape[0] == len(y)
    assert w is None
    assert len(pids) > 0


def test_fit_weighted_equals_plain_rapm_on_points():
    poss = _synth()
    X, y, _w, pids = _prepare(poss, "points")
    o, d, off_poss, def_poss = _fit_weighted(X, y)
    ref = nba_rapm(poss).sort("player_id")
    got = pl.DataFrame({"player_id": pids, "o": o, "d": d}).sort("player_id")
    # same design + same RidgeCV grid + unit weights => byte-close to nba_rapm
    assert np.allclose(got["o"].to_numpy(), ref["o_rapm"].to_numpy(), atol=1e-6)
    assert np.allclose(got["d"].to_numpy(), ref["d_rapm"].to_numpy(), atol=1e-6)


def test_fit_weighted_honors_weights():
    # planted: down-weighting half the games to ~0 must change the fit
    poss = _synth()
    X, y, _w, _pids = _prepare(poss, "points")
    o_unw, _d, _o, _dp = _fit_weighted(X, y)
    w = np.ones(len(y))
    w[: len(y) // 2] = 1e-6
    o_w, _d2, _o2, _dp2 = _fit_weighted(X, y, weights=w)
    assert not np.allclose(o_unw, o_w, atol=1e-3)
