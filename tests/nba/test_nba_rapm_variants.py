"""Tests for nba_rapm_variants (WP2: LA / four-factor / decay RAPM)."""

from __future__ import annotations

import datetime

import numpy as np
import polars as pl

from sportsdataverse.nba.nba_model_validation import _synthetic_possessions
from sportsdataverse.nba.nba_rapm import nba_rapm
from sportsdataverse.nba.nba_rapm_variants import (
    DECAY_RAPM_SCHEMA,
    ORACLE_RAPM_LAMBDAS,
    _fit_weighted,
    _prepare,
    decay_weights,
    nba_decay_rapm,
    oracle_rapm_alphas,
)


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


def test_oracle_rapm_alphas_scales_by_sample_count_not_player_count():
    # Regression pin: Ryan Davis's oracle (NBA_Tutorials_Ryan_Davis/rapm/rapm.py:112-125)
    # scales lambda by `train_x.shape[0]` (possessions / regression samples), NOT the
    # player count. lambda_to_alpha(l, samples) = (l * samples) / 2.0.
    alphas = oracle_rapm_alphas(50_000, ORACLE_RAPM_LAMBDAS)
    assert np.allclose(alphas, [250.0, 1250.0, 2500.0])


def _synth_with_dates(seed: int = 1, n_games: int = 20) -> pl.DataFrame:
    poss = _synth(seed=seed, n_games=n_games)
    # assign each game a distinct date, oldest game first
    gids = poss["game_id"].unique(maintain_order=True).to_list()
    base = datetime.date(2023, 1, 1)
    dmap = {g: base + datetime.timedelta(days=i) for i, g in enumerate(gids)}
    return poss.with_columns(pl.col("game_id").replace_strict(dmap, return_dtype=pl.Date).alias("game_date"))


def test_decay_rapm_empty_input():
    out = nba_decay_rapm(pl.DataFrame())
    assert out.height == 0
    assert dict(out.schema) == DECAY_RAPM_SCHEMA


def test_decay_rapm_asof_none_equals_plain_rapm():
    poss = _synth_with_dates()
    dec = nba_decay_rapm(poss, asof=None).sort("player_id")
    ref = nba_rapm(poss.drop("game_date")).sort("player_id")
    assert np.allclose(dec["decay_rapm"].to_numpy(), ref["rapm"].to_numpy(), atol=1e-6)


def test_decay_rapm_weighting_changes_fit():
    # Isolate the decay-WEIGHT effect from the ridge-schedule switch: both calls
    # pass `asof` (so both take the oracle-alphas / cv=ORACLE_RAPM_CV branch and,
    # since asof == the max game_date, neither filters any possessions --
    # `oracle_rapm_alphas(X.shape[0])` is evaluated at an identical sample count
    # on both sides). Only `half_life_days` differs: a huge half-life makes every
    # weight ~1.0 (decay-neutral), vs a short one that decays hard.
    #
    # A prior version compared asof=None (DEFAULT_RAPM_ALPHAS, cv=None) against
    # asof=<date> (oracle alphas, cv=5): that discriminates on the schedule
    # switch ALONE (empirically: max diff ~2.55 even with weights forced to 1),
    # so it would still "pass" if the `_w` decay-weight wiring were silently
    # broken -- it proved "two configs differ," not "recency weighting changes
    # the fit." This version holds the schedule fixed and isolates the
    # decay-only effect (empirically: max diff ~2.57 at atol=1e-3).
    poss = _synth_with_dates()
    asof = poss["game_date"].max()
    neutral = nba_decay_rapm(poss, asof=asof, half_life_days=1e9).sort("player_id")
    decayed = nba_decay_rapm(poss, asof=asof, half_life_days=5.0).sort("player_id")
    assert not np.allclose(neutral["decay_rapm"].to_numpy(), decayed["decay_rapm"].to_numpy(), atol=1e-3)


def test_decay_rapm_schema_and_dtypes():
    out = nba_decay_rapm(_synth_with_dates())
    assert dict(out.schema) == DECAY_RAPM_SCHEMA
