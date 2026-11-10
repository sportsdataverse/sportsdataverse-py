"""REML-penalised prior-informed RAPM (``nba_reml_rapm``, BB-D38) on the real 2023-24 possessions slice.

Fixture: ``tests/fixtures/bb_box_prior_v2`` (README there): the first 100 regular-season games of 2023-24
from the ``nba_stats_possessions`` release, and a real prior (the 2022-23 career-target rows).
"""

from __future__ import annotations

from pathlib import Path

import numpy as np
import polars as pl
import pytest

from sportsdataverse.nba.nba_adj_rapm import nba_adj_rapm
from sportsdataverse.nba.nba_rapm import RAPM_PENALTY, build_rapm_design
from sportsdataverse.nba.nba_reml_rapm import (
    REML_PENALTY_BOUNDS,
    REML_RAPM_SCHEMA,
    fit_reml_prior_ridge,
    nba_reml_rapm,
)

_FX = Path(__file__).resolve().parents[1] / "fixtures" / "bb_box_prior_v2"


@pytest.fixture(scope="module")
def poss() -> pl.DataFrame:
    return pl.read_parquet(_FX / "nba_possessions_2024_slice.parquet")


@pytest.fixture(scope="module")
def prior() -> dict[int, tuple[float, float]]:
    pan = pl.read_parquet(_FX / "nba_panel_2022_2023.parquet").filter(pl.col("season") == 2023)
    return {int(p): (o, d) for p, o, d in pan.select("player_id", "o_target", "d_target").iter_rows()}


def test_fixed_penalties_are_nba_adj_rapm(poss, prior) -> None:
    """At one given penalty for both sides it is nba_adj_rapm's ridge at that alpha and prior scale 1."""
    a = nba_adj_rapm(poss, prior, alpha=3000.0, prior_scale=1.0, n_samples=0)
    b = nba_reml_rapm(poss, prior, lambdas=(3000.0, 3000.0))
    assert a.schema["player_id"] == b.schema["player_id"]
    j = a.join(b, on="player_id")
    assert j.height == a.height == b.height
    assert (j["o_adj_rapm"] - j["o_reml_rapm"]).abs().max() < 1e-9
    assert (j["d_adj_rapm"] - j["d_reml_rapm"]).abs().max() < 1e-9


def test_zero_prior_reml_lands_near_the_cv_frozen_penalty(poss) -> None:
    """With no prior, REML's offense penalty is the order of the game-grouped CV one (#1121).

    Observed: 2,343 on this 100-game slice (2,793 on the full 2023-24 season) against
    RAPM_PENALTY["00"] = 2,683. Never widen the band to make it pass.
    """
    r = nba_reml_rapm(poss, {})
    assert 0.6 * RAPM_PENALTY["00"] < r["lambda_o"][0] < 1.5 * RAPM_PENALTY["00"]


def test_the_reml_pick_is_a_minimum_and_the_sides_differ(poss, prior) -> None:
    X, y, pids = build_rapm_design(poss)
    P = len(pids)
    mu = np.concatenate(
        [[prior.get(int(p), (0.0, 0.0))[0] for p in pids], [-prior.get(int(p), (0.0, 0.0))[1] for p in pids]]
    ) / 100.0  # fmt: skip
    fit = fit_reml_prior_ridge(X, y, mu)
    assert fit.coef.shape == (2 * P,) and fit.converged
    # offense and defense get their own penalty (observed 7,542 / the 1e7 bound on this slice: the
    # likelihood keeps falling as defense goes all-prior)
    assert abs(np.log(fit.lambda_o / fit.lambda_d)) > 1.0
    lo_b, hi_b = REML_PENALTY_BOUNDS
    for fo, fd in ((1.5, 1), (1 / 1.5, 1), (1, 1.5), (1, 1 / 1.5)):
        lam = (fit.lambda_o * fo, fit.lambda_d * fd)
        if not all(lo_b <= v <= hi_b for v in lam):
            continue  # a step out of the search box
        assert fit_reml_prior_ridge(X, y, mu, lambdas=lam).reml > fit.reml
    # the chosen penalties change the output: the REML fit is not the fixed-penalty fit
    fixed = fit_reml_prior_ridge(X, y, mu, lambdas=(3000.0, 3000.0))
    assert np.abs(fixed.coef - fit.coef).max() * 100 > 0.1


def test_prior_weight(poss, prior) -> None:
    r = nba_reml_rapm(poss, prior)
    assert list(r.schema.items()) == list(REML_RAPM_SCHEMA.items())
    for side, n in (("o", "off_poss"), ("d", "def_poss")):
        w = r[f"{side}_prior_weight"]
        assert w.min() > 0 and w.max() <= 1 + 1e-12
        # more possessions, less prior: the weight falls with possessions (rank correlation)
        assert r.select(pl.corr(n, f"{side}_prior_weight", method="spearman")).item() < -0.9
    assert ((r["o_reml_rapm"] + r["d_reml_rapm"]) - r["reml_rapm"]).abs().max() < 1e-12
    # the prior column is the prior given, and a player outside the prior gets (0, 0)
    pid = r["player_id"][0]
    assert r["o_box_prior"][0] == pytest.approx(prior.get(int(pid), (0.0, 0.0))[0])


def test_empty_input_has_the_schema() -> None:
    empty = pl.DataFrame(
        {c: pl.Series([], dtype=pl.Int64) for c in ["points"] + [f"{s}_player_{i}" for s in ("off", "def") for i in range(1, 6)]}
    )  # fmt: skip
    out = nba_reml_rapm(empty, {})
    assert out.height == 0 and list(out.schema.items()) == list(REML_RAPM_SCHEMA.items())
