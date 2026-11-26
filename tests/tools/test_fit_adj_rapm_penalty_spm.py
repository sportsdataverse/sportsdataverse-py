"""The SPM prior's minute weighting and penalty (``tools/fit_adj_rapm_penalty.py spm``) on real seasons.

``tests/fixtures/bb_adj_rapm_penalty/spm_cv.parquet`` is the pooled player-grouped CV error the ``spm`` stage
wrote from every season, and ``spm_rows_2024_2025.parquet`` four of the seasons' training rows it read (README
there). The gate trains on one season and scores the next: the equal-weight fit it replaced fails it.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import numpy as np
import polars as pl
import pytest
from scipy.stats import spearmanr

from sportsdataverse.nba.nba_spm import SPM_ALPHA, nba_spm, train_spm

_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(_ROOT / "tools"))  # the fitter imports fit_rapm_penalty as a sibling
_spec = importlib.util.spec_from_file_location("fit_adj_rapm_penalty", _ROOT / "tools" / "fit_adj_rapm_penalty.py")
fit = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(fit)  # type: ignore[union-attr]
_FX = _ROOT / "tests" / "fixtures" / "bb_adj_rapm_penalty"
#: league -> (off_poss floor of the scored players, Spearman floor, spread-ratio floor), 2024 -> 2025.
#: Observed 2026-11-25 (minute-weighted at SPM_ALPHA / the equal-weight alpha-100 rule it replaced): NBA Spearman
#: 0.493 / 0.358, spread ratio 0.537 / 0.216; WNBA 0.706 / 0.603, 0.491 / 0.315. The floors sit just under the first
#: and are never lowered to let a change pass; a spread above the target's own (ratio 1) fails as too wide.
_GATE = {"nba": (1000, 0.48, 0.52), "wnba": (500, 0.69, 0.47)}


def _rows(league: str, season: int) -> pl.DataFrame:
    rows = pl.read_parquet(_FX / "spm_rows_2024_2025.parquet")
    return rows.filter((pl.col("league") == league) & (pl.col("season") == season)).drop("league", "season")


def _pooled(league: str) -> pl.DataFrame:
    cv = pl.read_parquet(_FX / "spm_cv.parquet")
    train = list(fit.LEAGUES[league][1])
    return (
        cv.filter((pl.col("league") == league) & pl.col("season").is_in(train))
        .group_by("alpha")
        .agg(pl.col("sse").sum(), pl.len().alias("seasons"))
        .sort("alpha")
    )


def test_spm_alpha_is_the_pooled_cv_minimum() -> None:
    nba, wnba = _pooled("nba"), _pooled("wnba")
    assert (nba["seasons"] == 30).all() and (wnba["seasons"] == 30).all()
    assert nba.sort("sse")["alpha"][0] == SPM_ALPHA
    # the WNBA's own pick is 100; at the shared value its error is within 0.1% of that minimum
    assert wnba.sort("sse")["alpha"][0] == 100.0
    assert wnba.filter(pl.col("alpha") == SPM_ALPHA)["sse"][0] / wnba["sse"].min() < 1.001


def test_the_cv_fixture_is_the_shipped_fit() -> None:
    """``spm_cv`` (minute-weighted ``train_spm``) re-run on a committed season reproduces its fixture row."""
    cv = pl.read_parquet(_FX / "spm_cv.parquet").filter((pl.col("league") == "wnba") & (pl.col("season") == 2025))
    np.testing.assert_allclose(fit.spm_cv(_rows("wnba", 2025)), cv.sort("alpha")["sse"].to_numpy(), rtol=1e-9)


def _next_season(league: str, **kw) -> tuple[float, float]:
    train, test = _rows(league, 2024), _rows(league, 2025)
    floor = _GATE[league][0]
    s = nba_spm(test, train_spm(train, train.select("player_id", "o_rapm", "d_rapm"), **kw))
    assert s.schema["player_id"] == test.schema["player_id"]
    k = s.join(test.select("player_id", "rapm", "off_poss"), on="player_id").filter(pl.col("off_poss") >= floor)
    assert k.height == test.filter(pl.col("off_poss") >= floor).height
    return float(spearmanr(k["rapm"], k["spm"])[0]), float(k["spm"].std() / k["rapm"].std())


@pytest.mark.parametrize("league", ["nba", "wnba"])
def test_minute_weighting_tracks_next_season_rapm(league) -> None:
    """Trained on 2024, scored on 2025's box line against 2025's own plain RAPM.

    2025 is in no training row; its target is one of the 30 seasons pooled to choose ``SPM_ALPHA``.
    """
    _, rho_floor, ratio_floor = _GATE[league]
    rho, ratio = _next_season(league)  # train_spm's defaults: minute weights at SPM_ALPHA
    old_rho, old_ratio = _next_season(league, weight=None, alpha=100.0)  # the rule it replaced
    assert rho >= rho_floor and ratio_floor <= ratio <= 1.0, (rho, ratio)
    assert rho - old_rho >= 0.08 and ratio >= 1.4 * old_ratio, (rho, old_rho, ratio, old_ratio)
