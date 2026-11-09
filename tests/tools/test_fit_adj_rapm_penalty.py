"""The adjusted-RAPM penalty fitter (``tools/fit_adj_rapm_penalty.py``) and its frozen constants.

``tests/fixtures/bb_adj_rapm_penalty/cv_sse.parquet`` is the pooled grouped-CV error the fitter wrote
from every season (README there); the frozen penalty and prior scale are re-derived from it. The
fitter's ridge is checked against :func:`nba_adj_rapm` on the committed 2025 WNBA possessions
(``tests/fixtures/bb_factor_rapm``), with a prior from the committed 2025 ``wnba_player_impact`` SPM.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import numpy as np
import polars as pl
import pytest

from sportsdataverse.nba.nba_adj_rapm import ADJ_RAPM_PENALTY, ADJ_RAPM_PRIOR_SCALE, nba_adj_rapm
from sportsdataverse.nba.nba_player_factor_rapm import nba_factor_rapm_rows
from sportsdataverse.nba.nba_rapm import RAPM_PENALTY, build_rapm_design

_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(_ROOT / "tools"))  # the fitter imports fit_rapm_penalty as a sibling
_spec = importlib.util.spec_from_file_location("fit_adj_rapm_penalty", _ROOT / "tools" / "fit_adj_rapm_penalty.py")
fit = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(fit)  # type: ignore[union-attr]
_FX = _ROOT / "tests" / "fixtures"
_LEAGUE_ID = {"nba": "00", "wnba": "10"}


def _pooled(league: str, k: float) -> pl.DataFrame:
    cv = pl.read_parquet(_FX / "bb_adj_rapm_penalty" / "cv_sse.parquet")
    train = list(fit.LEAGUES[league][1])
    return (
        cv.filter((pl.col("league") == league) & (pl.col("k") == k) & pl.col("season").is_in(train))
        .group_by("alpha")
        .agg(pl.col("sse").sum(), pl.len().alias("seasons"))
        .sort("alpha")
    )


@pytest.mark.parametrize("league", ["nba", "wnba"])
def test_the_frozen_penalty_and_scale_are_the_pooled_cv_minimum(league) -> None:
    lid = _LEAGUE_ID[league]
    k = ADJ_RAPM_PRIOR_SCALE[lid]
    assert k == fit.CHOSEN_SCALE == 2.0
    p = _pooled(league, k)
    assert (p["seasons"] == 30).all()
    assert p.sort("sse")["alpha"][0] == pytest.approx(ADJ_RAPM_PENALTY[lid], rel=1e-12)
    # k = 0 is plain RAPM: its pooled pick is #1121's frozen value
    assert _pooled(league, 0.0).sort("sse")["alpha"][0] == pytest.approx(RAPM_PENALTY[lid], rel=1e-12)
    # the prior-scale evidence: at its own best penalty, k = 2 beats the unscaled prior on held-out games
    assert p["sse"].min() < _pooled(league, 1.0)["sse"].min()
    picks = {"nba": 3162.2776601683795, "wnba": 2275.8459260747886}  # k = 1, observed 2026-11-09
    assert _pooled(league, 1.0).sort("sse")["alpha"][0] == pytest.approx(picks[league], rel=1e-12)


def test_the_fitters_ridge_is_nba_adj_rapm() -> None:
    """On a real season, the fitter's coefficient path at (k, alpha) is nba_adj_rapm(prior_scale=k, alpha=alpha)."""
    rows = nba_factor_rapm_rows(
        pl.read_parquet(_FX / "bb_factor_rapm" / "wnba_possessions_2025.parquet"),
        pl.read_parquet(_FX / "bb_factor_rapm" / "wnba_schedule_2025.parquet"),
        "regular-season",
    )
    spm = pl.read_parquet(_FX / "bb_player_impact" / "wnba_player_impact_2025.parquet").filter(
        pl.col("season_type") == "Regular Season"
    )
    prior = {int(p): (o, d) for p, o, d in spm.select("player_id", "ospm", "dspm").iter_rows()}
    X, y, pids = build_rapm_design(rows)
    P = len(pids)
    pm = np.zeros(2 * P)
    for i, pid in enumerate(pids):
        o, d = prior.get(int(pid), (0.0, 0.0))
        pm[i], pm[P + i] = o / 100.0, -d / 100.0
    Y = np.column_stack([y - k * (X @ pm) for k in fit.PRIOR_SCALES])
    games = rows.drop_nulls([f"{s}_player_{i}" for s in ("off", "def") for i in range(1, 6)])["game_id"]
    res = fit.season_curves(X, Y, games.rank("dense").to_numpy())
    a = int(np.argmin(np.abs(fit.GRID - ADJ_RAPM_PENALTY["10"])))
    kc = fit.PRIOR_SCALES.index(2.0)
    beta = 2.0 * pm + res["coef"][kc, :, a]
    out = nba_adj_rapm(rows, prior, league_id="10", n_samples=2, seed=0)  # the league's frozen alpha and scale
    want = pl.DataFrame({"player_id": np.asarray(pids, dtype=np.int64), "o": 100 * beta[:P], "d": -100 * beta[P:]})
    j = out.join(want, on="player_id")
    assert j.height == P
    assert (j["o_adj_rapm"] - j["o"]).abs().max() < 1e-8 and (j["d_adj_rapm"] - j["d"]).abs().max() < 1e-8
