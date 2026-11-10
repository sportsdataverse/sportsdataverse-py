"""The BB-D38 merge gate, re-derived from what ``tools/fit_box_prior_v2.py`` wrote (run 2026-11-10).

``tests/fixtures/bb_box_prior_v2/gate.parquet`` holds the held-out squared error of every method, split and
NBA regular season 1998-2026 (README there). The floors below sit under the observed values with margin
and are never lowered to pass: if a refit falls below one, debug the model.
"""

from __future__ import annotations

import importlib.util
import json
from pathlib import Path

import numpy as np
import polars as pl
import pytest

_ROOT = Path(__file__).resolve().parents[2]
_spec = importlib.util.spec_from_file_location("fit_box_prior_v2", _ROOT / "tools" / "fit_box_prior_v2.py")
fit = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(fit)  # type: ignore[union-attr]
_FX = _ROOT / "tests" / "fixtures" / "bb_box_prior_v2"
_METHODS = {"adj", "rapm", "v2", "v2_fixed", "adj_reml", "prior"}


@pytest.fixture(scope="module")
def gate() -> pl.DataFrame:
    return pl.read_parquet(_FX / "gate.parquet")


@pytest.fixture(scope="module")
def report() -> dict:
    return json.loads((_FX / "report.json").read_text())


def _paired(gate: pl.DataFrame, split: pl.Expr, method: str) -> np.ndarray:
    """Per season, today's adjusted RAPM's held-out MSE minus ``method``'s, in basis points."""
    per = (
        gate.filter(split)
        .group_by("season", "method")
        .agg((pl.col("sse").sum() / pl.col("n").sum()).alias("mse"))
        .pivot("method", index="season", values="mse")
        .sort("season")
    )
    return (per["adj"] - per[method]).to_numpy() * 1e4


def test_the_gate_covers_every_season_split_and_method(gate) -> None:
    assert sorted(gate["season"].unique()) == list(fit.GATE_SEASONS)
    assert set(gate["method"].unique()) == _METHODS
    per = gate.group_by("season").agg(pl.col("split").n_unique().alias("splits"), pl.len())
    assert (per["splits"] == fit.FOLDS + 1).all() and (per["len"] == (fit.FOLDS + 1) * len(_METHODS)).all()
    # every method scored the same held-out possessions in a split
    assert gate.group_by("season", "split").agg(pl.col("n").n_unique())["n"].max() == 1


def test_the_default_fixed_penalty_beats_todays_adjusted_rapm(gate) -> None:
    """The shipped default (v2 prior at the fixed penalty). Observed: held-out +1.638 bp (SE 0.104), all 29
    seasons; chronological 2006-2026 +1.428 (SE 0.287), 17 of 21."""
    d = _paired(gate, pl.col("split").str.starts_with("fold"), "v2_fixed")
    assert d.size == 29 and d.mean() > 1.2 and d.mean() / (d.std(ddof=1) / np.sqrt(d.size)) > 8 and (d > 0).sum() >= 27
    c = _paired(gate.filter(pl.col("season") >= 2006), pl.col("split") == "chrono", "v2_fixed")
    assert c.size == 21 and c.mean() > 1.0 and c.mean() / (c.std(ddof=1) / np.sqrt(c.size)) > 3 and (c > 0).sum() >= 15


def test_v2_beats_todays_adjusted_rapm_on_held_out_games(gate) -> None:
    """Observed: +0.728 bp (SE 0.165, t 4.4), better in 22 of 29 seasons."""
    d = _paired(gate, pl.col("split").str.starts_with("fold"), "v2")
    se = d.std(ddof=1) / np.sqrt(d.size)
    assert d.size == 29
    assert d.mean() > 0.5 and d.mean() / se > 3.0 and (d > 0).sum() >= 18


def test_v2_beats_todays_adjusted_rapm_after_the_cut_date(gate) -> None:
    """Chronological: fit before the 60%-of-games date, score after.

    Gated on 2006+ only, where the prior is strictly walk-forward (1998-2005 priors learn from every other
    season, so "fit before a date" does not hold for them). Observed 2006-2026: +1.025 bp (SE 0.485, t 2.1),
    14 of 21; all 29 seasons +0.887 (SE 0.365), 20 of 29.
    """
    d = _paired(gate.filter(pl.col("season") >= 2006), pl.col("split") == "chrono", "v2")
    se = d.std(ddof=1) / np.sqrt(d.size)
    assert d.size == 21
    assert d.mean() > 0.5 and d.mean() / se > 1.5 and (d > 0).sum() >= 12


def test_the_report_is_the_gate(gate, report) -> None:
    for kind, split in (("heldout", pl.col("split").str.starts_with("fold")), ("chrono", pl.col("split") == "chrono")):
        for m in _METHODS - {"adj"}:
            d = _paired(gate, split, m)
            assert report[kind][f"adj_minus_{m}_bp"][0] == pytest.approx(d.mean(), rel=1e-9)


def test_the_new_prior_closes_the_oracle_8yr_gap(report) -> None:
    """Pooled 2020-26 vs an outside 8-season RAPM (Poss >= 10k): rank and spread both improve on today's SPM.

    Observed: Spearman 0.670 (SPM 0.539, BPM 0.678); std 1.33 (SPM 0.41, BPM 2.37, oracle 2.79). The
    prior does not beat BPM on rank or spread, and the season rating (Spearman 0.796) ranks below today's
    adjusted RAPM (0.906) against this oracle; neither is part of the gate.
    """
    r = report["oracle_8yr"]["poss_ge_10000"]
    assert r["n"] >= 480
    rho, sd = r["spearman"], r["std"]
    assert rho["box_prior_v2"] > rho["spm"] and rho["box_prior_v2"] > 0.65
    assert abs(sd["box_prior_v2"] - sd["Tot"]) < abs(sd["spm"] - sd["Tot"]) and sd["box_prior_v2"] > 1.2


def test_reml_weights_the_prior_heavily_and_offense_more(report) -> None:
    """Observed over 1998-2026 (possession-weighted): offense 0.902, defense 0.810, net 0.856."""
    w = report["prior_weight_all_seasons"]
    assert 0.85 < w["offense"] < 0.95 and 0.75 < w["defense"] < 0.87 and w["offense"] > w["defense"]


def test_the_fixed_penalty_beats_the_reml_pick(gate, report) -> None:
    """Why ``nba_reml_rapm`` defaults to the fixed penalty (owner decision 2026-11-10); REML is ``penalty="reml"``.

    v2 at nba_adj_rapm's frozen penalty minus v2 at the REML pick, held-out games: +0.911 bp (SE 0.136),
    fixed better in 27 of 29 seasons; chronological +0.623 (SE 0.285), 17 of 29. The frozen penalty was
    cross-validated on these seasons for another prior, so its edge is somewhat optimistic; refitting it for
    the v2 prior is a follow-up. If a refit flips this, revisit the default and the docs that cite it.
    """
    d = -_paired(gate, pl.col("split").str.starts_with("fold"), "v2") + _paired(
        gate, pl.col("split").str.starts_with("fold"), "v2_fixed"
    )
    assert d.mean() == pytest.approx(report["heldout"]["v2_minus_v2_fixed_bp"][0], rel=1e-9)
    assert d.mean() > 0.5 and (d > 0).sum() >= 24


# --- the harness's leakage wiring, on the real 2023-24 slice (the gate's numbers rest on it) ---


def _slice() -> tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame, pl.DataFrame]:
    poss = pl.read_parquet(_FX / "nba_possessions_2024_slice.parquet")
    logs = pl.read_parquet(_FX / "nba_player_logs_2024_slice.parquet")
    team = pl.read_parquet(_FX / "nba_team_logs_2024_slice.parquet")
    bio = pl.read_parquet(_FX / "nba_bio_2023_2024.parquet")
    return poss, logs, team, bio


def _prior_gap(a: dict, b: dict) -> float:
    assert a.keys() == b.keys()
    return max(abs(a[k][i] - b[k][i]) for k in a for i in (0, 1))


def test_a_split_never_reads_its_held_out_games() -> None:
    poss, logs, team, bio = _slice()
    bio = bio.filter(pl.col("season") == 2024)
    model = fit.train_box_prior(pl.read_parquet(_FX / "nba_panel_2022_2023.parquet"), season=2024)
    games = poss["game_id"].unique().sort()
    tr = games.head(60).to_list()
    train, test, prior = fit._split_inputs(poss, logs, team, bio, model, tr)
    assert set(train["game_id"]) == set(tr) and not set(test["game_id"]) & set(tr)
    out_game, in_game = games[80], games[0]

    def rewrite(game: str) -> tuple[pl.DataFrame, pl.DataFrame]:
        lg = logs.with_columns(
            pl.when(pl.col("game_id") == game).then(pl.col(c) + 40).otherwise(pl.col(c)).alias(c)
            for c in ("pts", "ast", "oreb", "dreb", "stl", "min")
        )
        ps = poss.with_columns(pl.when(pl.col("game_id") == game).then(pl.col("points") + 3).otherwise(pl.col("points")).alias("points"))  # fmt: skip
        return lg, ps

    lg, ps = rewrite(out_game)
    train2, _, prior2 = fit._split_inputs(ps, lg, team, bio, model, tr)
    assert train2.equals(train) and _prior_gap(prior, prior2) < 1e-9
    lg, ps = rewrite(in_game)  # the same rewrite of a training game moves both: the check can fail
    train3, _, prior3 = fit._split_inputs(ps, lg, team, bio, model, tr)
    assert not train3.equals(train) and _prior_gap(prior, prior3) > 0.01


def test_the_prior_for_a_season_never_reads_that_season() -> None:
    """Walk-forward from 2006, leave-season-out through 2005: rewrite the prior's own season and nothing moves."""
    poss, logs, team, bio = _slice()
    g = poss["game_id"].unique().sort()
    halves = {2005: g.head(50).to_list(), 2006: g.tail(50).to_list()}
    b = bio.filter(pl.col("season") == 2024)
    ages = pl.concat([b.select("player_id", pl.lit(s, pl.Int64).alias("season"), "age") for s in halves])

    def season(games: list[str]) -> tuple:
        p = poss.filter(pl.col("game_id").is_in(games))
        f = fit.box_prior_features(logs, team, p, b, game_ids=games)
        return fit.season_gram(p), f

    built = {s: season(gm) for s, gm in halves.items()}
    grams = {s: v[0] for s, v in built.items()}
    feats = {s: v[1].with_columns(pl.lit(s, pl.Int64).alias("season")) for s, v in built.items()}

    def coefs(s: int, grams: dict, feats: dict) -> np.ndarray:
        m, train = fit._prior_for(s, grams, ages, feats)
        assert s not in train and m.season == s
        return np.concatenate([m.o_coef.ravel(), m.d_coef.ravel(), m.o_intercept, m.d_intercept])

    for s in (2005, 2006):
        other = 4011 - s  # the other half
        base = coefs(s, grams, feats)
        # rewrite the prior's own season (with the other half's data): identical; rewrite its training season: it moves
        own_g, own_f = dict(grams), dict(feats)
        own_g[s], own_f[s] = grams[other], feats[other].with_columns(pl.lit(s, pl.Int64).alias("season"))
        assert np.abs(coefs(s, own_g, own_f) - base).max() < 1e-12
        tr_g, tr_f = dict(grams), dict(feats)
        tr_g[other] = grams[s]
        tr_f[other] = feats[s].with_columns(pl.lit(other, pl.Int64).alias("season"))
        assert np.abs(coefs(s, tr_g, tr_f) - base).max() > 1e-3
