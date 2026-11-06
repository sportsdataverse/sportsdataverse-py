"""Fit the plain-RAPM ridge penalty per league: game-grouped CV, pooled over seasons.

The fitted values are frozen in :data:`sportsdataverse.nba.nba_rapm.RAPM_PENALTY` with a comment
citing this script, never tuned by hand.

Why one penalty per league and not one per season. The ridge penalty is the ratio of the
per-possession noise variance to the prior variance of a player's coefficient, so it does not depend
on how many possessions a season has: one league-wide value keeps the same prior, and so the RAPM
scale, in every season (a player's own shrinkage still depends on his possessions). Choosing it per season (``RidgeCV``'s leave-one-possession-out over
an 8-step grid, the old rule) let the scale jump with the pick: NBA 2025-26 got 1,931 where 2023-24
and 2024-25 got 5,179, and the spread of RAPM rose from 1.33 to 2.34 per 100.

Why games are the folds. Possessions of one game share their lineups and score context, so leaving
out single possessions scores a held-out row with its game's other rows in the fit. Each fold here
holds out whole games (``GroupKFold`` on ``game_id``, 10 folds); a player unseen in the training folds
predicts 0. Measured, the leave-one-possession-out criterion does not under-shrink: pooled it picks
one grid step heavier (NBA 3,162, WNBA 2,276) than the grouped one.

Stages:

* ``curves`` (network: the ``{nba,wnba}_stats_possessions`` / ``_schedules`` release assets) writes,
  per league, season and season type, an ``.npz`` with the leave-one-possession-out MSE and the
  grouped-CV SSE per fold over :data:`GRID`, and the whole coefficient path (every player's ``o_rapm``
  / ``d_rapm`` at every grid value), so ``report`` can score any penalty offline.
* ``report`` reads them and prints, per league: the per-season picks (old rule, grouped CV, 1-SE),
  the pooled pick over the training seasons and the as-of pick of each season (pooled over that
  season and the ones before it), the RAPM spread per season at the old and the frozen penalty, the
  year-over-year Spearman and next-season out-of-sample R^2 at the old, frozen and as-of penalties
  with their paired differences from the old rule, and the same scores over a sweep of constant
  penalties. It writes ``report.json`` and ``cv_sse.parquet`` (the pooled grouped-CV error per league,
  season and grid point), which ``tests/fixtures/bb_rapm_penalty`` commits so a test re-derives
  :data:`~sportsdataverse.nba.nba_rapm.RAPM_PENALTY` offline.

Usage::

    uv run --frozen python tools/fit_rapm_penalty.py curves OUT_DIR
    uv run --frozen python tools/fit_rapm_penalty.py report OUT_DIR
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import numpy as np
import polars as pl
from scipy.sparse import csr_matrix
from scipy.stats import spearmanr
from sklearn.model_selection import GroupKFold

from sportsdataverse.nba.nba_rapm import DEFAULT_RAPM_ALPHAS, build_rapm_design

#: The penalty grid: 10 to 1,000,000 in steps of 10**(1/14) (x1.18). It contains every point of the
#: old 8-step grid ``DEFAULT_RAPM_ALPHAS`` (10**(2 + 3k/7)), so the old pick is read off the same curve.
GRID = np.logspace(1, 6, 71)
FOLDS = 10
#: league -> (season end years in the releases, training end years for the frozen value, rank gate,
#: year-over-year possession floor). Training is every complete season: NBA 2026-27 is in progress
#: (fit 2026-11-06); the WNBA 2026 season is complete.
LEAGUES = {
    "nba": (range(1997, 2028), range(1997, 2027), 400, 2000),
    "wnba": (range(1997, 2027), range(1997, 2027), 200, 1000),
}
#: Constant penalties the report scores year over year beside the rules (the old grid's middle points).
SWEEP = (1930.6977288832495, 2682.6957952797247, 5179.474679231207, 13894.95494373136)
#: The schedule season types fit, as the publishers fit them (regular season and playoffs apart).
SEASON_TYPES = {"rs": "regular-season", "po": "playoffs"}
_CHUNK = 20_000


def _centred(X: csr_matrix, y: np.ndarray) -> tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray, float]:
    """Eigen-decompose the centred Gram matrix: ``(eigenvalues, eigenvectors, V' X_c' y_c, column means, mean y)``."""
    n = X.shape[0]
    mean = np.asarray(X.mean(axis=0)).ravel()
    gram = (X.T @ X).toarray() - n * np.outer(mean, mean)
    lam, vec = np.linalg.eigh(gram)
    lam = np.maximum(lam, 0.0)
    ybar = float(y.mean())
    c = vec.T @ (np.asarray(X.T @ y).ravel() - n * mean * ybar)
    return lam, vec, c, mean, ybar


def season_curves(X: csr_matrix, y: np.ndarray, games: np.ndarray, grid: np.ndarray = GRID) -> dict[str, np.ndarray]:
    """One season's penalty curves and coefficient path over ``grid``.

    The ridge is the intercept-fitted one :func:`~sportsdataverse.nba.nba_rapm.nba_rapm` fits (the
    intercept unpenalized), solved through the eigenvectors of the centred Gram matrix so every grid
    value costs a matrix product, not a refit.

    Args:
        X: The ``(n, 2P)`` design of :func:`~sportsdataverse.nba.nba_rapm.build_rapm_design`.
        y: The ``(n,)`` points per possession.
        games: The ``(n,)`` game of each possession (the CV groups).
        grid: The penalties.

    Returns:
        ``loo_mse`` ``(A,)`` (leave-one-possession-out, the old rule's criterion), ``cv_sse`` /
        ``cv_n`` ``(FOLDS, A)`` / ``(FOLDS,)`` (game-grouped), and ``coef`` ``(2P, A)``.
    """
    n = X.shape[0]
    lam, vec, c, mean, ybar = _centred(X, y)
    inv = 1.0 / (lam[:, None] + grid[None, :])  # (2P, A)
    coef = vec @ (c[:, None] * inv)
    loo = np.zeros(grid.size)
    for start in range(0, n, _CHUNK):
        rows = slice(start, min(start + _CHUNK, n))
        z = np.asarray(X[rows] @ vec) - mean @ vec  # the chunk's centred design in the eigenbasis
        lever = 1.0 / n + (z**2) @ inv
        resid = (y[rows] - ybar)[:, None] - z @ (c[:, None] * inv)
        loo += ((resid / (1.0 - lever)) ** 2).sum(axis=0)
    folds = min(FOLDS, np.unique(games).size)
    cv_sse = np.zeros((folds, grid.size))
    cv_n = np.zeros(folds)
    for k, (train, test) in enumerate(GroupKFold(n_splits=folds).split(X, y, games)):
        t_lam, t_vec, t_c, t_mean, t_ybar = _centred(X[train], y[train])
        path = t_vec @ (t_c[:, None] / (t_lam[:, None] + grid[None, :]))
        pred = np.asarray(X[test] @ path) - t_mean @ path + t_ybar
        cv_sse[k] = ((y[test][:, None] - pred) ** 2).sum(axis=0)
        cv_n[k] = test.size
    return {"loo_mse": loo / n, "cv_sse": cv_sse, "cv_n": cv_n, "coef": coef}


def _rows(league: str, season: int) -> tuple[pl.DataFrame, pl.DataFrame]:
    """One season's possessions and schedule from the release assets (season named by its end year)."""
    if league == "nba":
        from sportsdataverse.nba import nba_loaders as N

        return N.load_nba_stats_possessions(season - 1), N.load_nba_stats_schedules(season - 1)
    from sportsdataverse.wnba import wnba_loaders as W

    return W.load_wnba_stats_possessions(season), W.load_wnba_stats_schedules(season)


def curves(out: Path) -> None:
    """Write one ``.npz`` per league, season and season type (skipping those already written)."""
    out.mkdir(parents=True, exist_ok=True)
    for league, (seasons, *_) in LEAGUES.items():
        for season in seasons:
            todo = [s for s in SEASON_TYPES if not (out / f"{league}_{season}_{s}.npz").exists()]
            if not todo:
                continue
            poss, sched = _rows(league, season)
            poss = poss.with_columns(pl.col("game_id").cast(pl.Utf8))
            for slug in todo:
                games = sched.filter(pl.col("season_type") == SEASON_TYPES[slug]).select(
                    pl.col("game_id").cast(pl.Utf8).unique()
                )
                part = poss.join(games, on="game_id", how="semi").drop_nulls(["game_id", "points"])
                X, y, pids = build_rapm_design(part)
                if X.shape[0] < 2 * FOLDS:
                    print(f"{league} {season} {slug}: {X.shape[0]} possessions, skipped", flush=True)
                    continue
                kept = part.drop_nulls([f"{s}_player_{i}" for s in ("off", "def") for i in range(1, 6)])
                gid = kept["game_id"].rank("dense").cast(pl.Int64).to_numpy()
                res = season_curves(X, y, gid)
                counts = np.asarray(X.sum(axis=0)).ravel()
                np.savez_compressed(out / f"{league}_{season}_{slug}.npz", pids=np.asarray(pids), counts=counts, **res)
                print(f"{league} {season} {slug}: n={X.shape[0]:,} P={len(pids)}", flush=True)


def _pick(curve: np.ndarray, grid: np.ndarray = GRID) -> float:
    return float(grid[int(np.argmin(curve))])


def _one_se(sse: np.ndarray, n: np.ndarray) -> float:
    """The largest penalty whose grouped-CV MSE is within one fold standard error of the minimum."""
    fold_mse = sse / n[:, None]
    mse = sse.sum(axis=0) / n.sum()
    best = int(np.argmin(mse))
    se = fold_mse[:, best].std(ddof=1) / np.sqrt(len(n))
    return float(GRID[np.nonzero(mse <= mse[best] + se)[0].max()])


def _ratings(z: dict, alpha: float) -> pl.DataFrame:
    """One season's plain RAPM at a grid penalty (per 100, defense sign-flipped)."""
    k = int(np.argmin(np.abs(np.log(GRID) - np.log(alpha))))
    P = z["pids"].size
    o, d = 100 * z["coef"][:P, k], -100 * z["coef"][P:, k]
    return pl.DataFrame(
        {"player_id": z["pids"], "rapm": o + d, "poss": z["counts"][:P] + z["counts"][P:]},
        schema={"player_id": pl.Int64, "rapm": pl.Float64, "poss": pl.Float64},
    )


def _forecast(frames: dict[int, pl.DataFrame], floor: int) -> list[dict]:
    """Year over year: Spearman, and next-season out-of-sample R^2 of RAPM as-is and of a fitted line.

    The line ``rapm(t+1) = a + b rapm(t)`` is fit on the pair before (``t-1 -> t``) and scored on
    ``t -> t+1``, so no pair scores a line fit on itself.
    """
    pairs = {}
    for t in sorted(frames):
        if t + 1 in frames:
            a, b = frames[t].filter(pl.col("poss") >= floor), frames[t + 1].filter(pl.col("poss") >= floor)
            assert a.schema["player_id"] == b.schema["player_id"]
            pair = a.join(b, on="player_id", suffix="_next")
            if pair.height >= 30:  # the season in progress has too few players over the floor yet
                pairs[t] = pair
    out = []
    for t, p in pairs.items():
        x, y = p["rapm"].to_numpy(), p["rapm_next"].to_numpy()
        ss = ((y - y.mean()) ** 2).sum()
        row = {
            "season": t,
            "n": p.height,
            "spearman": float(spearmanr(x, y)[0]),
            "r2_as_is": 1 - ((y - x) ** 2).sum() / ss,
        }
        if t - 1 in pairs:
            q = pairs[t - 1]
            slope, icept = np.polyfit(q["rapm"].to_numpy(), q["rapm_next"].to_numpy(), 1)
            row["r2_fitted"] = 1 - ((y - (icept + slope * x)) ** 2).sum() / ss
        out.append(row)
    return out


def _paired(yoy: dict[str, list[dict]], rule: str) -> dict[str, list[float]]:
    """Per pair, ``rule`` minus the old rule: ``{metric: [mean, standard error over pairs, pairs]}``."""
    both = pl.DataFrame(yoy[rule]).join(pl.DataFrame(yoy["old"]), on="season", suffix="_old")
    out = {}
    for m in ("spearman", "r2_as_is", "r2_fitted"):
        d = both.select(pl.col(m) - pl.col(f"{m}_old")).to_series().drop_nulls().to_numpy()
        out[m] = [float(d.mean()), float(d.std(ddof=1) / np.sqrt(d.size)), int(d.size)]
    return out


def _league_report(out: Path, league: str) -> dict:
    """Per-season picks and spread, the pooled and as-of picks, and the year-over-year scores, for one league."""
    seasons, train, gate, floor = LEAGUES[league]
    old_grid = np.isin(np.round(GRID, 6), np.round(DEFAULT_RAPM_ALPHAS, 6))
    z = {
        (s, t): dict(np.load(f)) for s in seasons for t in SEASON_TYPES if (f := out / f"{league}_{s}_{t}.npz").exists()
    }
    pooled = {s: v["cv_sse"].sum(axis=0) for (s, t), v in z.items() if t == "rs"}
    frozen = _pick(sum(pooled[s] for s in train if s in pooled))
    rows = []
    for (s, t), v in sorted(z.items()):
        mse = v["cv_sse"].sum(axis=0) / v["cv_n"].sum()
        old = float(GRID[old_grid][int(np.argmin(v["loo_mse"][old_grid]))])
        row = {
            "season": s,
            "type": t,
            "n": int(v["cv_n"].sum()),
            "players": int(v["pids"].size),
            "old": old,
            "loo_fine": _pick(v["loo_mse"]),
            "cv": _pick(mse),
            "cv_1se": _one_se(v["cv_sse"], v["cv_n"]),
            # pooled over this season and the ones before it: no later season informs it
            "asof": _pick(sum(pooled[q] for q in pooled if q <= s)),
            # flatness: the old rule's LOO MSE, worst of its 8 grid points over the best, and the
            # grouped-CV MSE at the old pick over the frozen value's (both minus 1)
            "loo_flat": float(v["loo_mse"][old_grid].max() / v["loo_mse"].min() - 1),
            "cv_old_vs_frozen": float(mse[np.isclose(GRID, old)][0] / mse[np.isclose(GRID, frozen)][0] - 1),
        }
        for name, a in (("old", old), ("asof", row["asof"]), ("frozen", frozen)):
            row[f"sd_{name}"] = float(_ratings(v, a).filter(pl.col("poss") >= gate)["rapm"].std())
        rows.append(row)
    rs = {s: z[(s, "rs")] for s in train if (s, "rs") in z}
    yoy = {}
    for name in ("old", "frozen", "asof"):
        pick = {r["season"]: (frozen if name == "frozen" else r[name]) for r in rows if r["type"] == "rs"}
        yoy[name] = _forecast({s: _ratings(v, pick[s]) for s, v in rs.items()}, floor)
    sweep = {}
    for a in SWEEP:
        f = pl.DataFrame(_forecast({s: _ratings(v, a) for s, v in rs.items()}, floor))
        sweep[f"{a:.0f}"] = f.select(pl.col("spearman", "r2_as_is", "r2_fitted").mean()).row(0, named=True)
    return {
        "frozen": frozen,
        "train": [min(train), max(train)],
        "seasons": rows,
        "yoy": yoy,
        "paired": {rule: _paired(yoy, rule) for rule in ("frozen", "asof")},
        "sweep": sweep,
    }


def report(out: Path) -> None:
    """Print the per-league report; write ``report.json`` and ``cv_sse.parquet`` to ``OUT_DIR``."""
    res = {league: _league_report(out, league) for league in LEAGUES}
    (out / "report.json").write_text(json.dumps(res, indent=1))
    sse = [
        {
            "league": lg,
            "season": s,
            "alpha": GRID.tolist(),
            "sse": np.load(f)["cv_sse"].sum(axis=0).tolist(),
        }
        for lg, (seasons, *_) in LEAGUES.items()
        for s in seasons
        if (f := out / f"{lg}_{s}_rs.npz").exists()
    ]
    pl.DataFrame(sse).explode("alpha", "sse", empty_as_null=False).write_parquet(out / "cv_sse.parquet")
    with pl.Config(tbl_rows=-1, tbl_cols=-1, tbl_width_chars=250, float_precision=3):
        for league, r in res.items():
            print(f"\n== {league}: frozen penalty {r['frozen']:,.0f} (grouped CV pooled over {r['train']})")
            print(pl.DataFrame(r["seasons"]))
            for rule, d in r["paired"].items():
                print(
                    f"{rule} - old, mean / SE over pairs:",
                    {m: [round(x, 4) for x in v[:2]] for m, v in d.items()},
                )
            for a, m in r["sweep"].items():
                print(f"constant {a}:", {k: round(v, 4) for k, v in m.items()})


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="fit_rapm_penalty")
    ap.add_argument("stage", choices=["curves", "report"])
    ap.add_argument("out", type=Path)
    a = ap.parse_args(argv)
    (curves if a.stage == "curves" else report)(a.out)
    return 0


if __name__ == "__main__":
    sys.exit(main())
