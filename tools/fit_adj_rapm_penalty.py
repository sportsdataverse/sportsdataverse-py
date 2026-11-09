"""Fit the adjusted-RAPM ridge penalty per league: game-grouped CV, pooled over seasons, at the prior.

The companion of ``tools/fit_rapm_penalty.py`` (#1121) for :func:`sportsdataverse.nba.nba_adj_rapm`,
whose ridge shrinks toward a box prior: the previous season's SPM (regular season and playoffs
blended by minutes, the ``player_impact`` producers' carry), trained on that season's plain RAPM at
:data:`~sportsdataverse.nba.nba_rapm.RAPM_PENALTY`. The response is residualized on the prior
(``y - k X prior``) and fit with an unpenalized intercept, as ``nba_adj_rapm`` does, so the curves
here are #1121's, evaluated on the residualized problem. The fitted value is frozen in
:data:`sportsdataverse.nba.nba_adj_rapm.ADJ_RAPM_PENALTY` with a comment citing this script.

``k`` scales the prior (:data:`PRIOR_SCALES`; ``k = 1`` is the prior as fit, ``k = 0`` is plain RAPM).
Every ``k`` shares one eigen-decomposition per fold, so the second shrinkage source, the prior's
own width, is scored on the same held-out games as the penalty.

Stages:

* ``curves`` (network: the possessions / schedule release assets; the leaguegamelog captures of the
  -raw stores, a local checkout or an ``http(s)://`` base) writes one ``.npz`` per league and season
  (regular season): the grouped-CV SSE per fold, ``k`` and grid point, the leave-one-possession-out
  MSE at ``k = 1`` (the old rule's criterion), the coefficient path per ``k``, and the season's plain
  RAPM and SPM (the forecast target and the prior baseline).
* ``report`` reads them and prints, per league: the pooled pick per ``k``, the per-season spread of
  adjusted RAPM at the old rule's pick and at the frozen value, and next-season R^2 / Spearman
  (against next season's plain RAPM) of adjusted RAPM before and after, plain RAPM and SPM. It writes
  ``report.json`` and ``cv_sse.parquet`` (pooled grouped-CV error per league, season, ``k`` and grid
  point), which ``tests/fixtures/bb_adj_rapm_penalty`` commits so a test re-derives the constant.

Usage::

    uv run --frozen python tools/fit_adj_rapm_penalty.py curves OUT_DIR --nba-raw DIR --wnba-raw DIR
    uv run --frozen python tools/fit_adj_rapm_penalty.py report OUT_DIR
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path

import numpy as np
import polars as pl
from scipy.sparse import csr_matrix
from scipy.stats import spearmanr
from sklearn.model_selection import GroupKFold

sys.path.insert(0, str(Path(__file__).resolve().parent))
from fit_rapm_penalty import FOLDS, GRID, SEASON_TYPES, _rows  # noqa: E402

from sportsdataverse.nba import box_features, nba_box_logs, nba_raw_store_season_frame, nba_spm, train_spm  # noqa: E402
from sportsdataverse.nba.nba_rapm import DEFAULT_RAPM_ALPHAS, build_rapm_design, nba_rapm  # noqa: E402

#: Prior scales scored (``k = 1``: the prior as fit; ``k = 0``: no prior, plain RAPM).
PRIOR_SCALES = (0.0, 0.5, 1.0, 1.5, 2.0, 2.5, 3.0, 4.0)
#: The scale frozen in ``ADJ_RAPM_PRIOR_SCALE`` (both leagues): where next-season R^2 and Spearman peak, the
#: conservative end of the range where held-out game error also falls (see the report).
CHOSEN_SCALE = 2.0
#: league -> (season end years, training end years, league id, year-over-year possession floor,
#: scale-check off_poss floor). Training is every complete season (fit 2026-11-09: NBA 2026-27 is in
#: progress, WNBA 2026 is complete). The first season has no prior (the producers' chain starts there).
LEAGUES = {
    "nba": (range(1997, 2027), range(1997, 2027), "00", 2000, 300),
    "wnba": (range(1997, 2027), range(1997, 2027), "10", 1000, 300),
}
_CHUNK = 20_000
_RETRIES = 4


def _season_str(league: str, season: int) -> str:
    return f"{season - 1}-{str(season)[2:]}" if league == "nba" else str(season)


def _logs(league: str, season: int, stype: str, raw: str) -> dict[str, pl.DataFrame]:
    """One season type's player and team box logs from a -raw store's leaguegamelog captures."""
    lid = LEAGUES[league][2]
    slug = "regular-season" if stype == "Regular Season" else "playoffs"

    def fetch(**kw):
        variant = slug + ("_p" if kw["player_or_team_abbreviation"] == "P" else "")
        frame = nba_raw_store_season_frame("leaguegamelog", season, variant, raw_store_dir=raw)
        if frame is None:
            raise SystemExit(f"{league} {season} leaguegamelog/{variant} is not in the raw store {raw}")
        return frame

    return nba_box_logs(_season_str(league, season), season_type=stype, league_id=lid, fetch=fetch)


def _blend(rs: pl.DataFrame, po: pl.DataFrame | None) -> pl.DataFrame:
    """Minute-weighted RS + playoff SPM, the producers' carry; a player in one frame keeps its values."""
    if po is None or po.height == 0:
        return rs.select("player_id", "ospm", "dspm")
    j = rs.join(po, on="player_id", how="full", coalesce=True, suffix="_po")
    w, wp = pl.col("min").fill_null(0), pl.col("min_po").fill_null(0)
    tot = w + wp
    return j.select(
        "player_id",
        *[
            pl.when(tot == 0)
            .then(pl.col(c).fill_null(pl.col(f"{c}_po")))
            .otherwise((pl.col(c).fill_null(0) * w + pl.col(f"{c}_po").fill_null(0) * wp) / tot)
            .alias(c)
            for c in ("ospm", "dspm")
        ],
    )


def _with_retry(fn, *args):
    for i in range(_RETRIES):
        try:
            return fn(*args)
        except Exception as e:  # noqa: BLE001 -- release 5xx / timeouts; the last one re-raises
            if i == _RETRIES - 1:
                raise
            print(f"  retry {i + 1}: {type(e).__name__}: {str(e)[:100]}", flush=True)
            time.sleep(15 * (i + 1))


def _decomp(X: csr_matrix) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    n = X.shape[0]
    mean = np.asarray(X.mean(axis=0)).ravel()
    lam, vec = np.linalg.eigh((X.T @ X).toarray() - n * np.outer(mean, mean))
    return np.maximum(lam, 0.0), vec, mean


def _proj(X: csr_matrix, vec: np.ndarray, mean: np.ndarray, Y: np.ndarray) -> np.ndarray:
    """``V' X_c' (Y - mean Y)`` for every column of ``Y`` ``(n, K)``."""
    n = X.shape[0]
    return vec.T @ (np.asarray(X.T @ Y) - n * np.outer(mean, Y.mean(axis=0)))


def season_curves(X: csr_matrix, Y: np.ndarray, games: np.ndarray, grid: np.ndarray = GRID) -> dict[str, np.ndarray]:
    """Grouped-CV SSE ``(K, F, A)``, coefficient path ``(K, 2P, A)`` and LOO MSE of column 2 (``k = 1``).

    ``Y`` holds one residualized response per prior scale; the intercept-fitted ridge of each shares
    the eigen-decomposition of the centred Gram matrix, per fold and on the whole season.
    """
    n, K = Y.shape
    lam, vec, mean = _decomp(X)
    inv = 1.0 / (lam[:, None] + grid[None, :])
    C = _proj(X, vec, mean, Y)  # (2P, K)
    coef = np.einsum("pq,qk,qa->kpa", vec, C, inv)
    k1 = PRIOR_SCALES.index(1.0)
    y1, ybar = Y[:, k1], float(Y[:, k1].mean())
    loo = np.zeros(grid.size)
    for start in range(0, n, _CHUNK):
        rows = slice(start, min(start + _CHUNK, n))
        z = np.asarray(X[rows] @ vec) - mean @ vec
        lever = 1.0 / n + (z**2) @ inv
        resid = (y1[rows] - ybar)[:, None] - z @ (C[:, k1][:, None] * inv)
        loo += ((resid / (1.0 - lever)) ** 2).sum(axis=0)
    folds = min(FOLDS, np.unique(games).size)
    cv_sse = np.zeros((K, folds, grid.size))
    cv_n = np.zeros(folds)
    for f, (train, test) in enumerate(GroupKFold(n_splits=folds).split(X, Y[:, 0], games)):
        t_lam, t_vec, t_mean = _decomp(X[train])
        t_C = _proj(X[train], t_vec, t_mean, Y[train])
        t_inv = 1.0 / (t_lam[:, None] + grid[None, :])
        Xt = np.asarray(X[test] @ t_vec) - t_mean @ t_vec  # (m, 2P) in the eigenbasis
        for k in range(K):
            pred = Xt @ (t_C[:, k][:, None] * t_inv) + Y[train, k].mean()
            cv_sse[k, f] = ((Y[test, k][:, None] - pred) ** 2).sum(axis=0)
        cv_n[f] = test.size
    return {"cv_sse": cv_sse, "cv_n": cv_n, "coef": coef, "loo_mse": loo / n}


def curves(out: Path, raw: dict[str, str]) -> None:
    """One ``.npz`` per league and regular season (skipping those written), carrying the prior forward."""
    out.mkdir(parents=True, exist_ok=True)
    for league, (seasons, _, lid, *_) in LEAGUES.items():
        prior: pl.DataFrame | None = None
        for season in seasons:
            f = out / f"{league}_{season}_rs.npz"
            p_next = out / f"{league}_{season}_prior_next.parquet"
            if f.exists() and p_next.exists():
                prior = pl.read_parquet(p_next)
                continue
            t0 = time.time()
            poss, sched = _with_retry(_rows, league, season)
            poss = poss.with_columns(pl.col("game_id").cast(pl.Utf8))
            games = sched.filter(pl.col("season_type") == SEASON_TYPES["rs"]).select(
                pl.col("game_id").cast(pl.Utf8).unique()
            )
            part = poss.join(games, on="game_id", how="semi").drop_nulls(["game_id", "points"])
            X, y, pids = build_rapm_design(part)
            kept = part.drop_nulls([f"{s}_player_{i}" for s in ("off", "def") for i in range(1, 6)])
            gid = kept["game_id"].rank("dense").cast(pl.Int64).to_numpy()
            # this season's plain RAPM (the target next year) and SPM (next season's prior)
            rapm = nba_rapm(part, league_id=lid)
            logs = _logs(league, season, "Regular Season", raw[league])
            bf = box_features(logs["player"], logs["team"])
            coef_spm = train_spm(bf, rapm.select("player_id", "o_rapm", "d_rapm"))
            spm = nba_spm(bf, coef_spm)
            try:
                lpo = _logs(league, season, "Playoffs", raw[league])
                spm_po = nba_spm(box_features(lpo["player"], lpo["team"]), coef_spm) if lpo["player"].height else None
            except SystemExit:
                spm_po = None
            P = len(pids)
            pm = np.zeros(2 * P)
            if prior is not None:
                look = {int(r[0]): (r[1], r[2]) for r in prior.select("player_id", "ospm", "dspm").iter_rows()}
                for i, pid in enumerate(pids):
                    o, d = look.get(int(pid), (0.0, 0.0))
                    pm[i], pm[P + i] = o / 100.0, -d / 100.0
            Xp = np.asarray(X @ pm).ravel()
            Y = np.column_stack([y - k * Xp for k in PRIOR_SCALES])
            res = season_curves(X, Y, gid)
            counts = np.asarray(X.sum(axis=0)).ravel()
            r = rapm.select("player_id", "rapm").join(
                pl.DataFrame({"player_id": np.asarray(pids, dtype=np.int64)}), on="player_id", how="right"
            )
            s = spm.select("player_id", "spm").join(
                pl.DataFrame({"player_id": np.asarray(pids, dtype=np.int64)}), on="player_id", how="right"
            )
            np.savez_compressed(
                f, pids=np.asarray(pids), counts=counts, prior=pm, has_prior=prior is not None,
                rapm=r["rapm"].to_numpy(), spm=s["spm"].to_numpy(), **res,
            )  # fmt: skip
            prior = _blend(spm, spm_po)
            prior.write_parquet(p_next)
            print(
                f"{league} {season}: n={X.shape[0]:,} P={P} prior={'yes' if res and pm.any() else 'no'} {time.time() - t0:.0f}s",
                flush=True,
            )


def _pick(curve: np.ndarray) -> float:
    return float(GRID[int(np.argmin(curve))])


def _adj(z: dict, k: int, alpha: float) -> pl.DataFrame:
    """One season's adjusted RAPM at prior scale index ``k`` and grid penalty ``alpha`` (per 100)."""
    a = int(np.argmin(np.abs(np.log(GRID) - np.log(alpha))))
    P = z["pids"].size
    beta = PRIOR_SCALES[k] * z["prior"] + z["coef"][k, :, a]
    o, d = 100 * beta[:P], -100 * beta[P:]
    return pl.DataFrame(
        {
            "player_id": z["pids"].astype(np.int64),
            "v": o + d,
            "off_poss": z["counts"][:P],
            "poss": z["counts"][:P] + z["counts"][P:],
        }
    )


def _col(z: dict, name: str) -> pl.DataFrame:
    P = z["pids"].size
    return pl.DataFrame(
        {
            "player_id": z["pids"].astype(np.int64),
            "v": z[name],
            "off_poss": z["counts"][:P],
            "poss": z["counts"][:P] + z["counts"][P:],
        }
    )


def _forecast(frames: dict[int, pl.DataFrame], target: dict[int, pl.DataFrame], floor: int) -> dict[str, list]:
    """Next-season fit to the target (next season's plain RAPM): Spearman and R^2 of a line fit the pair before."""
    pairs = {}
    for t, a in frames.items():
        if t + 1 in target:
            p = (
                a.filter(pl.col("poss") >= floor)
                .join(
                    target[t + 1].filter(pl.col("poss") >= floor).select("player_id", pl.col("v").alias("nxt")),
                    on="player_id",
                )
                .drop_nans()
            )
            if p.height >= 30:
                pairs[t] = p
    sp, r2 = {}, {}
    for t, p in pairs.items():
        x, y = p["v"].to_numpy(), p["nxt"].to_numpy()
        sp[t] = float(spearmanr(x, y)[0])
        if t - 1 in pairs:
            q = pairs[t - 1]
            b, a0 = np.polyfit(q["v"].to_numpy(), q["nxt"].to_numpy(), 1)
            r2[t] = float(1 - ((y - (a0 + b * x)) ** 2).sum() / ((y - y.mean()) ** 2).sum())
    return {"spearman": sp, "r2": r2}


def _summ(d: dict[int, float]) -> list[float]:
    v = np.array(list(d.values()))
    return [float(v.mean()), float(v.std(ddof=1) / np.sqrt(v.size)), int(v.size)] if v.size > 1 else [float("nan")] * 3


def _paired(a: dict[int, float], b: dict[int, float]) -> list[float]:
    keys = sorted(set(a) & set(b))
    return _summ({k: a[k] - b[k] for k in keys})


def _league_report(out: Path, league: str) -> dict:
    seasons, train, _, floor, sc_floor = LEAGUES[league]
    z = {s: dict(np.load(f)) for s in seasons if (f := out / f"{league}_{s}_rs.npz").exists()}
    missing = sorted(set(train) - set(z))
    if missing:
        raise SystemExit(f"{league}: no curves for training seasons {missing}; run `curves` first")
    old_grid = np.isin(np.round(GRID, 6), np.round(DEFAULT_RAPM_ALPHAS, 6))
    pooled = {k: sum(z[s]["cv_sse"][ki].sum(axis=0) for s in train) for ki, k in enumerate(PRIOR_SCALES)}
    picks = {k: _pick(c) for k, c in pooled.items()}
    k1, kc = PRIOR_SCALES.index(1.0), PRIOR_SCALES.index(CHOSEN_SCALE)
    frozen = picks[CHOSEN_SCALE]
    n_tot = sum(z[s]["cv_n"].sum() for s in train)
    best_mse = {k: float(pooled[k].min() / n_tot) for k in PRIOR_SCALES}
    # per-season held-out MSE at each k's pooled pick, paired against k = 1 (SE over seasons)
    per = {
        k: {s: float(z[s]["cv_sse"][ki][:, np.isclose(GRID, picks[k])].sum() / z[s]["cv_n"].sum()) for s in train}
        for ki, k in enumerate(PRIOR_SCALES)
    }
    vs_k1 = {f"{k}": [x * 1e4 for x in _paired(per[k], per[1.0])[:2]] + [len(train)] for k in PRIOR_SCALES}
    rows, before, after, plain, spm = [], {}, {}, {}, {}
    for s, v in sorted(z.items()):
        old = float(GRID[old_grid][int(np.argmin(v["loo_mse"][old_grid]))])
        b, a = _adj(v, k1, old), _adj(v, kc, frozen)
        before[s], after[s], plain[s], spm[s] = b, a, _col(v, "rapm"), _col(v, "spm")
        q = lambda f: float(f.filter(pl.col("off_poss") >= sc_floor)["v"].std())  # noqa: E731
        rows.append({"season": s, "old_pick": old, "sd_adj_before": q(b), "sd_adj_after": q(a), "sd_rapm": q(plain[s]),
                     "sd_spm": q(spm[s]), "n": int(b.filter(pl.col("off_poss") >= sc_floor).height)})  # fmt: skip
    fc = {
        name: _forecast(fr, plain, floor)
        for name, fr in (("adj_before", before), ("adj_after", after), ("rapm", plain), ("spm", spm))
    }
    for k in PRIOR_SCALES:  # each prior scale at its own pooled pick
        ki = PRIOR_SCALES.index(k)
        fc[f"adj_k{k}"] = _forecast({s: _adj(v, ki, picks[k]) for s, v in z.items()}, plain, floor)
    stab = {name: _forecast(fr, fr, floor)["spearman"] for name, fr in (("adj_before", before), ("adj_after", after))}
    return {
        "frozen": frozen, "scale": CHOSEN_SCALE, "picks": {f"{k}": p for k, p in picks.items()}, "best_mse": best_mse, "mse_vs_k1_bp": vs_k1,
        "seasons": rows,
        "forecast": {n: {m: _summ(d) for m, d in f.items()} for n, f in fc.items()},
        "after_minus_before": {m: _paired(fc["adj_after"][m], fc["adj_before"][m]) for m in ("spearman", "r2")},
        "after_minus_rapm": {m: _paired(fc["adj_after"][m], fc["rapm"][m]) for m in ("spearman", "r2")},
        "rank_stability": {n: _summ(d) for n, d in stab.items()},
        "rank_stability_after_minus_before": _paired(stab["adj_after"], stab["adj_before"]),
    }  # fmt: skip


def report(out: Path) -> None:
    res = {lg: _league_report(out, lg) for lg in LEAGUES}
    (out / "report.json").write_text(json.dumps(res, indent=1))
    sse = [
        {
            "league": lg,
            "season": s,
            "k": k,
            "alpha": GRID.tolist(),
            "sse": np.load(f)["cv_sse"][ki].sum(axis=0).tolist(),
        }
        for lg, (seasons, *_) in LEAGUES.items()
        for s in seasons
        if (f := out / f"{lg}_{s}_rs.npz").exists()
        for ki, k in enumerate(PRIOR_SCALES)
    ]
    pl.DataFrame(sse).explode("alpha", "sse").write_parquet(out / "cv_sse.parquet")
    with pl.Config(tbl_rows=-1, tbl_cols=-1, tbl_width_chars=250, float_precision=3):
        for lg, r in res.items():
            print(
                f"\n== {lg}: frozen {r['frozen']:,.0f} at prior scale {r['scale']}; pooled picks per scale {r['picks']}"
            )
            print("held-out MSE vs k=1, basis points (mean, SE):", r["mse_vs_k1_bp"])
            print(pl.DataFrame(r["seasons"]))
            print("forecast (mean, SE, pairs):", json.dumps(r["forecast"]))
            print("after - before:", r["after_minus_before"], "after - rapm:", r["after_minus_rapm"])
            print("rank stability:", r["rank_stability"], "after - before:", r["rank_stability_after_minus_before"])


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="fit_adj_rapm_penalty")
    ap.add_argument("stage", choices=["curves", "report"])
    ap.add_argument("out", type=Path)
    ap.add_argument(
        "--nba-raw", default="https://raw.githubusercontent.com/sportsdataverse/hoopR-nba-stats-raw/main/nba_stats/json"
    )
    ap.add_argument(
        "--wnba-raw",
        default="https://raw.githubusercontent.com/sportsdataverse/wehoop-wnba-stats-raw/main/wnba_stats/json",
    )
    ap.add_argument("--league", choices=list(LEAGUES), default=None, help="curves for one league only")
    a = ap.parse_args(argv)
    if a.stage == "curves":
        if a.league:
            for other in [lg for lg in LEAGUES if lg != a.league]:
                LEAGUES.pop(other)
        curves(a.out, {"nba": a.nba_raw, "wnba": a.wnba_raw})
    else:
        report(a.out)
    return 0


if __name__ == "__main__":
    sys.exit(main())
