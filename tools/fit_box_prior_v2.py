"""Fit and gate box prior v2 and the REML season rating (BB-D38), NBA regular seasons 1997-2026.

Stages (each skips work already on disk under ``OUT``):

* ``panel`` -- per season: the RAPM sufficient statistics (:func:`~sportsdataverse.nba.nba_box_prior.season_gram`),
  the full-season box-prior features and the ages. Inputs: the ``nba_stats_possessions`` release assets
  (``--poss-dir`` holding ``nba_possessions_{end year}.parquet``, else the loader) and the hoopR-nba-stats-raw
  store (``--raw``, a checkout or an ``http(s)://`` base): ``leaguegamelog`` (box), ``leaguedashplayerbiostats``
  (age) and the historical ``playerindex`` of the last season (height, listed position: one static value per
  player, so the chronological split reads a height captured after its cut; a height does not move with games).
* ``prior`` -- per season ``s``: the career target over :func:`box_prior_training_seasons` (walk-forward from
  2006, leave-season-out through 2005) centred on ``s``, the cross-fitted prior model, and ``s``'s prior.
* ``gate`` -- per complete season 1998-2026: five game-grouped folds and one chronological split (fit on the
  games before the date by which 60% of the season's games were played, score the rest). Each split fits on
  the training games only (the v2 prior's features rebuilt from them) and scores the held-out possessions'
  points under: ``adj`` (today's ``nba_adj_rapm``: the previous season's SPM prior x2 at the frozen penalty,
  read from ``--adj-prior``, the ``*_prior_next.parquet`` of ``tools/fit_adj_rapm_penalty.py``), ``rapm``
  (plain, frozen penalty), ``v2`` (v2 prior, REML penalties: the candidate), ``v2_fixed`` (v2 prior at the
  adjusted-RAPM penalty), ``adj_reml`` (the previous season's SPM unscaled, REML penalties) and ``prior``
  (the v2 prior alone). A player absent from the training games is scored at his prior: last season's SPM x2
  under ``adj``, the league average (0) under the v2 methods, which have no box line for him. Also the
  full-season v2 rating per season.
* ``report`` -- pooled held-out error per method, paired differences against ``adj`` and of ``v2_fixed``
  against ``v2`` (basis points of MSE, SE over seasons, all seasons and the strictly walk-forward 2006+), REML penalties and prior weights per season, and the long-window oracle check: Spearman and
  spread of the pooled 2020-2026 prior against an outside 8-season RAPM (``--oracle``, not redistributed;
  only the summary numbers are written) beside the current SPM / BPM / adjusted RAPM (``--impact-dir``, the
  ``nba_player_impact_{yr}.parquet`` release files). Writes ``report.json`` and ``gate.parquet``, which
  ``tests/fixtures/bb_box_prior_v2`` commits.

Usage::

    uv run --frozen python tools/fit_box_prior_v2.py panel OUT --poss-dir DIR --raw DIR
    uv run --frozen python tools/fit_box_prior_v2.py prior OUT
    uv run --frozen python tools/fit_box_prior_v2.py gate OUT --poss-dir DIR --raw DIR --adj-prior DIR
    uv run --frozen python tools/fit_box_prior_v2.py report OUT --oracle CSV --impact-dir DIR
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
import time
from pathlib import Path

import numpy as np
import polars as pl
from sklearn.model_selection import GroupKFold

from sportsdataverse.nba.nba_adj_rapm import ADJ_RAPM_PENALTY, ADJ_RAPM_PRIOR_SCALE, _fit_prior_ridge
from sportsdataverse.nba.nba_box_prior import (
    BoxPriorModel,
    apply_box_prior,
    box_prior_features,
    box_prior_training_seasons,
    career_rapm_target,
    season_gram,
    SeasonGram,
    train_box_prior,
)
from sportsdataverse.nba.nba_player_positions import _position_to_num
from sportsdataverse.nba.nba_possessions import nba_raw_store_season_frame
from sportsdataverse.nba.nba_rapm import RAPM_PENALTY, build_rapm_design
from sportsdataverse.nba.nba_reml_rapm import REML_PENALTY_BOUNDS, fit_reml_prior_ridge, nba_reml_rapm

#: Complete regular seasons (fit 2026-11-10): the career target's span and the priors fit. 2026-27 is in
#: progress and its player game logs are not in the raw store yet, so its prior is a publish-time follow-up.
SEASONS = range(1997, 2027)
COMPLETE = SEASONS
GATE_SEASONS = range(1998, 2027)
FOLDS = 5
CHRONO_SHARE = 0.6
_OFF = [f"off_player_{i}" for i in range(1, 6)]
_DEF = [f"def_player_{i}" for i in range(1, 6)]


def _poss(season: int, poss_dir: str | None) -> pl.DataFrame:
    if poss_dir:
        p = pl.read_parquet(f"{poss_dir}/nba_possessions_{season}.parquet")
    else:
        from sportsdataverse.nba.nba_loaders import load_nba_stats_possessions

        p = load_nba_stats_possessions(season - 1)
    p = p.with_columns(pl.col("game_id").cast(pl.Utf8))
    return p.filter(pl.col("game_id").str.starts_with("002")).drop_nulls(["points", *_OFF, *_DEF])


def _logs(season: int, raw: str) -> tuple[pl.DataFrame, pl.DataFrame]:
    def get(variant: str) -> pl.DataFrame:
        f = nba_raw_store_season_frame("leaguegamelog", season, variant, raw_store_dir=raw)
        if f is None:
            raise SystemExit(f"{season} leaguegamelog/{variant} is not in the raw store {raw}")
        return f.with_columns(
            pl.col("game_id").cast(pl.Utf8),
            pl.col("player_id" if "player_id" in f.columns else "team_id").cast(pl.Int64),
        )

    p, t = get("regular-season_p"), get("regular-season")
    return p.with_columns(pl.col("team_id").cast(pl.Int64)), t.with_columns(pl.col("team_id").cast(pl.Int64))


def _height(h: str | None) -> float | None:
    if not h or "-" not in str(h):
        return None
    ft, inch = str(h).split("-", 1)
    try:
        return 12.0 * float(ft) + float(inch)
    except ValueError:
        return None


def _bio(raw: str) -> tuple[pl.DataFrame, pl.DataFrame]:
    """``(ages: player_id, season, age)`` over :data:`SEASONS` and ``(static: player_id, height_in, position_num)``."""
    rows = []
    for s in SEASONS:
        b = nba_raw_store_season_frame("leaguedashplayerbiostats", s, "regular-season_totals", raw_store_dir=raw)
        if b is not None:
            rows.append(
                b.select(
                    pl.col("player_id").cast(pl.Int64),
                    pl.lit(s, pl.Int64).alias("season"),
                    pl.col("age").cast(pl.Float64),
                )
            )
    ages = pl.concat(rows).unique(["player_id", "season"])
    idx = nba_raw_store_season_frame("playerindex", max(SEASONS), None, raw_store_dir=raw)
    static = idx.select(
        pl.col("person_id").cast(pl.Int64).alias("player_id"),
        pl.col("height").map_elements(_height, return_dtype=pl.Float64).alias("height_in"),
        pl.col("position").map_elements(_position_to_num, return_dtype=pl.Float64).alias("position_num"),
    ).unique("player_id")
    return ages, static


def _fill_ages(ages: pl.DataFrame, players: pl.DataFrame) -> pl.DataFrame:
    """Every (player, season) of ``players`` gets an age: his own, else the nearest captured season's +/- the gap."""
    have = ages.rename({"season": "s0", "age": "a0"})
    j = players.join(have, on="player_id", how="left").with_columns(
        (pl.col("season") - pl.col("s0")).abs().alias("gap")
    )
    near = j.sort("gap").group_by("player_id", "season").first()
    return near.select("player_id", "season", (pl.col("a0") + (pl.col("season") - pl.col("s0"))).alias("age"))


def panel(out: Path, poss_dir: str | None, raw: str) -> None:
    out.mkdir(parents=True, exist_ok=True)
    ages, static = _bio(raw)
    static.write_parquet(out / "static.parquet")
    seen = []
    for s in SEASONS:
        g_f, f_f = out / f"gram_{s}.npz", out / f"feat_{s}.parquet"
        poss = _poss(s, poss_dir)
        players = pl.DataFrame(
            {
                "player_id": pl.Series(
                    sorted(set(np.concatenate([poss[c].to_numpy() for c in _OFF + _DEF]).tolist())), dtype=pl.Int64
                )
            }
        )
        seen.append(players.with_columns(pl.lit(s, pl.Int64).alias("season")))
        if g_f.exists() and f_f.exists():
            continue
        t0 = time.time()
        g = season_gram(poss)
        np.savez_compressed(g_f, pids=g.pids, gram=g.gram, xty=g.xty, counts=g.counts, n=g.n, ysum=g.ysum)
        pl_logs, tm_logs = _logs(s, raw)
        bio = _fill_ages(ages, players.with_columns(pl.lit(s, pl.Int64).alias("season"))).join(
            static, on="player_id", how="left"
        )
        feats = box_prior_features(pl_logs, tm_logs, poss, bio)
        feats.with_columns(pl.lit(s, pl.Int64).alias("season")).write_parquet(f_f)
        print(f"{s}: n={g.n:,} P={g.pids.size} feats={feats.height} {time.time() - t0:.0f}s", flush=True)
    _fill_ages(ages, pl.concat(seen)).write_parquet(out / "ages.parquet")


def _gram(out: Path, s: int) -> SeasonGram:
    z = np.load(out / f"gram_{s}.npz")
    return SeasonGram(
        pids=z["pids"], gram=z["gram"], xty=z["xty"], counts=z["counts"], n=int(z["n"]), ysum=float(z["ysum"])
    )


def _save_model(m: BoxPriorModel, f: Path) -> None:
    np.savez(f, o_coef=m.o_coef, o_intercept=m.o_intercept, d_coef=m.d_coef, d_intercept=m.d_intercept,
             season=m.season, train_seasons=np.asarray(m.train_seasons), feature_names=np.asarray(m.feature_names))  # fmt: skip


def _load_model(f: Path) -> BoxPriorModel:
    z = np.load(f)
    return BoxPriorModel(
        feature_names=[str(x) for x in z["feature_names"]], o_coef=z["o_coef"], o_intercept=z["o_intercept"],
        d_coef=z["d_coef"], d_intercept=z["d_intercept"], season=int(z["season"]), train_seasons=[int(x) for x in z["train_seasons"]],
    )  # fmt: skip


def _prior_for(
    s: int, grams: dict[int, SeasonGram], ages: pl.DataFrame, feats: dict[int, pl.DataFrame]
) -> tuple[BoxPriorModel, list[int]]:
    """Season ``s``'s prior model and its training seasons: the career target and the panel never read ``s``."""
    train = box_prior_training_seasons(s, grams)
    if s in train:
        raise AssertionError(f"the prior for {s} would train on {s}")
    target = career_rapm_target({t: grams[t] for t in train}, ages, seasons=train, center=s)
    pan = pl.concat([feats[t] for t in train]).join(
        target.select("player_id", "season", "o_target", "d_target"), on=["player_id", "season"], how="inner"
    )
    return train_box_prior(pan, season=s), train


def prior(out: Path) -> None:
    grams = {s: _gram(out, s) for s in COMPLETE}
    ages = pl.read_parquet(out / "ages.parquet")
    feats = {s: pl.read_parquet(out / f"feat_{s}.parquet") for s in SEASONS}
    if not (out / "career_target_all.parquet").exists():  # the long-window oracle check of the target itself
        career_rapm_target(grams, ages, seasons=COMPLETE, center=2023).write_parquet(out / "career_target_all.parquet")
    for s in SEASONS:
        f_m, f_p = out / f"model_{s}.npz", out / f"prior_{s}.parquet"
        if f_m.exists() and f_p.exists():
            continue
        t0 = time.time()
        model, train = _prior_for(s, grams, ages, feats)
        _save_model(model, f_m)
        apply_box_prior(feats[s], model).join(
            feats[s].select("player_id", "poss", "def_poss"), on="player_id"
        ).write_parquet(f_p)
        print(
            f"prior {s}: train {train[0]}-{train[-1]} ({len(train)}) {time.time() - t0:.0f}s",
            flush=True,
        )


def _predict(test: pl.DataFrame, coef: dict[int, tuple[float, float]], intercept: float) -> np.ndarray:
    """Points per held-out possession: intercept + offense coefficients + defense coefficients."""
    off = test.select(_OFF).to_numpy().astype(np.int64)
    de = test.select(_DEF).to_numpy().astype(np.int64)
    oc = np.vectorize(lambda p: coef.get(int(p), (0.0, 0.0))[0], otypes=[float])(off)
    dc = np.vectorize(lambda p: coef.get(int(p), (0.0, 0.0))[1], otypes=[float])(de)
    return intercept + oc.sum(1) + dc.sum(1)


def _coef_map(pids: list, beta: np.ndarray, fallback: dict[int, tuple[float, float]]) -> dict[int, tuple[float, float]]:
    """Per-possession ``{pid: (offense, defense)}``: the fit for training players, ``fallback`` (the prior) else."""
    P = len(pids)
    m = dict(fallback)
    m.update({int(p): (float(beta[i]), float(beta[P + i])) for i, p in enumerate(pids)})
    return m


def _prior_vec(pids: list, prior: dict[int, tuple[float, float]], scale: float = 1.0) -> np.ndarray:
    """Per-possession prior mean ``(2P,)`` from a per-100 ``{pid: (o, d)}``."""
    o = np.array([prior.get(int(p), (0.0, 0.0))[0] for p in pids])
    d = np.array([prior.get(int(p), (0.0, 0.0))[1] for p in pids])
    return scale * np.concatenate([o, -d]) / 100.0


def _per_poss(prior: dict[int, tuple[float, float]], scale: float = 1.0) -> dict[int, tuple[float, float]]:
    return {p: (scale * o / 100.0, -scale * d / 100.0) for p, (o, d) in prior.items()}


def _split_rows(
    s: int, split: str, train: pl.DataFrame, test: pl.DataFrame, v2_prior: dict, adj_prior: dict
) -> list[dict]:
    X, y, pids = build_rapm_design(train)
    yt = test["points"].to_numpy().astype(np.float64)
    rows = []

    def score(method: str, coef: dict, intercept: float, **extra) -> None:
        e = yt - _predict(test, coef, intercept)
        rows.append({"season": s, "split": split, "method": method, "sse": float(e @ e), "n": int(yt.size), **extra})

    k = ADJ_RAPM_PRIOR_SCALE["00"]
    f = _fit_prior_ridge(X, y, _prior_vec(pids, adj_prior, k), alpha=ADJ_RAPM_PENALTY["00"], n_samples=0)
    score("adj", _coef_map(pids, f.coef, _per_poss(adj_prior, k)), f.intercept)
    f = fit_reml_prior_ridge(X, y, np.zeros(2 * len(pids)), lambdas=(RAPM_PENALTY["00"],) * 2)
    score("rapm", _coef_map(pids, f.coef, {}), f.intercept)
    mu = _prior_vec(pids, v2_prior)
    counts = np.asarray(X.sum(axis=0)).ravel()
    P = len(pids)
    for method, lam in (("v2", None), ("v2_fixed", (ADJ_RAPM_PENALTY["00"],) * 2)):
        f = fit_reml_prior_ridge(X, y, mu, lambdas=lam)
        pw_o = float((f.prior_weight[:P] * counts[:P]).sum() / counts[:P].sum())
        pw_d = float((f.prior_weight[P:] * counts[P:]).sum() / counts[P:].sum())
        score(
            method,
            _coef_map(pids, f.coef, _per_poss(v2_prior)),
            f.intercept,
            lambda_o=f.lambda_o,
            lambda_d=f.lambda_d,
            prior_weight_o=pw_o,
            prior_weight_d=pw_d,
        )
    f = fit_reml_prior_ridge(X, y, _prior_vec(pids, adj_prior))
    score(
        "adj_reml", _coef_map(pids, f.coef, _per_poss(adj_prior)), f.intercept, lambda_o=f.lambda_o, lambda_d=f.lambda_d
    )
    score("prior", _per_poss(v2_prior), float((y - X @ mu).mean()))
    return rows


def _prior_dict(frame: pl.DataFrame) -> dict[int, tuple[float, float]]:
    cols = ("ospm", "dspm") if "ospm" in frame.columns else ("o_prior", "d_prior")
    return {int(r[0]): (float(r[1]), float(r[2])) for r in frame.select("player_id", *cols).iter_rows()}


def _split_inputs(
    poss: pl.DataFrame,
    pl_logs: pl.DataFrame,
    tm_logs: pl.DataFrame,
    bio: pl.DataFrame,
    model: BoxPriorModel,
    tr_games: list[str],
) -> tuple[pl.DataFrame, pl.DataFrame, dict[int, tuple[float, float]]]:
    """One split's training possessions, held-out possessions and v2 prior (features from training games only)."""
    train = poss.filter(pl.col("game_id").is_in(tr_games))
    test = poss.filter(~pl.col("game_id").is_in(tr_games))
    feats = box_prior_features(pl_logs, tm_logs, poss, bio, game_ids=tr_games)
    return train, test, _prior_dict(apply_box_prior(feats, model))


def gate(out: Path, poss_dir: str | None, raw: str, adj_dir: str) -> None:
    static = pl.read_parquet(out / "static.parquet")
    ages = pl.read_parquet(out / "ages.parquet")
    for s in GATE_SEASONS:
        f_g, f_r = out / f"gate_{s}.parquet", out / f"rating_{s}.parquet"
        if f_g.exists() and f_r.exists():
            continue
        t0 = time.time()
        poss = _poss(s, poss_dir)
        pl_logs, tm_logs = _logs(s, raw)
        bio = ages.filter(pl.col("season") == s).join(static, on="player_id", how="left")
        model = _load_model(out / f"model_{s}.npz")
        adj_prior = _prior_dict(pl.read_parquet(f"{adj_dir}/nba_{s - 1}_prior_next.parquet"))
        full_prior = _prior_dict(pl.read_parquet(out / f"prior_{s}.parquet"))
        nba_reml_rapm(poss, full_prior).with_columns(pl.lit(s, pl.Int64).alias("season")).write_parquet(f_r)
        games = poss["game_id"].unique().sort()
        gid = poss["game_id"].to_numpy()
        splits = [
            (f"fold{k}", poss["game_id"].gather(tr).unique())
            for k, (tr, _) in enumerate(GroupKFold(n_splits=FOLDS).split(gid, groups=gid))
        ]
        dates = pl_logs.select("game_id", "game_date").unique("game_id").sort("game_date")
        cut = dates["game_date"][int(CHRONO_SHARE * dates.height)]
        splits.append(("chrono", dates.filter(pl.col("game_date") < cut)["game_id"]))
        rows = []
        for name, tr_games in splits:
            train, test, v2_prior = _split_inputs(poss, pl_logs, tm_logs, bio, model, tr_games.to_list())
            rows += _split_rows(s, name, train, test, v2_prior, adj_prior)
        pl.DataFrame(rows).write_parquet(f_g)
        print(f"gate {s}: {len(games)} games, chrono cut {cut} {time.time() - t0:.0f}s", flush=True)


def _summ(v: np.ndarray) -> list[float]:
    return [float(v.mean()), float(v.std(ddof=1) / np.sqrt(v.size)), int(v.size)]


def _oracle(out: Path, oracle: str, impact_dir: str) -> dict:
    """The long-window oracle check: pooled 2020-2026 RS ratings vs an outside 8-season RAPM (Spearman, std at Poss >= 10k)."""
    e = pl.read_csv(oracle, schema_overrides={"ID": pl.Int64}).rename({"ID": "player_id"})
    sha = hashlib.sha256(Path(oracle).read_bytes()).hexdigest()
    yrs = range(2020, 2027)
    cur = pl.concat(
        [pl.read_parquet(f"{impact_dir}/nba_player_impact_{y}.parquet") for y in yrs], how="diagonal_relaxed"
    ).filter(pl.col("season_type") == "Regular Season")
    cur = cur.with_columns(pl.col("player_id").cast(pl.Int64))
    new = pl.concat([pl.read_parquet(out / f"rating_{y}.parquet") for y in yrs]).rename({"off_poss": "w"})
    new = new.with_columns((pl.col("o_box_prior") + pl.col("d_box_prior")).alias("box_prior_v2"))

    def pool(f: pl.DataFrame, cols: list[str], w: str) -> pl.DataFrame:
        return f.group_by("player_id").agg(*[((pl.col(c) * pl.col(w)).sum() / pl.col(w).sum()).alias(c) for c in cols])

    tgt = pl.read_parquet(out / "career_target_all.parquet").filter(pl.col("season").is_in(list(yrs)))
    tgt = tgt.with_columns((pl.col("o_target") + pl.col("d_target")).alias("career_target"))
    g = pool(cur, ["spm", "bpm", "rapm", "adj_rapm"], "off_poss").join(
        pool(new, ["box_prior_v2", "reml_rapm"], "w"), on="player_id", how="full", coalesce=True
    )
    g = g.join(pool(tgt, ["career_target"], "off_poss"), on="player_id", how="left")
    assert e.schema["player_id"] == g.schema["player_id"]
    j = e.join(g, on="player_id", how="inner")
    assert j.height >= 1100, f"oracle match fell to {j.height}"
    # career_target is the 1997-2026 fit centred on 2023: in-sample to the oracle window (a sanity row, not a gate)
    res = {
        "matched": j.height,
        "oracle_sha256": sha,
        "window": "2019-20 to 2026-27 regular seasons, captured 2026-11-02",
    }
    for thr in (0, 10_000):
        k = j.filter(pl.col("Poss") >= thr).drop_nulls()
        res[f"poss_ge_{thr}"] = {
            "n": k.height,
            "spearman": {c: round(k.select(pl.corr("Tot", c, method="spearman")).item(), 3) for c in ["spm", "bpm", "rapm", "adj_rapm", "box_prior_v2", "reml_rapm", "career_target"]},
            "std": {c: round(float(k[c].std()), 2) for c in ["Tot", "spm", "bpm", "rapm", "adj_rapm", "box_prior_v2", "reml_rapm", "career_target"]},
        }  # fmt: skip
    return res


def report(out: Path, oracle: str | None, impact_dir: str | None) -> None:
    g = pl.concat([pl.read_parquet(out / f"gate_{s}.parquet") for s in GATE_SEASONS], how="diagonal_relaxed")
    g.write_parquet(out / "gate.parquet")
    res: dict = {}
    for kind, filt in (("heldout", pl.col("split").str.starts_with("fold")), ("chrono", pl.col("split") == "chrono")):
        per = (
            g.filter(filt)
            .group_by("season", "method")
            .agg((pl.col("sse").sum() / pl.col("n").sum()).alias("mse"))
            .pivot("method", index="season", values="mse")
            .sort("season")
        )
        r = {
            "pooled_mse": {
                m: float(
                    g.filter(filt & (pl.col("method") == m))["sse"].sum()
                    / g.filter(filt & (pl.col("method") == m))["n"].sum()
                )
                for m in per.columns
                if m != "season"
            }
        }
        for m in per.columns:
            if m in ("season", "adj"):
                continue
            d = (per["adj"] - per[m]).to_numpy() * 1e4
            r[f"adj_minus_{m}_bp"] = _summ(d) + [int((d > 0).sum())]
            wf = per.filter(pl.col("season") >= 2006)
            dw = (wf["adj"] - wf[m]).to_numpy() * 1e4
            r[f"adj_minus_{m}_bp_2006plus"] = _summ(dw) + [int((dw > 0).sum())]
        # the penalty rule itself: positive = the fixed adjusted-RAPM penalty beats the REML pick
        for lab, frame in (("", per), ("_2006plus", per.filter(pl.col("season") >= 2006))):
            d = (frame["v2"] - frame["v2_fixed"]).to_numpy() * 1e4
            r[f"v2_minus_v2_fixed_bp{lab}"] = _summ(d) + [int((d > 0).sum())]
        res[kind] = r
    lam = (
        g.filter((pl.col("method") == "v2") & pl.col("split").str.starts_with("fold"))
        .group_by("season")
        .agg(
            pl.col("lambda_o").median(),
            pl.col("lambda_d").median(),
            pl.col("prior_weight_o").mean(),
            pl.col("prior_weight_d").mean(),
        )
        .sort("season")
    )
    res["reml_by_season"] = lam.to_dicts()
    ratings = pl.concat([pl.read_parquet(out / f"rating_{s}.parquet") for s in GATE_SEASONS])
    res["full_season"] = ratings.group_by("season").agg(
        pl.col("lambda_o").first(), pl.col("lambda_d").first(),
        ((pl.col("o_prior_weight") * pl.col("off_poss")).sum() / pl.col("off_poss").sum()).alias("prior_weight_o"),
        ((pl.col("d_prior_weight") * pl.col("def_poss")).sum() / pl.col("def_poss").sum()).alias("prior_weight_d"),
        pl.col("reml_rapm").filter(pl.col("off_poss") >= 1000).std().alias("sd_reml_rapm"),
        (pl.col("o_box_prior") + pl.col("d_box_prior")).filter(pl.col("off_poss") >= 1000).std().alias("sd_box_prior"),
    ).sort("season").to_dicts()  # fmt: skip
    wo, wd = pl.col("o_prior_weight") * pl.col("off_poss"), pl.col("d_prior_weight") * pl.col("def_poss")
    res["prior_weight_all_seasons"] = ratings.select(
        (wo.sum() / pl.col("off_poss").sum()).alias("offense"),
        (wd.sum() / pl.col("def_poss").sum()).alias("defense"),
        ((wo.sum() + wd.sum()) / (pl.col("off_poss").sum() + pl.col("def_poss").sum())).alias("net"),
        (pl.col("lambda_o") >= 0.99 * REML_PENALTY_BOUNDS[1]).filter(pl.col("player_id") == pl.col("player_id").first().over("season")).sum().alias("seasons_offense_at_bound"),
    ).to_dicts()[0]  # fmt: skip
    if oracle and impact_dir:
        res["oracle_8yr"] = _oracle(out, oracle, impact_dir)
    (out / "report.json").write_text(json.dumps(res, indent=1))
    print(json.dumps({k: v for k, v in res.items() if k not in ("reml_by_season", "full_season")}, indent=1))
    with pl.Config(tbl_rows=-1, tbl_cols=-1, float_precision=3):
        print(pl.DataFrame(res["full_season"]))


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="fit_box_prior_v2")
    ap.add_argument("stage", choices=["panel", "prior", "gate", "report"])
    ap.add_argument("out", type=Path)
    ap.add_argument("--poss-dir", default=None)
    ap.add_argument(
        "--raw", default="https://raw.githubusercontent.com/sportsdataverse/hoopR-nba-stats-raw/main/nba_stats/json"
    )
    ap.add_argument("--adj-prior", default=None)
    ap.add_argument("--oracle", default=None)
    ap.add_argument("--impact-dir", default=None)
    a = ap.parse_args(argv)
    if a.stage == "panel":
        panel(a.out, a.poss_dir, a.raw)
    elif a.stage == "prior":
        prior(a.out)
    elif a.stage == "gate":
        gate(a.out, a.poss_dir, a.raw, a.adj_prior)
    else:
        report(a.out, a.oracle, a.impact_dir)
    return 0


if __name__ == "__main__":
    sys.exit(main())
