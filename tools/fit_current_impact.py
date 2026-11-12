"""Fit and gate the current impact rating (BB-D39): NBA regular seasons, constants 2007-2013, gate 2014-2026.

Stages (each skips work already on disk under ``OUT``):

* ``inputs`` -- per season 2006-2026: the regular-season possessions with their game date
  (``poss_{s}.parquet``) and, at every evaluation date (opening night, then every 7 days through the last
  game day), two box priors built with the BB-D38 model for that season (``--d38``, the ``OUT`` of
  ``tools/fit_box_prior_v2.py``: ``model_{s}.npz`` learned from seasons before ``s``, ``ages`` / ``static``):
  ``window`` (:func:`~sportsdataverse.nba.nba_current_impact.current_impact_prior`: the 365 days of box
  lines before the date, so last season's at opening night) and ``season`` (D38's in-season prior: this
  season's games before the date only). Everything is built from games dated before the evaluation date.
* ``fit`` -- the four shock / decay SDs, by Nelder-Mead on log SD, minimizing the pooled 28-day forward
  MSE of the filter over the evaluation dates of :data:`FIT_SEASONS` (the filter runs from 2006, so 2006
  is warm-up only). Writes ``fit.json``.
* ``gate`` -- every method at every evaluation date of 2007-2026: the filter at the fitted SDs (one pass
  2006-2026), its ablations (no drift, no team shock, no season shock), the window prior alone, and the
  season-to-date rating (:func:`~sportsdataverse.nba.nba_reml_rapm.nba_reml_rapm`, fixed penalty, on the
  season's games before the date at the ``season`` prior: the baseline the gate is defined on) and the same
  at the ``window`` prior, plus both again at a penalty chosen on :data:`FIT_SEASONS` 28-day forward MSE
  from :data:`BASELINE_PENALTY_GRID` (``*_tuned``, written to ``baseline_fit.json``: the baseline gets the
  same tuning budget as the filter). Methods already in ``gate.parquet`` are not re-scored. Each predicts the points of every possession in the 28 days from the date (same season); the
  league level of each method is the mean residual of its ratings over the 20,000 possessions before the
  date. Writes ``gate.parquet`` (squared error per season, date and method).
* ``report`` -- pooled MSE per method, the baseline-minus-method margin in basis points over
  :data:`GATE_SEASONS` with a season-cluster bootstrap SE (whole seasons resampled: windows overlap and a
  rating's error is shared by every game of a season, so possessions, games and dates are not independent),
  per-season margins and margins by days into the season. Writes ``report.json``.

The gate, fixed before it ran: the filter's pooled 28-day forward MSE over 2014-2026 is lower than the
season-to-date rating's, and the 2.5% quantile of the season-cluster bootstrap margin is above zero.

Usage::

    uv run --frozen python tools/fit_current_impact.py inputs OUT --d38 DIR --poss-dir DIR --raw DIR
    uv run --frozen python tools/fit_current_impact.py fit OUT
    uv run --frozen python tools/fit_current_impact.py gate OUT
    uv run --frozen python tools/fit_current_impact.py report OUT
"""

from __future__ import annotations

import argparse
import datetime as dt
import importlib.util
import json
import sys
import time
from pathlib import Path

import numpy as np
import polars as pl
from scipy import stats
from scipy.optimize import minimize

from sportsdataverse.nba.nba_box_prior import apply_box_prior, box_prior_features
from sportsdataverse.nba.nba_current_impact import CurrentImpactParams, current_impact_prior, run_current_impact
from sportsdataverse.nba.nba_reml_rapm import nba_reml_rapm

_spec = importlib.util.spec_from_file_location("fit_box_prior_v2", Path(__file__).with_name("fit_box_prior_v2.py"))
_d38 = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_d38)  # type: ignore[union-attr]

#: The filter's first season: the box prior is strictly walk-forward from 2006 (earlier priors learned from
#: later seasons), so nothing before it is read, and its players all start at the entry SD.
START = 2006
FIT_SEASONS = range(2007, 2014)
GATE_SEASONS = range(2014, 2027)
SEASONS = range(START, 2027)
STEP_DAYS = 7
HORIZON_DAYS = 28
REF_POSS = 20_000
BOOT = 10_000
#: Penalties the tuned season-to-date baselines choose from on :data:`FIT_SEASONS` (3,728 = ADJ_RAPM_PENALTY).
BASELINE_PENALTY_GRID = (3_728.0, 7_500.0, 15_000.0, 30_000.0, 60_000.0)
_OFF = [f"off_player_{i}" for i in range(1, 6)]
_DEF = [f"def_player_{i}" for i in range(1, 6)]
_KEEP = ["game_id", "game_date", "season", "points", "offense_team_id", "defense_team_id", *_OFF, *_DEF]


def eval_dates(game_dates: pl.Series) -> list[dt.date]:
    """Opening night, then every :data:`STEP_DAYS` days through the season's last game day."""
    d0, d1 = game_dates.min(), game_dates.max()
    return [d0 + dt.timedelta(days=k) for k in range(0, (d1 - d0).days + 1, STEP_DAYS)]


def _season_inputs(s: int, d38: Path, poss_dir: str | None, raw: str) -> tuple[pl.DataFrame, pl.DataFrame]:
    model = _d38._load_model(d38 / f"model_{s}.npz")
    if s in model.train_seasons or max(model.train_seasons) >= s:
        raise AssertionError(f"the {s} box prior learned from season {max(model.train_seasons)}")
    bio = pl.read_parquet(d38 / "ages.parquet").filter(pl.col("season") == s)
    bio = bio.join(pl.read_parquet(d38 / "static.parquet"), on="player_id", how="left")
    frames = {}
    for t in (s - 1, s):
        p = _d38._poss(t, poss_dir)
        lp, lt = _d38._logs(t, raw)
        dates = lp.select("game_id", pl.col("game_date").str.to_date()).unique("game_id")
        frames[t] = (p.join(dates, on="game_id", how="inner"), lp, lt)
    poss = frames[s][0].with_columns(pl.lit(s, pl.Int64).alias("season")).select(_KEEP)
    both = [pl.concat([frames[t][k] for t in (s - 1, s)], how="diagonal_relaxed") for k in range(3)]
    rows = []
    for d in eval_dates(poss["game_date"]):
        w = current_impact_prior(both[1], both[2], both[0], bio, model, d)
        rows.append(w.with_columns(pl.lit("window").alias("kind")))
        gids = poss.filter(pl.col("game_date") < d)["game_id"].unique().to_list()
        if gids:
            f = box_prior_features(frames[s][1], frames[s][2], frames[s][0], bio, game_ids=gids)
            rows.append(
                apply_box_prior(f, model).select(
                    "player_id",
                    pl.lit(d, pl.Date).alias("prior_date"),
                    "o_prior",
                    "d_prior",
                    pl.lit("season").alias("kind"),
                )
            )
    return poss, pl.concat(rows)


def inputs(out: Path, d38: Path, poss_dir: str | None, raw: str) -> None:
    out.mkdir(parents=True, exist_ok=True)
    for s in SEASONS:
        fp, fq = out / f"poss_{s}.parquet", out / f"priors_{s}.parquet"
        if fp.exists() and fq.exists():
            continue
        t0 = time.time()
        poss, pri = _season_inputs(s, d38, poss_dir, raw)
        poss.write_parquet(fp)
        pri.write_parquet(fq)
        print(
            f"inputs {s}: {poss.height:,} poss, {pri['prior_date'].n_unique()} dates {time.time() - t0:.0f}s",
            flush=True,
        )


class _Data:
    """Every season's possessions (date order), priors and evaluation dates, read once."""

    def __init__(self, out: Path, seasons: range) -> None:
        self.seasons = seasons
        self.poss = {s: pl.read_parquet(out / f"poss_{s}.parquet").sort("game_date", "game_id") for s in seasons}
        self.priors = {s: pl.read_parquet(out / f"priors_{s}.parquet") for s in seasons}
        self.dates = {s: eval_dates(self.poss[s]["game_date"]) for s in seasons}
        allp = pl.concat([self.poss[s] for s in seasons])
        self.all_dates = allp["game_date"].to_numpy()
        self.all_off = allp.select(_OFF).to_numpy()
        self.all_def = allp.select(_DEF).to_numpy()
        self.all_y = allp["points"].to_numpy().astype(np.float64)

    def window_priors(self) -> pl.DataFrame:
        return pl.concat([self.priors[s].filter(pl.col("kind") == "window") for s in self.seasons]).drop("kind")

    def test(self, s: int, d: dt.date) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
        p = self.poss[s].filter(
            (pl.col("game_date") >= d) & (pl.col("game_date") < d + dt.timedelta(days=HORIZON_DAYS))
        )
        return p.select(_OFF).to_numpy(), p.select(_DEF).to_numpy(), p["points"].to_numpy().astype(np.float64)

    def ref(self, d: dt.date) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
        k = int(np.searchsorted(self.all_dates, np.datetime64(d), side="left"))
        lo = max(0, k - REF_POSS)
        return self.all_off[lo:k], self.all_def[lo:k], self.all_y[lo:k]


def _lookup(r: dict[int, tuple], ids: np.ndarray, j: int) -> np.ndarray:
    keys = np.fromiter(r.keys(), dtype=np.int64, count=len(r))
    vals = np.fromiter((v[j] for v in r.values()), dtype=np.float64, count=len(r))
    o = np.argsort(keys)
    keys, vals = keys[o], vals[o]
    if not keys.size:
        return np.zeros(ids.shape)
    pos = np.clip(np.searchsorted(keys, ids), 0, keys.size - 1)
    return np.where(keys[pos] == ids, vals[pos], 0.0)


def score(r: dict[int, tuple], test: tuple, ref: tuple) -> tuple[float, int]:
    """``(SSE, n)`` of ``r`` (``{pid: (o, d, ...)}`` per 100) on the test possessions; level from ``ref``."""

    def pred(off: np.ndarray, de: np.ndarray) -> np.ndarray:
        return (_lookup(r, off, 0).sum(1) - _lookup(r, de, 1).sum(1)) / 100.0

    level = float((ref[2] - pred(ref[0], ref[1])).mean())
    e = test[2] - level - pred(test[0], test[1])
    return float(e @ e), int(e.size)


def _filter_scores(data: _Data, params: CurrentImpactParams, seasons: range) -> list[tuple[int, dt.date, float, int]]:
    poss = pl.concat([data.poss[s] for s in data.seasons if s <= max(seasons)])
    days = [d for s in seasons for d in data.dates[s]]
    snaps = run_current_impact(poss, data.window_priors(), days, params=params)
    return [(s, d, *score(snaps[d][1], data.test(s, d), data.ref(d))) for s in seasons for d in data.dates[s]]


def _params(x: np.ndarray) -> CurrentImpactParams:
    e, s, t, d = np.exp(x)
    return CurrentImpactParams(entry_sd=float(e), season_sd=float(s), team_sd=float(t), drift_sd=float(d))


def fit(out: Path) -> None:
    data = _Data(out, range(START, max(FIT_SEASONS) + 1))
    trace = []

    def obj(x: np.ndarray) -> float:
        t0 = time.time()
        rows = _filter_scores(data, _params(x), FIT_SEASONS)
        mse = sum(r[2] for r in rows) / sum(r[3] for r in rows)
        trace.append({"sd": np.exp(x).round(5).tolist(), "mse": mse})
        print(f"fit {np.exp(x).round(4).tolist()} mse {mse:.6f} {time.time() - t0:.0f}s", flush=True)
        return mse

    x0 = np.log([1.8, 1.0, 0.5, 0.05])  # entry ~ the ridge penalty's prior SD; the rest a guess
    simplex = np.vstack([x0, x0 + 0.5 * np.eye(4)])  # steps of x1.65: the default 5% barely moves log(1.0)
    res = minimize(
        obj, x0, method="Nelder-Mead", options={"initial_simplex": simplex, "xatol": 0.05, "fatol": 2e-7, "maxfev": 80}
    )
    best = _params(res.x)
    (out / "fit.json").write_text(
        json.dumps({"fit_seasons": [min(FIT_SEASONS), max(FIT_SEASONS)], "start": START, "mse": float(res.fun),
                    "params": best.__dict__, "nfev": int(res.nfev), "message": str(res.message), "trace": trace}, indent=1)
    )  # fmt: skip
    print(json.dumps(best.__dict__), flush=True)


def _season_to_date(
    data: _Data, kind: str, seasons: range, lam: float | None = None
) -> list[tuple[int, dt.date, float, int]]:
    rows = []
    for s in seasons:
        pri = data.priors[s].filter(pl.col("kind") == kind)
        for d in data.dates[s]:
            p = pri.filter(pl.col("prior_date") == d)
            prior = {int(a): (b, c) for a, b, c in p.select("player_id", "o_prior", "d_prior").iter_rows()}
            train = data.poss[s].filter(pl.col("game_date") < d)
            r = {}
            if train.height:
                f = nba_reml_rapm(train, prior, penalty="fixed", lambdas=None if lam is None else (lam, lam))
                r = {int(a): (b, c) for a, b, c in f.select("player_id", "o_reml_rapm", "d_reml_rapm").iter_rows()}
            for q, (o, dd) in prior.items():  # a player with a box line but no full-lineup possession
                r.setdefault(q, (o, dd))
            rows.append((s, d, *score(r, data.test(s, d), data.ref(d))))
    return rows


def _prior_only(data: _Data, seasons: range) -> list[tuple[int, dt.date, float, int]]:
    rows = []
    for s in seasons:
        pri = data.priors[s].filter(pl.col("kind") == "window")
        for d in data.dates[s]:
            p = pri.filter(pl.col("prior_date") == d)
            r = {int(a): (b, c) for a, b, c in p.select("player_id", "o_prior", "d_prior").iter_rows()}
            rows.append((s, d, *score(r, data.test(s, d), data.ref(d))))
    return rows


def _tuned_penalty(data: _Data, kind: str) -> dict:
    """The season-to-date rating's penalty chosen on :data:`FIT_SEASONS` 28-day forward MSE (a fair baseline)."""
    mse = {}
    for lam in BASELINE_PENALTY_GRID:
        rows = _season_to_date(data, kind, FIT_SEASONS, lam)
        mse[lam] = sum(r[2] for r in rows) / sum(r[3] for r in rows)
        print(f"baseline {kind} lambda {lam:,.0f} mse {mse[lam]:.6f}", flush=True)
    return {"penalty": min(mse, key=mse.get), "mse_by_penalty": {str(k): v for k, v in mse.items()}}


#: Which filter SDs each filter method runs at, as overrides of the fitted ones.
_FILTER_METHODS = {
    "current": {}, "current_no_drift": {"drift_sd": 0.0}, "current_no_team": {"team_sd": 0.0},
    "current_no_season": {"season_sd": 0.0},
}  # fmt: skip


def fingerprints(out: Path) -> dict[str, str]:
    """What each scored method depends on: its filter SDs (from ``fit.json``) or its tuned penalty.

    ``gate.parquet`` stores it per row; ``gate`` re-scores a method whose stored fingerprint differs, and
    ``report`` refuses rows that do not match, so a refit can never be reported against stale scores.
    """
    best = json.loads((out / "fit.json").read_text())["params"]
    f_b = out / "baseline_fit.json"
    tuned = json.loads(f_b.read_text()) if f_b.exists() else {}
    fp = {m: json.dumps({**best, **o}, sort_keys=True) for m, o in _FILTER_METHODS.items()}
    fp |= {"window_prior": "", "season_to_date": "", "season_to_date_window_prior": ""}
    for m, kind in (("season_to_date_tuned", "season"), ("season_to_date_window_prior_tuned", "window")):
        fp[m] = f"penalty={tuned[kind]['penalty']}" if kind in tuned else "untuned"
    return fp


def gate(out: Path) -> None:
    """Score every method missing from ``gate.parquet`` or stored under another fingerprint."""
    f_g, f_b = out / "gate.parquet", out / "baseline_fit.json"
    done = pl.read_parquet(f_g) if f_g.exists() else None
    best = CurrentImpactParams(**json.loads((out / "fit.json").read_text())["params"])
    data = _Data(out, SEASONS)
    scored = range(START + 1, max(SEASONS) + 1)
    tuned = json.loads(f_b.read_text()) if f_b.exists() else {}

    def tuned_run(kind: str) -> list[tuple[int, dt.date, float, int]]:
        if kind not in tuned:
            tuned[kind] = _tuned_penalty(data, kind)
            f_b.write_text(json.dumps(tuned, indent=1))
        return _season_to_date(data, kind, scored, tuned[kind]["penalty"])

    methods = {m: (lambda o=o: _filter_scores(data, CurrentImpactParams(**{**best.__dict__, **o}), scored))
               for m, o in _FILTER_METHODS.items()}  # fmt: skip
    methods |= {
        "window_prior": lambda: _prior_only(data, scored),
        "season_to_date": lambda: _season_to_date(data, "season", scored),
        "season_to_date_window_prior": lambda: _season_to_date(data, "window", scored),
        "season_to_date_tuned": lambda: tuned_run("season"),
        "season_to_date_window_prior_tuned": lambda: tuned_run("window"),
    }
    keep = []
    for m, run in methods.items():
        fp = fingerprints(out)[m]
        if done is not None and done.filter((pl.col("method") == m) & (pl.col("fingerprint") == fp)).height:
            keep.append(done.filter(pl.col("method") == m))
            continue
        t0 = time.time()
        rows = run()
        fp = fingerprints(out)[m]  # a tuned baseline's penalty is known only after its run
        keep.append(pl.DataFrame([{"season": s, "as_of": d, "method": m, "sse": e, "n": n, "fingerprint": fp}
                                  for s, d, e, n in rows]))  # fmt: skip
        print(f"gate {m} {time.time() - t0:.0f}s", flush=True)
    pl.concat(keep).write_parquet(f_g)


def _per_season(g: pl.DataFrame, base: str, m: str) -> pl.DataFrame:
    """Per season: summed SSE and n of ``base`` and ``m``."""
    w = (
        g.filter(pl.col("method").is_in([base, m]))
        .group_by("season", "method")
        .agg(pl.col("sse").sum(), pl.col("n").sum())
    )
    return w.pivot("method", index="season", values=["sse", "n"]).sort("season")


def margin(g: pl.DataFrame, base: str, m: str, seasons: range, *, boot: int = BOOT, seed: int = 0) -> dict:
    """Pooled ``base`` minus ``m`` MSE (bp), its season-cluster bootstrap SE / 95% interval, per-season t."""
    per = _per_season(g.filter(pl.col("season").is_in(list(seasons))), base, m)
    sb, sm, n = per[f"sse_{base}"].to_numpy(), per[f"sse_{m}"].to_numpy(), per[f"n_{m}"].to_numpy()
    if not np.array_equal(n, per[f"n_{base}"].to_numpy()):
        raise AssertionError(f"{base} and {m} scored different possessions")
    point = 1e4 * (sb.sum() - sm.sum()) / n.sum()
    rng = np.random.default_rng(seed)
    idx = rng.integers(0, n.size, size=(boot, n.size))
    bs = 1e4 * (sb[idx].sum(1) - sm[idx].sum(1)) / n[idx].sum(1)
    d = 1e4 * (sb - sm) / n
    return {
        "margin_bp": float(point),
        "boot_se_bp": float(bs.std(ddof=1)),
        "boot_ci95_bp": [float(np.quantile(bs, 0.025)), float(np.quantile(bs, 0.975))],
        "seasons": int(n.size),
        "seasons_better": int((d > 0).sum()),
        "per_season_mean_bp": float(d.mean()),
        "per_season_se_bp": float(d.std(ddof=1) / np.sqrt(d.size)),
        "per_season_t95_bp": [
            float(x) for x in stats.t.interval(0.95, d.size - 1, loc=d.mean(), scale=d.std(ddof=1) / np.sqrt(d.size))
        ],
        "per_season_bp": {int(s): round(float(x), 3) for s, x in zip(per["season"], d, strict=True)},
    }


#: Buckets of days into the season (from opening night) the gate margin is also reported by.
DAYS_IN = {"0-27": (0, 27), "28-83": (28, 83), "84+": (84, 10_000)}
BASELINES = (
    "season_to_date",
    "season_to_date_window_prior",
    "season_to_date_tuned",
    "season_to_date_window_prior_tuned",
)


def with_days_in(g: pl.DataFrame) -> pl.DataFrame:
    """``gate.parquet`` plus ``days_in``: days from the season's opening night to ``as_of``."""
    starts = g.group_by("season").agg(pl.col("as_of").min().alias("d0"))
    return g.join(starts, on="season").with_columns((pl.col("as_of") - pl.col("d0")).dt.total_days().alias("days_in"))


def gate_result(g: pl.DataFrame) -> dict:
    """The pass rule (fixed before the run) and its pieces, all from ``gate.parquet``."""
    g = with_days_in(g)
    m = margin(g, "season_to_date", "current", GATE_SEASONS)
    return {
        "margin": m,
        "pass": bool(m["margin_bp"] > 0 and m["boot_ci95_bp"][0] > 0),
        "by_days_in": {
            lab: margin(g.filter(pl.col("days_in").is_between(a, b)), "season_to_date", "current", GATE_SEASONS)
            for lab, (a, b) in DAYS_IN.items()
        },
        "current_vs_baselines": {b: margin(g, b, "current", GATE_SEASONS) for b in BASELINES if b in set(g["method"])},
        "ablations_vs_current": {
            a: margin(g, a, "current", GATE_SEASONS)
            for a in ("current_no_drift", "current_no_team", "current_no_season", "window_prior")
        },
    }


def report(out: Path) -> None:
    g = pl.read_parquet(out / "gate.parquet")
    fp = fingerprints(out)
    stale = g.filter(pl.col("fingerprint") != pl.col("method").replace_strict(fp, default=None))["method"].unique()
    if stale.len():
        raise SystemExit(f"gate.parquet rows of {stale.to_list()} were scored under other parameters: re-run gate")
    res: dict = {"fit": {k: v for k, v in json.loads((out / "fit.json").read_text()).items() if k != "trace"}}
    if (out / "baseline_fit.json").exists():
        res["baseline_fit"] = json.loads((out / "baseline_fit.json").read_text())
    for lab, seasons in (("gate", GATE_SEASONS), ("fit", FIT_SEASONS)):
        sub = g.filter(pl.col("season").is_in(list(seasons)))
        res[f"{lab}_pooled_mse"] = {
            m: float(f["sse"].sum() / f["n"].sum()) for (m,), f in sub.group_by("method", maintain_order=True)
        }
        res[f"{lab}_vs_season_to_date"] = {
            m: margin(g, "season_to_date", m, seasons) for m in g["method"].unique().sort() if m != "season_to_date"
        }
    r = gate_result(g)
    res["gate_pass"] = r["pass"]
    res["gate_by_days_in"] = r["by_days_in"]
    res["current_vs_baselines"] = r["current_vs_baselines"]
    res["ablations_vs_current"] = r["ablations_vs_current"]
    (out / "report.json").write_text(json.dumps(res, indent=1))
    print(json.dumps({k: res[k] for k in ("fit", "gate_pooled_mse", "gate_pass")}, indent=1))
    for k in ("current_vs_baselines", "ablations_vs_current", "gate_by_days_in"):
        for mm, v in res[k].items():
            print(
                f"{k:24s} {mm:34s} {v['margin_bp']:+8.3f} bp (boot SE {v['boot_se_bp']:.3f}, CI {v['boot_ci95_bp'][0]:+.3f}..{v['boot_ci95_bp'][1]:+.3f}; t95 {v['per_season_t95_bp'][0]:+.3f}..{v['per_season_t95_bp'][1]:+.3f}) {v['seasons_better']}/{v['seasons']}"
            )


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="fit_current_impact")
    ap.add_argument("stage", choices=["inputs", "fit", "gate", "report"])
    ap.add_argument("out", type=Path)
    ap.add_argument("--d38", type=Path, default=None)
    ap.add_argument("--poss-dir", default=None)
    ap.add_argument(
        "--raw", default="https://raw.githubusercontent.com/sportsdataverse/hoopR-nba-stats-raw/main/nba_stats/json"
    )
    a = ap.parse_args(argv)
    if a.stage == "inputs":
        inputs(a.out, a.d38, a.poss_dir, a.raw)
    elif a.stage == "fit":
        fit(a.out)
    elif a.stage == "gate":
        gate(a.out)
    else:
        report(a.out)
    return 0


if __name__ == "__main__":
    sys.exit(main())
