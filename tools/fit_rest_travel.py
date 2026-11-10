# tools/fit_rest_travel.py
"""Fit the rest / back-to-back / travel effect on the pro game margin, walk-forward by season (BB-D23).

The fitted values are frozen into
``sportsdataverse.nba.nba_rest_travel.REST_TRAVEL_CONSTANTS`` with a comment citing this script.

**The base projection** is the season sim's own game model, as shipped: the as-of possession
ridge at :data:`~sportsdataverse.nba.nba_season_sim.SIM_CONSTANTS`, shrunk toward the preseason
prior at :data:`~sportsdataverse.nba.nba_season_sim.PRIOR_CONSTANTS`, rated at weekly cutoffs
from the first game (``tools/fit_preseason_prior.py``'s ``pro_season``): the ratings use the
games strictly before the cutoff and project every later regular-season game. Win probability
is ``Phi(margin / sd)`` with the shipped sd.

**The effect.** OLS, with no intercept, of the residual ``actual margin - base margin`` on the
home-minus-away rest and travel differences (:data:`~sportsdataverse.nba.nba_rest_travel.FEATURES`),
one row per game: its projection at the latest cutoff before it. No intercept, so the effect is
zero for two teams in the same state and the home term stays the game model's. The first run had
one (``--intercept`` reproduces it): it came out -0.34 points, alone it scored no better than the
base, and dropping it was decided after seeing that run's holdout (both pass the gate on 3 of 3
NBA holdout seasons). The features are read off the schedule (dates and sites of the teams'
earlier games and of this one), so they are known before the tip.

**Look-ahead in scoring.** The schedule is the final one: rescheduled dates, the December games
added after Cup group play and who played in the Cup knockout are as played, not as known at an
early cutoff. That is right for the fit (it measures real rest); for scoring it is a small
optimistic look-ahead, which as-of schedules (not kept) would remove. The base also gives the home
term to the few neutral-site games (``pro_season`` predates the neutral list), about three a season.

**Walk-forward.** Each HOLDOUT season is scored with coefficients fit on the TRAIN seasons and the
holdout seasons before it only, on every (cutoff, game) pair, against the base projection: log
loss, Brier and margin MAE, with paired differences (SE clustered by game), also pooled over the
holdout seasons, plus calibration (the slope of the outcome on the logit, and observed vs predicted
by back-to-back difference). The gate, set in the build brief before any fit: all three improve
on at least two holdout seasons. The shipped coefficients are the fit on every season (train and
holdout), the next walk-forward step, so they are never scored out of sample themselves.

Inputs: the ``{nba,wnba}_stats_possessions`` / ``_schedules`` releases (network), cached as
parquet under ``--cache`` (``pro_season/`` is the fit_pro_season_ratings cache).

Usage::

    uv run python tools/fit_rest_travel.py --league nba --train 2018 2019 2022 2023 --holdout 2024 2025 2026
    uv run python tools/fit_rest_travel.py --league wnba --train 2018 2019 2021 --holdout 2024 2025
"""

from __future__ import annotations

import argparse
import importlib
import json
import sys
from pathlib import Path

import numpy as np
import polars as pl
from scipy.stats import norm

sys.path.insert(0, str(Path(__file__).resolve().parent))
from fit_preseason_prior import pro_season  # noqa: E402
from fit_pro_season_ratings import load_season  # noqa: E402

from sportsdataverse._common.metrics import brier_score, log_loss_score  # noqa: E402
from sportsdataverse.nba.nba_rest_travel import FEATURES, game_rest_travel, rest_travel_margin  # noqa: E402
from sportsdataverse.nba.nba_team_season_rates import _league_loaders  # noqa: E402

nba_season_sim = importlib.import_module("sportsdataverse.nba.nba_season_sim")

_PRO = {"nba": "00", "wnba": "10"}
_D = [f"d_{f}" for f in FEATURES]


def schedule_games(league_id: str, season: int, cache: Path) -> pl.DataFrame:
    """The season's regular-season and Cup games with both teams known (the rest sequence), cached."""
    path = cache / f"sched_{league_id}_{season}.parquet"
    if not path.exists():
        _, load_schedule, offset = _league_loaders(league_id)
        cache.mkdir(parents=True, exist_ok=True)
        load_schedule([season - offset]).write_parquet(path)
    return (
        pl.read_parquet(path)
        .filter(pl.col("season_type").is_in(["regular-season", "nba-cup"]))
        .select(
            pl.lit(season, dtype=pl.Int64).alias("season"),
            pl.col("game_id").cast(pl.Utf8),
            pl.col("game_date").cast(pl.Utf8).str.slice(0, 10).str.to_date(),
            pl.col("home_team_id").cast(pl.Int64),
            pl.col("away_team_id").cast(pl.Int64),
        )
        .filter((pl.col("home_team_id") > 0) & (pl.col("away_team_id") > 0))
    )


def season_frame(league_id: str, season: int, cache: Path) -> pl.DataFrame:
    """One row per (cutoff, game): base margin, actual margin, outcome, and the rest differences."""
    lam, _ = nba_season_sim.SIM_CONSTANTS[league_id]
    reg, _ = nba_season_sim.PRIOR_CONSTANTS[league_id]
    pro = cache / "pro_season"
    base = pro_season(league_id, season, pro, {"base": (reg, lam)})
    _, games = load_season(league_id, season, pro)
    diffs = game_rest_travel(schedule_games(league_id, season, cache), league_id).select("game_id", *_D)
    for frame in (games, diffs):
        assert frame.schema["game_id"] == base.schema["game_id"], "game_id dtype differs across the join"
    out = base.join(games.select("game_id", "margin"), on="game_id", how="left", validate="m:1").join(
        diffs, on="game_id", how="left", validate="m:1"
    )
    bad = out.filter(pl.any_horizontal(pl.col(c).is_null() for c in ["margin", *_D]))
    if bad.height:
        raise ValueError(f"season {season}: {bad.height} scored rows lack a margin or a rest feature")
    return out


def latest(frame: pl.DataFrame) -> pl.DataFrame:
    """Each game once: its projection at the latest cutoff before it."""
    return frame.sort("week").group_by("season", "game_id").last()


def fit(frame: pl.DataFrame, intercept: bool = False) -> dict[str, float]:
    """OLS of the residual on the differences, one row per game; coefficients and SEs."""
    one = latest(frame)
    x = np.column_stack([*([np.ones(one.height)] if intercept else []), *(one[c].to_numpy() for c in _D)])
    r = (one["margin"] - one["base"]).to_numpy()
    beta, *_ = np.linalg.lstsq(x, r, rcond=None)
    resid = r - x @ beta
    cov = np.linalg.inv(x.T @ x) * (resid @ resid) / (x.shape[0] - x.shape[1])
    names = [*(["intercept"] if intercept else []), *FEATURES]
    out = {n: round(float(b), 4) for n, b in zip(names, beta, strict=True)}
    out.update({f"{n}_se": round(float(s), 4) for n, s in zip(names, np.sqrt(np.diag(cov)), strict=True)})
    out["games"] = one.height
    return out


def _ll(y: np.ndarray, p: np.ndarray) -> np.ndarray:
    p = np.clip(p, 1e-15, 1 - 1e-15)
    return -(y * np.log(p) + (1 - y) * np.log(1 - p))


def _paired(game_id: pl.Series, d: np.ndarray) -> dict[str, float]:
    mean = float(d.mean())
    sums = pl.DataFrame({"g": game_id, "d": d}).group_by("g").agg(pl.col("d").sum(), pl.len().alias("n"))
    se = float(np.sqrt(((sums["d"].to_numpy() - sums["n"].to_numpy() * mean) ** 2).sum()) / d.size)
    return {"diff": round(mean, 5), "se": round(se, 5)}


def _calibration(y: np.ndarray, p: np.ndarray) -> float:
    """Slope of the logistic regression of ``y`` on ``logit(p)`` (1 is calibrated), by Newton's method."""
    z = np.log(np.clip(p, 1e-12, 1 - 1e-12) / np.clip(1 - p, 1e-12, 1))
    x = np.column_stack([np.ones_like(z), z])
    b = np.array([0.0, 1.0])
    for _ in range(25):
        q = 1 / (1 + np.exp(-x @ b))
        b = b + np.linalg.solve(x.T @ (x * (q * (1 - q))[:, None]), x.T @ (y - q))
    return round(float(b[1]), 4)


def _margin(sub: pl.DataFrame, coefs: dict[str, float]) -> np.ndarray:
    """Base margin plus the rest and travel points (and the intercept of an ``--intercept`` fit)."""
    return sub["base"].to_numpy() + rest_travel_margin(sub, coefs=coefs) + coefs.get("intercept", 0.0)


def score(frame: pl.DataFrame, coefs: dict[str, float], sd: float) -> dict[str, object]:
    """Base vs base + rest/travel on every (cutoff, game) pair, and on each game's latest cutoff."""

    def block(sub: pl.DataFrame) -> dict[str, object]:
        y, m = sub["y"].to_numpy(), sub["margin"].to_numpy()
        mb = sub["base"].to_numpy()
        ma = _margin(sub, coefs)
        pb, pa = norm.cdf(mb / sd), norm.cdf(ma / sd)
        res: dict[str, object] = {"n": sub.height, "games": sub["game_id"].n_unique()}
        for name, (b, a) in {
            "log_loss": (log_loss_score(y, pb), log_loss_score(y, pa)),
            "brier": (brier_score(y, pb), brier_score(y, pa)),
            "margin_mae": (float(np.abs(m - mb).mean()), float(np.abs(m - ma).mean())),
        }.items():
            res[name] = {"base": round(b, 5), "rest_travel": round(a, 5), "improved": bool(a < b)}
        res["paired_log_loss"] = _paired(sub["game_id"], _ll(y, pa) - _ll(y, pb))
        res["paired_brier"] = _paired(sub["game_id"], (y - pa) ** 2 - (y - pb) ** 2)
        res["paired_abs_error"] = _paired(sub["game_id"], np.abs(m - ma) - np.abs(m - mb))
        res["calibration_slope"] = {"base": _calibration(y, pb), "rest_travel": _calibration(y, pa)}
        d = sub["d_b2b"].to_numpy()
        res["by_d_b2b"] = {
            f"{k:+d}": {
                "n": int((d == k).sum()),
                "y": round(float(y[d == k].mean()), 4),
                "p_base": round(float(pb[d == k].mean()), 4),
                "p_rest_travel": round(float(pa[d == k].mean()), 4),
            }
            for k in (-1, 0, 1)
            if (d == k).any()
        }
        return res

    return {"all_cutoffs": block(frame), "latest_cutoff": block(latest(frame))}


def run(lg: str, train: list[int], holdout: list[int], cache: Path, intercept: bool = False) -> dict[str, object]:
    league_id = _PRO[lg]
    _, sd = nba_season_sim.SIM_CONSTANTS[league_id]
    frames = {s: season_frame(league_id, s, cache) for s in [*train, *holdout]}
    walk: dict[str, object] = {}
    passed = 0
    scored = []
    for i, s in enumerate(holdout):
        seen = [*train, *holdout[:i]]
        coefs = fit(pl.concat([frames[t] for t in seen]), intercept)
        sc = score(frames[s], coefs, sd)
        scored.append(frames[s].with_columns(pl.Series("adjusted", _margin(frames[s], coefs))))
        allc = sc["all_cutoffs"]
        ok = all(allc[k]["improved"] for k in ("log_loss", "brier", "margin_mae"))  # type: ignore[index]
        passed += ok
        walk[str(s)] = {"fit_on": seen, "coefs": coefs, "all_three_improved": ok, **sc}
    final = fit(pl.concat(list(frames.values())), intercept)
    pooled = pl.concat(scored)
    y, m = pooled["y"].to_numpy(), pooled["margin"].to_numpy()
    mb, ma = pooled["base"].to_numpy(), pooled["adjusted"].to_numpy()
    pb, pa = norm.cdf(mb / sd), norm.cdf(ma / sd)
    gid = pooled["season"].cast(pl.Utf8) + pooled["game_id"]
    return {
        "intercept": intercept,
        "holdout_pooled_all_cutoffs": {
            "paired_log_loss": _paired(gid, _ll(y, pa) - _ll(y, pb)),
            "paired_brier": _paired(gid, (y - pa) ** 2 - (y - pb) ** 2),
            "paired_abs_error": _paired(gid, np.abs(m - ma) - np.abs(m - mb)),
        },
        "league": lg,
        "train": train,
        "holdout": holdout,
        "sd": sd,
        "walk_forward": walk,
        "gate": {"seasons_improved": passed, "needed": 2, "passed": passed >= 2},
        "shipped_fit_all_seasons": final,
    }


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[1])
    ap.add_argument("--league", choices=sorted(_PRO), required=True)
    ap.add_argument("--train", type=int, nargs="+", required=True, help="seasons by END year")
    ap.add_argument("--holdout", type=int, nargs="+", required=True, help="seasons by END year, in order")
    ap.add_argument("--cache", type=Path, default=Path(".cache"))
    ap.add_argument("--out", type=Path, default=None, help="write the result json here")
    ap.add_argument("--intercept", action="store_true", help="fit an intercept too (the first, unshipped run)")
    args = ap.parse_args(argv)
    if set(args.train) & set(args.holdout):
        ap.error("train and holdout seasons must be disjoint")
    if args.holdout != sorted(args.holdout) or min(args.holdout) <= max(args.train):
        ap.error("holdout seasons must be in order and after every train season (walk-forward)")
    result = run(args.league, args.train, args.holdout, args.cache, args.intercept)
    text = json.dumps(result, indent=2, default=str)
    print(text)
    if args.out:
        args.out.write_text(text + "\n", encoding="utf-8", newline="\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
