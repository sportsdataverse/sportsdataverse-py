"""Fit the college pregame constants (``LEAGUE_CONSTANTS["mens" | "womens"]``) on the 2024 as-of-date backtest.

Walks the fit season date by date, builds as-of ratings from games strictly before each date, predicts
every game and fits, on the games where both teams have at least ``MIN_PRIOR_GAMES`` earlier games:

* ``em_scale`` and ``hfa``: joint least squares of the actual margin on the raw AdjEM difference and a
  non-neutral indicator.
* ``margin_sd``: the residual standard deviation of that fit.
* ``avg_tempo``: one multiplicative step that makes the mean expected total equal the mean actual total
  (an expected total is proportional to ``1 / avg_tempo``).
* ``bubble_adj_em``: the mean AdjEM of engine ranks 40-50 on the full fit season (the WAB baseline).

The ratings engine reads ``hfa`` and ``avg_tempo``, so the fit is iterated to a fixed point: each pass
writes its ``hfa`` / ``avg_tempo`` into the constants and walks again, until neither moves by more than
``TOL``. Reads only the committed ``tests/fixtures/{m,w}bb_prediction`` fixtures (offline).

``--score SEASON ...`` instead writes the weekly as-of predictions of those seasons (games before the
Monday of the game's week, both teams with ``MIN_PRIOR_GAMES`` earlier games, as the 2024 backtest tests
build them) to ``--out``, from the released schedule and team box. ``--compare OLD NEW`` scores two such
files against each other, paired, with SEs clustered by (season, week).

``--ft-weight`` overrides the engine's free-throw possession weight (0.44 today). The 2026-11-26 refit at
the college 0.475 (``--ft-weight 0.475``) gave hfa 2.9291 / 2.6427, em_scale 0.5816 / 0.6278, margin_sd
11.2203 / 12.1722, avg_tempo 70.3014 / 72.4083, bubble_adj_em 20.191 / 27.265 (men / women); it passed its
2025-2026 pregame gate, and the engine stays at 0.44 by the owner's decision
(``mbb_team_ratings.raw_game_efficiency``). Its paired score was ``--score 2025 2026`` once as shipped and
once with ``--ft-weight 0.475 --constants '{...}'`` (the refit values), then ``--compare``.
``--keep-zero-turnover-games`` turns off the engine's later zero-turnover game drop, which the men's shipped
constants predate: with it they reproduce to 4 dp.

Usage::

    uv run python tools/models/mbb_prediction/fit_pregame.py --league mens
    uv run python tools/models/mbb_prediction/fit_pregame.py --league womens --score 2025 2026 --out wbb.parquet
    uv run python tools/models/mbb_prediction/fit_pregame.py --league mens --ft-weight 0.475
    uv run python tools/models/mbb_prediction/fit_pregame.py --league mens --keep-zero-turnover-games
    uv run python tools/models/mbb_prediction/fit_pregame.py --compare old.parquet new.parquet
"""

from __future__ import annotations

import argparse
import dataclasses
import importlib
import json
import sys
from pathlib import Path

import numpy as np
import polars as pl

from sportsdataverse._common.ratings import id_text
from sportsdataverse.mbb.mbb_game_predict import mbb_predict_games
from sportsdataverse.mbb.mbb_prediction_constants import as_of_ratings_split, get_constants

# the modules, not the package's star-exported functions of the same name
pc = importlib.import_module("sportsdataverse.mbb.mbb_prediction_constants")
tr = importlib.import_module("sportsdataverse.mbb.mbb_team_ratings")
REPO = Path(__file__).resolve().parents[3]
#: A game enters the fit only when both teams have this many earlier games at the as-of date.
MIN_PRIOR_GAMES = 8
#: Fixed-point tolerance on ``hfa`` and ``avg_tempo``.
TOL = 1e-4
BUBBLE_RANKS = (40, 50)


def _fixtures(league: str) -> tuple[pl.DataFrame, pl.DataFrame]:
    d = REPO / "tests" / "fixtures" / ("wbb_prediction" if league == "womens" else "mbb_prediction")
    return pl.read_parquet(d / "results_2024.parquet"), pl.read_parquet(d / "team_box_2024.parquet")


def _ratings(results: pl.DataFrame, box: pl.DataFrame, league: str) -> tuple[pl.DataFrame, pl.DataFrame]:
    eff = tr.raw_game_efficiency(results, box)
    counts = eff.group_by("team_id").agg(pl.len().alias("n_games"))
    ratings = (
        tr.adjust_efficiency(eff, league=league)
        .join(tr.adjust_tempo(eff, league=league), on=["season", "team_id"])
        .select("team_id", "adj_o", "adj_d", "adj_em", "adj_tempo")
    )
    return ratings, counts


def _predict(day: pl.DataFrame, ratings: pl.DataFrame, counts: pl.DataFrame, league: str) -> pl.DataFrame:
    """The day's games: the shipped prediction, the raw AdjEM difference and the outcome."""
    neutral = mbb_predict_games(
        day.select("game_id", "home_team_id", "away_team_id").with_columns(pl.lit(True).alias("neutral_site")),
        ratings,
        league=league,
    ).select("game_id", (pl.col("exp_margin") / get_constants(league).em_scale).alias("em_diff"), "exp_total")
    real = mbb_predict_games(
        day.select("game_id", "home_team_id", "away_team_id", "neutral_site"), ratings, league=league
    ).select("game_id", "exp_margin", "home_win_prob")
    return (
        day.select(
            "game_id",
            "season",
            "date",
            "home_team_id",
            "away_team_id",
            "neutral_site",
            (pl.col("home_score") - pl.col("away_score")).cast(pl.Float64).alias("actual_margin"),
            (pl.col("home_score") + pl.col("away_score")).cast(pl.Float64).alias("actual_total"),
        )
        .join(neutral, on="game_id")
        .join(real, on="game_id")
        .join(counts.rename({"team_id": "home_team_id", "n_games": "home_n"}), on="home_team_id", how="left")
        .join(counts.rename({"team_id": "away_team_id", "n_games": "away_n"}), on="away_team_id", how="left")
    )


def _eligible(frame: pl.DataFrame) -> pl.DataFrame:
    return frame.filter(
        (pl.col("home_n") >= MIN_PRIOR_GAMES) & (pl.col("away_n") >= MIN_PRIOR_GAMES) & pl.col("em_diff").is_finite()
    )


def walk_daily(results: pl.DataFrame, box: pl.DataFrame, league: str) -> pl.DataFrame:
    """One row per eligible game of the fit season, rated from games strictly before its date."""
    frames = []
    for d in results.get_column("date").unique().sort().to_list():
        if results.filter(pl.col("date") < d).height < 300:  # the engine has nothing useful in the opening days
            continue
        ratings, counts = _ratings(as_of_ratings_split(results, d), box.filter(pl.col("game_date") < d), league)
        frames.append(_predict(results.filter(pl.col("date") == d), ratings, counts, league))
    return _eligible(pl.concat(frames))


def walk_weekly(results: pl.DataFrame, box: pl.DataFrame, league: str) -> pl.DataFrame:
    """One row per eligible game, rated from games before the Monday of its week (the backtest tests' walk)."""
    results = results.with_columns(pl.col("date").dt.truncate("1w").alias("cutoff"))
    frames = []
    for (cutoff,), week in results.group_by("cutoff", maintain_order=True):
        if results.filter(pl.col("date") < cutoff).height < 300:
            continue
        ratings, counts = _ratings(
            results.filter(pl.col("date") < cutoff), box.filter(pl.col("game_date") < cutoff), league
        )
        frames.append(_predict(week, ratings, counts, league).with_columns(pl.lit(cutoff).alias("cutoff")))
    return _eligible(pl.concat(frames))


def fit_once(results: pl.DataFrame, box: pl.DataFrame, league: str) -> dict[str, float]:
    """One pass at the current constants."""
    e = walk_daily(results, box, league)
    em = e["em_diff"].to_numpy()
    non_neutral = (e["neutral_site"] == False).cast(pl.Float64).to_numpy()  # noqa: E712
    actual = e["actual_margin"].to_numpy()
    (em_scale, hfa), *_ = np.linalg.lstsq(np.column_stack([em, non_neutral]), actual, rcond=None)
    resid = actual - (em_scale * em + hfa * non_neutral)
    t_cur = get_constants(league).avg_tempo
    full, _ = _ratings(results, box, league)
    lo, hi = BUBBLE_RANKS
    bubble = full.sort("adj_em", descending=True).slice(lo - 1, hi - lo + 1)["adj_em"].mean()
    return {
        "games": e.height,
        "hfa": float(hfa),
        "em_scale": float(em_scale),
        "margin_sd": float(np.std(resid)),
        "avg_tempo": t_cur * float(e["exp_total"].mean()) / float(e["actual_total"].mean()),
        "bubble_adj_em": float(bubble),
    }


def _set(league: str, **values: float) -> None:
    pc.LEAGUE_CONSTANTS[league] = dataclasses.replace(pc.LEAGUE_CONSTANTS[league], **values)


def fit(league: str, max_iter: int = 20) -> dict[str, float]:
    """The fixed point in ``hfa`` / ``avg_tempo``, then every constant at it."""
    results, box = _fixtures(league)
    for i in range(max_iter):
        c = get_constants(league)
        f = fit_once(results, box, league)
        print(f"pass {i}: " + json.dumps({k: round(v, 4) for k, v in f.items()}), flush=True)
        if abs(f["hfa"] - c.hfa) < TOL and abs(f["avg_tempo"] - c.avg_tempo) < TOL:
            return f
        _set(league, hfa=f["hfa"], avg_tempo=f["avg_tempo"], em_scale=f["em_scale"])
    raise RuntimeError(f"no fixed point in {max_iter} passes")


def _season_frames(seasons: list[int], league: str) -> tuple[pl.DataFrame, pl.DataFrame]:
    """The released completed games and team box of ``seasons``, in the fixtures' shape."""
    load_sched, load_box = tr._league_loaders(league)
    sched = tr._normalize_schedule(load_sched(seasons))
    results = (
        sched.filter((pl.col("status_type_completed") == True) & pl.col("home_score").is_not_null())  # noqa: E712
        .select(
            id_text(sched, "game_id"),
            pl.col("season").cast(pl.Int64),
            pl.col("date").cast(pl.Date),
            id_text(sched, "home_team_id"),
            id_text(sched, "away_team_id"),
            pl.col("home_score").cast(pl.Int64),
            pl.col("away_score").cast(pl.Int64),
            pl.col("neutral_site").cast(pl.Boolean),
        )
        .drop_nulls(["home_score", "away_score"])
        .unique(subset=["game_id"])
        .sort("date", "game_id")
    )
    box = load_box(seasons).with_columns(pl.col("game_date").cast(pl.Date))
    return results, box


def score(seasons: list[int], league: str) -> pl.DataFrame:
    """The weekly as-of predictions of ``seasons``, season by season."""
    out = []
    for s in seasons:
        results, box = _season_frames([s], league)
        out.append(walk_weekly(results, box, league))
    return pl.concat(out)


def _cr1_se(d: np.ndarray, g: np.ndarray) -> float:
    """CR1 SE of the mean of ``d`` clustered by ``g``."""
    _, inv = np.unique(g, return_inverse=True)
    sums = np.bincount(inv, weights=d)
    n_g = len(sums)
    return float(np.sqrt(n_g / (n_g - 1) * np.sum((sums - d.mean() * np.bincount(inv)) ** 2)) / len(d))


def _slope(p: np.ndarray, y: np.ndarray) -> float:
    return float(np.polyfit(p, y, 1)[0])


def compare(old: pl.DataFrame, new: pl.DataFrame, reps: int = 2000, seed: int = 20261126) -> dict[str, dict]:
    """Paired old-vs-new gate metrics: Brier, margin MAE, calibration slope, expected-total bias."""
    assert old.schema["game_id"] == new.schema["game_id"], "game_id dtype differs"
    j = old.join(new, on="game_id", suffix="_new").filter(pl.col("actual_margin") != 0)
    y = (j["actual_margin"] > 0).cast(pl.Float64).to_numpy()
    week = (j["season"].cast(pl.Utf8) + "_" + j["cutoff"].cast(pl.Utf8)).to_numpy()
    out: dict[str, dict] = {"n_games": {"value": j.height}, "n_clusters": {"value": int(len(np.unique(week)))}}
    for name, loss in {
        "brier": lambda p, m: (p - y) ** 2,
        "margin_mae": lambda p, m: np.abs(m - j["actual_margin"].to_numpy()),
    }.items():
        lo = loss(j["home_win_prob"].to_numpy(), j["exp_margin"].to_numpy())
        ln = loss(j["home_win_prob_new"].to_numpy(), j["exp_margin_new"].to_numpy())
        d = ln - lo
        se = _cr1_se(d, week)
        out[name] = {"old": lo.mean(), "new": ln.mean(), "diff": d.mean(), "se": se, "pass": d.mean() <= 2 * se}
    po, pn = j["home_win_prob"].to_numpy(), j["home_win_prob_new"].to_numpy()
    so, sn = _slope(po, y), _slope(pn, y)
    rng = np.random.default_rng(seed)
    clusters = np.unique(week)
    idx = {c: np.flatnonzero(week == c) for c in clusters}
    boot = []
    for _ in range(reps):
        rows = np.concatenate([idx[c] for c in rng.choice(clusters, len(clusters))])
        boot.append(abs(_slope(pn[rows], y[rows]) - 1) - abs(_slope(po[rows], y[rows]) - 1))
    d = abs(sn - 1) - abs(so - 1)
    se = float(np.std(boot, ddof=1))
    out["calibration_slope"] = {"old": so, "new": sn, "diff_dist_from_1": d, "se": se, "pass": d <= 2 * se}
    bias = abs(float(j["exp_total_new"].mean()) - float(j["actual_total"].mean()))
    out["total_bias"] = {
        "old": abs(float(j["exp_total"].mean()) - float(j["actual_total"].mean())),
        "new": bias,
        "pass": bias <= 2.0,
    }
    return out


def main(argv: list[str] | None = None) -> int:
    """CLI: fit (default), ``--score`` a season set, or ``--compare`` two scored files."""
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--league", choices=("mens", "womens"), default="mens")
    ap.add_argument("--ft-weight", type=float, default=None, help="override the engine's FT possession weight")
    ap.add_argument("--score", type=int, nargs="+", default=None, help="write these seasons' weekly predictions")
    ap.add_argument("--out", type=Path, default=None)
    ap.add_argument("--constants", type=json.loads, default=None, help="score with these constants, a JSON object")
    ap.add_argument(
        "--keep-zero-turnover-games", action="store_true", help="the engine before its zero-turnover game drop"
    )
    ap.add_argument("--compare", type=Path, nargs=2, default=None, metavar=("OLD", "NEW"))
    args = ap.parse_args(argv)
    if args.compare:
        res = compare(*(pl.read_parquet(p) for p in args.compare))
        print(json.dumps(res, indent=2, default=lambda v: v.item() if hasattr(v, "item") else str(v)))
        return 0
    if args.keep_zero_turnover_games:
        tr._drop_zero_turnover_games = lambda paired: paired
    if args.ft_weight is not None:
        results, box = _fixtures(args.league)
        before = tr.raw_game_efficiency(results, box)["poss"].mean()
        tr.PRO_FT_POSSESSION_WEIGHT = args.ft_weight  # the engine reads this module global at call time
        if args.ft_weight != 0.44 and tr.raw_game_efficiency(results, box)["poss"].mean() == before:
            raise RuntimeError("--ft-weight did not change the engine's possessions")
    if args.constants:
        _set(args.league, **args.constants)
    if args.score:
        frame = score(args.score, args.league)
        frame.write_parquet(args.out)
        print(f"wrote {frame.height} games to {args.out}")
        return 0
    f = fit(args.league)
    print(json.dumps({k: round(v, 4) for k, v in f.items()}, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
