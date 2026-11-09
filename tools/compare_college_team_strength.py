# tools/compare_college_team_strength.py
"""Compare the two college team-strength methods, in-season and as of each week, on held-out seasons.

The two methods ``mbb_team_strength`` / ``wbb_team_strength`` publish side by side:

* **fixed point** (``adj_net_rtg``): the published ``mbb_ratings`` / ``wbb_ratings`` engine,
  :func:`sportsdataverse.mbb.mbb_team_ratings.adjust_efficiency` on ESPN box-score games;
* **possession ridge** (``adj_net_rtg_ridge``): the NBA / WNBA ridge,
  :func:`sportsdataverse._common.team_strength.possession_ridge_ratings` on the stats.ncaa.org
  possessions, at the shipped penalty ``COLLEGE_RIDGE_LAMBDA`` (fit on 2016-2023 by
  ``tools/fit_team_strength_ridge.py``, disjoint from the default holdout).

Neither uses a preseason prior here: the team-strength table publishes current-season ratings.

**As-of protocol.** Cutoffs every 7 days from the season's first game. Both methods are fit on the
games strictly before the cutoff and score the Division I vs Division I regular-season games
(conference tournaments included) in the 7 days from it, so each game is scored once, by ratings
that never saw it. Week 0 (nothing played) is skipped. A game is scored only when both methods rate
both teams.

**Game model.** Each method's predicted home margin is ``a * d_net + b * home`` (``home`` 1 off a
neutral site), and its win probability ``Phi(margin / sigma)``. ``a``, ``b`` and ``sigma`` are least
squares on the TRAIN seasons, per method and per phase (weeks 1-4, 5-8, 9+), so neither method is
scored on the other's scale. The HOLDOUT seasons are then scored once: log loss, Brier and margin MAE,
with the per-game difference (ridge minus fixed point) and its standard error.

**Agreement and stability.** Spearman between the two methods' ratings of the Division I teams, at
the season's end and at every cutoff; for each method, Spearman between its rating at a cutoff and
its own season-end rating (how early it settles), and the mean week-over-week rank change.

Inputs (network, cached as parquet under ``--cache``): the ESPN college schedules and team boxes
(through ``tools/fit_preseason_prior.load_college``) and the ``ncaa_{mbb,wbb}_possessions`` releases.

Usage::

    uv run python tools/compare_college_team_strength.py --league mbb \
        --train 2016 2017 2018 2019 2020 2022 2023 --holdout 2024 2025 2026
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import sys
from pathlib import Path

import numpy as np
import polars as pl
from scipy.stats import norm

sys.path.insert(0, str(Path(__file__).resolve().parent))
from fit_preseason_prior import cutoffs, load_college  # noqa: E402

from sportsdataverse._common.metrics import brier_score, log_loss_score  # noqa: E402
from sportsdataverse._common.team_strength import (  # noqa: E402
    COLLEGE_RIDGE_FIT_SEASONS,
    COLLEGE_RIDGE_LAMBDA,
    possession_ridge_ratings,
)
from sportsdataverse.mbb.mbb_team_ratings import adjust_efficiency  # noqa: E402
from sportsdataverse.mbb.mbb_team_season_rates import _league_loaders, ncaa_possession_rate_rows  # noqa: E402

_COLLEGE = {"mbb": "mens", "wbb": "womens"}
METHODS = ("fixed", "ridge")
PHASES = ((1, 4), (5, 8), (9, 99))
#: The fixed point's in-engine constants (hfa, margin_sd, em_scale, avg_tempo) were fit on this
#: season's as-of backtest (``mbb_prediction_constants``), so it is not clean for that method;
#: ``holdout_untouched`` leaves it out.
FIXED_POINT_CONSTANTS_SEASON = 2024


def load_rows(lg: str, season: int, cache: Path) -> pl.DataFrame:
    """The season's possession rows with their game date (ESPN's, as the fixed point uses), cached.

    A game ESPN does not carry keeps the stats.ncaa.org ``game_date``.
    """
    path = cache / f"{lg}_ridge_rows_espn_date_{season}.parquet"
    if path.exists():
        return pl.read_parquet(path)
    load_possessions, _, load_schedule = _league_loaders(_COLLEGE[lg])
    poss, (espn, _, _) = load_possessions(season), load_college(lg, season, cache)
    rows = ncaa_possession_rate_rows(poss, load_schedule(season), league=_COLLEGE[lg])
    espn_date = espn.select(pl.col("game_id").cast(pl.Int64).alias("espn_game_id"), pl.col("date").alias("_espn"))
    dates = (
        poss.select(
            pl.col("contest_id").cast(pl.Utf8).alias("game_id"),
            pl.col("espn_game_id").cast(pl.Int64),
            pl.col("game_date").str.to_date("%m/%d/%Y").alias("_ncaa"),
        )
        .unique("game_id")
        .join(espn_date.unique("espn_game_id"), on="espn_game_id", how="left", validate="m:1")
        .select("game_id", pl.coalesce("_espn", "_ncaa").alias("date"))
    )
    rows = rows.join(dates, on="game_id", how="left", validate="m:1")
    cache.mkdir(parents=True, exist_ok=True)
    rows.write_parquet(path)
    return rows


def _ratings(lg: str, eff: pl.DataFrame, rows: pl.DataFrame, lam: float) -> dict[str, dict[str, float]]:
    """team_id (text) -> net rating, per method."""
    fixed = adjust_efficiency(eff, league=_COLLEGE[lg]) if eff.height else pl.DataFrame({"team_id": [], "adj_em": []})
    ridge = possession_ridge_ratings(rows.drop("date"), lam=lam)
    return {
        "fixed": dict(zip(fixed["team_id"].to_list(), fixed["adj_em"].to_list(), strict=True)),
        "ridge": dict(zip(ridge["team_id"].cast(pl.Utf8).to_list(), ridge["adj_net_rtg"].to_list(), strict=True)),
    }


def _spearman(a: dict[str, float], b: dict[str, float], teams: set[str]) -> float:
    keys = sorted(teams & a.keys() & b.keys())
    frame = pl.DataFrame({"a": [a[k] for k in keys], "b": [b[k] for k in keys]})
    return float(frame.select(pl.corr("a", "b", method="spearman")).item())


def _ranks(r: dict[str, float], teams: set[str]) -> dict[str, float]:
    keys = sorted(teams & r.keys())
    rk = pl.Series([r[k] for k in keys]).rank("average", descending=True).to_list()
    return dict(zip(keys, rk, strict=True))


def season_frames(lg: str, season: int, cache: Path, lam: float) -> tuple[pl.DataFrame, pl.DataFrame]:
    """(one row per scored game, one row per cutoff of agreement / stability measures)."""
    sched, eff, teams = load_college(lg, season, cache)
    rows = load_rows(lg, season, cache)
    d1 = set(teams["team_id"].to_list())
    games = sched.filter(
        (pl.col("completed") == True)  # noqa: E712
        & (pl.col("season_type") == 2)
        & pl.col("home_score").is_not_null()
        & pl.col("away_score").is_not_null()
        & pl.col("home_team_id").is_in(sorted(d1))
        & pl.col("away_team_id").is_in(sorted(d1))
    )
    final = _ratings(lg, eff, rows, lam)
    scored, cuts, prev = [], [], None
    for week, cut in cutoffs(eff["date"].min(), games["date"].max()):  # type: ignore[arg-type]
        if week == 0:
            continue
        now = _ratings(lg, eff.filter(pl.col("date") < cut), rows.filter(pl.col("date") < cut), lam)
        wk = games.filter((pl.col("date") >= cut) & (pl.col("date") < cut + dt.timedelta(days=7)))
        rec: dict[str, object] = {"season": season, "week": week, "between": _spearman(now["fixed"], now["ridge"], d1)}
        ranks = {m: _ranks(now[m], d1) for m in METHODS}
        for m in METHODS:
            rec[f"{m}_vs_final"] = _spearman(now[m], final[m], d1)
            if prev is not None:
                common = ranks[m].keys() & prev[m].keys()
                rec[f"{m}_rank_move"] = float(np.mean([abs(ranks[m][t] - prev[m][t]) for t in common]))
        prev = ranks
        cuts.append(rec)
        h, a = wk["home_team_id"].to_list(), wk["away_team_id"].to_list()
        both = [all(t in now[m] for m in METHODS for t in (x, y)) for x, y in zip(h, a, strict=True)]
        rec["unscored"] = len(both) - sum(both)
        wk = wk.filter(pl.Series(both, dtype=pl.Boolean))
        if wk.height == 0:
            continue
        h, a = wk["home_team_id"].to_list(), wk["away_team_id"].to_list()
        scored.append(
            pl.DataFrame(
                {
                    "season": season,
                    "week": week,
                    "game_id": wk["game_id"].cast(pl.Utf8).to_list(),
                    "margin": (wk["home_score"] - wk["away_score"]).cast(pl.Float64).to_list(),
                    "home": (~wk["neutral_site"].fill_null(False)).cast(pl.Float64).to_list(),
                    **{f"d_{m}": [now[m][x] - now[m][y] for x, y in zip(h, a, strict=True)] for m in METHODS},
                }
            )
        )
    return pl.concat(scored), pl.DataFrame(cuts)


def _phase(week: pl.Expr) -> pl.Expr:
    out = pl.lit(None, dtype=pl.Utf8)
    for lo, hi in PHASES:
        out = pl.when((week >= lo) & (week <= hi)).then(pl.lit(f"{lo}-{hi if hi < 99 else ''}")).otherwise(out)
    return out


def fit_maps(train: pl.DataFrame) -> dict[tuple[str, str], tuple[float, float, float]]:
    """(method, phase) -> (a, b, sigma), least squares of the home margin on the train seasons."""
    out = {}
    for (phase,), sub in train.group_by("phase", maintain_order=True):
        y = sub["margin"].to_numpy()
        for m in METHODS:
            x = np.column_stack([sub[f"d_{m}"].to_numpy(), sub["home"].to_numpy()])
            (a, b), *_ = np.linalg.lstsq(x, y, rcond=None)
            sigma = float(np.sqrt(np.mean((y - x @ np.array([a, b])) ** 2)))
            out[(m, str(phase))] = (float(a), float(b), sigma)
    return out


def predict(frame: pl.DataFrame, maps: dict[tuple[str, str], tuple[float, float, float]]) -> pl.DataFrame:
    cols = []
    for m in METHODS:
        a, b, s = (
            pl.col("phase").replace_strict({p: maps[(mm, p)][i] for (mm, p) in maps if mm == m}) for i in range(3)
        )
        mu = a * pl.col(f"d_{m}") + b * pl.col("home")
        cols += [mu.alias(f"mu_{m}"), (mu / s).alias(f"z_{m}")]
    out = frame.with_columns(cols)
    return out.with_columns(
        *[pl.Series(f"p_{m}", norm.cdf(out[f"z_{m}"].to_numpy())) for m in METHODS],
        (pl.col("margin") > 0).cast(pl.Float64).alias("y"),
    )


def _ll(y: np.ndarray, p: np.ndarray) -> np.ndarray:
    p = np.clip(p, 1e-15, 1 - 1e-15)
    return -(y * np.log(p) + (1 - y) * np.log(1 - p))


def score(frame: pl.DataFrame) -> dict[str, object]:
    """Both methods' scores and the paired ridge-minus-fixed differences.

    ``se`` treats games as independent; ``se_week`` clusters them by (season, week), since every
    game of one week is scored by the same two fits. Quote ``se_week`` (``weeks`` clusters).
    """
    y = frame["y"].to_numpy()
    out: dict[str, object] = {"games": frame.height}
    for m in METHODS:
        p, err = frame[f"p_{m}"].to_numpy(), np.abs(frame["margin"].to_numpy() - frame[f"mu_{m}"].to_numpy())
        out[m] = {"log_loss": log_loss_score(y, p), "brier": brier_score(y, p), "margin_mae": float(err.mean())}
    for key, fn in (
        ("log_loss", lambda m: _ll(y, frame[f"p_{m}"].to_numpy())),
        ("brier", lambda m: (frame[f"p_{m}"].to_numpy() - y) ** 2),
        ("margin_mae", lambda m: np.abs(frame["margin"].to_numpy() - frame[f"mu_{m}"].to_numpy())),
    ):
        d = fn("ridge") - fn("fixed")
        sums = (
            pl.DataFrame({"s": frame["season"], "w": frame["week"], "d": d})
            .group_by("s", "w")
            .agg(pl.col("d").sum(), pl.len().alias("n"))
        )
        dev = sums["d"].to_numpy() - sums["n"].to_numpy() * d.mean()
        out[f"ridge_minus_fixed_{key}"] = {
            "diff": float(d.mean()),
            "se": float(d.std(ddof=1) / np.sqrt(d.size)),
            "se_week": float(np.sqrt((dev**2).sum()) / d.size),
            "weeks": sums.height,
        }
    return out


def _rounded(x: object) -> object:
    if isinstance(x, float):
        return round(x, 5)
    if isinstance(x, dict):
        return {k: _rounded(v) for k, v in x.items()}
    return x


def compare(lg: str, train: list[int], holdout: list[int], cache: Path, lam: float) -> dict[str, object]:
    built = {s: season_frames(lg, s, cache, lam) for s in [*train, *holdout]}
    games = {s: g.with_columns(_phase(pl.col("week")).alias("phase")) for s, (g, _) in built.items()}
    maps = fit_maps(pl.concat([games[s] for s in train]))
    ho = predict(pl.concat([games[s] for s in holdout]), maps)
    if ho["phase"].n_unique() < len(PHASES):
        raise ValueError("a phase has no scored holdout game: the inputs are incomplete")
    clean = [s for s in holdout if s != FIXED_POINT_CONSTANTS_SEASON]
    cuts = pl.concat([built[s][1] for s in holdout]).with_columns(_phase(pl.col("week")).alias("phase"))
    stab = cuts.group_by("phase").agg(pl.exclude("season", "week", "unscored").mean()).sort("phase")
    finals = {}
    for s in holdout:
        _, eff, teams = load_college(lg, s, cache)
        r = _ratings(lg, eff, load_rows(lg, s, cache), lam)
        finals[str(s)] = _spearman(r["fixed"], r["ridge"], set(teams["team_id"].to_list()))
    return _rounded(
        {
            "league": lg,
            "lam": lam,
            "train": train,
            "holdout": holdout,
            "maps": {f"{m} {p}": dict(zip(("a", "b", "sigma"), v, strict=True)) for (m, p), v in sorted(maps.items())},
            "unscored_games_holdout": int(cuts["unscored"].sum()),
            "holdout_all": score(ho),
            **(
                {"holdout_untouched": {"seasons": clean, **score(ho.filter(pl.col("season").is_in(clean)))}}
                if clean
                else {}
            ),
            "holdout_by_season": {str(s): score(ho.filter(pl.col("season") == s)) for s in holdout},
            "holdout_by_phase": {str(p): score(ho.filter(pl.col("phase") == p)) for p in sorted(set(ho["phase"]))},
            "final_spearman_between": finals,
            "in_season_by_phase": {
                row["phase"]: {k: v for k, v in row.items() if k != "phase"} for row in stab.to_dicts()
            },
        }
    )  # type: ignore[return-value]


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[1])
    ap.add_argument("--league", choices=sorted(_COLLEGE), required=True)
    ap.add_argument("--train", type=int, nargs="+", required=True, help="seasons by END year")
    ap.add_argument("--holdout", type=int, nargs="+", required=True, help="seasons by END year")
    ap.add_argument("--cache", type=Path, default=Path(".cache/preseason_prior"))
    ap.add_argument("--out", type=Path, default=None, help="write the result json here")
    args = ap.parse_args(argv)
    if set(args.train) & set(args.holdout):
        ap.error("train and holdout seasons must be disjoint")
    if set(args.holdout) & set(COLLEGE_RIDGE_FIT_SEASONS):
        ap.error(f"holdout seasons must be disjoint from the penalty's fit seasons {COLLEGE_RIDGE_FIT_SEASONS}")
    result = compare(args.league, args.train, args.holdout, args.cache, COLLEGE_RIDGE_LAMBDA[_COLLEGE[args.league]])
    text = json.dumps(result, indent=2)
    print(text)
    if args.out:
        args.out.write_text(text + "\n", encoding="utf-8", newline="\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
