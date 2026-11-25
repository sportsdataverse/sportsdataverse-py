# tools/fit_college_ridge_prior.py
"""Fit the college possession ridge's last-season prior, and re-pick its penalty jointly with it.

The ridge behind ``adj_*_rtg_ridge`` (:func:`sportsdataverse._common.team_strength.possession_ridge_ratings`)
shrinks every team toward average. With ``prior=`` it shrinks a team toward ``r`` times its previous
season-end rating instead (:func:`~sportsdataverse._common.team_strength.last_season_prior`), never
faded: the penalty's weight falls off by itself as possessions pile up.

**What is fit, on the TRAIN seasons only.** For each penalty on the grid:

* ``r``: the no-intercept least squares of a team's season-end net rating on its previous season-end
  net rating, both at that penalty, over the train seasons;
* the as-of game model ``a * d_net + b * home``, per phase (weeks 1-4, 5-8, 9+), as
  ``tools/compare_college_team_strength.py`` fits it.

The penalty is the one with the lowest train as-of margin MAE. The HOLDOUT seasons are scored once,
for the chosen (penalty, r), the prior at the shipped penalty and the shipped fit (no prior,
:data:`~sportsdataverse._common.team_strength.COLLEGE_RIDGE_LAMBDA`). Each difference carries its
standard error clustered by (season, week), because every game of a week is scored by one fit.

**As-of protocol** (``tools/compare_college_team_strength.py``): cutoffs every 7 days from the
season's first game; each fit reads the possessions strictly before the cutoff (asserted) and scores
the Division I vs Division I regular-season games of the next 7 days. The prior for season ``s`` is
built from season ``s - 1`` alone, which is over before ``s`` starts.

Inputs (network, cached as parquet under ``--cache``): as ``compare_college_team_strength.py``.

Usage::

    uv run python tools/fit_college_ridge_prior.py --league mbb --train 2022 2023 --holdout 2024 2025 2026
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import sys
from pathlib import Path

import numpy as np
import polars as pl

sys.path.insert(0, str(Path(__file__).resolve().parent))
from compare_college_team_strength import _COLLEGE, _phase, load_rows  # noqa: E402
from fit_preseason_prior import cutoffs, load_college  # noqa: E402

from sportsdataverse._common.team_strength import (  # noqa: E402
    COLLEGE_RIDGE_FIT_SEASONS,
    COLLEGE_RIDGE_LAMBDA,
    last_season_prior,
    possession_ridge_ratings,
)

#: The penalty grid, per league: the shipped value and its halvings and doublings.
LAMS = {"mbb": (50.0, 100.0, 200.0, 400.0), "wbb": (12.5, 25.0, 50.0, 100.0, 200.0)}


def _net(rows: pl.DataFrame, lam: float, prior: pl.DataFrame | None) -> dict[str, float]:
    """team_id (text) -> net rating of the ridge on ``rows``."""
    out = possession_ridge_ratings(rows, lam=lam, prior=prior)
    return dict(zip(out["team_id"].cast(pl.Utf8).to_list(), out["adj_net_rtg"].to_list(), strict=True))


def regression(lg: str, train: list[int], lam: float, cache: Path) -> float:
    """``r``: season-end net on the previous season-end net, no intercept, over ``train``."""
    xs: list[float] = []
    ys: list[float] = []
    for s in train:
        cur, prev = (_net(load_rows(lg, x, cache).drop("date"), lam, None) for x in (s, s - 1))
        common = sorted(cur.keys() & prev.keys())
        xs += [prev[t] for t in common]
        ys += [cur[t] for t in common]
    x, y = np.asarray(xs), np.asarray(ys)
    return float(x @ y / (x @ x))


def season_walk(lg: str, season: int, variants: dict[str, tuple[float, float | None]], cache: Path) -> pl.DataFrame:
    """One row per scored game: the home margin and each variant's as-of net difference."""
    sched, eff, teams = load_college(lg, season, cache)
    rows = load_rows(lg, season, cache)
    prev = load_rows(lg, season - 1, cache).drop("date")
    priors = {
        v: None if r is None else last_season_prior(prev, lam=lam, regression=r) for v, (lam, r) in variants.items()
    }
    d1 = set(teams["team_id"].to_list())
    games = sched.filter(
        (pl.col("completed") == True)  # noqa: E712
        & (pl.col("season_type") == 2)
        & pl.col("home_score").is_not_null()
        & pl.col("away_score").is_not_null()
        & pl.col("home_team_id").is_in(sorted(d1))
        & pl.col("away_team_id").is_in(sorted(d1))
    )
    out = []
    for week, cut in cutoffs(eff["date"].min(), games["date"].max()):
        train = rows.filter(pl.col("date") < cut)
        wk = games.filter((pl.col("date") >= cut) & (pl.col("date") < cut + dt.timedelta(days=7)))
        if week == 0 or train.height == 0 or wk.height == 0:
            continue
        assert train["date"].max() < cut, "as-of leak: a possession on or after the cutoff"
        nets = {v: _net(train.drop("date"), lam, priors[v]) for v, (lam, _) in variants.items()}
        h, a = wk["home_team_id"].cast(pl.Utf8).to_list(), wk["away_team_id"].cast(pl.Utf8).to_list()
        keep = [all(x in n and y in n for n in nets.values()) for x, y in zip(h, a, strict=True)]
        wk = wk.filter(pl.Series(keep))
        pairs = [(x, y) for (x, y), k in zip(zip(h, a, strict=True), keep, strict=True) if k]
        if not pairs:
            continue
        frame = {
            "season": [season] * len(pairs),
            "week": [week] * len(pairs),
            "margin": (wk["home_score"] - wk["away_score"]).cast(pl.Float64).to_list(),
            "home": (~wk["neutral_site"].fill_null(False)).cast(pl.Float64).to_list(),
        }
        for v, n in nets.items():
            frame[f"d_{v}"] = [n[x] - n[y] for x, y in pairs]
        out.append(pl.DataFrame(frame))
    return pl.concat(out).with_columns(_phase(pl.col("week")).alias("phase"))


def fit_maps(train: pl.DataFrame, names: list[str]) -> dict[tuple[str, str], tuple[float, float]]:
    """(variant, phase) -> (a, b): least squares of the home margin on the train seasons."""
    maps = {}
    for (phase,), sub in train.group_by("phase", maintain_order=True):
        y = sub["margin"].to_numpy()
        for v in names:
            x = np.column_stack([sub[f"d_{v}"].to_numpy(), sub["home"].to_numpy()])
            (a, b), *_ = np.linalg.lstsq(x, y, rcond=None)
            maps[(v, str(phase))] = (float(a), float(b))
    return maps


def abs_errors(frame: pl.DataFrame, maps: dict[tuple[str, str], tuple[float, float]], v: str) -> np.ndarray:
    """Per-game absolute margin error of variant ``v``."""
    ph = frame["phase"].to_list()
    a = np.array([maps[(v, p)][0] for p in ph])
    b = np.array([maps[(v, p)][1] for p in ph])
    return np.abs(frame["margin"].to_numpy() - (a * frame[f"d_{v}"].to_numpy() + b * frame["home"].to_numpy()))


def paired(frame: pl.DataFrame, d: np.ndarray) -> dict[str, float]:
    """Mean of a per-game difference and its standard error clustered by (season, week)."""
    sums = (
        pl.DataFrame({"s": frame["season"], "w": frame["week"], "d": d})
        .group_by("s", "w")
        .agg(pl.col("d").sum(), pl.len().alias("n"))
    )
    dev = sums["d"].to_numpy() - sums["n"].to_numpy() * d.mean()
    return {"diff": round(float(d.mean()), 4), "se": round(float(np.sqrt((dev**2).sum()) / d.size), 4)}


def holdout_report(ho: pl.DataFrame, maps: dict[tuple[str, str], tuple[float, float]], chosen: str, ref: str) -> dict:
    """Holdout MAE of every variant and the three comparisons, overall and per phase."""
    out: dict[str, object] = {"games": ho.height, "weeks": ho.select("season", "week").n_unique()}
    for label, sub in [("all", ho), *[(p, ho.filter(pl.col("phase") == p)) for p in sorted(ho["phase"].unique())]]:
        err = {v: abs_errors(sub, maps, v) for v in {chosen, ref, "shipped"}}
        out[label] = {
            "n": sub.height,
            "mae": {v: round(float(e.mean()), 4) for v, e in err.items()},
            "chosen_vs_shipped": paired(sub, err[chosen] - err["shipped"]),
            "prior_at_shipped_lam_vs_shipped": paired(sub, err[ref] - err["shipped"]),
            "chosen_vs_prior_at_shipped_lam": paired(sub, err[chosen] - err[ref]),
        }
    return out


def fit(lg: str, train: list[int], holdout: list[int], cache: Path) -> dict[str, object]:
    """Fit ``r`` per penalty and the as-of maps on ``train``, pick the penalty, score ``holdout`` once."""
    assert not set(train) & set(holdout), "train and holdout seasons overlap"
    assert not set(holdout) & set(COLLEGE_RIDGE_FIT_SEASONS), "holdout overlaps the penalty's fit seasons"
    lam0 = COLLEGE_RIDGE_LAMBDA[_COLLEGE[lg]]
    rs = {lam: regression(lg, train, lam, cache) for lam in LAMS[lg]}
    variants: dict[str, tuple[float, float | None]] = {"shipped": (lam0, None)}
    variants |= {f"prior_lam{lam:g}": (lam, r) for lam, r in rs.items()}
    names = list(variants)
    walks = {s: season_walk(lg, s, variants, cache) for s in [*train, *holdout]}
    tr = pl.concat([walks[s] for s in train])
    ho = pl.concat([walks[s] for s in holdout])
    maps = fit_maps(tr, names)
    train_mae = {v: round(float(abs_errors(tr, maps, v).mean()), 4) for v in names}
    chosen = min((v for v in names if v != "shipped"), key=train_mae.__getitem__)
    ref = f"prior_lam{lam0:g}"
    return {
        "league": lg,
        "train": train,
        "holdout": holdout,
        "shipped_lam": lam0,
        "regression_by_lam": {f"{lam:g}": round(r, 4) for lam, r in rs.items()},
        "train_games": tr.height,
        "train_mae": train_mae,
        "chosen": chosen,
        "chosen_lam": variants[chosen][0],
        "chosen_regression": round(float(variants[chosen][1] or 0.0), 4),
        "holdout_scored_once": holdout_report(ho, maps, chosen, ref),
    }


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[1])
    ap.add_argument("--league", choices=["mbb", "wbb"], default="mbb")
    ap.add_argument("--train", type=int, nargs="+", default=[2022, 2023])
    ap.add_argument("--holdout", type=int, nargs="+", default=[2024, 2025, 2026])
    ap.add_argument("--cache", type=Path, default=Path("dev/team_strength_cache"))
    ap.add_argument("--out", type=Path, default=None)
    args = ap.parse_args(argv)
    result = fit(args.league, args.train, args.holdout, args.cache)
    text = json.dumps(result, indent=1)
    if args.out:
        args.out.write_text(text, encoding="utf-8")
    print(text)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
