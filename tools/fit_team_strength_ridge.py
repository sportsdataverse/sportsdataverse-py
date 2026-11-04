# tools/fit_team_strength_ridge.py
"""Fit the NBA / WNBA team-strength ridge penalty by season-disjoint, game-grouped CV.

The fitted values are frozen into
``sportsdataverse._common.team_strength.RIDGE_LAMBDA`` with a comment citing this script --
never tuned ad hoc.

The model is the one :func:`sportsdataverse._common.team_strength.possession_ridge_ratings`
fits: points per 100 on each possession, regressed on the offense and defense team indicators
and a home term, solved by :func:`sportsdataverse._common.ratings.opponent_adjusted_ridge`.

Two levels of holdout:

* **Within a season, folds are games** (``int(game_id) % 5``), not possessions: the
  possessions of one game share its players and score, so a possession-level split would leak
  and flatter small penalties. Each fold is fit on the other four and scored on its own games'
  possessions by MSE of points per 100.
* **Across seasons, the penalty is chosen on other seasons.** For each season, the penalty
  that minimizes the pooled CV error of the *other* seasons is scored on that season, and the
  regret against that season's own best is reported. The frozen value is the penalty with the
  lowest pooled error over all seasons; the leave-one-season-out table shows it carries to a
  season it was not chosen on.

Inputs are the ``nba_stats_possessions`` / ``wnba_stats_possessions`` and schedule releases,
read through the package loaders (network).

Usage::

    uv run python tools/fit_team_strength_ridge.py --league nba --seasons 2022 2023 2024 2025 2026
    uv run python tools/fit_team_strength_ridge.py --league wnba --seasons 2021 2022 2023 2024 2025
"""

from __future__ import annotations

import argparse
import sys

import numpy as np
import polars as pl

from sportsdataverse._common.team_strength import ridge_design
from sportsdataverse._common.ratings import opponent_adjusted_ridge
from sportsdataverse.nba.nba_team_season_rates import _league_loaders, nba_possession_rate_rows

GRID = [25.0, 50.0, 100.0, 200.0, 400.0, 800.0, 1600.0, 3200.0]
_LEAGUE_ID = {"nba": "00", "wnba": "10"}
_FOLDS = 5


def season_cv(rows: pl.DataFrame, grid: list[float]) -> dict[float, tuple[float, int]]:
    """Game-grouped K-fold (sum of squared error, n) per penalty for one season's possessions."""
    design = ridge_design(rows).with_columns((pl.col("game_id").cast(pl.Int64) % _FOLDS).alias("_fold"))
    out = {lam: (0.0, 0) for lam in grid}
    for k in range(_FOLDS):
        train, test = design.filter(pl.col("_fold") != k), design.filter(pl.col("_fold") == k)
        for lam in grid:
            coef, b0, hfa = opponent_adjusted_ridge(
                train,
                off_col="offense_id",
                def_col="defense_id",
                home_col="offense_id",
                resp_col="y",
                lam=lam,
                hfa_col="hfa",
            )
            off = dict(zip(coef["team_id"], coef["off_coef"]))
            de = dict(zip(coef["team_id"], coef["def_coef"]))
            # A held-out team with no training rows gets the ridge prior mean, 0.
            pred = (
                b0
                + np.array([off.get(t, 0.0) for t in test["offense_id"]])
                + np.array([de.get(t, 0.0) for t in test["defense_id"]])
                + hfa * test["hfa"].to_numpy()
            )
            sse = float(((test["y"].to_numpy() - pred) ** 2).sum())
            s, n = out[lam]
            out[lam] = (s + sse, n + test.height)
    return out


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[1])
    ap.add_argument("--league", choices=sorted(_LEAGUE_ID), required=True)
    ap.add_argument("--seasons", type=int, nargs="+", required=True, help="seasons by END year")
    args = ap.parse_args(argv)
    if len(set(args.seasons)) < 2:
        ap.error("--seasons needs at least two distinct seasons: each one is scored on a penalty chosen on the others")

    league_id = _LEAGUE_ID[args.league]
    load_possessions, load_schedule, offset = _league_loaders(league_id)
    per_season: dict[int, dict[float, tuple[float, int]]] = {}
    for season in args.seasons:
        rows = nba_possession_rate_rows(
            load_possessions([season - offset]), load_schedule([season - offset]), league_id=league_id
        )
        per_season[season] = season_cv(rows, GRID)
        best = min(GRID, key=lambda lam: per_season[season][lam][0])
        mse = {lam: s / n for lam, (s, n) in per_season[season].items()}
        print(
            f"{args.league} {season}: n={rows.height} best={best:g} "
            + " ".join(f"{lam:g}:{mse[lam]:.2f}" for lam in GRID)
        )

    def pooled(seasons: list[int]) -> dict[float, float]:
        return {
            lam: sum(per_season[s][lam][0] for s in seasons) / sum(per_season[s][lam][1] for s in seasons)
            for lam in GRID
        }

    print("leave-one-season-out:")
    for season in args.seasons:
        others = pooled([s for s in args.seasons if s != season])
        chosen = min(GRID, key=others.__getitem__)
        own = {lam: s / n for lam, (s, n) in per_season[season].items()}
        regret = own[chosen] - min(own.values())
        print(
            f"  hold out {season}: chosen on the others={chosen:g}  held-out MSE={own[chosen]:.3f}  regret={regret:.4f}"
        )
    allp = pooled(args.seasons)
    final = min(GRID, key=allp.__getitem__)
    print(
        f"{args.league} pooled best: {final:g} (MSE {allp[final]:.3f}); "
        + " ".join(f"{lam:g}:{allp[lam]:.3f}" for lam in GRID)
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
