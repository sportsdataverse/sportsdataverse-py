# tools/fit_ncaa_rapm_lambda.py
"""Fit the league-wide NCAA RAPM ridge penalty by game-grouped 5-fold CV.

The fitted value is frozen into
``sportsdataverse.mbb.mbb_ncaa_rapm_league.DEFAULT_RIDGE_LAMBDA`` with a
comment citing this script -- never tuned ad hoc.

Folds are GAMES (``contest_id % 5``), not possessions: possessions within a
game share lineups and score context, so a possession-level split would leak
and flatter small penalties. For each fold the model is fit on the other four
folds' possessions and scored on the held-out games' stints by
possession-weighted MSE of pts-per-100 (players unseen in training predict 0,
as they must -- shrinkage is the thing being chosen).

Inputs are the published ``ncaa-{mbb,wbb}-hoops-data`` parquet trees
(``possessions``, ``team_rosters`` and the optional ``name_changes``), read from
sibling checkouts under ``--data-root``.

The 2024 fit behind ``DEFAULT_RIDGE_LAMBDA = 1000.0`` (mbb weighted OOS MSE
5145.33, wbb 4910.74, both minimized at 1000 over the grid below) was run from
this script's previous untracked home, ``dev/ncaa_rapm/fit_lambda_league.py``;
the only change in the move is that the data root is an argument instead of a
path hard-coded in a sibling dev module.

Usage::

    uv run python tools/fit_ncaa_rapm_lambda.py --league mbb --season 2024 \
        --data-root ~/Documents/GitHub-Data/sdv-dev
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import polars as pl

from sportsdataverse.mbb.mbb_ncaa_rapm_input import (
    build_player_xwalk,
    expand_xwalk_aliases,
    observed_pairs,
    resolve_possessions,
)
from sportsdataverse.mbb.mbb_ncaa_rapm_league import aggregate_stints, solve_rapm_league

GRID = [50.0, 100.0, 200.0, 500.0, 1000.0, 2000.0, 5000.0]

#: league -> (data repo under --data-root, league dir, file stem)
_DATA = {
    "mbb": ("hoopR-dev/ncaa-mbb-hoops-data", "mbb", "ncaa_mbb"),
    "wbb": ("wehoop-dev/ncaa-wbb-hoops-data", "wbb", "ncaa_wbb"),
}


def _data_file(root: Path, league: str, dataset: str, season: "int | None" = None) -> Path:
    repo, lg, stem = _DATA[league]
    name = f"{stem}_{dataset}_{season}.parquet" if season else f"{stem}_{dataset}.parquet"
    return root / repo / lg / dataset / "parquet" / name


def weighted_mse(stints: pl.DataFrame, players: pl.DataFrame, info: dict) -> "tuple[float, float]":
    """Possession-weighted MSE of predicted vs observed pts/100 on held-out stints."""
    o = dict(zip(players["player_id"].to_list(), players["orapm"].to_list()))
    d = dict(zip(players["player_id"].to_list(), players["drapm"].to_list()))
    mu, hca = info["intercept"], info["hca"]
    num = 0.0
    den = 0.0
    for off_ids, def_ids, is_home, n, pts in stints.select(
        "off_ids", "def_ids", "is_home_offense", "n_poss", "pts"
    ).rows():
        pred = mu + sum(o.get(p, 0.0) for p in off_ids) - sum(d.get(p, 0.0) for p in def_ids)
        pred += hca * (1.0 if is_home else -1.0)
        y = 100.0 * pts / n
        num += n * (y - pred) ** 2
        den += n
    return num, den


def main(argv: "list[str] | None" = None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--league", choices=("mbb", "wbb"), required=True)
    ap.add_argument("--season", type=int, default=2024)
    ap.add_argument(
        "--data-root",
        type=Path,
        default=Path(__file__).resolve().parents[2],
        help="directory holding the hoopR-dev/ and wehoop-dev/ checkouts (default: the sdv-py checkout's parent)",
    )
    a = ap.parse_args(argv)

    possessions = pl.read_parquet(_data_file(a.data_root, a.league, "possessions", a.season))
    rosters = pl.read_parquet(_data_file(a.data_root, a.league, "team_rosters", a.season))
    nc_f = _data_file(a.data_root, a.league, "name_changes")
    name_changes = None
    if nc_f.is_file():
        name_changes = pl.read_parquet(nc_f).filter(pl.col("season").cast(pl.Utf8) == str(a.season))

    xwalk = expand_xwalk_aliases(build_player_xwalk(rosters), observed_pairs(possessions), name_changes=name_changes)
    resolved = resolve_possessions(possessions, xwalk).with_columns(
        (pl.col("contest_id").cast(pl.Int64) % 5).alias("_fold")
    )
    print(f"{a.league} {a.season}: {resolved.height} resolved possessions, 5 game-folds", flush=True)

    scores = {lam: [0.0, 0.0] for lam in GRID}
    for k in range(5):
        train = aggregate_stints(resolved.filter(pl.col("_fold") != k))
        test = aggregate_stints(resolved.filter(pl.col("_fold") == k))
        for lam in GRID:
            players, info = solve_rapm_league(train, ridge_lambda=lam)
            num, den = weighted_mse(test, players, info)
            scores[lam][0] += num
            scores[lam][1] += den
        print(f"  fold {k} done", flush=True)

    best = None
    for lam in GRID:
        mse = scores[lam][0] / scores[lam][1]
        print(f"  lambda={lam:>7.0f}  weighted OOS MSE = {mse:.4f}", flush=True)
        if best is None or mse < best[1]:
            best = (lam, mse)
    print(f"BEST lambda={best[0]:.0f} (MSE {best[1]:.4f})", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
