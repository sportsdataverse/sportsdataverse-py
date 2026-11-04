# tools/fit_team_luck_pythag.py
"""Fit the basketball Pythagorean exponent per league on season final scores, train/holdout disjoint.

The fitted values are frozen into ``sportsdataverse._common.team_luck.PYTHAG_EXPONENT`` with a
comment citing this script -- never tuned ad hoc.

The model is the one :func:`sportsdataverse._common.team_luck.team_luck` applies:
``exp_wins = games * pts^x / (pts^x + opp_pts^x)`` on a team's season points for and against.
``x`` is chosen on the TRAIN seasons only, as the grid value (0.01 steps over 1..30) with the
least squared error in wins over every qualified team-season; the HOLDOUT seasons (later, never
seen by the fit) are scored by mean absolute error in wins. The published exponents from the
literature are scored on the same holdout for reference: Morey's 13.91, Oliver's 14 (Basketball
on Paper; Basketball-Reference's Pythagorean W-L), Hollinger's 16.5, and for college KenPom's
10.25 (his original) and 11.5 (his current, on adjusted efficiencies).

A team-season qualifies with at least half the most games any team played that season (BB-D3's
gate): it keeps the college non-D1 opponents, a game or two each, out of the fit.

Inputs are the season final scores the production code reads, through the same adapters:
``nba_stats_schedules`` / ``wnba_stats_schedules`` (regular season) and the ESPN
``{mbb,wbb}_schedule`` releases (every completed game), via the package loaders (network).
``--write`` saves the per-team-season records to ``tests/fixtures/bb_team_luck/`` so the test
suite re-runs this fit offline.

The spans are :data:`SPANS`: every season the releases carry through the last complete one, the
last ten (nine in college) held out.

Usage::

    uv run python tools/fit_team_luck_pythag.py --league nba --write
    uv run python tools/fit_team_luck_pythag.py --league wbb --train 2002 2017 --holdout 2018 2026
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import numpy as np
import polars as pl

from sportsdataverse._common.team_luck import QUALIFIER, season_records

#: league -> (train, holdout) seasons by END year, disjoint, the holdout after the train.
SPANS: dict[str, tuple[range, range]] = {
    "nba": (range(1997, 2017), range(2017, 2027)),
    "wnba": (range(1997, 2016), range(2016, 2026)),
    "mbb": (range(2002, 2018), range(2018, 2027)),
    "wbb": (range(2002, 2018), range(2018, 2027)),
}
GRID = np.round(np.arange(1.0, 30.0 + 1e-9, 0.01), 2)
REFERENCE: dict[str, dict[str, float]] = {
    "nba": {"Morey 13.91": 13.91, "Oliver 14": 14.0, "Hollinger 16.5": 16.5},
    "wnba": {"Morey 13.91": 13.91, "Oliver 14": 14.0, "Hollinger 16.5": 16.5},
    "mbb": {"KenPom 10.25": 10.25, "KenPom 11.5": 11.5, "Oliver 14": 14.0},
    "wbb": {"KenPom 10.25": 10.25, "KenPom 11.5": 11.5, "Oliver 14": 14.0},
}
FIXTURES = Path(__file__).resolve().parents[1] / "tests" / "fixtures" / "bb_team_luck"


def qualified(records: pl.DataFrame) -> pl.DataFrame:
    """The team-seasons with at least half the most games any team played that season (the ranks' gate)."""
    return records.filter(QUALIFIER)


def _arrays(records: pl.DataFrame) -> tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray]:
    r = qualified(records)
    return (
        r["pts"].to_numpy().astype(float),
        r["opp_pts"].to_numpy().astype(float),
        r["games"].to_numpy().astype(float),
        r["wins"].to_numpy().astype(float),
    )


def expected_wins(pts: np.ndarray, opp: np.ndarray, games: np.ndarray, x: float) -> np.ndarray:
    """``games * pts^x / (pts^x + opp^x)``, as ``team_luck`` computes it."""
    return games / (1.0 + (opp / pts) ** x)


def fit_exponent(records: pl.DataFrame) -> float:
    """The grid exponent with the least squared error in wins over the qualified team-seasons."""
    pts, opp, games, wins = _arrays(records)
    if pts.size == 0:
        raise ValueError("fit_exponent: no qualified team-seasons to fit")
    sse = [float(((wins - expected_wins(pts, opp, games, x)) ** 2).sum()) for x in GRID]
    return float(GRID[int(np.argmin(sse))])


def score(records: pl.DataFrame, x: float) -> tuple[float, float, int]:
    """(MAE, RMSE) in wins and the team-season count, over the qualified team-seasons."""
    pts, opp, games, wins = _arrays(records)
    err = wins - expected_wins(pts, opp, games, x)
    return float(np.abs(err).mean()), float(np.sqrt((err**2).mean())), int(err.size)


def load_records(league: str, seasons: list[int]) -> pl.DataFrame:
    """Per-team-season records from the final scores the production adapters read (network)."""
    if league in ("nba", "wnba"):
        from sportsdataverse.nba.nba_team_luck import nba_game_results
        from sportsdataverse.nba.nba_team_season_rates import _league_loaders

        _, load_schedule, offset = _league_loaders("00" if league == "nba" else "10")
        results = nba_game_results(load_schedule([s - offset for s in seasons]))
    else:
        from sportsdataverse.mbb.mbb_team_luck import espn_game_results
        from sportsdataverse.mbb.mbb_team_season_rates import _league_loaders

        _, _, load_schedule = _league_loaders("mens" if league == "mbb" else "womens")
        results = espn_game_results(load_schedule(seasons))
    return season_records(results)


def report(league: str, records: pl.DataFrame, train: list[int], holdout: list[int]) -> float:
    """Fit on ``train``, print the holdout table and return the exponent."""
    tr = records.filter(pl.col("season").is_in(train))
    ho = records.filter(pl.col("season").is_in(holdout))
    x = fit_exponent(tr)
    tr_mae, tr_rmse, tr_n = score(tr, x)
    ho_mae, ho_rmse, ho_n = score(ho, x)
    print(f"{league}: fitted x={x:.2f} on {train[0]}-{train[-1]} ({tr_n} team-seasons)")
    print(f"  train   MAE {tr_mae:.3f} wins, RMSE {tr_rmse:.3f}")
    print(f"  holdout MAE {ho_mae:.3f} wins, RMSE {ho_rmse:.3f} on {holdout[0]}-{holdout[-1]} ({ho_n} team-seasons)")
    for name, ref in REFERENCE[league].items():
        mae, rmse, _ = score(ho, ref)
        # paired by holdout season: fitted MAE minus the reference's, its spread, seasons the fit wins
        deltas = [
            score(ho.filter(pl.col("season") == s), x)[0] - score(ho.filter(pl.col("season") == s), ref)[0]
            for s in holdout
        ]
        d = np.array(deltas)
        print(
            f"  holdout at {name}: MAE {mae:.3f}, RMSE {rmse:.3f}; per-season delta {d.mean():+.3f} "
            f"(sd {d.std(ddof=1):.3f}), fit better in {(d < 0).sum()} of {d.size}"
        )
    own = fit_exponent(ho)
    print(f"  holdout's own best x={own:.2f} (MAE {score(ho, own)[0]:.3f})")
    for s in holdout:
        one = ho.filter(pl.col("season") == s)
        if one.height:
            mae, _, n = score(one, x)
            print(f"    {s}: MAE {mae:.3f} ({n})")
    return x


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[1])
    ap.add_argument("--league", choices=sorted(REFERENCE), required=True)
    ap.add_argument("--train", type=int, nargs=2, metavar=("FIRST", "LAST"), help="END years (default: SPANS)")
    ap.add_argument("--holdout", type=int, nargs=2, metavar=("FIRST", "LAST"), help="END years (default: SPANS)")
    ap.add_argument("--write", action="store_true", help="save the records under tests/fixtures/bb_team_luck/")
    args = ap.parse_args(argv)
    span_train, span_holdout = SPANS[args.league]
    train = list(range(args.train[0], args.train[1] + 1)) if args.train else list(span_train)
    holdout = list(range(args.holdout[0], args.holdout[1] + 1)) if args.holdout else list(span_holdout)
    if max(train) >= min(holdout):
        ap.error("--holdout must come after --train: later seasons the fit never saw")
    records = load_records(args.league, train + holdout)
    report(args.league, records, train, holdout)
    if args.write:
        FIXTURES.mkdir(parents=True, exist_ok=True)
        path = FIXTURES / f"{args.league}_season_records.parquet"
        records.write_parquet(path, compression="zstd", compression_level=19)
        print(f"wrote {path} ({records.height} rows)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
