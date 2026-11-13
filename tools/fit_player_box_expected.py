# tools/fit_player_box_expected.py
"""Fit the player box expectation (its ``EXPECTED_COEFS``) and the per-36 rank gates, and score both on holdout seasons.

The values are frozen into :mod:`sportsdataverse._common.player_box_expected` -- never tuned by hand.

* **Model**: per stat, a Poisson regression of the game's count on ``ln(usg / rate_usg)`` and
  ``ln(minutes / rate_mpg)`` with ``ln(<s>_base)`` as offset (the module docstring), by Newton's
  method, on the TRAIN seasons' regular-season rows (every game in college) whose season without
  the game has at least ``FIT_MIN_RATE_GAMES`` games. The HOLDOUT seasons (all later) are then
  scored with those train coefficients: MAE and RMSE of ``<s>_exp`` against ``<s>_base`` on the
  same rows. Nothing of a holdout season enters the fit, and no game enters its own baseline.
* **Gates**: ``fit_shot_zone_gates``'s player split-half method on the per-minute rates (a
  player-season's odd / even games, binned on the smaller half's minutes), ``fit_rate_gates.fit_m``
  and ``gate``, and ``frozen_gates``' holdout rule.

``--write`` saves ``tests/fixtures/bb_player_box_expected/<league>_fit.json``; the tests pin the
module's constants to it.

Usage::

    uv run python tools/fit_player_box_expected.py --league nba --data /tmp/pbx_cache --write
"""

from __future__ import annotations

import argparse
import importlib
import json
import math
import sys
from pathlib import Path

import numpy as np
import polars as pl

sys.path.insert(0, str(Path(__file__).resolve().parent))
from fit_rate_gates import _cached  # noqa: E402
from fit_shot_zone_gates import PLAYER_BINS, _row, frozen_gates, player_curve  # noqa: E402

from sportsdataverse._common.player_box_expected import (  # noqa: E402
    FIT_MIN_RATE_GAMES,
    STATS,
    player_game_box_rows,
    player_game_expected_box,
)

#: league -> (train, holdout) seasons by END year, disjoint, the holdout after the train.
SPANS: dict[str, tuple[range, range]] = {
    "nba": (range(2022, 2025), range(2025, 2027)),
    "wnba": (range(2021, 2024), range(2024, 2027)),
    "mbb": (range(2022, 2025), range(2025, 2027)),
    "wbb": (range(2022, 2025), range(2025, 2027)),
}
FIXTURES = Path(__file__).resolve().parents[1] / "tests" / "fixtures" / "bb_player_box_expected"


def rows(league: str, seasons: range, data: Path | None) -> pl.DataFrame:
    """The played player-game rows of the seasons, through the producer's own reader."""
    mod = importlib.import_module(f"sportsdataverse.{league}.{league}_loaders")
    load = mod.__dict__[f"load_{league}_player_boxscore"]
    return pl.concat(
        [
            _cached(data, f"{league}_pbx_rows_{s}.parquet", lambda s=s: player_game_box_rows(load([s]), league=league))
            for s in seasons
        ]
    )


def _design(e: pl.DataFrame) -> pl.DataFrame:
    """The fit / scoring rows: a season without the game of enough games, both usages known."""
    x_usg = (pl.col("_usg").clip(lower_bound=1.0) / pl.col("rate_usg_pct")).log()
    x_min = (pl.col("minutes") / (pl.col("rate_minutes") / pl.col("rate_games"))).log()
    return (
        e.with_columns(pl.col("usg_pct").alias("_usg"))
        .with_columns(x_usg.alias("x_usg"), x_min.alias("x_min"))
        .filter(
            (pl.col("rate_games") >= FIT_MIN_RATE_GAMES)
            & pl.col("x_usg").is_finite()
            & pl.col("x_min").is_finite()
            & (pl.col("pts_base") >= 0)
        )
    )


def fit_poisson(y: np.ndarray, offset: np.ndarray, x: np.ndarray, iters: int = 50) -> np.ndarray:
    """Poisson log-link coefficients ``[a, b...]`` with ``offset``, by Newton's method."""
    xm = np.column_stack([np.ones(len(y)), x])
    beta = np.zeros(xm.shape[1])
    for _ in range(iters):
        mu = np.exp(offset + xm @ beta)
        step = np.linalg.solve(xm.T @ (xm * mu[:, None]), xm.T @ (y - mu))
        beta += step
        if np.abs(step).max() < 1e-10:
            break
    return beta


def fit_coefs(train: pl.DataFrame) -> dict[str, tuple[float, float, float]]:
    """Stat -> ``(a, b_usg, b_min)`` on the train rows (``in_rate`` only, a positive baseline)."""
    d = _design(train.filter(pl.col("_in_rate")))
    out = {}
    for s in STATS:
        k = d.filter(pl.col(f"{s}_base") > 0)
        b = fit_poisson(
            k[s].cast(pl.Float64).to_numpy(),
            np.log(k[f"{s}_base"].to_numpy()),
            k.select("x_usg", "x_min").to_numpy(),
        )
        out[s] = tuple(round(float(v), 4) for v in b)
    return out


def score(holdout: pl.DataFrame) -> dict[str, dict[str, float]]:
    """Per stat: rows, MAE and RMSE of the baseline and the model on the holdout rows."""
    d = _design(holdout).filter(pl.all_horizontal(pl.col(f"{s}_exp").is_not_null() for s in STATS))
    out = {}
    for s in STATS:
        y = d[s].cast(pl.Float64)
        eb, em = (d[f"{s}_{k}"] - y for k in ("base", "exp"))
        out[s] = {
            "rows": d.height,
            "mean": round(float(y.mean()), 4),
            "base_mae": round(float(eb.abs().mean()), 4),
            "exp_mae": round(float(em.abs().mean()), 4),
            "base_rmse": round(float((eb**2).mean() ** 0.5), 4),
            "exp_rmse": round(float((em**2).mean() ** 0.5), 4),
            "base_bias": round(float(eb.mean()), 4),
            "exp_bias": round(float(em.mean()), 4),
        }
    return out


def gate_fits(league: str, r: pl.DataFrame, span: str, seasons: range) -> list[dict]:
    """The per-36 split-half curves: a player-season's odd / even in-rate games."""
    h = (
        r.filter(pl.col("in_rate"))
        .sort("season", "athlete_id", "game_date", "game_id")
        .with_columns(
            pl.lit(0).alias("team_id"),
            pl.col("athlete_id").alias("player_id"),
            (pl.int_range(pl.len()).over(["season", "athlete_id"]) % 2).alias("half"),
        )
        .group_by("season", "team_id", "player_id", "half")
        .agg(pl.col("minutes").sum(), *[pl.col(src).sum() for src in STATS.values()])
    )
    bins, min_bin = PLAYER_BINS[league]
    return [
        _row("player", league, f"{s}_per36", span, seasons, player_curve(h, src, "minutes", bins=bins, min_bin=min_bin))
        for s, src in STATS.items()
    ]


def fit_league(league: str, data: Path | None = None) -> dict:
    """The league's coefficients, gates and holdout scores, as the fixture JSON."""
    train_s, hold_s = SPANS[league]
    tr, ho = rows(league, train_s, data), rows(league, hold_s, data)

    def expected(r: pl.DataFrame, coefs: dict | None) -> pl.DataFrame:
        flat = dict.fromkeys(STATS, (0.0, 0.0, 0.0))
        e = player_game_expected_box(r, league=league, coefs=coefs or flat, first_season=0)
        return e.join(
            r.select("game_id", "athlete_id", pl.col("in_rate").alias("_in_rate")), on=["game_id", "athlete_id"]
        )

    coefs = fit_coefs(expected(tr, None))
    fits = pl.DataFrame(gate_fits(league, tr, "train", train_s) + gate_fits(league, ho, "holdout", hold_s))
    gates = frozen_gates(fits, "player")
    return {
        "league": league,
        "train": f"{train_s.start}-{train_s.stop - 1}",
        "holdout": f"{hold_s.start}-{hold_s.stop - 1}",
        "coefs": {s: list(v) for s, v in coefs.items()},
        "holdout_scores": score(expected(ho, coefs)),
        "per36_gates": {f"{s}_per36": gates.get(f"{s}_per36", math.inf) for s in STATS},
        "gate_m": {f"{r['rate']}:{r['span']}": r["m"] for r in fits.iter_rows(named=True)},
    }


def main(argv: list[str] | None = None) -> int:
    """CLI: fit a league, print the coefficients, gates and holdout scores, optionally write the fixture."""
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--league", choices=sorted(SPANS), required=True)
    ap.add_argument("--data", type=Path, default=None, help="cache the player-game rows here")
    ap.add_argument("--write", action="store_true", help=f"save the fit to {FIXTURES}")
    args = ap.parse_args(argv)
    fit = fit_league(args.league, args.data)
    print(json.dumps(fit, indent=2, default=str))
    if args.write:
        FIXTURES.mkdir(parents=True, exist_ok=True)
        (FIXTURES / f"{args.league}_fit.json").write_text(json.dumps(fit, indent=2, default=str) + "\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
