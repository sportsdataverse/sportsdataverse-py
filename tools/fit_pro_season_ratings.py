# tools/fit_pro_season_ratings.py
"""Pick the NBA / WNBA season-sim ratings source and penalty on an as-of-date holdout.

The fitted values are frozen into ``sportsdataverse.nba.nba_season_sim.SIM_CONSTANTS`` with a
comment citing this script -- never tuned ad hoc.

**The question.** A remaining-schedule sim reads team ratings as of a date and predicts every
game after it. Two ratings sources exist:

* ``box``: the existing :func:`sportsdataverse.nba.nba_team_ratings.nba_team_ratings` estimator
  (box-score possessions, the KenPom-style fixed point, ``adjust_efficiency`` /
  ``adjust_pace``) scored through :func:`sportsdataverse.nba.nba_game_predict.nba_predict_games`
  with its shipped ``hfa`` / ``margin_sd``. Here the box counts are summed from the same
  possession rows, so both sources see the same games.
* ``ridge``: BB-D19's possession ridge
  (:func:`sportsdataverse._common.team_strength.possession_ridge_ratings`) at a penalty from
  ``GRID``. Its predicted home margin is ``(net_home - net_away + 2 * hfa) * poss / 100``, with
  the ridge's own home term and the as-of mean possessions per team-game.

**The holdout.** For each season, cutoffs every 7 days from day 14 of the regular season: the
ratings use the games strictly before the cutoff, and score every regular-season game on or after
it. Win probability is ``Phi(margin / sigma)``. ``sigma`` (and, for the ridge, the penalty) is
chosen on the TRAIN seasons by pooled log loss; the HOLDOUT seasons are scored once with those
choices. ``box`` is also reported as shipped (its own ``margin_sd``).

Train and holdout seasons are disjoint. The WNBA seasons 2022, 2023 and 2026 are left out: the
2026-11-02 sim-vs-market spec reserves them for later pre-registered WNBA fits.

Inputs are the ``{nba,wnba}_stats_possessions`` / ``_schedules`` releases (network), cached as
parquet under ``--cache``.

Usage::

    uv run python tools/fit_pro_season_ratings.py --league nba \
        --train 2018 2019 2022 2023 --holdout 2024 2025 2026
    uv run python tools/fit_pro_season_ratings.py --league wnba --train 2018 2019 2021 --holdout 2024 2025
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

from sportsdataverse._common.metrics import brier_score, log_loss_score
from sportsdataverse._common.team_strength import possession_ridge_ratings
from sportsdataverse.nba.nba_game_predict import nba_predict_games
from sportsdataverse.nba.nba_prediction_constants import get_constants
from sportsdataverse.nba.nba_team_ratings import adjust_efficiency, adjust_pace, raw_game_efficiency
from sportsdataverse.nba.nba_team_season_rates import _league_loaders, nba_possession_rate_rows

GRID = [6.25, 12.5, 25.0, 50.0, 100.0, 200.0, 400.0, 800.0, 1600.0, 3200.0, 6400.0, 12800.0, 25600.0]
SIGMAS = np.round(np.arange(6.0, 22.01, 0.1), 2)
_LEAGUE_ID = {"nba": "00", "wnba": "10"}


def load_season(league_id: str, season: int, cache: Path) -> tuple[pl.DataFrame, pl.DataFrame]:
    """(possession rate rows with ``date``, regular-season games) for one season, cached."""
    tag = f"{league_id}_{season}"
    rows_p, games_p = cache / f"rows_{tag}.parquet", cache / f"games_{tag}.parquet"
    if rows_p.exists() and games_p.exists():
        return pl.read_parquet(rows_p), pl.read_parquet(games_p)
    load_possessions, load_schedule, offset = _league_loaders(league_id)
    schedule = load_schedule([season - offset])
    rows = nba_possession_rate_rows(load_possessions([season - offset]), schedule, league_id=league_id)
    games = schedule.filter(
        (pl.col("season_type") == "regular-season")
        & pl.col("home_pts").is_not_null()
        & pl.col("away_pts").is_not_null()
    ).select(
        pl.col("game_id").cast(pl.Utf8),
        pl.lit(season, dtype=pl.Int64).alias("season"),
        pl.col("game_date").str.to_date().alias("date"),
        pl.col("home_team_id").cast(pl.Int64),
        pl.col("away_team_id").cast(pl.Int64),
        (pl.col("home_pts") - pl.col("away_pts")).cast(pl.Float64).alias("margin"),
    )
    assert rows.schema["game_id"] == games.schema["game_id"]
    rows = rows.join(games.select("game_id", "date"), on="game_id", how="inner", validate="m:1")
    cache.mkdir(parents=True, exist_ok=True)
    rows.write_parquet(rows_p)
    games.write_parquet(games_p)
    return rows, games


def _box_from_rows(rows: pl.DataFrame) -> pl.DataFrame:
    """ESPN-team-box-shaped counts per (game, team), summed from the possession rows."""
    return (
        rows.group_by("game_id", "offense_team_id")
        .agg(
            pl.col("fga").sum().alias("field_goals_attempted"),
            pl.col("oreb").sum().alias("offensive_rebounds"),
            pl.col("tov").sum().alias("turnovers"),
            pl.col("fta").sum().alias("free_throws_attempted"),
            pl.col("points").sum().alias("team_score"),
        )
        .rename({"offense_team_id": "team_id"})
    )


def box_margins(rows: pl.DataFrame, games: pl.DataFrame, future: pl.DataFrame, league_id: str) -> np.ndarray:
    """``nba_predict_games`` margins for ``future`` from the box fixed point on ``rows``."""
    sched = games.select(
        "game_id", "season", "date", "home_team_id", "away_team_id", pl.lit(False).alias("neutral_site")
    )
    eff = raw_game_efficiency(sched, _box_from_rows(rows))
    ratings = adjust_efficiency(eff, league_id=league_id).join(
        adjust_pace(eff, league_id=league_id), on=["season", "team_id"], how="left", validate="1:1"
    )
    fut = future.select("game_id", pl.col("home_team_id").cast(pl.Utf8), pl.col("away_team_id").cast(pl.Utf8))
    pred = nba_predict_games(fut, ratings, league_id=league_id)
    return pred.get_column("exp_margin").fill_null(get_constants(league_id).hfa).to_numpy()


def ridge_margins(rows: pl.DataFrame, future: pl.DataFrame, lam: float) -> np.ndarray:
    """Ridge-rating margins for ``future``, the ridge's own home term, as-of possessions per game."""
    rat = possession_ridge_ratings(rows, lam=lam)
    poss = rows.height / (2.0 * rows.get_column("game_id").n_unique())
    hfa = float(rat.get_column("hfa")[0])
    net = dict(zip(rat.get_column("team_id").to_list(), rat.get_column("adj_net_rtg").to_list(), strict=True))
    h = np.array([net.get(t, 0.0) for t in future.get_column("home_team_id").to_list()])
    a = np.array([net.get(t, 0.0) for t in future.get_column("away_team_id").to_list()])
    return (h - a + 2.0 * hfa) * poss / 100.0


def season_predictions(league_id: str, season: int, cache: Path) -> pl.DataFrame:
    """One row per (cutoff, future game): realized margin and every method's predicted margin."""
    rows, games = load_season(league_id, season, cache)
    start, end = games.get_column("date").min(), games.get_column("date").max()
    out = []
    cutoff = start + dt.timedelta(days=14)
    while cutoff <= end:
        past_rows = rows.filter(pl.col("date") < cutoff)
        past_games = games.filter(pl.col("date") < cutoff)
        future = games.filter(pl.col("date") >= cutoff)
        frame = {
            "season": season,
            "cutoff": cutoff,
            "game_id": future.get_column("game_id").to_list(),
            "days_ahead": [(d - cutoff).days for d in future.get_column("date").to_list()],
            "share_played": past_games.height / games.height,
            "y": (future.get_column("margin") > 0).cast(pl.Float64).to_list(),
            "margin": future.get_column("margin").to_list(),
            "box": box_margins(past_rows, past_games, future, league_id),
        }
        for lam in GRID:
            frame[f"ridge_{lam:g}"] = ridge_margins(past_rows, future, lam)
        out.append(pl.DataFrame(frame))
        cutoff += dt.timedelta(days=7)
    return pl.concat(out)


def best_sigma(margin: np.ndarray, y: np.ndarray) -> float:
    """The probit scale minimizing log loss of ``Phi(margin / sigma)`` on the grid."""
    losses = [log_loss_score(y, norm.cdf(margin / s)) for s in SIGMAS]
    return float(SIGMAS[int(np.argmin(losses))])


def score(margin: np.ndarray, y: np.ndarray, sigma: float, realized: np.ndarray) -> dict[str, float]:
    p = norm.cdf(margin / sigma)
    return {
        "log_loss": round(log_loss_score(y, p), 5),
        "brier": round(brier_score(y, p), 5),
        "margin_mae": round(float(np.abs(realized - margin).mean()), 3),
        "n": int(y.size),
    }


def paired_log_loss(hold: pl.DataFrame, a: str, sa: float, b: str, sb: float) -> dict[str, float]:
    """Mean per-prediction log-loss difference ``a - b`` with its SE clustered by game.

    A game is scored once per cutoff before it, so its predictions are not independent; the
    cluster is the game.
    """
    y = hold["y"].to_numpy()

    def ll(m: np.ndarray, s: float) -> np.ndarray:
        p = np.clip(norm.cdf(m / s), 1e-15, 1 - 1e-15)
        return -(y * np.log(p) + (1 - y) * np.log(1 - p))

    d = ll(hold[a].to_numpy(), sa) - ll(hold[b].to_numpy(), sb)
    frame = pl.DataFrame({"g": hold["game_id"], "d": d})
    mean = float(d.mean())
    sums = frame.group_by("g").agg(pl.col("d").sum().alias("s"), pl.len().alias("n"))
    se = float(np.sqrt(((sums["s"].to_numpy() - sums["n"].to_numpy() * mean) ** 2).sum()) / d.size)
    return {"diff": round(mean, 5), "se": round(se, 5), "games": sums.height}


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[1])
    ap.add_argument("--league", choices=sorted(_LEAGUE_ID), required=True)
    ap.add_argument("--train", type=int, nargs="+", required=True, help="seasons by END year")
    ap.add_argument("--holdout", type=int, nargs="+", required=True, help="seasons by END year")
    ap.add_argument("--cache", type=Path, default=Path(".cache/pro_season"))
    ap.add_argument("--out", type=Path, default=None, help="write the result json here")
    args = ap.parse_args(argv)
    if set(args.train) & set(args.holdout):
        ap.error("train and holdout seasons must be disjoint")

    league_id = _LEAGUE_ID[args.league]
    preds = {s: season_predictions(league_id, s, args.cache) for s in [*args.train, *args.holdout]}
    train = pl.concat([preds[s] for s in args.train])
    hold = pl.concat([preds[s] for s in args.holdout])
    y_tr, y_ho = train["y"].to_numpy(), hold["y"].to_numpy()
    m_tr, m_ho = train["margin"].to_numpy(), hold["margin"].to_numpy()

    methods = ["box", *[f"ridge_{lam:g}" for lam in GRID]]
    sigma = {m: best_sigma(train[m].to_numpy(), y_tr) for m in methods}
    train_ll = {m: score(train[m].to_numpy(), y_tr, sigma[m], m_tr)["log_loss"] for m in methods}
    ridge_best = min(methods[1:], key=train_ll.__getitem__)
    chosen = min(methods, key=train_ll.__getitem__)
    shipped_sd = get_constants(league_id).margin_sd
    result = {
        "league": args.league,
        "train": args.train,
        "holdout_seasons": args.holdout,
        "sigma_on_train": sigma,
        "train_log_loss": train_ll,
        "ridge_best_on_train": ridge_best,
        "chosen_on_train": chosen,
        "holdout": {
            "box_as_shipped": score(hold["box"].to_numpy(), y_ho, shipped_sd, m_ho),
            **{m: score(hold[m].to_numpy(), y_ho, sigma[m], m_ho) for m in methods},
        },
        "holdout_by_season": {
            str(s): {
                m: score(preds[s][m].to_numpy(), preds[s]["y"].to_numpy(), sigma[m], preds[s]["margin"].to_numpy())
                for m in ("box", ridge_best)
            }
            for s in args.holdout
        },
        "holdout_by_phase": {},
        "holdout_paired": {
            f"{ridge_best} - box": paired_log_loss(hold, ridge_best, sigma[ridge_best], "box", sigma["box"]),
            f"{ridge_best} - box_as_shipped": paired_log_loss(hold, ridge_best, sigma[ridge_best], "box", shipped_sd),
        },
    }
    phase = pl.when(pl.col("share_played") < 1 / 3).then(pl.lit("early")).when(pl.col("share_played") < 2 / 3)
    hold = hold.with_columns(phase.then(pl.lit("mid")).otherwise(pl.lit("late")).alias("phase"))
    for (ph,), sub in hold.group_by("phase", maintain_order=True):
        result["holdout_by_phase"][ph] = {
            m: score(sub[m].to_numpy(), sub["y"].to_numpy(), sigma[m], sub["margin"].to_numpy())
            for m in ("box", ridge_best)
        }
    text = json.dumps(result, indent=2, default=str)
    print(text)
    if args.out:
        args.out.write_text(text + "\n", encoding="utf-8", newline="\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
