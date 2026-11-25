"""Fit HFA / margin_sd / total_scale / avg_xgf / avg_total_goals for LEAGUE_CONSTANTS["nhl"] by walking the
2023 backtest with strict as-of-date ratings (Task 2.3).

Labels are settled scores (``_settled_scores``): the pbp logs every scoring shootout attempt as a GOAL, but a
shootout settles as one goal to its winner. avg_total_goals is the mean settled total of the three most
recent complete regular seasons before the season being predicted (SEED_SEASONS); refit it once a season,
after the regular season closes, never from an in-progress season. The run also rewrites
``tests/fixtures/nhl_prediction/settled_totals_2024_2026.parquet``, which pins that value.

Run once (downloads the 2022-23 .. 2025-26 pbp + schedules; no live-API gate needed):

    uv run python tools/models/nhl_prediction/fit_pregame.py

Prints the fitted values -- paste them into
``sportsdataverse/nhl/nhl_prediction_constants.py``'s ``LEAGUE_CONSTANTS["nhl"]``.
"""

from __future__ import annotations

from pathlib import Path

import numpy as np
import polars as pl
from scipy.optimize import minimize
from scipy.stats import norm

from sportsdataverse.nhl.nhl_loaders import load_nhl_pbp_full, load_nhl_schedules
from sportsdataverse.nhl.nhl_market import expected_goals
from sportsdataverse.nhl.nhl_prediction_constants import (
    _settled_scores,
    as_of_ratings_split,
    brier_score,
    get_constants,
)
from sportsdataverse.nhl.nhl_team_ratings import adjust_rate_opponent, team_game_xg_rates

SEASON = 2023
# avg_xgf is the mean of each season's EV xG-for per team-game, 2022-23 .. 2025-26.
AVG_XGF_SEASONS = (2023, 2024, 2025, 2026)
# avg_total_goals is the mean settled total of the last three complete regular seasons before the one being
# predicted (2026-27 here): 2023-24 .. 2025-26.
SEED_SEASONS = (2024, 2025, 2026)
SEED_FIXTURE = (
    Path(__file__).resolve().parents[3] / "tests" / "fixtures" / "nhl_prediction" / "settled_totals_2024_2026.parquet"
)


def build_game_rates(season: int = SEASON) -> tuple[pl.DataFrame, pl.DataFrame]:
    pbp = load_nhl_pbp_full([season])
    schedule = load_nhl_schedules([season])
    sched = schedule.filter(pl.col("game_type") == "R").select(
        pl.col("game_id"),
        pl.col("season"),
        pl.col("home_score").cast(pl.Int64, strict=False),
        pl.col("away_score").cast(pl.Int64, strict=False),
        pl.col("game_date").str.strptime(pl.Date, "%Y-%m-%d", strict=False).alias("date"),
        pl.col("home_team_abbr").alias("home_abbr"),
        pl.col("away_team_abbr").alias("away_abbr"),
        pl.lit(False).alias("neutral_site"),
    )
    rates = team_game_xg_rates(pbp, sched)

    # Realized goals for the fitting *labels* -- settled scores from the pbp's own GOAL events, never
    # load_nhl_schedules' home_score/away_score: those are a placeholder constant for every season <= 2023
    # (e.g. every single 2022-23 game reporting "2-3"); see the fixtures README +
    # nhl_team_ratings.team_game_xg_rates note. _settled_scores counts a shootout as one goal to its
    # winner; counting every shootout GOAL event added 0.06-0.11 goals a game to the total.
    settled = _settled_scores(pbp).with_columns(pl.col("game_id").cast(pl.Utf8))
    sched = sched.with_columns(pl.col("game_id").cast(pl.Int64).cast(pl.Utf8))
    sched = sched.join(settled, on="game_id", how="left").with_columns(
        pl.col("home_goals").is_not_null().alias("has_pbp"),
        pl.col("home_goals").fill_null(0).cast(pl.Int64),
        pl.col("away_goals").fill_null(0).cast(pl.Int64),
    )
    return rates, sched


def main() -> None:
    const = get_constants("nhl")
    rates, sched = build_game_rates()

    dates = sorted(rates["date"].unique().to_list())
    # Skip the first ~20 calendar dates so every team has a handful of games
    # before we ask for an as-of rating (early-season ratings are pure prior).
    eval_dates = dates[20:]

    records = []
    for d in eval_dates:
        as_of = as_of_ratings_split(rates, d)
        if as_of.is_empty():
            continue
        xg_adj = adjust_rate_opponent(
            as_of, for_col="xgf", against_col="xga", hfa=const.hfa, avg=const.avg_xgf, shrink_k=const.shrink_k
        )
        today_games = sched.filter(pl.col("date") == d)
        home_map = dict(
            zip(xg_adj["team"].to_list(), zip(xg_adj["adj_for"].to_list(), xg_adj["adj_against"].to_list()))
        )
        for row in today_games.iter_rows(named=True):
            h = home_map.get(row["home_abbr"])
            a = home_map.get(row["away_abbr"])
            if h is None or a is None:
                continue
            eg_home, eg_away = expected_goals(h[0], h[1], a[0], a[1], False, league="nhl")
            records.append(
                {
                    "exp_margin": eg_home - eg_away,
                    "exp_total": eg_home + eg_away,
                    "actual_margin": row["home_goals"] - row["away_goals"],
                    "actual_total": row["home_goals"] + row["away_goals"],
                    "home_win": 1 if row["home_goals"] > row["away_goals"] else 0,
                }
            )

    df = pl.DataFrame(records)
    print(f"Evaluated {df.height} games (seed hfa={const.hfa}, avg_xgf={const.avg_xgf})")

    # hfa and margin_sd are fitted JOINTLY by Brier of Phi((exp_margin_without_hfa + hfa) / margin_sd).
    # Both live on exp_margin's own compressed, shrunk-rating scale. Until 2026-11-13 hfa was the
    # mean real-goal margin residual (~0.16 goals) while margin_sd was fitted on the compressed
    # scale, so their ratio overstated home ice: mean home win prob 0.571 against 0.519 actual.
    y_win = df["home_win"].to_numpy()
    margin_no_hfa = df["exp_margin"].to_numpy() - const.hfa  # expected_goals() bakes the seed hfa in

    def brier_for(params: np.ndarray) -> float:
        return brier_score(y_win, norm.cdf((margin_no_hfa + params[0]) / params[1]))

    result = minimize(brier_for, x0=[const.hfa, const.margin_sd], bounds=[(-1.0, 1.0), (0.05, 6.0)])
    fitted_hfa, margin_sd = float(result.x[0]), float(result.x[1])

    # Diagnostic only: the real-goal-scale home margin residual and SD the old fit used.
    hfa_residual = float((df["actual_margin"] - df["exp_margin"]).mean())
    resid_sd = float((df["actual_margin"] - (df["exp_margin"] + hfa_residual)).std())

    # total_scale: OLS slope of realized total on exp_total (through a free intercept, slope only reported).
    x = df["exp_total"].to_numpy()
    y = df["actual_total"].to_numpy()
    slope = float(np.cov(x, y, bias=True)[0, 1] / np.var(x))

    print(f"hfa (joint Brier fit)           = {fitted_hfa:.4f}  <- write this into LEAGUE_CONSTANTS")
    print(f"margin_sd (joint Brier fit)     = {margin_sd:.4f}  <- write this into LEAGUE_CONSTANTS")
    print(
        f"mean home win prob / actual     = {float(norm.cdf((margin_no_hfa + fitted_hfa) / margin_sd).mean()):.4f}"
        f" / {float(y_win.mean()):.4f}"
    )
    print(f"diagnostic: real-goal home margin residual = {hfa_residual:.4f}, residual std = {resid_sd:.4f}")
    built = {SEASON: (rates, sched)} | {s: build_game_rates(s) for s in set(AVG_XGF_SEASONS + SEED_SEASONS) - {SEASON}}
    xgf = [float(built[s][0]["xgf"].mean()) for s in AVG_XGF_SEASONS]
    # avg_xgf ships at 2 dp, so that is the value to copy; the 4 dp mean is a diagnostic.
    print(f"avg_xgf (mean EV xG-for per team-game) = {np.mean(xgf):.2f}  <- write this into LEAGUE_CONSTANTS")
    print(f"  mean {np.mean(xgf):.4f}; per season {AVG_XGF_SEASONS}: " + " / ".join(f"{v:.3f}" for v in xgf))
    print(f"total_scale (OLS slope)         = {slope:.4f}  <- write this into LEAGUE_CONSTANTS")
    # predict_total stretches the raw total around 2 * avg_xgf (its own mean) and adds back
    # avg_total_goals, so that pair is the OLS intercept. The mean raw total should sit on 2 * avg_xgf.
    print(f"mean settled total, walked {SEASON} games = {float(y.mean()):.4f}  (reference)")
    seed = _seed_total(built)
    print(f"avg_total_goals (mean settled total {SEED_SEASONS}) = {seed:.2f}  <- write this into LEAGUE_CONSTANTS")
    print(
        f"mean raw total                  = {float(x.mean()):.4f}  (should be ~2 * avg_xgf = {2 * const.avg_xgf:.2f})"
    )
    print(f"naive baseline Brier (p=0.5)    = {brier_score(df['home_win'].to_numpy(), np.full(df.height, 0.5)):.4f}")
    print(f"fitted-model Brier              = {brier_for(result.x):.4f}")


def _seed_total(built: dict[int, tuple[pl.DataFrame, pl.DataFrame]]) -> float:
    """Mean settled total over SEED_SEASONS' regular-season games that have pbp; writes the pin fixture."""
    games = pl.concat(
        [
            built[s][1].filter(pl.col("has_pbp")).with_columns(pl.lit(s, dtype=pl.Int64).alias("season"))
            for s in SEED_SEASONS
        ]
    ).select(pl.col("game_id").cast(pl.Int64), "season", "home_goals", "away_goals", "home_score", "away_score")
    for (season,), g in games.group_by("season", maintain_order=True):
        total = g["home_goals"] + g["away_goals"]
        agree = ((g["home_goals"] == g["home_score"]) & (g["away_goals"] == g["away_score"])).mean()
        print(
            f"  {season}: {g.height} games, mean settled total {total.mean():.3f}; matches the schedule score {agree:.4f}"
        )
    games.sort("game_id").write_parquet(SEED_FIXTURE)
    by_season = games.group_by("season").agg((pl.col("home_goals") + pl.col("away_goals")).mean().alias("t"))
    return float(by_season["t"].mean())


if __name__ == "__main__":
    main()
