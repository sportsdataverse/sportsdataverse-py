"""Build tests/fixtures/pwhl_prediction/{game_rates_2024_2026,backtest_predictions_2024_2026}.parquet
-- the T5.3 PWHL categorical-shot_quality xG-proxy backtest fixtures. Public
data-release parquet downloads (no live-API gate needed); regenerate with:

    uv run python dev/pwhl_prediction/build_pwhl_xg_fixture.py

Method: fit ONE shot-quality xG-proxy model pooled across all 3 captured
PWHL seasons (2024-2026) for stability (few hundred goals/season), then for
EACH season independently run an as-of-date walk-forward (never mixing
ratings across seasons -- team strength resets each PWHL season) using the
same adjust_rate_opponent + nhl_market core the NHL prediction spine uses.
Burn-in is the first BURN_IN_DATES unique game-dates per season (smaller
than the NHL backtest's dates[20:] -- PWHL seasons have far fewer unique
dates: 2024 spans ~40, so a 20-date burn-in would evaluate almost nothing).
"""

from __future__ import annotations

import polars as pl

from sportsdataverse.nhl.nhl_market import expected_goals, win_prob_from_margin
from sportsdataverse.nhl.nhl_prediction_constants import as_of_ratings_split, get_constants
from sportsdataverse.nhl.nhl_team_ratings import adjust_rate_opponent
from sportsdataverse.pwhl.pwhl_loaders import load_pwhl_game_info, load_pwhl_pbp, load_pwhl_schedules
from sportsdataverse.pwhl.pwhl_xg_proxy import fit_shot_quality_xg, pwhl_team_game_xg_rates

FIXTURES_DIR = "tests/fixtures/pwhl_prediction"
SEASONS = (2024, 2025, 2026)
BURN_IN_DATES = 10


def _pooled_shots() -> pl.DataFrame:
    narrow = []
    for season in SEASONS:
        pbp = load_pwhl_pbp(season)
        if pbp.is_empty():
            continue
        narrow.append(pbp.select(pl.lit(season).alias("season"), "game_id", "event", "shot_quality", "goal"))
    return pl.concat(narrow, how="vertical_relaxed")


def main() -> None:
    pooled_shots = _pooled_shots()
    shots_path = f"{FIXTURES_DIR}/shots_2024_2026.parquet"
    pooled_shots.write_parquet(shots_path)
    print(f"wrote {shots_path}: {pooled_shots.shape}")
    model = fit_shot_quality_xg(pooled_shots)
    print(f"pooled shot-quality weights: {model.weights} fallback={model.fallback_rate:.4f}")

    const = get_constants("pwhl")
    all_game_rates = []
    all_backtest_records = []

    for season in SEASONS:
        pbp = load_pwhl_pbp(season)
        sched = load_pwhl_schedules(season)
        info = load_pwhl_game_info(season)
        rates = pwhl_team_game_xg_rates(pbp, sched, game_info=info, xg_model=model)
        all_game_rates.append(rates)

        results = (
            sched.filter(pl.col("game_type") == "regular")
            .select(
                pl.col("game_id").cast(pl.Int64),
                pl.col("home_team"),
                pl.col("away_team"),
                pl.col("home_score").cast(pl.Int64),
                pl.col("away_score").cast(pl.Int64),
            )
            .join(
                info.select(
                    pl.col("game_id").cast(pl.Int64),
                    pl.col("game_date_iso")
                    .str.slice(0, 10)
                    .str.strptime(pl.Date, "%Y-%m-%d", strict=False)
                    .alias("date"),
                ).unique(subset=["game_id"]),
                on="game_id",
                how="left",
            )
            .drop_nulls("date")
        )

        dates = sorted(rates.drop_nulls("date")["date"].unique().to_list())
        if len(dates) <= BURN_IN_DATES:
            print(f"season {season}: only {len(dates)} unique dates, skipping (burn-in {BURN_IN_DATES})")
            continue
        eval_dates = dates[BURN_IN_DATES:]

        for d in eval_dates:
            as_of = as_of_ratings_split(rates, d)
            if as_of.is_empty():
                continue
            xg_adj = adjust_rate_opponent(
                as_of, for_col="xgf", against_col="xga", hfa=const.hfa, avg=const.avg_xgf, shrink_k=const.shrink_k
            )
            home_map = dict(
                zip(xg_adj["team"].to_list(), zip(xg_adj["adj_for"].to_list(), xg_adj["adj_against"].to_list()))
            )
            today = results.filter(pl.col("date") == d)
            for row in today.iter_rows(named=True):
                h = home_map.get(row["home_team"])
                a = home_map.get(row["away_team"])
                if h is None or a is None:
                    continue
                eg_home, eg_away = expected_goals(h[0], h[1], a[0], a[1], False, league="pwhl")
                exp_margin = eg_home - eg_away
                all_backtest_records.append(
                    {
                        "season": season,
                        "game_id": str(row["game_id"]),
                        "date": d,
                        "exp_margin": exp_margin,
                        "home_win_prob": win_prob_from_margin(exp_margin, league="pwhl"),
                        "home_win": 1 if row["home_score"] > row["away_score"] else 0,
                    }
                )
        print(f"season {season}: {len(dates)} unique dates, evaluated {len(eval_dates)}")

    game_rates_out = pl.concat(all_game_rates, how="vertical_relaxed")
    game_rates_path = f"{FIXTURES_DIR}/game_rates_2024_2026.parquet"
    game_rates_out.write_parquet(game_rates_path)
    print(f"wrote {game_rates_path}: {game_rates_out.shape}")

    backtest_out = pl.DataFrame(all_backtest_records)
    backtest_path = f"{FIXTURES_DIR}/backtest_predictions_2024_2026.parquet"
    backtest_out.write_parquet(backtest_path)
    print(f"wrote {backtest_path}: {backtest_out.shape}")

    import numpy as np

    from sportsdataverse.nhl.nhl_prediction_constants import brier_score, calibration_table

    y = backtest_out["home_win"].to_numpy()
    p = backtest_out["home_win_prob"].to_numpy()
    model_brier = brier_score(y, p)
    naive_brier = brier_score(y, np.full(len(y), 0.5))
    print(f"\nn_games_evaluated = {len(y)}")
    print(f"naive Brier (p=0.5) = {naive_brier:.4f}")
    print(f"model Brier         = {model_brier:.4f}")
    cal = calibration_table(y, p, n_bins=5)
    print(cal)


if __name__ == "__main__":
    main()
