"""Build the Task 3.4 in-game WP calibration fixtures from the 2023 held-out
season (training was on 2022 -- see train_in_game_wp.py).

Run once (downloads the full 2023 season pbp; no live-API gate needed):

    uv run python dev/nhl_prediction/build_in_game_wp_calibration_fixture.py

Writes:
  tests/fixtures/nhl_prediction/in_game_wp_calibration_2023.parquet
    (bin_mid, mean_pred, mean_actual, n -- from nhl_prediction_constants.calibration_table)
  tests/fixtures/nhl_prediction/in_game_wp_pulled_goalie_2023.parquet
    (mean_pred, mean_actual, n -- for the subset of plays with either goalie pulled)
"""

from __future__ import annotations

import polars as pl

from sportsdataverse.nhl.nhl_loaders import load_nhl_pbp_full, load_nhl_schedules
from sportsdataverse.nhl.nhl_market import expected_goals, nhl_in_game_win_prob, win_prob_from_margin
from sportsdataverse.nhl.nhl_prediction_constants import as_of_ratings_split, calibration_table, get_constants
from sportsdataverse.nhl.nhl_team_ratings import adjust_rate_opponent, team_game_xg_rates

SEASON = 2023
FIXTURES_DIR = "tests/fixtures/nhl_prediction"


def build_pregame_probs(pbp: pl.DataFrame) -> dict[str, float]:
    schedule = load_nhl_schedules([SEASON])
    sched = schedule.filter(pl.col("game_type") == "R").select(
        pl.col("game_id"),
        pl.col("season"),
        pl.col("game_date").str.strptime(pl.Date, "%Y-%m-%d", strict=False).alias("date"),
        pl.col("home_team_abbr").alias("home_abbr"),
        pl.col("away_team_abbr").alias("away_abbr"),
        pl.lit(False).alias("neutral_site"),
    )
    rates = team_game_xg_rates(pbp, sched)
    const = get_constants("nhl")
    dates = sorted(rates["date"].unique().to_list())
    eval_dates = dates[20:]
    game_lookup = sched.with_columns(pl.col("game_id").cast(pl.Int64).cast(pl.Utf8))

    probs: dict[str, float] = {}
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
        today = game_lookup.filter(pl.col("date") == d)
        for row in today.iter_rows(named=True):
            h = home_map.get(row["home_abbr"])
            a = home_map.get(row["away_abbr"])
            if h is None or a is None:
                continue
            eg_home, eg_away = expected_goals(h[0], h[1], a[0], a[1], False, league="nhl")
            probs[row["game_id"]] = win_prob_from_margin(eg_home - eg_away, league="nhl")
    return probs


def main() -> None:
    print(f"Loading {SEASON} pbp...")
    pbp = load_nhl_pbp_full([SEASON])
    pbp = pbp.filter(pl.col("game_id").is_not_null())
    pbp = pbp.with_columns(pl.col("game_id").cast(pl.Int64).cast(pl.Utf8).alias("game_id_str"))

    print("Building as-of pregame probabilities...")
    pregame_probs = build_pregame_probs(pbp)
    print(f"  -> {len(pregame_probs)} games")

    goals = (
        pbp.filter(pl.col("event_type") == "GOAL").group_by(["game_id_str", "event_team_abbr"]).agg(pl.len().alias("g"))
    )
    home_away = pbp.select("game_id_str", "home_abbr", "away_abbr").unique(subset=["game_id_str"])
    home_goals = goals.rename({"event_team_abbr": "home_abbr", "g": "home_g"})
    away_goals = goals.rename({"event_team_abbr": "away_abbr", "g": "away_g"})
    labels = (
        home_away.join(home_goals, on=["game_id_str", "home_abbr"], how="left")
        .join(away_goals, on=["game_id_str", "away_abbr"], how="left")
        .with_columns(pl.col("home_g").fill_null(0), pl.col("away_g").fill_null(0))
        .with_columns((pl.col("home_g") > pl.col("away_g")).cast(pl.Int8).alias("home_win"))
        .select("game_id_str", "home_win")
    )

    print("Scoring in-game WP for every play...")
    frames = []
    for gid, sub in pbp.group_by("game_id_str"):
        game_id = gid[0] if isinstance(gid, tuple) else gid
        p = pregame_probs.get(game_id)
        if p is None:
            continue
        wp = nhl_in_game_win_prob(sub, pregame_home_prob=p)
        pulled = (sub["home_goalie_in"].fill_null(1) == 0) | (sub["away_goalie_in"].fill_null(1) == 0)
        frames.append(
            pl.DataFrame(
                {
                    "game_id_str": [game_id] * sub.height,
                    "home_win_prob": wp["home_win_prob"].to_list(),
                    "pulled": pulled.to_list(),
                }
            )
        )
    df = (
        pl.concat(frames, how="vertical_relaxed")
        .join(labels, on="game_id_str", how="left")
        .drop_nulls(subset=["home_win"])
    )
    print(f"  -> {df.height} plays across {df['game_id_str'].n_unique()} games")

    overall_cal = calibration_table(df["home_win"].to_numpy(), df["home_win_prob"].to_numpy(), n_bins=10)
    overall_cal.write_parquet(f"{FIXTURES_DIR}/in_game_wp_calibration_2023.parquet")
    print("Overall calibration:")
    print(overall_cal)

    pulled_df = df.filter(pl.col("pulled"))
    pulled_cal = pulled_df.select(
        pl.col("home_win_prob").mean().alias("mean_pred"),
        pl.col("home_win").mean().alias("mean_actual"),
        pl.len().alias("n"),
    )
    pulled_cal.write_parquet(f"{FIXTURES_DIR}/in_game_wp_pulled_goalie_2023.parquet")
    print("Pulled-goalie subset calibration:")
    print(pulled_cal)


if __name__ == "__main__":
    main()
