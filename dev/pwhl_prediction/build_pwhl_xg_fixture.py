"""Build the T5.3 PWHL categorical-shot_quality xG-proxy backtest fixtures --
DE-LEAKED, held-out design. Public data-release downloads (no live gate);
regenerate with:

    uv run python dev/pwhl_prediction/build_pwhl_xg_fixture.py

Leakage discipline (two blockers the first cut had, now closed):

1. margin_sd is fit on a TRAINING season set (2025) and the gate scores the
   HELD-OUT season (2026) only. Because win_prob = Phi(exp_margin/margin_sd)
   -> 0.5 (=naive) as sd grows, an IN-sample sd fit could never lose to naive;
   fitting on 2025 and reporting 2026 makes "beats naive" falsifiable.
2. The tier weights are fit on shots from STRICTLY PRIOR complete seasons
   (2024 for the 2025 walk; 2024+2025 for the 2026 held-out walk) -- every
   training shot predates every game it scores, so no game sees weights fit on
   its own or later data. (This is a leak-free SUBSET of what shipped
   `pwhl_ratings_from_proxy(as_of_date=)` uses -- it additionally folds in
   intra-season pre-cutoff shots; the gate stays conservative.)

Ratings are still per-season (team strength resets) and per-as-of-date
(only games strictly before d), using the same adjust_rate_opponent +
nhl_market core the NHL spine uses. Burn-in = first BURN_IN_DATES unique
game-dates per season (PWHL seasons have few dates).
"""

from __future__ import annotations

import numpy as np
import polars as pl

from sportsdataverse.nhl.nhl_market import expected_goals
from sportsdataverse.nhl.nhl_prediction_constants import (
    as_of_ratings_split,
    brier_score,
    calibration_table,
    get_constants,
)
from sportsdataverse.nhl.nhl_team_ratings import adjust_rate_opponent
from sportsdataverse.pwhl.pwhl_loaders import load_pwhl_game_info, load_pwhl_pbp, load_pwhl_schedules
from sportsdataverse.pwhl.pwhl_xg_proxy import fit_shot_quality_xg, pwhl_team_game_xg_rates
from scipy.stats import norm

FIXTURES_DIR = "tests/fixtures/pwhl_prediction"
SEASONS = (2024, 2025, 2026)
SD_FIT_SEASONS = (2025,)  # fit margin_sd here (tier weights from 2024)
HELD_OUT_SEASON = 2026  # gate scores here (tier weights from 2024+2025)
BURN_IN_DATES = 10


def _load_all() -> dict[int, tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame]]:
    out = {}
    for season in SEASONS:
        pbp, sched, info = load_pwhl_pbp(season), load_pwhl_schedules(season), load_pwhl_game_info(season)
        if not pbp.is_empty() and not sched.is_empty():
            out[season] = (pbp, sched, info)
    return out


def _prior_complete_shots(loaded: dict, season: int) -> pl.DataFrame:
    """Narrow shot rows from every COMPLETE season strictly before `season`."""
    narrow = [
        pbp.select(pl.lit(s).alias("season"), "game_id", "event", "shot_quality", "goal")
        for s, (pbp, _sc, _in) in loaded.items()
        if s < season
    ]
    return pl.concat(narrow, how="vertical_relaxed") if narrow else pl.DataFrame()


def _season_results(sched: pl.DataFrame, info: pl.DataFrame) -> pl.DataFrame:
    dates = info.select(
        pl.col("game_id").cast(pl.Int64),
        pl.col("game_date_iso").str.slice(0, 10).str.strptime(pl.Date, "%Y-%m-%d", strict=False).alias("date"),
    ).unique(subset=["game_id"])
    return (
        sched.filter(pl.col("game_type") == "regular")
        .select(
            pl.col("game_id").cast(pl.Int64),
            "home_team",
            "away_team",
            pl.col("home_score").cast(pl.Int64),
            pl.col("away_score").cast(pl.Int64),
        )
        .join(dates, on="game_id", how="left")
        .drop_nulls("date")
    )


def _walk(season: int, loaded: dict, model) -> tuple[pl.DataFrame, list[dict]]:
    """As-of walk over `season` with a FIXED (leak-free) tier-weight model.

    Returns (season game_rates, per-game records with exp_margin + outcome).
    exp_margin is sd-independent; win_prob is applied later with the chosen sd.
    """
    pbp, sched, info = loaded[season]
    const = get_constants("pwhl")
    rates = pwhl_team_game_xg_rates(pbp, sched, game_info=info, xg_model=model)
    results = _season_results(sched, info)

    dates = sorted(rates.drop_nulls("date")["date"].unique().to_list())
    records: list[dict] = []
    for d in dates[BURN_IN_DATES:]:
        as_of = as_of_ratings_split(rates, d)
        if as_of.is_empty():
            continue
        xg_adj = adjust_rate_opponent(
            as_of, for_col="xgf", against_col="xga", hfa=const.hfa, avg=const.avg_xgf, shrink_k=const.shrink_k
        )
        home_map = dict(
            zip(xg_adj["team"].to_list(), zip(xg_adj["adj_for"].to_list(), xg_adj["adj_against"].to_list()))
        )
        for row in results.filter(pl.col("date") == d).iter_rows(named=True):
            h, a = home_map.get(row["home_team"]), home_map.get(row["away_team"])
            if h is None or a is None:
                continue
            eg_home, eg_away = expected_goals(h[0], h[1], a[0], a[1], False, league="pwhl")
            records.append(
                {
                    "season": season,
                    "game_id": str(row["game_id"]),
                    "date": d,
                    "exp_margin": eg_home - eg_away,
                    "home_win": 1 if row["home_score"] > row["away_score"] else 0,
                }
            )
    print(f"season {season}: {len(dates)} dates, evaluated {len(dates[BURN_IN_DATES:])}, {len(records)} games")
    return rates, records


def main() -> None:
    loaded = _load_all()

    # --- Training: fit margin_sd on SD_FIT_SEASONS (tier weights from prior seasons) ---
    train_shots = _prior_complete_shots(loaded, HELD_OUT_SEASON)  # 2024+2025 -> also the held-out training pool
    train_shots.write_parquet(f"{FIXTURES_DIR}/shots_train_2024_2025.parquet")
    print(f"wrote shots_train_2024_2025.parquet: {train_shots.shape}")

    sd_records: list[dict] = []
    for s in SD_FIT_SEASONS:
        m = fit_shot_quality_xg(_prior_complete_shots(loaded, s))  # 2024 for s=2025
        _, recs = _walk(s, loaded, m)
        sd_records += recs
    sd_df = pl.DataFrame(sd_records)
    y_tr, m_tr = sd_df["home_win"].to_numpy(), sd_df["exp_margin"].to_numpy()
    grid = np.arange(0.05, 3.0, 0.01)
    best_sd = float(min(grid, key=lambda sd: brier_score(y_tr, norm.cdf(m_tr / sd))))
    print(f"\nmargin_sd fit on {SD_FIT_SEASONS} ({len(y_tr)} games): {best_sd:.4f}")
    print(
        f"  train Brier @ best_sd = {brier_score(y_tr, norm.cdf(m_tr / best_sd)):.4f} vs naive {brier_score(y_tr, np.full(len(y_tr), 0.5)):.4f}"
    )

    # --- Held-out: score HELD_OUT_SEASON with tier weights from all prior seasons + the fit sd ---
    held_model = fit_shot_quality_xg(train_shots)  # 2024+2025
    print(f"held-out tier weights (2024+2025): {held_model.weights} fallback={held_model.fallback_rate:.4f}")
    held_rates, held_recs = _walk(HELD_OUT_SEASON, loaded, held_model)
    held_df = pl.DataFrame(held_recs)
    held_df = held_df.with_columns(pl.Series("home_win_prob", norm.cdf(held_df["exp_margin"].to_numpy() / best_sd)))

    held_rates.write_parquet(f"{FIXTURES_DIR}/game_rates_heldout_2026.parquet")
    held_df.write_parquet(f"{FIXTURES_DIR}/backtest_heldout_2026.parquet")
    print(f"wrote game_rates_heldout_2026.parquet: {held_rates.shape}")
    print(f"wrote backtest_heldout_2026.parquet: {held_df.shape}")

    # --- Held-out report (the honest, out-of-sample number) ---
    y, p = held_df["home_win"].to_numpy(), held_df["home_win_prob"].to_numpy()
    model_brier, naive_brier = brier_score(y, p), brier_score(y, np.full(len(y), 0.5))
    print(f"\n=== HELD-OUT ({HELD_OUT_SEASON}), n={len(y)} ===")
    print(f"naive Brier (p=0.5) = {naive_brier:.4f}")
    print(f"held-out model Brier = {model_brier:.4f}  (delta {model_brier - naive_brier:+.4f})")
    for nb in (3, 5):
        print(f"calibration n_bins={nb}:")
        print(calibration_table(y, p, n_bins=nb))


if __name__ == "__main__":
    main()
