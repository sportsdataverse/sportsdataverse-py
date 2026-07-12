"""Build the T5.3/T5.3b PWHL xG backtest fixtures -- DE-LEAKED, held-out,
DUAL-METHOD (categorical shot-quality proxy vs real coordinate xG). Public
data-release downloads (no live gate); regenerate with:

    uv run python dev/pwhl_prediction/build_pwhl_xg_fixture.py

Leakage discipline (two blockers the first cut had, now closed; the T5.3b
coords method walks the IDENTICAL design):

1. margin_sd is fit PER METHOD on a TRAINING season set (2025) and the gate
   scores the HELD-OUT season (2026) only. Because win_prob =
   Phi(exp_margin/margin_sd) -> 0.5 (=naive) as sd grows, an IN-sample sd fit
   could never lose to naive; fitting on 2025 and reporting 2026 makes
   "beats naive" falsifiable. (Per-method because a different xG scale
   changes the exp_margin scale.)
2. The xG model (tier weights OR coordinate logistic) is fit on shots from
   STRICTLY PRIOR complete seasons (2024 for the 2025 walk; 2024+2025 for the
   2026 held-out walk) -- every training shot predates every game it scores,
   so no game sees a model fit on its own or later data. (This is a leak-free
   SUBSET of what shipped `pwhl_ratings_from_proxy(as_of_date=)` uses -- it
   additionally folds in intra-season pre-cutoff shots; the gate stays
   conservative.)

Ratings are still per-season (team strength resets) and per-as-of-date
(only games strictly before d), using the same adjust_rate_opponent +
nhl_market core the NHL spine uses. Burn-in = first BURN_IN_DATES unique
game-dates per season (PWHL seasons have few dates).
"""

from __future__ import annotations

import numpy as np
import polars as pl
from scipy.stats import norm
from sklearn.metrics import log_loss, roc_auc_score

from sportsdataverse.nhl.nhl_market import expected_goals
from sportsdataverse.nhl.nhl_prediction_constants import (
    as_of_ratings_split,
    brier_score,
    calibration_table,
    get_constants,
)
from sportsdataverse.nhl.nhl_team_ratings import adjust_rate_opponent
from sportsdataverse.pwhl.pwhl_loaders import load_pwhl_game_info, load_pwhl_pbp, load_pwhl_schedules
from sportsdataverse.pwhl.pwhl_xg_proxy import (
    fit_pwhl_coord_xg,
    fit_shot_quality_xg,
    pwhl_team_game_xg_rates,
)

FIXTURES_DIR = "tests/fixtures/pwhl_prediction"
SEASONS = (2024, 2025, 2026)
SD_FIT_SEASONS = (2025,)  # fit margin_sd here (xG model from 2024)
HELD_OUT_SEASON = 2026  # gate scores here (xG model from 2024+2025)
BURN_IN_DATES = 10
METHODS = ("quality", "coords")
_FITTERS = {"quality": fit_shot_quality_xg, "coords": fit_pwhl_coord_xg}


def _load_all() -> dict[int, tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame]]:
    out = {}
    for season in SEASONS:
        pbp, sched, info = load_pwhl_pbp(season), load_pwhl_schedules(season), load_pwhl_game_info(season)
        if not pbp.is_empty() and not sched.is_empty():
            out[season] = (pbp, sched, info)
    return out


def _prior_complete_shots(loaded: dict, season: int) -> pl.DataFrame:
    """Narrow shot rows (now incl. coordinates) from every COMPLETE season strictly before `season`."""
    narrow = [
        pbp.select(pl.lit(s).alias("season"), "game_id", "event", "shot_quality", "goal", "x_coord", "y_coord")
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


def _walk(season: int, loaded: dict, model, xg_method: str) -> tuple[pl.DataFrame, list[dict]]:
    """As-of walk over `season` with a FIXED (leak-free) xG model.

    Returns (season game_rates, per-game records with exp_margin + outcome).
    exp_margin is sd-independent; win_prob is applied later with the chosen sd.
    """
    pbp, sched, info = loaded[season]
    const = get_constants("pwhl")
    rates = pwhl_team_game_xg_rates(pbp, sched, game_info=info, xg_model=model, xg_method=xg_method)
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
    print(f"  [{xg_method}] season {season}: {len(dates)} dates, {len(records)} games")
    return rates, records


def _coord_model_report(train_shots: pl.DataFrame) -> None:
    """The coord-xG model's own internal-consistency numbers (in-sample on the train pool)."""
    model = fit_pwhl_coord_xg(train_shots)
    shots = train_shots.filter(
        (pl.col("event") == "shot") & pl.col("x_coord").is_not_null() & pl.col("y_coord").is_not_null()
    )
    y = shots["goal"].cast(pl.Int64).to_numpy()
    p = model.predict(shots).to_numpy()
    base = np.full(len(y), y.mean())
    grid = pl.DataFrame({"x_coord": [85.0, 59.0, 25.0], "y_coord": [0.0, 0.0, 0.0]})
    curve = model.predict(grid).to_list()
    print("\n=== coord-xG model internal report (fit on 2024+2025) ===")
    print(f"fit N (coord-complete shots) = {len(y)}, goal rate = {y.mean():.4f}")
    print(f"in-sample AUC = {roc_auc_score(y, p):.4f}")
    print(f"in-sample logloss = {log_loss(y, p):.4f} vs base-rate {log_loss(y, base):.4f}")
    print(f"fitted curve at 4/30/64 ft straight-on: {[round(v, 4) for v in curve]}")


def main() -> None:
    loaded = _load_all()

    # --- Training pool: shots from all seasons strictly prior to the held-out one ---
    train_shots = _prior_complete_shots(loaded, HELD_OUT_SEASON)  # 2024+2025
    train_shots.write_parquet(f"{FIXTURES_DIR}/shots_train_2024_2025.parquet")
    print(f"wrote shots_train_2024_2025.parquet: {train_shots.shape}")
    _coord_model_report(train_shots)

    held: dict[str, pl.DataFrame] = {}
    held_rates_by_method: dict[str, pl.DataFrame] = {}
    for method in METHODS:
        fitter = _FITTERS[method]
        print(f"\n--- method = {method} ---")
        # margin_sd fit on SD_FIT_SEASONS (xG model from strictly-prior seasons)
        sd_records: list[dict] = []
        for s in SD_FIT_SEASONS:
            m = fitter(_prior_complete_shots(loaded, s))  # 2024 for s=2025
            _, recs = _walk(s, loaded, m, method)
            sd_records += recs
        sd_df = pl.DataFrame(sd_records)
        y_tr, m_tr = sd_df["home_win"].to_numpy(), sd_df["exp_margin"].to_numpy()
        grid = np.arange(0.05, 3.0, 0.01)
        best_sd = float(min(grid, key=lambda sd: brier_score(y_tr, norm.cdf(m_tr / sd))))
        print(f"margin_sd fit on {SD_FIT_SEASONS} ({len(y_tr)} games): {best_sd:.4f}")
        print(f"  train Brier @ best_sd = {brier_score(y_tr, norm.cdf(m_tr / best_sd)):.4f}")

        # held-out walk with the 2024+2025-fit model + the 2025-fit sd
        held_model = fitter(train_shots)
        held_rates, held_recs = _walk(HELD_OUT_SEASON, loaded, held_model, method)
        held_df = pl.DataFrame(held_recs)
        held_df = held_df.with_columns(pl.Series("home_win_prob", norm.cdf(held_df["exp_margin"].to_numpy() / best_sd)))
        held[method] = held_df
        held_rates_by_method[method] = held_rates

        y, p = held_df["home_win"].to_numpy(), held_df["home_win_prob"].to_numpy()
        model_brier, naive_brier = brier_score(y, p), brier_score(y, np.full(len(y), 0.5))
        # per-game Brier-vs-naive diff SE (the powered-gate yardstick)
        d = (p - y) ** 2 - 0.25
        print(f"=== HELD-OUT ({HELD_OUT_SEASON}), method={method}, n={len(y)} ===")
        print(
            f"naive Brier = {naive_brier:.4f} | model Brier = {model_brier:.4f} (delta {model_brier - naive_brier:+.4f}, SE {d.std(ddof=1) / np.sqrt(len(d)):.4f})"
        )
        for nb in (3, 5):
            print(f"calibration n_bins={nb}:")
            print(calibration_table(y, p, n_bins=nb))

    # --- Merge both methods into one backtest fixture (quality keeps legacy names) ---
    q, c = held["quality"], held["coords"]
    assert q.schema["game_id"] == c.schema["game_id"]
    assert q.height == c.height, f"method walks diverged: quality {q.height} vs coords {c.height} games"
    merged = q.join(
        c.select(
            "game_id",
            pl.col("exp_margin").alias("exp_margin_coords"),
            pl.col("home_win_prob").alias("home_win_prob_coords"),
        ),
        on="game_id",
        how="inner",
    )
    assert merged.height == q.height, "game_id join dropped rows across methods"

    # game_rates fixture is scored by the DEFAULT method (coords, T5.3b)
    held_rates_by_method["coords"].write_parquet(f"{FIXTURES_DIR}/game_rates_heldout_2026.parquet")
    merged.write_parquet(f"{FIXTURES_DIR}/backtest_heldout_2026.parquet")
    print(f"\nwrote game_rates_heldout_2026.parquet: {held_rates_by_method['coords'].shape}")
    print(f"wrote backtest_heldout_2026.parquet: {merged.shape}")

    # --- Paired method comparison (same games, same outcomes) ---
    y = merged["home_win"].to_numpy()
    pq = merged["home_win_prob"].to_numpy()
    pc = merged["home_win_prob_coords"].to_numpy()
    d = (pc - y) ** 2 - (pq - y) ** 2
    print("\n=== paired coords-vs-quality Brier diff (negative = coords better) ===")
    print(f"mean diff = {d.mean():+.4f}, paired SE = {d.std(ddof=1) / np.sqrt(len(d)):.4f}, n = {len(d)}")


if __name__ == "__main__":
    main()
