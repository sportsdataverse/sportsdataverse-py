"""Fit prop_kappa/pos_priors for LEAGUE_CONSTANTS["nhl"] from 2024 skater
boxscores via a method-of-moments variance decomposition (Task 4.2).

``load_nhl_skater_boxscores`` only publishes seasons >= 2024 (season 2024 ==
the 2023-24 season, per sdv-py's season-encoding convention -- confirmed the
source ``season`` column reports ``20232024``), so this fit -- and the
Task 4.2 backtest -- use season 2024 while ratings/market (Phases 1-3) stay
on the 2023 corpus. Documented in the fixtures README.

Run once (downloads the full 2024 season skater boxscores; no live-API gate
needed):

    uv run python dev/nhl_prediction/fit_props.py

Prints the fitted values -- paste them into
``sportsdataverse/nhl/nhl_prediction_constants.py``'s ``LEAGUE_CONSTANTS["nhl"]``.
"""

from __future__ import annotations

import numpy as np
import polars as pl

from sportsdataverse.nhl.nhl_loaders import load_nhl_skater_boxscores

SEASON = 2024
STAT_COLUMN = {"shots": "shots_on_goal", "points": "points"}


def method_of_moments_kappa(per_player: pl.DataFrame, col: str) -> tuple[float, float, float]:
    """Between-player vs within-player variance decomposition -> EB kappa.

    For a Poisson-ish per-game count stat, the observed per-player mean rate
    has variance = (within-player variance / games) + between-player
    variance. Solving for kappa (games-equivalent shrink strength) via the
    ratio of within-to-between variance is the standard method-of-moments EB
    estimator (James-Stein family).
    """
    grand_mean = float(per_player[col].mean())
    weights = per_player["games"].to_numpy().astype(float)
    means = per_player[col].to_numpy().astype(float)
    between_var = float(np.average((means - grand_mean) ** 2, weights=weights))
    # within-player variance: Poisson approx, variance ~= mean (count data)
    within_var = grand_mean
    avg_games = float(np.mean(weights))
    if between_var <= 0:
        return grand_mean, 999.0, avg_games
    kappa = within_var / between_var
    return grand_mean, float(max(kappa, 1.0)), avg_games


def main() -> None:
    print(f"Loading season {SEASON} skater boxscores...")
    box = load_nhl_skater_boxscores([SEASON])
    box = box.filter(pl.col("position") != "G").with_columns(
        pl.when(pl.col("position") == "D").then(pl.lit("D")).otherwise(pl.lit("F")).alias("pos_bucket")
    )
    print(f"  -> {box.height} skater-game rows")

    results = {}
    for stat, col in STAT_COLUMN.items():
        results[stat] = {}
        for pos in ("F", "D"):
            sub = box.filter(pl.col("pos_bucket") == pos)
            per_player = sub.group_by("player_id").agg(pl.col(col).mean().alias(col), pl.len().alias("games"))
            prior, kappa, avg_games = method_of_moments_kappa(per_player, col)
            results[stat][pos] = {"prior": prior, "kappa": kappa, "avg_games": avg_games}
            print(f"{stat} / {pos}: prior={prior:.4f} kappa={kappa:.4f} (avg games/player={avg_games:.1f})")

    # A single kappa per stat family (not per position) is used in
    # LEAGUE_CONSTANTS; average the two position-level kappas, weighted by
    # sample size, as the single fitted value.
    print("\nFitted values for LEAGUE_CONSTANTS['nhl']:")
    for stat in STAT_COLUMN:
        kappa_f = results[stat]["F"]["kappa"]
        kappa_d = results[stat]["D"]["kappa"]
        combined_kappa = float(np.mean([kappa_f, kappa_d]))
        print(f"  prop_kappa[{stat!r}] = {combined_kappa:.4f}")
        print(
            f"  pos_priors[{stat!r}] = {{'F': {results[stat]['F']['prior']:.4f}, 'D': {results[stat]['D']['prior']:.4f}}}"
        )


if __name__ == "__main__":
    main()
