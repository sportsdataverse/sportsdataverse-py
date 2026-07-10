"""Fit sigma / HFA for the NBA pregame closed-form model on the 2023-24 backtest.

Gitignored working script (``dev/`` is not tracked). Run:

    uv run python dev/nba_prediction/fit_pregame.py

For a stratified sample of unique game dates (skipping an early-season
warmup window so as-of ratings are non-degenerate), computes as-of-date
team ratings using ONLY games strictly before that date
(``nba_team_ratings(..., as_of_date=date)`` -- the leakage boundary), predicts
each game on that date with the *current* seeded ``em_scale``-free closed
form, and collects:

* ``hfa = mean(actual_margin)`` over non-neutral games (before any HFA is
  added back -- i.e. the mean home-court edge left in the residual once
  team strength is accounted for).
* ``margin_sd = std(actual_margin - exp_margin)`` over the full residual set
  (exp_margin computed with the fitted hfa plugged back in).

Paste the printed ``hfa``/``margin_sd`` into ``LEAGUE_CONSTANTS["00"]`` in
``sportsdataverse/nba/nba_prediction_constants.py``.
"""

from __future__ import annotations

from pathlib import Path

import numpy as np
import polars as pl

from sportsdataverse.nba.nba_prediction_constants import as_of_ratings_split
from sportsdataverse.nba.nba_team_ratings import nba_team_ratings

FIXTURE_DIR = Path(__file__).resolve().parents[2] / "tests" / "fixtures" / "nba_prediction"


def _patch_loaders(results: pl.DataFrame, team_box: pl.DataFrame) -> None:
    import sportsdataverse.nba.nba_team_ratings as mod

    mod.load_nba_schedule = lambda seasons: results  # type: ignore[assignment]
    mod.load_nba_team_boxscore = lambda seasons: team_box  # type: ignore[assignment]


def fit(*, league_id: str = "00", warmup_games: int = 150, date_stride: int = 3) -> tuple[float, float]:
    results = pl.read_parquet(FIXTURE_DIR / "results_2024.parquet")
    team_box = pl.read_parquet(FIXTURE_DIR / "team_box_2024.parquet")
    _patch_loaders(results, team_box)

    dates = results.sort("date")["date"].unique(maintain_order=True).to_list()

    net_diff_scaled_list: list[float] = []
    actual_list: list[float] = []
    neutral_list: list[bool] = []

    for i, cutoff in enumerate(dates):
        prior = as_of_ratings_split(results, cutoff)
        if prior.height < warmup_games:
            continue
        if i % date_stride != 0:
            continue
        ratings = nba_team_ratings(2024, league_id=league_id, as_of_date=cutoff)
        if ratings.height == 0:
            continue
        rmap = {row["team_id"]: row for row in ratings.iter_rows(named=True)}
        avg_pace = float(ratings["adj_pace"].mean())
        day_games = results.filter(pl.col("date") == cutoff)
        for g in day_games.iter_rows(named=True):
            h, a = rmap.get(g["home_team_id"]), rmap.get(g["away_team_id"])
            if h is None or a is None:
                continue
            poss = h["adj_pace"] * a["adj_pace"] / avg_pace
            net_diff_scaled_list.append((h["adj_net_rtg"] - a["adj_net_rtg"]) * poss / 100.0)
            actual_list.append(float(g["home_score"] - g["away_score"]))
            neutral_list.append(bool(g["neutral_site"]))

    net_diff_scaled = np.array(net_diff_scaled_list)
    actual = np.array(actual_list)
    neutral = np.array(neutral_list)

    # hfa = mean home-court edge left in the residual (net_diff already accounts for
    # team strength), measured over non-neutral games only.
    non_neutral_resid = (actual - net_diff_scaled)[~neutral]
    hfa = float(np.mean(non_neutral_resid)) if non_neutral_resid.size else 0.0

    # exp_margin with the fitted hfa plugged back in (0 for neutral games) --
    # margin_sd is the residual std around THIS full prediction.
    exp_margin = net_diff_scaled + np.where(neutral, 0.0, hfa)
    margin_sd = float(np.std(actual - exp_margin))

    print(f"n_games_used={len(actual)} hfa={hfa:.4f} margin_sd={margin_sd:.4f}")
    return hfa, margin_sd


if __name__ == "__main__":
    fit()
