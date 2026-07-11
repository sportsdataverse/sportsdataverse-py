"""Fit ELO_K / ELO_HFA against the committed results corpus (T6.4, Task 4.2).

Grid search over (k, hfa) minimizing the as-of-date game Brier score of
``mlb_team_elo``'s ``home_win_prob_elo`` column. Run offline (no network --
reads the already-committed fixture):

    uv run python dev/mlb_game_state/fit_elo.py

Paste the printed best (k, hfa) into ``ELO_K`` / ``ELO_HFA`` in
``sportsdataverse/mlb/mlb_game_state_constants.py``.
"""

from __future__ import annotations

from pathlib import Path

import polars as pl

from sportsdataverse.mlb.mlb_game_state_constants import brier_score
from sportsdataverse.mlb.mlb_team_projection import mlb_team_elo

REPO_ROOT = Path(__file__).resolve().parents[2]
FIXTURE_DIR = REPO_ROOT / "tests" / "fixtures" / "mlb_game_state"

K_GRID = [2.0, 4.0, 6.0, 8.0, 10.0, 12.0, 16.0, 20.0]
HFA_GRID = [0.0, 12.0, 24.0, 36.0, 48.0, 60.0]


def main() -> None:
    results = pl.read_parquet(FIXTURE_DIR / "results_corpus.parquet")
    home_won = results.select(
        "game_id", (pl.col("home_score") > pl.col("away_score")).cast(pl.Float64).alias("home_won")
    )

    best = None
    for k in K_GRID:
        for hfa in HFA_GRID:
            elo = mlb_team_elo(results, k=k, hfa=hfa)
            chk = elo.join(home_won, on="game_id", how="inner")
            b = brier_score(chk["home_won"].to_numpy(), chk["home_win_prob_elo"].to_numpy())
            if best is None or b < best[0]:
                best = (b, k, hfa)
            print(f"k={k:5.1f} hfa={hfa:5.1f} brier={b:.5f}")

    assert best is not None
    print(f"\nBest: k={best[1]}, hfa={best[2]}, brier={best[0]:.5f}")


if __name__ == "__main__":
    main()
