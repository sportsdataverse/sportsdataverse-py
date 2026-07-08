"""Fit PACE_CONSTANTS (expected-plays OLS) on 2021-2023 (Task 2.2).

Reads the committed fixture pbp (tests/fixtures/nfl_scheme) so the fit is
reproducible; schedules (total_line) come from load_nfl_schedule.
Paste the printed dict into nfl_scheme_constants.PACE_CONSTANTS.
"""

from pathlib import Path

import numpy as np
import polars as pl

from sportsdataverse.nfl.nfl_gamescript import team_game_pace
from sportsdataverse.nfl.nfl_loaders import load_nfl_schedule

FIXTURES = Path(__file__).resolve().parents[2] / "tests" / "fixtures" / "nfl_scheme"


def main() -> None:
    pbp = pl.read_parquet(FIXTURES / "pbp_2021_2023_slice.parquet")
    pace = team_game_pace(pbp)
    sched = load_nfl_schedule([2021, 2022, 2023]).select(pl.col("game_id").cast(pl.Utf8), "total_line")
    opp = pace.select("game_id", "posteam", "neutral_sec_per_play").rename(
        {"posteam": "opp_team", "neutral_sec_per_play": "opp_neutral_sec_per_play"}
    )
    d = (
        pace.join(sched, on="game_id", how="left")
        .join(opp, on="game_id", how="inner")
        .filter(pl.col("posteam") != pl.col("opp_team"))
        .drop_nulls(["neutral_sec_per_play", "opp_neutral_sec_per_play", "total_line"])
    )
    x = np.column_stack(
        [
            np.ones(d.height),
            d["neutral_sec_per_play"].to_numpy(),
            d["opp_neutral_sec_per_play"].to_numpy(),
            d["total_line"].to_numpy(),
        ]
    ).astype(float)
    yv = d["off_plays"].to_numpy().astype(float)
    coef, *_ = np.linalg.lstsq(x, yv, rcond=None)
    pred = x @ coef
    print("n team-games:", d.height)
    print("fit MAE:", float(np.mean(np.abs(pred - yv))))
    print(
        {
            "intercept": float(coef[0]),
            "b_pace": float(coef[1]),
            "b_opp_pace": float(coef[2]),
            "b_total": float(coef[3]),
        }
    )


if __name__ == "__main__":
    main()
