"""Fit ENVIRONMENT_FG_COEF (Task 3.2; network required).

Logistic fit of field_goal_made on [wind, temp-60, altitude_kft] with the
shipped fg_model's logit(base_make_prob) as a fixed offset (coefficient 1).
scipy.optimize only (no sklearn).  Paste the printed dict into
nfl_scheme_constants.ENVIRONMENT_FG_COEF.
"""

import numpy as np
import polars as pl
from scipy.optimize import minimize
from scipy.special import expit, logit

from sportsdataverse.nfl.nfl_kicker_rating import _with_era_and_roof
from sportsdataverse.nfl.nfl_fourth_down import fg_make_probability
from sportsdataverse.nfl.nfl_loaders import load_nfl_pbp
from sportsdataverse.nfl.nfl_scheme_constants import STADIUM_ALTITUDE

SEASONS = list(range(2010, 2024))
TEMP_BASELINE = 60.0


def main() -> None:
    pbp = load_nfl_pbp(SEASONS).filter(pl.col("play_type") == "field_goal")
    df = _with_era_and_roof(
        pbp.select("season", "yardline_100", "roof", "temp", "wind", "home_team", "field_goal_result").drop_nulls(
            ["yardline_100", "field_goal_result"]
        )
    )
    base = fg_make_probability(
        df["yardline_100"].to_numpy().astype(float),
        df["fg_roof"].to_numpy().astype(float),
        df.select("era0", "era1", "era2", "era3", "era4").to_numpy().astype(float),
    )
    keep = (base > 1e-6) & (base < 1 - 1e-6)
    df = df.filter(pl.Series(keep))
    offset = logit(np.clip(base[keep], 1e-6, 1 - 1e-6))
    y = (df["field_goal_result"] == "made").cast(pl.Int64).to_numpy().astype(float)

    indoor = df["roof"].is_in(["dome", "closed"]).fill_null(False).to_numpy()
    wind = np.where(indoor, 0.0, df["wind"].fill_null(0.0).to_numpy().astype(float))
    temp_raw = df["temp"].fill_null(TEMP_BASELINE).to_numpy().astype(float)
    temp = np.where(indoor, TEMP_BASELINE, temp_raw) - TEMP_BASELINE
    alt = df["home_team"].replace_strict(STADIUM_ALTITUDE, default=0.0, return_dtype=pl.Float64).to_numpy() / 1000.0
    w = np.column_stack([wind, temp, alt])

    def nll(theta: np.ndarray) -> float:
        p = np.clip(expit(offset + w @ theta), 1e-9, 1 - 1e-9)
        return float(-np.mean(y * np.log(p) + (1 - y) * np.log(1 - p)))

    res = minimize(nll, x0=np.zeros(3), method="BFGS")
    print("n attempts:", df.height, "converged:", res.success)
    print("nll null:", nll(np.zeros(3)), "nll fit:", float(res.fun))
    print(
        {
            "wind": float(res.x[0]),
            "temp": float(res.x[1]),
            "altitude_kft": float(res.x[2]),
            "temp_baseline": TEMP_BASELINE,
        }
    )


if __name__ == "__main__":
    main()
