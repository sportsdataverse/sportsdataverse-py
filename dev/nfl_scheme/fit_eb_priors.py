"""Fit EB_PRIOR pseudo-counts K_fg + K_pressure (Tasks 3.3 / 5.2; offline).

Reads the committed fixtures (tests/fixtures/nfl_scheme).
- K_fg: split-half within kicker-season — attempts alternately assigned to
  halves; K minimizes MSE(shrunk half-1 fgoe_per_att, raw half-2 fgoe_per_att).
- K_pressure: season-to-season — shrunk 2021/2022 adjusted pressure rates
  predict the next season's raw rate; K (dropback scale) minimizes MSE,
  averaged over the allowed + generated sides.
Paste the printed values into nfl_scheme_constants.EB_PRIOR.
"""

from pathlib import Path

import numpy as np
import polars as pl

from sportsdataverse.nfl.nfl_kicker_rating import env_adjusted_make_prob
from sportsdataverse.nfl.nfl_line_grades import adjust_pressure_pairs, pressure_pairs

FIXTURES = Path(__file__).resolve().parents[2] / "tests" / "fixtures" / "nfl_scheme"


def fit_k_fg() -> float:
    fg = pl.read_parquet(FIXTURES / "fg_attempts_2019_2023.parquet")
    fg = env_adjusted_make_prob(fg).with_columns((pl.col("field_goal_result") == "made").cast(pl.Int64).alias("made"))
    fg = fg.filter(pl.col("kicker_player_id").is_not_null()).with_columns(
        pl.int_range(pl.len()).over("season", "kicker_player_id").alias("attempt_idx")
    )
    halves = (
        fg.with_columns((pl.col("attempt_idx") % 2).alias("half"))
        .group_by("season", "kicker_player_id", "half")
        .agg(
            pl.len().alias("att"),
            ((pl.col("made") - pl.col("exp_make_prob")).sum() / pl.len()).alias("fgoe_pa"),
        )
    )
    h1 = halves.filter(pl.col("half") == 0).rename({"att": "att1", "fgoe_pa": "fgoe1"})
    h2 = halves.filter(pl.col("half") == 1).rename({"att": "att2", "fgoe_pa": "fgoe2"})
    j = h1.join(h2, on=["season", "kicker_player_id"], how="inner").filter(
        (pl.col("att1") >= 5) & (pl.col("att2") >= 5)
    )
    att1 = j["att1"].to_numpy().astype(float)
    f1 = j["fgoe1"].to_numpy()
    f2 = j["fgoe2"].to_numpy()
    grid = np.arange(1.0, 1001.0, 1.0)
    mses = [float(np.mean((f1 * att1 / (att1 + k) - f2) ** 2)) for k in grid]
    best = grid[int(np.argmin(mses))]
    print(f"K_fg: n kicker-seasons={j.height}, best K={best}, mse={min(mses):.6f}")
    return float(best)


def fit_k_pressure() -> float:
    pbp = pl.read_parquet(FIXTURES / "pbp_2021_2023_slice.parquet")
    adj = adjust_pressure_pairs(pressure_pairs(pbp))
    errs = {}
    grid = np.arange(0.0, 10001.0, 50.0)
    for k in grid:
        sq = []
        for side, ncol in (
            ("adj_pressure_rate_allowed", "dropbacks_off"),
            ("adj_pressure_rate_generated", "dropbacks_def"),
        ):
            raw_side = side.replace("adj_", "")
            for y in (2021, 2022):
                a = adj.filter(pl.col("season") == y)
                b = adj.filter(pl.col("season") == y + 1).select("team", raw_side)
                jj = a.join(b, on="team", how="inner", suffix="_next")
                mu = float(jj[side].mean())
                n = jj[ncol].to_numpy().astype(float)
                shrunk = mu + (jj[side].to_numpy() - mu) * n / (n + k)
                nxt = jj[raw_side + "_next"].to_numpy()
                sq.append(np.mean((shrunk - nxt) ** 2))
        errs[float(k)] = float(np.mean(sq))
    best = min(errs, key=lambda kk: errs[kk])
    print(f"K_pressure: best K={best}, mse={errs[best]:.8f} (K=0 mse={errs[0.0]:.8f})")
    return best


if __name__ == "__main__":
    k_fg = fit_k_fg()
    k_pressure = fit_k_pressure()
    print({"K_fg": k_fg, "K_pressure": k_pressure})
