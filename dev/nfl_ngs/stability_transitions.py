"""Evidence script for the EB-shrinkage stability finding (cited in tests).

Measures corr(metric_N, raw_{N+1}) for raw vs weekly-sigma2 EB-shrunk values
across four NGS season transitions (2019->20 ... 2022->23), live data.

Observed output (run 2026-07-08):

    rushing  2019->2020 n=31 s_raw=0.4821 s_shrunk=0.5625 d=+0.0804
    rushing  2020->2021 n=32 s_raw=0.3963 s_shrunk=0.4128 d=+0.0165
    rushing  2021->2022 n=34 s_raw=0.2549 s_shrunk=0.2816 d=+0.0268
    rushing  2022->2023 n=33 s_raw=0.0448 s_shrunk=0.0045 d=-0.0403   (mean +0.021)
    receiving 2019->2020 n=88 s_raw=0.4289 s_shrunk=0.4389 d=+0.0100
    receiving 2020->2021 n=89 s_raw=0.5323 s_shrunk=0.4834 d=-0.0489
    receiving 2021->2022 n=84 s_raw=0.4152 s_shrunk=0.4447 d=+0.0295
    receiving 2022->2023 n=83 s_raw=0.3939 s_shrunk=0.3984 d=+0.0045  (mean -0.001)

Conclusion: shrinkage helps on average for rushing and is ~neutral for
receiving; the pinned 2022->2023 rushing transition has ~zero base signal
(raw corr 0.045, n=33) so its gate is strict-xfailed in
tests/nfl/test_nfl_ngs_oracle.py with this script as the citation.
"""

import numpy as np
import polars as pl

from sportsdataverse.nfl import load_nfl_nextgen_stats
from sportsdataverse.nfl.nfl_ngs_constants import empirical_bayes_shrink, weekly_sigma2

SPECS = [
    ("rushing", "rush_yards_over_expected_per_att", "rush_attempts"),
    ("receiving", "avg_yac_above_expectation", "receptions"),
]


def main() -> None:
    for stat_type, raw_col, weight_col in SPECS:
        raw = load_nfl_nextgen_stats(seasons=[2019, 2020, 2021, 2022, 2023], stat_type=stat_type)
        wk = raw.filter(pl.col("week") > 0).drop_nulls([raw_col, weight_col])
        sea = raw.filter(pl.col("week") == 0)

        def model(season: int) -> pl.DataFrame:
            g = sea.filter(pl.col("season") == season)
            x = g[raw_col].to_numpy().astype(float)
            n = g[weight_col].to_numpy().astype(float)
            mu = float(np.average(x, weights=n))
            s2 = weekly_sigma2(wk.filter(pl.col("season") == season), raw_col, weight_col)
            shrunk, _ = empirical_bayes_shrink(x, n, prior_mean=mu, sigma2=s2)
            return g.select("player_gsis_id").with_columns(pl.Series("shrunk", shrunk), pl.Series("raw", x))

        deltas = []
        for a, b in [(2019, 2020), (2020, 2021), (2021, 2022), (2022, 2023)]:
            cur, nxt = model(a), model(b)
            j = cur.join(
                nxt.select("player_gsis_id", pl.col("raw").alias("rn")),
                on="player_gsis_id",
                how="inner",
            )
            sr = float(np.corrcoef(j["raw"].to_numpy(), j["rn"].to_numpy())[0, 1])
            ss = float(np.corrcoef(j["shrunk"].to_numpy(), j["rn"].to_numpy())[0, 1])
            deltas.append(ss - sr)
            print(f"{stat_type} {a}->{b} n={j.height} s_raw={sr:.4f} s_shrunk={ss:.4f} d={ss - sr:+.4f}")
        print(f"{stat_type} mean_delta={np.mean(deltas):+.4f}")


if __name__ == "__main__":
    main()
