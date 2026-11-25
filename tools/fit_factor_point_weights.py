"""Check the fixed factor-point weights (BB-D42) against a league-season regression.

``sportsdataverse._common.factor_points`` prices each four-factor gap with fixed linear weights
computed from the season's own league counts. This script fits the alternative the roadmap names,
an OLS of a team's ``off_rtg - lg_rtg`` on its four factor-rate gaps (eFG%, TOV%, ORB%, FT rate,
each minus the league's), per league over the release seasons, and reports for each factor the
correlation across team-seasons between the fixed-price part and the regression's part
(coefficient times gap), plus both fits' share of the variance. The fixed weights are kept when
they track the regression: they add up exactly, need no refit and price a single row (a lineup)
the same way as a team.

Usage (network: reads the possession releases)::

    uv run python tools/fit_factor_point_weights.py --league nba --seasons 2016:2026
"""

from __future__ import annotations

import argparse

import numpy as np
import polars as pl

_RATES = ("efg_pct", "tov_pct", "orb_pct", "ft_rate")
_PARTS = ("off_efg_pts", "off_tov_pts", "off_orb_pts", "off_ft_pts")


def _rates(league: str, seasons: list[int]) -> pl.DataFrame:
    if league in ("nba", "wnba"):
        from sportsdataverse.nba.nba_team_season_rates import nba_team_season_rates

        return nba_team_season_rates(seasons, league_id="00" if league == "nba" else "10")
    from sportsdataverse.mbb.mbb_team_season_rates import mbb_team_season_rates

    return mbb_team_season_rates(seasons, league="mens" if league == "mbb" else "womens")


def compare(rates: pl.DataFrame) -> dict[str, float]:
    """The fixed parts against an OLS on the factor-rate gaps, over the ``all`` split's rows."""
    full = rates.filter(pl.col("split") == "all").drop_nulls([*_RATES, *_PARTS])
    gaps = full.select(  # each rate's gap from its season's possession-weighted league value
        (pl.col(r) - (pl.col(r) * pl.col("poss")).sum().over("season") / pl.col("poss").sum().over("season")).alias(r)
        for r in _RATES
    ).to_numpy()
    y = (full["off_rtg"] - full["lg_rtg"]).to_numpy()
    beta, *_ = np.linalg.lstsq(gaps, y, rcond=None)
    fit = gaps @ beta
    out = {"rows": float(full.height), "ols_r2": 1 - float(np.var(y - fit) / np.var(y))}
    for i, (rate, part) in enumerate(zip(_RATES, _PARTS)):
        out[f"beta_{rate}"] = float(beta[i])
        out[f"corr_{part}"] = float(np.corrcoef(full[part].to_numpy(), gaps[:, i] * beta[i])[0, 1])
    return out


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--league", choices=("nba", "wnba", "mbb", "wbb"), default="nba")
    ap.add_argument("--seasons", default="2016:2026", help="first:last, inclusive")
    args = ap.parse_args(argv)
    first, last = (int(x) for x in args.seasons.split(":"))
    for key, value in compare(_rates(args.league, list(range(first, last + 1)))).items():
        print(f"{key:>22}  {value:.4f}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
