"""BB-D10 evidence: the numbers in ``tests/fixtures/bb_factor_rapm/README.md``, from the release assets.

Reads the ``{nba,wnba}_stats_possessions`` / ``_schedules`` and ``{nba,wnba}_player_impact`` releases
(network; about 4 GB peak for an NBA season) and prints, per league-season:

* the recombination error of the split and its totals against the published plain RAPM;
* the share of possessions whose remainder part (``other``) is non-zero;
* each part's own RidgeCV penalty beside the shared one;
* the year-over-year Spearman of every component over consecutive seasons.

Usage::

    uv run --frozen python tools/models/factor_rapm/factor_rapm_evidence.py
"""

from __future__ import annotations

import polars as pl
from sklearn.linear_model import RidgeCV

from sportsdataverse._common.factor_rapm import FACTORS, factor_rapm, factor_targets
from sportsdataverse.nba import nba_loaders as N
from sportsdataverse.nba.nba_player_factor_rapm import nba_factor_rapm_rows
from sportsdataverse.nba.nba_rapm import DEFAULT_RAPM_ALPHAS, build_rapm_design
from sportsdataverse.wnba import wnba_loaders as W

#: league -> (seasons named by their end year, rank gate, year-over-year possession floor).
_LEAGUES = {"nba": ((2025, 2026), 400, 2000), "wnba": ((2024, 2025), 200, 1000)}
_OFF = [f"o_{f}_rapm" for f in FACTORS]
_DEF = [f"d_{'drb' if f == 'orb' else f}_rapm" for f in FACTORS]
_COMPONENTS = ["rapm", "o_rapm", "d_rapm", *_OFF, *_DEF]


def _season(league: str, season: int) -> tuple[pl.DataFrame, pl.DataFrame]:
    """The regular-season possession rows and the published regular-season player_impact of one season."""
    if league == "nba":
        poss, sched = N.load_nba_stats_possessions(season - 1), N.load_nba_stats_schedules(season - 1)
        impact = N.load_nba_player_impact(season)
    else:
        poss, sched = W.load_wnba_stats_possessions(season), W.load_wnba_stats_schedules(season)
        impact = W.load_wnba_player_impact(season)
    rows = nba_factor_rapm_rows(poss, sched, "regular-season")
    return rows, impact.filter(pl.col("season_type") == "Regular Season")


def main() -> None:
    for league, (seasons, gate, yoy_floor) in _LEAGUES.items():
        fits = {}
        for season in seasons:
            rows, impact = _season(league, season)
            out = factor_rapm(rows, player_dtype=pl.Int64(), min_poss=gate)
            fits[season] = out
            off = out.select((pl.sum_horizontal(_OFF) - pl.col("o_rapm")).abs().max()).item()
            deff = out.select((pl.sum_horizontal(_DEF) - pl.col("d_rapm")).abs().max()).item()
            both = out.join(impact, on="player_id", suffix="_pub", validate="1:1")
            vs = both.select(
                pl.corr("o_rapm", "o_rapm_pub").alias("r_o"),
                pl.corr("d_rapm", "d_rapm_pub").alias("r_d"),
                (pl.col("o_rapm") - pl.col("o_rapm_pub")).abs().max().alias("max_o"),
                (pl.col("d_rapm") - pl.col("d_rapm_pub")).abs().max().alias("max_d"),
                (pl.col("off_poss") - pl.col("off_poss_pub")).abs().max().alias("max_poss"),
            ).row(0, named=True)
            v = out["pts_per_fga"][0]
            parts = factor_targets(rows, v)
            other = parts.filter(pl.col("other").abs() > 1e-9).height / parts.height
            X, _, _ = build_rapm_design(rows)
            own = {f: float(RidgeCV(alphas=DEFAULT_RAPM_ALPHAS).fit(X, parts[f].to_numpy()).alpha_) for f in FACTORS}
            print(
                f"{league} {season}: {out.height} players, {rows.height} possessions, recombination "
                f"{off:.1e} / {deff:.1e}; vs player_impact ({both.height} matched) {vs}; other non-zero "
                f"{100 * other:.2f}%; shared alpha {out['ridge_alpha'][0]:.0f}, own alphas "
                + ", ".join(f"{f} {a:.0f}" for f, a in own.items())
            )
        a, b = (fits[s].filter(pl.col("off_poss") + pl.col("def_poss") >= yoy_floor) for s in seasons)
        pairs = a.join(b, on="player_id", suffix="_next")
        rho = {c: round(pairs.select(pl.corr(c, f"{c}_next", method="spearman")).item(), 3) for c in _COMPONENTS}
        print(f"{league} {seasons[0]} -> {seasons[1]}, {pairs.height} players over {yoy_floor} possessions: {rho}")


if __name__ == "__main__":
    main()
