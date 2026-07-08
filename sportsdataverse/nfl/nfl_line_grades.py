"""NFL OL/DL unit grades ⑤ — opponent-adjusted, EB-shrunk pressure grades.

A "pressure" is ``sack == 1 or qb_hit == 1`` on a ``qb_dropback == 1`` play;
offense charged to ``posteam`` (allowed), defense credited to ``defteam``
(generated).  Opponent adjustment is a league-agnostic additive fixed point
(rate ~ mu + alpha_offense + beta_defense per season); grades are
``50 + 15*z*n/(n+K_pressure)`` with the split-half-fitted ``K_pressure``.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, List, Union

import numpy as np
import polars as pl

from sportsdataverse.nfl.nfl_scheme_constants import EB_PRIOR

if TYPE_CHECKING:  # pragma: no cover
    import pandas as pd

_RATES_SCHEMA: dict = {
    "season": pl.Int64,
    "team": pl.Utf8,
    "dropbacks_off": pl.Int64,
    "pressures_allowed": pl.Int64,
    "pressure_rate_allowed": pl.Float64,
    "dropbacks_def": pl.Int64,
    "pressures_generated": pl.Int64,
    "pressure_rate_generated": pl.Float64,
}

_ADJ_SCHEMA: dict = {
    **_RATES_SCHEMA,
    "adj_pressure_rate_allowed": pl.Float64,
    "adj_pressure_rate_generated": pl.Float64,
}

_GRADES_SCHEMA: dict = {
    **_ADJ_SCHEMA,
    "ol_pass_block_grade": pl.Float64,
    "dl_pass_rush_grade": pl.Float64,
}

_PRESSURE_EXPR = ((pl.col("sack") == 1) | (pl.col("qb_hit") == 1)).cast(pl.Int64).alias("pressure")


def _dropbacks(pbp: pl.DataFrame) -> pl.DataFrame:
    return pbp.filter(
        (pl.col("qb_dropback") == 1) & pl.col("posteam").is_not_null() & pl.col("defteam").is_not_null()
    ).with_columns(pl.col("posteam").cast(pl.Utf8), pl.col("defteam").cast(pl.Utf8), _PRESSURE_EXPR)


def team_pressure_rates(pbp: pl.DataFrame) -> pl.DataFrame:
    """Per (season, team) raw pressure rates, both sides of the ball.

    Args:
        pbp: nflverse-format pbp with ``season`` / ``posteam`` / ``defteam`` /
            ``qb_dropback`` / ``sack`` / ``qb_hit``.

    Returns:
        Per ``(season, team)``: ``dropbacks_off``, ``pressures_allowed``,
        ``pressure_rate_allowed``, ``dropbacks_def``, ``pressures_generated``,
        ``pressure_rate_generated``.  Empty input yields a zero-row frame.

    Example:
        Quick start::

            from sportsdataverse.nfl import load_nfl_pbp
            from sportsdataverse.nfl.nfl_line_grades import team_pressure_rates
            rates = team_pressure_rates(load_nfl_pbp([2023]))
            print(rates.sort("pressure_rate_generated", descending=True).head())
    """
    df = _dropbacks(pbp)
    if df.height == 0:
        return pl.DataFrame(schema=_RATES_SCHEMA)
    off = df.group_by("season", pl.col("posteam").alias("team")).agg(
        pl.len().cast(pl.Int64).alias("dropbacks_off"),
        pl.col("pressure").sum().cast(pl.Int64).alias("pressures_allowed"),
    )
    deff = df.group_by("season", pl.col("defteam").alias("team")).agg(
        pl.len().cast(pl.Int64).alias("dropbacks_def"),
        pl.col("pressure").sum().cast(pl.Int64).alias("pressures_generated"),
    )
    assert off.schema["team"] == deff.schema["team"]
    return (
        off.join(deff, on=["season", "team"], how="full", coalesce=True)
        .with_columns(
            (pl.col("pressures_allowed") / pl.col("dropbacks_off")).alias("pressure_rate_allowed"),
            (pl.col("pressures_generated") / pl.col("dropbacks_def")).alias("pressure_rate_generated"),
            pl.col("season").cast(pl.Int64),
        )
        .select(list(_RATES_SCHEMA.keys()))
        .sort("season", "team")
    )


def pressure_pairs(pbp: pl.DataFrame) -> pl.DataFrame:
    """Per (season, off_team, def_team) dropbacks + pressures (matchup grid)."""
    df = _dropbacks(pbp)
    if df.height == 0:
        return pl.DataFrame(
            schema={
                "season": pl.Int64,
                "off_team": pl.Utf8,
                "def_team": pl.Utf8,
                "dropbacks": pl.Int64,
                "pressures": pl.Int64,
            }
        )
    return (
        df.group_by(
            "season",
            pl.col("posteam").alias("off_team"),
            pl.col("defteam").alias("def_team"),
        )
        .agg(
            pl.len().cast(pl.Int64).alias("dropbacks"),
            pl.col("pressure").sum().cast(pl.Int64).alias("pressures"),
        )
        .with_columns(pl.col("season").cast(pl.Int64))
        .sort("season", "off_team", "def_team")
    )


def adjust_pressure_pairs(pairs: pl.DataFrame, *, max_iter: int = 50, tol: float = 1e-4) -> pl.DataFrame:
    """Opponent-adjust matchup pressure rates via an additive fixed point.

    Fits ``rate(off, def) ~ mu + alpha_off + beta_def`` per season by
    alternating dropback-weighted residual means (league-mean-centered);
    league-agnostic (no NFL constant inside).

    Args:
        pairs: Output of :func:`pressure_pairs` (or any frame with ``season``,
            ``off_team``, ``def_team``, ``dropbacks``, ``pressures``).
        max_iter: Fixed-point iteration cap.
        tol: Max-abs-change convergence tolerance.

    Returns:
        Per ``(season, team)``: raw allowed/generated rates + counts and
        ``adj_pressure_rate_allowed`` (``mu + alpha``) /
        ``adj_pressure_rate_generated`` (``mu + beta``).

    Example:
        Quick start::

            from sportsdataverse.nfl import load_nfl_pbp
            from sportsdataverse.nfl.nfl_line_grades import (
                adjust_pressure_pairs, pressure_pairs,
            )
            adj = adjust_pressure_pairs(pressure_pairs(load_nfl_pbp([2023])))
            print(adj.sort("adj_pressure_rate_generated", descending=True).head())
    """
    if pairs.height == 0:
        return pl.DataFrame(schema=_ADJ_SCHEMA)

    out_frames: List[pl.DataFrame] = []
    for (season,), grp in pairs.group_by("season"):
        teams = sorted(set(grp["off_team"].to_list()) | set(grp["def_team"].to_list()))
        idx = {t: i for i, t in enumerate(teams)}
        oi = np.array([idx[t] for t in grp["off_team"].to_list()])
        di = np.array([idx[t] for t in grp["def_team"].to_list()])
        w = grp["dropbacks"].to_numpy().astype(float)
        rate = grp["pressures"].to_numpy().astype(float) / np.maximum(w, 1.0)
        mu = float(np.average(rate, weights=w))
        n = len(teams)
        alpha = np.zeros(n)
        beta = np.zeros(n)
        for _ in range(max_iter):
            resid_a = rate - mu - beta[di]
            num_a = np.zeros(n)
            den_a = np.zeros(n)
            np.add.at(num_a, oi, w * resid_a)
            np.add.at(den_a, oi, w)
            new_alpha = np.divide(num_a, den_a, out=np.zeros(n), where=den_a > 0)
            resid_b = rate - mu - new_alpha[oi]
            num_b = np.zeros(n)
            den_b = np.zeros(n)
            np.add.at(num_b, di, w * resid_b)
            np.add.at(den_b, di, w)
            new_beta = np.divide(num_b, den_b, out=np.zeros(n), where=den_b > 0)
            delta = max(
                float(np.max(np.abs(new_alpha - alpha))),
                float(np.max(np.abs(new_beta - beta))),
            )
            alpha, beta = new_alpha, new_beta
            if delta < tol:
                break

        # raw per-team aggregates
        off_raw = grp.group_by(pl.col("off_team").alias("team")).agg(
            pl.col("dropbacks").sum().cast(pl.Int64).alias("dropbacks_off"),
            pl.col("pressures").sum().cast(pl.Int64).alias("pressures_allowed"),
        )
        def_raw = grp.group_by(pl.col("def_team").alias("team")).agg(
            pl.col("dropbacks").sum().cast(pl.Int64).alias("dropbacks_def"),
            pl.col("pressures").sum().cast(pl.Int64).alias("pressures_generated"),
        )
        adj = pl.DataFrame(
            {
                "team": teams,
                "adj_pressure_rate_allowed": (mu + alpha).astype(float),
                "adj_pressure_rate_generated": (mu + beta).astype(float),
            }
        )
        merged = (
            off_raw.join(def_raw, on="team", how="full", coalesce=True)
            .join(adj, on="team", how="left")
            .with_columns(
                pl.lit(season, dtype=pl.Int64).alias("season"),
                (pl.col("pressures_allowed") / pl.col("dropbacks_off")).alias("pressure_rate_allowed"),
                (pl.col("pressures_generated") / pl.col("dropbacks_def")).alias("pressure_rate_generated"),
            )
            .select(list(_ADJ_SCHEMA.keys()))
        )
        out_frames.append(merged)
    return pl.concat(out_frames).sort("season", "team")


def _line_grades_from(adj: pl.DataFrame) -> pl.DataFrame:
    """EB-shrunk 0-100 grades from (adjusted) rates: 50 + 15*z*n/(n+K)."""
    if adj.height == 0:
        return pl.DataFrame(schema=_GRADES_SCHEMA)
    k = EB_PRIOR["K_pressure"]
    out = adj
    grades = []
    for rate_col, n_col, alias, invert in (
        ("adj_pressure_rate_allowed", "dropbacks_off", "ol_pass_block_grade", True),
        ("adj_pressure_rate_generated", "dropbacks_def", "dl_pass_rush_grade", False),
    ):
        mu = out[rate_col].mean()
        sd = out[rate_col].std()
        if sd is None or sd == 0.0:
            grades.append(pl.lit(50.0).alias(alias))
            continue
        z = (pl.col(rate_col) - mu) / sd
        if invert:
            z = -z
        shrink = pl.col(n_col).fill_null(0) / (pl.col(n_col).fill_null(0) + k)
        grades.append((50.0 + 15.0 * z * shrink).alias(alias))
    return out.with_columns(grades).select(list(_GRADES_SCHEMA.keys())).sort("season", "team")


def nfl_line_grades(
    seasons: Union[int, List[int]],
    *,
    return_as_pandas: bool = False,
) -> Union[pl.DataFrame, "pd.DataFrame"]:
    """Team-season OL pass-block + DL pass-rush grades (opponent-adjusted, EB-shrunk).

    Loads pbp, builds the matchup pressure grid, opponent-adjusts it, grades
    both units on a 0-100 board (``50 + 15*z*n/(n+K_pressure)``), and joins
    PFR's independent team pressure measurement
    (``load_nfl_pfr_advstats(stat_type="def", summary_level="season")``,
    ``prss`` summed to team / pbp dropbacks faced) as ``pfr_pressure_pct``.

    Args:
        seasons: Season or list of seasons (PFR advstats coverage is 2018+).
        return_as_pandas: When ``True``, return a ``pandas.DataFrame``.

    Returns:
        Per ``(season, team)``: raw + adjusted pressure rates and dropback
        counts, ``ol_pass_block_grade``, ``dl_pass_rush_grade``,
        ``pfr_pressure_pct``.  Empty seasons yield a zero-row frame.

    Example:
        Quick start::

            from sportsdataverse.nfl.nfl_line_grades import nfl_line_grades
            g = nfl_line_grades([2023])
            print(g.sort("dl_pass_rush_grade", descending=True).head())

        See Also:
            * `nflfastR`_ -- sack / qb_hit source columns.

        .. _nflfastR: https://www.nflfastr.com
    """
    from sportsdataverse.nfl.nfl_loaders import load_nfl_pbp, load_nfl_pfr_advstats

    season_list = [seasons] if isinstance(seasons, int) else list(seasons)
    schema = {**_GRADES_SCHEMA, "pfr_pressure_pct": pl.Float64}
    if not season_list:
        out: pl.DataFrame = pl.DataFrame(schema=schema)
        return out.to_pandas() if return_as_pandas else out
    pbp = load_nfl_pbp(season_list)
    grades = _line_grades_from(adjust_pressure_pairs(pressure_pairs(pbp)))
    pfr = load_nfl_pfr_advstats(season_list, stat_type="def", summary_level="season")
    pfr_team = (
        pfr.filter(~pl.col("tm").str.contains("TM"))
        .group_by(pl.col("season").cast(pl.Int64), pl.col("tm").cast(pl.Utf8).alias("team"))
        .agg(pl.col("prss").sum().alias("pfr_pressures"))
    )
    assert grades.schema["team"] == pfr_team.schema["team"]
    out = (
        grades.join(pfr_team, on=["season", "team"], how="left")
        .with_columns((pl.col("pfr_pressures") / pl.col("dropbacks_def")).alias("pfr_pressure_pct"))
        .select(list(schema.keys()))
    )
    return out.to_pandas() if return_as_pandas else out
