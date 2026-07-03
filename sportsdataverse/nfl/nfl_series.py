"""Series conversion rates -- a faithful polars port of nflfastR's
``calculate_series_conversion_rates`` (``calculate_series_conversion_rates.R``).

Consumes a caller-supplied play-by-play frame carrying ``series`` /
``series_success`` / ``series_result`` (added by the ``add_series_data`` port --
see the reference Sec 7 docstring for the exact semantics) plus ``posteam`` /
``defteam``. Produces per-team offense (``off_*``) and defense (``def_*``) rate
columns at either the season grain (``weekly=False``, default) or the
season+week grain (``weekly=True``).

**Operator-precedence quirk transcribed verbatim (reference Sec 11):** the R
source literally computes ``off_scr_Nth`` as
``mean(last_down == N * conversion)``, which (R operator precedence: ``*``
binds tighter than ``==``) parses as ``mean(last_down == (N * conversion))``.
Because ``conversion`` is 0/1, this is accidentally equivalent to
``mean((last_down == N) & (conversion == 1))`` -- the naming survives even
though the R expression as written is confusing. That accidentally-correct
formula is what is ported here.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Literal, overload

import polars as pl

if TYPE_CHECKING:
    import pandas as pd

# Series-result buckets shared by offense + defense.
_FG_RESULTS: tuple[str, ...] = ("Field goal", "Missed field goal")
_TO_RESULTS: tuple[str, ...] = (
    "Turnover on downs",
    "Turnover",
    "Opp touchdown",
    "Safety",
    "End of half",
)

_RATE_SUFFIXES: tuple[str, ...] = (
    "n",
    "scr",
    "scr_1st",
    "scr_2nd",
    "scr_3rd",
    "scr_4th",
    "1st",
    "td",
    "fg",
    "punt",
    "to",
)


def _empty_series_frame(*, weekly: bool, return_as_pandas: bool) -> pl.DataFrame | "pd.DataFrame":
    """Return a zero-row frame carrying the documented schema."""
    schema: dict[str, type[pl.DataType] | pl.DataType] = {"season": pl.Int64, "team": pl.Utf8}
    if weekly:
        schema["week"] = pl.Int64
    for side in ("off", "def"):
        for suffix in _RATE_SUFFIXES:
            schema[f"{side}_{suffix}"] = pl.Int64 if suffix == "n" else pl.Float64
    out = pl.DataFrame(schema=schema)
    if return_as_pandas:
        return out.to_pandas()
    return out


def _team_series_rates(pbp: pl.DataFrame, *, team_col: str, prefix: str, grp: list[str]) -> pl.DataFrame:
    """Collapse plays -> per-series conversion/result/last_down, then -> team rates.

    Mirrors the two-stage R pipeline: series-level ``first()``/``last()``
    collapse over ``(season, week, team, series)`` (relies on the input frame
    already being in play order within a series), then a team-level ``mean()``
    over ``grp`` (``(season, team)`` or ``(season, team, week)``).
    """
    per_series = (
        pbp.filter(pl.col("down").is_not_null() & (pl.col("series_result") != "QB kneel"))
        .group_by(["season", "week", pl.col(team_col).alias("team"), "series"])
        .agg(
            pl.first("series_success").alias("conversion"),
            pl.first("series_result").alias("result"),
            pl.last("down").alias("last_down"),
        )
    )

    def _scr_nth(n: int) -> pl.Expr:
        return ((pl.col("last_down") == n) & (pl.col("conversion") == 1)).cast(pl.Float64)

    return per_series.group_by(grp).agg(
        pl.len().cast(pl.Int64).alias(f"{prefix}_n"),
        pl.col("conversion").cast(pl.Float64).mean().alias(f"{prefix}_scr"),
        _scr_nth(1).mean().alias(f"{prefix}_scr_1st"),
        _scr_nth(2).mean().alias(f"{prefix}_scr_2nd"),
        _scr_nth(3).mean().alias(f"{prefix}_scr_3rd"),
        _scr_nth(4).mean().alias(f"{prefix}_scr_4th"),
        (pl.col("result") == "First down").cast(pl.Float64).mean().alias(f"{prefix}_1st"),
        (pl.col("result") == "Touchdown").cast(pl.Float64).mean().alias(f"{prefix}_td"),
        pl.col("result").is_in(_FG_RESULTS).cast(pl.Float64).mean().alias(f"{prefix}_fg"),
        (pl.col("result") == "Punt").cast(pl.Float64).mean().alias(f"{prefix}_punt"),
        pl.col("result").is_in(_TO_RESULTS).cast(pl.Float64).mean().alias(f"{prefix}_to"),
    )


@overload
def calculate_nfl_series_conversion_rates(
    pbp: pl.DataFrame,
    *,
    weekly: bool = ...,
    return_as_pandas: Literal[False] = ...,
) -> pl.DataFrame: ...
@overload
def calculate_nfl_series_conversion_rates(
    pbp: pl.DataFrame,
    *,
    weekly: bool = ...,
    return_as_pandas: Literal[True],
) -> "pd.DataFrame": ...
def calculate_nfl_series_conversion_rates(
    pbp: pl.DataFrame,
    *,
    weekly: bool = False,
    return_as_pandas: bool = False,
) -> pl.DataFrame | "pd.DataFrame":
    """Compute per-team offense + defense series conversion rates.

    A faithful polars port of nflfastR's ``calculate_series_conversion_rates``.
    Series where ``down`` is null (kickoffs, PAT/2pt attempts, non-plays, no
    ``posteam``) and series ending in a ``"QB kneel"`` are excluded from the
    series count before rates are computed, matching the R source.

    Args:
        pbp: Play-by-play frame carrying ``season``, ``week``, ``posteam``,
            ``defteam``, ``down``, ``series``, ``series_success``, and
            ``series_result`` (added by the ``add_series_data`` port). Rows
            must already be in play order within each series so the internal
            ``first()``/``last()`` series collapse is correct.
        weekly: If ``True``, group on ``(season, team, week)``; if ``False``
            (default), group on ``(season, team)`` -- collapsing every week
            into one season-level rate.
        return_as_pandas: If ``True`` return a pandas DataFrame; else polars.

    Returns:
        A polars (or pandas) DataFrame with one row per team (per week when
        ``weekly=True``), ``off_n``/``def_n`` (series count) plus the
        ``off_*``/``def_*`` rate columns documented in reference Sec 11.
        A team with offensive series but zero defensive series in a group (or
        vice versa -- effectively never happens in real data) carries nulls in
        the missing side rather than being dropped (full outer join).

    Example:
        Season-level rates::

            from sportsdataverse.nfl import calculate_nfl_series_conversion_rates
            rates = calculate_nfl_series_conversion_rates(pbp)
            rates.filter(pl.col("team") == "KC").select("off_scr", "def_scr")

        Weekly grain::

            weekly = calculate_nfl_series_conversion_rates(pbp, weekly=True)

        Pipeline next step (one line)::

            rates.sort("off_scr", descending=True).head()

    See Also:
        * `nflfastR <https://www.nflfastr.com>`_ -- the ``calculate_series_conversion_rates`` source
        * `nflreadpy <https://github.com/nflverse/nflreadpy>`_ -- nflverse loaders (Python)

    .. _nflfastR: https://www.nflfastr.com
    .. _nflreadpy: https://github.com/nflverse/nflreadpy
    """
    if pbp.height == 0:
        return _empty_series_frame(weekly=weekly, return_as_pandas=return_as_pandas)

    grp = ["season", "team", "week"] if weekly else ["season", "team"]

    offense = _team_series_rates(pbp, team_col="posteam", prefix="off", grp=grp)
    defense = _team_series_rates(pbp, team_col="defteam", prefix="def", grp=grp)

    combined = offense.join(defense, on=grp, how="full", coalesce=True).sort(grp)

    if return_as_pandas:
        return combined.to_pandas()
    return combined
