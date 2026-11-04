"""League context for season tables: ranks, percentiles, cohort percentiles, sample sizes, league averages.

The basketball counterpart of the football baselines (roadmap F1 / F5 / F6, BB-D2). The
rules are ported from the football producers (``cfb_data_build.team_summaries`` and
``league_averages`` in cfbfastR-cfb-data; nfl-data carries the same code), so a season table
in any league follows one suffix grammar:

* ``<m>_rank`` -- 1 is best, ``average`` ties, a Float64. It is computed among qualifiers that
  have a finite ``<m>``, within the ``by`` group (the season). Pass a lower-is-better metric
  (DRtg, turnover rate) in ``lower_is_better``.
* ``<m>_pct`` -- the Weibull percentile ``100 * (n + 1 - rank) / (n + 1)``, where ``n`` is the
  number of ranked rows in the group. The best row sits at ``100 * n / (n + 1)`` and nobody gets
  exactly 0 or 100.
* ``<m>{suffix}`` (``_conf_pct``, ``_pos_pct``) -- the same percentile within a cohort,
  ranking ``<m>_rank`` so the direction carries over.
* ``<m>_n`` -- the sample size of the rate: the possessions, games or attempts it is computed
  over. A null denominator is 0.
* :func:`league_averages` -- one row per ``(season, level, entity, category, metric)`` with the
  ``mean / median / sd / n`` of the qualifying rows. Its ``n`` is the cohort count, the number of
  finite qualifying values (not a rate's ``<m>_n`` sample size). Where ranks exist it is the ``n``
  of the ``_pct`` formula above, whose denominator is ``n + 1``; it ignores the floor, so a
  season below the floor still reports its ``n`` beside null ranks.

There is one deliberate difference from football. Football floors only the cohort
percentiles. Here the floor (owner decision D-F5, :data:`MIN_COHORT_TEAMS` and
:data:`MIN_COHORT_PLAYERS`) also nulls the league-wide ``_rank`` / ``_pct``, because a WNBA
season or an early as-of-date cut can leave fewer than five teams. A rank or percentile below
the floor, or in a group with fewer than two distinct values, is null, never a number.

**Internal** -- not re-exported at the top-level ``sportsdataverse`` package; season-table
builders import from here.
"""

from __future__ import annotations

from collections.abc import Iterable, Sequence

import polars as pl

__all__ = [
    "LEAGUE_AVERAGES_SCHEMA",
    "MIN_COHORT_PLAYERS",
    "MIN_COHORT_TEAMS",
    "attach_cohort_percentiles",
    "attach_ranks",
    "attach_sample_sizes",
    "league_averages",
]

#: Cohort floors (owner decision D-F5, 2026-09-26; BB-D2): fewer ranked rows in a group than
#: this gives a null rank and percentile, never a percentile of two.
MIN_COHORT_TEAMS = 5
MIN_COHORT_PLAYERS = 10

#: The football F1 ``league_averages`` schema, column for column.
LEAGUE_AVERAGES_SCHEMA: dict[str, pl.DataType] = {
    "season": pl.Int64(),
    "level": pl.Utf8(),
    "entity": pl.Utf8(),
    "category": pl.Utf8(),
    "metric": pl.Utf8(),
    "mean": pl.Float64(),
    "median": pl.Float64(),
    "sd": pl.Float64(),
    "n": pl.Int64(),
    "qualifier_min": pl.Float64(),
}


def _check_columns(df: pl.DataFrame, names: Iterable[str], what: str) -> None:
    missing = sorted(set(names) - set(df.columns))
    if missing:
        raise KeyError(f"{what} missing from the frame: {missing}")


def attach_ranks(
    df: pl.DataFrame,
    metrics: Sequence[str],
    *,
    min_cohort: int,
    by: Sequence[str] = ("season",),
    lower_is_better: Sequence[str] = (),
    qualifier: pl.Expr | None = None,
) -> pl.DataFrame:
    """Add ``<m>_rank`` and ``<m>_pct`` for every metric, among qualifiers within ``by``.

    A row gets a null rank and percentile when its metric is null or non-finite, when it fails
    ``qualifier``, when a ``by`` key is null, when its group has fewer than ``min_cohort`` ranked
    rows, or when the group has fewer than two distinct values. Every other row keeps its
    rank: nothing is dropped.

    Args:
        df: The season table, one row per team or player and season.
        metrics: The columns to rank.
        min_cohort: The floor: :data:`MIN_COHORT_TEAMS` or :data:`MIN_COHORT_PLAYERS`.
        by: The group keys a rank is computed within. Defaults to ``("season",)``.
        lower_is_better: The metrics where a lower value ranks first (DRtg, turnover rate).
            Each one must also be in ``metrics``.
        qualifier: A boolean expression that rows must pass to be ranked (a minutes or
            possessions gate). ``None`` ranks every row.

    Returns:
        ``df`` with ``<m>_rank`` and then ``<m>_pct`` (both Float64) appended for every
        metric, in ``metrics`` order. An empty frame comes back with those columns.

    Raises:
        KeyError: If a metric or a ``by`` key is not a column of ``df``.
        ValueError: If a ``lower_is_better`` name is not in ``metrics``. Ignoring it would
            silently rank that metric high-is-good.

    Example:
        Quick start::

            import polars as pl
            from sportsdataverse._common.league_context import MIN_COHORT_TEAMS, attach_ranks
            teams = pl.DataFrame({"season": [2024] * 5, "def_rtg": [110.0, 112.0, 108.0, 115.0, 111.0]})
            attach_ranks(teams, ["def_rtg"], min_cohort=MIN_COHORT_TEAMS, lower_is_better=["def_rtg"])
    """
    lower = set(lower_is_better)
    stray = sorted(lower - set(metrics))
    if stray:
        raise ValueError(
            f"lower_is_better entries missing from metrics: {stray}. "
            "They would be ignored and their metrics ranked high-is-good."
        )
    keys = list(by)
    _check_columns(df, [*metrics, *keys], "metrics or by keys")
    gate = pl.lit(True) if qualifier is None else qualifier
    keyed = pl.all_horizontal([pl.col(k).is_not_null() for k in keys]) if keys else pl.lit(True)
    ranks, pcts = [], []
    for m in metrics:
        v = pl.when(gate & keyed & pl.col(m).cast(pl.Float64).is_finite()).then(pl.col(m).cast(pl.Float64))
        n = v.count().over(keys) if keys else v.count()
        distinct = v.drop_nulls().n_unique().over(keys) if keys else v.drop_nulls().n_unique()
        r = v.rank(method="average", descending=m not in lower)
        r = r.over(keys) if keys else r
        ok = v.is_not_null() & (n >= min_cohort) & (distinct > 1)
        ranks.append(pl.when(ok).then(r).cast(pl.Float64).alias(f"{m}_rank"))
        pcts.append(pl.when(ok).then(100 * (n + 1 - r) / (n + 1)).cast(pl.Float64).alias(f"{m}_pct"))
    return df.with_columns(ranks).with_columns(pcts)


def attach_cohort_percentiles(
    df: pl.DataFrame,
    metrics: Sequence[str],
    *,
    cohort: str,
    suffix: str,
    min_cohort: int,
    by: Sequence[str] = ("season",),
) -> pl.DataFrame:
    """Add ``<m>{suffix}``, the percentile of each ``<m>_rank`` within ``cohort`` (football F5).

    Run :func:`attach_ranks` first. Within each ``(*by, cohort)`` group, the cohort rank ``r``
    is the rank of ``<m>_rank`` ascending (so best-first in the metric's own direction), over
    the rows whose ``<m>_rank`` is non-null. The value is ``100 * (n + 1 - r) / (n + 1)``. It is
    null when ``<m>_rank`` or the cohort key is null, or when the cohort has fewer than
    ``min_cohort`` ranked rows.

    Args:
        df: A frame that :func:`attach_ranks` has already ranked.
        metrics: The ranked metrics (their ``<m>_rank`` columns must exist).
        cohort: The cohort column: a conference or a position group. Null means no cohort.
        suffix: The output suffix, ``"_conf_pct"`` or ``"_pos_pct"`` by convention.
        min_cohort: The floor: :data:`MIN_COHORT_TEAMS` or :data:`MIN_COHORT_PLAYERS`.
        by: The keys the league ranks were computed within. Defaults to ``("season",)``.

    Returns:
        ``df`` with ``<m>{suffix}`` (Float64) appended for every metric.

    Raises:
        KeyError: If a ``<m>_rank``, the cohort or a ``by`` key is not a column of ``df``.

    Example:
        Quick start::

            import polars as pl
            from sportsdataverse._common.league_context import (
                MIN_COHORT_TEAMS,
                attach_cohort_percentiles,
                attach_ranks,
            )
            teams = pl.DataFrame(
                {"season": [2024] * 6, "conference": ["East"] * 6, "net": [5.0, -1.0, 2.0, 0.5, -3.0, 7.0]}
            )
            ranked = attach_ranks(teams, ["net"], min_cohort=MIN_COHORT_TEAMS)
            attach_cohort_percentiles(
                ranked, ["net"], cohort="conference", suffix="_conf_pct", min_cohort=MIN_COHORT_TEAMS
            )
    """
    keys = [*by, cohort]
    _check_columns(df, [*(f"{m}_rank" for m in metrics), *keys], "ranks, cohort or by keys")
    exprs = []
    for m in metrics:
        rc = pl.col(f"{m}_rank")
        has = rc.is_not_null() & pl.all_horizontal([pl.col(k).is_not_null() for k in keys])
        n = has.sum().over(keys)
        r = pl.when(has).then(rc).rank(method="average").over(keys)
        exprs.append(
            pl.when(has & (n >= min_cohort)).then(100 * (n + 1 - r) / (n + 1)).cast(pl.Float64).alias(f"{m}{suffix}")
        )
    return df.with_columns(exprs)


def attach_sample_sizes(df: pl.DataFrame, denominators: dict[str, pl.Expr]) -> pl.DataFrame:
    """Add ``<m>_n``, the count each rate is computed over (football F6). A null count is 0.

    Args:
        df: The season table.
        denominators: Rate column -> the expression for its sample size (possessions, games,
            attempts). A rate the frame does not carry is skipped. A float count (estimated
            possessions) is rounded to the nearest whole number.

    Returns:
        ``df`` with an Int64 ``<m>_n`` appended for every rate it carries.

    Example:
        Quick start::

            import polars as pl
            from sportsdataverse._common.league_context import attach_sample_sizes
            attach_sample_sizes(teams, {"adj_pace": pl.col("games")})
    """
    present = {m: e for m, e in denominators.items() if m in df.columns}
    return df.with_columns(
        *[e.cast(pl.Float64).round(0).fill_null(0).cast(pl.Int64).alias(f"{m}_n") for m, e in present.items()]
    )


def league_averages(
    df: pl.DataFrame,
    metrics: Sequence[str],
    *,
    level: str,
    entity: str,
    category: str,
    qualifier: pl.Expr | None = None,
    qualifier_min: float | None = None,
) -> pl.DataFrame:
    """The league-average rows of a season table: mean / median / sd (ddof=1) / n per metric (football F1).

    The statistics describe the distribution of the qualifying entity rows (an unweighted mean
    over teams or qualified players), not a pooled league rate. That is the population
    :func:`attach_ranks` ranks. ``n`` is the cohort count: the finite qualifying values, the
    ``n`` in the ``_pct`` formula (whose denominator is ``n + 1``), not a rate's ``<m>_n``
    sample size. It applies neither the ``min_cohort`` floor nor the distinct-value check, so a
    season whose ``_rank`` / ``_pct`` are null still reports its mean, median, sd and ``n``. A
    null or non-finite value is skipped and does not count toward ``n``. A metric with no finite
    value in a season gets no row, and a row with a null ``season`` is dropped.

    Args:
        df: The season table. It must carry an integer ``season`` column.
        metrics: The columns to summarize.
        level: The population label (``"nba"``, ``"wnba"``, ``"d1"``).
        entity: ``"team"`` or ``"player"``.
        category: The table the rows describe (``"team_ratings"``).
        qualifier: The same gate passed to :func:`attach_ranks`. ``None`` keeps every row.
        qualifier_min: The gate's threshold, recorded on each row. Null when there is no gate.

    Returns:
        One row per ``(season, metric)`` in :data:`LEAGUE_AVERAGES_SCHEMA`. An empty input
        returns that schema with zero rows.

    Raises:
        KeyError: If a metric or ``season`` is not a column of ``df``.

    Example:
        Quick start::

            from sportsdataverse._common.league_context import league_averages
            from sportsdataverse.nba.nba_team_ratings import nba_team_ratings
            ratings = nba_team_ratings(2024)
            league_averages(ratings, ["adj_net_rtg"], level="nba", entity="team", category="team_ratings")
    """
    _check_columns(df, [*metrics, "season"], "metrics or season")
    if qualifier is not None:
        df = df.filter(qualifier)
    if df.height == 0 or not metrics:
        return pl.DataFrame(schema=LEAGUE_AVERAGES_SCHEMA)
    long = (
        df.select(pl.col("season").cast(pl.Int64), *[pl.col(m).cast(pl.Float64) for m in metrics])
        # value_name must not collide with a metric's own name (polars 2.0 raises DuplicateError)
        .unpivot(index="season", variable_name="metric", value_name="__value")
        # A null season would group as its own season row, one that attach_ranks never ranks.
        .filter(pl.col("season").is_not_null() & pl.col("__value").is_finite())
    )
    # Sorting each group's values makes the float sums a pure function of the value multiset,
    # so identical rebuilds produce byte-identical parquet (the football producers' rule).
    out = long.group_by(["season", "metric"], maintain_order=True).agg(
        mean=pl.col("__value").sort().mean(),
        median=pl.col("__value").sort().median(),
        sd=pl.col("__value").sort().std(),
        n=pl.len(),
    )
    return (
        out.with_columns(
            level=pl.lit(level),
            entity=pl.lit(entity),
            category=pl.lit(category),
            qualifier_min=pl.lit(qualifier_min, dtype=pl.Float64),
        )
        .select(list(LEAGUE_AVERAGES_SCHEMA))
        .cast(LEAGUE_AVERAGES_SCHEMA)
        .sort(["season", "metric"])
    )
