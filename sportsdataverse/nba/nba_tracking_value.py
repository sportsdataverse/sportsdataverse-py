"""nba_tracking_value -- SportVU-tracking over-expected value models (league-agnostic).

Six descriptive value models, each a residual of a realized tracking outcome
against a computed league/role baseline for the same season slice:
``residual = realized - opportunities * (sum(realized)/sum(opportunities))`` within
each role bucket. Baselines are recomputed every call (no fitted constant, no
artifact). ``league_id`` selects NBA ("00") / WNBA ("10") / G-League ("20").

Methodology follows the published Second Spectrum / SportVU over-expected
conventions (public rebound-chance and defended-FG% definitions) -- original
implementation, nothing ported.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Callable, Optional, Union

import polars as pl

from sportsdataverse.nba.nba_player_positions import nba_player_positions
from sportsdataverse.nba.nba_stats import nba_stats_leaguedashptstats
from sportsdataverse.nba.nba_stats_parsers import parse_nba_stats_result_sets
from sportsdataverse.nba.nba_tracking_value_constants import MEASURE_SPECS, MeasureSpec

if TYPE_CHECKING:
    import pandas as pd

# No __all__: matches the sibling nba_shot_value.py convention -- `import *`
# picks up the public (non-underscore) function names by Python's default
# rule; tests import the private `_`-prefixed helpers directly from this
# submodule rather than via a star-import.


def _season_str(season: "int | str") -> str:
    """Normalize a season to the ``"YYYY-YY"`` string ``stats.nba.com`` expects.

    Args:
        season: Either an ``int`` season-ending year (``2024`` -> the 2023-24
            season) or an already-formatted ``"YYYY-YY"`` string (passthrough).

    Returns:
        The ``"YYYY-YY"`` season string.
    """
    if isinstance(season, str):
        return season
    return f"{season - 1}-{str(season)[-2:]}"


def _pin_ids(df: pl.DataFrame) -> pl.DataFrame:
    """Pin ``player_id``/``team_id`` to ``Utf8`` via an ``Int64`` intermediate cast.

    Casting straight from a float would stringify a float-origin id as
    ``"123.0"``; the ``Int64`` intermediate avoids that (see project ID/join-key
    discipline).

    Args:
        df: Frame that may carry ``player_id`` and/or ``team_id``.

    Returns:
        *df* with those columns cast to ``Utf8`` (columns not present are untouched).
    """
    for col in ("player_id", "team_id"):
        if col in df.columns and df.schema[col] != pl.Utf8:
            df = df.with_columns(pl.col(col).cast(pl.Int64, strict=False).cast(pl.Utf8).alias(col))
    return df


def _fetch_leaguedash_tracking(
    season: "int | str",
    measure: str,
    *,
    league_id: str = "00",
    per_mode: str = "Totals",
    player_or_team: str = "Player",
    _get_fn: Optional[Callable[..., dict]] = None,
) -> pl.DataFrame:
    """Fetch + parse one ``leaguedashptstats`` measure, ids pinned to ``Utf8``.

    Args:
        season: Season (``int`` ending-year or ``"YYYY-YY"`` string).
        measure: ``pt_measure_type`` value, e.g. ``"Rebounding"``, ``"Drives"``.
        league_id: ``"00"`` NBA, ``"10"`` WNBA, ``"20"`` G-League.
        per_mode: ``per_mode_simple`` value (default ``"Totals"``).
        player_or_team: ``"Player"`` or ``"Team"``.
        _get_fn: Injectable replacement for ``nba_stats_leaguedashptstats`` that
            returns the raw ``{resultSets: [...]}`` dict directly -- lets offline
            tests bypass the live transport entirely.

    Returns:
        A tidy ``pl.DataFrame`` with ``player_id``/``team_id`` as ``Utf8``, or a
        zero-row ``pl.DataFrame()`` on an empty/malformed payload.
    """
    season_str = _season_str(season)
    fetch = _get_fn if _get_fn is not None else nba_stats_leaguedashptstats
    raw = fetch(
        pt_measure_type=measure,
        season=season_str,
        league_id=league_id,
        per_mode_simple=per_mode,
        player_or_team=player_or_team,
        return_parsed=False,
    )
    df = parse_nba_stats_result_sets(raw)
    if not isinstance(df, pl.DataFrame) or df.height == 0:
        return pl.DataFrame()
    return _pin_ids(df)


def _position_num_to_bucket(position_num: float) -> str:
    """Map the BPM 1-5 numeric position scale to a ``guard``/``wing``/``big`` bucket."""
    if position_num < 2.5:
        return "guard"
    if position_num < 3.75:
        return "wing"
    return "big"


def _attach_role_bucket(
    df: pl.DataFrame,
    season: "int | str",
    *,
    league_id: str = "00",
    positions: Optional[pl.DataFrame] = None,
) -> pl.DataFrame:
    """Left-join a ``guard``/``wing``/``big`` role bucket onto *df* by ``player_id``.

    When *positions* is not supplied, loads them via :func:`nba_player_positions`
    (numeric 1-5 scale) and buckets guard/wing/big off that scale. Missing
    positions -- or a wholly unavailable positions source (sparse G-League/early
    WNBA tracking) -- degrade to the single ``"all"`` bucket rather than raising.

    Args:
        df: Frame carrying ``player_id`` (``Utf8``).
        season: Season passed through to :func:`nba_player_positions` when *positions*
            is not supplied.
        league_id: ``"00"`` NBA, ``"10"`` WNBA, ``"20"`` G-League.
        positions: Optional pre-fetched positions frame with either
            ``player_id:Utf8, position_bucket:Utf8`` (used as-is) or
            ``player_id, position_num`` (bucketed here). Injectable for tests.

    Returns:
        *df* with an added ``position_bucket`` column (``"all"`` fill for any
        player without a resolved bucket).
    """
    if df.height == 0:
        return df.with_columns(pl.lit("all").alias("position_bucket"))

    if positions is None:
        try:
            positions = nba_player_positions(_season_str(season), league_id=league_id)
        except Exception:
            positions = None

    if positions is None or positions.height == 0:
        return df.with_columns(pl.lit("all").alias("position_bucket"))

    if "position_bucket" not in positions.columns and "position_num" in positions.columns:
        positions = positions.with_columns(
            pl.col("position_num").map_elements(_position_num_to_bucket, return_dtype=pl.Utf8).alias("position_bucket")
        )

    positions = _pin_ids(positions)
    assert df.schema["player_id"] == positions.schema["player_id"], "player_id dtype mismatch before role join"
    out = df.join(positions.select("player_id", "position_bucket"), on="player_id", how="left")
    return out.with_columns(pl.col("position_bucket").fill_null("all"))


def _over_expected(
    df: pl.DataFrame,
    *,
    actual: str,
    denom: str,
    group_cols: list[str],
    out_prefix: str,
) -> pl.DataFrame:
    """The shared over-expected engine: ``residual = actual - denom * bucket_rate``.

    ``bucket_rate = Σ(actual)/Σ(denom)`` computed within each ``group_cols`` bucket
    from the SAME rows being scored (never a fitted/stored constant), so
    ``Σ(residual) == 0`` within every bucket by construction.

    Args:
        df: Input frame carrying *actual*, *denom*, and *group_cols*.
        actual: Realized-outcome column name.
        denom: Opportunity/denominator column name.
        group_cols: Baseline-scope columns (e.g. ``["position_bucket"]``); empty
            list computes one league-wide baseline.
        out_prefix: Output-column prefix -- produces ``{prefix}_baseline_rate``,
            ``{prefix}_expected``, ``{prefix}_oe``.

    Returns:
        *df* with the three added columns. When *df* is empty or missing *actual*/
        *denom*, the three columns are added as all-null ``Float64`` (graceful
        degradation, never raises).
    """
    rate_col, exp_col, oe_col = f"{out_prefix}_baseline_rate", f"{out_prefix}_expected", f"{out_prefix}_oe"
    if df.height == 0 or actual not in df.columns or denom not in df.columns:
        return df.with_columns(
            [
                pl.lit(None, dtype=pl.Float64).alias(rate_col),
                pl.lit(None, dtype=pl.Float64).alias(exp_col),
                pl.lit(None, dtype=pl.Float64).alias(oe_col),
            ]
        )
    gb = group_cols or []
    rate_expr = (
        pl.when(pl.col(denom).sum() > 0)
        .then(pl.col(actual).sum() / pl.col(denom).sum())
        .otherwise(None)
        .alias(rate_col)
    )
    if gb:
        rate = df.group_by(gb).agg(rate_expr)
        out = df.join(rate, on=gb, how="left")
    else:
        rate = df.select(rate_expr)
        out = df.join(rate, how="cross")
    return out.with_columns((pl.col(denom).cast(pl.Float64) * pl.col(rate_col)).alias(exp_col)).with_columns(
        (pl.col(actual).cast(pl.Float64) - pl.col(exp_col)).alias(oe_col)
    )


# ---------------------------------------------------------------------------
# Shared multi-season / schema helpers used by every public model function
# ---------------------------------------------------------------------------


def _season_label(season: "int | str") -> int:
    """Return the ``Int64`` season-ending-year label for output rows (``"2023-24" -> 2024``)."""
    if isinstance(season, int):
        return season
    return int(season.split("-")[0]) + 1


def _as_season_list(seasons: "int | str | list") -> list:
    """Normalize the public ``seasons`` argument to a list."""
    if isinstance(seasons, (list, tuple)):
        return list(seasons)
    return [seasons]


def _finalize_schema(df: pl.DataFrame, schema: "dict[str, pl.DataType]") -> pl.DataFrame:
    """Add any missing documented columns as typed nulls, then select+cast to *schema*."""
    for col, dtype in schema.items():
        if col not in df.columns:
            df = df.with_columns(pl.lit(None, dtype=dtype).alias(col))
    return df.select([pl.col(c).cast(t) for c, t in schema.items()])


def _expected_from_difficulty(df: pl.DataFrame, spec: MeasureSpec, group_cols: "list[str]") -> pl.DataFrame:
    """Recompute ``{prefix}_expected``/``{prefix}_oe`` as a sum of per-difficulty-bucket
    ``denom * bucket_rate`` contributions (e.g. contested vs uncontested rebound chances).

    When ``spec.extra_denoms`` is empty, or any of its (actual, denom) column pairs are
    missing from *df*, this is a no-op: the plain single-rate ``_over_expected`` result
    already on *df* stands (the documented graceful degradation).

    Args:
        df: Frame already carrying the plain ``_over_expected`` output for *spec*.
        spec: The measure spec whose ``extra_denoms`` difficulty buckets to apply.
        group_cols: Baseline-scope columns (e.g. ``["position_bucket"]``).

    Returns:
        *df* with ``{spec.out_prefix}_expected``/``_oe`` replaced by the
        difficulty-weighted values, or unchanged when the difficulty columns
        are unavailable.
    """
    if not spec.extra_denoms:
        return df
    for actual_col, denom_col in spec.extra_denoms.values():
        if actual_col not in df.columns or denom_col not in df.columns:
            return df

    exp_col, oe_col = f"{spec.out_prefix}_expected", f"{spec.out_prefix}_oe"
    gb = group_cols or []
    total_expected: "Optional[pl.Expr]" = None
    rate_cols: list[str] = []
    for actual_col, denom_col in spec.extra_denoms.values():
        rate_alias = f"__rate_{actual_col}"
        rate_cols.append(rate_alias)
        rate_expr = (
            pl.when(pl.col(denom_col).sum() > 0)
            .then(pl.col(actual_col).sum() / pl.col(denom_col).sum())
            .otherwise(None)
            .alias(rate_alias)
        )
        if gb:
            rate = df.group_by(gb).agg(rate_expr)
            df = df.join(rate, on=gb, how="left")
        else:
            rate = df.select(rate_expr)
            df = df.join(rate, how="cross")
        contribution = pl.col(denom_col).cast(pl.Float64) * pl.col(rate_alias)
        total_expected = contribution if total_expected is None else (total_expected + contribution)

    df = df.with_columns(total_expected.alias(exp_col))
    df = df.with_columns((pl.col(spec.actual).cast(pl.Float64) - pl.col(exp_col)).alias(oe_col))
    return df.drop(rate_cols)


# ---------------------------------------------------------------------------
# Phase 1 -- rebounding-over-expected
# ---------------------------------------------------------------------------

_REB_OE_SCHEMA: "dict[str, pl.DataType]" = {
    "season": pl.Int64,
    "player_id": pl.Utf8,
    "player_name": pl.Utf8,
    "team_id": pl.Utf8,
    "position_bucket": pl.Utf8,
    "gp": pl.Int64,
    "min": pl.Float64,
    "reb": pl.Float64,
    "reb_chances": pl.Float64,
    "reb_baseline_rate": pl.Float64,
    "reb_expected": pl.Float64,
    "reb_oe": pl.Float64,
    "reb_oe_per_36": pl.Float64,
    "oreb_oe": pl.Float64,
    "dreb_oe": pl.Float64,
    "league_id": pl.Utf8,
}


def nba_tracking_reb_oe(
    seasons: "int | str | list",
    *,
    league_id: str = "00",
    per_mode: str = "Totals",
    by_position: bool = True,
    positions: Optional[pl.DataFrame] = None,
    return_as_pandas: bool = False,
    _get_fn: Optional[Callable[..., dict]] = None,
) -> "Union[pl.DataFrame, pd.DataFrame]":
    """Rebounding-over-expected: ``reb_oe`` plus OREB/DREB splits, per player-season.

    Fetches the ``Rebounding`` ``leaguedashptstats`` measure, attaches a
    ``guard``/``wing``/``big`` role bucket, and computes
    ``reb_oe = reb - reb_chances * bucket_rate`` (contest-difficulty-adjusted
    when the endpoint carries separate contested/uncontested CHANCE columns;
    the live ``stats.nba.com`` payload currently does not, so this degrades
    gracefully to the plain rate -- see the fixtures README for the finding).
    OREB/DREB residuals are computed identically against their own chance
    columns. Baselines are recomputed from the same season slice on every
    call -- there is no fitted constant or bundled artifact.

    Args:
        seasons: A single season (``int`` ending-year or ``"YYYY-YY"`` string)
            or a list of seasons to concatenate.
        league_id: ``"00"`` NBA (default), ``"10"`` WNBA, ``"20"`` G-League.
        per_mode: ``per_mode_simple`` passed to the fetch (default ``"Totals"``).
        by_position: Compute the baseline within ``guard``/``wing``/``big``
            buckets (default). ``False`` forces one league-wide bucket.
        positions: Optional pre-fetched positions frame (see
            :func:`_attach_role_bucket`); mostly for injecting a fixture in tests.
        return_as_pandas: Return a :class:`pandas.DataFrame` instead of polars.
        _get_fn: Injectable replacement for ``nba_stats_leaguedashptstats``
            returning the raw payload dict directly -- offline testing hook.

    Returns:
        One row per player-season:
        ``season, player_id, player_name, team_id, position_bucket, gp, min,
        reb, reb_chances, reb_baseline_rate, reb_expected, reb_oe,
        reb_oe_per_36, oreb_oe, dreb_oe, league_id``.
        Empty/malformed input returns a zero-row frame with this schema.

    Example:
        Quick start::

            from sportsdataverse.nba import nba_tracking_reb_oe
            df = nba_tracking_reb_oe(2024)
            print(df.sort("reb_oe", descending=True).head())

        League-wide baseline (no position split)::

            df_all = nba_tracking_reb_oe(2024, by_position=False)

        Pandas output::

            df_pd = nba_tracking_reb_oe(2024, return_as_pandas=True)

        See Also:
            * `nba_api`_ -- Python NBA/WNBA stats API client
            * `hoopR`_ -- R companion package for NBA/MBB data

        .. _nba_api: https://github.com/swar/nba_api
        .. _hoopR: https://hoopR.sportsdataverse.org
    """
    season_list = _as_season_list(seasons)
    spec = MEASURE_SPECS["reb"]

    frames = []
    for season in season_list:
        fetched = _fetch_leaguedash_tracking(
            season, spec.measure, league_id=league_id, per_mode=per_mode, _get_fn=_get_fn
        )
        if fetched.height == 0:
            continue
        frames.append(fetched.with_columns(pl.lit(_season_label(season)).alias("season")))

    if not frames:
        out = pl.DataFrame(schema=_REB_OE_SCHEMA)
        return out.to_pandas() if return_as_pandas else out

    df = pl.concat(frames, how="diagonal_relaxed")

    if by_position:
        df = _attach_role_bucket(df, season_list[0], league_id=league_id, positions=positions)
        group_cols: "list[str]" = ["position_bucket"]
    else:
        df = df.with_columns(pl.lit("all").alias("position_bucket"))
        group_cols = []

    out = _over_expected(df, actual=spec.actual, denom=spec.denom, group_cols=group_cols, out_prefix=spec.out_prefix)
    out = _expected_from_difficulty(out, spec, group_cols)

    for sub in ("oreb", "dreb"):
        denom_sub = f"{sub}_chances"
        if sub in out.columns and denom_sub in out.columns:
            out = _over_expected(out, actual=sub, denom=denom_sub, group_cols=group_cols, out_prefix=sub)
        else:
            out = out.with_columns(pl.lit(None, dtype=pl.Float64).alias(f"{sub}_oe"))

    out = out.with_columns(
        pl.when(pl.col("min") > 0)
        .then(pl.col("reb_oe") / (pl.col("min") / 36.0))
        .otherwise(None)
        .alias("reb_oe_per_36")
    )
    out = out.with_columns(pl.lit(league_id).alias("league_id"))

    out = _finalize_schema(out, _REB_OE_SCHEMA)
    return out.to_pandas() if return_as_pandas else out


# ---------------------------------------------------------------------------
# Phase 2 -- expected assists / passer value
# ---------------------------------------------------------------------------

_AST_OE_SCHEMA: "dict[str, pl.DataType]" = {
    "season": pl.Int64,
    "player_id": pl.Utf8,
    "player_name": pl.Utf8,
    "team_id": pl.Utf8,
    "position_bucket": pl.Utf8,
    "gp": pl.Int64,
    "min": pl.Float64,
    "ast": pl.Float64,
    "passes": pl.Float64,
    "ast_baseline_rate": pl.Float64,
    "ast_expected": pl.Float64,
    "ast_oe": pl.Float64,
    "ast_oe_per_36": pl.Float64,
    "ast_pts_created": pl.Float64,
    "league_id": pl.Utf8,
}


def _enrich_potential_assists(
    df: pl.DataFrame,
    season: "int | str",
    *,
    league_id: str,
    max_players: int,
    _pass_get_fn: Optional[Callable[..., dict]] = None,
) -> pl.DataFrame:
    """Left-join a ``potential_assists`` column onto the top-*max_players* passers.

    Fetches ``nba_stats_playerdashptpass`` one player at a time (capped at
    *max_players*, ranked by the ``passes`` column already on *df*) and sums
    each response's ``POTENTIAL_AST`` column across its per-teammate rows.
    Never a hard dependency -- players outside the fetched set (or any player
    whose fetch fails/returns no rows) simply keep a null ``potential_assists``.

    Args:
        df: Frame carrying ``player_id`` (``Utf8``) and ``passes``.
        season: Season passed to the per-player fetch.
        league_id: ``"00"`` NBA, ``"10"`` WNBA, ``"20"`` G-League.
        max_players: Cap on the number of per-player fetches.
        _pass_get_fn: Injectable replacement for ``nba_stats_playerdashptpass``
            returning the raw payload dict directly.

    Returns:
        *df* with an added ``potential_assists`` column (``null`` where unresolved).
    """
    from sportsdataverse.nba.nba_stats import nba_stats_playerdashptpass  # noqa: PLC0415

    fetch = _pass_get_fn if _pass_get_fn is not None else nba_stats_playerdashptpass
    top_ids = df.sort("passes", descending=True).head(max_players)["player_id"].to_list()

    rows: list[dict] = []
    for pid in top_ids:
        raw = fetch(player_id=pid, season=_season_str(season), league_id=league_id, return_parsed=False)
        parsed = parse_nba_stats_result_sets(raw)
        frame = parsed if isinstance(parsed, pl.DataFrame) else None
        if frame is None and isinstance(parsed, dict):
            for candidate in parsed.values():
                if isinstance(candidate, pl.DataFrame) and "potential_ast" in candidate.columns:
                    frame = candidate
                    break
        if frame is None or frame.height == 0 or "potential_ast" not in frame.columns:
            continue
        rows.append({"player_id": pid, "potential_assists": float(frame["potential_ast"].sum())})

    if not rows:
        return df.with_columns(pl.lit(None, dtype=pl.Float64).alias("potential_assists"))
    enriched = pl.DataFrame(rows).with_columns(pl.col("player_id").cast(pl.Utf8))
    return df.join(enriched, on="player_id", how="left")


def nba_tracking_pass_value(
    seasons: "int | str | list",
    *,
    league_id: str = "00",
    per_mode: str = "Totals",
    by_position: bool = True,
    positions: Optional[pl.DataFrame] = None,
    fetch_potential_assists: bool = False,
    max_players: int = 0,
    return_as_pandas: bool = False,
    _get_fn: Optional[Callable[..., dict]] = None,
    _pass_get_fn: Optional[Callable[..., dict]] = None,
) -> "Union[pl.DataFrame, pd.DataFrame]":
    """Expected-assists / passer value: ``ast_oe`` per player-season.

    Fetches the ``Passing`` ``leaguedashptstats`` measure (one call) and computes
    ``ast_oe = ast - passes * bucket_assist_rate``. When
    ``fetch_potential_assists=True``, also fetches ``nba_stats_playerdashptpass``
    for the top-``max_players`` passers (capped, optional -- never a hard
    dependency) and recomputes the residual against the richer
    ``potential_assists`` denominator for that subset; ``max_players=0``
    (default) makes exactly one request total. ``ast_pts_created`` is passed
    through directly from the Passing measure (it is already computed there;
    not re-derived).

    Args:
        seasons: A single season or list of seasons.
        league_id: ``"00"`` NBA (default), ``"10"`` WNBA, ``"20"`` G-League.
        per_mode: ``per_mode_simple`` passed to the fetch (default ``"Totals"``).
        by_position: Compute the baseline within role buckets (default);
            ``False`` forces one league-wide bucket.
        positions: Optional pre-fetched positions frame.
        fetch_potential_assists: Enrich the top passers with
            ``playerdashptpass`` potential-assist counts.
        max_players: Cap on per-player enrichment fetches; ``0`` disables
            enrichment regardless of ``fetch_potential_assists``.
        return_as_pandas: Return a :class:`pandas.DataFrame` instead of polars.
        _get_fn: Injectable replacement for ``nba_stats_leaguedashptstats``.
        _pass_get_fn: Injectable replacement for ``nba_stats_playerdashptpass``.

    Returns:
        One row per player-season:
        ``season, player_id, player_name, team_id, position_bucket, gp, min,
        ast, passes, ast_baseline_rate, ast_expected, ast_oe, ast_oe_per_36,
        ast_pts_created, league_id``. Empty/malformed input returns a
        zero-row frame with this schema.

    Example:
        Quick start::

            from sportsdataverse.nba import nba_tracking_pass_value
            df = nba_tracking_pass_value(2024)
            print(df.sort("ast_oe", descending=True).head())

        With potential-assist enrichment for the top 50 passers::

            df = nba_tracking_pass_value(2024, fetch_potential_assists=True, max_players=50)

        See Also:
            * `nba_api`_ -- Python NBA/WNBA stats API client

        .. _nba_api: https://github.com/swar/nba_api
    """
    season_list = _as_season_list(seasons)
    spec = MEASURE_SPECS["ast"]

    frames = []
    for season in season_list:
        fetched = _fetch_leaguedash_tracking(
            season, spec.measure, league_id=league_id, per_mode=per_mode, _get_fn=_get_fn
        )
        if fetched.height == 0:
            continue
        if spec.denom in fetched.columns and "passes" not in fetched.columns:
            fetched = fetched.rename({spec.denom: "passes"})
        frames.append(fetched.with_columns(pl.lit(_season_label(season)).alias("season")))

    if not frames:
        out = pl.DataFrame(schema=_AST_OE_SCHEMA)
        return out.to_pandas() if return_as_pandas else out

    df = pl.concat(frames, how="diagonal_relaxed")

    if by_position:
        df = _attach_role_bucket(df, season_list[0], league_id=league_id, positions=positions)
        group_cols: "list[str]" = ["position_bucket"]
    else:
        df = df.with_columns(pl.lit("all").alias("position_bucket"))
        group_cols = []

    denom_col = "passes"
    if fetch_potential_assists and max_players > 0 and "passes" in df.columns:
        df = _enrich_potential_assists(
            df, season_list[0], league_id=league_id, max_players=max_players, _pass_get_fn=_pass_get_fn
        )
        if "potential_assists" in df.columns:
            denom_col = "potential_assists"

    out = _over_expected(df, actual="ast", denom=denom_col, group_cols=group_cols, out_prefix="ast")
    out = out.with_columns(
        pl.when(pl.col("min") > 0)
        .then(pl.col("ast_oe") / (pl.col("min") / 36.0))
        .otherwise(None)
        .alias("ast_oe_per_36")
    )
    out = out.with_columns(pl.lit(league_id).alias("league_id"))

    out = _finalize_schema(out, _AST_OE_SCHEMA)
    return out.to_pandas() if return_as_pandas else out


def _zscore_within(df: pl.DataFrame, num: str, denom: str, group_cols: "list[str]", out_col: str) -> pl.DataFrame:
    """Add ``out_col`` = z-score of ``num/denom`` within each ``group_cols`` bucket.

    Null-safe: rows with ``denom <= 0`` or a zero-variance bucket get a null
    z-score rather than a division error.

    Args:
        df: Input frame carrying *num* and *denom*.
        num: Numerator column.
        denom: Denominator column.
        group_cols: Baseline-scope columns (e.g. ``["position_bucket"]``); empty
            computes one league-wide z-score.
        out_col: Name of the output z-score column.

    Returns:
        *df* with *out_col* added.
    """
    ratio_col = "__zscore_ratio"
    df = df.with_columns(
        pl.when(pl.col(denom) > 0).then(pl.col(num).cast(pl.Float64) / pl.col(denom)).otherwise(None).alias(ratio_col)
    )
    gb = group_cols or []
    mean_expr = pl.col(ratio_col).mean().alias("__zscore_mean")
    std_expr = pl.col(ratio_col).std().alias("__zscore_std")
    if gb:
        stats = df.group_by(gb).agg(mean_expr, std_expr)
        df = df.join(stats, on=gb, how="left")
    else:
        stats = df.select(mean_expr, std_expr)
        df = df.join(stats, how="cross")
    df = df.with_columns(
        pl.when(pl.col("__zscore_std") > 0)
        .then((pl.col(ratio_col) - pl.col("__zscore_mean")) / pl.col("__zscore_std"))
        .otherwise(None)
        .alias(out_col)
    )
    return df.drop([ratio_col, "__zscore_mean", "__zscore_std"])


# ---------------------------------------------------------------------------
# Phase 3 -- drive value & rim-pressure
# ---------------------------------------------------------------------------

_DRIVE_OE_SCHEMA: "dict[str, pl.DataType]" = {
    "season": pl.Int64,
    "player_id": pl.Utf8,
    "player_name": pl.Utf8,
    "team_id": pl.Utf8,
    "position_bucket": pl.Utf8,
    "gp": pl.Int64,
    "min": pl.Float64,
    "drives": pl.Float64,
    "drive_pts": pl.Float64,
    "drive_baseline_rate": pl.Float64,
    "drive_expected": pl.Float64,
    "drive_pts_oe": pl.Float64,
    "drive_pts_oe_per_36": pl.Float64,
    "drive_fta": pl.Float64,
    "rim_pressure": pl.Float64,
    "drive_ast": pl.Float64,
    "drive_tov": pl.Float64,
    "league_id": pl.Utf8,
}


def nba_tracking_drive_value(
    seasons: "int | str | list",
    *,
    league_id: str = "00",
    per_mode: str = "Totals",
    by_position: bool = True,
    positions: Optional[pl.DataFrame] = None,
    return_as_pandas: bool = False,
    _get_fn: Optional[Callable[..., dict]] = None,
) -> "Union[pl.DataFrame, pd.DataFrame]":
    """Drive value over expected + rim-pressure, per player-season.

    Fetches the ``Drives`` ``leaguedashptstats`` measure and computes
    ``drive_pts_oe = drive_pts - drives * bucket_pts_per_drive``. ``rim_pressure``
    is the z-score of ``drive_fta / drives`` within the player's role bucket
    (a proxy for foul-drawing pressure independent of scoring efficiency).
    ``drive_ast``/``drive_tov`` are passed through unchanged.

    Args:
        seasons: A single season or list of seasons.
        league_id: ``"00"`` NBA (default), ``"10"`` WNBA, ``"20"`` G-League.
        per_mode: ``per_mode_simple`` passed to the fetch (default ``"Totals"``).
        by_position: Compute the baseline within role buckets (default);
            ``False`` forces one league-wide bucket.
        positions: Optional pre-fetched positions frame.
        return_as_pandas: Return a :class:`pandas.DataFrame` instead of polars.
        _get_fn: Injectable replacement for ``nba_stats_leaguedashptstats``.

    Returns:
        One row per player-season:
        ``season, player_id, player_name, team_id, position_bucket, gp, min,
        drives, drive_pts, drive_baseline_rate, drive_expected, drive_pts_oe,
        drive_pts_oe_per_36, drive_fta, rim_pressure, drive_ast, drive_tov,
        league_id``. Empty/malformed input returns a zero-row frame with this schema.

    Example:
        Quick start::

            from sportsdataverse.nba import nba_tracking_drive_value
            df = nba_tracking_drive_value(2024)
            print(df.sort("drive_pts_oe", descending=True).head())

        See Also:
            * `nba_api`_ -- Python NBA/WNBA stats API client

        .. _nba_api: https://github.com/swar/nba_api
    """
    season_list = _as_season_list(seasons)
    spec = MEASURE_SPECS["drive"]

    frames = []
    for season in season_list:
        fetched = _fetch_leaguedash_tracking(
            season, spec.measure, league_id=league_id, per_mode=per_mode, _get_fn=_get_fn
        )
        if fetched.height == 0:
            continue
        frames.append(fetched.with_columns(pl.lit(_season_label(season)).alias("season")))

    if not frames:
        out = pl.DataFrame(schema=_DRIVE_OE_SCHEMA)
        return out.to_pandas() if return_as_pandas else out

    df = pl.concat(frames, how="diagonal_relaxed")

    if by_position:
        df = _attach_role_bucket(df, season_list[0], league_id=league_id, positions=positions)
        group_cols: "list[str]" = ["position_bucket"]
    else:
        df = df.with_columns(pl.lit("all").alias("position_bucket"))
        group_cols = []

    out = _over_expected(df, actual=spec.actual, denom=spec.denom, group_cols=group_cols, out_prefix="drive")
    out = out.rename({"drive_oe": "drive_pts_oe"})
    out = out.with_columns(
        pl.when(pl.col("min") > 0)
        .then(pl.col("drive_pts_oe") / (pl.col("min") / 36.0))
        .otherwise(None)
        .alias("drive_pts_oe_per_36")
    )
    if "drive_fta" in out.columns:
        out = _zscore_within(out, "drive_fta", "drives", group_cols, "rim_pressure")
    else:
        out = out.with_columns(pl.lit(None, dtype=pl.Float64).alias("rim_pressure"))
    out = out.with_columns(pl.lit(league_id).alias("league_id"))

    out = _finalize_schema(out, _DRIVE_OE_SCHEMA)
    return out.to_pandas() if return_as_pandas else out


# ---------------------------------------------------------------------------
# Phase 4 -- catch-&-shoot vs pull-up efficiency
# ---------------------------------------------------------------------------

_SHOT_DIET_SCHEMA: "dict[str, pl.DataType]" = {
    "season": pl.Int64,
    "player_id": pl.Utf8,
    "player_name": pl.Utf8,
    "team_id": pl.Utf8,
    "position_bucket": pl.Utf8,
    "cs_fga": pl.Float64,
    "cs_pts": pl.Float64,
    "cs_pts_oe": pl.Float64,
    "pu_fga": pl.Float64,
    "pu_pts": pl.Float64,
    "pu_pts_oe": pl.Float64,
    "shot_diet_delta": pl.Float64,
    "league_id": pl.Utf8,
}


def _fetch_and_score(
    season_list: "list",
    spec_key: str,
    *,
    league_id: str,
    per_mode: str,
    by_position: bool,
    positions: Optional[pl.DataFrame],
    _get_fn: Optional[Callable[..., dict]],
) -> pl.DataFrame:
    """Fetch one measure across *season_list*, attach role bucket, score OE."""
    spec = MEASURE_SPECS[spec_key]
    frames = []
    for season in season_list:
        fetched = _fetch_leaguedash_tracking(
            season, spec.measure, league_id=league_id, per_mode=per_mode, _get_fn=_get_fn
        )
        if fetched.height == 0:
            continue
        frames.append(fetched.with_columns(pl.lit(_season_label(season)).alias("season")))
    if not frames:
        return pl.DataFrame()
    df = pl.concat(frames, how="diagonal_relaxed")
    if by_position:
        df = _attach_role_bucket(df, season_list[0], league_id=league_id, positions=positions)
    else:
        df = df.with_columns(pl.lit("all").alias("position_bucket"))
    group_cols = ["position_bucket"] if by_position else []
    out = _over_expected(df, actual=spec.actual, denom=spec.denom, group_cols=group_cols, out_prefix=spec.out_prefix)
    # Rename the source actual/denom columns to the short documented output
    # names (e.g. catch_shoot_pts -> cs_pts) -- MEASURE_SPECS.actual/denom are
    # the real leaguedashptstats column names, not the short output prefix.
    rename_map = {spec.denom: f"{spec.out_prefix}_fga", spec.actual: f"{spec.out_prefix}_pts"}
    rename_map = {k: v for k, v in rename_map.items() if k in out.columns and k != v}
    return out.rename(rename_map) if rename_map else out


def nba_tracking_shot_diet_value(
    seasons: "int | str | list",
    *,
    league_id: str = "00",
    per_mode: str = "Totals",
    by_position: bool = True,
    positions: Optional[pl.DataFrame] = None,
    return_as_pandas: bool = False,
    _get_fn: Optional[Callable[..., dict]] = None,
) -> "Union[pl.DataFrame, pd.DataFrame]":
    """Catch-&-shoot vs pull-up points-over-expected, per player-season.

    Fetches ``CatchShoot`` and ``PullUpShot`` (two calls), scores each with the
    shared engine, joins on ``player_id`` (dtype-asserted ``Utf8`` both sides
    first), and computes ``shot_diet_delta = (cs_pts_oe / cs_fga) -
    (pu_pts_oe / pu_fga)`` (null-safe on zero attempts) -- positive means the
    player's efficiency edge comes from catch-&-shoot, negative from
    off-the-dribble.

    Args:
        seasons: A single season or list of seasons.
        league_id: ``"00"`` NBA (default), ``"10"`` WNBA, ``"20"`` G-League.
        per_mode: ``per_mode_simple`` passed to each fetch (default ``"Totals"``).
        by_position: Compute each measure's baseline within role buckets
            (default); ``False`` forces one league-wide bucket.
        positions: Optional pre-fetched positions frame.
        return_as_pandas: Return a :class:`pandas.DataFrame` instead of polars.
        _get_fn: Injectable replacement for ``nba_stats_leaguedashptstats``,
            dispatched by the ``pt_measure_type`` kwarg for each of the two calls.

    Returns:
        One row per player-season:
        ``season, player_id, player_name, team_id, position_bucket, cs_fga,
        cs_pts, cs_pts_oe, pu_fga, pu_pts, pu_pts_oe, shot_diet_delta,
        league_id``. Empty/malformed input returns a zero-row frame with this schema.

    Example:
        Quick start::

            from sportsdataverse.nba import nba_tracking_shot_diet_value
            df = nba_tracking_shot_diet_value(2024)
            print(df.sort("cs_pts_oe", descending=True).head())

        See Also:
            * `nba_api`_ -- Python NBA/WNBA stats API client

        .. _nba_api: https://github.com/swar/nba_api
    """
    season_list = _as_season_list(seasons)

    cs = _fetch_and_score(
        season_list,
        "cs",
        league_id=league_id,
        per_mode=per_mode,
        by_position=by_position,
        positions=positions,
        _get_fn=_get_fn,
    )
    pu = _fetch_and_score(
        season_list,
        "pu",
        league_id=league_id,
        per_mode=per_mode,
        by_position=by_position,
        positions=positions,
        _get_fn=_get_fn,
    )

    if cs.height == 0 and pu.height == 0:
        out = pl.DataFrame(schema=_SHOT_DIET_SCHEMA)
        return out.to_pandas() if return_as_pandas else out

    if "cs_oe" in cs.columns:
        cs = cs.rename({"cs_oe": "cs_pts_oe"})
    if "pu_oe" in pu.columns:
        pu = pu.rename({"pu_oe": "pu_pts_oe"})

    identity_cols = ["season", "player_id", "player_name", "team_id", "position_bucket"]
    if cs.height > 0 and pu.height > 0:
        assert cs.schema["player_id"] == pu.schema["player_id"], "player_id dtype mismatch before cs/pu join"
        join_keys = [c for c in identity_cols if c in cs.columns and c in pu.columns]
        pu_extra = pu.select([*join_keys, "pu_fga", "pu_pts", "pu_pts_oe"])
        out = cs.join(pu_extra, on=join_keys, how="full", coalesce=True)
    elif cs.height > 0:
        out = cs
    else:
        out = pu

    for col in ("cs_fga", "cs_pts", "cs_pts_oe", "pu_fga", "pu_pts", "pu_pts_oe"):
        if col not in out.columns:
            out = out.with_columns(pl.lit(None, dtype=pl.Float64).alias(col))

    out = out.with_columns(
        pl.when((pl.col("cs_fga") > 0) & (pl.col("pu_fga") > 0))
        .then((pl.col("cs_pts_oe") / pl.col("cs_fga")) - (pl.col("pu_pts_oe") / pl.col("pu_fga")))
        .otherwise(None)
        .alias("shot_diet_delta")
    )
    out = out.with_columns(pl.lit(league_id).alias("league_id"))

    out = _finalize_schema(out, _SHOT_DIET_SCHEMA)
    return out.to_pandas() if return_as_pandas else out
