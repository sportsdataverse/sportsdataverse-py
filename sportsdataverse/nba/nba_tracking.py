"""nba_tracking — multi-season tracking aggregation engine.

Aggregates ``leaguedashptstats`` (and any compatible tracking) frames
across seasons or split slices.  Column discovery is fully data-driven:

* **Additive numeric columns** — numeric dtype, name does NOT end in
  ``_pct``, not the ``entity_key``, not a string identity column.
* **Recomputable rate columns** — any ``*_fg_pct`` recomputed as
  Σ``*_fgm`` / Σ``*_fga``; any ``*_ft_pct`` recomputed as
  Σ``*_ftm`` / Σ``*_fta``.  Denominator == 0 → ``null``.
* **Dropped columns** — all remaining ``*_pct`` columns (non-recomputable
  "% of total" rates whose denominator is not present in the frame).
* **Identity columns** — string-typed columns that are not the
  ``entity_key``; kept via ``first()`` per entity.

The engine never raises on empty or malformed input: an empty list or a
list of zero-row frames returns a zero-row ``pl.DataFrame``.

Example:
    Quick start::

        import json, pathlib
        import polars as pl
        from sportsdataverse.nba.nba_stats_parsers import parse_nba_stats_result_sets
        from sportsdataverse.nba.nba_tracking import aggregate_tracking_frames, TRACKING_ENTITY_KEYS

        raw_2324 = json.loads(pathlib.Path("leaguedashptstats_drives_player_2324.json").read_text())
        raw_2223 = json.loads(pathlib.Path("leaguedashptstats_drives_player_2223.json").read_text())
        frames = [parse_nba_stats_result_sets(r) for r in (raw_2223, raw_2324)]
        agg = aggregate_tracking_frames(frames, entity_key=TRACKING_ENTITY_KEYS["Player"])
        print(agg.shape)

    Team-level aggregation::

        agg_team = aggregate_tracking_frames(frames, entity_key=TRACKING_ENTITY_KEYS["Team"])
"""

from __future__ import annotations

import re
from collections.abc import Sequence
from typing import TYPE_CHECKING, Final, Literal, Union, overload

import polars as pl

from sportsdataverse.nba import nba_stats
from sportsdataverse.nba.nba_stats_parsers import parse_nba_stats_result_sets

if TYPE_CHECKING:
    import pandas as pd

__all__ = [
    "TRACKING_ENTITY_KEYS",
    "aggregate_tracking_frames",
    "nba_tracking_aggregate",
    "_fetch_ptstats",
]

# ---------------------------------------------------------------------------
# Public constants
# ---------------------------------------------------------------------------

TRACKING_ENTITY_KEYS: Final[dict[str, str]] = {
    "Player": "player_id",
    "Team": "team_id",
}

# Pattern that identifies a recomputable field-goal percentage:
#   <prefix>_fg_pct  →  needs <prefix>_fgm / <prefix>_fga
_FG_PCT_RE: Final[re.Pattern[str]] = re.compile(r"^(.+)_fg_pct$")

# Pattern that identifies a recomputable free-throw percentage:
#   <prefix>_ft_pct  →  needs <prefix>_ftm / <prefix>_fta
_FT_PCT_RE: Final[re.Pattern[str]] = re.compile(r"^(.+)_ft_pct$")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _is_numeric(dtype: pl.DataType) -> bool:
    """Return True when *dtype* is any numeric polars type."""
    return dtype.is_numeric()


def _null_safe_divide(
    numerator: pl.Expr,
    denominator: pl.Expr,
) -> pl.Expr:
    """Return numerator / denominator as Float64; null when denominator is 0 or null.

    The explicit ``denominator == 0`` guard handles the zero case; polars null
    propagation handles a null denominator (the division yields null on its own).
    """
    return pl.when(denominator == 0).then(None).otherwise(numerator.cast(pl.Float64) / denominator.cast(pl.Float64))


def _classify_columns(
    schema: pl.Schema,
    entity_key: str,
) -> tuple[list[str], list[str], list[str]]:
    """Return (additive_cols, identity_cols, pct_cols) from *schema*.

    Args:
        schema: The polars schema of the concatenated frame.
        entity_key: The group-by key column (excluded from all lists).

    Returns:
        A 3-tuple:
        * ``additive_cols`` — numeric, non-``*_pct``, non-key columns to SUM.
        * ``identity_cols`` — string/categorical non-key columns to keep via ``first()``.
        * ``pct_cols`` — all ``*_pct`` columns (will be dropped or recomputed).
    """
    additive: list[str] = []
    identity: list[str] = []
    pct: list[str] = []

    for col_name, dtype in schema.items():
        if col_name == entity_key:
            continue
        if col_name.endswith("_pct"):
            pct.append(col_name)
        elif _is_numeric(dtype):
            additive.append(col_name)
        else:
            # String / Categorical / other non-numeric → identity
            identity.append(col_name)

    return additive, identity, pct


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def aggregate_tracking_frames(
    frames: list[pl.DataFrame],
    *,
    entity_key: str,
) -> pl.DataFrame:
    """Aggregate tracking frames across seasons (or any split slices).

    Concatenates *frames*, groups by *entity_key*, sums additive numeric
    columns, recomputes ``*_fg_pct`` / ``*_ft_pct`` from summed
    makes/attempts, and drops all other ``*_pct`` columns.

    The function is column-driven: it works for any ``leaguedashptstats``
    measure type (Drives, SpeedDistance, Touches, …) — the additive and
    recomputable sets are discovered from the frame schema, not hardcoded.

    Args:
        frames: List of tidy polars DataFrames, each from a single
            ``(season, season_type, pt_measure_type)`` call with
            ``per_mode_simple="Totals"``.  May be empty or contain
            zero-row frames — both return a zero-row DataFrame.
        entity_key: Column to group by.  Use
            ``TRACKING_ENTITY_KEYS["Player"]`` (``"player_id"``) or
            ``TRACKING_ENTITY_KEYS["Team"]`` (``"team_id"``).

    Returns:
        A polars DataFrame with one row per entity:
        * Additive numeric columns summed across all input frames.
        * ``*_fg_pct`` and ``*_ft_pct`` recomputed from summed
          makes/attempts (``null`` when denominator is 0).
        * All other ``*_pct`` columns dropped.
        * Identity string columns (``player_name``, ``team_abbreviation``,
          …) kept via ``first()`` per entity.

    Example:
        Two-season player aggregation::

            agg = aggregate_tracking_frames(
                [frame_2223, frame_2324],
                entity_key="player_id",
            )
            print(agg.head())

        Never raises on empty input::

            empty = aggregate_tracking_frames([], entity_key="player_id")
            print(empty.shape)  # (0, 0)
    """
    # Never-raise: filter out None entries defensively
    valid: list[pl.DataFrame] = [f for f in frames if f is not None and f.shape[0] > 0]

    if not valid:
        return pl.DataFrame()

    combined: pl.DataFrame = pl.concat(valid, how="diagonal_relaxed")

    if combined.shape[0] == 0:
        return pl.DataFrame()

    schema = combined.schema
    additive_cols, identity_cols, pct_cols = _classify_columns(schema, entity_key)

    # Identify recomputable pct columns and their component pair names
    fg_pct_cols: dict[str, tuple[str, str]] = {}  # pct_col → (fgm_col, fga_col)
    ft_pct_cols: dict[str, tuple[str, str]] = {}  # pct_col → (ftm_col, fta_col)

    for pct_col in pct_cols:
        m_fg = _FG_PCT_RE.match(pct_col)
        if m_fg:
            prefix = m_fg.group(1)
            fgm_col = f"{prefix}_fgm"
            fga_col = f"{prefix}_fga"
            if fgm_col in schema and fga_col in schema:
                fg_pct_cols[pct_col] = (fgm_col, fga_col)
            continue

        m_ft = _FT_PCT_RE.match(pct_col)
        if m_ft:
            prefix = m_ft.group(1)
            ftm_col = f"{prefix}_ftm"
            fta_col = f"{prefix}_fta"
            if ftm_col in schema and fta_col in schema:
                ft_pct_cols[pct_col] = (ftm_col, fta_col)

    # Build the group_by aggregation expressions
    agg_exprs: list[pl.Expr] = []

    for col in additive_cols:
        agg_exprs.append(pl.col(col).sum())

    for col in identity_cols:
        agg_exprs.append(pl.col(col).first())

    grouped: pl.DataFrame = combined.group_by(entity_key).agg(agg_exprs)

    # Recompute fg_pct / ft_pct from the summed makes/attempts
    recompute_exprs: list[pl.Expr] = []

    for pct_col, (fgm_col, fga_col) in fg_pct_cols.items():
        recompute_exprs.append(_null_safe_divide(pl.col(fgm_col), pl.col(fga_col)).alias(pct_col))

    for pct_col, (ftm_col, fta_col) in ft_pct_cols.items():
        recompute_exprs.append(_null_safe_divide(pl.col(ftm_col), pl.col(fta_col)).alias(pct_col))

    if recompute_exprs:
        grouped = grouped.with_columns(recompute_exprs)

    # Drop non-recomputable *_pct columns.
    # In the expected path this list is empty: the non-recomputable *_pct cols were
    # never added to agg_exprs, so they are already absent after the group_by. The
    # `c in existing_cols` filter is therefore defensive (dead in the normal path) —
    # it keeps the drop safe for any future measure type whose schema routes a *_pct
    # column into the grouped frame.
    recomputable_pcts = set(fg_pct_cols) | set(ft_pct_cols)
    existing_cols = set(grouped.columns)
    cols_to_drop = [c for c in pct_cols if c not in recomputable_pcts and c in existing_cols]
    if cols_to_drop:
        grouped = grouped.drop(cols_to_drop)

    return grouped


# ---------------------------------------------------------------------------
# Public fetcher
# ---------------------------------------------------------------------------


def _fetch_ptstats(
    season: str,
    season_type: str,
    measure_type: str,
    player_or_team: str,
    league_id: str,
) -> dict:
    """Fetch a single ``leaguedashptstats`` slice and return the raw payload.

    This thin wrapper is a module-level function (not a closure) so tests can
    monkeypatch it without touching the real ``nba_stats`` wrapper.

    Args:
        season: NBA season string, e.g. ``"2023-24"``.
        season_type: Season type, e.g. ``"Regular Season"`` or ``"Playoffs"``.
        measure_type: Tracking measure type, e.g. ``"Drives"`` or
            ``"SpeedDistance"``.
        player_or_team: ``"Player"`` or ``"Team"``.
        league_id: League identifier; ``"00"`` for NBA, ``"20"`` for G-League,
            ``"10"`` for WNBA.

    Returns:
        Raw JSON payload as a ``dict`` (the ``return_parsed=False`` response
        from ``nba_stats_leaguedashptstats``).
    """
    return nba_stats.nba_stats_leaguedashptstats(
        season=season,
        season_type_all_star=season_type,
        pt_measure_type=measure_type,
        per_mode_simple="Totals",
        player_or_team=player_or_team,
        league_id=league_id,
        return_parsed=False,
    )


@overload
def nba_tracking_aggregate(
    measure_type: str = ...,
    player_or_team: str = ...,
    seasons: Sequence[str] = ...,
    season_types: Sequence[str] = ...,
    *,
    league_id: str = ...,
    return_as_pandas: Literal[False],
) -> pl.DataFrame: ...


@overload
def nba_tracking_aggregate(
    measure_type: str = ...,
    player_or_team: str = ...,
    seasons: Sequence[str] = ...,
    season_types: Sequence[str] = ...,
    *,
    league_id: str = ...,
    return_as_pandas: Literal[True],
) -> pd.DataFrame: ...


@overload
def nba_tracking_aggregate(
    measure_type: str = ...,
    player_or_team: str = ...,
    seasons: Sequence[str] = ...,
    season_types: Sequence[str] = ...,
    *,
    league_id: str = ...,
    return_as_pandas: bool = ...,
) -> Union[pl.DataFrame, pd.DataFrame]: ...


def nba_tracking_aggregate(
    measure_type: str = "Drives",
    player_or_team: str = "Player",
    seasons: Sequence[str] = ("2023-24",),
    season_types: Sequence[str] = ("Regular Season",),
    *,
    league_id: str = "00",
    return_as_pandas: bool = False,
) -> Union[pl.DataFrame, pd.DataFrame]:
    """Aggregate ``leaguedashptstats`` tracking data across seasons and season types.

    For each ``(season, season_type)`` combination, fetches raw Totals data
    from ``stats.nba.com``, parses it into a tidy frame, then aggregates all
    frames via :func:`aggregate_tracking_frames`.  Additive counting columns
    are summed; ``*_fg_pct`` / ``*_ft_pct`` columns are recomputed from the
    summed makes and attempts; all other ``*_pct`` columns (e.g.
    ``drive_pts_pct``, ``drive_passes_pct``) are dropped because their
    denominators are not present in the tracking frame and are therefore
    not correctly aggregatable.

    .. note::
        **Totals only** — the wrapper is always called with
        ``per_mode_simple="Totals"``.  PerGame or Per36 modes would require
        a different aggregation strategy (weighted averaging, not summation)
        and are not supported in this phase.

        **``leaguedashptstats`` measure types only** — supported values
        include ``"Drives"``, ``"SpeedDistance"``, ``"Touches"``,
        ``"Passing"``, ``"ElbowTouch"``, ``"PostTouch"``, ``"PaintTouch"``
        and others from the ``pt_measure_type`` parameter.  Other tracking
        endpoints (``leaguedashptdefend``, hustle, shot-zone dashboards) are
        a planned follow-up (P3.1) and reuse the same engine.

    Args:
        measure_type: Tracking measure type, e.g. ``"Drives"`` (default),
            ``"SpeedDistance"``, ``"Touches"``.  Passed directly to
            ``nba_stats_leaguedashptstats`` as ``pt_measure_type``.
        player_or_team: ``"Player"`` (default) or ``"Team"``.  Determines
            which entity key is used for grouping in
            :func:`aggregate_tracking_frames`.
        seasons: Sequence of NBA season strings to aggregate, e.g.
            ``("2022-23", "2023-24")``.  Default is ``("2023-24",)``.
        season_types: Sequence of season-type strings to aggregate, e.g.
            ``("Regular Season", "Playoffs")``.  Default is
            ``("Regular Season",)``.
        league_id: League identifier.  ``"00"`` for NBA (default),
            ``"20"`` for G-League, ``"10"`` for WNBA.
        return_as_pandas: If ``True``, convert the result to a
            :class:`pandas.DataFrame` before returning.  Default ``False``
            returns a :class:`polars.DataFrame`.

    Returns:
        A :class:`polars.DataFrame` (or :class:`pandas.DataFrame` when
        ``return_as_pandas=True``) with one row per entity (player or team):

        * **Additive numeric columns** (e.g. ``drives``, ``drive_fgm``,
          ``drive_fga``, ``gp``, ``w``, ``l``) summed across all
          ``(season, season_type)`` slices.
        * ``*_fg_pct`` / ``*_ft_pct`` recomputed from summed makes/attempts
          (``null`` when the denominator is 0).
        * ``*_pct`` "percent of total" columns dropped (e.g.
          ``drive_pts_pct``, ``drive_passes_pct``).
        * Identity string columns (e.g. ``player_name``,
          ``team_abbreviation``) kept via ``first()`` per entity.

        Returns a zero-row DataFrame on empty or malformed input without
        raising an exception.

    Example:
        Two-season player Drives aggregation::

            from sportsdataverse.nba.nba_tracking import nba_tracking_aggregate

            df = nba_tracking_aggregate(
                measure_type="Drives",
                player_or_team="Player",
                seasons=("2022-23", "2023-24"),
            )
            print(df.shape)

        Single-season team SpeedDistance::

            df_team = nba_tracking_aggregate(
                measure_type="SpeedDistance",
                player_or_team="Team",
                seasons=("2023-24",),
            )
            print(df_team.columns)

        Pandas output::

            df_pd = nba_tracking_aggregate(seasons=("2023-24",), return_as_pandas=True)
            print(type(df_pd))

        See Also:
            * `nba_api`_ — companion NBA statistics Python client

        .. _nba_api: https://github.com/swar/nba_api
    """
    entity_key = TRACKING_ENTITY_KEYS[player_or_team]

    frames: list[pl.DataFrame] = []
    for season in seasons:
        for season_type in season_types:
            raw = _fetch_ptstats(season, season_type, measure_type, player_or_team, league_id)
            parsed = parse_nba_stats_result_sets(raw)
            frames.append(parsed)

    result = aggregate_tracking_frames(frames, entity_key=entity_key)

    if return_as_pandas:
        return result.to_pandas()
    return result
