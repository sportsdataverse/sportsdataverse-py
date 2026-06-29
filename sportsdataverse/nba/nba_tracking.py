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
from typing import Final

import polars as pl

__all__ = ["TRACKING_ENTITY_KEYS", "aggregate_tracking_frames"]

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

# String-type column names that carry identity info (not numeric, not key)
_IDENTITY_DTYPES: Final[frozenset[type]] = frozenset({pl.String, pl.Utf8, pl.Categorical})


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
    """Return numerator / denominator as Float64; null when denominator == 0."""
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

    # Drop non-recomputable *_pct columns that ended up in the frame
    # (recomputable ones were added via with_columns; the rest were never included
    # in the agg_exprs, so they are already absent — guard with an existence check)
    recomputable_pcts = set(fg_pct_cols) | set(ft_pct_cols)
    existing_cols = set(grouped.columns)
    cols_to_drop = [c for c in pct_cols if c not in recomputable_pcts and c in existing_cols]
    if cols_to_drop:
        grouped = grouped.drop(cols_to_drop)

    return grouped
