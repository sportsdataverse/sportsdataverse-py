"""Cross-provider source-agreement — flag where two feeds disagree.

The SDV thesis is that we hold many sibling providers per sport (ESPN,
stats.nba.com, the NCAA feed, multiple odds books). When two of them describe
the same games, silent disagreement is a data-quality signal worth surfacing
— the adapted role of the source-agreement flags from the feature-engineering
stack.

:func:`reconcile` joins two frames on shared keys and emits one long-form row
per (key, column): the two values, whether they agree (within an optional
numeric tolerance), and the absolute difference. :func:`agreement_summary`
rolls that up per column, and :func:`key_coverage` reports presence
disagreement (a row one provider has and the other lacks — itself a flag).

Join keys get the project's dtype discipline: the reconcile asserts
``left.schema[k] == right.schema[k]`` before joining, so a silent
int-vs-str id mismatch surfaces as an error instead of an empty match.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Sequence

import polars as pl


def _is_numeric(dtype: pl.DataType) -> bool:
    return dtype.is_numeric()


def reconcile(
    left: pl.DataFrame,
    right: pl.DataFrame,
    *,
    keys: Sequence[str],
    compare: Optional[Sequence[str]] = None,
    tol: float = 0.0,
) -> pl.DataFrame:
    """Long-form per-(key, column) agreement between two provider frames.

    Args:
        left: The first provider's frame.
        right: The second provider's frame.
        keys: Join-key columns present in both (the entity identity).
        compare: Columns to compare; defaults to the shared non-key columns.
        tol: Absolute tolerance for numeric agreement (``|a - b| <= tol``).
            Non-numeric columns compare for exact string equality.

    Returns:
        One row per matched key and compared column: the ``keys``, ``column``,
        ``left_value`` / ``right_value`` (stringified), ``agree`` (bool; two
        nulls agree, one null disagrees), and ``abs_diff`` (Float64, null for
        non-numeric columns).

    Raises:
        ValueError: On a missing key, a join-key dtype mismatch, or no
            comparable columns.

    Example:
        Flag box-vs-pbp scoring disagreement::

            from sportsdataverse.modeling.integrity import reconcile, agreement_summary
            recon = reconcile(espn_logs, stats_logs, keys=["game_id", "player_id"],
                              compare=["pts", "reb", "ast"], tol=0.0)
            agreement_summary(recon).filter(pl.col("agree_rate") < 1.0)
    """
    keys = list(keys)
    for k in keys:
        if k not in left.columns or k not in right.columns:
            raise ValueError(f"join key {k!r} missing from {'left' if k not in left.columns else 'right'}")
        if left.schema[k] != right.schema[k]:
            raise ValueError(
                f"join-key dtype mismatch on {k!r}: left={left.schema[k]} right={right.schema[k]} "
                "(fix the id dtype at the boundary before reconciling)"
            )
    if compare is None:
        shared = [c for c in left.columns if c in right.columns and c not in keys]
        compare = shared
    compare = [c for c in compare if c not in keys]
    if not compare:
        raise ValueError("no comparable columns (compare is empty after excluding keys)")

    frames: List[pl.DataFrame] = []
    for col in compare:
        if col not in left.columns or col not in right.columns:
            raise ValueError(f"compare column {col!r} missing from one side")
        joined = left.select(*keys, pl.col(col).alias("_l")).join(
            right.select(*keys, pl.col(col).alias("_r")), on=keys, how="inner"
        )
        both_null = pl.col("_l").is_null() & pl.col("_r").is_null()
        if _is_numeric(left.schema[col]) and _is_numeric(right.schema[col]):
            joined = joined.with_columns((pl.col("_l") - pl.col("_r")).abs().cast(pl.Float64).alias("abs_diff"))
            agree = both_null | (pl.col("abs_diff") <= tol).fill_null(False)
        else:
            joined = joined.with_columns(pl.lit(None, dtype=pl.Float64).alias("abs_diff"))
            eq = (pl.col("_l").cast(pl.Utf8) == pl.col("_r").cast(pl.Utf8)).fill_null(False)
            agree = both_null | eq
        frames.append(
            joined.select(
                *keys,
                pl.lit(col).alias("column"),
                pl.col("_l").cast(pl.Utf8).alias("left_value"),
                pl.col("_r").cast(pl.Utf8).alias("right_value"),
                agree.alias("agree"),
                pl.col("abs_diff"),
            )
        )
    return pl.concat(frames, how="vertical")


def agreement_summary(recon: pl.DataFrame) -> pl.DataFrame:
    """Per-column agreement roll-up of a :func:`reconcile` frame.

    Args:
        recon: The output of :func:`reconcile`.

    Returns:
        One row per compared column: ``column``, ``n`` (matched rows),
        ``n_agree``, ``n_disagree``, ``agree_rate``, ``mean_abs_diff``,
        ``max_abs_diff`` (the numeric-difference stats are null for
        non-numeric columns).

    Example:
        Quick start::

            summary = agreement_summary(recon)
            summary.sort("agree_rate").head()
    """
    if recon.height == 0:
        return pl.DataFrame(
            schema={
                "column": pl.Utf8,
                "n": pl.Int64,
                "n_agree": pl.Int64,
                "n_disagree": pl.Int64,
                "agree_rate": pl.Float64,
                "mean_abs_diff": pl.Float64,
                "max_abs_diff": pl.Float64,
            }
        )
    return (
        recon.group_by("column")
        .agg(
            pl.len().alias("n"),
            pl.col("agree").cast(pl.Int64).sum().alias("n_agree"),
            (pl.col("agree") == False).cast(pl.Int64).sum().alias("n_disagree"),  # noqa: E712
            pl.col("agree").cast(pl.Int64).mean().alias("agree_rate"),
            pl.col("abs_diff").mean().alias("mean_abs_diff"),
            pl.col("abs_diff").max().alias("max_abs_diff"),
        )
        .sort("column")
    )


def key_coverage(
    left: pl.DataFrame,
    right: pl.DataFrame,
    *,
    keys: Sequence[str],
) -> Dict[str, Any]:
    """Presence agreement — keys one provider has and the other lacks.

    Args:
        left: The first provider's frame.
        right: The second provider's frame.
        keys: Join-key columns.

    Returns:
        ``{n_left, n_right, n_shared, only_left, only_right, coverage}`` where
        ``coverage`` is ``n_shared / max(n_left, n_right)`` (1.0 = identical
        key sets).

    Raises:
        ValueError: On a join-key dtype mismatch.

    Example:
        Quick start::

            key_coverage(espn_plays, stats_plays, keys=["game_id", "play_id"])
    """
    keys = list(keys)
    for k in keys:
        if left.schema[k] != right.schema[k]:
            raise ValueError(f"join-key dtype mismatch on {k!r}: left={left.schema[k]} right={right.schema[k]}")
    lk = left.select(keys).unique()
    rk = right.select(keys).unique()
    shared = lk.join(rk, on=keys, how="inner").height
    n_left, n_right = lk.height, rk.height
    return {
        "n_left": n_left,
        "n_right": n_right,
        "n_shared": shared,
        "only_left": n_left - shared,
        "only_right": n_right - shared,
        "coverage": shared / max(n_left, n_right) if max(n_left, n_right) else 1.0,
    }
