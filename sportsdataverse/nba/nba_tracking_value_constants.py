"""Tracking-value shared config + offline oracle helpers (league-agnostic).

Baselines are COMPUTED from the scored slice at call time; nothing here is a
fitted coefficient. Only structural data lives here: league ids, the per-measure
column map, the role taxonomy, and the known-elite rank-sanity allowlists.
"""

from __future__ import annotations

from dataclasses import dataclass, field

import polars as pl

__all__ = [
    "LEAGUE_IDS",
    "ROLE_BUCKETS",
    "MeasureSpec",
    "MEASURE_SPECS",
    "ELITE_ORACLE",
    "residual_sums_to_zero",
    "top_k_ids",
]

LEAGUE_IDS: dict[str, str] = {"nba": "00", "wnba": "10", "gleague": "20"}
ROLE_BUCKETS: dict[str, list[str]] = {"nba": ["guard", "wing", "big"], "wnba": ["guard", "wing", "big"]}


@dataclass(frozen=True)
class MeasureSpec:
    """Per-model column map for a ``leaguedashptstats`` measure.

    Attributes:
        measure: ``pt_measure_type`` sent to ``nba_stats_leaguedashptstats``.
        actual: Realized-outcome column (snake_case).
        denom: Opportunity column (snake_case).
        out_prefix: Output-column prefix, e.g. ``"reb"`` -> ``reb_oe``.
        extra_denoms: Difficulty buckets: ``label -> (actual_col, denom_col)``.
    """

    measure: str
    actual: str
    denom: str
    out_prefix: str
    extra_denoms: dict[str, tuple[str, str]] = field(default_factory=dict)


MEASURE_SPECS: dict[str, MeasureSpec] = {
    "reb": MeasureSpec(
        "Rebounding",
        "reb",
        "reb_chances",
        "reb",
        {
            "contested": ("reb_contest", "reb_contest_chances"),
            "uncontested": ("reb_uncontest", "reb_uncontest_chances"),
        },
    ),
    "ast": MeasureSpec("Possessions", "ast", "passes", "ast"),
    "drive": MeasureSpec("Drives", "drive_pts", "drives", "drive"),
    "cs": MeasureSpec("CatchShoot", "catch_shoot_pts", "catch_shoot_fga", "cs"),
    "pu": MeasureSpec("PullUpShot", "pull_up_pts", "pull_up_fga", "pu"),
    "touch": MeasureSpec("Possessions", "pts", "touches", "touch"),
    "rim": MeasureSpec("Defense", "d_fgm", "d_fga", "rim"),
}

# Utf8 player_ids of consensus-elite players per category for the 2023-24 gate.
# Frozen in Task 0.3 from public consensus; the rank-sanity oracle asserts each
# appears in the model's top-K. NEVER edit to make a gate pass -- debug the model.
ELITE_ORACLE: dict[str, dict[str, list[str]]] = {"2023-24": {}}


def residual_sums_to_zero(df: pl.DataFrame, oe_col: str, group_cols: list[str], tol: float = 1e-6) -> bool:
    """Return True iff *oe_col* sums to ~0 within every group of *group_cols*.

    This is the construction-invariant oracle: an attempts-weighted baseline
    guarantees ``Σ(realized) == Σ(expected)`` within each bucket, so
    ``Σ(over_expected)`` must be ~0. A failure means a join dropped rows, a
    denominator leaked across buckets, or a null crept into the sum.

    Args:
        df: Frame carrying *oe_col* and *group_cols*.
        oe_col: The over-expected residual column to check.
        group_cols: Baseline-scope columns (e.g. ``["position_bucket"]``).
        tol: Absolute tolerance for the per-group sum.

    Returns:
        ``True`` when *df* is empty or missing *oe_col* (nothing to violate),
        or when every group's residual sum is within *tol* of zero.

    Example:
        Quick start::

            import polars as pl
            from sportsdataverse.nba.nba_tracking_value_constants import residual_sums_to_zero

            df = pl.DataFrame({"b": ["g", "g"], "oe": [1.0, -1.0]})
            assert residual_sums_to_zero(df, "oe", ["b"])
    """
    if df.height == 0 or oe_col not in df.columns:
        return True
    if group_cols:
        g = df.group_by(group_cols).agg(pl.col(oe_col).sum().alias("s"))
    else:
        g = df.select(pl.col(oe_col).sum().alias("s"))
    return bool((g["s"].abs() < tol).all())


def top_k_ids(df: pl.DataFrame, value_col: str, id_col: str = "player_id", k: int = 25) -> list[str]:
    """Return the top-*k* ``id_col`` values sorted descending by *value_col*.

    Args:
        df: Frame carrying *id_col* and *value_col*.
        value_col: Column to sort descending by.
        id_col: Identity column to return (default ``"player_id"``).
        k: Number of ids to return.

    Returns:
        List of the top-*k* ids, highest *value_col* first.

    Example:
        Quick start::

            import polars as pl
            from sportsdataverse.nba.nba_tracking_value_constants import top_k_ids

            df = pl.DataFrame({"player_id": ["a", "b", "c"], "v": [3.0, 1.0, 2.0]})
            assert top_k_ids(df, "v", k=2) == ["a", "c"]
    """
    return df.sort(value_col, descending=True).head(k)[id_col].to_list()
