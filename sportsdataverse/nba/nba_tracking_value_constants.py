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


# Column names below are confirmed against a real 2023-24 capture
# (tests/fixtures/nba_stats/tracking/, see its README) -- NOT the design doc's
# guesses. Two corrections from the original design:
#   * assists live on the "Passing" pt_measure_type, not "Possessions"
#     ("Possessions" carries no ast/passes columns at all).
#   * "Defense" only exposes rim-band defended shooting (``def_rim_*``); there
#     is no separate overall ``d_fga``/``d_fg_pct``, so the rim model reads
#     directly off ``def_rim_*`` -- it is already rim-only, no extra filtering
#     needed.
# reb's contest/uncontest CHANCE columns (as opposed to made-rebound counts)
# do not exist on this endpoint either -- extra_denoms is intentionally empty
# and the model degrades to the plain (actual, denom) rate (see
# _expected_from_difficulty in nba_tracking_value.py).
MEASURE_SPECS: dict[str, MeasureSpec] = {
    "reb": MeasureSpec("Rebounding", "reb", "reb_chances", "reb", {}),
    "ast": MeasureSpec("Passing", "ast", "passes_made", "ast"),
    "drive": MeasureSpec("Drives", "drive_pts", "drives", "drive"),
    "cs": MeasureSpec("CatchShoot", "catch_shoot_pts", "catch_shoot_fga", "cs"),
    "pu": MeasureSpec("PullUpShot", "pull_up_pts", "pull_up_fga", "pu"),
    "touch": MeasureSpec("Possessions", "points", "touches", "touch"),
    "rim": MeasureSpec("Defense", "def_rim_fgm", "def_rim_fga", "rim"),
}

# Utf8 player_ids of consensus-elite players per category for the 2023-24 gate.
# Frozen in Task 0.3 from public consensus (cross-checked against the raw
# leaders -- reb_chances / ast / drive_pts / catch_shoot_pts / points /
# def_rim_fga -- in the committed fixtures); the rank-sanity oracle asserts
# each appears in the model's top-K. NEVER edit to make a gate pass -- debug
# the model.
ELITE_ORACLE: dict[str, dict[str, list[str]]] = {
    "2023-24": {
        # Elite rebounders: Sabonis, Gobert, A. Davis, Jokic, Nurkic, J. Allen,
        # Giannis, Vucevic, Capela, Wembanyama.
        "reb": [
            "1627734",
            "203497",
            "203076",
            "203999",
            "203994",
            "1628386",
            "203507",
            "202696",
            "203991",
            "1641705",
        ],
        # Elite passers: Haliburton, Jokic, Doncic, Sabonis, Harden, VanVleet,
        # Trae Young, LeBron, Brunson, Dejounte Murray.
        "ast": [
            "1630169",
            "203999",
            "1629029",
            "1627734",
            "201935",
            "1627832",
            "1629027",
            "2544",
            "1628973",
            "1627749",
        ],
        # Elite drivers: SGA, Brunson, Doncic, Zion, A. Edwards, DeRozan,
        # De'Aaron Fox, Giannis, Dejounte Murray, Maxey.
        "drive": [
            "1628983",
            "1628973",
            "1629029",
            "1629627",
            "1630162",
            "201942",
            "1628368",
            "203507",
            "1627749",
            "1630178",
        ],
        # Elite catch-and-shoot shooters: DiVincenzo, Klay Thompson, MPJ,
        # Bogdanovic, Curry, Beasley, Hauser, Bridges, Markkanen, Hield.
        "shot": [
            "1628978",
            "202691",
            "1629008",
            "203992",
            "201939",
            "1627736",
            "1630573",
            "1628969",
            "1628374",
            "1627741",
        ],
        # Elite high-usage-efficient scorers: Jokic, Giannis, SGA, Doncic,
        # Curry, Durant, Tatum, DeRozan, A. Davis, Booker.
        "touch": [
            "203999",
            "203507",
            "1628983",
            "1629029",
            "201939",
            "201142",
            "1628369",
            "201942",
            "203076",
            "1626164",
        ],
        # Elite rim protectors: Gobert, Holmgren, Turner, Wembanyama, J. Allen,
        # B. Lopez, Nurkic, A. Davis, Claxton, Gafford.
        "rim": [
            "203497",
            "1631096",
            "1626167",
            "1641705",
            "1628386",
            "201572",
            "203994",
            "203076",
            "1629651",
            "1629655",
        ],
    }
}


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
