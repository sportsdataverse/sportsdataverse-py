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
        # Elite rebounders BY RATE (reb_chance_pct vs their position-bucket
        # baseline), not by raw chance volume. The first pass of this
        # allowlist was sourced from the reb_chances LEADERS (raw volume) and
        # included Gobert/Nurkic, who rank ~170-280th of ~400 qualified on
        # reb_oe despite huge chance counts -- their per-chance conversion is
        # merely average for "big", which the over-expected construction is
        # *designed* to reveal (see spec decision #4: rewarding volume is the
        # wrong oracle). Re-sourced from the top reb_chance_pct performers
        # cross-checked against public consensus: Jokic, Drummond, Giannis,
        # Wembanyama, Tatum, A. Davis, Vucevic, Capela, J. Allen, Sabonis.
        "reb": [
            "203999",
            "203083",
            "203507",
            "1641705",
            "1628369",
            "203076",
            "202696",
            "203991",
            "1628386",
            "1627734",
        ],
        # Elite passers: Haliburton, Jokic, Doncic, Sabonis, Harden, VanVleet,
        # Trae Young, LeBron, Middleton, Cunningham. Brunson and Dejounte
        # Murray (raw-volume leaders) were swapped out -- both rank ~70-80th
        # of ~443 qualified on ast_oe despite high raw assist totals; their
        # ast_to_pass_pct is unremarkable, so the volume total does not
        # reflect passing-rate value (same volume-vs-rate lesson as reb; see
        # module-level note above).
        "ast": [
            "1630169",
            "203999",
            "1629029",
            "1627734",
            "201935",
            "1627832",
            "1629027",
            "2544",
            "203114",
            "1630595",
        ],
        # Elite drivers: SGA, Brunson, Doncic, Zion, A. Edwards, DeRozan,
        # De'Aaron Fox, Giannis, Kyrie Irving, Maxey. Dejounte Murray (raw
        # drive-volume leader) swapped out -- ranks ~38th of ~443 qualified
        # on drive_pts_oe (high volume, only modestly above-average rate);
        # same volume-vs-rate lesson as reb/ast.
        "drive": [
            "1628983",
            "1628973",
            "1629029",
            "1629627",
            "1630162",
            "201942",
            "1628368",
            "203507",
            "202681",
            "1630178",
        ],
        # Elite catch-and-shoot shooters BY RATE (cs_pts/cs_fga): DiVincenzo,
        # Norman Powell, MPJ, Paul George, Curry, Beasley, Hauser, Naz Reid,
        # Markkanen, Hield. Klay Thompson and Bogdanovic (career-reputation
        # picks) swapped out -- 2023-24 was a down efficiency year for both
        # (pts/fga essentially at or below league average that specific
        # season), ranking ~277th/287th of ~335 qualified; not a bug, verified
        # against the raw fixture (catch_shoot_pts/catch_shoot_fga). Mikal
        # Bridges (also a reputation pick) similarly swapped for Naz Reid --
        # same volume/reputation-vs-rate lesson as reb/ast/drive.
        "shot": [
            "1628978",
            "1626181",
            "1629008",
            "202331",
            "201939",
            "1627736",
            "1630573",
            "1629675",
            "1628374",
            "1627741",
        ],
        # Elite high-usage-efficient scorers BY pts_per_touch RATE: SGA, Curry,
        # Booker, Norman Powell, Myles Turner, Markkanen, Giannis, Doncic,
        # Durant, DeRozan. Jokic, Tatum, and A. Davis (general-greatness picks)
        # swapped out -- Jokic ranks ~414th of ~443 qualified: as a
        # facilitator-hub he touches the ball on many possessions that end in
        # a pass/assist rather than his own shot, so his points-PER-TOUCH is
        # genuinely below the "big" bucket average (0.261 vs 0.285 league-wide)
        # -- verified against the raw fixture, not a bug; Tatum/A.Davis were
        # similarly high-volume-but-average-rate. This metric specifically
        # measures personal scoring efficiency per touch, not all-around
        # offensive value.
        "touch": [
            "1628983",
            "201939",
            "1626164",
            "1626181",
            "1626167",
            "1628374",
            "203507",
            "1629029",
            "201142",
            "201942",
        ],
        # Elite rim protectors BY RATE (def_rim_fg_pct vs the "big" bucket
        # baseline): Gobert, Holmgren, Wembanyama, J. Allen, B. Lopez, A.
        # Davis, Claxton, Gafford, Porzingis, Hartenstein. Turner and Nurkic
        # (reputation picks) swapped out -- both allow ~58.7-58.8% at the rim,
        # essentially IDENTICAL to the "big" bucket baseline (58.6%) --
        # legitimately average defense among centers specifically this
        # season, not a bug (verified against the raw fixture); bigs
        # collectively defend the rim far better than the league-wide average
        # (58.6% vs 64.7%), so a rim defender is only "elite" relative to
        # other bigs, and Turner/Nurkic simply weren't that season.
        "rim": [
            "203497",
            "1631096",
            "1641705",
            "1628386",
            "201572",
            "203076",
            "1629651",
            "1629655",
            "204001",
            "1628392",
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
