"""Oracle gates for the shot-value spine (offline, corpus-driven).

The committed 2022-23 corpus is a zone-diverse ~40 ELITE-player set (see the
fixtures README), so applying the LEAGUE baseline to it nets +5.3% (these
shooters beat league average — selection, not miscalibration). The Phase-1
calibration gate therefore tests the model's true INVARIANT — join integrity
+ 2/3 value assignment — via self-calibration (a baseline built from the same
shots must net to ~0), and separately bounds the elite-fixture over-
performance. Observed at gate authorship (2026-07-08): self-calibration
0.00007, league ratio 1.053, split-half reliability 0.699 raw → 0.707 shrunk.
NEVER loosen a gate to pass — debug the model.
"""

from __future__ import annotations

import numpy as np
import polars as pl

from sportsdataverse.nba.nba_shot_value import (
    make_prob_by_context,
    score_shot_xpoints,
    shot_selection_quality,
)
from sportsdataverse.nba.nba_shot_value_constants import (
    get_shrinkage_k,
    points_calibration_error,
    split_half_reliability,
)

_ZONE_KEYS = ["shot_zone_basic", "shot_zone_area", "shot_zone_range"]
ZONE_BANDS = {
    "Restricted Area": (0.58, 0.68),
    "Mid-Range": (0.36, 0.44),
    "Left Corner 3": (0.36, 0.42),
    "Right Corner 3": (0.36, 0.42),
}


# ---------------------------------------------------------------------------
# Phase 1 — xPoints calibration + zone bands
# ---------------------------------------------------------------------------


def test_self_calibration_invariant(shot_value_corpus):
    """A zone baseline built from THESE shots must calibrate them to ~0 --
    guards the zone join (no dropped rows) and the 2/3 value assignment,
    independent of player-selection bias. Observed 0.00007."""
    shots = shot_value_corpus["shots"]
    own = (
        shots.group_by(_ZONE_KEYS)
        .agg(pl.len().alias("fga"), pl.col("shot_made_flag").sum().alias("fgm"))
        .with_columns((pl.col("fgm") / pl.col("fga")).alias("fg_pct"))
    )
    scored = score_shot_xpoints(shots, own)
    assert scored.filter(pl.col("base_fg_pct").is_null()).height == 0, "unmatched zones after fallback"
    err = points_calibration_error(scored["xpoints"].to_numpy(), scored["actual_points"].to_numpy())
    assert err <= 0.005, f"self-calibration {err:.5f} > 0.005 — a zone join dropped rows or a 2/3 value is wrong"


def test_value_assignment_integrity(shot_value_corpus):
    """Every 3PT-zone shot scores value 3, every 2PT-zone shot value 2."""
    scored = score_shot_xpoints(shot_value_corpus["shots"], shot_value_corpus["league_avgs"])
    bad = scored.filter(
        ((pl.col("shot_type").str.starts_with("3")) & (pl.col("shot_value") != 3))
        | ((pl.col("shot_type").str.starts_with("2")) & (pl.col("shot_value") != 2))
    )
    assert bad.height == 0, f"{bad.height} shots with a wrong 2/3 value"


def test_league_baseline_overperformance_bounded(shot_value_corpus):
    """Applying the LEAGUE baseline, the elite fixture scores 0-10% above it
    (these are above-average shooters; the model applies the baseline
    correctly, they simply beat it). Observed ratio 1.053."""
    scored = score_shot_xpoints(shot_value_corpus["shots"], shot_value_corpus["league_avgs"])
    ratio = float(scored["actual_points"].sum() / scored["xpoints"].sum())
    assert 1.0 <= ratio <= 1.10, f"overperformance ratio {ratio:.4f} outside [1.0, 1.10]"


def test_zone_fg_pct_bands(shot_value_corpus):
    la = shot_value_corpus["league_avgs"]
    for zone, (lo, hi) in ZONE_BANDS.items():
        rows = la.filter(pl.col("shot_zone_basic") == zone)
        if rows.height == 0:
            continue
        agg = rows.select((pl.col("fgm").sum() / pl.col("fga").sum()).alias("p")).row(0, named=True)["p"]
        assert lo <= agg <= hi, f"{zone} FG% {agg:.3f} outside published band [{lo}, {hi}]"


# ---------------------------------------------------------------------------
# Phase 2 — defender / shot-clock context
# ---------------------------------------------------------------------------


def test_defender_context_plausible_and_joint_ordered(shot_value_corpus):
    """The defender/shot-clock marginals are plausible FG% and the
    independence-combined joint is bounded + ordered by its marginals.

    NOTE (documented, NOT a loosened gate): unconditional FG%-by-defender-
    distance is NOT monotone in openness on this corpus -- it is location-
    confounded. On the elite/big-heavy fixture the tightest bucket (0.537) BEATS
    wide-open (0.464) because tight coverage skews to rim attacks (layups/dunks
    ~65%) while "wide open" is dominated by long threes (~35%). Monotonicity
    only holds WITHIN a shot zone; the public API's aggregate bucket table
    does not zone-split, so we gate on plausibility + joint consistency, and
    record the confound rather than assert a property the data refutes."""
    from sportsdataverse.nba.nba_shot_value import make_prob_joint

    tables = make_prob_by_context(shot_value_corpus["ptshots"])
    d, c = tables["defender"], tables["shot_clock"]
    for tbl in (d, c):
        if tbl.height:
            assert tbl.filter((pl.col("fg_pct") < 0.25) | (pl.col("fg_pct") > 0.75)).height == 0, (
                "a context bucket FG% is outside the plausible [0.25, 0.75] band"
            )
    if d.height and c.height:
        joint = make_prob_joint(d, c, overall_fg_pct=0.47)
        vals = joint["joint_fg_pct"].to_numpy()
        assert ((vals > 0.0) & (vals < 1.0)).all(), "joint make-prob out of (0,1)"
        # the joint's lowest cell pairs the lowest-FG% defender bucket with the
        # lowest-FG% clock bucket (odds math is monotone in its marginals)
        worst = joint.sort("joint_fg_pct").row(0, named=True)
        assert worst["close_def_dist_range"] == d.sort("fg_pct").row(0, named=True)["bucket"]
        assert worst["shot_clock_range"] == c.sort("fg_pct").row(0, named=True)["bucket"]


# ---------------------------------------------------------------------------
# Phase 3 — shooter-talent split-half reliability (with the fitted k)
# ---------------------------------------------------------------------------


def test_shrunk_reliability_beats_raw(shot_value_corpus):
    """Shrinking with the fitted k does not reduce cross-half reliability
    (observed 0.699 raw → 0.707 shrunk). If it regresses, refit k -- do not
    loosen. As-of note: talent is regression-to-mean, no leakage here (both
    halves are the same season, split by parity for the reliability check)."""
    scored = score_shot_xpoints(shot_value_corpus["shots"], shot_value_corpus["league_avgs"]).with_columns(
        (pl.int_range(pl.len()).over("player_id") % 2).alias("half")
    )

    def half(h):
        return (
            scored.filter(pl.col("half") == h)
            .group_by("player_id")
            .agg(
                pl.len().alias("n"),
                ((pl.col("shot_made_flag").sum() - pl.col("base_fg_pct").sum()) / pl.len()).alias("raw"),
            )
        )

    a = half(0).join(half(1), on="player_id", suffix="_b").filter((pl.col("n") >= 25) & (pl.col("n_b") >= 25))
    n1, r1, r2 = a["n"].to_numpy(), a["raw"].to_numpy(), a["raw_b"].to_numpy()
    k = get_shrinkage_k("00")
    shrunk = r1 * n1 / (n1 + k)
    r_raw = split_half_reliability(r1, r2)
    r_shrunk = split_half_reliability(shrunk, r2)
    assert r_shrunk >= r_raw, "shrinkage reduced cross-half reliability — refit k, do not loosen"
    assert r_shrunk >= 0.40, "reliability floor (observed 0.707 at fit time; refit if it regresses)"


# ---------------------------------------------------------------------------
# Phase 4 — selection rank sanity + zone bands
# ---------------------------------------------------------------------------


def test_selection_rank_sanity(shot_value_corpus):
    """A rim-heavy big (Jokic 203999) selects higher-value shots than a
    mid-range guard (DeRozan 201942) -- both documented in the README."""
    scored = score_shot_xpoints(shot_value_corpus["shots"], shot_value_corpus["league_avgs"])
    sel = shot_selection_quality(scored, min_attempts=50)
    jokic = sel.filter(pl.col("player_id") == 203999)
    derozan = sel.filter(pl.col("player_id") == 201942)
    if jokic.height and derozan.height:
        assert jokic.row(0, named=True)["selection_quality"] > derozan.row(0, named=True)["selection_quality"], (
            "rim-heavy big did not out-select the mid-range guard"
        )
    # every selection value is finite
    assert np.isfinite(sel["selection_quality"].to_numpy()).all()
