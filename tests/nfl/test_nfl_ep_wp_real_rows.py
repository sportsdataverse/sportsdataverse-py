"""Real-row regression pins for the NFL EP / WP / CP feature conventions.

Fixtures (``tests/fixtures/nfl_ep_wp/``, provenance in its README) are real
nflverse rows carrying nflverse's own model outputs (``nflverse_ep`` /
``nflverse_wp`` / ``nflverse_vegas_wp`` / ``nflverse_cp``); the tests re-score
them with the bundled boosters and compare.

What is pinned, and why
-----------------------
* **Kickoff touchback substitution stays nflfastR-parity (80 / 75).**  The 2024
  dynamic kickoff moved the touchback to the 30 (yards-to-endzone 70) and 2025 to
  the 35 (65).  Measured 2026-09-01 on the FULL 2023–2025 seasons
  (``dev/kickoff_audit_extra.py``): nflverse's kickoff ``ep`` matches sdv-py
  scored at 75 with mean|d| 0.103 / 0.103 / 0.105 (bias +0.05), but at the rule
  spot the gap is 0.295 (2024 @ 70, bias +0.29) and 0.552 (2025 @ 65, bias
  +0.55) — the oracle itself still substitutes 75 (``helper_add_ep_wp.R`` L355 /
  L1047).  So the constant is a *parity convention*, and this file pins it AND
  pins the data fact that the rule moved (modal next-play yardline after a
  touchback: 80 → 75 → 70 → 65), so the day nflfastR moves, the convention pin
  fails loudly instead of parity drifting quietly.
* **Per-season agreement floors** derived from the values observed on this
  fixture at gate time (40 kickoff rows / season, 2014–2025): kickoff-row ``ep``
  r 0.906–0.990 and mean|d| 0.040–0.130 (2025 is the widest at 0.1296 — the
  same model-vintage gap as 2018–2024, not a kickoff-rule gap: on the full
  seasons 2024/2025 mean|d| 0.1027/0.1055 sit inside the 2018–2023 band
  0.1028–0.1057); ``wp`` r ≥ 0.99701, ``vegas_wp`` r ≥ 0.99763.  Floors sit
  strictly below the observed values (never-lower rule: a floor is lowered
  only with a documented re-derivation, never to make a run pass).
* **``distance_to_sticks = air_yards - ydstogo`` (nflfastR sign).**  The sign
  bit once (xYAC port).  On 300 real 2023 pass rows the bundled CP model agrees
  with nflverse ``cp`` at r 0.99618 with the nflfastR sign and only r 0.89913
  with the sign flipped — so a flipped sign fails the correct-sign floor AND the
  flipped-sign ceiling.
* **CPOE is percentage points**: ``cpoe == 100 * (complete_pass - cp)`` exactly,
  so a completed pass at cp 0.4 reads +60, not +0.6.
"""

from __future__ import annotations

from pathlib import Path

import numpy as np
import polars as pl
import pytest

from sportsdataverse.nfl.ep_wp import (
    CP_FEATURES,
    _add_wp_aux,
    _apply_feature_substitution,
    _load_model,
    _make_cp_mutations,
    calculate_completion_probability,
    calculate_expected_points,
    calculate_win_probability,
)
from sportsdataverse.nfl.model_vars import TOUCHBACK_YARDLINE_POST_2016, TOUCHBACK_YARDLINE_PRE_2016

FIXTURES = Path(__file__).resolve().parents[1] / "fixtures" / "nfl_ep_wp"

# Floors — measured 2026-09-01 on these fixtures (see module docstring). Never lower.
_KICKOFF_EP_R_FLOOR = 0.88  # observed min 0.90599 (2025)
_KICKOFF_EP_MAD_CEILING = 0.16  # observed max 0.12956 (2025)
_KICKOFF_WP_R_FLOOR = 0.99  # observed min 0.99701 (2016)
_KICKOFF_VEGAS_WP_R_FLOOR = 0.99  # observed min 0.99763 (2017)
_CP_R_FLOOR = 0.99  # observed 0.99618 with the nflfastR sign
_CP_FLIPPED_R_CEILING = 0.95  # observed 0.89913 with the sign flipped
_MIN_ROWS_PER_SEASON = 30  # 40 captured per season; guards a truncated fixture

#: The spot the RULE put the ball on after a kickoff touchback, by season — read
#: off the fixture (modal next-play yardline_100). The CONVENTION (what both
#: nflfastR and sdv-py substitute) stays 75 from 2016 on.
_RULE_TOUCHBACK_SPOT = {2014: 80, 2015: 80, **{s: 75 for s in range(2016, 2024)}, 2024: 70, 2025: 65}


def _r(a: pl.Series, b: pl.Series) -> float:
    return float(np.corrcoef(a.cast(pl.Float64).to_numpy(), b.cast(pl.Float64).to_numpy())[0, 1])


def _mad(a: pl.Series, b: pl.Series) -> float:
    return float(np.abs(a.cast(pl.Float64).to_numpy() - b.cast(pl.Float64).to_numpy()).mean())


@pytest.fixture(scope="module")
def kickoff_rows() -> pl.DataFrame:
    df = pl.read_parquet(FIXTURES / "kickoff_rows_2014_2025.parquet")
    assert df.height >= 12 * _MIN_ROWS_PER_SEASON, "kickoff fixture looks truncated"
    return df


@pytest.fixture(scope="module")
def kickoff_scored(kickoff_rows: pl.DataFrame) -> pl.DataFrame:
    """Re-score the fixture exactly as ``enrich_nfl_pbp`` does for kickoff rows."""
    substituted = _apply_feature_substitution(kickoff_rows)
    ep = calculate_expected_points(substituted)
    wp = calculate_win_probability(_add_wp_aux(substituted))
    return kickoff_rows.with_columns(
        substituted["yardline_100"].alias("substituted_yardline_100"),
        ep["ep"].alias("sdv_ep"),
        wp["wp"].alias("sdv_wp"),
        wp["vegas_wp"].alias("sdv_vegas_wp"),
    )


@pytest.fixture(scope="module")
def pass_rows() -> pl.DataFrame:
    df = pl.read_parquet(FIXTURES / "pass_rows_2023.parquet")
    assert df.height >= 250, "pass fixture looks truncated"
    return df


# ---------------------------------------------------------------------------
# Kickoff touchback substitution — convention vs rule
# ---------------------------------------------------------------------------


def test_substitution_convention_is_80_pre_2016_then_75(kickoff_scored: pl.DataFrame) -> None:
    """Every kickoff row is scored from 80 (< 2016) or 75 (2016+) — including 2024 and 2025.

    The rule spot moved to 70 (2024) and 65 (2025) but the oracle did not; see
    ``test_rule_spot_moved_but_convention_did_not`` for the data side of that.
    """
    by_season = (
        kickoff_scored.group_by("season").agg(pl.col("substituted_yardline_100").unique().alias("spots")).sort("season")
    )
    for season, spots in by_season.iter_rows():
        expected = TOUCHBACK_YARDLINE_PRE_2016 if season < 2016 else TOUCHBACK_YARDLINE_POST_2016
        assert spots == [expected], f"{season}: substituted yardline {spots}, expected [{expected}]"


def test_rule_spot_moved_but_convention_did_not(kickoff_rows: pl.DataFrame) -> None:
    """The data shows the touchback landing at 80 → 75 → 70 → 65; the substitution stays 75.

    This is the pin that turns the dynamic-kickoff finding into a test: if the
    fixture is ever re-captured against a nflverse release where nflfastR moved
    its substitution, the parity floors below fail and THIS test says why.
    """
    tb = (
        kickoff_rows.filter(pl.col("touchback") == 1)
        .group_by("season")
        .agg(pl.col("next_play_yardline_100").drop_nulls().mode().first().alias("rule_spot"), pl.len().alias("n"))
        .sort("season")
    )
    observed = dict(zip(tb["season"].to_list(), tb["rule_spot"].to_list()))
    assert observed == _RULE_TOUCHBACK_SPOT, observed
    assert (tb["n"] >= 10).all(), tb.to_dicts()
    # The convention is NOT the rule spot from 2024 on — deliberately (parity with nflverse).
    assert _RULE_TOUCHBACK_SPOT[2024] != TOUCHBACK_YARDLINE_POST_2016
    assert _RULE_TOUCHBACK_SPOT[2025] != TOUCHBACK_YARDLINE_POST_2016


@pytest.mark.parametrize("season", sorted(_RULE_TOUCHBACK_SPOT))
def test_kickoff_row_parity_with_nflverse_per_season(kickoff_scored: pl.DataFrame, season: int) -> None:
    """sdv-py's kickoff-row ep / wp / vegas_wp agree with nflverse's within the measured floors.

    Kickoff-row ``ep`` is a low-variance comparison (every row is 1st-and-10
    from the same spot; only clock and score vary), so its r is ~0.9–0.99 while
    non-kickoff rows sit at ~0.995 — the residual is the era-aware retrain vs
    nflfastR's 2020 model, identical in size for 2024/2025 and 2018–2023.
    """
    s = kickoff_scored.filter(pl.col("season") == season)
    assert s.height >= _MIN_ROWS_PER_SEASON, f"{season}: only {s.height} rows"
    r_ep, mad_ep = _r(s["sdv_ep"], s["nflverse_ep"]), _mad(s["sdv_ep"], s["nflverse_ep"])
    assert r_ep >= _KICKOFF_EP_R_FLOOR, f"{season}: kickoff ep r {r_ep:.4f} < {_KICKOFF_EP_R_FLOOR}"
    assert mad_ep <= _KICKOFF_EP_MAD_CEILING, f"{season}: kickoff ep mean|d| {mad_ep:.4f} > {_KICKOFF_EP_MAD_CEILING}"
    r_wp = _r(s["sdv_wp"], s["nflverse_wp"])
    assert r_wp >= _KICKOFF_WP_R_FLOOR, f"{season}: kickoff wp r {r_wp:.4f}"
    r_vwp = _r(s["sdv_vegas_wp"], s["nflverse_vegas_wp"])
    assert r_vwp >= _KICKOFF_VEGAS_WP_R_FLOOR, f"{season}: kickoff vegas_wp r {r_vwp:.4f}"


def test_dynamic_kickoff_seasons_are_not_worse_than_the_prior_era(kickoff_scored: pl.DataFrame) -> None:
    """2024–2025 kickoff-row agreement sits with 2018–2023, not below it.

    Pooled (80 rows vs 240 rows) so the check is not at the mercy of one
    40-row season: observed pooled mean|d| 2024–25 ≈ 0.116 vs 2018–23 ≈ 0.098
    on this fixture (full seasons: 0.104 vs 0.104). A kickoff-rule-driven gap
    would be ~0.3 (scored at 70) or ~0.55 (scored at 65) — see module docstring.
    """
    recent = kickoff_scored.filter(pl.col("season") >= 2024)
    prior = kickoff_scored.filter(pl.col("season").is_between(2018, 2023))
    gap = _mad(recent["sdv_ep"], recent["nflverse_ep"]) - _mad(prior["sdv_ep"], prior["nflverse_ep"])
    assert gap < 0.1, f"2024-25 kickoff ep mean|d| exceeds 2018-23 by {gap:.4f} — a rule-spot gap would be >= 0.19"


# ---------------------------------------------------------------------------
# distance_to_sticks sign + CPOE scale on real pass rows
# ---------------------------------------------------------------------------


def test_distance_to_sticks_is_air_yards_minus_ydstogo(pass_rows: pl.DataFrame) -> None:
    feats = _make_cp_mutations(pass_rows)
    expected = (pass_rows["air_yards"] - pass_rows["ydstogo"]).cast(pl.Float64)
    assert (feats["distance_to_sticks"].cast(pl.Float64) == expected).all()
    # Sign sanity on a real row: a throw past the sticks is positive.
    past = pass_rows.filter(pl.col("air_yards") > pl.col("ydstogo"))
    assert past.height > 0
    assert (_make_cp_mutations(past)["distance_to_sticks"] > 0).all()


def test_cp_parity_pins_the_sticks_sign(pass_rows: pl.DataFrame) -> None:
    """The nflfastR sign reproduces nflverse cp (r ≥ 0.99); the flipped sign does not (r < 0.95)."""
    scored = calculate_completion_probability(pass_rows)
    r_ok = _r(scored["cp"], pass_rows["nflverse_cp"])
    assert r_ok >= _CP_R_FLOOR, f"cp vs nflverse r {r_ok:.5f} < {_CP_R_FLOOR}"

    from xgboost import DMatrix

    feats = _make_cp_mutations(pass_rows)
    flipped = feats.with_columns((-pl.col("distance_to_sticks")).alias("distance_to_sticks"))
    x_flipped = flipped.select(CP_FEATURES).to_numpy().astype(np.float32)
    preds = _load_model("cp_model.ubj").predict(DMatrix(x_flipped, feature_names=CP_FEATURES))
    r_flipped = float(np.corrcoef(preds, pass_rows["nflverse_cp"].to_numpy())[0, 1])
    assert r_flipped < _CP_FLIPPED_R_CEILING, f"flipped sign still agrees (r {r_flipped:.5f}) — the pin is toothless"
    assert r_ok - r_flipped > 0.05


def test_cpoe_is_percentage_points(pass_rows: pl.DataFrame) -> None:
    scored = calculate_completion_probability(pass_rows)
    expected = 100.0 * (scored["complete_pass"].cast(pl.Float64) - scored["cp"])
    assert np.allclose(scored["cpoe"].to_numpy(), expected.to_numpy())
    assert scored["cpoe"].abs().max() > 30, "cpoe should be in percentage points (tens), not a 0-1 rate"
