"""Real-row regression pins for the NFL EP / WP / CP feature conventions.

Fixtures (``tests/fixtures/nfl_ep_wp/``, provenance in its README) are real
nflverse rows carrying nflverse's own model outputs (``nflverse_ep`` /
``nflverse_wp`` / ``nflverse_vegas_wp`` / ``nflverse_cp``); the tests re-score
them with the bundled boosters and compare.

What is pinned, and why
-----------------------
* **Kickoff touchback substitution is the RULE spot (80 / 75 / 70 / 65).**  The
  2024 dynamic kickoff moved the touchback to the 30 (yards-to-endzone 70) and
  2025 to the 35 (65) — ``rules/nfl.yaml`` ``nfl-2024-dynamic-kickoff`` /
  ``nfl-2025-kickoff-touchback-to-35`` — and ``model_vars.touchback_yardline``
  resolves them.  The oracle does not: nflfastR still substitutes
  ``ifelse(season < 2016, 80, 75)`` (``helper_add_ep_wp.R`` L355 / L1047), so
  on 2024-25 kickoff rows sdv-py now diverges from nflverse ``ep`` BY DESIGN
  (owner-accepted 2026-10: the rule beats the oracle).  Measured on this
  fixture: 2024 mean|d| 0.3183 (bias +0.316, r 0.9532) and 2025 0.6027 (bias
  +0.602, r 0.8596) against 0.1039 / 0.1398 when re-scored at nflfastR's 75 —
  so this file pins (a) that the substitution IS the rule spot the data shows
  (modal next-play yardline after a touchback), (b) per-season agreement bands
  measured at the rule spot, and (c) that re-scoring 2024-25 at 75 brings the
  gap back inside the 2018-2023 band, i.e. the spot is the whole divergence.
  ``wp`` is barely touched (r ≥ 0.998 either way: a 5-10 yard spot shift at
  1st-and-10 moves WP far less than EP).
* **Per-season agreement floors** derived from the values observed on this
  fixture at gate time (40 kickoff rows / season, 2014–2025): kickoff-row ``ep``
  r 0.937–0.986 and mean|d| 0.038–0.106 through 2023 (the model-vintage gap),
  the 2024 / 2025 rule-spot bands above; ``wp`` r ≥ 0.99701, ``vegas_wp`` r ≥
  0.99763.  Floors sit strictly on the passing side of the observed values
  (never-lower rule: a floor moves only with a documented re-derivation, never
  to make a run pass — the 2024-25 bands are that re-derivation).
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
from sportsdataverse.nfl.model_vars import TOUCHBACK_YARDLINE_POST_2016, touchback_yardline

FIXTURES = Path(__file__).resolve().parents[1] / "fixtures" / "nfl_ep_wp"

# Floors — measured 2026-09-01 on these fixtures (see module docstring). Never lower.
_KICKOFF_EP_R_FLOOR = 0.88  # observed min 0.93741 (2015) at the rule spot
_KICKOFF_EP_MAD_CEILING = 0.16  # observed max 0.10607 (2019) at the rule spot, seasons <= 2023
# 2024-25 are scored at the rule spot (70 / 65) while nflverse scores at 75 — the measured,
# owner-accepted divergence (2026-10-10): 2024 mean|d| 0.3183 r 0.9532; 2025 mean|d| 0.6027 r 0.8596.
_KICKOFF_EP_RULE_SPOT_BANDS = {2024: (0.36, 0.93), 2025: (0.65, 0.84)}  # season: (mad ceiling, r floor)
_KICKOFF_WP_R_FLOOR = 0.99  # observed min 0.99701 (2016)
_KICKOFF_VEGAS_WP_R_FLOOR = 0.99  # observed min 0.99763 (2017)
_CP_R_FLOOR = 0.99  # observed 0.99618 with the nflfastR sign
_CP_FLIPPED_R_CEILING = 0.95  # observed 0.89913 with the sign flipped
_MIN_ROWS_PER_SEASON = 30  # 40 captured per season; guards a truncated fixture

#: The spot the RULE put the ball on after a kickoff touchback, by season — read
#: off the fixture (modal next-play yardline_100). sdv-py substitutes exactly
#: this (``touchback_yardline``); nflfastR stays at 75 from 2016 on.
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


def test_substitution_is_the_rule_spot_by_season(kickoff_scored: pl.DataFrame) -> None:
    """Every kickoff row is scored from the season's rule spot: 80 / 75 / 70 / 65.

    rules/nfl.yaml nfl-2016-kickoff-touchback-to-25, nfl-2024-dynamic-kickoff,
    nfl-2025-kickoff-touchback-to-35.  Until 2026-10 the 2024-25 rows were
    scored at 75 as a parity convention with nflfastR; see
    ``test_rule_spot_from_the_data_is_what_the_resolver_returns`` for the data side.
    """
    by_season = (
        kickoff_scored.group_by("season").agg(pl.col("substituted_yardline_100").unique().alias("spots")).sort("season")
    )
    assert by_season.height == 12
    for season, spots in by_season.iter_rows():
        expected = touchback_yardline(season)
        assert spots == [expected], f"{season}: substituted yardline {spots}, expected [{expected}]"
        assert expected == _RULE_TOUCHBACK_SPOT[season]


def test_rule_spot_from_the_data_is_what_the_resolver_returns(kickoff_rows: pl.DataFrame) -> None:
    """The data shows the touchback landing at 80 → 75 → 70 → 65, and so does ``touchback_yardline``.

    Modal next-play ``yardline_100`` after a real touchback, per season, against
    the resolver every consumer routes through.  nflfastR's 75 is NOT the rule
    spot from 2024 on — that is the divergence the per-season bands measure.
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
    assert observed == {s: touchback_yardline(s) for s in observed}
    assert touchback_yardline(2024) != TOUCHBACK_YARDLINE_POST_2016
    assert touchback_yardline(2025) != TOUCHBACK_YARDLINE_POST_2016


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
    mad_ceiling, r_floor = _KICKOFF_EP_RULE_SPOT_BANDS.get(season, (_KICKOFF_EP_MAD_CEILING, _KICKOFF_EP_R_FLOOR))
    assert r_ep >= r_floor, f"{season}: kickoff ep r {r_ep:.4f} < {r_floor}"
    assert mad_ep <= mad_ceiling, f"{season}: kickoff ep mean|d| {mad_ep:.4f} > {mad_ceiling}"
    if season in _KICKOFF_EP_RULE_SPOT_BANDS:
        # The rule-spot gap is real and in the expected direction: a shorter field at 1st-and-10
        # is worth MORE, so sdv-py (70 / 65) sits above nflverse (75) on every 2024-25 kickoff row.
        assert mad_ep > _KICKOFF_EP_MAD_CEILING, f"{season}: gap {mad_ep:.4f} vanished — did nflfastR move its spot?"
        assert bool((s["sdv_ep"] > s["nflverse_ep"]).mean() > 0.95), season
    r_wp = _r(s["sdv_wp"], s["nflverse_wp"])
    assert r_wp >= _KICKOFF_WP_R_FLOOR, f"{season}: kickoff wp r {r_wp:.4f}"
    r_vwp = _r(s["sdv_vegas_wp"], s["nflverse_vegas_wp"])
    assert r_vwp >= _KICKOFF_VEGAS_WP_R_FLOOR, f"{season}: kickoff vegas_wp r {r_vwp:.4f}"


def test_dynamic_kickoff_gap_to_nflverse_is_the_spot_and_nothing_else(kickoff_rows: pl.DataFrame) -> None:
    """Re-scoring 2024-25 kickoff rows at nflfastR's 75 puts them back inside the 2018-23 band.

    The rule-spot divergence (mean|d| 0.318 / 0.603 at 70 / 65) must collapse to
    the model-vintage gap (observed 0.1039 / 0.1398 at 75, inside the <= 0.16
    ceiling the 2018-2023 seasons hold) when the only thing changed is the
    spot.  If it does not, something other than the touchback moved.
    """
    recent = kickoff_rows.filter(pl.col("season") >= 2024)
    at_75 = _apply_feature_substitution(recent, touchback_yardline=TOUCHBACK_YARDLINE_POST_2016)
    assert at_75["yardline_100"].unique().to_list() == [TOUCHBACK_YARDLINE_POST_2016]
    ep_75 = calculate_expected_points(at_75)["ep"]
    ep_rule = calculate_expected_points(_apply_feature_substitution(recent))["ep"]
    for season in (2024, 2025):
        m = (recent["season"] == season).to_numpy()
        mad_75 = float(np.abs(ep_75.to_numpy()[m] - recent["nflverse_ep"].to_numpy()[m]).mean())
        mad_rule = float(np.abs(ep_rule.to_numpy()[m] - recent["nflverse_ep"].to_numpy()[m]).mean())
        assert mad_75 <= _KICKOFF_EP_MAD_CEILING, f"{season}: at 75 mean|d| {mad_75:.4f} — not the spot alone"
        assert mad_rule - mad_75 >= 0.19, f"{season}: rule-spot gap only {mad_rule - mad_75:.4f}"


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
