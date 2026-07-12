"""Oracle / calibration gates for the NHL player-impact spine.

Every gate here is derived from an *observed* value on the committed fixture
(``tests/fixtures/nhl_player_impact/``) and documented in the assertion's neighboring
comment -- never lowered to make a failure pass (see the Global Constraints in the
implementation plan: "never lower the gate to pass -- debug the model").

The EvolvingHockey (skater RAPM / WAR, ``eh_skaters.parquet``) and MoneyPuck (goalie
GSAx, ``mp_gsax.parquet``) concurrent-validity fixtures were originally shipped as
documented zero-row stubs (both sources were believed scrape-blocked/paywalled) but are
now real captures -- MoneyPuck's season-summary CSVs are a public, license-free-with-
credit download (``dev/nhl_player_impact/capture_moneypuck.py``), and EvolvingHockey's
skater RAPM/GAR exports were captured against the project's own Pro Subscriber login
(``dev/nhl_player_impact/eh_capture.py`` + ``build_eh_fixture.py``). See
``tests/fixtures/nhl_player_impact/README.md`` for the full capture provenance and
licensing credit. The external-oracle assertions below still ``pytest.skip`` (not fake
a pass) if a fixture ever reverts to zero rows; the internal construction-invariant
gates always run regardless.
"""

from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest

from sportsdataverse.nhl.nhl_gsax import nhl_goalie_gsax
from sportsdataverse.nhl.nhl_player_impact_constants import calibration_table, spearman_corr
from sportsdataverse.nhl.nhl_rapm import nhl_skater_rapm
from sportsdataverse.nhl.nhl_xg import nhl_xg

FIX = Path(__file__).parent.parent / "fixtures" / "nhl_player_impact"
MODELS = FIX / "xg_models"


def _pbp() -> pl.DataFrame:
    return pl.read_parquet(FIX / "pbp_sample.parquet")


def _scored() -> pl.DataFrame:
    return nhl_xg(_pbp(), model_dir=MODELS)


# Observed on the 3-game fixture (228 5v5 shots, 11 goals): |sum(xg) - goals| / goals ==
# 0.178. TOL is set a bit above that observed ratio to allow small re-scoring jitter
# (e.g. a booster/xgboost version bump) without masking a real feature-mapping bug --
# NOT widened to paper over a mismatch. If this fails, check the feature-prep column
# mapping (era one-hots, x_fixed sign, strength-state routing) against fastRhockey
# before touching TOL.
XG_5V5_TOL = 0.30


def test_xg_calibration_5v5_sum_matches_goals_within_tol():
    scored = _scored().filter(pl.col("xg").is_not_null())
    s = scored.filter(pl.col("strength_state") == "5v5")
    tot_xg = s["xg"].sum()
    tot_goals = s.filter(pl.col("event_type") == "GOAL").height
    ratio = abs(tot_xg - tot_goals) / max(tot_goals, 1)
    assert ratio <= XG_5V5_TOL, f"5v5 xG calibration off: sum_xg={tot_xg:.2f} goals={tot_goals} ratio={ratio:.3f}"


def test_xg_calibration_reliability_is_monotone():
    scored = _scored().filter(pl.col("xg").is_not_null())
    goal_flag = (scored["event_type"] == "GOAL").cast(pl.Int64).to_numpy()
    tbl = calibration_table(goal_flag, scored["xg"].to_numpy(), n_bins=5)
    actual = tbl["mean_actual"].to_numpy()
    assert (actual == sorted(actual)).all(), f"calibration table not monotone: {tbl}"


# Observed on the 3-game fixture: league-wide sum(gsax) == 3.98 (a handful of goals --
# a 3-game sample is far too small for Sigma(xg) to converge to Sigma(goals); the
# league-wide-approx-0 property is a large-sample calibration claim, not an exact
# per-sample identity). TOL is set from that observed magnitude, generous enough for a
# tiny fixture without being vacuous (it would still catch a gross attribution bug --
# e.g. double-counting every shot, which would roughly double this value).
GSAX_SUM_TOL = 6.0


def test_gsax_league_sum_near_zero_within_small_sample_tolerance():
    gsax = nhl_goalie_gsax(_pbp(), pl.DataFrame(), model_dir=MODELS)
    total = gsax["gsax"].sum()
    assert abs(total) <= GSAX_SUM_TOL, f"league sum(gsax) off: {total:.2f} (tol={GSAX_SUM_TOL})"


def test_gsax_moneypuck_concurrent_validity():
    mp = pl.read_parquet(FIX / "mp_gsax.parquet")
    if mp.height == 0:
        pytest.skip(
            "MoneyPuck per-goalie GSAx sample is data-blocked -- "
            "see tests/fixtures/nhl_player_impact/README.md capture contract."
        )
    mine = nhl_goalie_gsax(_pbp(), pl.DataFrame(), model_dir=MODELS)
    assert mine.schema["player_id"] == mp.schema["player_id"]
    joined = mine.join(mp, on="player_id", how="inner")
    # This is a small-sample SANITY check, not a statistically-powered validity gate:
    # only 6 goalies appear in the 3-game internal fixture, so it can catch a gross
    # attribution/sign regression but not certify concurrent validity (a powered goalie
    # GSAx gate needs a full-season sdv-py build -- deferred, same as the WAR gate
    # below). >=6, not >0: matches the cited n=6 provenance and a 1-player overlap makes
    # spearman_corr(n=1) nan -> a vacuous pass.
    assert joined.height >= 6, "too few overlapping goalies for a meaningful correlation"
    # Observed (deterministic -- build_stints tiebreak fix in nhl_rapm.py; GSAx doesn't
    # use stints but the value is stable regardless) on the committed 2024-25 MoneyPuck
    # season goalie table (captured via dev/nhl_player_impact/capture_moneypuck.py, a
    # public/free-with-credit download -- see the fixture README): n=6 overlapping
    # goalies, Spearman(3-game gsax, season gsax) == 0.771. FLOOR is set a bit below
    # that observed value (not invented) -- a 6-goalie sample is sensitive to any single
    # data point, so a modest margin absorbs a booster/rescoring jitter without masking
    # a real attribution regression.
    FLOOR = 0.65
    corr = spearman_corr(joined["gsax"].to_numpy(), joined["gsax_right"].to_numpy())
    assert corr >= FLOOR, f"GSAx vs MoneyPuck concurrent validity below floor: {corr:.3f} < {FLOOR}"


def _shifts() -> pl.DataFrame:
    return pl.read_parquet(FIX / "shifts_sample.parquet")


def test_rapm_off_coefficients_are_ridge_centered():
    # Observed on the 3-game fixture: mean(off_coef) == 0.080 (a ridge regularizes
    # toward, but does not force to exactly, zero absent a shared reference level).
    # A gross sign-flip or scaling bug (e.g. per-game instead of per-60) would blow this
    # far past a small band around the observed value.
    rapm = nhl_skater_rapm(_pbp(), _shifts(), model_dir=MODELS)
    assert rapm.height > 0
    mean_off = rapm["xg_rapm_off"].mean()
    assert abs(mean_off) < 1.0, f"off coefficients not ridge-centered: mean={mean_off:.3f}"


def test_rapm_evolvinghockey_concurrent_validity():
    eh = pl.read_parquet(FIX / "eh_skaters.parquet")
    if eh.height == 0:
        pytest.skip(
            "EvolvingHockey per-skater RAPM/WAR sample is data-blocked (subscription-gated) -- "
            "see tests/fixtures/nhl_player_impact/README.md capture contract."
        )
    # strength_states=["5v5"] -- NOT the default all-situations call: EvolvingHockey's
    # skater RAPM tool has no "All situations combined" table (RAPM is inherently
    # strength-segmented into EV/PP/SH), so eh_skaters.parquet's xg_rapm column is
    # EH's EV table. "5v5" is this codebase's own even-strength proxy (see
    # nhl_skater_war's ev_off/ev_def components), matching that definition instead of
    # comparing mismatched all-situations-vs-EV-only numbers.
    mine = nhl_skater_rapm(_pbp(), _shifts(), model_dir=MODELS, strength_states=["5v5"])
    assert mine.schema["player_id"] == eh.schema["player_id"]
    joined = mine.join(eh, on="player_id", how="inner")
    # >=50, not >5: the fixture pairs n=72 skaters, so a crosswalk collapse (e.g. a name
    # normalization change silently dropping most joins) should fail loudly rather than
    # pass on a vacuous small-n correlation.
    assert joined.height >= 50, "too few overlapping skaters for a meaningful correlation"
    # Observed (reproducible -- the build_stints .mode() tiebreak fix in nhl_rapm.py
    # pins the CV lambda selection, so this no longer bounces run-to-run) on the
    # committed 2024-25 EvolvingHockey EV skater-RAPM export (captured via
    # dev/nhl_player_impact/eh_capture.py against the account's own Pro Subscriber login
    # -- see the fixture README): n=72 overlapping skaters (name-crosswalked, not
    # id-crosswalked -- EH ships no NHL playerId), Spearman(3-game 5v5 xg_rapm, season
    # EV xG±/60) == 0.406. FLOOR is set well below that observed value (not invented):
    # a 3-game sample gives RAPM's ridge very little data to separate individual
    # skaters' effects from their frequent linemates, so this is real but modest signal
    # -- debug (not widen) if a future recapture regresses well below this. n=72 also
    # clears the ~0.23 two-sided Spearman significance threshold, so 0.406 is a powered
    # (not noise-band) magnitude gate, unlike the WAR gate below.
    FLOOR = 0.30
    corr = spearman_corr(joined["xg_rapm"].to_numpy(), joined["xg_rapm_right"].to_numpy())
    assert corr >= FLOOR, f"skater RAPM vs EvolvingHockey concurrent validity below floor: {corr:.3f} < {FLOOR}"


# Observed on the 3-game fixture (3087 on-ice combinations, min_toi=0): Spearman(summed
# member RAPM, observed on-ice xGF-xGA) == 0.235. This is real signal (positive,
# non-trivial) but modest -- the combinatorial "every co-occurring size-3/2 subset counts
# as its own unit" construction (see nhl_unit_ratings' data-availability caveat) smooths
# a lot of variance across heavily overlapping units on a 3-game sample. FLOOR is set a
# bit below the observed value, not invented -- debug (not widen) if this regresses.
UNIT_RATINGS_FLOOR = 0.15


def test_unit_ratings_internal_gate_summed_rapm_tracks_on_ice_xg_diff():
    from sportsdataverse.nhl.nhl_unit_ratings import nhl_unit_ratings

    units = nhl_unit_ratings(_pbp(), _shifts(), model_dir=MODELS, min_toi=0.0)
    assert units.height > 0
    corr = spearman_corr(units["summed_rapm"].to_numpy(), (units["on_ice_xgf"] - units["on_ice_xga"]).to_numpy())
    assert corr >= UNIT_RATINGS_FLOOR, f"unit-ratings internal gate below floor: {corr:.3f} < {UNIT_RATINGS_FLOOR}"


# Observed on the 3-game fixture: reconstructed league PP xGF rate == 10.18 per 60
# (ratio 1.64x the seeded LEAGUE_CONSTANTS["nhl"].league_xg_rate_pp=6.2). A 3-game
# sample's PP rate legitimately runs hot/cold vs. a full-season-fit constant -- this is
# a soft sanity check that the strength-state filter + rate math are right (order of
# magnitude), not a tight fit to this specific tiny sample. BAND is a x3 multiplicative
# window around the constant, generous enough for small-sample noise while still
# catching a real unit/sign bug (e.g. a per-game instead of per-60 scaling error, which
# would be off by ~16-27x for a 3-game, ~90-shift sample).
PP_RATE_RATIO_BAND = (1.0 / 3.0, 3.0)


def test_special_teams_pp_rate_reconciles_with_league_constant_order_of_magnitude():
    from sportsdataverse.nhl.nhl_player_impact_constants import get_constants
    from sportsdataverse.nhl.nhl_rapm import build_stints

    scored = _scored()
    stints = build_stints(_shifts(), scored)

    def _counts(s: str | None) -> tuple[int, int] | None:
        if not s or "v" not in s:
            return None
        a, b = s.split("v", 1)
        try:
            return int(a), int(b)
        except ValueError:
            return None

    pp_xgf, pp_duration = 0.0, 0.0
    for rec in stints.to_dicts():
        c = _counts(rec.get("strength_state"))
        if c is None or c[0] == c[1]:
            continue
        pp_xgf += float((rec["xgf_home"] if c[0] > c[1] else rec["xgf_away"]) or 0.0)
        pp_duration += rec["duration"]

    assert pp_duration > 0, "no PP stints found on the fixture"
    observed_rate = pp_xgf * 3600.0 / pp_duration
    ratio = observed_rate / get_constants("nhl").league_xg_rate_pp
    assert PP_RATE_RATIO_BAND[0] <= ratio <= PP_RATE_RATIO_BAND[1], (
        f"PP rate reconciliation off: observed={observed_rate:.2f}/60 ratio={ratio:.2f} outside {PP_RATE_RATIO_BAND}"
    )


def test_war_runs_on_real_fixture_and_is_bounded():
    from sportsdataverse.nhl.nhl_war import nhl_skater_war

    war = nhl_skater_war(_pbp(), _shifts(), model_dir=MODELS)
    assert war.height > 0
    # A real construction-invariant sanity bound (not a calibration claim): no single
    # skater's WAR over a 3-game sample should be wildly outsized (catches, e.g., the
    # unsigned-underflow class of bug fixed in nhl_war.py -- a wrap-around would blow
    # this by many orders of magnitude).
    assert war["war"].abs().max() < 50.0

    # NOTE: the plan's "team Sigma(war) approx team wins-above-replacement" gate is a
    # season-scale check (a 3-game sample has no meaningful win total to compare
    # against) -- tracked as a follow-up once a full-season fixture/build is available,
    # not faked here.


def test_war_evolvinghockey_concurrent_validity():
    from sportsdataverse.nhl.nhl_war import nhl_skater_war

    eh = pl.read_parquet(FIX / "eh_skaters.parquet")
    if eh.height == 0:
        pytest.skip(
            "EvolvingHockey per-skater WAR sample is data-blocked (subscription-gated) -- "
            "see tests/fixtures/nhl_player_impact/README.md capture contract."
        )
    mine = nhl_skater_war(_pbp(), _shifts(), model_dir=MODELS)
    assert mine.schema["player_id"] == eh.schema["player_id"]
    joined = mine.join(eh, on="player_id", how="inner")
    # >=50, not >5: the fixture pairs n=72 skaters, so a crosswalk collapse should fail
    # loudly rather than pass on a vacuous small-n correlation.
    assert joined.height >= 50, "too few overlapping skaters for a meaningful correlation"
    # This is a DIRECTIONAL (sign) gate, NOT a magnitude concurrent-validity gate. The
    # observed Spearman(3-game war, season WAR) is only ~0.131 (reproducible after the
    # nhl_rapm.py tiebreak fix) on n=72 -- below the ~0.23 two-sided Spearman
    # significance threshold, i.e. inside the noise band, so ANY magnitude floor there
    # (0.10, 0.13, ...) would be cleared by a nontrivial fraction of pure-noise draws
    # and wouldn't actually certify anything. WAR sums several individually-noisy
    # components (EV off/def, PP, PK, faceoffs, penalties) over just 3 games, so the
    # composite is underpowered at this sample size. We assert only that the association
    # is positive (the model isn't anti-correlated with the established public metric);
    # a powered magnitude concurrent-validity gate needs a full-season sdv-py WAR build
    # -- deferred, mirroring the season-scale team-sum(war) gate deferred in
    # test_war_runs_on_real_fixture_and_is_bounded above.
    corr = spearman_corr(joined["war"].to_numpy(), joined["war_right"].to_numpy())
    assert corr > 0, f"WAR vs EvolvingHockey is not even directionally positive: {corr:.3f}"
