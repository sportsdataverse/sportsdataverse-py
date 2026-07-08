"""Oracle / calibration gates for the NHL player-impact spine.

Every gate here is derived from an *observed* value on the committed fixture
(``tests/fixtures/nhl_player_impact/``) and documented in the assertion's neighboring
comment -- never lowered to make a failure pass (see the Global Constraints in the
implementation plan: "never lower the gate to pass -- debug the model").

The EvolvingHockey (skater RAPM / WAR) and MoneyPuck (goalie GSAx) concurrent-validity
fixtures ship as documented zero-row stubs (both sources are scrape-blocked/paywalled --
see ``tests/fixtures/nhl_player_impact/README.md``). Those external-oracle assertions are
skipped (not faked) whenever the fixture is empty; the internal construction-invariant
gates always run.
"""

from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest

from sportsdataverse.nhl.nhl_player_impact_constants import calibration_table
from sportsdataverse.nhl.nhl_xg import nhl_xg

FIX = Path(__file__).parent.parent / "fixtures" / "nhl_player_impact"
MODELS = FIX / "xg_models"


def _scored() -> pl.DataFrame:
    pbp = pl.read_parquet(FIX / "pbp_sample.parquet")
    return nhl_xg(pbp, model_dir=MODELS)


# Observed on the 3-game fixture (281 5v5 shots, 12 goals): |sum(xg) - goals| / goals ==
# 0.294. TOL is set a bit above that observed ratio to allow small re-scoring jitter
# (e.g. a booster/xgboost version bump) without masking a real feature-mapping bug --
# NOT widened to paper over a mismatch. If this fails, check the feature-prep column
# mapping (era one-hots, x_fixed sign, strength-state routing) against fastRhockey
# before touching TOL.
XG_5V5_TOL = 0.35


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


def test_xg_moneypuck_concurrent_gate_skipped_when_oracle_blocked():
    mp = pl.read_parquet(FIX / "mp_gsax.parquet")
    if mp.height == 0:
        pytest.skip(
            "MoneyPuck per-shot/per-player xG sample is data-blocked (scrape-gated) -- "
            "see tests/fixtures/nhl_player_impact/README.md capture contract."
        )
    # Concurrent-validity path (runs once a licensed MoneyPuck export is captured).
    raise AssertionError("mp_gsax.parquet is non-empty but the concurrent xG gate is unimplemented")
