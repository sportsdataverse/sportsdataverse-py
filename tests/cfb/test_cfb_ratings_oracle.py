"""Oracle gate for the CFB ratings engine (T2.1 Task 1.5).

Builds full-season 2023 ratings from the committed ``pbp_2023_sample.parquet``
(as-of-date ``None``, matching the published end-of-season oracles) and asserts
they track ESPN FPI, SP+, and Fremeau FEI. The efficiency ratings are the
``cfb_adjusted_epa`` fit; FEI uses the default :class:`RatingsConfig`.
Observed values were re-measured 2026-11-25 on the recaptured fixture with
that engine and its score-margin garbage-time rule (the naive-WP-band engine's
value on the same fixture in brackets).

Floors are set from the value observed at gate time and documented here, per the
binding "never lower a gate to make it pass -- debug the model" rule: the fixture
+ engine are proven correct, so a floor below the observed value guards against
regression without inviting a silently-degraded model.
"""

from __future__ import annotations

from pathlib import Path

import polars as pl

from sportsdataverse.cfb.cfb_prediction_constants import spearman_corr
from sportsdataverse.cfb.cfb_ratings import efficiency_ratings, fei_ratings

_FIX = Path(__file__).resolve().parents[1] / "fixtures" / "cfb_prediction"
_PBP = pl.read_parquet(_FIX / "pbp_2023_sample.parquet")
_FPI = pl.read_parquet(_FIX / "fpi_2023.parquet")
_SP = pl.read_parquet(_FIX / "sp_plus_2023.parquet")
_FEI = pl.read_parquet(_FIX / "fei_2023.parquet")


def test_adj_net_tracks_fpi() -> None:
    """Overall opponent-adjusted net EPA vs ESPN FPI (observed 0.950 [0.949])."""
    e = efficiency_ratings(_PBP).join(_FPI, on="team_id", how="inner")
    assert e.schema["team_id"] == _FPI.schema["team_id"] == pl.Utf8
    r = spearman_corr(e["adj_net"].to_numpy(), e["fpi"].to_numpy())
    assert r >= 0.90, r


def test_adj_net_tracks_sp_plus_overall() -> None:
    """Overall net EPA vs SP+ overall (observed 0.945 [0.920]) -- the net rating's second
    published peer. For context, FPI and SP+ overall themselves only agree at
    0.963, so adj_net sits near that oracle-vs-oracle ceiling with both.
    """
    e = efficiency_ratings(_PBP).join(_SP, on="team_id", how="inner")
    r = spearman_corr(e["adj_net"].to_numpy(), e["sp_overall"].to_numpy())
    assert r >= 0.90, r


def test_adj_off_tracks_sp_plus_off() -> None:
    """Offensive EPA vs SP+ offense (observed 0.869 [0.854]; 0.836 / 0.849 earlier).

    2026-07-28 semantic change (owner-directed): adj_off_epa switched from the
    competitive-only ridge coefficient to the R adjust_epa netted average, which
    includes garbage-time plays by construction (gameonpaper parity). Observed r
    moved 0.849 -> 0.836; the floor is re-derived just below the NEW statistic's
    observed value, not lowered to pass the old one.

    SP+ offense is a differently-constructed, noisier metric than play-level EPA
    and the ESPN ``team_id`` <-> SP+ join drops a few teams, so this caps around
    0.849 across every ``ridge_lambda`` swept -- the floor is set just below that
    observed ceiling, not at the plan's original 0.85 estimate.
    """
    e = efficiency_ratings(_PBP).join(_SP, on="team_id", how="inner")
    r = spearman_corr(e["adj_off_epa"].to_numpy(), e["sp_off"].to_numpy())
    assert r >= 0.82, r


def test_adj_def_tracks_sp_plus_def() -> None:
    """Defensive EPA-allowed vs SP+ defense (observed 0.809 [0.783]).

    Both are "lower is better", so the correlation is positive without a
    sign-flip. Floor from observation; defense is noisier than the net rating.
    """
    e = efficiency_ratings(_PBP).join(_SP, on="team_id", how="inner")
    r = spearman_corr(e["adj_def_epa"].to_numpy(), e["sp_def"].to_numpy())
    assert r >= 0.72, r


def test_fei_net_tracks_fei() -> None:
    """Opponent-adjusted drive efficiency vs Fremeau FEI (observed 0.967)."""
    f = fei_ratings(_PBP).join(_FEI, on="team_id", how="inner")
    r = spearman_corr(f["fei_net"].to_numpy(), f["fei"].to_numpy())
    assert r >= 0.90, r


#: Spread of ``adj_net`` on this fixture, measured 2026-11-25: 0.187 (margin
#: rule), 0.154 (naive-WP band), 0.176 (pre-2026-11-25 engine). The band sits
#: below the lowest and ~1.4x above the highest, so a 1.8x scale error (0.34)
#: falls outside it.
_ADJ_NET_SD_BAND = (0.12, 0.26)
#: Max ``adj_net`` band. Measured 0.542 on this fixture (0.424 under the
#: naive-WP band, 0.434 under the pre-2026-11-25 engine). Leaving garbage time
#: out of a team's own EPA widens the top: final 2014-2025 snapshots top out at
#: 0.43-0.71 per season (0.33-0.49 under the WP band), so the upper bound is
#: re-derived from that full-season ceiling (it was 0.55, 0.008 above this
#: fixture). A 1.8x scale error puts the max near 0.98.
_ADJ_NET_MAX_BAND = (0.15, 0.75)


def test_adj_net_is_on_the_netted_scale() -> None:
    """Scale gate -- the one failure mode Spearman gates cannot see.

    ``adj_net`` is the R ``adjust_epa`` netted statistic. The pre-2026-07-28
    coefficient+intercept scale ran ~1.8x hotter on full-season data and
    shipped because every gate here was rank-based. The max alone no longer
    separates the two (correct full-season maxima now reach 0.71), so the
    spread is gated too, and the gate is checked against a 1.8x rescale.
    """
    e = efficiency_ratings(_PBP)
    top, sd = float(e["adj_net"].max()), float(e["adj_net"].std())
    assert _ADJ_NET_MAX_BAND[0] <= top <= _ADJ_NET_MAX_BAND[1], f"max adj_net {top:.3f}"
    assert _ADJ_NET_SD_BAND[0] <= sd <= _ADJ_NET_SD_BAND[1], f"adj_net sd {sd:.3f}"
    assert not _ADJ_NET_SD_BAND[0] <= 1.8 * sd <= _ADJ_NET_SD_BAND[1]  # guard the guard
