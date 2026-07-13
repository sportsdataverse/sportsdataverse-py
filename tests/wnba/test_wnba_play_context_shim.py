"""Offline tests for the WNBA play-context shim (``wnba_engine.wnba_play_context``).

The shim is a thin binding of the league-agnostic
:func:`sportsdataverse.nba.nba_play_context.add_play_context` core onto the
``stats.wnba.com`` fetcher — so the tests prove two things:

1. **Delegation is byte-identical.** ``wnba_play_context(gid)`` must equal
   ``add_play_context(enhanced_pbp)`` on the same fixture. If it ever diverges,
   a WNBA-specific code path has been introduced, which is exactly what the
   shim exists to prevent.

2. **The oracle gate holds on WNBA data.** The transition-frequency gate is
   re-run at the SAME band as the NBA (``[0.12, 0.21]``, see
   ``tests/nba/test_nba_play_context.py``) — the sibling-league parity rule from
   the model-spine skill: identical thresholds, sibling fixtures. CTG is
   NBA-only, so there is no published WNBA transition rate; the band is
   inherited from the NBA calibration and is a *sanity* gate on the shared
   engine, not a WNBA-specific claim.

No network: the module-level ``_fetch_*`` helpers are monkeypatched to the
committed ``tests/fixtures/wnba_engine`` payloads.
"""

from __future__ import annotations

import json
import pathlib

import pandas as pd
import polars as pl
import pytest

import sportsdataverse.wnba.wnba_engine as W
from sportsdataverse.nba.nba_enhanced_pbp import enhanced_pbp_from_payload
from sportsdataverse.nba.nba_play_context import (
    PLAY_CONTEXT_POSSESSIONS_SCHEMA,
    add_play_context,
)

FXR = pathlib.Path("tests/fixtures/wnba_engine")
GAMES = ["1022400001", "1022400003"]

#: Same band as the NBA gate (tests/nba/test_nba_play_context.py). Sibling-league
#: parity: identical thresholds on the sibling's fixtures.
TRANSITION_FREQ_GATE = (0.12, 0.21)

#: Observed on the two committed WNBA fixtures at the NBA-calibrated 6.0s default:
#: 0.167 (1022400001) and 0.160 (1022400003) -> mean 0.163. That is the SAME mean
#: the three NBA fixtures produce (0.163), which is the substantive finding here:
#: the transition knob fitted on the NBA transfers to the WNBA unchanged. The knob
#: is steep either way (WNBA scan: 4s=0.07, 6s=0.16, 8s=0.24, 10s=0.32), so this
#: agreement is not an artifact of a flat region.
OBSERVED_WNBA_TRANSITION_FREQ_MEAN = 0.163


def _patch(monkeypatch, gid: str) -> None:
    fx = FXR / gid
    monkeypatch.setattr(W, "_fetch_pbp", lambda g: json.loads((fx / "playbyplayv3.json").read_text()))


def _enh(gid: str) -> pl.DataFrame:
    payload = json.loads((FXR / gid / "playbyplayv3.json").read_text())
    return enhanced_pbp_from_payload(payload, league_id="10")


@pytest.mark.parametrize("gid", GAMES)
def test_shim_emits_the_play_context_columns(monkeypatch, gid: str) -> None:
    _patch(monkeypatch, gid)
    out = W.wnba_play_context(gid)
    assert isinstance(out, pl.DataFrame)
    assert out.height > 0
    for col, dtype in PLAY_CONTEXT_POSSESSIONS_SCHEMA.items():
        assert col in out.columns, f"missing play-context column {col}"
        assert out.schema[col] == dtype, f"{col}: {out.schema[col]} != {dtype}"


@pytest.mark.parametrize("gid", GAMES)
def test_shim_delegates_to_the_nba_core(monkeypatch, gid: str) -> None:
    """The shim must add NO WNBA-specific logic — output equals the core's."""
    _patch(monkeypatch, gid)
    shim = W.wnba_play_context(gid)
    core = add_play_context(_enh(gid))
    assert shim.equals(core)


@pytest.mark.parametrize("gid", GAMES)
def test_shim_pandas_flag(monkeypatch, gid: str) -> None:
    _patch(monkeypatch, gid)
    assert isinstance(W.wnba_play_context(gid, return_as_pandas=True), pd.DataFrame)


def test_transition_frequency_gate_on_wnba_fixtures(monkeypatch) -> None:
    """ORACLE GATE (sibling-league parity, identical NBA thresholds).

    Never lower this gate to make it pass — debug the engine instead. If WNBA
    genuinely runs at a different transition rate than the NBA, that is a
    *finding* to record and calibrate (a WNBA ``transition_seconds``), not a
    reason to widen the band.
    """
    freqs = []
    for gid in GAMES:
        _patch(monkeypatch, gid)
        poss = W.wnba_play_context(gid)
        clean = poss.filter(
            (pl.col("count_as_possession") == True)  # noqa: E712
            & (pl.col("is_garbage_time") == False)  # noqa: E712
            & (pl.col("is_heave_possession") == False)  # noqa: E712
        )
        assert clean.height >= 150, f"{gid}: only {clean.height} clean possessions — fixture shrunk?"
        freqs.append(clean["is_transition"].mean())

    mean = sum(freqs) / len(freqs)
    lo, hi = TRANSITION_FREQ_GATE
    assert lo <= mean <= hi, (
        f"WNBA transition freq {mean:.3f} outside the NBA band {TRANSITION_FREQ_GATE} (per-game {freqs})"
    )
    assert mean == pytest.approx(OBSERVED_WNBA_TRANSITION_FREQ_MEAN, abs=0.02)


@pytest.mark.parametrize("gid", GAMES)
def test_start_types_are_the_full_taxonomy(monkeypatch, gid: str) -> None:
    """The zone-split taxonomy must actually resolve on WNBA shot coordinates."""
    _patch(monkeypatch, gid)
    poss = W.wnba_play_context(gid)
    detail = set(poss["possession_start_type_detail"].drop_nulls().to_list())
    # at least one zone-qualified start type resolved (not everything collapsing
    # to OffDeadball — which is what a coordinate/zone failure would look like)
    zoned = {s for s in detail if s.endswith(("Make", "Miss", "Block")) and s not in ("OffFTMake", "OffFTMiss")}
    assert zoned, f"{gid}: no zone-qualified start types — shot-zone classification failed on WNBA coords"
    buckets = set(poss["possession_start_type_ctg"].drop_nulls().to_list())
    assert {"off_made", "off_live_rebound"} <= buckets, f"{gid}: CTG buckets missing: {buckets}"
