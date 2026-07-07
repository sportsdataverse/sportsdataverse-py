"""Task 6 of Phase B: the pbpstats-live possession oracle round-trip.

Like-for-like gate — sdv's :func:`~sportsdataverse.nba.nba_possessions.build_possessions`
vs pbpstats' own ``live`` provider, both fed from the SAME committed cdn
fixture (``tests/fixtures/nba_engine/{gid}/cdn_playbyplay.json`` +
``cdn_boxscore.json``). Two assertions:

1. :func:`test_possession_count_matches_oracle` — filtered possession counts
   (``count_as_possession``) on both sides, with a fully-classified per-game
   residual in :data:`EXPECTED_RESIDUALS`.
2. :func:`test_boundary_by_boundary_diff` — the per-possession END action
   numbers must agree set-for-set, except for a bounded, cited exclusion set
   (:data:`_BOUNDARY_EXCLUSIONS`) capturing pbpstats-live engine artifacts sdv
   deliberately does not reproduce.

Action numbers are directly comparable across the two engines: Phase A verified
that the cdn feed's ``actionNumber`` equals the v3 ``action_number`` 1-to-1.

Gated on ``SDV_PBPSTATS_ROOT`` pointing at a local
https://github.com/dblackrun/pbpstats checkout (pbpstats is NOT a project
dependency). Unset → every test in this file skips cleanly; no workflow sets it
(stats.nba.com's sibling hosts and the vendored checkout are opt-in). The full
residual classification lives in ``dev/phase-b-residuals.md``.
"""

from __future__ import annotations

import functools
import json
import os
import pathlib
import sys
from typing import Any, Dict, List, Set

import polars as pl
import pytest

from sportsdataverse.nba.nba_enhanced_pbp import enhanced_pbp_from_payload
from sportsdataverse.nba.nba_possessions import build_possessions

FXROOT = pathlib.Path("tests/fixtures/nba_engine")

GAMES: List[str] = ["0022100001", "0022200001", "0022300001"]

# Opt-in local pbpstats checkout (mirrors the Phase A adapter round-trip). Unset
# → skip cleanly; the tests never hit the network (committed fixtures only).
_PBPSTATS_ROOT = os.environ.get("SDV_PBPSTATS_ROOT", "")

# ---------------------------------------------------------------------------
# Classified residuals (see dev/phase-b-residuals.md for the full write-up).
# ---------------------------------------------------------------------------

#: Allowed filtered residual per game (sdv_filtered - oracle_filtered). Every
#: nonzero entry is a pbpstats-live engine artifact sdv does not reproduce; each
#: cites the classified boundaries (see ``_BOUNDARY_EXCLUSIONS`` + the residuals
#: note). Games absent here default to 0 (exact filtered agreement).
EXPECTED_RESIDUALS: Dict[str, int] = {
    # S2 and-1/timeout split: pbpstats ends a possession at the made dunk 59
    # (the intervening BKN timeout, not a foul, defeats its 1-event and-1
    # look-ahead) and makes the lone timeout 61 its own possession; both count.
    # The S1 end-of-game marker (oracle 693) is offset by sdv's p4-end
    # possession (692), so only {59, 61} move the count. => -2.
    "0022100001": -2,
    # S2 flagrant-retention split: pbpstats ends PHI's possession at the made
    # last flagrant FT 63 (no flagrant possession-retention modeling) and makes
    # the following lone BOS defensive foul 64 its own possession; both count
    # (+2). Plus S1 end-of-game marker 643 that sdv drops (+1). => -3.
    "0022200001": -3,
    # 0022300001: 0 (exact) — all S1 markers are cnt=False or offset.
}

#: Bounded, cited boundary-set exclusions per game: the per-possession END
#: action numbers that differ between the two engines, each an enumerated
#: pbpstats-live artifact. ``sdv_only`` = boundaries sdv emits that the oracle
#: does not; ``oracle_only`` = the reverse. Any mismatch OUTSIDE these sets
#: fails :func:`test_boundary_by_boundary_diff` (the exclusion is exact, not a
#: floor). Classifications: S1 = end-of-period/end-of-game marker possession;
#: S2 = and-1/flagrant possession-retention split. See dev/phase-b-residuals.md.
_BOUNDARY_EXCLUSIONS: Dict[str, Dict[str, Set[int]]] = {
    "0022100001": {
        "sdv_only": {
            692,  # S1: sdv's p4-end possession (the trailing buzzer shot);
            #      pbpstats folds the same events into its end-of-game item 693.
        },
        "oracle_only": {
            59,  # S2: made dunk that pbpstats ends a possession at (and-1 split).
            61,  # S2: lone BKN timeout pbpstats makes its own possession.
            164,  # S1: EndOfPeriod p1 marker possession.
            327,  # S1: EndOfPeriod p2 marker possession.
            693,  # S1: end-of-game marker item (offsets sdv-only 692).
        },
    },
    "0022200001": {
        "sdv_only": set(),
        "oracle_only": {
            63,  # S2: made last flagrant FT pbpstats ends PHI's possession at.
            64,  # S2: lone BOS defensive foul pbpstats makes its own possession.
            194,  # S1: EndOfPeriod p1 marker possession.
            643,  # S1: end-of-game marker item sdv drops (trailing group).
        },
    },
    "0022300001": {
        "sdv_only": {
            693,  # S1: sdv's p4-end possession; pbpstats uses end-of-game 694.
        },
        "oracle_only": {
            495,  # S1: EndOfPeriod p3 marker possession.
            694,  # S1: end-of-game marker item (offsets sdv-only 693).
        },
    },
}


# ---------------------------------------------------------------------------
# Fixtures / helpers
# ---------------------------------------------------------------------------


@functools.lru_cache(maxsize=None)
def _enh(game_id: str) -> pl.DataFrame:
    """Enhanced v3 pbp frame for one committed fixture (cached; never mutated)."""
    payload = json.loads((FXROOT / game_id / "playbyplayv3.json").read_text())
    return enhanced_pbp_from_payload(payload)


def _pbpstats_client_or_skip() -> Any:
    """Import pbpstats' ``Client`` from the opt-in checkout, or skip the test."""
    if not _PBPSTATS_ROOT:
        pytest.skip("SDV_PBPSTATS_ROOT not set; local pbpstats checkout unavailable")
    if _PBPSTATS_ROOT not in sys.path:
        sys.path.insert(0, _PBPSTATS_ROOT)
    try:
        from pbpstats.client import Client
    except ImportError as exc:
        pytest.skip(f"pbpstats not importable in this env (vendored, not a dep): {exc}")
    return Client


def _pbpstats_possessions(game_id: str, tmp_path: pathlib.Path) -> List[Any]:
    """pbpstats-live possessions for one game, fed file-mode from the cdn fixtures.

    Writes the committed cdn play-by-play + boxscore into pbpstats' ``live``
    file-mode paths (``{dir}/pbp/live_{gid}.json`` +
    ``{dir}/game_details/live_{gid}.json``) and returns
    ``Client(...).Game(gid).possessions.items``. Entirely offline. Skips (rather
    than fails) when the checkout is unavailable or a cdn fixture is missing.
    """
    Client = _pbpstats_client_or_skip()

    cdn_pbp = FXROOT / game_id / "cdn_playbyplay.json"
    cdn_box = FXROOT / game_id / "cdn_boxscore.json"
    if not cdn_pbp.exists() or not cdn_box.exists():
        pytest.skip(f"cdn fixture missing for {game_id}")

    game_dir = tmp_path / game_id
    (game_dir / "pbp").mkdir(parents=True, exist_ok=True)
    (game_dir / "game_details").mkdir(parents=True, exist_ok=True)
    (game_dir / "pbp" / f"live_{game_id}.json").write_text(cdn_pbp.read_text())
    (game_dir / "game_details" / f"live_{game_id}.json").write_text(cdn_box.read_text())

    client = Client(
        {
            "dir": str(game_dir),
            "Boxscore": {"source": "file", "data_provider": "live"},
            "Possessions": {"source": "file", "data_provider": "live"},
        }
    )
    items: List[Any] = client.Game(game_id).possessions.items
    return items


def _oracle_filtered(possessions: List[Any]) -> int:
    """Number of oracle possessions counting as a real possession.

    pbpstats splits events at every possession-ending event, so each
    possession's last event is its possession-ending event; ``count_as_possession``
    is True on that event iff the possession counts. ``any(...)`` over the
    possession's events is equivalent (only the ending event can return True)
    and tolerant of any event type lacking the attribute.
    """
    return sum(
        1
        for possession in possessions
        if any(getattr(event, "count_as_possession", False) for event in possession.events)
    )


def _oracle_boundaries(possessions: List[Any]) -> Set[int]:
    """The per-possession END action numbers (``event_num``) of the oracle."""
    return {possession.events[-1].event_num for possession in possessions}


def _sdv_boundaries(enhanced: pl.DataFrame, sdv: pl.DataFrame) -> Set[int]:
    """The per-possession END action numbers of sdv (end_order_index → action_number)."""
    idx: Dict[int, int] = dict(zip(enhanced["order_index"].to_list(), enhanced["action_number"].to_list()))
    return {idx[i] for i in sdv["end_order_index"].to_list()}


# ---------------------------------------------------------------------------
# The gate
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("game_id", GAMES)
def test_possession_count_matches_oracle(game_id: str, tmp_path: pathlib.Path) -> None:
    """Filtered possession counts match the oracle to within the classified residual."""
    enhanced = _enh(game_id)
    sdv = build_possessions(enhanced)
    oracle = _pbpstats_possessions(game_id, tmp_path)

    sdv_raw = sdv.height
    sdv_filtered = sdv.filter(pl.col("count_as_possession") == True).height  # noqa: E712
    oracle_raw = len(oracle)
    oracle_filtered = _oracle_filtered(oracle)

    allowed = EXPECTED_RESIDUALS.get(game_id, 0)
    assert sdv_filtered - oracle_filtered == allowed, {
        "game_id": game_id,
        "sdv_raw": sdv_raw,
        "sdv_filtered": sdv_filtered,
        "oracle_raw": oracle_raw,
        "oracle_filtered": oracle_filtered,
        "actual_delta": sdv_filtered - oracle_filtered,
        "allowed_delta": allowed,
    }


@pytest.mark.parametrize("game_id", GAMES)
def test_boundary_by_boundary_diff(game_id: str, tmp_path: pathlib.Path) -> None:
    """Per-possession END action numbers agree set-for-set, modulo cited exclusions."""
    enhanced = _enh(game_id)
    sdv = build_possessions(enhanced)
    oracle = _pbpstats_possessions(game_id, tmp_path)

    sdv_bounds = _sdv_boundaries(enhanced, sdv)
    oracle_bounds = _oracle_boundaries(oracle)

    only_sdv = sdv_bounds - oracle_bounds
    only_oracle = oracle_bounds - sdv_bounds

    expected = _BOUNDARY_EXCLUSIONS[game_id]
    assert only_sdv == expected["sdv_only"], {
        "game_id": game_id,
        "unexpected_sdv_only": sorted(only_sdv - expected["sdv_only"]),
        "missing_sdv_only": sorted(expected["sdv_only"] - only_sdv),
    }
    assert only_oracle == expected["oracle_only"], {
        "game_id": game_id,
        "unexpected_oracle_only": sorted(only_oracle - expected["oracle_only"]),
        "missing_oracle_only": sorted(expected["oracle_only"] - only_oracle),
    }
