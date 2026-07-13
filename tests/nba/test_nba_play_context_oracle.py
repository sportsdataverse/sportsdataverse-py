"""Task 4: the pbpstats-live possession *start-type* oracle round-trip.

Like-for-like gate on the coarse possession start type. sdv's
:func:`~sportsdataverse.nba.nba_possessions.build_possessions` attaches a coarse
``possession_start_type`` (the five pbpstats families: ``OffDeadball`` /
``OffTimeout`` / ``OffMadeShot`` / ``OffMissedShot`` / ``OffLiveBallTurnover``, no
shot-type buckets). pbpstats' own ``possession_start_type`` is finer
(``OffFTMake``, ``OffArc3Miss``, ...); :func:`_coarsen` folds it down to the same
five families so the two are directly comparable. Both engines are fed from the
SAME committed cdn fixture (``tests/fixtures/nba_engine/{gid}/cdn_playbyplay.json``
+ ``cdn_boxscore.json``).

Alignment is by possession END action number — the intersection of the two
engines' possession boundaries. Phase A verified the cdn feed's ``actionNumber``
equals the v3 ``action_number`` 1-to-1, so END numbers are directly comparable;
possessions whose boundary only one engine emits (the S1/S2 artifacts the
possession oracle classifies) cannot be paired and are excluded from the rate.

Gate (floors from OBSERVED values — never lowered to make a test pass):

* per-game agreement ``>= 0.98`` (observed 0.99502 / 0.98953 / 1.00000)
* total agreement    ``>= 0.99`` (observed 592/595 = 0.99496)

The three residuals are pinned EXACTLY in :data:`EXPECTED_RESIDUALS` (an equality,
not a floor): any NEW disagreement fails the gate. Each is a pbpstats-live
possession-*segmentation* artifact — the same S2 family the possession oracle
(``test_nba_possession_oracle.py``) already classifies — NOT an sdv start-type bug.
sdv follows pbpstats' OWN documented ``possession_start_type`` rule
(``possession.py:206-242``); the disagreement is downstream of pbpstats-live
drawing a possession boundary where sdv does not, which changes the previous-
possession-ending event and hence the coarse start type. In every residual sdv is
on the side pbpstats' documented rule prescribes. Two sub-mechanisms:

* **end_action 494 / 578** (sdv ``OffMadeShot`` vs oracle ``OffTimeout``): sdv's
  possession does not start at the timeout's clock, so its documented boundary-
  timeout rule yields no timeout; pbpstats-live reaches ``OffTimeout`` only via a
  shifted boundary around the timeout/FT sequence.
* **end_action 70** (sdv ``OffMissedShot`` vs oracle ``OffDeadball``): pbpstats-live
  ends the previous possession at a personal foul (a dead-ball event, event 64), so
  its previous-possession-ending event is a ``Foul`` → ``OffDeadball``; sdv's prior
  boundary is the defensive rebound → ``OffMissedShot``. A foul-boundary segmentation
  difference, not the timeout family.

Gated on ``SDV_PBPSTATS_ROOT`` (a local https://github.com/dblackrun/pbpstats
checkout; pbpstats is NOT a project dependency). Unset → every test skips cleanly;
no workflow sets it. The pbpstats client plumbing + committed-fixture transport is
reused from :mod:`tests.nba.test_nba_possession_oracle`.
"""

from __future__ import annotations

import pathlib
from typing import Any, Dict, List, Set, Tuple

import polars as pl
import pytest

from sportsdataverse.nba.nba_possessions import build_possessions
from tests.nba.test_nba_possession_oracle import GAMES, _enh, _pbpstats_possessions

#: Observed shared-boundary count per game. Min-size guard: a shrunken / partial
#: re-capture must FAIL, not vacuously pass a rate check on a handful of rows.
OBSERVED_SHARED: Dict[str, int] = {"0022100001": 201, "0022200001": 191, "0022300001": 203}

PER_GAME_FLOOR = 0.98
TOTAL_FLOOR = 0.99

#: The classified start-type residuals, pinned exactly (equality, not a floor):
#: game_id -> {possession END action number: (sdv_coarse, oracle_fine)}. Games
#: absent default to no residuals (exact agreement). See the module docstring for
#: the classification — every entry is a pbpstats-live boundary artifact, not an
#: sdv bug.
EXPECTED_RESIDUALS: Dict[str, Dict[int, Tuple[str, str]]] = {
    "0022100001": {494: ("OffMadeShot", "OffTimeout")},
    "0022200001": {70: ("OffMissedShot", "OffDeadball"), 578: ("OffMadeShot", "OffTimeout")},
    "0022300001": {},
}


def _coarsen(fine: str) -> str:
    """pbpstats fine ``possession_start_type`` → sdv's five coarse families."""
    if fine.endswith("Make"):
        return "OffMadeShot"
    if fine.endswith("Miss") or fine.endswith("Block"):
        return "OffMissedShot"
    return fine  # OffDeadball / OffTimeout / OffLiveBallTurnover pass through


def _sdv_start_types(enhanced: pl.DataFrame, sdv: pl.DataFrame) -> Dict[int, str]:
    """{possession END action number: sdv coarse start type} (end_order_index → action_number)."""
    idx: Dict[int, int] = dict(zip(enhanced["order_index"].to_list(), enhanced["action_number"].to_list()))
    return {idx[i]: t for i, t in zip(sdv["end_order_index"].to_list(), sdv["possession_start_type"].to_list())}


def _compare(game_id: str, tmp_path: pathlib.Path) -> Tuple[Set[int], Dict[int, Tuple[str, str]]]:
    """Shared END boundaries and the coarse start-type disagreements on them.

    ``disagree`` maps each disagreeing END action number to ``(sdv_coarse,
    oracle_fine)`` so a failure message names the pbpstats fine type that moved.
    """
    enhanced = _enh(game_id)
    sdv_map = _sdv_start_types(enhanced, build_possessions(enhanced))
    oracle: List[Any] = _pbpstats_possessions(game_id, tmp_path)
    orc_fine: Dict[int, str] = {p.events[-1].event_num: p.possession_start_type for p in oracle}
    shared = set(sdv_map) & set(orc_fine)
    disagree = {k: (sdv_map[k], orc_fine[k]) for k in shared if sdv_map[k] != _coarsen(orc_fine[k])}
    return shared, disagree


@pytest.mark.parametrize("game_id", GAMES)
def test_start_type_agreement_meets_floor(game_id: str, tmp_path: pathlib.Path) -> None:
    """Per-game coarse start-type agreement clears the floor, residuals pinned exactly."""
    shared, disagree = _compare(game_id, tmp_path)

    assert len(shared) >= OBSERVED_SHARED[game_id], {
        "game_id": game_id,
        "shared": len(shared),
        "expected_at_least": OBSERVED_SHARED[game_id],
    }
    rate = (len(shared) - len(disagree)) / len(shared)
    assert rate >= PER_GAME_FLOOR, {"game_id": game_id, "rate": rate, "floor": PER_GAME_FLOOR}
    # Exact — any start-type disagreement NOT in the classified set fails here.
    assert disagree == EXPECTED_RESIDUALS[game_id], {
        "game_id": game_id,
        "unexpected": {k: v for k, v in disagree.items() if k not in EXPECTED_RESIDUALS[game_id]},
        "missing": {k: v for k, v in EXPECTED_RESIDUALS[game_id].items() if k not in disagree},
    }


def test_total_start_type_agreement_meets_floor(tmp_path: pathlib.Path) -> None:
    """Aggregate coarse start-type agreement over all three fixtures clears the total floor."""
    tot_shared = tot_disagree = 0
    for game_id in GAMES:
        shared, disagree = _compare(game_id, tmp_path)
        tot_shared += len(shared)
        tot_disagree += len(disagree)

    assert tot_shared >= sum(OBSERVED_SHARED.values()), {"total_shared": tot_shared}
    rate = (tot_shared - tot_disagree) / tot_shared
    assert rate >= TOTAL_FLOOR, {"total_rate": rate, "floor": TOTAL_FLOOR, "shared": tot_shared}
