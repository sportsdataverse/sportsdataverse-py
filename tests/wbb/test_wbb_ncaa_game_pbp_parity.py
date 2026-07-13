"""WBB parity: ``parse_ncaa_bb_game_pbp(period_model=(4, 600, 300))`` vs oracle.

The WBB oracle was produced by wbigballR (an MBB-halves bigballR fork) run on
quarter-format pages, so time-derived columns are wrong-by-construction in the
oracle (``WBB_CLOCK_TAINTED``). Empirical per-column verdicts (all 4 fixture
games, row counts and row order identical everywhere):

* MATCH-ALL (promoted from the tainted list to strict parity): ``period``
  (oracle Half_Status numbers coincide with quarter numbers), ``poss_num`` /
  ``poss_team`` (the possession loop only compares seconds *within* a period,
  where the R clock error is a constant offset), and ``status`` (the
  sub-parity check happened to agree on every fixture game).
* ``event_type``: strict except held-ball jumpballs — the oracle (wbigballR)
  says ``"held Jumpball"``, the port deliberately adopts bigballR's
  ``"Jumpball (held ball)"`` (divergence spec §2.6 / §3 ``heldball_label``
  knob: one canonical label for both leagues). Compared strictly after
  mapping the oracle label.
* DIVERGES → invariant-tested: ``game_time`` / ``game_seconds`` /
  ``event_length`` / ``poss_length`` (R's halves math offsets every quarter),
  ``is_transition`` (depends on poss_length at period boundaries),
  ``home_1``..``away_5`` + ``sub_deviate`` (R's period-start clock strings
  ``"20:00"``/``"05:00"`` never match quarter starts, so R's starter inference
  and lineup walk ran on unfiltered sub lists), and ``is_garbage_time``
  (thresholds keyed to game_seconds + lineups).
"""

from __future__ import annotations

import polars as pl
import pytest

from sportsdataverse.mbb.mbb_ncaa_game_pbp import PBP_SCHEMA, parse_ncaa_bb_game_pbp
from tests.mbb._bigballr_oracle import (
    GAMES,
    HTML_DIR,
    WBB_CLOCK_TAINTED,
    load_oracle_pbp,
)

WBB_PERIOD_MODEL = (4, 600, 300)

#: Tainted columns proven to match the oracle on every fixture game.
PROMOTED_TO_STRICT = ["period", "poss_num", "poss_team", "status"]

#: Overtime periods per fixture game (regulation ends at 2400 = 4x600s).
EXPECTED_OTS = {
    "5722355": 0,
    "5732292": 0,
    "5728709": 1,
    "5733807": 2,
}


@pytest.fixture(scope="module")
def oracle() -> pl.DataFrame:
    return load_oracle_pbp("wbb")


def _parse(game_id: str) -> pl.DataFrame:
    """Parse in FAITHFUL mode — the shipped default fixes bigballR's missing
    technical/flagrant possession rules (BUG-3), which the R oracle lacks."""
    html = (HTML_DIR / f"pbp_{game_id}.html").read_text(encoding="utf-8")
    return parse_ncaa_bb_game_pbp(html, game_id, period_model=WBB_PERIOD_MODEL, fix_technicals=False)


@pytest.mark.parametrize("game_id", GAMES["wbb"])
def test_wbb_pbp_parity(game_id: str, oracle: pl.DataFrame) -> None:
    got = _parse(game_id)
    exp = oracle.filter(pl.col("game_id") == game_id)

    assert got.columns == list(PBP_SCHEMA)
    assert got.height == exp.height, f"row count {got.height} != oracle {exp.height}"
    strict = [c for c in PBP_SCHEMA if c not in WBB_CLOCK_TAINTED and c != "event_type"] + PROMOTED_TO_STRICT
    for col in strict:
        assert got[col].to_list() == exp[col].to_list(), f"column {col!r} diverges"

    # event_type: strict modulo the deliberate held-ball label divergence
    # (wbigballR "held Jumpball" -> canonical bigballR "Jumpball (held ball)").
    exp_types = ["Jumpball (held ball)" if t == "held Jumpball" else t for t in exp["event_type"].to_list()]
    assert got["event_type"].to_list() == exp_types


@pytest.mark.parametrize("game_id", GAMES["wbb"])
def test_wbb_pbp_clock_invariants(game_id: str) -> None:
    """Invariants for the oracle-tainted clock-derived columns."""
    got = _parse(game_id)
    n_ot = EXPECTED_OTS[game_id]
    final_end = 4 * 600 + 300 * n_ot

    # a regulation game's clock lives in (0, 2400]; +300 per OT — the last
    # event may precede the final buzzer, but must fall inside the final period
    max_gs = got["game_seconds"].max()
    assert isinstance(max_gs, int)
    assert final_end - 300 < max_gs <= final_end
    assert got["period"].min() == 1
    assert got["period"].max() == 4 + n_ot
    assert got["period"].is_between(1, 6).all()

    # game_seconds non-decreasing within each period
    for _, grp in got.group_by("period", maintain_order=True):
        secs = grp["game_seconds"].to_list()
        assert all(a <= b for a, b in zip(secs, secs[1:]))

    assert (got["event_length"] >= 0).all()
    assert got["event_length"].null_count() == 0
    assert (got["poss_length"] >= 0).all()
    # lineups fully populated + statuses clean on these fixtures
    for k in range(1, 6):
        assert got[f"home_{k}"].null_count() == 0
        assert got[f"away_{k}"].null_count() == 0
    assert got["status"].unique().to_list() == ["CLEAN"]
    assert got["is_transition"].dtype == pl.Boolean
    assert got["is_garbage_time"].dtype == pl.Boolean
