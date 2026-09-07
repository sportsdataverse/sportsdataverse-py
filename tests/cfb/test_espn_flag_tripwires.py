# tests/cfb/test_espn_flag_tripwires.py
"""QA tripwires comparing ESPN's native per-play flags to the package's derived signals.

ESPN now ships two native booleans on every CFB play -- ``isTurnover`` and ``isPenalty``
(populated back to 2018) -- which survive the flattening pipeline as columns. They are NOT
used as a source of truth (the text-based derivation is richer and more reliable on
interceptions; the ESPN flags silently drop ~16% of plain interceptions on sparse-text
plays), but they make excellent regression tripwires:

* **Turnover coverage** -- every play ESPN flags ``isTurnover=True`` must be accounted for by
  the derivation as either a giveaway (``is_turnover``) or a blocked-punt possession loss
  (``is_blocked_punt_turnover``). This would have caught the interception-erasure bug where a
  pick-then-fumble was relabeled ``Fumble Recovery (Opponent)`` and lost its ``is_turnover``.
* **Penalty coverage** -- every play ESPN flags ``isPenalty=True`` (penalty is the play's
  primary outcome) must have ``penalty_flag=True`` (the broad "penalty mentioned" signal). The
  reverse does not hold (``penalty_flag`` also catches penalties tacked onto real plays), so it
  is intentionally not asserted.

The reverse direction (derived True / ESPN False) is EXPECTED and not asserted: ESPN drops some
interceptions and does not flag special-teams fumbles. These tests run offline on the captured
fixtures (no network); see test_box_score_attribution_offline.py for the same mocking pattern.
"""

from __future__ import annotations

import json
from pathlib import Path

import polars as pl
import pytest

from sportsdataverse.cfb.cfb_pbp import CFBPlayProcess

FIX = Path(__file__).parent / "fixtures"
GIDS = [401754598, 401309854, 401112081, 401135269, 401032062]


def _play_df(monkeypatch, gid: int) -> pl.DataFrame:
    summary = json.loads((FIX / f"summary_{gid}.json").read_text(encoding="utf-8"))

    class _Resp:
        def json(self):
            return summary

    monkeypatch.setattr("sportsdataverse.cfb.cfb_pbp.download", lambda *a, **k: _Resp())
    proc = CFBPlayProcess(gameId=gid)
    proc.join_participants = False  # offline: no participants fetch
    proc.espn_cfb_pbp()
    proc.run_processing_pipeline()
    return pl.from_dicts(proc.plays_json, infer_schema_length=None)


@pytest.mark.parametrize("gid", GIDS)
def test_espn_native_flags_present(monkeypatch, gid):
    df = _play_df(monkeypatch, gid)
    for col in [
        "isTurnover",
        "isPenalty",
        "is_turnover",
        "is_blocked_punt_turnover",
        "is_blocked_fg_turnover",
        "penalty_flag",
    ]:
        assert col in df.columns, f"{gid}: missing column {col}"
    # Tripwires are only meaningful if the fixture actually exercises the flags.
    assert df.filter(pl.col("isTurnover") == True).height > 0, f"{gid}: no isTurnover plays to check"
    assert df.filter(pl.col("isPenalty") == True).height > 0, f"{gid}: no isPenalty plays to check"


@pytest.mark.parametrize("gid", GIDS)
def test_isturnover_implies_derived_turnover(monkeypatch, gid):
    # Every ESPN-flagged turnover must be a giveaway (is_turnover) or a blocked-kick
    # possession loss (is_blocked_punt_turnover / is_blocked_fg_turnover).
    df = _play_df(monkeypatch, gid)
    viol = df.filter(
        (pl.col("isTurnover") == True)
        & (pl.col("is_turnover") != True)
        & (pl.col("is_blocked_punt_turnover") != True)
        & (pl.col("is_blocked_fg_turnover") != True),
    )
    assert viol.height == 0, (
        f"{gid}: {viol.height} play(s) ESPN flags isTurnover=True but the derivation does not "
        f"account for (none of is_turnover / is_blocked_punt_turnover / is_blocked_fg_turnover): "
        f"{viol.select(['type.text', 'text']).to_dicts()}"
    )


@pytest.mark.parametrize("gid", GIDS)
def test_ispenalty_implies_penalty_flag(monkeypatch, gid):
    # Every play whose primary outcome ESPN marks a penalty must trip penalty_flag.
    df = _play_df(monkeypatch, gid)
    viol = df.filter((pl.col("isPenalty") == True) & (pl.col("penalty_flag") != True))
    assert viol.height == 0, (
        f"{gid}: {viol.height} play(s) ESPN flags isPenalty=True but penalty_flag is False: "
        f"{viol.select(['type.text', 'text']).to_dicts()}"
    )


def test_plays_frame_retained_for_window_reaggregation(monkeypatch):
    """The enriched polars frame must survive run_processing_pipeline.

    plays_json becomes a list of dicts at the end of the pipeline (and Game on
    Paper's Flask layer then mutates the records, nesting start/end and
    deleting the flat dotted columns) -- so windowed re-aggregation (?span=
    boxes: filter the frame, re-run create_box_score) needs the ORIGINAL frame
    kept as plays_frame. Its silent absence broke every ?span= box request.
    """
    gid = GIDS[0]
    summary = json.loads((FIX / f"summary_{gid}.json").read_text(encoding="utf-8"))

    class _Resp:
        def json(self):
            return summary

    monkeypatch.setattr("sportsdataverse.cfb.cfb_pbp.download", lambda *a, **k: _Resp())
    proc = CFBPlayProcess(gameId=gid)
    proc.join_participants = False
    proc.espn_cfb_pbp()
    proc.run_processing_pipeline()

    assert isinstance(proc.plays_frame, pl.DataFrame)
    assert proc.plays_frame.height == len(proc.plays_json)
    # the flat dotted schema create_box_score consumes, not the nested shape
    assert "start.down" in proc.plays_frame.columns
    # and it is actually re-aggregable: a windowed box computes without error
    # (the fixture game must exercise this -- an empty window would let the
    # test pass without ever calling create_box_score)
    q3 = proc.plays_frame.filter(pl.col("period") == 3)
    assert q3.height > 0
    box = proc.create_box_score(q3)
    assert box
    # the post-pipeline prob methods must not leave the frame stale
    proc.add_fourth_down_probs()
    assert proc.plays_frame.height == len(proc.plays_json)
    assert "fourth_down_recommendation" in proc.plays_frame.columns
