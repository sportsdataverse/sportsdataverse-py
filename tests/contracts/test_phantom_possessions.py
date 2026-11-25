"""The phantom-possession check on three real contests of the 2026 ``ncaa_mbb_possessions`` release.

Fixture: tests/fixtures/ncaa_mbb_possessions/phantom_contests_2026.parquet (provenance in its README).
"""

from __future__ import annotations

from pathlib import Path

import polars as pl

from sportsdataverse.validation.findings import CheckContext, Severity
from tools.validation.checks import phantom_possessions

FIX = Path(__file__).resolve().parents[1] / "fixtures" / "ncaa_mbb_possessions" / "phantom_contests_2026.parquet"
CTX = CheckContext(domain="mbb", dataset="ncaa_mbb_possessions", schema={})


def test_phantom_counts_per_team_game() -> None:
    counts = phantom_possessions.phantom_counts(pl.read_parquet(FIX))
    assert counts.rows() == [
        ("6388769", "Buffalo", 0, 66),
        ("6388769", "Southern Miss.", 1, 67),
        ("6406421", "Lindenwood", 1, 72),
        ("6406421", "Southern Ind.", 0, 73),
        ("6465160", "Hampton", 4, 76),
        ("6465160", "Howard", 7, 79),
    ]


def test_a_teams_last_possession_of_a_period_can_be_a_phantom() -> None:
    # Lindenwood's phantom is its own last possession of the period, not the period's last: a window
    # taken inside the team group would read it as the period's last and drop it.
    frame = pl.read_parquet(FIX).filter(pl.col("contest_id") == "6406421")
    counts = phantom_possessions.phantom_counts(frame)
    assert counts.filter(pl.col("poss_team") == "Lindenwood")["phantoms"].item() == 1


def test_only_the_tail_team_game_is_flagged() -> None:
    findings = phantom_possessions.run("ncaa_mbb_possessions", pl.read_parquet(FIX), CTX)
    assert [(f.locator["contest_id"], f.locator["poss_team"], f.actual) for f in findings] == [("6465160", "Howard", 7)]
    assert findings[0].severity is Severity.WARN and findings[0].needs_judgment


def test_a_quick_turnover_is_not_a_phantom() -> None:
    frame = pl.read_parquet(FIX).filter(pl.col("contest_id") == "6465160")
    as_turnovers = frame.with_columns(pl.lit("Turnover").alias("last_event_type"))
    assert phantom_possessions.phantom_counts(as_turnovers)["phantoms"].sum() == 0


def test_other_frames_are_skipped() -> None:
    assert phantom_possessions.run("cfb_pbp", pl.DataFrame({"game_id": [1]}), CTX) == []
