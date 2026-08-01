"""Regression tests for the rushing opportunity / highlight-yards decomposition.

``opportunity_run`` was implemented as ``rush AND yds_rushed <= 4`` -- the INVERSE
of the cfbfastR oracle (``espn_cfb_15_team_summaries_creation.R``):

    opportunity_run = ((rush == 1) & (yds_rushed >= 4))

and of the sibling cfb-data producer, which agrees with the R. The inversion had a
second, silent consequence: ``opp_highlight_yards`` gates on ``opportunity_run``
while ``highlight_yards`` only accrues from 4 rushing yards up, so the two
conditions could never co-occur and the column was **identically 0 in every
published row** (verified across 162,950 plays in the 2024 release).

Nothing failed when the flag was inverted -- no test pinned either invariant,
which is exactly why it survived. These run the real pipeline over a committed
summary fixture and pin both.
"""

from __future__ import annotations

import json
from pathlib import Path

import polars as pl
import pytest

from sportsdataverse.cfb.cfb_pbp import CFBPlayProcess

FIX = Path(__file__).parent / "fixtures"


@pytest.fixture(scope="module")
def plays(request) -> pl.DataFrame:
    """Processed play frame for a committed game, via the offline fixture path."""
    summary = json.loads((FIX / "summary_401628455.json").read_text())

    class _Resp:
        def json(self):
            return summary

    mp = pytest.MonkeyPatch()
    request.addfinalizer(mp.undo)
    mp.setattr("sportsdataverse.cfb.cfb_pbp.download", lambda *a, **k: _Resp())

    # join_participants=False keeps this offline. The monkeypatch above only
    # replaces cfb_pbp.download; the participants path makes its OWN cdn sidecar
    # and $ref fetches, so leaving the default True would let this "offline"
    # regression test reach the network. None of the rushing decomposition under
    # test depends on participant joins.
    proc = CFBPlayProcess(gameId=401628455, join_participants=False)
    proc.espn_cfb_pbp()
    out = proc.run_processing_pipeline()
    df = out["plays"] if isinstance(out, dict) else out
    if isinstance(df, pl.DataFrame):
        return df
    # the pipeline hands back a list of dicts; scan every row when inferring so a
    # column that is null for the first N plays does not get the wrong dtype
    return pl.from_dicts(df, infer_schema_length=None)


def _rushes(plays: pl.DataFrame) -> pl.DataFrame:
    return plays.filter(pl.col("rush") == True).drop_nulls("yds_rushed")  # noqa: E712


def test_opportunity_run_marks_carries_that_reached_four_yards(plays):
    """The oracle is ``yds_rushed >= 4`` -- a gain, not a stuff.

    Asserted as an implication over every rush in the game rather than a fixed
    count, so the test does not re-encode this fixture's box score.
    """
    r = _rushes(plays)
    bad = r.filter(pl.col("opportunity_run") != (pl.col("yds_rushed") >= 4))
    assert bad.height == 0, (
        f"{bad.height} rushes disagree with `yds_rushed >= 4`; "
        f"sample={bad.select(['yds_rushed', 'opportunity_run']).head(5).to_dicts()}"
    )


def test_short_rush_is_not_an_opportunity(plays):
    """Guards the exact regression: sub-4-yard carries must not be flagged."""
    short = _rushes(plays).filter(pl.col("yds_rushed") < 4)
    assert short.height > 0, "fixture has no short rushes to test"
    assert not short["opportunity_run"].any()


def test_opp_highlight_yards_is_not_degenerate(plays):
    """Some carry must carry non-zero opportunity highlight yards.

    With the inverted flag this column could only ever be 0, so a single non-zero
    value anywhere is what distinguishes fixed from broken.
    """
    vals = _rushes(plays)["opp_highlight_yards"].drop_nulls()
    assert vals.len() > 0, "no opp_highlight_yards values in the fixture"
    assert (vals > 0).any(), "opp_highlight_yards is identically 0 -- opportunity_run is inverted"


def test_opp_highlight_yards_passes_through_on_opportunity_runs(plays):
    """On an opportunity run the column is exactly highlight_yards; else 0."""
    r = _rushes(plays).drop_nulls(["highlight_yards", "opp_highlight_yards"])
    on = r.filter(pl.col("opportunity_run") == True)  # noqa: E712
    off = r.filter(pl.col("opportunity_run") == False)  # noqa: E712
    assert (on["opp_highlight_yards"] == on["highlight_yards"]).all()
    assert (off["opp_highlight_yards"] == 0).all()
