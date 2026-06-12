"""Offline test for the gh-release-list tag parser used by --audit-releases.

Regression for the title-vs-tag bug: a release whose human-friendly title differs
from its tag (``ESPN NBA Draft`` / ``espn_nba_draft``) must be keyed by TAG, not
title — otherwise the audit reports the loader's tag as a dead release.
"""

from __future__ import annotations

from tools.codegen.generate import _parse_release_tags

# Real-shaped `gh release list` rows: TITLE \t TYPE \t TAG \t PUBLISHED.
# Mixes title==tag, title!=tag, and the "Latest" TYPE marker + a blank line.
_SAMPLE = (
    "nhl_pbp_full\t\tnhl_pbp_full\t2026-04-07T13:36:02Z\n"
    "ESPN NBA Draft\t\tespn_nba_draft\t2026-05-30T11:55:57Z\n"
    "ESPN MBB Standings\t\tespn_mens_college_basketball_standings\t2026-05-30T14:51:30Z\n"
    "espn_cfb_injuries\tLatest\tespn_cfb_injuries\t2026-06-04T02:22:01Z\n"
    "truncated\trow\n"  # < 4 columns -> ignored, never misclassified as a tag
    "\n"
)


def test_parses_tag_column_not_title() -> None:
    tags = _parse_release_tags(_SAMPLE)
    assert tags == sorted(
        [
            "nhl_pbp_full",
            "espn_nba_draft",
            "espn_mens_college_basketball_standings",
            "espn_cfb_injuries",
        ]
    )
    # The human title must NOT leak in as a "tag".
    assert "ESPN NBA Draft" not in tags
    # A truncated (<4-column) row contributes nothing.
    assert "truncated" not in tags and "row" not in tags


def test_empty_input() -> None:
    assert _parse_release_tags("") == []
    assert _parse_release_tags("\n\n") == []
