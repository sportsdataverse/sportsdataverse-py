"""`_is_documented` is satisfied by a reference block, never by a mention (A-M3)."""

from __future__ import annotations

import pytest

from tools.codegen import generate

CORPUS = (
    "see `player_stats_v3` and load_cfb_pbp(seasons=2024); ESPN's [espn_nba_pbp](x#espn_nba_pbp)\n"
    "## find_team\n"
    "### espn_cfb_calendar {#espn_cfb_calendar}\n"
    "## `load_nfl_pbp`\n"
    "| `load_pbp` | rows come from load_pbp |\n"
    "## player_stats_v3\n"
    "# Score\n"
    "## Year of the game\n"
)


@pytest.mark.parametrize(
    ("name", "documented"),
    [
        ("find_team", True),  # ## name
        ("espn_cfb_calendar", True),  # ### name {#anchor} (family split pages)
        ("load_nfl_pbp", True),  # ## `name`
        ("load_cfb_pbp", False),  # a call in prose
        ("espn_nba_pbp", False),  # a link to an anchor is not the block itself
        ("load_pbp", False),  # a returns-table cell
        ("player_stats", False),  # the heading must be the whole name
        ("Score", False),  # a page title (#) is not a reference block
        ("Year", False),  # a heading that only starts with the name
    ],
)
def test_only_a_heading_that_is_the_name_documents_it(name: str, documented: bool) -> None:
    assert generate._is_documented(name, CORPUS) is documented


def test_corpus_headings_are_the_reference_block_names() -> None:
    assert generate._corpus_headings("## a_b\ntext c\n### d {#d}\n## e f\n") == frozenset({"a_b", "d"})


def test_a_changed_corpus_is_read_again() -> None:
    assert generate._is_documented("n", "## n") is True
    assert generate._is_documented("n", "a n") is False
