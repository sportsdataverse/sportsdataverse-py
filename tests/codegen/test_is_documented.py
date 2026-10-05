"""`_is_documented` answers from one word set per corpus; it must agree with the regex it replaced."""

from __future__ import annotations

import re

import pytest

from tools.codegen import generate

CORPUS = (
    "see `player_stats_v3` and load_cfb_pbp(seasons=2024); ESPN's [espn_nba_pbp](x#espn_nba_pbp)\n"
    "## find_team\n| `café_stats` | done |\n"
)


def _regex(name: str, corpus: str) -> bool:
    return re.search(r"\b" + re.escape(name) + r"\b", corpus) is not None


@pytest.mark.parametrize(
    "name",
    [
        "player_stats",
        "player_stats_v3",
        "load_cfb_pbp",
        "load_cfb",
        "espn_nba_pbp",
        "find_team",
        "café_stats",
        "café",
        "s",
        "ESPN",
        "espn",
        "done",
        "x",
        "a-b",
        "x#espn",
    ],
)
def test_word_set_agrees_with_the_regex(name: str) -> None:
    assert generate._is_documented(name, CORPUS) == _regex(name, CORPUS)


def test_corpus_words_are_the_maximal_word_runs() -> None:
    assert generate._corpus_words("a_b, c-d `e`") == frozenset({"a_b", "c", "d", "e"})
