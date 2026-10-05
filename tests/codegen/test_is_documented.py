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


@pytest.mark.parametrize("name", ["a-b", "x.y", "x.y()"])
def test_a_name_with_non_word_characters_found_by_the_regex_fallback(name: str) -> None:
    # not one word run, so the word set alone would say "absent"; the regex fallback finds it
    corpus = "see a-b and x.y() here"
    assert generate._is_documented(name, corpus) is _regex(name, corpus)
    assert generate._is_documented("a-b", corpus) is True


def test_a_changed_corpus_is_read_again() -> None:
    assert generate._is_documented("n", "a n") is True
    assert generate._is_documented("n", "a") is False
