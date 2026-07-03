from __future__ import annotations

from sportsdataverse.nba.nba_oracle_data import normalize_player_name


def test_normalize_lowercases_and_strips_punctuation():
    assert normalize_player_name("A.J. Green") == "aj green"


def test_normalize_strips_diacritics():
    # real stats.nba.com spells this "Nikola Jokić" (Serbian ć); the DARKO CSV
    # spells it "Nikola Jokic" (plain ASCII) -- both must fold to the same key.
    assert normalize_player_name("Nikola Jokić") == normalize_player_name("Nikola Jokic")
    assert normalize_player_name("Nikola Jokić") == "nikola jokic"


def test_normalize_strips_suffix():
    assert normalize_player_name("Gary Trent Jr.") == normalize_player_name("Gary Trent")
    assert normalize_player_name("Gary Trent Jr.") == "gary trent"


def test_normalize_collapses_whitespace():
    assert normalize_player_name("  Kevin   Durant  ") == "kevin durant"


def test_normalize_empty_string():
    assert normalize_player_name("") == ""
