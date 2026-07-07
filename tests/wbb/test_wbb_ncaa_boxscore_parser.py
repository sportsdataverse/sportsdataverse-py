"""Identity tests: the wbb boxscore_parser shim re-exports the mbb core by reference."""

from __future__ import annotations


def test_wbb_ncaa_boxscore_parser_shim_is_mbb_core():
    from sportsdataverse.mbb import mbb_ncaa_boxscore_parser as m
    from sportsdataverse.wbb import wbb_ncaa_boxscore_parser as w

    assert w.get_box_lineup is m.get_box_lineup
    assert w.inject_validated_players is m.inject_validated_players
    assert w.parse_period_from_filename is m.parse_period_from_filename
    assert w.parse_date is m.parse_date
    assert w.parse_final_score is m.parse_final_score
    assert w.parse_players_from_boxscore is m.parse_players_from_boxscore
    assert w.validate_box_score is m.validate_box_score


def test_wbb_ncaa_boxscore_parser_all_matches_reexported_symbols():
    from sportsdataverse.wbb import wbb_ncaa_boxscore_parser as w

    assert set(w.__all__) == {name for name in w.__all__ if hasattr(w, name)}
    assert len(w.__all__) == 7


def test_wbb_ncaa_boxscore_parser_all_matches_mbb_all():
    from sportsdataverse.mbb import mbb_ncaa_boxscore_parser as m
    from sportsdataverse.wbb import wbb_ncaa_boxscore_parser as w

    assert set(w.__all__) == set(m.__all__)
