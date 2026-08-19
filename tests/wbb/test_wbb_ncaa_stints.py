"""Identity tests: the wbb data_quality/names/stints shims re-export the mbb core by reference."""

from __future__ import annotations


def test_wbb_ncaa_data_quality_shim_is_mbb_core():
    from sportsdataverse.mbb import mbb_ncaa_data_quality as m
    from sportsdataverse.wbb import wbb_ncaa_data_quality as w

    assert w.ParseError is m.ParseError
    assert w.build_sub_error is m.build_sub_error
    assert w.combos is m.combos
    assert w.fix_combos is m.fix_combos
    assert w.alias_combos is m.alias_combos
    assert w.generic_misspellings is m.generic_misspellings
    assert w.misspellings is m.misspellings
    assert w.players_with_duplicate_names is m.players_with_duplicate_names
    assert w.team_aliases is m.team_aliases


def test_wbb_ncaa_data_quality_all_matches_reexported_symbols():
    from sportsdataverse.wbb import wbb_ncaa_data_quality as w

    assert set(w.__all__) == {name for name in w.__all__ if hasattr(w, name)}
    assert len(w.__all__) == 9


def test_wbb_ncaa_names_shim_is_mbb_core():
    from sportsdataverse.mbb import mbb_ncaa_names as m
    from sportsdataverse.wbb import wbb_ncaa_names as w

    assert w.MIN_SURNAME_SCORE is m.MIN_SURNAME_SCORE
    assert w.MIN_FIRST_NAME_SCORE is m.MIN_FIRST_NAME_SCORE
    assert w.MIN_OVERALL_SCORE is m.MIN_OVERALL_SCORE
    assert w.MIN_USEFUL_SURNAME_LEN is m.MIN_USEFUL_SURNAME_LEN
    assert w.MIN_USEFUL_FIRST_NAME_LEN is m.MIN_USEFUL_FIRST_NAME_LEN
    assert w.TidyPlayerContext is m.TidyPlayerContext
    assert w.build_tidy_player_context is m.build_tidy_player_context
    assert w.tidy_player is m.tidy_player
    assert w.convert_from_initials is m.convert_from_initials
    assert w.convert_from_digits is m.convert_from_digits
    assert w.NoSurnameMatch is m.NoSurnameMatch
    assert w.WeakSurnameMatch is m.WeakSurnameMatch
    assert w.StrongSurnameMatch is m.StrongSurnameMatch
    assert w.MatchResult is m.MatchResult
    assert w.FuzzyMatchError is m.FuzzyMatchError
    assert w.box_aware_compare is m.box_aware_compare
    assert w.fuzzy_box_match is m.fuzzy_box_match


def test_wbb_ncaa_names_all_matches_reexported_symbols():
    from sportsdataverse.wbb import wbb_ncaa_names as w

    assert set(w.__all__) == {name for name in w.__all__ if hasattr(w, name)}
    # 18 since the sibling-code fix added `code_from_box` -- the boundary helper
    # every PBP path must use instead of re-deriving with `build_player_code`.
    assert len(w.__all__) == 18


def test_wbb_ncaa_stints_shim_is_mbb_core():
    from sportsdataverse.mbb import mbb_ncaa_stints as m
    from sportsdataverse.wbb import wbb_ncaa_stints as w

    assert w.PLAYER_CODE_MAX_LENGTH is m.PLAYER_CODE_MAX_LENGTH
    assert w.PLAYER_CODE_MAX_FRAGMENT_LENGTH is m.PLAYER_CODE_MAX_FRAGMENT_LENGTH
    assert w.remove_diacritics is m.remove_diacritics
    assert w.build_player_code is m.build_player_code
    assert w.parse_team_name is m.parse_team_name
    assert w.SUB_SAFETY_DELTA_MINS is m.SUB_SAFETY_DELTA_MINS
    assert w.SubInEvent is m.SubInEvent
    assert w.SubOutEvent is m.SubOutEvent
    assert w.OtherTeamEvent is m.OtherTeamEvent
    assert w.OtherOpponentEvent is m.OtherOpponentEvent
    assert w.GameBreakEvent is m.GameBreakEvent
    assert w.GameEndEvent is m.GameEndEvent
    assert w.SubEvent is m.SubEvent
    assert w.MiscGameEvent is m.MiscGameEvent
    assert w.MiscGameBreak is m.MiscGameBreak
    assert w.PlayByPlayEvent is m.PlayByPlayEvent
    assert w.LineupBuildingState is m.LineupBuildingState
    assert w.reorder_and_reverse is m.reorder_and_reverse
    assert w.build_partial_lineup_list is m.build_partial_lineup_list
    assert w.build_new_player_list is m.build_new_player_list
    assert w.build_lineup_id is m.build_lineup_id
    assert w.start_time_from_period is m.start_time_from_period
    assert w.duration_from_period is m.duration_from_period


def test_wbb_ncaa_stints_all_matches_reexported_symbols():
    from sportsdataverse.wbb import wbb_ncaa_stints as w

    assert set(w.__all__) == {name for name in w.__all__ if hasattr(w, name)}
    assert len(w.__all__) == 23
