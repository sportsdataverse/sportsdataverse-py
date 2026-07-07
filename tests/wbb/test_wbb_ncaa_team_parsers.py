"""Identity tests: the wbb team_parsers shim re-exports the mbb core by reference."""

from __future__ import annotations


def test_wbb_ncaa_team_parsers_shim_is_mbb_core():
    from sportsdataverse.mbb import mbb_ncaa_team_parsers as m
    from sportsdataverse.wbb import wbb_ncaa_team_parsers as w

    assert w.get_team_triples is m.get_team_triples
    assert w.build_lineup_cli_array is m.build_lineup_cli_array
    assert w.build_available_team_list is m.build_available_team_list
    assert w.ScheduleBuilders is m.ScheduleBuilders
    assert w.v0_builders is m.v0_builders
    assert w.v1_builders is m.v1_builders
    assert w.get_neutral_games is m.get_neutral_games


def test_wbb_ncaa_team_parsers_all_matches_reexported_symbols():
    from sportsdataverse.wbb import wbb_ncaa_team_parsers as w

    assert set(w.__all__) == {name for name in w.__all__ if hasattr(w, name)}
    assert len(w.__all__) == 7


def test_wbb_ncaa_team_parsers_all_matches_mbb_all():
    from sportsdataverse.mbb import mbb_ncaa_team_parsers as m
    from sportsdataverse.wbb import wbb_ncaa_team_parsers as w

    assert set(w.__all__) == set(m.__all__)
