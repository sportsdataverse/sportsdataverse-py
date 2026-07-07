"""Identity tests: the wbb pbp_parser shim re-exports the mbb core by reference."""

from __future__ import annotations


def test_wbb_ncaa_pbp_parser_shim_is_mbb_core():
    from sportsdataverse.mbb import mbb_ncaa_pbp_parser as m
    from sportsdataverse.wbb import wbb_ncaa_pbp_parser as w

    assert w.PbpBuilders is m.PbpBuilders
    assert w.v0_builders is m.v0_builders
    assert w.v1_builders is m.v1_builders
    assert w.parse_game_score is m.parse_game_score
    assert w.parse_desc_game_time is m.parse_desc_game_time
    assert w.parse_game_event is m.parse_game_event
    assert w.enrich_and_reverse_game_events is m.enrich_and_reverse_game_events
    assert w.parse_game_events is m.parse_game_events
    assert w.get_sorted_pbp_events is m.get_sorted_pbp_events
    assert w.create_lineup_data is m.create_lineup_data


def test_wbb_ncaa_pbp_parser_all_matches_reexported_symbols():
    from sportsdataverse.wbb import wbb_ncaa_pbp_parser as w

    assert set(w.__all__) == {name for name in w.__all__ if hasattr(w, name)}
    assert len(w.__all__) == 10


def test_wbb_ncaa_pbp_parser_all_matches_mbb_all():
    from sportsdataverse.mbb import mbb_ncaa_pbp_parser as m
    from sportsdataverse.wbb import wbb_ncaa_pbp_parser as w

    assert set(w.__all__) == set(m.__all__)
