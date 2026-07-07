"""Identity tests: the wbb shot_parser shim re-exports the mbb core by reference."""

from __future__ import annotations


def test_wbb_ncaa_shot_parser_shim_is_mbb_core():
    from sportsdataverse.mbb import mbb_ncaa_shot_parser as m
    from sportsdataverse.wbb import wbb_ncaa_shot_parser as w

    assert w.ShotMapDimensions is m.ShotMapDimensions
    assert w.ShotEventBuilders is m.ShotEventBuilders
    assert w.v1_builders is m.v1_builders
    assert w.create_shot_event_data is m.create_shot_event_data
    assert w.shot_js_to_html is m.shot_js_to_html
    assert w.parse_shot_html is m.parse_shot_html
    assert w.build_base_event is m.build_base_event
    assert w.phase1_shot_event_enrichment is m.phase1_shot_event_enrichment
    assert w.get_ascending_time is m.get_ascending_time
    assert w.is_team_shooting_left_to_start is m.is_team_shooting_left_to_start
    assert w.is_women_game is m.is_women_game
    assert w.transform_shot_location is m.transform_shot_location


def test_wbb_ncaa_shot_parser_all_matches_reexported_symbols():
    from sportsdataverse.wbb import wbb_ncaa_shot_parser as w

    assert set(w.__all__) == {name for name in w.__all__ if hasattr(w, name)}
    assert len(w.__all__) == 12


def test_wbb_ncaa_shot_parser_all_matches_mbb_all():
    from sportsdataverse.mbb import mbb_ncaa_shot_parser as m
    from sportsdataverse.wbb import wbb_ncaa_shot_parser as w

    assert set(w.__all__) == set(m.__all__)
