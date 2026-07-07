"""Identity tests: the wbb pbp_glue shim re-exports the mbb core by reference."""

from __future__ import annotations


def test_wbb_ncaa_pbp_glue_shim_is_mbb_core():
    from sportsdataverse.mbb import mbb_ncaa_pbp_glue as m
    from sportsdataverse.wbb import wbb_ncaa_pbp_glue as w

    assert w.PeekableIterator is m.PeekableIterator
    assert w.inject_starting_lineup_into_box is m.inject_starting_lineup_into_box
    assert w.enrich_shot_events_with_pbp is m.enrich_shot_events_with_pbp
    assert w.find_lineup is m.find_lineup
    assert w.find_pbp_clump is m.find_pbp_clump
    assert w.matching_player is m.matching_player
    assert w.extract_player_from_ev is m.extract_player_from_ev
    assert w.right_kind_of_shot is m.right_kind_of_shot
    assert w.shot_value is m.shot_value


def test_wbb_ncaa_pbp_glue_all_matches_reexported_symbols():
    from sportsdataverse.wbb import wbb_ncaa_pbp_glue as w

    assert set(w.__all__) == {name for name in w.__all__ if hasattr(w, name)}
    assert len(w.__all__) == 9


def test_wbb_ncaa_pbp_glue_all_matches_mbb_all():
    from sportsdataverse.mbb import mbb_ncaa_pbp_glue as m
    from sportsdataverse.wbb import wbb_ncaa_pbp_glue as w

    assert set(w.__all__) == set(m.__all__)
