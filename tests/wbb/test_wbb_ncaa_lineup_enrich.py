"""Identity tests: the wbb lineup_enrich shim re-exports the mbb core by reference."""

from __future__ import annotations


def test_wbb_ncaa_lineup_enrich_shim_is_mbb_core():
    from sportsdataverse.mbb import mbb_ncaa_lineup_enrich as m
    from sportsdataverse.wbb import wbb_ncaa_lineup_enrich as w

    assert w.enrich_lineup is m.enrich_lineup
    assert w.add_stats_to_lineups is m.add_stats_to_lineups
    assert w.fix_possible_score_swap_bug is m.fix_possible_score_swap_bug
    assert w.enrich_stats is m.enrich_stats
    assert w.ensure_ev_uniqueness is m.ensure_ev_uniqueness
    assert w.is_scramble is m.is_scramble
    assert w.is_end_of_game_fouling_vs_fastbreak is m.is_end_of_game_fouling_vs_fastbreak
    assert w.is_transition is m.is_transition
    assert w.create_player_events is m.create_player_events
    assert w.sum_event_stats is m.sum_event_stats
    assert w.sum_shot_infos is m.sum_shot_infos


def test_wbb_ncaa_lineup_enrich_all_matches_reexported_symbols():
    from sportsdataverse.wbb import wbb_ncaa_lineup_enrich as w

    assert set(w.__all__) == {name for name in w.__all__ if hasattr(w, name)}
    assert len(w.__all__) == 11
