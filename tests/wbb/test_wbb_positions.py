"""Identity tests: the wbb positions shim re-exports the mbb core by reference."""

from __future__ import annotations


def test_wbb_positions_shim_is_mbb_core():
    from sportsdataverse.mbb import mbb_positions as m
    from sportsdataverse.wbb import wbb_positions as w

    assert w.POSITION_FEATURE_INIT is m.POSITION_FEATURE_INIT
    assert w.TRAD_POS_LIST is m.TRAD_POS_LIST
    assert w.POSITION_FEATURE_WEIGHTS is m.POSITION_FEATURE_WEIGHTS
    assert w.POSITION_FEATURE_AVERAGES is m.POSITION_FEATURE_AVERAGES
    assert w.HEIGHT_MEAN_STDS is m.HEIGHT_MEAN_STDS
    assert w.AVERAGE_SCORES_BY_POS is m.AVERAGE_SCORES_BY_POS
    assert w.ID_TO_POSITION is m.ID_TO_POSITION
    assert w.ABSOLUTE_POSITION_FIXES is m.ABSOLUTE_POSITION_FIXES
    assert w.RELATIVE_POSITION_FIXES is m.RELATIVE_POSITION_FIXES
    assert w.regress_shot_quality is m.regress_shot_quality
    assert w.build_position_confidences is m.build_position_confidences
    assert w.incorporate_height is m.incorporate_height
    assert w.build_position is m.build_position
    assert w.using_roster_pos is m.using_roster_pos
    assert w.pos_class_to_score is m.pos_class_to_score
    assert w.order_lineup is m.order_lineup
    assert w.apply_relative_positional_overrides is m.apply_relative_positional_overrides
    assert w.build_positional_aware_filter is m.build_positional_aware_filter
    assert w.test_positional_aware_filter is m.test_positional_aware_filter


def test_wbb_positions_all_matches_reexported_symbols():
    from sportsdataverse.wbb import wbb_positions as w

    assert set(w.__all__) == {name for name in w.__all__ if hasattr(w, name)}
    # Every re-exported public mbb_positions function/constant needed by
    # downstream callers is covered -- see test_wbb_positions_shim_is_mbb_core
    # for the per-symbol identity assertions this parity check summarizes.
    assert len(w.__all__) == 19
