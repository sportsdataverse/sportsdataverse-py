"""Identity tests: the wbb ratings/luck shims re-export the mbb core by reference."""

from __future__ import annotations


def test_wbb_ratings_shim_is_mbb_core():
    from sportsdataverse.mbb import mbb_ratings as m
    from sportsdataverse.wbb import wbb_ratings as w

    assert w.build_o_rtg is m.build_o_rtg
    assert w.build_d_rtg is m.build_d_rtg
    assert w.build_net_points is m.build_net_points
    assert w.adjust_off_rating_stats is m.adjust_off_rating_stats
    assert w.build_productivity is m.build_productivity


def test_wbb_luck_shim_is_mbb_core():
    from sportsdataverse.mbb import mbb_luck as m
    from sportsdataverse.wbb import wbb_luck as w

    assert w.LUCK_AFFECTED_FIELDS is m.LUCK_AFFECTED_FIELDS
    assert w.build_exp_3p is m.build_exp_3p
    assert w.build_adjusted_3p is m.build_adjusted_3p
    assert w.build_3p_shot_info is m.build_3p_shot_info
    assert w.calc_off_team_luck_adj is m.calc_off_team_luck_adj
    assert w.calc_off_player_luck_adj is m.calc_off_player_luck_adj
    assert w.calc_def_team_luck_adj is m.calc_def_team_luck_adj
    assert w.calc_def_player_luck_adj is m.calc_def_player_luck_adj
    assert w.inject_luck is m.inject_luck
