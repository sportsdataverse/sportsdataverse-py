"""Identity tests: the wbb lineup-stats shim re-exports the mbb core by reference."""

from __future__ import annotations


def test_wbb_shim_is_mbb_core():
    from sportsdataverse.mbb import mbb_lineup_stats as m
    from sportsdataverse.wbb import wbb_lineup_stats as w

    assert w.lineup_to_team_report is m.lineup_to_team_report
    assert w.calculate_aggregated_lineup_stats is m.calculate_aggregated_lineup_stats
    assert w.weighted_avg is m.weighted_avg
    assert w.complete_weighted_avg is m.complete_weighted_avg
    assert w.build_efficiency_margins is m.build_efficiency_margins
    assert w.get_stats_diff is m.get_stats_diff
