"""Identity tests: the wbb NCAA shims re-export the mbb core by reference."""

from __future__ import annotations


def test_wbb_ncaa_models_shim_is_mbb_core():
    from sportsdataverse.mbb import mbb_ncaa_models as m
    from sportsdataverse.wbb import wbb_ncaa_models as w

    assert w.LocationType is m.LocationType
    assert w.Score is m.Score
    assert w.TeamId is m.TeamId
    assert w.PlayerId is m.PlayerId
    assert w.Year is m.Year
    assert w.TeamSeasonId is m.TeamSeasonId
    assert w.Direction is m.Direction
    assert w.RawGameEvent is m.RawGameEvent
    assert w.PossessionEvent is m.PossessionEvent
    assert w.ScoreInfo is m.ScoreInfo
    assert w.LineupId is m.LineupId
    assert w.PlayerCodeId is m.PlayerCodeId
    assert w.ShotClockStats is m.ShotClockStats
    assert w.FieldGoalStats is m.FieldGoalStats
    assert w.AssistEvent is m.AssistEvent
    assert w.AssistInfo is m.AssistInfo
    assert w.PlayerShotInfo is m.PlayerShotInfo
    assert w.LineupEventStats is m.LineupEventStats
    assert w.LineupEvent is m.LineupEvent
    assert w.PossCalcFragment is m.PossCalcFragment
    assert w.poss_calc_fragment_sum is m.poss_calc_fragment_sum
    assert w.score_to_tuple is m.score_to_tuple


def test_wbb_ncaa_models_all_matches_reexported_symbols():
    from sportsdataverse.wbb import wbb_ncaa_models as w

    assert set(w.__all__) == {name for name in w.__all__ if hasattr(w, name)}
    assert len(w.__all__) == 22


def test_wbb_ncaa_events_shim_is_mbb_core():
    from sportsdataverse.mbb import mbb_ncaa_events as m
    from sportsdataverse.wbb import wbb_ncaa_events as w

    assert w.is_gen2 is m.is_gen2
    assert w.parse_game_time is m.parse_game_time
    assert w.parse_team_sub_in is m.parse_team_sub_in
    assert w.parse_team_sub_out is m.parse_team_sub_out
    assert w.parse_any_play is m.parse_any_play
    assert w.parse_jumpball_won_or_lost is m.parse_jumpball_won_or_lost
    assert w.parse_jumpball_won is m.parse_jumpball_won
    assert w.parse_timeout is m.parse_timeout
    assert w.parse_rim_made is m.parse_rim_made
    assert w.parse_rim_missed is m.parse_rim_missed
    assert w.parse_two_pointer_made is m.parse_two_pointer_made
    assert w.parse_two_pointer_missed is m.parse_two_pointer_missed
    assert w.parse_three_pointer_made is m.parse_three_pointer_made
    assert w.parse_three_pointer_missed is m.parse_three_pointer_missed
    assert w.parse_shot_made is m.parse_shot_made
    assert w.parse_shot_missed is m.parse_shot_missed
    assert w.parse_shot_blocked is m.parse_shot_blocked
    assert w.parse_rebound is m.parse_rebound
    assert w.parse_offensive_rebound is m.parse_offensive_rebound
    assert w.parse_defensive_rebound is m.parse_defensive_rebound
    assert w.parse_deadball_rebound is m.parse_deadball_rebound
    assert w.parse_offensive_deadball_rebound is m.parse_offensive_deadball_rebound
    assert w.parse_live_offensive_rebound is m.parse_live_offensive_rebound
    assert w.parse_free_throw_made is m.parse_free_throw_made
    assert w.parse_free_throw_missed is m.parse_free_throw_missed
    assert w.parse_free_throw_attempt is m.parse_free_throw_attempt
    assert w.parse_free_throw_event is m.parse_free_throw_event
    assert w.parse_free_throw_event_attempt_gen2 is m.parse_free_throw_event_attempt_gen2
    assert w.parse_turnover is m.parse_turnover
    assert w.parse_stolen is m.parse_stolen
    assert w.parse_assist is m.parse_assist
    assert w.parse_personal_foul is m.parse_personal_foul
    assert w.parse_technical_foul is m.parse_technical_foul
    assert w.parse_flagrant_foul is m.parse_flagrant_foul
    assert w.parse_offensive_foul is m.parse_offensive_foul
    assert w.parse_foul_info is m.parse_foul_info
    assert w.parse_offensive_event is m.parse_offensive_event
    assert w.parse_defensive_action_event is m.parse_defensive_action_event
    assert w.parse_defensive_info_event is m.parse_defensive_info_event
    assert w.parse_defensive_event is m.parse_defensive_event


def test_wbb_ncaa_events_all_matches_reexported_symbols():
    from sportsdataverse.wbb import wbb_ncaa_events as w

    assert set(w.__all__) == {name for name in w.__all__ if hasattr(w, name)}
    assert len(w.__all__) == 40


def test_wbb_ncaa_possessions_shim_is_mbb_core():
    from sportsdataverse.mbb import mbb_ncaa_possessions as m
    from sportsdataverse.wbb import wbb_ncaa_possessions as w

    assert w.ConcurrentClump is m.ConcurrentClump
    assert w.PossState is m.PossState
    assert w.lineup_as_raw_clumps is m.lineup_as_raw_clumps
    assert w.concurrent_event_handler is m.concurrent_event_handler
    assert w.count_matching is m.count_matching
    assert w.calculate_stats is m.calculate_stats
    assert w.calculate_possessions_by_event is m.calculate_possessions_by_event
    assert w.calculate_possessions is m.calculate_possessions
    assert w.lineup_balancer is m.lineup_balancer
    assert w.lineup_fixer is m.lineup_fixer
    assert w.assign_to_right_lineup is m.assign_to_right_lineup


def test_wbb_ncaa_possessions_all_matches_reexported_symbols():
    from sportsdataverse.wbb import wbb_ncaa_possessions as w

    assert set(w.__all__) == {name for name in w.__all__ if hasattr(w, name)}
    assert len(w.__all__) == 11
