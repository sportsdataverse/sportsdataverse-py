"""Oracle test for :mod:`sportsdataverse.mbb.mbb_ncaa_stint_validation` (Task 5d.1).

``test_validate_lineup_oracle`` is a 1:1 transliteration of the
``"validate_lineup"`` block in ``LineupErrorAnalysisUtilsTests.scala``
(``:55-120``, read-only cbb-explorer clone) -- the ONLY upstream oracle for
``LineupErrorAnalysisUtils.scala``'s validation half (the clumping/
self-healing functions ported in later 5d sub-tasks have no such oracle; see
that module's docstring).
"""

from __future__ import annotations

from dataclasses import replace
from datetime import datetime

from sportsdataverse.mbb.mbb_ncaa_models import (
    LineupEvent,
    LineupEventStats,
    LineupId,
    LocationType,
    RawGameEvent,
    ScoreInfo,
    TeamId,
    TeamSeasonId,
    Year,
)
from sportsdataverse.mbb.mbb_ncaa_stint_validation import ValidationError, validate_lineup
from sportsdataverse.mbb.mbb_ncaa_stints import build_player_code


def test_validate_lineup_oracle() -> None:
    """Transliterated from ``LineupErrorAnalysisUtilsTests.scala:55-120``.

    Builds 8 player codes (``build_player_code(name, None)``, matching the
    oracle's ``ExtractorUtils.build_player_code(_, None)``), a
    ``base_lineup`` template (``:72-88``), 6 lineup variants derived from it
    (``:90-100``), and asserts ``validate_lineup(...).toList`` for each --
    ported here as a plain list-equality assert (the oracle's ``.toList`` on
    an ``Enumeration`` ``Set`` always yields declaration order, which is
    exactly what this port's :func:`~sportsdataverse.mbb
    .mbb_ncaa_stint_validation.validate_lineup` returns natively -- see that
    module's "Return shape" docstring note).
    """
    player_names = [
        "Player One",
        "Player Two",
        "Player Three",
        "Player Four",
        "Player Five",
        "Player Six",
        "Player Seven",
    ]
    all_players = [build_player_code(name, None) for name in player_names]
    player1, player2, player3, player4, player5, _player6, _player7 = all_players
    all_player_set = {p.code for p in all_players}
    player8 = build_player_code("Player Eight", None)

    valid_players = [player1, player2, player3, player4, player5]
    too_few_players = [player1, player2, player3, player4]
    unknown_player = [player1, player2, player3, player4, player8]
    multi_bad = [player8] + valid_players

    my_team = TeamSeasonId(TeamId("TestTeam1"), Year(2017))
    other_team = TeamSeasonId(TeamId("TestTeam2"), Year(2017))
    base_lineup = LineupEvent(
        date=datetime.now(),
        location_type=LocationType.HOME,
        start_min=0.0,
        end_min=-100.0,
        duration_mins=0.0,
        score_info=ScoreInfo.empty(),
        team=my_team,
        opponent=other_team,
        lineup_id=LineupId.unknown,
        players=[],
        players_in=[],
        players_out=[],
        raw_game_events=[],
        team_stats=LineupEventStats.empty(),
        opponent_stats=LineupEventStats.empty(),
    )

    good_lineup = replace(base_lineup, players=valid_players)
    lineup_too_many = replace(base_lineup, players=all_players)
    lineup_too_few = replace(base_lineup, players=too_few_players)
    lineup_unknown_player = replace(base_lineup, players=unknown_player)
    lineup_multi_bad = replace(base_lineup, players=multi_bad)
    lineup_inactive = replace(
        base_lineup,
        players=valid_players,
        raw_game_events=[RawGameEvent.for_team("0:00,0-0,PLAYER,BAD Does Stuff", 0.0)],
    )

    assert validate_lineup(good_lineup, base_lineup, all_player_set) == []
    assert validate_lineup(lineup_too_many, base_lineup, all_player_set) == [
        ValidationError.WRONG_NUMBER_OF_PLAYERS,
    ]
    assert validate_lineup(lineup_too_few, base_lineup, all_player_set) == [
        ValidationError.WRONG_NUMBER_OF_PLAYERS,
    ]
    assert validate_lineup(lineup_unknown_player, base_lineup, all_player_set) == [
        ValidationError.UNKNOWN_PLAYERS,
    ]
    assert validate_lineup(lineup_multi_bad, base_lineup, all_player_set) == [
        ValidationError.WRONG_NUMBER_OF_PLAYERS,
        ValidationError.UNKNOWN_PLAYERS,
    ]
    assert validate_lineup(lineup_inactive, base_lineup, all_player_set) == [
        ValidationError.INACTIVE_PLAYERS,
    ]
