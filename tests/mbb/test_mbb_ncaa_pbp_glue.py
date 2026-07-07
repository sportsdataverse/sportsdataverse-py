"""Offline tests for ``mbb_ncaa_pbp_glue`` -- a 1:1 transliteration of
cbb-explorer's ``PlayByPlayUtilsTests.scala`` (762 lines, pure inline
fixtures), plus one smoke test for ``inject_starting_lineup_into_box`` (which
the upstream oracle does not cover).

Fixture-building mirrors the Scala oracle exactly: ``box_players`` /
``box_lineup`` / ``base_shot_event`` / the ``base_*_pbp`` events, then per-test
copies via :func:`dataclasses.replace` (Scala's ``.copy``).
"""

from __future__ import annotations

from dataclasses import replace
from datetime import datetime

from sportsdataverse.mbb.mbb_ncaa_models import (
    LineupEvent,
    LineupEventStats,
    LineupId,
    LocationType,
    PlayerCodeId,
    PlayerId,
    RawGameEvent,
    Score,
    ScoreInfo,
    TeamId,
    TeamSeasonId,
    Year,
)
from sportsdataverse.mbb.mbb_ncaa_names import build_tidy_player_context
from sportsdataverse.mbb.mbb_ncaa_pbp_glue import (
    PeekableIterator,
    enrich_shot_events_with_pbp,
    extract_player_from_ev,
    find_lineup,
    find_pbp_clump,
    inject_starting_lineup_into_box,
    matching_player,
    right_kind_of_shot,
    shot_value,
)
from sportsdataverse.mbb.mbb_ncaa_stints import (
    GameBreakEvent,
    OtherOpponentEvent,
    OtherTeamEvent,
    build_player_code,
)

# A fixed datetime (Scala uses `new DateTime()`; a constant keeps LineupEvent /
# ShotEvent equality deterministic across the replace()-based copies).
_NOW = datetime(2023, 1, 1, 17, 0, 0)

box_players = [build_player_code("Long, Jahari", None)]

box_lineup = LineupEvent(
    date=_NOW,
    location_type=LocationType.HOME,
    start_min=0.0,
    end_min=-100.0,
    duration_mins=0.0,
    score_info=ScoreInfo.empty(),
    team=TeamSeasonId(TeamId("Maryland"), Year(2023)),
    opponent=TeamSeasonId(TeamId("Penn St."), Year(2023)),
    lineup_id=LineupId.unknown,
    players=box_players,
    players_in=[],
    players_out=[],
    raw_game_events=[],
    team_stats=LineupEventStats.empty(),
    opponent_stats=LineupEventStats.empty(),
)

tidy_ctx = build_tidy_player_context(box_lineup)


def _base_shot_event():
    """A fresh copy of the oracle's ``base_shot_event`` (ShotEvent import kept
    local to avoid a top-level unused import when only replace() is used)."""
    from sportsdataverse.mbb.mbb_ncaa_models import ShotEvent, ShotGeo, ShotLocation

    return ShotEvent(
        player=box_players[0] if box_players else None,
        date=_NOW,
        location_type=LocationType.HOME,
        team=TeamSeasonId(TeamId("Maryland"), Year(2023)),
        opponent=TeamSeasonId(TeamId("Penn St."), Year(2023)),
        is_off=False,
        lineup_id=None,
        players=box_players,
        score=Score(0, 0),
        min=0.0,
        raw_event=None,
        loc=ShotLocation(x=0.0, y=0.0),
        geo=ShotGeo(lat=0.0, lon=0.0),
        dist=0.0,
        pts=1,
        value=1,
        ast_by=None,
        is_ast=None,
        is_trans=None,
    )


base_shot_event = _base_shot_event()

base_team_pbp = OtherTeamEvent(
    min=5.0,
    score=Score(0, 0),
    event_string="18:28:00,0-0,Kyle Guy, assist",
)

base_team_2p_pbp = replace(
    base_team_pbp,
    event_string="18:28:00,0-0,Eric Ayala, 2pt jumpshot 2ndchance made",
)

base_team_3p_pbp = replace(
    base_team_pbp,
    event_string="18:28:00,0-0,Eric Ayala, 3pt jumpshot 2ndchance fastbreak made",
)

base_oppo_pbp = OtherOpponentEvent(
    min=5.0,
    score=Score(0, 0),
    event_string="18:28:00,0-0,Kyle Guy, assist",
)

base_misc_pbp_ev = GameBreakEvent(min=5.0, score=Score(0, 0))


def test_enrich_shot_events_with_pbp():
    raw_shots = [
        replace(base_shot_event, min=5.0, is_off=True),  # (shot1, on lineup boundary)
        replace(base_shot_event, min=6.0, is_off=True),  # (shot2)
        replace(base_shot_event, min=7.0, dist=27.0, is_off=True),  # (shot3, 3P in transition)
        replace(base_shot_event, min=8.0, is_off=False),  # (no PbP match)
        replace(base_shot_event, min=11.0, is_off=True),  # (discarded: matches 2 PbPs)
        replace(base_shot_event, min=13.0, is_off=True),  # (shot4)
        replace(base_shot_event, min=14.5, is_off=True),  # (shot5)
        replace(base_shot_event, min=14.5, is_off=True),  # (shot6)
        replace(base_shot_event, min=16.0, is_off=True),  # (no lineup match)
    ]
    lineups = [
        replace(box_lineup, start_min=0.0, end_min=5.0, lineup_id=LineupId("test1")),
        replace(box_lineup, start_min=5.0, end_min=10.0, lineup_id=LineupId("test2")),
        replace(box_lineup, start_min=10.0, end_min=15.0, lineup_id=LineupId("test3")),
    ]
    pbp_events = [
        replace(base_team_2p_pbp, min=5.0),  # (shot1)
        # At 6 minutes: assisted shot (shot2)
        replace(base_team_2p_pbp, min=6.0),
        replace(base_team_pbp, min=6.0),
        # At 7 mins: multiple shots, only one right distance (shot3)
        replace(base_team_2p_pbp, min=7.0),
        replace(base_team_3p_pbp, min=7.0),
        replace(base_team_pbp, min=9.0),  # rogue assist ignored (time mismatch)
        # At 11 mins: multiple shots both right distance -> discard
        replace(base_team_2p_pbp, min=11.0),
        replace(base_team_2p_pbp, min=11.0),
        # At 13 mins: one right player, one wrong (shot4)
        replace(base_team_2p_pbp, min=13.0),
        replace(base_team_2p_pbp, min=13.0, event_string="18:28:00,0-0,Jahari Long, 3pt jumpshot 2ndchance made"),
        # 2 valid at same time (shot5, shot6)
        replace(base_team_2p_pbp, min=14.5, event_string="18:28:00,0-0,Jahari Long, 2pt jumpshot 2ndchance made"),
        replace(base_team_2p_pbp, min=14.5, event_string="18:28:00,0-0,Jahari Long, 2pt jumpshot 2ndchance made"),
        # Matches PbP but not lineup
        replace(base_team_2p_pbp, min=16.0),
    ]

    for using_bad_lineups in (False, True):
        enriched = enrich_shot_events_with_pbp(
            raw_shots,
            pbp_events,
            [] if using_bad_lineups else lineups,
            lineups if using_bad_lineups else [],
            box_lineup,
        )
        assert len(enriched) == 6
        shot1, shot2, shot3, shot4 = enriched[0], enriched[1], enriched[2], enriched[3]

        # Lineup correlation
        if using_bad_lineups:
            assert shot1.lineup_id is None
            assert shot2.lineup_id is None
            assert shot3.lineup_id is None
            assert shot4.lineup_id is None
        else:
            assert shot1.lineup_id == LineupId("test1")
            assert shot2.lineup_id == LineupId("test2")
            assert shot3.lineup_id == LineupId("test2")
            assert shot4.lineup_id == LineupId("test3")

        # Assist calcs
        assert shot1.is_ast is None
        assert shot2.is_ast is True
        assert shot2.ast_by == PlayerCodeId("KyGuy", PlayerId("Guy, Kyle"))
        assert shot3.is_ast is None
        assert shot4.is_ast is None
        for shot in enriched:
            if shot.is_ast is not True:
                assert shot.ast_by is None

        # Shot value
        assert shot1.value == 2
        assert shot2.value == 2
        assert shot3.value == 3
        assert shot4.value == 3

        # Transition calcs
        assert shot1.is_trans is None
        assert shot2.is_trans is None
        assert shot3.is_trans is True
        assert shot4.is_trans is None


def test_find_lineup_curr_pbp_none():
    lineup1 = replace(box_lineup, start_min=0.0, end_min=5.0)
    lineup2 = replace(box_lineup, start_min=5.0, end_min=10.0)
    lineup3 = replace(box_lineup, start_min=10.0, end_min=15.0)

    # Quick check for shot being before lineup:
    before_stashed_lineups = [lineup2, lineup3]
    before_lineup_res = find_lineup(
        replace(base_shot_event, min=2.5),
        None,
        before_stashed_lineups,
        PeekableIterator([]),
    )
    assert before_lineup_res == (None, before_stashed_lineups)

    lineup_post_gap = replace(box_lineup, start_min=30.0, end_min=35.0)

    # (shot, expected_lineup, expected_iterator_remaining, expected_stash)
    test_scenarios = [
        (replace(base_shot_event, min=0.0), lineup1, True, []),
        (replace(base_shot_event, min=1.0), lineup1, True, []),
        (replace(base_shot_event, min=5.0), lineup1, True, []),
        (replace(base_shot_event, min=5.1), lineup2, True, []),
        (replace(base_shot_event, min=10.0), lineup2, True, []),
        (replace(base_shot_event, min=10.5), lineup3, True, []),
        (replace(base_shot_event, min=15.0), lineup3, True, []),
        (replace(base_shot_event, min=15.0), lineup3, True, []),
        (replace(base_shot_event, min=25.0), None, False, [lineup_post_gap]),
        (replace(base_shot_event, min=40.0), None, False, []),
    ]

    for scenario_shot, expected_lineup, expected_iter_remaining, expected_stash in test_scenarios:
        expected_lineup_list = [expected_lineup] if expected_lineup is not None else []
        for is_off in (True, False):
            shot = replace(scenario_shot, is_off=is_off)

            direct_result = find_lineup(shot, None, list(expected_lineup_list), PeekableIterator([]))
            assert direct_result == (expected_lineup, expected_lineup_list)

            lineup_it = PeekableIterator([lineup1, lineup2, lineup3, lineup_post_gap])
            iterating_result = find_lineup(shot, None, [], lineup_it)
            assert iterating_result == (expected_lineup, expected_lineup_list + expected_stash)
            assert lineup_it.has_next() == expected_iter_remaining

            # Search vs the stash:
            stash = [lineup1, lineup2, lineup3, lineup_post_gap]
            stashing_lineup_it = PeekableIterator([lineup_post_gap])  # (should remain untouched)
            stashing_result = find_lineup(shot, None, stash, stashing_lineup_it)
            if expected_lineup is not None:
                remaining_stash = [lu for lu in stash if not lu.end_min <= expected_lineup.end_min]
            elif expected_stash:
                remaining_stash = expected_stash
            else:
                remaining_stash = []
            assert stashing_result == (expected_lineup, expected_lineup_list + remaining_stash)
            assert stashing_lineup_it.has_next() == (shot.min < 40.0)


def test_find_lineup_curr_pbp_some():
    # Scenario 1: event is first encountered
    scenario_1_lineup_1 = replace(
        box_lineup,
        start_min=0.0,
        end_min=5.0,
        raw_game_events=[RawGameEvent(5.0, base_team_pbp.event_string)],
    )
    scenario_1_it = PeekableIterator([scenario_1_lineup_1, box_lineup])
    scenario_1_result = find_lineup(
        replace(base_shot_event, min=5.0, is_off=True),
        replace(base_team_pbp, min=5.0),
        [],
        scenario_1_it,
    )
    assert scenario_1_result == (scenario_1_lineup_1, [scenario_1_lineup_1])
    assert next(scenario_1_it) == box_lineup

    # Scenario 2: event is a subsequent event
    scenario_2_lineup_1 = replace(
        box_lineup,
        start_min=0.0,
        end_min=5.0,
        raw_game_events=[RawGameEvent(5.0, base_team_pbp.event_string)],  # (wrong direction vs shot.is_off)
    )
    scenario_2_lineup_2 = replace(
        box_lineup,
        start_min=5.0,
        end_min=10.0,
        raw_game_events=[RawGameEvent(5.0, None, base_team_pbp.event_string)],
    )
    scenario_2_it = PeekableIterator([scenario_2_lineup_1, scenario_2_lineup_2, box_lineup])
    scenario_2_result = find_lineup(
        replace(base_shot_event, min=5.0, is_off=False),
        replace(base_team_pbp, min=5.0),
        [],
        scenario_2_it,
    )
    assert scenario_2_result == (scenario_2_lineup_2, [scenario_2_lineup_1, scenario_2_lineup_2])
    assert next(scenario_2_it) == box_lineup

    # Scenario 3: event is not in any, go back to fallback
    scenario_3_lineup_1 = replace(box_lineup, start_min=0.0, end_min=5.0)
    scenario_3_lineup_2 = replace(box_lineup, start_min=5.0, end_min=10.0)
    scenario_3_lineup_3 = replace(box_lineup, start_min=10.0, end_min=10.0)
    scenario_3_it = PeekableIterator([scenario_3_lineup_1, scenario_3_lineup_2, scenario_3_lineup_3])
    scenario_3_result = find_lineup(
        replace(base_shot_event, min=5.0, is_off=False),
        replace(base_team_pbp, min=5.0),
        [],
        scenario_3_it,
    )
    assert scenario_3_result == (
        scenario_3_lineup_1,
        [scenario_3_lineup_1, scenario_3_lineup_2, scenario_3_lineup_3],
    )


def test_shot_value():
    assert shot_value("18:28:00,0-0,Kyle Guy, assist") == 0
    assert shot_value("18:28:00,0-0,Eric Ayala, 3pt jumpshot made") == 3
    assert shot_value("18:28:00,0-0,Eric Ayala, 3pt jumpshot 2ndchance missed") == 3
    assert shot_value("18:28:00,0-0,Jalen Smith, 2pt drivinglayup 2ndchance;pointsinthepaint made") == 2
    assert shot_value("18:28:00,0-0,Eric Carter, 2pt layup missed") == 2
    assert shot_value("18:28:00,0-0,Eric Ayala, 2pt jumpshot 2ndchance missed") == 2
    assert shot_value("04:28:0,52-59,Team, rebound deadballdeadball") == -1


def test_find_pbp_clump():
    # (shot, pbp_curr, pbp_remaining, next, expected)
    test_scenarios = [
        (replace(base_shot_event, min=5.0), [], [], None, ([], None)),
        (
            replace(base_shot_event, min=5.0),
            [],
            [replace(base_team_pbp, min=10.0)],
            None,
            ([], replace(base_team_pbp, min=10.0)),
        ),
        (
            replace(base_shot_event, min=5.0),
            [],
            [
                replace(base_team_pbp, min=2.5),
                replace(base_team_pbp, min=5.0),
                replace(base_team_pbp, min=5.0),
                replace(base_team_pbp, min=10.0),
            ],
            None,
            (
                [replace(base_team_pbp, min=5.0), replace(base_team_pbp, min=5.0)],
                replace(base_team_pbp, min=10.0),
            ),
        ),
        (
            replace(base_shot_event, min=5.0),
            [],
            [replace(base_team_pbp, min=5.0), replace(base_team_pbp, min=5.0)],
            replace(base_team_pbp, min=2.5),
            ([replace(base_team_pbp, min=5.0), replace(base_team_pbp, min=5.0)], None),
        ),
        (
            replace(base_shot_event, min=5.0),
            [replace(base_team_pbp, min=5.0), replace(base_team_pbp, min=5.0)],
            [replace(base_team_pbp, min=10.0)],
            None,
            ([replace(base_team_pbp, min=5.0), replace(base_team_pbp, min=5.0)], None),
        ),
        (
            replace(base_shot_event, min=10.0),
            [],
            [replace(base_team_pbp, min=10.0), replace(base_team_pbp, min=13.0)],
            None,
            ([replace(base_team_pbp, min=10.0)], replace(base_team_pbp, min=13.0)),
        ),
        (
            replace(base_shot_event, min=5.0),
            [replace(base_team_pbp, min=5.0), replace(base_team_pbp, min=5.0)],
            [],
            None,
            ([replace(base_team_pbp, min=5.0), replace(base_team_pbp, min=5.0)], None),
        ),
        # Test removing old events from curr:
        (
            replace(base_shot_event, min=8.0),
            [replace(base_team_pbp, min=7.0)],
            [replace(base_team_pbp, min=13.0)],
            replace(base_team_pbp, min=9.0),
            ([], replace(base_team_pbp, min=9.0)),
        ),
    ]
    for test_num, (shot, pbp_curr, pbp_remaining, nxt, expected) in enumerate(test_scenarios):
        result = find_pbp_clump(shot.min, PeekableIterator(pbp_remaining), pbp_curr, nxt)
        assert result == expected, f"[{test_num}]: [{result}] != [{expected}]"


def test_extract_player_from_ev():
    # (ev_str, is_off, expected)
    test_scenarios = [
        (
            "18:28:00,0-0,Jahar Long, 3pt jumpshot made",
            True,  # (means error above will be corrected)
            build_player_code("Long, Jahari", None),
        ),
        (
            "18:28:00,0-0,Jahar Long, 3pt jumpshot made",
            False,  # (means error above will not be corrected)
            build_player_code("Long, Jahar", None),
        ),
    ]
    for ev_str, is_off, expected in test_scenarios:
        result = extract_player_from_ev(
            replace(base_shot_event, is_off=is_off),
            replace(base_team_pbp, event_string=ev_str),
            tidy_ctx,
        )
        assert result == expected


def test_matching_player():
    # (ev_str, is_off, code_match, expected)
    test_scenarios = [
        ("18:28:00,0-0,Jahar Long, 3pt jumpshot made", True, False, True),
        ("18:28:00,0-0,Jahar Long, 3pt jumpshot made", False, False, False),
        ("18:28:00,0-0,Jahar Long, 3pt jumpshot made", False, True, True),
    ]
    for ev_str, is_off, code_match, expected in test_scenarios:
        assert (
            matching_player(
                replace(base_shot_event, is_off=is_off, player=box_players[0] if box_players else None),
                replace(base_team_pbp, event_string=ev_str),
                tidy_ctx,
                code_match=code_match,
            )
            == expected
        )


def test_right_kind_of_shot():
    # (ev_str, shot, (strict, lax))
    test_scenarios = [
        ("18:28:00,0-0,Jahari Long, 3pt jumpshot made", replace(base_shot_event, pts=1, dist=27.0), (True, True)),
        ("18:28:00,0-0,Jahari Long, 3pt jumpshot made", replace(base_shot_event, pts=0, dist=12.0), (False, False)),
        ("18:28:00,0-0,Jahari Long, 3pt jumpshot missed", replace(base_shot_event, pts=1, dist=27.0), (False, False)),
        ("18:28:00,0-0,Jahari Long, 3pt jumpshot missed", replace(base_shot_event, pts=0, dist=27.0), (True, True)),
        ("18:28:00,0-0,Jahari Long, 2pt jumpshot made", replace(base_shot_event, pts=1, dist=12.0), (True, True)),
        # For these 2 the PbP will fail, which means it will treat both as missed
        ("Jahari Long, 2pt jumpshot missed", replace(base_shot_event, pts=0, dist=27.0), (False, True)),
        ("Jahari Long, 3pt jumpshot made", replace(base_shot_event, pts=1, dist=12.0), (False, False)),
    ]
    for ev_str, shot, (exp_strict, exp_lax) in test_scenarios:
        assert (
            right_kind_of_shot(
                replace(shot, raw_event=ev_str),
                replace(base_team_pbp, event_string=ev_str),
                strict=True,
            )
            == exp_strict
        )
        assert (
            right_kind_of_shot(
                replace(shot, raw_event=ev_str),
                replace(base_team_pbp, event_string=ev_str),
                strict=False,
            )
            == exp_lax
        )


def test_inject_starting_lineup_into_box_smoke():
    """Smoke test (no upstream oracle): a player mentioned in a team-side play
    before any sub is a starter; a player subbed in first is excluded. The
    inferred starter is promoted to the front of the roster order."""
    smith = build_player_code("Smith, Jalen", None)  # code "JaSmith"
    long = build_player_code("Long, Jahari", None)  # code "JaLong"
    # Box roster order intentionally Smith-first so the reorder is observable.
    box = replace(box_lineup, players=[smith, long])

    from sportsdataverse.mbb.mbb_ncaa_stints import SubInEvent

    pbp = [
        # Long is mentioned in a team play (min > last_sub_time 0) -> starter
        OtherTeamEvent(min=1.0, score=Score(0, 0), event_string="18:28:00,0-0,Jahari Long, 2pt jumpshot made"),
        # Smith subbed in (never seen before) -> excluded
        SubInEvent(min=2.0, score=Score(0, 0), player_name="Jalen Smith"),
    ]

    result = inject_starting_lineup_into_box(pbp, box, ([], []), 1)
    # Long (the inferred starter) leads; Smith (excluded) trails.
    assert [p.id.name for p in result.players] == ["Long, Jahari", "Smith, Jalen"]
