"""Oracle tests for :mod:`sportsdataverse.mbb.mbb_ncaa_shot_parser` (Task 5e.5).

1:1 transliteration of ``ShotEventParserTests.scala`` (``utest``,
``"ShotEventParser"`` block) -- a PURE INLINE oracle, no HTML fixture. Every
inline SVG/JS snippet and expected value below is transliterated verbatim
from the Scala source; ``_double_formatter``/``_event_formatter`` mirror the
oracle's own float-rounding test helpers (Scala ``0.01 * (in * 100.0).toInt``
-- truncating toward zero, same as Python's ``int()``).
"""

from __future__ import annotations

from dataclasses import replace
from datetime import datetime

from sportsdataverse.mbb.mbb_ncaa_html import parse_html
from sportsdataverse.mbb.mbb_ncaa_models import (
    LineupEvent,
    LineupEventStats,
    LineupId,
    LocationType,
    PlayerCodeId,
    PlayerId,
    Score,
    ScoreInfo,
    ShotEvent,
    ShotGeo,
    ShotLocation,
    TeamId,
    TeamSeasonId,
    Year,
)
from sportsdataverse.mbb.mbb_ncaa_names import build_tidy_player_context
from sportsdataverse.mbb.mbb_ncaa_shot_parser import (
    ShotMapDimensions,
    build_base_event,
    create_shot_event_data,
    get_ascending_time,
    is_team_shooting_left_to_start,
    is_women_game,
    parse_shot_html,
    phase1_shot_event_enrichment,
    shot_js_to_html,
    transform_shot_location,
    v1_builders,
)
from sportsdataverse.mbb.mbb_ncaa_stints import build_player_code

# ---------------------------------------------------------------------------
# Shared fixtures (``ShotEventParserTests.scala:48-82``)
# ---------------------------------------------------------------------------

_BOX_PLAYERS = [build_player_code("Long, Jahari", None)]

_BOX_LINEUP = LineupEvent(
    date=datetime.now(),
    location_type=LocationType.HOME,
    start_min=0.0,
    end_min=-100.0,
    duration_mins=0.0,
    score_info=ScoreInfo.empty(),
    team=TeamSeasonId(TeamId("Maryland"), Year(2023)),
    opponent=TeamSeasonId(TeamId("Penn St."), Year(2023)),
    lineup_id=LineupId.unknown,
    players=_BOX_PLAYERS,
    players_in=[],
    players_out=[],
    raw_game_events=[],
    team_stats=LineupEventStats.empty(),
    opponent_stats=LineupEventStats.empty(),
)

_TIDY_CTX = build_tidy_player_context(_BOX_LINEUP)

_BASE_EVENT_HTML = (
    '<circle cx="310.2" cy="235" r="5" style="fill: white; stroke: blue; stroke-width: 3px; '
    'display: inline;" id="play_2565239320" class="period_1 player_768305773 team_392 shot missed">'
    "<title>1st 13:05:00 : missed by Jahari Long(Maryland) 9-6</title></circle>"
)


def _first_circle(html: str):  # type: ignore[no-untyped-def]
    """Parse an inline HTML snippet and return its first ``circle.shot``."""
    return v1_builders.shot_event_finder(parse_html(html))


_base_event_result = parse_shot_html(
    _first_circle(_BASE_EVENT_HTML)[0], _BOX_LINEUP, v1_builders, _TIDY_CTX, target_team_first=True
)
assert isinstance(_base_event_result, tuple)
BASE_EVENT: ShotEvent = _base_event_result[1]


def _double_formatter(value: float) -> float:
    """``0.01 * (in * 100.0).toInt`` -- truncating float rounding to 2dp
    (``ShotEventParserTests.double_formatter``)."""
    return 0.01 * int(value * 100.0)


def _event_formatter(ev: ShotEvent) -> ShotEvent:
    """Rounds ``min``/``loc``/``dist`` to 2dp and drops ``raw_event`` (debug
    only) before comparison (``ShotEventParserTests.event_formatter``)."""
    return replace(
        ev,
        min=_double_formatter(ev.min),
        raw_event=None,
        loc=ShotLocation(x=_double_formatter(ev.loc.x), y=_double_formatter(ev.loc.y)),
        dist=_double_formatter(ev.dist),
    )


# ---------------------------------------------------------------------------
# "shot_js_to_html"
# ---------------------------------------------------------------------------


def test_shot_js_to_html() -> None:
    script_html = """
      <script>
        def addShot(x, y, z, blah, title, etc) {}
      </script>
      <script>
        addShot(27.0, 77.0, 392, false, 2587511021, '1st 19:26:00 : missed by Donta Scott(Maryland) 0-0', 'period_1 player_769731016 team_392', false); //2587511021, 11
        addShot(92.0, 55.0, 796, false, 2587511023, '1st 18:55:00 : missed by Tyler Wahl(Wisconsin) 0-0', 'period_1 player_769731004 team_796', false); //2587511023, 13
        addShot(90.0, 64.0, 796, false, 2587511028, '1st 18:22:00 : missed by Steven Crowl(Wisconsin) 0-0', 'period_1 player_769731006 team_796', false); //2587511028, 20
        addShot(94.0, 53.0, 450, false, 2552825138, '1st 13:10:00 : missed by De&#39;Shayne Montgomery(Mount St. Mary&#39;s) 4-7', 'period_1 player_767427911 team_450', false); //2552825138, 116
      </script>
    """
    script_doc = parse_html(script_html)
    scripts = script_doc.select("script")
    joined = "\n".join(str(s) for s in scripts if "addShot(" in str(s)[:128])
    assert joined

    circles = shot_js_to_html(joined)
    results = [(v1_builders.shot_location_finder(el), v1_builders.event_player_finder(el)) for el in circles]

    assert results == [
        ((253.8, 385.0), "Scott, Donta"),
        ((864.8000000000001, 275.0), "Wahl, Tyler"),
        ((846.0, 320.0), "Crowl, Steven"),
        ((883.6, 265.0), "Montgomery, De'Shayne"),
    ]


# ---------------------------------------------------------------------------
# "parse_shot_html"
# ---------------------------------------------------------------------------

_BASE_EVENT_1 = replace(
    build_base_event(_BOX_LINEUP),
    loc=ShotLocation(x=310.2, y=235),
    min=13.08,
    player=_BOX_PLAYERS[0],
    score=Score(9, 6),
)

_BASE_EVENT_2 = replace(
    build_base_event(_BOX_LINEUP),
    loc=ShotLocation(x=629.8000000000001, y=185),
    min=4.46,
    player=PlayerCodeId("KaClary", PlayerId("Clary, Kanye")),
    score=Score(25, 20),
    is_off=False,
    pts=1,
)


def test_parse_shot_html_valid_scenarios() -> None:
    valid_scenarios = [
        # Base scenario:
        (_BASE_EVENT_HTML, _BASE_EVENT_1, 1, _BOX_LINEUP, True),
        # Weird name scenario:
        (
            '<circle cx="310.2" cy="235" r="5" id="play_2565239320" class="period_1 player_768305773 '
            'team_392 shot missed"><title>1st 13:05:00 : missed by Russell (Deuce) Dean(Maryland) '
            "9-6</title></circle>",
            replace(
                _BASE_EVENT_1,
                is_off=True,
                player=PlayerCodeId("Ru(deuce)Dean", PlayerId("Dean, Russell (Deuce)")),
            ),
            1,
            _BOX_LINEUP,
            True,
        ),
        # Location swap (Away):
        (
            _BASE_EVENT_HTML,
            replace(_BASE_EVENT_1, score=Score(6, 9), location_type=LocationType.AWAY),
            1,
            replace(_BOX_LINEUP, location_type=LocationType.AWAY),
            True,
        ),
        # Neutral, target first:
        (
            _BASE_EVENT_HTML,
            replace(_BASE_EVENT_1, score=Score(9, 6), location_type=LocationType.NEUTRAL),
            1,
            replace(_BOX_LINEUP, location_type=LocationType.NEUTRAL),
            True,
        ),
        # Neutral, target NOT first:
        (
            _BASE_EVENT_HTML,
            replace(_BASE_EVENT_1, score=Score(6, 9), location_type=LocationType.NEUTRAL),
            1,
            replace(_BOX_LINEUP, location_type=LocationType.NEUTRAL),
            False,
        ),
        # Weird team name (nested parens), opponent shot:
        (
            '<circle cx="310.2" cy="235" r="5" id="play_2565239320" class="period_1 player_768305773 '
            'team_392 shot missed"><title>1st 13:05:00 : missed by Jahari Long(St. Francis (PA)) '
            "9-6</title></circle>",
            replace(
                _BASE_EVENT_1,
                is_off=False,
                opponent=TeamSeasonId(TeamId("St. Francis (PA)"), Year(2023)),
            ),
            1,
            replace(_BOX_LINEUP, opponent=TeamSeasonId(TeamId("St. Francis (PA)"), Year(2023))),
            False,
        ),
        # Base opponent scenario:
        (
            '<circle cx="629.8000000000001" cy="185" r="5" id="play_2565239462" class="period_1 '
            'player_768305790 team_539 shot made"><title>1st 04:28:00 : made by Kanye Clary(Penn St.) '
            "25-20</title></circle>",
            _BASE_EVENT_2,
            1,
            _BOX_LINEUP,
            True,
        ),
        # Same, period 2 (class period_1, title "2nd"):
        (
            '<circle cx="629.8000000000001" cy="185" r="5" id="play_2565239462" class="period_1 '
            'player_768305790 team_539 shot made"><title>2nd 04:28:00 : made by Kanye Clary(Penn St.) '
            "25-20</title></circle>",
            _BASE_EVENT_2,
            2,
            _BOX_LINEUP,
            True,
        ),
        # period 3 (class period_2, title "3rd"):
        (
            '<circle cx="629.8000000000001" cy="185" r="5" id="play_2565239462" class="period_2 '
            'player_768305790 team_539 shot made"><title>3rd 04:28:00 : made by Kanye Clary(Penn St.) '
            "25-20</title></circle>",
            _BASE_EVENT_2,
            3,
            _BOX_LINEUP,
            True,
        ),
        # period 4 (class period_3, title "4th"):
        (
            '<circle cx="629.8000000000001" cy="185" r="5" id="play_2565239462" class="period_3 '
            'player_768305790 team_539 shot made"><title>4th 04:28:00 : made by Kanye Clary(Penn St.) '
            "25-20</title></circle>",
            _BASE_EVENT_2,
            4,
            _BOX_LINEUP,
            True,
        ),
        # period 5 (class period_4, title "5th"):
        (
            '<circle cx="629.8000000000001" cy="185" r="5" id="play_2565239462" class="period_4 '
            'player_768305790 team_539 shot made"><title>5th 04:28:00 : made by Kanye Clary(Penn St.) '
            "25-20</title></circle>",
            _BASE_EVENT_2,
            5,
            _BOX_LINEUP,
            True,
        ),
    ]

    for html, expected, expected_period, box, target in valid_scenarios:
        circles = _first_circle(html)
        assert len(circles) == 1
        result = parse_shot_html(circles[0], box, v1_builders, _TIDY_CTX, target)
        assert isinstance(result, tuple), result
        period, shot_event = result
        assert period == expected_period
        assert _event_formatter(shot_event) == _event_formatter(expected)


def test_parse_shot_html_invalid_scenarios() -> None:
    # As above, but missing each of the key fields: cx/cy/title/ the score in
    # title / the time in title / the team in title.
    invalid_scenarios = [
        (
            '<circle cx="629.8000000000001" cy="185" r="5" id="play_2565239462" class="player_768305790 '
            'team_539 shot made"><title>04:28:00 : made by Kanye Clary(Penn St.) 25-20</title></circle>',
            "[0]",  # (period)
        ),
        (
            '<circle cx="310.2" cy="235" r="5" id="play_2565239320" class="period_1 player_768305773 '
            'team_392 shot missed"><title>1st: missed by Jahari Long(Maryland) 9-6</title></circle>',
            "[0,1]",  # (time; period regex also fails without a proper HH:MM:SS)
        ),
        (
            '<circle cx="629.8000000000001" cy="185" r="5" id="play_2565239462" class="period_4 '
            'player_768305790 team_539 shot made"><title>4th 04:28:00 : made by (Penn St.) '
            "25-20</title></circle>",
            "[2]",  # (player)
        ),
        (
            '<circle cy="235" r="5" id="play_2565239320" class="period_1 player_768305773 team_392 '
            'shot missed"><title>1st 13:05:00 : missed by Jahari Long(Maryland) 9-6</title></circle>',
            "[3]",  # (location, missing cx)
        ),
        (
            '<circle cx="310.2" r="5" id="play_2565239320" class="period_1 player_768305773 team_392 '
            'shot missed"><title>1st 13:05:00 : missed by Jahari Long(Maryland) 9-6</title></circle>',
            "[3]",  # (location, missing cy)
        ),
        (
            '<circle cx="310.2" cy="235" r="5" id="play_2565239320" class="period_1 player_768305773 '
            'team_392 shot missed"><title>1st 13:05:00 : missed by Jahari Long(Maryland)</title></circle>',
            "[2,4,6]",  # (score; use the score to find the team and player so if one is missing so is the other)
        ),
        (
            '<circle cx="310.2" cy="235" r="5" id="play_2565239320" class="period_1 player_768305773 '
            'team_392 shot"><title>1st 13:05:00 : taken by Jahari Long(Maryland) 9-6</title></circle>',
            "[2,5,6]",  # (shot result)
        ),
        (
            '<circle cx="310.2" cy="235" r="5" id="play_2565239320" class="period_1 player_768305773 '
            'team_392 shot missed"><title>1st 13:05:00 : missed by Jahari Long 9-6</title></circle>',
            "[2,6]",  # (team; use the team to find the player so if one is missing so is the other)
        ),
        (
            '<circle cx="310.2" cy="235" r="5" id="play_2565239320" class="period_1 player_768305773 '
            'team_392 shot missed"></circle>',
            "[0,1,2,4,5,6]",  # (if title is missing, that's all fields exception location)
        ),
    ]

    for html, error_fragment in invalid_scenarios:
        circles = _first_circle(html)
        assert len(circles) == 1
        result = parse_shot_html(circles[0], _BOX_LINEUP, v1_builders, _TIDY_CTX, target_team_first=True)
        assert isinstance(result, list)
        assert error_fragment in result[0].messages[0], result[0].messages


# ---------------------------------------------------------------------------
# "is_women_game"
# ---------------------------------------------------------------------------


def test_is_women_game() -> None:
    scenarios = [
        ([(1, replace(BASE_EVENT, min=9.0))], False),  # not enough periods
        (
            [
                (1, replace(BASE_EVENT, min=11.0)),  # shot taken before quarter
                (3, replace(BASE_EVENT, min=9.0)),
                (4, replace(BASE_EVENT, min=9.0)),
            ],
            False,  # shot taken before quarter
        ),
        (
            [
                (1, replace(BASE_EVENT, min=9.0)),
                (2, replace(BASE_EVENT, min=9.0)),
                (3, replace(BASE_EVENT, min=9.0)),
                (3, replace(BASE_EVENT, min=9.0)),
            ],
            False,  # not enough periods
        ),
        (
            [
                (1, replace(BASE_EVENT, min=9.0)),
                (2, replace(BASE_EVENT, min=9.0)),
                (3, replace(BASE_EVENT, min=9.0)),
                (4, replace(BASE_EVENT, min=9.0)),
            ],
            True,
        ),
    ]
    for events, expected in scenarios:
        assert is_women_game(events) == expected


# ---------------------------------------------------------------------------
# "get_ascending_time"
# ---------------------------------------------------------------------------


def test_get_ascending_time_women() -> None:
    cases = [
        (get_ascending_time(replace(BASE_EVENT, min=4.0), 1, True), 6.0),
        (get_ascending_time(replace(BASE_EVENT, min=6.0), 2, True), 14.0),
        (get_ascending_time(replace(BASE_EVENT, min=1.0), 3, True), 29.0),
        (get_ascending_time(replace(BASE_EVENT, min=8.0), 4, True), 32.0),
        (get_ascending_time(replace(BASE_EVENT, min=2.0), 5, True), 43.0),
        (get_ascending_time(replace(BASE_EVENT, min=0.0), 6, True), 50.0),
    ]
    for result, expected in cases:
        assert result == expected


def test_get_ascending_time_men() -> None:
    cases = [
        (get_ascending_time(replace(BASE_EVENT, min=4.0), 1, False), 16.0),
        (get_ascending_time(replace(BASE_EVENT, min=6.0), 2, False), 34.0),
        (get_ascending_time(replace(BASE_EVENT, min=1.0), 3, False), 44.0),
        (get_ascending_time(replace(BASE_EVENT, min=4.0), 4, False), 46.0),
    ]
    for result, expected in cases:
        assert result == expected


# ---------------------------------------------------------------------------
# "is_team_shooting_left_to_start"
# ---------------------------------------------------------------------------


def test_is_team_shooting_left_to_start() -> None:
    switched_x_base_event = replace(BASE_EVENT, loc=replace(BASE_EVENT.loc, x=1000))
    test_case_1 = [
        (1, BASE_EVENT),
        (1, BASE_EVENT),
        (1, BASE_EVENT),
        (1, switched_x_base_event),
        (1, replace(switched_x_base_event, is_off=False)),
        (1, replace(switched_x_base_event, is_off=False)),
        (1, replace(switched_x_base_event, is_off=False)),
        (1, replace(switched_x_base_event, is_off=False)),
        (1, replace(switched_x_base_event, is_off=False)),
        (1, replace(switched_x_base_event, is_off=False)),
        (2, switched_x_base_event),
        (2, switched_x_base_event),
        (2, switched_x_base_event),
        (2, switched_x_base_event),
        (2, switched_x_base_event),
        (2, switched_x_base_event),
    ]
    assert is_team_shooting_left_to_start(test_case_1) == (True, 1)

    test_case_2 = [
        (period, replace(ev, loc=replace(BASE_EVENT.loc, x=300)))
        if ev.loc.x > 800
        else (period, replace(ev, loc=replace(BASE_EVENT.loc, x=1000)))
        for period, ev in test_case_1
    ]
    assert is_team_shooting_left_to_start(test_case_2) == (False, 1)


# ---------------------------------------------------------------------------
# "phase1_shot_event_enrichment/transform_shot_location"
# ---------------------------------------------------------------------------


def test_phase1_shot_event_enrichment_and_transform_shot_location() -> None:
    test_case = [
        (1, BASE_EVENT),
        (
            1,
            replace(
                BASE_EVENT,
                loc=ShotLocation(
                    x=ShotMapDimensions.court_length_x_px - BASE_EVENT.loc.x,
                    y=ShotMapDimensions.court_width_y_px - BASE_EVENT.loc.y,
                ),
                is_off=False,  # (will convert back to the same location)
            ),
        ),
        (
            2,
            replace(
                BASE_EVENT,
                loc=ShotLocation(
                    x=ShotMapDimensions.court_length_x_px - BASE_EVENT.loc.x,
                    y=ShotMapDimensions.court_width_y_px - BASE_EVENT.loc.y,
                ),
            ),  # (2nd period so will be the same location)
        ),
        (
            2,
            replace(
                BASE_EVENT,
                loc=ShotLocation(x=BASE_EVENT.loc.x, y=BASE_EVENT.loc.y),
                is_off=False,  # (2nd period so will be the same location)
            ),
        ),
        # Some edge cases:
        # error checker will flip this back again because its dist will be silly:
        (
            2,
            replace(
                BASE_EVENT,
                loc=ShotLocation(
                    x=ShotMapDimensions.court_length_x_px - BASE_EVENT.loc.x,
                    y=ShotMapDimensions.court_width_y_px - BASE_EVENT.loc.y,
                ),
                is_off=False,
            ),
        ),
        # error checker will ignore this one because it could be a half court heave:
        (
            2,
            replace(
                BASE_EVENT,
                loc=ShotLocation(
                    x=ShotMapDimensions.court_length_x_px - BASE_EVENT.loc.x,
                    y=ShotMapDimensions.court_width_y_px - BASE_EVENT.loc.y,
                ),
                is_off=False,
                min=0.05,
            ),
        ),
    ]

    # (reminder pixel x and y are: cx="310.2" cy="235")
    transformed_base_event = _event_formatter(
        replace(
            BASE_EVENT,
            loc=ShotLocation(x=26.02, y=1.5),
            geo=ShotGeo(lat=40.75031148982409, lon=-73.99301510956438),
            dist=26.060000000000002,
        )
    )

    results = [_event_formatter(ev) for ev in phase1_shot_event_enrichment(test_case)]
    assert len(results) == 6
    t1, t2, t3, t4, t5, t6 = results
    assert t1 == replace(transformed_base_event, min=6.91)
    assert t2 == replace(transformed_base_event, min=6.91, is_off=False)
    assert t3 == replace(transformed_base_event, min=26.91)
    assert t4 == replace(transformed_base_event, min=26.91, is_off=False)
    assert t5 == replace(transformed_base_event, min=26.91, is_off=False)
    assert t6.dist > 55.0  # (half court heave)

    result_x, result_y, _, _ = transform_shot_location(
        x=ShotMapDimensions.court_length_x_px - BASE_EVENT.loc.x,
        y=ShotMapDimensions.court_width_y_px - BASE_EVENT.loc.y,
        second_half_switch=False,  # (1st period)
        team_shooting_left_in_first_period=False,  # (just want to check this case)
        is_offensive=True,
    )
    assert _double_formatter(result_x) == transformed_base_event.loc.x
    assert _double_formatter(result_y) == transformed_base_event.loc.y


# ---------------------------------------------------------------------------
# "create_shot_event_data" -- NOT part of the Scala oracle (``ShotEventParserTests
# .scala`` only exercises the sub-functions above), so these are project-added
# smoke tests covering the entry-point's own branching (baked-in circles vs.
# the JS-fallback path) per this project's "non-trivial logic gets one
# runnable check" convention.
# ---------------------------------------------------------------------------

_TEAM_IMAGES_HTML = '<table align="center"><tr><td><img alt="Maryland"></td><td><img alt="Penn St."></td></tr></table>'


def test_create_shot_event_data_baked_in_circle_smoke() -> None:
    html = _TEAM_IMAGES_HTML + _BASE_EVENT_HTML
    result = create_shot_event_data("test.html", html, _BOX_LINEUP)
    assert isinstance(result, list)
    assert len(result) == 1
    assert isinstance(result[0], ShotEvent)
    assert result[0].is_off is True
    assert result[0].dist > 0.0


def test_create_shot_event_data_js_fallback_smoke() -> None:
    # Each addShot(...) call must be on its own line -- shot_js_to_html
    # splits the script text on "\n" and fullmatches line by line (see
    # test_shot_js_to_html, which reproduces the same real-fixture shape).
    script_html = (
        _TEAM_IMAGES_HTML + "<script>\n"
        "addShot(27.0, 77.0, 392, false, 1, "
        "'1st 19:26:00 : missed by Jahari Long(Maryland) 0-0', 'period_1 player_1 team_392', false);\n"
        "</script>"
    )
    result = create_shot_event_data("test.html", script_html, _BOX_LINEUP)
    assert isinstance(result, list)
    assert len(result) == 1
    assert isinstance(result[0], ShotEvent)
    assert result[0].player is not None
    assert result[0].player.id.name == "Long, Jahari"


def test_create_shot_event_data_no_shots_is_parse_error() -> None:
    result = create_shot_event_data("test.html", _TEAM_IMAGES_HTML, _BOX_LINEUP)
    assert isinstance(result, list)
    assert result and not isinstance(result[0], ShotEvent)
