import polars as pl

from sportsdataverse.cfb import cfb_drive_summary as drive_summary

HOME, AWAY = "10", "20"


def _drive(
    tid,
    drive_id,
    result,
    yards,
    plays,
    top,
    start_yl,
    period,
    is_score=False,
    last_score=(0, 0),
    start_text=None,
):
    return {
        "id": drive_id,
        "team": {"id": tid, "abbreviation": "HH" if tid == "10" else "AA"},
        "result": result,
        "isScore": is_score,
        "yards": yards,
        "offensivePlays": plays,
        "timeElapsed": {"displayValue": top},
        "start": {
            "yardLine": start_yl,
            "period": {"number": period},
            "clock": {"displayValue": "12:00"},
            "text": start_text or "",
        },
        "end": {"text": "T 50", "clock": {"displayValue": "09:00"}},
        "description": f"{plays} plays, {yards} yards",
        "plays": [
            {
                "homeScore": last_score[0],
                "awayScore": last_score[1],
                "text": "the finishing play",
                "scoringPlay": is_score,
            },
            # trailing post-score penalty entry must NOT win
            {
                "homeScore": last_score[0],
                "awayScore": last_score[1],
                "text": "PENALTY after the play",
            },
        ],
    }


def _frame():
    # scrimmage plays across two teams; enough flags for the aggregates
    return pl.DataFrame(
        {
            "game_play_number": [1, 2, 3, 4, 5, 6],
            "scrimmage_play": [True] * 6,
            "pos_team": [10, 10, 10, 20, 20, 20],
            "down": [1, 3, 3, 1, 3, 4],
            "distance": [10, 4, 9, 10, 2, 1],
            "first_down_created": [True, True, True, False, False, True],
            "firstD_by_penalty": [False, False, False, True, False, False],
            "touchdown": [False, False, False, False, False, False],
            "rush": [True, False, False, False, True, True],
            "pass": [False, True, True, True, False, False],
            "period": [1, 1, 2, 2, 3, 4],
            "statYardage": [12, 6, 0, 45, 3, 1],
            "text": ["a", "b", "c", "big gain", "e", "f"],
            "start.adj_TimeSecsRem": [3600.0, 3300.0, 1800.0, 1500.0, 900.0, 300.0],
            "start.homeScore": [0, 0, 7, 7, 7, 7],
            "start.awayScore": [0, 0, 0, 0, 10, 10],
            "homeTeamId": [10] * 6,
            "awayTeamId": [20] * 6,
            "drive.id": ["d1", "d1", "d2", "d3", "d3", "d3"],
        }
    )


def _drives():
    return [
        # home: 75-yd TD drive (available-yards success + fd success via score)
        _drive(HOME, "d1", "TD", 75, 8, "3:10", 25, 1, is_score=True, last_score=(7, 0)),
        # away: three-and-out (forced by home)
        _drive(AWAY, "dX", "PUNT", 4, 3, "1:20", 20, 1, last_score=(7, 0)),
        # home: interception thrown
        _drive(HOME, "d2", "INT", 10, 4, "2:00", 30, 2, last_score=(7, 0)),
        # away: ensuing possession scores -> points off turnovers
        _drive(AWAY, "d3", "TD", 60, 7, "5:10", 40, 2, is_score=True, last_score=(7, 7)),
        # away: field goal, 12-play drive
        _drive(AWAY, "d4", "FG", 40, 12, "6:01", 35, 3, is_score=True, last_score=(7, 10)),
        # home: downs, no first down (fails both success metrics)
        _drive(HOME, "d5", "DOWNS", 9, 4, "0:50", 45, 4, last_score=(7, 10)),
    ]


def test_drive_summary_counts_and_rates():
    out = drive_summary.create_drive_summary(_drives(), _frame(), HOME, AWAY)
    h, a = out["teams"][HOME], out["teams"][AWAY]

    assert h["total_drives"] == 3 and a["total_drives"] == 3
    assert h["scoring_drives"] == 1 and a["scoring_drives"] == 2
    assert h["td_drives"] == 1 and a["td_drives"] == 1
    assert a["three_and_outs"] == 1 and h["forced_three_and_outs"] == 1
    # away scored the ensuing TD after home's INT
    assert a["points_off_turnovers"] == 7 and h["points_off_turnovers"] == 0
    # both success definitions, named separately: home TD drive succeeds both;
    # d2 (INT, but first_down_created on its plays) succeeds fd only;
    # d5 (DOWNS, no first down, 9/55 yds) fails both
    assert h["drive_success_rate_fd"] == round(2 / 3, 3)
    assert h["drive_success_rate_ay"] == round(1 / 3, 3)
    # long-drive buckets
    assert h["long_drives_70yds"] == 1 and a["long_drives_10plays"] == 1
    assert a["drives_over_5min"] == 2 and h["drives_under_1min"] == 1


def test_drive_summary_frame_aggregates():
    out = drive_summary.create_drive_summary(_drives(), _frame(), HOME, AWAY)
    h, a = out["teams"][HOME], out["teams"][AWAY]
    assert h["avg_third_down_distance"] == 6.5  # (4+9)/2
    assert h["third_downs"] == {"made": 2, "att": 2}
    assert a["fourth_downs"] == {"made": 1, "att": 1}
    assert h["first_downs"] == {"rush": 1, "pass": 2, "penalty": 0}
    assert a["first_downs"] == {"rush": 1, "pass": 0, "penalty": 1}
    # largest lead + score-state clock (home led 7-0 through the middle rows)
    assert h["largest_lead"] == 7 and a["largest_lead"] == 3
    # intervals belong to each play's OUTCOME: the opening TD drive's time
    # counts as leading (not the tied pre-score state), and the tail after
    # the last play carries the final score state
    assert h["time_leading_seconds"] == 1800
    assert a["time_leading_seconds"] == 1500
    assert h["time_tied_seconds"] == 300
    # long plays sorted, positive gains only, per team
    assert out["longPlays"][AWAY][0]["yards"] == 45
    assert all(p["yards"] > 0 for ps in out["longPlays"].values() for ps in [ps] for p in ps)


def test_drive_chart_obtained_and_scores():
    out = drive_summary.create_drive_summary(_drives(), _frame(), HOME, AWAY)
    chart = out["chart"]
    assert [c["obtained"] for c in chart] == ["KO", "KO", "PUNT", "INT", "KO", "KO"]
    assert chart[2]["result"] == "INT" and chart[2]["plays"] == 4
    # scores list carries the finishing play and the drive description
    assert len(out["scores"]) == 3
    assert out["scores"][0]["finishing_play"] == "the finishing play"


def test_start_field_position_orientation():
    # ESPN's yardLine axis is home-oriented; start.text is authoritative
    drives = [
        # home at its OWN 25 via text
        _drive(HOME, "d1", "PUNT", 5, 4, "2:00", 25, 1, start_text="HH 25"),
        # away at its OWN 25: text names the away side
        _drive(AWAY, "d2", "PUNT", 5, 4, "2:00", 75, 1, start_text="AA 25"),
        # away in HOME territory at the HH 30 -> 30 to go
        _drive(AWAY, "d3", "PUNT", 5, 4, "2:00", 30, 2, start_text="HH 30"),
        # no text: fall back to the home-oriented axis (away yte == yardLine)
        _drive(AWAY, "d4", "PUNT", 5, 4, "2:00", 60, 3),
    ]
    out = drive_summary.create_drive_summary(drives, _frame(), HOME, AWAY)
    assert out["teams"][HOME]["avg_start_yards_to_endzone"] == 75.0
    # away: 75 (own 25) + 30 (opp 30) + 60 (fallback) -> 55.0
    assert out["teams"][AWAY]["avg_start_yards_to_endzone"] == 55.0
    assert out["teams"][HOME]["avg_start_text"] == "OWN 25"


def test_drive_summary_fails_open_on_bad_inputs():
    assert drive_summary.create_drive_summary([], _frame(), HOME, AWAY) is None
    assert drive_summary.create_drive_summary(_drives(), pl.DataFrame(), HOME, AWAY) is None
    assert drive_summary.create_drive_summary(_drives(), None, HOME, AWAY) is None


def test_windowed_build_books_drives_to_start_quarter():
    out = drive_summary.create_drive_summary(_drives(), _frame(), HOME, AWAY, periods={2})
    h, a = out["teams"][HOME], out["teams"][AWAY]
    # Q2: home's INT drive, away's ensuing TD -- and pts-off-TO still credits
    # because the out-of-window context (running score, prev drive) is kept
    assert h["total_drives"] == 1 and a["total_drives"] == 1
    assert a["points_off_turnovers"] == 7
    # lead and clock state window too -- see the _clock_frame tests below for
    # the bounds themselves; this fixture's clock does not match its periods
    assert "largest_lead" in h and "time_leading_seconds" in h
    # chart holds only in-window drives
    assert [c["period"] for c in out["chart"]] == [2, 2]
    # first-down sources window with everything else (incl. penalty count)
    assert h["first_downs"] == {"rush": 0, "pass": 1, "penalty": 0}
    assert a["first_downs"] == {"rush": 0, "pass": 0, "penalty": 1}


def test_windowed_build_empty_window_is_none():
    assert drive_summary.create_drive_summary(_drives(), _frame(), HOME, AWAY, periods="ot") is None


def test_playprocess_delegate_matches_module():
    from sportsdataverse.cfb.cfb_pbp import CFBPlayProcess

    direct = drive_summary.create_drive_summary(_drives(), _frame(), HOME, AWAY)
    via_method = CFBPlayProcess.create_drive_summary(object(), _frame(), _drives())
    assert via_method == direct


def test_accepts_raw_espn_drives_grouping():
    grouping = {"previous": _drives()[:-1], "current": _drives()[-1]}
    flat = drive_summary.create_drive_summary(_drives(), _frame(), HOME, AWAY)
    from_grouping = drive_summary.create_drive_summary(grouping, _frame(), HOME, AWAY)
    assert from_grouping == flat


def test_delegate_fails_open_on_unusable_frame():
    from sportsdataverse.cfb.cfb_pbp import CFBPlayProcess

    assert CFBPlayProcess.create_drive_summary(object(), pl.DataFrame(), _drives()) is None
    assert CFBPlayProcess.create_drive_summary(object(), None, _drives()) is None


# --- windowed score-state clock ------------------------------------------
# The shared _frame() fixture labels a play at 1500 game-seconds remaining as
# Q2, which is really Q3 on the regulation axis. That is harmless for the
# aggregates it was built for, but a clock-bounds test has to run on a frame
# whose periods and clock agree, so this one does.


def _clock_frame():
    return pl.DataFrame(
        {
            "game_play_number": [1, 2, 3, 4, 5, 6, 7, 8],
            "scrimmage_play": [True] * 8,
            "pos_team": [10, 20, 10, 20, 20, 10, 20, 10],
            "down": [1, 1, 1, 1, 1, 1, 1, 1],
            "distance": [10] * 8,
            "first_down_created": [False] * 8,
            "firstD_by_penalty": [False] * 8,
            "touchdown": [False] * 8,
            "rush": [True, False] * 4,
            "pass": [False, True] * 4,
            "period": [1, 1, 2, 2, 3, 3, 4, 4],
            # strictly inside each quarter: Q1 3600-2700, Q2 2700-1800,
            # Q3 1800-900, Q4 900-0
            "start.adj_TimeSecsRem": [3500.0, 3000.0, 2600.0, 2000.0, 1700.0, 1200.0, 800.0, 200.0],
            "start.homeScore": [0, 0, 0, 0, 7, 7, 7, 7],
            "start.awayScore": [0, 0, 0, 0, 0, 7, 7, 7],
            "statYardage": [5] * 8,
            "text": list("abcdefgh"),
            "homeTeamId": [10] * 8,
            "awayTeamId": [20] * 8,
            "drive.id": ["c1", "c2", "c3", "c4", "c5", "c6", "c7", "c8"],
        }
    )


def _clock_drives():
    return [
        _drive(HOME, "c1", "PUNT", 10, 3, "1:40", 25, 1, last_score=(0, 0)),
        _drive(AWAY, "c2", "PUNT", 10, 3, "1:40", 25, 1, last_score=(0, 0)),
        _drive(HOME, "c3", "TD", 75, 8, "3:20", 25, 2, is_score=True, last_score=(7, 0)),
        _drive(AWAY, "c4", "PUNT", 10, 3, "1:40", 25, 2, last_score=(7, 0)),
        _drive(AWAY, "c5", "TD", 75, 8, "3:20", 25, 3, is_score=True, last_score=(7, 7)),
        _drive(HOME, "c6", "PUNT", 10, 3, "1:40", 25, 3, last_score=(7, 7)),
        _drive(AWAY, "c7", "FG", 40, 8, "3:20", 25, 4, is_score=True, last_score=(7, 10)),
        _drive(HOME, "c8", "DOWNS", 9, 4, "0:50", 45, 4, last_score=(7, 10)),
    ]


def _accounted(out):
    h, a = out["teams"][HOME], out["teams"][AWAY]
    return h["time_leading_seconds"] + a["time_leading_seconds"] + h["time_tied_seconds"]


def test_windowed_clock_accounts_for_exactly_the_window():
    """Every second of the window is charged to somebody, and none outside it.

    This is the regression that matters: before the bounds fix the last play's
    remainder ran to 0:00 of the GAME, so a Q3 build silently swallowed the
    fourth quarter. A quarter is 900 seconds and the accounting must say so.
    """
    for periods, length in (
        ({1}, 900),
        ({2}, 900),
        ({3}, 900),
        ({4}, 900),
        ({1, 2}, 1800),
        ({3, 4}, 1800),
        # gapped: two quarters, and none of the one between them
        ({1, 3}, 1800),
        ({1, 2, 4}, 2700),
        (None, 3600),
    ):
        out = drive_summary.create_drive_summary(_clock_drives(), _clock_frame(), HOME, AWAY, periods=periods)
        assert _accounted(out) == length, (periods, _accounted(out))


def test_windowed_clock_uses_the_window_score_not_the_final():
    # Q2: home goes ahead 7-0 and stays there; the game ends 7-10, and that
    # final must not colour a second of this window.
    out = drive_summary.create_drive_summary(_clock_drives(), _clock_frame(), HOME, AWAY, periods={2})
    h, a = out["teams"][HOME], out["teams"][AWAY]
    assert a["time_leading_seconds"] == 0
    assert h["time_leading_seconds"] + h["time_tied_seconds"] == 900
    # entering Q2 the game was level, so the tied share is real, not an artefact
    assert h["time_tied_seconds"] > 0
    # Q4 opens level at 7-7 and away's field goal lands late: the quarter is
    # tied until then, and only the tail belongs to away. The tail is charged
    # from the score at the WINDOW's end -- which here happens to equal the
    # game's, so the sharper check is that it is neither 0 (the closing score
    # ignored) nor 900 (the whole quarter mislabelled).
    q4 = drive_summary.create_drive_summary(_clock_drives(), _clock_frame(), HOME, AWAY, periods={4})
    assert q4["teams"][HOME]["time_leading_seconds"] == 0
    assert q4["teams"][AWAY]["time_leading_seconds"] == 200
    assert q4["teams"][HOME]["time_tied_seconds"] == 700


def test_windowed_largest_lead_sees_the_window_edges():
    # Home's 7-0 lead is established in Q2 and gone by the end of Q3. A Q3
    # build must still report it: the window OPENS with home up seven, which no
    # in-window snap after the tying score would show on its own.
    q3 = drive_summary.create_drive_summary(_clock_drives(), _clock_frame(), HOME, AWAY, periods={3})
    assert q3["teams"][HOME]["largest_lead"] == 7
    assert q3["teams"][AWAY]["largest_lead"] == 0


def test_ot_window_reports_lead_but_no_clock():
    """OT has no clock axis to integrate over, so the time split must be absent.

    ESPN files every OT play under one period number and adj_TimeSecsRem
    collapses there -- see the sort note in cfb_pbp.
    """
    frame = _clock_frame().with_columns(period=pl.lit(5, dtype=pl.Int64))
    drives = [
        _drive(AWAY, "o1", "TD", 25, 3, "0:00", 25, 5, is_score=True, last_score=(7, 17)),
        _drive(HOME, "o2", "DOWNS", 12, 4, "0:00", 25, 5, last_score=(7, 17)),
    ]
    out = drive_summary.create_drive_summary(drives, frame, HOME, AWAY, periods="ot")
    h = out["teams"][HOME]
    assert "largest_lead" in h
    assert "time_leading_seconds" not in h and "time_tied_seconds" not in h


def test_clock_intervals_table():
    assert drive_summary._clock_intervals(None) == [(3600, 0)]
    assert drive_summary._clock_intervals({1}) == [(3600, 2700)]
    assert drive_summary._clock_intervals({3}) == [(1800, 900)]
    assert drive_summary._clock_intervals({1, 2}) == [(3600, 1800)]
    assert drive_summary._clock_intervals({4}) == [(900, 0)]
    assert drive_summary._clock_intervals("ot") is None
    assert drive_summary._clock_intervals({5, 6}) is None
    # a gapped set is two runs, never one span across the quarter it skipped
    assert drive_summary._clock_intervals({1, 3}) == [(3600, 2700), (1800, 900)]
    assert drive_summary._clock_intervals({1, 2, 4}) == [(3600, 1800), (900, 0)]


def test_boundary_score_comes_from_plays_not_drive_outcomes():
    """A drive that starts in Q2 and scores in Q3 must not colour Q2's clock.

    The drive books to Q2, so a Q3 window excludes it -- but its points land
    inside Q3. Taking the window's opening score from drive outcomes would put
    those seven points on the wrong side of the boundary in both directions.
    """
    frame = _clock_frame().with_columns(
        # home's go-ahead score now happens just INSIDE Q3, on a drive that
        # started in Q2: the play at 1700 is the first one that sees 7-0
        pl.Series("start.homeScore", [0, 0, 0, 0, 0, 7, 7, 7]),
        pl.Series("start.awayScore", [0, 0, 0, 0, 0, 0, 0, 0]),
    )
    drives = _clock_drives()
    q2 = drive_summary.create_drive_summary(drives, frame, HOME, AWAY, periods={2})
    q3 = drive_summary.create_drive_summary(drives, frame, HOME, AWAY, periods={3})
    # Q2 was level throughout -- the score arrives after the quarter ends
    assert q2["teams"][HOME]["time_leading_seconds"] == 0
    assert q2["teams"][HOME]["time_tied_seconds"] == 900
    assert q2["teams"][HOME]["largest_lead"] == 0
    # Q3 carries the lead, and both windows still account for their full length
    assert q3["teams"][HOME]["time_leading_seconds"] > 0
    assert _accounted(q2) == 900 and _accounted(q3) == 900


def test_gapped_window_skips_the_quarter_between():
    """{1, 3} must charge Q1 and Q3 and nothing from Q2."""
    q1 = drive_summary.create_drive_summary(_clock_drives(), _clock_frame(), HOME, AWAY, periods={1})
    q3 = drive_summary.create_drive_summary(_clock_drives(), _clock_frame(), HOME, AWAY, periods={3})
    both = drive_summary.create_drive_summary(_clock_drives(), _clock_frame(), HOME, AWAY, periods={1, 3})
    for team in (HOME, AWAY):
        assert (
            both["teams"][team]["time_leading_seconds"]
            == q1["teams"][team]["time_leading_seconds"] + q3["teams"][team]["time_leading_seconds"]
        )
    assert both["teams"][HOME]["time_tied_seconds"] == (
        q1["teams"][HOME]["time_tied_seconds"] + q3["teams"][HOME]["time_tied_seconds"]
    )
