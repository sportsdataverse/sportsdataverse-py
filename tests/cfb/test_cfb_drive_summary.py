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
    # game-level lines never ship on a windowed build
    assert "largest_lead" not in h and "time_leading_seconds" not in h
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
