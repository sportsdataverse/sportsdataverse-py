"""Drive summary + drive chart, StatBroadcast-style.

Computed from two sources, deliberately split:
- the ESPN drives grouping (ordered, with ``team.id``) for anything drive-level
  -- drive ids on PLAYS span both teams, so per-drive attribution must come
  from ``drive.team``, never from plays grouped by drive id;
- the enriched plays frame produced by :meth:`CFBPlayProcess.run_processing_pipeline`
  (retained as ``plays_frame``) for play-flag aggregates (third downs,
  first-down sources, long plays, score-state clock).

Definitions follow the StatBroadcast game book where one exists:
- three-and-out: <= 3 offensive plays, lost by punt;
- a 3rd/4th-down TD counts as a conversion;
- points off turnovers: points the defense scores during the turnover drive
  (pick-six et al) plus points on the ensuing possession;
- drive success ships under TWO named metrics (maintainer decision):
  ``drive_success_rate_fd`` (>= 1 first down or score) and
  ``drive_success_rate_ay`` (>= 50% of available yards, TDs are 100%).
Known approximation: a drive's time of possession is assigned to the quarter
it STARTED in (a drive spanning the break books whole to the earlier quarter).
"""

import re

import polars as pl

__all__ = ["create_drive_summary"]

_TURNOVER_RESULTS = {"INT", "FUMBLE", "INT TD", "FUMBLE TD"}
_SCORING_KICKOFF_NEXT = {"TD", "FG", "INT TD", "FUMBLE TD", "MADE FG"}


def _top_seconds(drive):
    raw = ((drive.get("timeElapsed") or {}).get("displayValue") or "").strip()
    parts = raw.split(":")
    if len(parts) == 2 and all(p.isdigit() for p in parts):
        return int(parts[0]) * 60 + int(parts[1])
    return None


def _spot(drive, which):
    node = drive.get(which) or {}
    return (node.get("text") or "").strip() or None


def _clock(drive, which):
    node = drive.get(which) or {}
    return ((node.get("clock") or {}).get("displayValue") or "").strip() or None


def _period(drive):
    return ((drive.get("start") or {}).get("period") or {}).get("number")


def _team_id(drive):
    tid = (drive.get("team") or {}).get("id")
    return str(tid) if tid is not None else None


def _yards_to_endzone(drive, is_home):
    """Distance to the end zone at the drive start, for the DRIVING team.

    ESPN's drive ``start.yardLine`` runs on a HOME-oriented 0-100 axis, so
    the away team's distance is ``yardLine`` and the home team's is
    ``100 - yardLine``. The ``start.text`` ("TCU 25") is authoritative when
    it parses: the named side says whose half the ball is on.
    """
    start = drive.get("start") or {}
    text = (start.get("text") or "").strip()
    abbr = ((drive.get("team") or {}).get("abbreviation") or "").upper()
    m = re.match(r"([A-Z&'-]+)\s+(\d{1,2})$", text.upper()) if text else None
    if m and abbr:
        side, num = m.group(1), int(m.group(2))
        return (100 - num) if side == abbr else num
    yl = start.get("yardLine")
    if not isinstance(yl, (int, float)):
        return None
    return (100 - yl) if is_home else yl


def _score_after(drive, prev_home, prev_away):
    """(home, away) after the drive's last play; carried forward when absent."""
    plays = drive.get("plays") or []
    for p in reversed(plays):
        h, a = p.get("homeScore"), p.get("awayScore")
        if h is not None and a is not None:
            return int(h), int(a)
    return prev_home, prev_away


def _obtained(prev_result, is_first_of_half):
    if is_first_of_half or prev_result is None:
        return "KO"
    r = prev_result.upper()
    if r in _SCORING_KICKOFF_NEXT:
        return "KO"
    if r == "PUNT":
        return "PUNT"
    if "INT" in r:
        return "INT"
    if "FUMBLE" in r:
        return "FUM"
    if r == "DOWNS":
        return "DOWNS"
    if "FG" in r:  # missed FG variants
        return "FG MISS"
    return r


# Regulation runs 3600 game-seconds down to 0, so quarter q spans
# 3600 - 900*(q-1) down to 3600 - 900*q. `middle_8` is defined against the same
# axis in cfb_pbp (1560 <= t <= 2040, i.e. halftime +/- 4:00).
_REG_SECONDS = 3600
_QUARTER_SECONDS = 900


def _clock_intervals(periods: set[int] | str | None) -> list[tuple[int, int]] | None:
    """The window's ``adj_TimeSecsRem`` intervals, or None when it has no clock.

    One ``(high, low)`` interval per CONTIGUOUS run of selected quarters, so a
    gapped set like ``{1, 3}`` yields two intervals and never charges the second
    quarter it did not ask for.

    Returns None for overtime: from 2023 ESPN files every OT play under one
    ``period.number`` and the clock-derived ``adj_TimeSecsRem`` collapses (see
    the sort note in ``cfb_pbp``), so there is no axis to integrate time over.
    A window of only OT therefore reports ``largest_lead`` but no time split.
    """
    if periods is None:
        return [(_REG_SECONDS, 0)]
    if isinstance(periods, str):  # the "ot" sentinel, and any other string
        return None
    reg = sorted(p for p in periods if 1 <= p <= 4)
    if not reg:
        return None
    runs: list[tuple[int, int]] = []
    first = prev = reg[0]
    for q in reg[1:]:
        if q != prev + 1:
            runs.append((first, prev))
            first = q
        prev = q
    runs.append((first, prev))
    return [
        (_REG_SECONDS - _QUARTER_SECONDS * (lo_q - 1), _REG_SECONDS - _QUARTER_SECONDS * hi_q) for lo_q, hi_q in runs
    ]


def create_drive_summary(
    drives: list[dict] | dict,
    frame: pl.DataFrame,
    home_id: str | int,
    away_id: str | int,
    periods: set[int] | str | None = None,
) -> dict | None:
    """Build the StatBroadcast-style drive summary, chart, and long-play lists.

    A drive belongs to the quarter it STARTED in. On a windowed build the
    full drive sequence still provides context (running score, the previous
    drive for OBTAINED and points-off-turnovers), but only in-window drives
    are counted, charted, or listed. ``largest_lead`` and the time-leading /
    time-tied split window too: the score state is read from the whole
    regulation play sequence and then clipped to the window's clock intervals
    (one per contiguous run of quarters, so a gapped set never charges the
    quarter it skipped). Under ``"ot"`` the clock has no axis to integrate over
    and only ``largest_lead`` ships.

    Args:
        drives (list[dict]): the ESPN drives grouping, in game order
            (``previous`` plus the in-progress ``current`` drive, if any).
        frame (pl.DataFrame): the enriched plays frame from
            :meth:`CFBPlayProcess.run_processing_pipeline` (``plays_frame``).
        home_id: ESPN home team id.
        away_id: ESPN away team id.
        periods: optional window -- a set of quarter numbers (e.g. ``{1, 2}``)
            or the string ``"ot"`` (every period > 4). ``None`` = full game.

    Returns:
        dict: ``{"teams": {...}, "chart": [...], "scores": [...],
        "longPlays": {...}}`` keyed by team id, or ``None`` when the inputs
        are unusable (no drives, empty frame, or an empty window).

    Example:
        Full-game summary from a processed game::

            summary = create_drive_summary(drives, game.plays_frame, "52", "61")
    """
    if isinstance(drives, dict):  # the raw ESPN grouping, not yet flattened
        flat = list(drives.get("previous") or [])
        if drives.get("current"):
            flat.append(drives["current"])
        drives = flat
    if not drives or not isinstance(frame, pl.DataFrame) or frame.height == 0:
        return None

    def in_window(p):
        if periods is None:
            return True
        if periods == "ot":
            return p is not None and p > 4
        return p in periods

    home_id, away_id = str(home_id), str(away_id)
    team_ids = {home_id, away_id}

    def blank():
        return {
            "total_drives": 0,
            "scoring_drives": 0,
            "td_drives": 0,
            "yards": 0,
            "plays": 0,
            "top_seconds": 0,
            "long_drives_70yds": 0,
            "long_drives_10plays": 0,
            "drives_over_5min": 0,
            "drives_under_1min": 0,
            # DURATION, not clock position: a drive that took under two minutes
            # of game clock, whenever it happened. `under_2` on the plays frame
            # is the other thing -- snaps inside the final two minutes of a half.
            "drives_under_2min": 0,
            "scoring_drives_under_2min": 0,
            "three_and_outs": 0,
            "forced_three_and_outs": 0,
            "points_off_turnovers": 0,
            "success_fd": 0,
            "success_ay": 0,
            "start_yte_sum": 0,
            "start_yte_n": 0,
            "top_by_quarter": {},
        }

    teams = {home_id: blank(), away_id: blank()}
    chart = []
    scores = []

    h, a = 0, 0
    # score entering and leaving the window, for the score-state clock below.
    # On a full-game build these collapse to 0-0 and the final score.
    win_open: tuple[int, int] | None = None
    win_close = (0, 0)
    ordered = [d for d in drives if _team_id(d) in team_ids]
    for i, d in enumerate(ordered):
        tid = _team_id(d)
        opp = away_id if tid == home_id else home_id
        t = teams[tid]
        result = (d.get("result") or "").upper()
        top = _top_seconds(d)
        yards = d.get("yards") or 0
        plays = d.get("offensivePlays") or 0
        yte = _yards_to_endzone(d, tid == home_id)
        period = _period(d)

        prev = ordered[i - 1] if i > 0 else None
        first_of_half = prev is None or (
            _period(prev) is not None and period is not None and (_period(prev) <= 2 < period)
        )

        if not in_window(period):
            # out-of-window drives still advance the running score so
            # points-off-turnovers and OBTAINED stay correct at the seams
            h, a = _score_after(d, h, a)
            continue

        if win_open is None:
            win_open = (h, a)

        t["total_drives"] += 1
        t["yards"] += yards
        t["plays"] += plays
        if top is not None:
            t["top_seconds"] += top
            if period is not None:
                q = t["top_by_quarter"].setdefault(int(period), 0)
                t["top_by_quarter"][int(period)] = q + top
            if top >= 300:
                t["drives_over_5min"] += 1
            if top < 60:
                t["drives_under_1min"] += 1
            if top < 120:
                t["drives_under_2min"] += 1
                if d.get("isScore"):
                    t["scoring_drives_under_2min"] += 1
        if yards >= 70:
            t["long_drives_70yds"] += 1
        if plays >= 10:
            t["long_drives_10plays"] += 1
        if yte is not None:
            t["start_yte_sum"] += yte
            t["start_yte_n"] += 1
        if d.get("isScore"):
            t["scoring_drives"] += 1
        if result in ("TD",):
            t["td_drives"] += 1
        if plays <= 3 and result == "PUNT":
            t["three_and_outs"] += 1
            teams[opp]["forced_three_and_outs"] += 1

        # score deltas across this drive, for points-off-turnovers + scores list
        h2, a2 = _score_after(d, h, a)
        own_delta = (h2 - h) if tid == home_id else (a2 - a)
        opp_delta = (a2 - a) if tid == home_id else (h2 - h)
        # a defensive score during the turnover drive itself (pick-six)
        if result in _TURNOVER_RESULTS and opp_delta > 0:
            teams[opp]["points_off_turnovers"] += opp_delta
        # the ensuing possession after a turnover
        if (
            prev is not None
            and (prev.get("result") or "").upper() in _TURNOVER_RESULTS
            and _team_id(prev) == opp
            and own_delta > 0
        ):
            t["points_off_turnovers"] += own_delta

        if d.get("isScore"):
            # the SCORING play, not merely the last play with text -- a
            # post-score penalty or PAT note can be the drive's final entry
            finishing = None
            for p in reversed(d.get("plays") or []):
                if p.get("scoringPlay") and p.get("text"):
                    finishing = p["text"]
                    break
            if finishing is None:
                for p in reversed(d.get("plays") or []):
                    if p.get("text"):
                        finishing = p["text"]
                        break
            scores.append(
                {
                    "team_id": tid,
                    "period": period,
                    "result": result,
                    "description": d.get("description"),
                    "finishing_play": finishing,
                }
            )

        chart.append(
            {
                "team_id": tid,
                "period": period,
                "start_spot": _spot(d, "start"),
                "start_clock": _clock(d, "start"),
                "obtained": _obtained((prev.get("result") if prev else None), first_of_half),
                "end_spot": _spot(d, "end"),
                "end_clock": _clock(d, "end"),
                "result": result or None,
                "plays": plays,
                "yards": yards,
                "top_seconds": top,
            }
        )

        # drive success, both named metrics
        succeeded_fd = bool(d.get("isScore"))
        if not succeeded_fd:
            sub = frame.filter((pl.col("drive.id") == d.get("id")) & (pl.col("pos_team").cast(pl.Utf8) == tid))
            succeeded_fd = bool(
                sub.select(
                    (pl.col("first_down_created") == True).any()  # noqa: E712
                    | (pl.col("firstD_by_penalty") == True).any()  # noqa: E712
                ).item()
            )
        if succeeded_fd:
            t["success_fd"] += 1
        if yte and (yards / yte) >= 0.5:
            t["success_ay"] += 1
        elif result == "TD":
            t["success_ay"] += 1

        h, a = h2, a2
        win_close = (h, a)

    # frame-side aggregates per team, windowed to match
    scrim = frame.filter(pl.col("scrimmage_play") == True)  # noqa: E712
    if periods == "ot":
        scrim = scrim.filter(pl.col("period") > 4)
    elif periods is not None:
        scrim = scrim.filter(pl.col("period").is_in(sorted(periods)))
    for tid, t in teams.items():
        mine = scrim.filter(pl.col("pos_team").cast(pl.Utf8) == tid)
        third = mine.filter(pl.col("down") == 3)
        fourth = mine.filter(pl.col("down") == 4)
        conv = lambda df: int(  # noqa: E731
            df.select(((pl.col("first_down_created") == True) | (pl.col("touchdown") == True)).sum()).item()  # noqa: E712
        )
        _third_dist = third["distance"].mean() if third.height else None
        t["avg_third_down_distance"] = round(_third_dist, 1) if _third_dist is not None else None
        t["third_downs"] = {"made": conv(third), "att": third.height}
        t["fourth_downs"] = {"made": conv(fourth), "att": fourth.height}
        t["first_downs"] = {
            "rush": int(mine.select(((pl.col("first_down_created") == True) & (pl.col("rush") == True)).sum()).item()),  # noqa: E712
            "pass": int(mine.select(((pl.col("first_down_created") == True) & (pl.col("pass") == True)).sum()).item()),  # noqa: E712
            "penalty": int(mine.select(pl.col("firstD_by_penalty").sum()).item()),
        }

        n = t["total_drives"] or 1
        t["scoring_pct"] = round(t["scoring_drives"] / n, 3)
        t["td_rate"] = round(t["td_drives"] / n, 3)
        t["drive_success_rate_fd"] = round(t["success_fd"] / n, 3)
        t["drive_success_rate_ay"] = round(t["success_ay"] / n, 3)
        t["avg_yards"] = round(t["yards"] / n, 1)
        t["avg_plays"] = round(t["plays"] / n, 1)
        t["avg_top_seconds"] = round(t["top_seconds"] / n)
        if t["start_yte_n"]:
            yte = t["start_yte_sum"] / t["start_yte_n"]
            own = round(100 - yte)
            t["avg_start_yards_to_endzone"] = round(yte, 1)
            t["avg_start_text"] = f"OWN {own}" if own <= 50 else f"OPP {100 - own}"
        else:
            t["avg_start_yards_to_endzone"] = None
            t["avg_start_text"] = None
        del t["success_fd"], t["success_ay"], t["start_yte_sum"], t["start_yte_n"]

    # largest lead + seconds leading/trailing/tied from the score-state clock.
    #
    # Built from the WHOLE regulation sequence, not the windowed one. A drive
    # books to the quarter it started in, so a drive that starts in Q2 and
    # scores in Q3 is out of a Q3 window while its points land inside it --
    # taking the boundary score from drive outcomes would credit those points
    # to the wrong side of the edge. Play start scores do not have that problem.
    #
    # Each consecutive pair of plays defines an interval whose score state is
    # the LATER play's start score: the stretch after a play belongs to that
    # play's outcome, so a go-ahead score counts its own aftermath as leading.
    # The window then just clips those intervals.
    reg_all = (
        frame.filter((pl.col("scrimmage_play") == True) & (pl.col("period") <= 4))  # noqa: E712
        .sort("game_play_number")
        .select(["start.adj_TimeSecsRem", "start.homeScore", "start.awayScore"])
        .to_dicts()
    )
    final_h, final_a = h, a  # running score after the chart walk = final score
    segments: list[tuple[float, float, int, int]] = []
    if reg_all:
        first_rem = reg_all[0].get("start.adj_TimeSecsRem")
        if first_rem is not None and first_rem < _REG_SECONDS:
            # kickoff to the first snap, at 0-0
            segments.append((_REG_SECONDS, first_rem, 0, 0))
        for j, r in enumerate(reg_all):
            t0 = r.get("start.adj_TimeSecsRem")
            if t0 is None:
                continue
            if j + 1 < len(reg_all):
                nxt = reg_all[j + 1]
                t1 = nxt.get("start.adj_TimeSecsRem")
                sh = nxt.get("start.homeScore") or 0
                sa = nxt.get("start.awayScore") or 0
            else:
                t1, sh, sa = 0, final_h, final_a
            if t1 is not None and t0 > t1:
                segments.append((t0, t1, sh, sa))

    lead = {home_id: 0, away_id: 0}
    clockstate = {home_id: 0.0, away_id: 0.0, "tied": 0.0}
    intervals = _clock_intervals(periods)
    if intervals is not None:
        for hi, lo in intervals:
            for t0, t1, sh, sa in segments:
                overlap = min(t0, hi) - max(t1, lo)
                if overlap <= 0:
                    continue
                key = home_id if sh > sa else away_id if sa > sh else "tied"
                clockstate[key] += overlap
                # only states the window actually saw can set its largest lead
                lead[home_id] = max(lead[home_id], sh - sa)
                lead[away_id] = max(lead[away_id], sa - sh)
    else:
        # overtime: no clock axis, so the lead comes from the drive boundaries
        open_h, open_a = win_open if win_open is not None else (0, 0)
        for ph, pa in ((open_h, open_a), win_close):
            lead[home_id] = max(lead[home_id], ph - pa)
            lead[away_id] = max(lead[away_id], pa - ph)

    for tid, t in teams.items():
        t["largest_lead"] = lead[tid]
    if intervals is not None:
        for tid, t in teams.items():
            t["time_leading_seconds"] = round(clockstate[tid])
        tied_s = round(clockstate["tied"])
        for t in teams.values():
            t["time_tied_seconds"] = tied_s

    # long plays: top 5 scrimmage gains per team
    long_plays = {}
    for tid in team_ids:
        top5 = (
            scrim.filter(pl.col("pos_team").cast(pl.Utf8) == tid)
            .sort("statYardage", descending=True, nulls_last=True)
            .head(5)
            .select(["statYardage", "period", "text"])
            .to_dicts()
        )
        long_plays[tid] = [
            {"yards": p["statYardage"], "period": p["period"], "text": p["text"]}
            for p in top5
            if (p["statYardage"] or 0) > 0
        ]

    if periods is not None and not chart:
        return None  # nothing happened in this window
    return {"teams": teams, "chart": chart, "scores": scores, "longPlays": long_plays}
