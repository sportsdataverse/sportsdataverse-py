"""NCAA (stats.ncaa.org) contest pages -> an ESPN-summary-shaped dict ``CFBPlayProcess`` consumes.

The projection, and **only** the projection: the parse is the graduated one
(:func:`sportsdataverse.cfb.cfb_ncaa_pbp.parse_cfb_ncaa_pbp` +
:func:`sportsdataverse.cfb.cfb_ncaa_box` + :func:`sportsdataverse.cfb.to_cfbfastr`), which
already owns every NCAA text quirk anyone has paid for -- side-code splitting, the drive-title
team vote, event-sourced running scores, the ``_own_side`` orientation fix, the NC1-NC10 round
of fixes, and cfbfastR's own conventions (a kickoff row's possession is the **returning** team;
``yards_to_goal_end`` is measured for whoever holds the ball **after** the play). None of that
is re-derived here. This module re-skins that frame as ESPN: absolute yard lines, ESPN's play
types, the try folded into its touchdown, the drive grouping and a renderable header.

The output is validated by :func:`sportsdataverse.football.sources.contract._validate_summary`
and consumed through ``espn_cfb_pbp(summary=)``; the dispatcher registers it as ``source="ncaa"``
for the CFB (:mod:`sportsdataverse.football.sources.dispatch`).

Everything here is ``_``-prefixed and nothing is re-exported from ``sportsdataverse.cfb``: like
the Shield and Yahoo adapters this is a private, experimental surface, so no codegen or
reference-doc regeneration is involved.

Why this source matters out of proportion to its place in the failover order: **it is the only
source that carries an FCS-hosted game, in any era.** Fox, CBS and Yahoo were each measured
empty on FCS-hosted CFB (Fox 0/4, CBS 404, Yahoo HTTP 200 with zero plays); the NCAA archive has
19,998 payloads back to fall 2013, all of them with play-by-play, and 43% of them are FCS-vs-FCS.

Documented divergences from a real ESPN summary (measured, Stage 2 ``s2-ncaa-cfb`` gate):

* **No ESPN play ids.** The emitted id is ``{espn_event_id}{index:04d}``: stable, unique and
  increasing, but it joins nothing ESPN-sourced. Parity has to join on game state.
* **Overtime is synthesized**, one row per OT drive with no down and no distance, because
  stats.ncaa.org's pbp pages omit OT entirely (``to_cfbfastr`` rebuilds them from the drives tab
  and the scoring summary). Win probability on those rows is not meaningful.
* **Text grammar is NCAA LiveStats'**, and names are ``Last,First``. The processor already
  strips the clock, jersey and formation prefixes, and ESPN's own 2025+ CFB feed uses the same
  grammar, so most text-derived columns survive; the ones that do not are
  :data:`...contract.KNOWN_LOSSY` for ``("cfb", "ncaa")`` and dispatch stamps them into
  provenance.
* ``boxscore`` is empty: ESPN's box is the only source of ESPN athlete ids.
* **No odds.** The stored closing line (``odds_override``) is the only line a failover game gets.
"""

from __future__ import annotations

from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

import polars as pl

from sportsdataverse.cfb.ncaa_pbp.fetch import _espn_team_ids_from_bundle, _has_plays, _ncaa_team_ids
from sportsdataverse.cfb.yahoo_pbp.to_espn_summary import ESPN_PLAY_TYPES

#: ``type.text`` -> ``(type.id, type.abbreviation)``. The shared ESPN table (the Yahoo CFB
#: adapter enumerated it from 24 captured ESPN college-football summaries) inverted, plus the
#: labels ``to_cfbfastr`` emits that Yahoo never produces. ``type.id`` is a Game on Paper
#: presentation field; a label with no ESPN id known from a captured payload gets ``None``
#: rather than an invented number.
_TYPE_BY_TEXT: Dict[str, Tuple[Optional[str], Optional[str]]] = {
    text: (type_id, abbreviation) for type_id, (text, abbreviation) in ESPN_PLAY_TYPES.items()
}
_TYPE_BY_TEXT.update(
    {
        "Interception Return": ("26", "INTR"),
        "Kickoff Return (Offense)": ("53", "K"),
        "Blocked Punt": ("17", None),
        "Blocked Punt Touchdown": ("35", "TD"),
        "Blocked Field Goal Touchdown": ("38", "TD"),
        "Missed Field Goal Return Touchdown": ("40", "TD"),
        "Punt Team Fumble Recovery": ("55", None),
        "Punt Team Fumble Recovery Touchdown": ("56", "TD"),
        "Kickoff Team Fumble Recovery": ("57", None),
        "Kickoff Team Fumble Recovery Touchdown": ("58", "TD"),
        "Fumble Recovery (Opponent) Touchdown": ("39", "TD"),
        "End of Half": ("65", "EH"),
        "End of Game": ("66", "EG"),
        "Unknown": (None, None),
    }
)

#: ``play_type`` labels ESPN folds into the touchdown they follow.
_TRY_TYPES = frozenset({"Extra Point Good", "Extra Point Missed", "Two Point Pass", "Two Point Rush"})
#: Labels that stop the clock rather than describing a snap; the play *before* one of these ends
#: at the next real snap's spot, not at the stoppage row's.
_STOPPAGE = frozenset({"Timeout", "End Period", "End of Half", "End of Game"})
#: Labels on which the offence keeps the ball, so a row with no next snap still has a derivable
#: end spot from its own yardage.
_KEEPS_THE_BALL = frozenset({"Rush", "Pass Reception", "Pass Incompletion", "Sack", "Penalty"})

_ORDINAL = {1: "1st", 2: "2nd", 3: "3rd", 4: "4th"}

#: ``type.text`` -> the ``(end.down, end.distance)`` ESPN's own CFB summaries state instead of the
#: next snap's. Counted over 60 captured ESPN summaries: every touchdown label and every made
#: field goal is ``(-1, -1)`` (171/171 Passing Touchdown, 128/128 Field Goal Good, 7/7
#: Interception Return Touchdown, ...), and a **returned** kickoff is ``(-1, 10)`` -- not the 1st
#: and 10 the receiving team actually faces. The distinction is not cosmetic: the EP model reads
#: ``down``, so emitting the next snap's 1 & 10 on a kickoff return moved EP_end by about one
#: point on every one of them.
_FIXED_END_DOWN: Dict[str, Tuple[int, int]] = {
    "Kickoff Return (Offense)": (-1, 10),
    "Field Goal Good": (-1, -1),
}


def _norm(name: Optional[str]) -> str:
    """Normalise a team label for cross-surface matching (the mapper's own rule)."""
    from sportsdataverse.cfb.cfb_ncaa_cfbfastr import _norm_team

    return _norm_team(name)


def _frame(rows: Any, schema: Mapping[str, Any]) -> pl.DataFrame:
    """A polars frame in ``schema``; empty rows still carry the documented columns."""
    return pl.DataFrame(rows, schema=dict(schema)) if rows else pl.DataFrame(schema=dict(schema))


def _parse_bundle(
    bundle: Mapping[str, Any], season: Optional[int]
) -> Tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame, pl.DataFrame]:
    """``(cfbfastR frame, raw pbp frame, linescore frame, drive-title frame)`` for one bundle.

    Exactly the producer's own wiring (``ncaa_mfb_03_games_parse`` + ``builders.build_pbp_cfbfastr``),
    so a game adapted here and the same game in the published ``ncaa_mfb_pbp_cfbfastr`` asset come
    from one code path.
    """
    from sportsdataverse.cfb.cfb_ncaa_box import (
        parse_cfb_ncaa_drives,
        parse_cfb_ncaa_linescore,
        parse_cfb_ncaa_scoring_summary,
    )
    from sportsdataverse.cfb.cfb_ncaa_cfbfastr import to_cfbfastr
    from sportsdataverse.cfb.cfb_ncaa_pbp import parse_cfb_ncaa_drive_titles, parse_cfb_ncaa_pbp

    contest_id = bundle.get("contest_id")
    pbp_html = bundle.get("play_by_play") or ""
    box_html = bundle.get("box_score") or ""
    pbp = parse_cfb_ncaa_pbp(pbp_html, contest_id=contest_id)
    linescore = parse_cfb_ncaa_linescore(box_html, contest_id=contest_id)
    drives = parse_cfb_ncaa_drives(bundle.get("drives") or "", contest_id=contest_id)
    drive_titles = parse_cfb_ncaa_drive_titles(pbp_html, contest_id=contest_id)
    cfbfastr = to_cfbfastr(
        pbp,
        season=season,
        drives=drives,
        linescore=linescore,
        drive_titles=drive_titles,
        ot_drives=drives,
        scoring_summary=parse_cfb_ncaa_scoring_summary(box_html, contest_id=contest_id),
    )
    return cfbfastr, pbp, linescore, drive_titles


def _clocks(rows: Sequence[Mapping[str, Any]], drive_titles: pl.DataFrame) -> Tuple[List[str], int]:
    """A display clock for every row, and how many of them the page did not state.

    stats.ncaa.org prints ``(MM:SS)`` in front of a play only when the game's stat crew entered
    one, and on a large minority of pages -- 85-89% of rows in 6 of the 22 games the Stage 2 gate
    sampled -- it never does. The clock is an **EP and WP input**, so filling those rows with
    ``0:00`` puts every play at the end of its quarter and moves EP by 0.5-1.5 points a row: on
    the clockless games EP_start correlation against ESPN was .80-.88 with a 0:00 fill, against
    .96-.998 on the games that state a clock.

    Two real sources fill them, in order: the drive's own ``h5`` title, which states the clock the
    drive **started** at, and then the last clock actually stated -- carried forward, which is
    what ESPN's own CFB feed does anyway (it repeats one clock across consecutive plays; the
    Stage 2 Yahoo gate measured 9.5% row-level agreement between ESPN's clock and Yahoo's). A
    period with neither starts at 15:00. Nothing is interpolated: a carried clock is a real clock
    from the same drive, not a guess at elapsed time.
    """
    by_drive = {}
    if drive_titles.height and {"drive_number", "start_clock"} <= set(drive_titles.columns):
        by_drive = {t["drive_number"]: t["start_clock"] for t in drive_titles.to_dicts() if t.get("start_clock")}
    out: List[str] = []
    carried = 0
    last_clock = "15:00"
    last_period, last_drive = None, None
    for row in rows:
        period, drive = row.get("period"), row.get("drive_number")
        if period != last_period:
            last_clock, last_period = "15:00", period
        stated = row.get("clock.minutes") is not None or row.get("clock.seconds") is not None
        if stated:
            last_clock = _clock(row.get("clock.minutes"), row.get("clock.seconds"))
        else:
            if drive != last_drive and by_drive.get(drive):
                last_clock = _clock(*_split_clock(by_drive[drive]))
            carried += 1
        last_drive = drive
        out.append(last_clock)
    return out, carried


def _split_clock(text: str) -> Tuple[Optional[int], Optional[int]]:
    """``"07:12"`` -> ``(7, 12)``; anything else -> ``(None, None)``."""
    parts = str(text or "").split(":")
    if len(parts) != 2 or not all(p.strip().isdigit() for p in parts):
        return None, None
    return int(parts[0]), int(parts[1])


def _is_final(pbp: pl.DataFrame) -> bool:
    """True when the play-by-play page reaches the end of the game.

    The last row of a completed contest page is the ``End of game, clock 00:00.`` period marker.
    A page captured (or truncated) mid-game has no such row, which is the only shape signal a
    payload gives -- stats.ncaa.org states no status of its own. Read off the **raw** parse
    because ``to_cfbfastr`` appends synthesized overtime rows after it.
    """
    if not pbp.height or "play_text" not in pbp.columns:
        return False
    return bool(pbp.get_column("play_text").str.contains(r"(?i)end of game").fill_null(False).any())


def _sides(cfbfastr: pl.DataFrame, linescore: pl.DataFrame) -> Tuple[Optional[str], Optional[str]]:
    """``(home team name, away team name)`` -- the linescore is the only surface that states it."""
    row = cfbfastr.row(0, named=True) if cfbfastr.height else {}
    home, away = row.get("home"), row.get("away")
    if home and away:
        return home, away
    if linescore.height and {"team", "home_away"} <= set(linescore.columns):
        by_side = {r["home_away"]: r["team"] for r in linescore.to_dicts() if r.get("team")}
        return by_side.get("home", home), by_side.get("away", away)
    return home, away


def _clock(minutes: Optional[int], seconds: Optional[int]) -> str:
    """``(7, 12)`` -> ``"7:12"``. NCAA text states a clock only where the page prints one."""
    if minutes is None and seconds is None:
        return "0:00"
    return f"{int(minutes or 0)}:{int(seconds or 0):02d}"


def _down_distance_text(row: Mapping[str, Any]) -> Optional[str]:
    """``"2nd & Goal at UK 3"`` -- the only ``downDistanceText`` the processor reads (goal test)."""
    down = row.get("down")
    if not down:
        return None
    label = _ORDINAL.get(int(down), f"{int(down)}th")
    togo = "Goal" if row.get("Goal_To_Go") else str(int(row.get("distance") or 0))
    spot = row.get("yard_line")
    return f"{label} & {togo}" + (f" at {spot}" if spot else "")


def _point_after(row: Mapping[str, Any]) -> Dict[str, Any]:
    """ESPN's ``pointAfterAttempt`` for one NCAA try row (``abbreviation`` + ``value`` are read)."""
    label = row.get("play_type")
    two_point = label in ("Two Point Pass", "Two Point Rush")
    good = bool(row.get("score_pts")) and int(row.get("score_pts") or 0) != 0
    if two_point:
        text = "Two Point Conversion Good" if good else "Two Point Attempt Failed"
        return {"id": 62 if good else 63, "text": text, "abbreviation": text, "value": 2 if good else 0}
    good = label == "Extra Point Good"
    text = "Extra Point Good" if good else "Extra Point Missed"
    return {"id": 61 if good else 64, "text": text, "abbreviation": text, "value": 1 if good else 0}


def _trailing_end(play: Dict[str, Any]) -> Dict[str, Any]:
    """The end state of a play the feed states no next snap for, derived from its own yardage.

    Fires on the last row of every game and on the newest row of every truncated (in-progress)
    payload -- the row Game on Paper renders at the top of the page. Taking the play's own start
    would say the ball never moved, so a 19-yard gain would be scored as a 0-yard one against a
    spot no feed reports. The NCAA parse usually states the end spot itself
    (``yards_to_goal_end``, lifted from the play text's "to the ALA31"); where it does not, a
    scrimmage snap's end is ``start - yards gained``, and a kick or a turnover -- where the
    yardage says nothing about where the next team starts -- keeps the start spot.
    """
    start = play["start"]
    to_endzone = play.pop("_yards_to_goal_end", None)
    if to_endzone is None:
        gained = int(play.get("statYardage") or 0) if play["type"]["text"] in _KEEPS_THE_BALL else 0
        to_endzone = start["yardsToEndzone"]
        if to_endzone is not None and gained:
            to_endzone = max(0, min(100, int(to_endzone) - gained))
    return {
        "down": start["down"],
        "distance": start["distance"],
        "yardLine": _yard_line(to_endzone, play["_end_is_home"]),
        "yardsToEndzone": to_endzone,
        "team": {"id": start["team"]["id"]},
    }


def _yard_line(to_endzone: Optional[int], is_home: bool) -> Optional[int]:
    """ESPN's absolute yard line (0 = away end zone, 100 = home end zone)."""
    if to_endzone is None:
        return None
    return (100 - int(to_endzone)) if is_home else int(to_endzone)


def _kickoff_frame(plays: List[Dict[str, Any]], home_id: str, away_id: str) -> int:
    """Re-frame every kickoff row onto the **kicking** team, ESPN's convention. Returns the count.

    ESPN's raw summary puts a kickoff's ``start.team.id`` and yard line on the team that kicked
    -- its own 35, absolute yard line 35 when it is the home club -- and ``CFBPlayProcess`` then
    derives ``pos_team`` as the *return* team from it (``kicking_team`` / ``return_team``).
    ``to_cfbfastr`` states the same spot (``yards_to_goal`` 65, measured in the kicker's
    direction, which is the C15 convention) but leaves ``pos_team`` as the **drive's** team,
    which on a stats.ncaa.org page is whichever club's drive the row was printed under. Passing
    that through unchanged made the processor read the kicking team backwards on **50 of 52**
    kickoff rows over the first six Stage 2 gate games -- every kickoff's EPA, and the
    ``pos_team_receives_2H_kickoff`` flag that feeds win probability.

    The kicking team is derived, never assumed: it is the club that does **not** have the ball
    when play resumes -- the next scrimmage snap's possession -- except on a return touchdown,
    where the feed names the scorer. A kickoff with neither (the last row of a truncated page)
    keeps what the mapper said.
    """
    sides = (str(home_id), str(away_id))

    def other(team: Any) -> Optional[str]:
        return next((t for t in sides if t != str(team)), None)

    reframed = 0
    for i, play in enumerate(plays):
        label = play["type"]["text"]
        if not label.startswith("Kickoff"):
            continue
        if label == "Kickoff Return Touchdown" and play["_scoring_team"]:
            kick_team = other(play["_scoring_team"])
        elif label == "Kickoff Team Fumble Recovery Touchdown" and play["_scoring_team"]:
            kick_team = str(play["_scoring_team"])
        else:
            snap = next((p for p in plays[i + 1 :] if (p["start"]["down"] or 0) >= 1), None)
            kick_team = other(snap["start"]["team"]["id"]) if snap else None
        if not kick_team or kick_team == str(play["start"]["team"]["id"]):
            continue
        play["start"]["team"]["id"] = kick_team
        play["start"]["yardLine"] = _yard_line(play["start"]["yardsToEndzone"], kick_team == str(home_id))
        reframed += 1
    return reframed


def _fill_end_state(plays: List[Dict[str, Any]], home_id: str) -> None:
    """Fill every play's ``end`` from its own end spot and the **next** snap, across drives.

    ``yards_to_goal_end`` already answers "where did the ball finish, in whose frame" -- the
    mapper measures it for the team holding the ball *after* the play, flipping it on a change
    of possession and setting it to 0 on a touchdown, which is cfbfastR's convention and ESPN's.
    So the end **spot** comes from the play itself; only ``down``/``distance`` (which the NCAA
    page never states for an end state) are read off the next real snap, exactly as ESPN builds
    its own -- and the search crosses drive boundaries, because a punt ends in the receiving
    team's frame on the first row of the next drive.

    A **touchdown** takes ESPN's scoring end state: ``down -1``, ``distance 0``,
    ``yardsToEndzone 0``, credited to **the team the feed says scored**. That is the defence on a
    pick six or a blocked-kick return -- ``score_pts`` is signed relative to the team in
    possession, so a negative value names the defence, and crediting possession instead would
    flip the spot 100 yards.
    """
    for i, play in enumerate(plays):
        nxt = next((p for p in plays[i + 1 :] if p["type"]["text"] not in _STOPPAGE), None)
        if play["_touchdown"]:
            team = play["_scoring_team"] or play["start"]["team"]["id"]
            play["end"] = {
                "down": -1,
                "distance": -1,
                "yardLine": 100 if str(team) == str(home_id) else 0,
                "yardsToEndzone": 0,
                "team": {"id": team},
            }
        elif nxt is None:
            play["end"] = _trailing_end(play)
        else:
            to_endzone = play.pop("_yards_to_goal_end", None)
            end_team = nxt["start"]["team"]["id"] or play["start"]["team"]["id"]
            if to_endzone is None:
                to_endzone = nxt["start"]["yardsToEndzone"]
            down, distance = _FIXED_END_DOWN.get(
                play["type"]["text"], (nxt["start"]["down"], nxt["start"]["distance"])
            )
            play["end"] = {
                "down": down,
                "distance": distance,
                "yardLine": _yard_line(to_endzone, str(end_team) == str(home_id)),
                "yardsToEndzone": to_endzone,
                "team": {"id": end_team},
            }
        play.pop("_yards_to_goal_end", None)
        for key in ("_touchdown", "_scoring_team", "_end_is_home"):
            play.pop(key, None)


def _competitor(
    side: str, order: int, espn_team_id: str, school: Optional[str], full_name: Optional[str], score: Any, winner: bool
) -> Dict[str, Any]:
    """One ``header.competitions[0].competitors[]`` entry, home first.

    ``team.name`` is the mascot, and it must not be empty: the processor charges a ``Timeout``
    row to whichever club's name parts the text contains (longest match wins), and ``""`` is
    contained in every string, so an empty mascot charges every timeout to both clubs. The
    contest page's own team link states ``"{school} {mascot}"`` ("Florida St. Seminoles"), so the
    mascot is the label minus the linescore's school name -- a real token from the payload. A
    page that links no label falls back to the school itself rather than inventing one.
    """
    label = str(school or "").strip() or f"Team {espn_team_id}"
    full = str(full_name or "").strip() or label
    mascot = full[len(label) :].strip() if full.startswith(label) else ""
    mascot = mascot or label
    return {
        "id": str(espn_team_id),
        "uid": f"s:20~l:23~t:{espn_team_id}",
        "order": order,
        "homeAway": side,
        "winner": bool(winner),
        "team": {
            "id": str(espn_team_id),
            "uid": f"s:20~l:23~t:{espn_team_id}",
            "location": label,
            "name": mascot,
            "nickname": full,
            "abbreviation": label[:4].upper(),
            "displayName": full,
            "shortDisplayName": label,
            "color": None,
            "alternateColor": None,
            "logos": [],
        },
        "score": str(score if score is not None else 0),
        "linescores": [],
        "record": [],
    }


def _status(final: bool, period: Optional[int], clock: str) -> Dict[str, Any]:
    """``header.competitions[0].status``; stats.ncaa.org states no status, so it is read by shape."""
    period = int(period or 0)
    if final:
        detail = "Final" if period <= 4 else "Final/OT"
        return {
            "clock": 0.0,
            "displayClock": "0:00",
            "period": period,
            "type": {
                "id": "3",
                "name": "STATUS_FINAL",
                "state": "post",
                "completed": True,
                "description": "Final",
                "detail": detail,
                "shortDetail": detail,
            },
        }
    detail = f"{clock} - {_ORDINAL.get(period, str(period))}" if period <= 4 else f"{clock} - OT"
    return {
        "clock": 0.0,
        "displayClock": clock,
        "period": period,
        "type": {
            "id": "2",
            "name": "STATUS_IN_PROGRESS",
            "state": "in",
            "completed": False,
            "description": "In Progress",
            "detail": detail,
            "shortDetail": detail,
        },
    }


def _pickcenter(odds: Optional[Mapping[str, Any]]) -> List[Dict[str, Any]]:
    """The stored closing line as a one-provider ``pickcenter`` (NCAA carries no odds at all)."""
    if not odds or odds.get("gameSpread") is None or odds.get("overUnder") is None:
        return []
    return [
        {
            "provider": {"id": "0", "name": "stored closing line", "priority": 0},
            "spread": abs(float(odds["gameSpread"])),
            "overUnder": float(odds["overUnder"]),
            "homeTeamOdds": {"favorite": bool(odds.get("homeFavorite"))},
            "awayTeamOdds": {"favorite": not bool(odds.get("homeFavorite"))},
        }
    ]


def _drive(event_id: str, index: int, meta: Mapping[str, Any], plays: Sequence[Dict[str, Any]]) -> Dict[str, Any]:
    """One ``drives.previous[]`` / ``drives.current`` entry; ESPN's drive id is ``{event}{index}``."""
    result = meta.get("result")
    label = meta.get("team")
    # ESPN's drive.yards is the drive's NET yards, and the processor SUMS it, so it has to be a
    # number on every drive: a column of nulls arrives as Utf8 and the box score raises.
    start_ytg, end = plays[0]["start"]["yardsToEndzone"], plays[-1]["end"]
    same_team = str(end["team"]["id"]) == str(plays[0]["start"]["team"]["id"])
    end_ytg = end["yardsToEndzone"] if same_team else plays[-1]["start"]["yardsToEndzone"]
    yards = int(start_ytg) - int(end_ytg) if None not in (start_ytg, end_ytg) else 0
    return {
        "id": f"{event_id}{index}",
        "description": f"{len(plays)} plays, {yards} yards",
        "team": {
            "shortDisplayName": label,
            "displayName": label,
            "name": label,
            "abbreviation": label,
        },
        "start": {
            "period": {"number": plays[0]["period"]["number"], "type": "quarter"},
            "clock": {"displayValue": plays[0]["clock"]["displayValue"]},
            "yardLine": plays[0]["start"]["yardLine"],
            "text": plays[0]["start"].get("downDistanceText"),
        },
        "end": {
            "period": {"number": plays[-1]["period"]["number"], "type": "quarter"},
            "clock": {"displayValue": plays[-1]["clock"]["displayValue"]},
            "yardLine": plays[-1]["end"]["yardLine"],
        },
        "timeElapsed": {"displayValue": None},
        "yards": yards,
        "isScore": bool(meta.get("scoring")),
        "offensivePlays": len(plays),
        "result": result,
        "shortDisplayResult": result,
        "displayResult": result,
        "plays": list(plays),
    }


def _header(
    event_id: str,
    season: Optional[int],
    week: Optional[int],
    home_id: str,
    away_id: str,
    home_name: Optional[str],
    away_name: Optional[str],
    full_names: Mapping[str, Optional[str]],
    home_score: int,
    away_score: int,
    final: bool,
    period: Optional[int],
    clock: str,
    kickoff: Optional[str],
) -> Dict[str, Any]:
    """The renderable ``header``: season / week / competitors / status."""
    return {
        "id": event_id,
        "uid": f"s:20~l:23~e:{event_id}",
        "season": {"year": season, "type": 2},
        "week": week,
        "timeValid": True,
        "competitions": [
            {
                "id": event_id,
                "uid": f"s:20~l:23~e:{event_id}~c:{event_id}",
                "date": kickoff,
                "neutralSite": False,
                "conferenceCompetition": False,
                "boxscoreAvailable": False,
                "commentaryAvailable": False,
                "liveAvailable": False,
                "onWatchESPN": False,
                "recent": False,
                "boxscoreSource": "none",
                "playByPlaySource": "full",
                "status": _status(final, period, clock),
                "competitors": [
                    _competitor(
                        "home",
                        0,
                        home_id,
                        home_name,
                        full_names.get("home"),
                        home_score,
                        final and home_score > away_score,
                    ),
                    _competitor(
                        "away",
                        1,
                        away_id,
                        away_name,
                        full_names.get("away"),
                        away_score,
                        final and away_score > home_score,
                    ),
                ],
            }
        ],
    }


def _ncaa_to_espn_summary(
    bundle: Mapping[str, Any],
    idmap_row: Mapping[str, Any],
    *,
    odds: Optional[Mapping[str, Any]] = None,
    team_ids: Optional[Mapping[str, str]] = None,
) -> Tuple[Dict[str, Any], List[str]]:
    """Project one stats.ncaa.org contest (final or truncated) onto an ESPN-summary-shaped dict.

    Args:
        bundle: the contest's ``play_by_play`` / ``box_score`` / ``drives`` HTML pages (an archive
            payload, or :func:`...fetch._fetch_bundle`'s live equivalent).
        idmap_row: the game's id-map row
            (:data:`sportsdataverse.football.sources.idmap.GAME_SCHEMA`). ``espn_event_id`` is
            required; ``home_espn_team_id`` / ``away_espn_team_id`` are used when present and
            otherwise come from ``team_ids``. ``CFBPlayProcess`` casts ``team.id`` to ``int`` and
            uses it for logos, possession and the box score, so they must be ESPN's.
        odds: ``{gameSpread, overUnder, homeFavorite, gameSpreadAvailable}`` (a stored closing
            line) -- NCAA states no odds of its own.
        team_ids: ``{"home"/"away": espn_team_id}`` resolved from the vendored crosswalk or
            ESPN's schedule, used for whichever side the id-map row does not carry.

    Returns:
        ``(summary, notes)``.

        | item | type | description |
        |---|---|---|
        | summary | dict | An ESPN-summary-shaped payload: `header`, `drives.previous` (+ `drives.current` on a truncated page), `gameInfo`, `pickcenter` and empty `boxscore` / passthrough arrays. Feed it to `espn_cfb_pbp(summary=)`. |
        | notes | list[str] | Adapter-side degradations worth surfacing in provenance: synthesized overtime rows, a try with no touchdown to fold into, rows whose possession was carried forward, and the open drive synthesized for a truncated page. |

    Raises:
        KeyError: ``idmap_row`` states no ``espn_event_id``.
        ValueError: the bundle carries no play-by-play, or the ESPN team ids for both sides
            could not be resolved (possession could not be attributed to either club).

    Example:
        Adapt an archive payload and process it::

            import gzip, json
            from sportsdataverse.cfb import CFBPlayProcess
            from sportsdataverse.cfb.ncaa_pbp.to_espn_summary import _ncaa_to_espn_summary

            with gzip.open("mfb/raw/2026/6400689.json.gz", "rt") as fh:
                bundle = json.load(fh)
            row = {"espn_event_id": "401752665", "season": 2025,
                   "home_espn_team_id": "52", "away_espn_team_id": "333"}
            summary, notes = _ncaa_to_espn_summary(bundle, row)
            proc = CFBPlayProcess(gameId=401752665, join_participants=False)
            proc.espn_cfb_pbp(summary=summary)
            result = proc.run_processing_pipeline()
    """
    event_id = str(idmap_row["espn_event_id"])
    season = idmap_row.get("season")
    cfbfastr, pbp, linescore, drive_titles = _parse_bundle(bundle, int(season) if season is not None else None)
    if not cfbfastr.height:
        raise ValueError("bundle carries no play-by-play")

    home_name, away_name = _sides(cfbfastr, linescore)
    resolved = dict(team_ids or {})
    home_id = str(idmap_row.get("home_espn_team_id") or resolved.get("home") or "") or None
    away_id = str(idmap_row.get("away_espn_team_id") or resolved.get("away") or "") or None
    if not home_id or not away_id or home_id == away_id:
        # A null team id must not reach the frame: the processor forward-fills possession and
        # floats the whole column, and with both sides equal every play is credited to one club
        # -- silently, since the contract's all-null test only fires at a null rate of 1.0.
        raise ValueError("no ESPN team ids for both sides: possession cannot be attributed")

    notes: List[str] = []
    final = _is_final(pbp)
    # "{school} {mascot}" off the page's own team links, matched to the linescore's school name
    full_names = {
        side: next(
            (label for label in _ncaa_team_ids(bundle.get("play_by_play") or "") if name and label.startswith(name)),
            name,
        )
        for side, name in (("home", home_name), ("away", away_name))
    }
    home_key, away_key = _norm(home_name), _norm(away_name)
    id_by_team = {home_key: home_id, away_key: away_id}
    name_by_id = {home_id: home_name, away_id: away_name}

    rows = cfbfastr.to_dicts()
    clocks, carried_clocks = _clocks(rows, drive_titles)
    if carried_clocks:
        notes.append(
            f"{carried_clocks} of {len(rows)} plays state no clock on the page: each takes its drive's "
            "stated start clock, then the last clock stated (the clock is an EP/WP input)"
        )
    if any(r.get("ot_synthesized") for r in rows):
        notes.append(
            "overtime is synthesized: stats.ncaa.org pbp pages omit OT drives, so one row per OT "
            "drive is rebuilt from the drives tab with no down or distance"
        )

    emitted: List[Dict[str, Any]] = []
    drives: List[Tuple[Dict[str, Any], List[Dict[str, Any]]]] = []
    last_touchdown: Optional[int] = None
    last_team: Optional[str] = None
    last_home_score, last_away_score = 0, 0
    carried = 0
    for index, row in enumerate(rows):
        label = row.get("play_type") or "Unknown"
        pos = id_by_team.get(_norm(row.get("pos_team")))
        if pos is None:
            # a drive title the linescore spells differently, or a row the page left unattributed:
            # carry the previous row's possession rather than emitting a null, which the processor
            # forward-fills anyway -- but on the way nulls start.yardLine and floats the column.
            pos = last_team
            carried += 1
        last_team = pos or last_team
        pos_is_home = pos == home_id
        pos_score, def_score = row.get("pos_team_score"), row.get("def_pos_team_score")
        home_score = pos_score if pos_is_home else def_score
        away_score = def_score if pos_is_home else pos_score
        home_score = last_home_score if home_score is None else int(home_score)
        away_score = last_away_score if away_score is None else int(away_score)

        if label in _TRY_TYPES:
            # ESPN folds the try into its touchdown: same play id, one text, one score step. The
            # try is NOT always the row right after the touchdown -- a timeout, a penalty on the
            # try or a replay review sits between them -- so this anchors on the newest touchdown
            # rather than on ``emitted[-1]``, which would hang ``pointAfterAttempt`` off a Timeout
            # row and leave the touchdown stepping the scoreboard by 6.
            if last_touchdown is None:
                notes.append(f"{label} with no touchdown to fold into (play {len(emitted) + 1})")
                continue
            target = emitted[last_touchdown]
            text = row.get("play_text") or ""
            if text and text not in target["text"]:
                target["text"] = f"{target['text']} ({text})"
            target["pointAfterAttempt"] = _point_after(row)
            for later in emitted[last_touchdown:]:
                later["homeScore"], later["awayScore"] = home_score, away_score
            last_home_score, last_away_score = home_score, away_score
            continue

        last_home_score, last_away_score = home_score, away_score
        type_id, abbreviation = _TYPE_BY_TEXT.get(label, (None, None))
        to_endzone = row.get("yards_to_goal")
        touchdown = bool(row.get("touchdown"))
        # ``score_pts`` is signed relative to the team in possession, so a negative value on a
        # touchdown names the DEFENCE as the scorer (a pick six, a blocked-kick return).
        scoring_team = None
        if touchdown:
            points = int(row.get("score_pts") or 0)
            scoring_team = pos if points >= 0 else (away_id if pos_is_home else home_id)
        play: Dict[str, Any] = {
            "id": f"{event_id}{len(emitted) + 1:04d}",
            "sequenceNumber": str(len(emitted) + 1),
            "type": {"id": type_id, "text": label, "abbreviation": abbreviation},
            "text": row.get("play_text") or "",
            "awayScore": away_score,
            "homeScore": home_score,
            "period": {"number": row.get("period")},
            "clock": {"displayValue": clocks[index]},
            "scoringPlay": bool(row.get("scoring_play")),
            "priority": bool(row.get("scoring_play")),
            "statYardage": int(row.get("yards_gained") or 0),
            "start": {
                "down": int(row.get("down") or 0),
                "distance": int(row.get("distance") or 0),
                "yardLine": _yard_line(to_endzone, pos_is_home),
                "yardsToEndzone": to_endzone,
                "downDistanceText": _down_distance_text(row),
                "team": {"id": pos},
            },
            "end": {},
            "_yards_to_goal_end": row.get("yards_to_goal_end"),
            "_touchdown": touchdown,
            "_scoring_team": scoring_team,
            "_end_is_home": pos_is_home,
        }
        emitted.append(play)
        if touchdown:
            last_touchdown = len(emitted) - 1
        meta = {
            "number": row.get("drive_number"),
            "team": (name_by_id.get(pos) if pos else None) or row.get("pos_team"),
            "result": row.get("drive_result"),
            "scoring": row.get("drive_scoring"),
        }
        if drives and drives[-1][0]["number"] == meta["number"]:
            drives[-1][1].append(play)
        else:
            drives.append((meta, [play]))

    if carried:
        notes.append(f"{carried} plays name a team the linescore does not: possession carried from the preceding play")
    reframed = _kickoff_frame(emitted, home_id, away_id)
    if reframed:
        notes.append(f"{reframed} kickoff rows were re-framed onto the kicking team (ESPN's convention)")
    _fill_end_state(emitted, home_id)

    previous = [_drive(event_id, i, meta, plays) for i, (meta, plays) in enumerate(drives, start=1) if plays]
    current: Optional[Dict[str, Any]] = None
    if previous and not final:
        # a truncated page's last drive is still open: it becomes ``drives.current`` so Game on
        # Paper renders it as the current drive and the processor still sees its plays. It has not
        # ended, so it states no outcome, whatever the page's drive title says.
        current = previous.pop()
        current["result"] = current["shortDisplayResult"] = current["displayResult"] = "In Progress"
        current["isScore"] = False
        notes.append("the page does not reach the end of the game: the open drive was moved to drives.current")

    last_play = emitted[-1] if emitted else {}
    period = (last_play.get("period") or {}).get("number")
    clock = (last_play.get("clock") or {}).get("displayValue") or "0:00"
    home_score = int(last_play.get("homeScore") or 0)
    away_score = int(last_play.get("awayScore") or 0)
    venue = linescore.row(0, named=True) if linescore.height else {}
    return {
        "boxscore": {"teams": [], "players": []},
        "format": {"regulation": {"periods": 4}},
        "gameInfo": {"venue": {"fullName": venue.get("venue")}, "attendance": venue.get("attendance")},
        "drives": {"previous": previous, **({"current": current} if current else {})},
        "leaders": [],
        "broadcasts": [],
        "predictor": {},
        "pickcenter": _pickcenter(odds),
        "againstTheSpread": [],
        "odds": [],
        "winprobability": [],
        "header": _header(
            event_id,
            int(season) if season is not None else None,
            idmap_row.get("week"),
            home_id,
            away_id,
            home_name,
            away_name,
            full_names,
            home_score,
            away_score,
            final,
            period,
            clock,
            venue.get("game_date"),
        ),
        "scoringPlays": [],
        "videos": [],
        "standings": {},
    }, notes


def _ncaa_adapter(league: str, espn_id: int, ctx: Any) -> Any:
    """Dispatch adapter for ``source="ncaa"`` (CFB). Registered in :mod:`...sources.dispatch`.

    Hands over to the next source (:class:`...dispatch.SourceUnavailable`) when the contest id
    cannot be resolved without inventing one, when neither the archive nor a live fetch produces
    a bundle, when the bundle carries no play-by-play markup at all -- the shape stats.ncaa.org
    answers HTTP 200 with for a game it does not hold, including every FBS game outside the
    FBS/FCS divisions it sweeps -- and when no ESPN team id can be resolved for both clubs.
    """
    from sportsdataverse.football.sources.dispatch import AdaptedGame, SourceUnavailable
    from sportsdataverse.football.sources.idmap import _odds_override_from_row

    from sportsdataverse.cfb.ncaa_pbp.fetch import _archive_bundle, _fetch_bundle, _resolve_contest_id

    row = dict(ctx.idmap_row or {})
    row.setdefault("espn_event_id", str(espn_id))
    bundle = ctx.payload
    contest_id, resolved_by = _resolve_contest_id(ctx.idmap_row)
    if bundle is None:
        if not contest_id:
            raise SourceUnavailable(f"cfb {espn_id}: no ncaa_game_id (stats.ncaa.org contest id) in the id map")
        season = row.get("season")
        bundle = _archive_bundle(contest_id, season=int(season) if season is not None else None)
        source = "archive"
        if bundle is None:
            try:
                bundle = _fetch_bundle(contest_id)
                source = "live"
            except Exception as exc:  # noqa: BLE001 -- network / browser / proxy -> the next source
                raise SourceUnavailable(
                    f"cfb {espn_id}: stats.ncaa.org contest {contest_id} is not in the archive and the "
                    f"live fetch failed: {type(exc).__name__}: {exc}"
                ) from exc
    else:
        source = "payload"
    if not isinstance(bundle, Mapping):
        raise SourceUnavailable(f"cfb {espn_id}: ncaa payload is a {type(bundle).__name__}, not a contest bundle")
    if not _has_plays(bundle):
        # checked BEFORE the id-map fields: "NCAA does not carry this game" is a fact about the
        # source and is true whatever the row holds, and it is the message the fall-through log
        # should carry.
        raise SourceUnavailable(
            f"cfb {espn_id}: stats.ncaa.org contest {contest_id or bundle.get('contest_id')} carries no play-by-play"
        )

    team_ids: Dict[str, str] = {}
    if not (row.get("home_espn_team_id") and row.get("away_espn_team_id")):
        from sportsdataverse.cfb.cfb_ncaa_box import parse_cfb_ncaa_linescore

        linescore = parse_cfb_ncaa_linescore(bundle.get("box_score") or "")
        sides = {r["team"]: r["home_away"] for r in linescore.to_dicts() if r.get("team")}
        team_ids = _espn_team_ids_from_bundle(bundle.get("play_by_play") or "", sides)
        if ctx.payload is None and len(team_ids) < 2:
            # last leg, and network-bound: ESPN's own schedule for the game's date. Skipped on the
            # injected-payload path, which is what keeps an offline replay offline.
            from sportsdataverse.cfb.ncaa_pbp.fetch import _espn_team_ids_from_schedule

            date = linescore.row(0, named=True).get("game_date") if linescore.height else None
            team_ids = {**_espn_team_ids_from_schedule(espn_id, date), **team_ids}
        if len(team_ids) < 2:
            raise SourceUnavailable(f"cfb {espn_id}: no ESPN team id for both clubs (id map, crosswalk, schedule)")
        resolved_by = f"{resolved_by}+crosswalk" if resolved_by != "unresolved" else "crosswalk"

    odds = ctx.odds_override or _odds_override_from_row(ctx.idmap_row)
    try:
        summary, notes = _ncaa_to_espn_summary(bundle, row, odds=odds, team_ids=team_ids)
    except ValueError as exc:
        raise SourceUnavailable(f"cfb {espn_id}: {exc}") from exc
    served = summary["drives"]["previous"] + [d for d in (summary["drives"].get("current"),) if d]
    if not any(d.get("plays") for d in served):
        # checked over BOTH groupings, since a truncated page's only drive is the open one, which
        # lives under ``current``; the contract would otherwise fail on ``drives[].plays[]``.
        raise SourceUnavailable(f"cfb {espn_id}: ncaa drive chart carries no plays")
    return AdaptedGame(
        summary=summary,
        participants=ctx.participants,
        odds_override=ctx.odds_override,
        native_ids={
            "espn_event_id": str(espn_id),
            "ncaa_contest_id": contest_id or bundle.get("contest_id"),
            "ncaa_id_resolved_by": resolved_by,
            "ncaa_bundle_source": source,
            "ncaa_captured_at": bundle.get("captured_at"),
        },
        notes=notes,
    )


def _register_ncaa() -> None:
    """Register :func:`_ncaa_adapter` with the dispatcher (called on import)."""
    from sportsdataverse.football.sources.dispatch import _register

    _register("cfb", "ncaa")(_ncaa_adapter)


_register_ncaa()
