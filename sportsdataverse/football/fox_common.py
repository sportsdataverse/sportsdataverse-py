"""Fox Sports (Bifrost) ``{sport}/event/{id}/data`` -> an ESPN-summary-shaped dict (private, experimental).

One module for both leagues: Fox's NFL and college-football feeds are the *same* Bifrost
product with a different sport slug, and the two things that actually decide the numbers --
the play geometry and the drive grouping -- are identical in both. ``sportsdataverse.nfl.fox_pbp``
and ``sportsdataverse.cfb.fox_pbp`` are thin: a play-type table, an id resolver and the
dispatch registration.

Fox is **last** in both failover orders (``...sources.dispatch.SOURCE_ORDER``). Its value is
that it shares an upstream with neither GSIS nor ESPN, plus a closing line; its cost is that
its play text is a Stats-Perform grammar, not GSIS, so most player-name columns degrade
(:data:`...contract.KNOWN_LOSSY`). **Team-level fidelity is the bar** -- game state, EP, EPA,
WP, drives and the team box -- not player attribution.

What this module owns, and why each piece is not optional:

* **Geometry.** ``modalPlay.play.events[].yardStart/yardEnd`` are **absolute** 0-100 positions
  measured from the **away** team's goal line, fixed for the whole game (no quarter flip); the
  end zones sit at -5 and 105. So ``yardsToEndzone = 100 - pos`` when the away team has the
  ball and ``pos`` when the home team does, and ``yardLine = 100 - pos`` either way. Reading
  ``yardStart`` *as* yards-to-goal -- what the shipped ``cfb.cfb_pbp_fox`` does -- mirrors the
  field on every away-offense play: 50.0%/53.7% of spots right and EP_start r 0.16/0.53 on the
  two 2026 games measured (``2026-09-16-cfb-alt-sources/B_fox.md`` §7).
* **Possession from the drive, never from the play's image.** ``play.image.altText`` names the
  team *credited* with the play (the sacker, the interceptor, the penalised club) in CFB and a
  *player headshot* in NFL. The drive group's ``entityLink.contentUri`` is the team with the
  ball -- the **receiving** team on a kickoff, which is why ``start.team`` is flipped there to
  ESPN's kicking-team convention.
* **Drive quarter re-attribution.** Fox files a drive group under the quarter it **ends** in,
  and admin rows carry no ``periodOfPlay`` at all. Taking the section's quarter put
  "End Quarter 1" in Q2, which made the lagged end clock 0 and forced ``end.yardsToEndzone``
  to 99 (``nfl_pbp.py:4500``) on the first play of Q2 and Q4 (ΔEPA 1.6 and 2.9).
* **Newest-first live order.** Until ``eventStatus == 3`` Fox serves the whole ``pbp`` tree
  **reversed** -- sections, groups and plays all descending (measured over 291 live snapshots
  of DET @ BUF, ``2026-09-16-nfl-alt-sources/fox_scratch/live_11052/``). Fox play ids are a
  single ascending 1..N sequence over the game, so :func:`_flatten` sorts on them and both
  orders collapse to the same frame.
* **Period-end rows carry down/distance 0.** Copying the preceding snap's state makes
  ``NFLPlayProcess``'s duplicate-text filter (``nfl_pbp.py:689``,
  ``text.is_in(lead_text.implode())``) silently **drop that snap** -- the final kneel and a
  3rd-down incompletion vanished before this was fixed.
* **The PAT is its own Fox row** and is folded into the touchdown it belongs to, anchored on
  the newest touchdown rather than on the preceding row: a timeout or a penalty on the try
  sits between them often enough that ``emitted[-1]`` hangs ``pointAfterAttempt`` off a
  stoppage and leaves the touchdown stepping the scoreboard by 6.
* **"TV Timeout" is not a charged timeout.** Fox distinguishes it from a team ``Timeout`` row
  (whose text names the club), so it maps to ESPN's ``Official Timeout``.

Documented divergences from a real ESPN summary:

* **No ESPN or GSIS play ids.** ``modalPlay.play.id`` is Fox's own 1..N numbering, so the
  emitted id is ``{espn_event_id}{fox play id:04d}``: stable, unique and increasing, joining
  nothing ESPN-sourced. Parity has to join on game state.
* **No air yards / YAC**, so ``cp`` / ``cpoe`` are null, and the play text is a different
  grammar, so rusher / receiver / sacker / returner names are mostly null.
* ``boxscore`` is empty: ESPN's box is the only source of ESPN athlete ids.
* **Coverage floors**: NFL play-by-play from the **2024** season, CFB **2022** full (2021
  early-season only), and **zero** FCS-hosted games in any era. Fox answers HTTP 200 with the
  ``pbp`` key simply **absent** in all three cases, which is why coverage is detected by shape
  (:func:`_has_pbp`) and never by status code.
"""

from __future__ import annotations

import re
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

#: Fox section / ``periodOfPlay`` labels -> period number.
_PERIODS: Dict[str, int] = {
    "1ST": 1,
    "2ND": 2,
    "3RD": 3,
    "4TH": 4,
    "OT": 5,
    "1ST QUARTER": 1,
    "2ND QUARTER": 2,
    "3RD QUARTER": 3,
    "4TH QUARTER": 4,
    "OVERTIME": 5,
    "1ST OT": 5,
    "2ND OT": 6,
    "3RD OT": 7,
    "4TH OT": 8,
}

#: ESPN ``type.text`` values that stop the clock rather than describing a snap. The end state
#: of the play *before* one of these is the next real snap's spot, not the stoppage row's.
_STOPPAGE = frozenset({"Timeout", "Official Timeout", "Two-minute warning", "End Period", "End of Half", "End of Game"})

#: The two stoppages the end-state search must **stop** at rather than read through. The snap
#: after one of them is the next half's kickoff, in the other team's frame: reading through it
#: gave the clock-killing kneel at the end of a half an end spot of 65 yards to the goal, in
#: the receiving team's frame, on 33 of the 41 halves in the evidence capture -- ESPN's own
#: summaries carry the next-half snap's spot on only 3 of those 41.
_HALF_BOUNDARY = frozenset({"End of Half", "End of Game"})

#: ESPN type ids on which the offence keeps the ball where it left it, so a play with no next
#: snap (the last row of a final, the newest row of every live poll) still has a derivable end
#: spot: ``start - statYardage``. On a turnover or a kick the yardage says nothing about where
#: the next team starts, so those keep their own start rather than a guess.
_KEEPS_THE_BALL = frozenset({"3", "5", "7", "24"})

#: ESPN types whose end state is the goal line the **scoring** team was attacking.
_TOUCHDOWN_TYPES = frozenset({"32", "34", "36", "37", "38", "39", "67", "68"})

#: Types that hand the ball to the other club *inside their own drive group*. Fox occasionally
#: files the receiving team's first row (a pre-snap penalty, a replay review) under the drive
#: that just ended, and the group's ``entityLink`` still names the team that lost the ball --
#: so that row, and through it the punt's end state, comes out in the wrong frame (measured:
#: the punt before it read 1 yard to the end zone instead of 99, 6.1 EPA). A drive group is one
#: possession, so anything after its possession-ending play belongs to the other side.
#: Kickoffs are excluded: their group already names the *receiving* team.
_CHANGES_POSSESSION = frozenset({"17", "18", "26", "29", "30", "52", "60"})


#: A touchdown the officials took off the board; its text still says TOUCHDOWN.
_NULLIFIED_RE = re.compile(r"(?i)\bnullified\b|\boverturned\b|\bno play\b|\breversed\b")
_END_QUARTER_RE = re.compile(r"(?i)end\s+(?:of\s+)?quarter\s*(\d)")
_END_OVERTIME_RE = re.compile(r"(?i)end\s+(?:of\s+)?(?:the\s+)?overtime")
_RECOVERED_RE = re.compile(r"RECOVERED by ([A-Z]{2,4})[- ]")
_PENALTY_RE = re.compile(
    r"PENALTY on ([A-Z]{2,4})(?:-[^,]+)?, ([^,]+), (\d+) yards?, (accepted|declined|offsetting)", re.I
)
_FG_YARDS_RE = re.compile(r"(?i)\b(\d{1,2})[- ]yard field goal")
#: ``"12 plays, -7 yards, 5:01"`` -> plays / yards / elapsed. The separator class excludes the
#: minus sign: a greedy ``\D+`` eats it, and a drive that LOST ground then reported its yardage
#: as a gain (88 of the drives in the evidence capture state negative yards).
_DRIVE_SUBTITLE_RE = re.compile(r"\s*(-?\d+) plays?[^\d-]+(-?\d+) yards?\D+(\d+:\d+)")
_CLOCK_RE = re.compile(r"^(\d{1,2}):(\d{2})$")
_WHITESPACE_RE = re.compile(r"\s+")
_ORDINAL = {1: "1st", 2: "2nd", 3: "3rd", 4: "4th"}
_TITLE_DOWN_RE = re.compile(r"(?i)\s*(\d)(?:st|nd|rd|th)\s*&\s*(\d+|goal)")
_TITLE_SPOT_RE = re.compile(r"^([A-Z&.\s]{2,12})?\s*(\d{1,2})$")


def _clock(value: Optional[str]) -> str:
    """Fox ``"07:12"`` -> ESPN ``"7:12"``; anything unparseable becomes ``"0:00"``.

    The contract rejects a summary whose ``clock.displayValue`` is not ``MM:SS``, and the
    processor parses it into ``TimeSecsRem``, an EP/WP input on every row.
    """
    match = _CLOCK_RE.match(str(value or "").strip())
    return f"{int(match.group(1))}:{match.group(2)}" if match else "0:00"


def _text(raw: Optional[str], fixes: Mapping[str, str]) -> str:
    """One line of play text with Fox's club codes rewritten to the emitted abbreviations.

    Fox's NFL text is Stats Perform's (``JAC``, ``CLE``) while its own header says ``JAX``;
    the processor charges a timeout and attributes a penalty or a fumble recovery by matching
    the **emitted** abbreviation against this text, so a code it cannot match costs every
    timeout in the game (an EP / WP / 4th-down input) and every penalty's team.
    """
    flat = _WHITESPACE_RE.sub(" ", str(raw or "").replace("\r", " ").replace("\n", " ")).strip()
    if not fixes:
        return flat
    return re.sub(r"\b(" + "|".join(re.escape(k) for k in fixes) + r")\b", lambda m: fixes[m.group(1)], flat)


def _fox_team_id(entity: Any) -> Optional[str]:
    """The Fox team id out of a ``.../teams/{id}`` uri on a header team or a drive's entityLink."""
    if not isinstance(entity, Mapping):
        return None
    for key in ("contentUri", "uri"):
        match = re.search(r"/teams/(\d+)", str(entity.get(key) or ""))
        if match:
            return match.group(1)
    return None


def _has_pbp(fox: Any) -> bool:
    """True when the payload actually carries play-by-play.

    Fox answers **HTTP 200 with the ``pbp`` key simply absent** for a covered-but-unbackfilled
    game: every NFL game before the 2024 season, every CFB game before 2022 (2021 outside the
    early season), and **every FCS-hosted game in every era**. Detecting that by shape is the
    only way to tell "Fox does not have this game" from "the request failed" -- the shared
    ``_get`` returns ``{}`` for the latter, so a bare truthiness test conflates the two.
    """
    if not isinstance(fox, Mapping):
        return False
    sections = (fox.get("pbp") or {}).get("sections") if isinstance(fox.get("pbp"), Mapping) else None
    if not isinstance(sections, list):
        return False
    return any((g or {}).get("plays") for s in sections if isinstance(s, Mapping) for g in (s.get("groups") or []))


def _fox_event_data(sport: str, fox_event_id: Any, **kwargs: Any) -> Dict[str, Any]:
    """``GET bifrost/v1/{sport}/event/{id}/data`` -- the whole game payload."""
    from sportsdataverse._fox_layout import fox_get

    return fox_get(f"{sport}/event/{fox_event_id}/data", **kwargs)


def _fox_event_odds(sport: str, fox_event_id: Any, **kwargs: Any) -> Dict[str, Any]:
    """``GET bifrost/v1/{sport}/event/{id}/odds`` -- the six-pack, including the closing line."""
    from sportsdataverse._fox_layout import fox_get

    return fox_get(f"{sport}/event/{fox_event_id}/odds", **kwargs)


def _fox_odds_override(odds: Any, home_full_name: Optional[str]) -> Optional[Dict[str, Any]]:
    """Fox's **closing** six-pack line -> the processor's ``odds_override``, or None.

    Fox is the only alternate football source that ships a closing line with the game, which is
    half the reason it is worth keeping at the end of the failover order: without one the
    processor falls back to a 2.5 / 55.5 default that moves ``wp_before`` on every row.

    Returns None rather than a partial dict when the spread or the favourite cannot be read --
    a made-up line is worse than the documented default.
    """
    if not isinstance(odds, Mapping) or not home_full_name:
        return None
    rows = ((odds.get("sixPack") or {}).get("odds") or {}).get("rows") or []
    spread: Optional[float] = None
    total: Optional[float] = None
    home_favourite: Optional[bool] = None
    for row in rows:
        values = [(v or {}).get("odds") for v in (row or {}).get("values") or []]
        if len(values) < 3 or values[0] is None:
            continue
        try:
            line = float(values[0])
        except (TypeError, ValueError):
            continue
        if str(row.get("fullText") or "").strip().lower() == str(home_full_name).strip().lower():
            home_favourite, spread = line < 0, abs(line)
        match = re.search(r"(\d+(?:\.\d+)?)", str(values[2] or ""))
        if match:
            total = float(match.group(1))
    if spread is None or total is None or home_favourite is None:
        return None
    return {
        "gameSpread": spread,
        "overUnder": total,
        "homeFavorite": home_favourite,
        "gameSpreadAvailable": True,
    }


def _pickcenter(odds: Optional[Mapping[str, Any]]) -> List[Dict[str, Any]]:
    """The resolved line as a one-provider ``pickcenter`` array (the offline odds path)."""
    if not odds or odds.get("gameSpread") is None or odds.get("overUnder") is None:
        return []
    return [
        {
            "provider": {"id": "0", "name": "fox closing line", "priority": 0},
            "spread": abs(float(odds["gameSpread"])),
            "overUnder": float(odds["overUnder"]),
            "homeTeamOdds": {"favorite": bool(odds.get("homeFavorite"))},
            "awayTeamOdds": {"favorite": not bool(odds.get("homeFavorite"))},
        }
    ]


def _events(play: Mapping[str, Any]) -> List[Dict[str, Any]]:
    """``modalPlay.play.events[]`` -- the typed segments of one play, or ``[]`` on an admin row."""
    modal = play.get("modalPlay") if isinstance(play.get("modalPlay"), Mapping) else {}
    inner = (modal or {}).get("play") if isinstance((modal or {}).get("play"), Mapping) else {}
    events = (inner or {}).get("events")
    return [e for e in events or [] if isinstance(e, Mapping)]


def _pos(value: Any) -> Optional[int]:
    """One ``yardStart`` / ``yardEnd`` clamped into the 0-100 field (the end zones are -5/105)."""
    if value is None:
        return None
    try:
        return max(0, min(100, int(value)))
    except (TypeError, ValueError):
        return None


def _parse_title(title: Optional[str]) -> Dict[str, Any]:
    """``"1st & 10 · CLE 40"`` -> down / distance / field-position side + yard line.

    The title is the only place Fox states down and distance. ``"4th & 6 ·  50"`` (midfield)
    has no side, and a kick row is just ``"JAX 35"``.
    """
    flat = re.sub(r"[^\x00-\x7f]", "|", str(title or ""))
    down_match = _TITLE_DOWN_RE.match(flat)
    down = int(down_match.group(1)) if down_match else None
    distance = None
    if down_match:
        distance = 0 if down_match.group(2).lower() == "goal" else int(down_match.group(2))
    spot_match = _TITLE_SPOT_RE.match(flat.split("|")[-1].strip())
    return {
        "down": down,
        "distance": distance,
        "side": (spot_match.group(1) or "").strip() or None if spot_match else None,
        "yard_line": int(spot_match.group(2)) if spot_match else None,
    }


def _period_of(play: Mapping[str, Any], previous: Optional[int], section_period: Optional[int]) -> Optional[int]:
    """The period of one row, working around Fox's two period defects.

    A drive group is filed under the quarter it **ends** in and admin rows carry no
    ``periodOfPlay`` at all, so the section's title is wrong for exactly the rows that matter:
    the "End Quarter N" row of every quarter. Order of truth: the row's own ``periodOfPlay``,
    the quarter named in an "End Quarter N" text, the previous row's period, the section.
    """
    stated = _PERIODS.get(str(play.get("periodOfPlay") or "").upper())
    if stated:
        return stated
    description = str(play.get("playDescription") or "")
    match = _END_QUARTER_RE.search(description)
    if match:
        return int(match.group(1))
    if _END_OVERTIME_RE.search(description):
        return max(previous or 5, 5)
    return previous if previous is not None else section_period


def _flatten(fox: Mapping[str, Any]) -> List[Dict[str, Any]]:
    """Every play in chronological order, each carrying its drive group and repaired period.

    **Sorted on Fox's own play id**, which is one ascending 1..N sequence over the game. That
    is what makes the live path work: until the game is final Fox serves the entire ``pbp``
    tree reversed -- sections, groups *and* plays all descending -- so reading it in feed order
    would file the last play of the game as the first, invert every drive and give the whole
    game the wrong end state. Sorting collapses both orders onto the same frame, so the live
    and final paths are the same code (measured over 291 live snapshots plus the final of
    DET @ BUF: ids ``193...1`` live, ``1...194`` at FINAL).
    """
    rows: List[Dict[str, Any]] = []
    for section in (fox.get("pbp") or {}).get("sections") or []:
        if not isinstance(section, Mapping):
            continue
        section_period = _PERIODS.get(str(section.get("title") or "").upper())
        for group in section.get("groups") or []:
            if not isinstance(group, Mapping):
                continue
            for play in group.get("plays") or []:
                if not isinstance(play, Mapping) or play.get("id") is None:
                    continue
                try:
                    order = int(play["id"])
                except (TypeError, ValueError):
                    continue
                rows.append({"order": order, "play": play, "group": group, "section_period": section_period})
    rows.sort(key=lambda r: r["order"])
    previous: Optional[int] = None
    for row in rows:
        previous = _period_of(row["play"], previous, row["section_period"])
        row["period"] = previous
    return rows


def _drive_order(rows: Sequence[Mapping[str, Any]]) -> List[Any]:
    """Drive group ids in chronological order (first appearance in the sorted play list)."""
    order: List[Any] = []
    for row in rows:
        gid = row["group"].get("id")
        if gid not in order:
            order.append(gid)
    return order


def _classify(
    play: Mapping[str, Any],
    events: Sequence[Mapping[str, Any]],
    offence: Optional[str],
    side_of: Mapping[str, str],
    period: Optional[int],
) -> Tuple[str, Optional[str]]:
    """``(ESPN type id, scoring side)`` for one Fox row.

    The scoring side is **not** always the offence: a pick six, a punt or kickoff return
    touchdown and a fumble return touchdown are scored by the other club, and typing them off
    possession would flip their end state a hundred yards (the same defect the #540 review
    found on a strip-sack touchdown).
    """
    title = str(play.get("title") or "").strip().lower()
    description = str(play.get("playDescription") or "")
    low = description.lower()
    other = None if offence is None else ("home" if offence == "away" else "away")
    if not events:
        if "period end" in title or "end of period" in title:
            if period == 2:
                return "65", None
            return ("66", None) if "end game" in low or "end of game" in low else ("2", None)
        if "game end" in title or "end game" in low:
            return "66", None
        if "two minute" in title:
            return "75", None
        if "tv timeout" in title or "official timeout" in title:
            return "74", None
        if "timeout" in title:
            return "21", None
        return "2", None

    codes = {str(e.get("text") or "").upper() for e in events}
    # Fox does NOT always emit a ``TD`` event code: 15 of the 98 touchdowns across the captured
    # games carry only ``KICK``/``RET`` and say TOUCHDOWN in the text alone (every kickoff-return
    # and muffed-kick score). Typing those off the codes leaves the return score as a plain
    # Kickoff -- no ``scoringPlay``, no end state at the goal line, and the scoreboard steps on
    # the wrong row.
    touchdown = "TD" in codes or ("TOUCHDOWN" in description.upper() and not _NULLIFIED_RE.search(low))
    recovered = _RECOVERED_RE.findall(description)
    recovering = side_of.get(recovered[-1].upper()) if recovered else None
    fumble = "fumble" in low and recovered and "overturned" not in low

    if any(c.startswith("PAT") or "2-PT" in c or c == "2PT" for c in codes) or "two-point conversion attempt" in low:
        return "PAT", None
    if "KICK" in codes:
        return ("32", offence) if touchdown else ("53", None)
    if "PUNT" in codes:
        if touchdown:
            return "37" if "blocked" in low else "34", other
        return ("17", None) if "blocked" in low else ("52", None)
    if "FG - GOOD" in codes:
        return "59", offence
    if any(c.startswith("FG") for c in codes):
        if touchdown:
            return "38", other
        return ("18", None) if "blocked" in low else ("60", None)
    if "INT" in codes:
        return ("36", other) if touchdown else ("26", None)
    if fumble and touchdown:
        return "39", recovering
    if fumble:
        return ("9", None) if recovering == offence else ("29", None)
    if "SACK" in codes:
        return "7", None
    if "safety" in low:
        return "20", other
    if "PEN" in codes and not (codes & {"RUSH", "PASS", "INC", "SACK"}):
        # A flag on a play that still happened keeps the PLAY's ESPN type, not "Penalty":
        # ESPN types "... rush left for 2 yards ... PENALTY on OKLA, Personal Foul, 15 yards"
        # a Rush, and typing it Penalty drops it out of ``scrimmage_play``. Fox collapses such
        # a row to a single ``PEN`` event and states the action only in the text, so the action
        # is read back from there.
        #
        # A **wiped** play ("- No Play") stays a Penalty even though ESPN's NFL feed types it
        # by the action: on the wiped rows Fox states only the ENFORCEMENT geometry, so taking
        # ESPN's type means booking the enforcement as a rush gain or zeroing an incompletion
        # that ESPN books at the penalty's yards. Measured over the 18-game NFL gate, typing
        # them by the action cost EPA r .9738 -> .9562 (statYardage .972 -> .957); the type
        # string agrees more often and the numbers agree less, so the numbers win.
        from_text = None if "no play" in low else _action_from_text(low)
        if from_text is None:
            return "8", None
        return (from_text, offence) if from_text == "59" else (from_text, None)
    if touchdown:
        return ("67", offence) if " pass " in f" {low} " or "catch made" in low else ("68", offence)
    if "INC" in codes:
        return "3", None
    if "PASS" in codes:
        return "24", None
    if "RUSH" in codes:
        return "5", None
    return "8", None


#: The action a Fox row states only in its text, for rows whose events are a bare ``PEN``.
#: Ordered: the first pattern that matches wins.
_TEXT_ACTIONS: Tuple[Tuple[Any, str], ...] = (
    (re.compile(r"(?i)field goal.{0,30}\bis blocked|blocked the kick"), "18"),
    (re.compile(r"(?i)field goal.{0,30}\bis (?:no good|short|wide)"), "60"),
    (re.compile(r"(?i)field goal.{0,30}\bis good"), "59"),
    (re.compile(r"(?i)\bpunts?\b"), "52"),
    (re.compile(r"(?i)\bkicks\b"), "53"),
    (re.compile(r"(?i)\bintercepted\b"), "26"),
    (re.compile(r"(?i)\bsacked at\b"), "7"),
    (re.compile(r"(?i)pass incomplete|incomplete\.|spikes the ball"), "3"),
    (re.compile(r"(?i)catch made by|pass .{0,25}complete\b"), "24"),
    (re.compile(r"(?i)\brushed\b|\bscrambles\b|\bkneels\b|\bup the middle\b"), "5"),
)


def _action_from_text(low: str) -> Optional[str]:
    """The ESPN type a penalty-only Fox row states in its text, or None when it states none."""
    for pattern, type_id in _TEXT_ACTIONS:
        if pattern.search(low):
            return type_id
    return None


def _stat_yardage(
    play: Mapping[str, Any],
    events: Sequence[Mapping[str, Any]],
    type_id: str,
    offence_sign: int,
    kicking_sign: int,
) -> int:
    """ESPN's ``statYardage`` for one Fox row.

    Three shapes, because ESPN books three different things under one key:

    * a **field goal** is booked as its distance, which only the text states;
    * a **kick, punt or interception** is booked as the *return*, in the returning team's
      direction -- never the kick's own flight, which is what the shipped CFB adapter used and
      what inflated ``advBoxScore.team.total_yards`` to 1.8x ESPN's;
    * everything else is the first event's displacement in the offence's direction.
    """
    if type_id in ("59", "60", "18", "38"):
        match = _FG_YARDS_RE.search(str(play.get("playDescription") or ""))
        return int(match.group(1)) if match else 0
    if type_id == "3":
        # An incompletion gains nothing. Fox's event geometry for one is the THROW's flight
        # (``INC`` spans line of scrimmage -> where the ball landed), and booking that as
        # ``statYardage`` credited a 7-yard gain on an incompletion -- 6.2 EPA on one row.
        return 0

    def segment(event: Mapping[str, Any], sign: int) -> int:
        start, end = _pos(event.get("yardStart")), _pos(event.get("yardEnd"))
        return 0 if start is None or end is None else sign * (end - start)

    if type_id in ("53", "32", "52", "34", "17", "37", "26", "36"):
        returns = [e for e in events if str(e.get("text") or "").upper() == "RET"]
        return segment(returns[0], -kicking_sign) if returns else 0
    return segment(events[0], offence_sign) if events else 0


def _penalty(description: str, side_of: Mapping[str, str], team_ids: Mapping[str, str]) -> Optional[Dict[str, Any]]:
    """ESPN's ``penalty`` sub-object from Fox's penalty sentence, or None when there is none."""
    match = _PENALTY_RE.search(description)
    if not match:
        return None
    side = side_of.get(match.group(1).upper())
    return {
        "type": {"text": match.group(2).split(" / ")[0].strip()},
        "yards": int(match.group(3)),
        "status": {"text": match.group(4).title()},
        "team": {"id": team_ids.get(side)} if side else {},
    }


def _down_distance_text(down: Optional[int], distance: Optional[int], spot: Optional[str]) -> Optional[str]:
    """``"2nd & Goal at UK 3"`` -- the processor reads this only for its goal-to-go test."""
    if not down:
        return None
    label = _ORDINAL.get(int(down), f"{int(down)}th")
    togo = "Goal" if not distance else str(int(distance))
    return f"{label} & {togo}" + (f" at {spot}" if spot else "")


_XP_GOOD_RE = re.compile(r"(?i)\bextra point is good\b")
_TWO_POINT_RE = re.compile(r"(?i)\btwo[- ]point conversion attempt\b")


def _try_grammar(text: str, good: bool, two_point: bool) -> str:
    """Fox's try sentence rewritten into the casing ESPN's NFL text uses.

    ``NFLPlayProcess`` reads the try off the **text**, case-sensitively
    (``nfl_pbp.py:2936``: ``"extra point is GOOD"``, ``"(J.Elliott Kick)"``) and
    ``ep_wp.calculate_epa`` scores a touchdown ``+7`` or ``+6.92`` off the resulting
    ``xp_made`` flag -- not off ``pointAfterAttempt``, which both feeds state identically.
    Fox writes "extra point is good.", so without this every made extra point in the game is
    scored as a miss: EP_end 6.0 instead of 7.0, ~1.0 EPA on every touchdown.

    Only the casing changes, and only when the try actually succeeded, so a miss ("extra point
    is no good") can never be turned into a make.
    """
    if two_point:
        return _TWO_POINT_RE.sub("TWO-POINT CONVERSION ATTEMPT", text)
    return _XP_GOOD_RE.sub("extra point is GOOD", text) if good else text


def _fold_target(emitted: List[Dict[str, Any]], last_touchdown: int) -> Dict[str, Any]:
    """The row a point-after belongs to: the **newest touchdown**, never simply the row before it.

    Fox emits the try as its own row, and it is not always the row after the touchdown -- a
    timeout, a penalty on the try or a replay review sits between them. Anchoring on
    ``emitted[-1]`` hangs ``pointAfterAttempt`` off that stoppage and leaves the touchdown
    stepping the scoreboard by 6 (735 of the 30,279 tries in ``nfl-raw``, #540 finding 1).
    """
    return emitted[last_touchdown]


def _trailing_end(play: Dict[str, Any], team_ids: Mapping[str, str], home_side: str) -> Dict[str, Any]:
    """The end state of the one play the feed states no next snap for, from its own yardage.

    Fires on the last row of every final and -- the case that matters -- on the **newest row of
    every live poll**, which is the row Game on Paper renders at the top of the page. Taking
    the play's own start says the ball never moved, so a 19-yard gain ends where it began and
    its EPA is computed against a spot no feed reports (the #541 review measured 27 such rows).
    Only a plain scrimmage snap keeps the ball, so only those are derived.

    **Fourth down short of the sticks is the exception**: the ball turns over where it lies,
    and ESPN writes that row's end in the DEFENCE's frame at 1st & 10 -- 51 of 51 such rows in
    the ESPN summaries of this capture. Keeping the offence's frame is a ~100-yard, multi-EPA
    error on the highest-leverage snap in football, on the newest row of a live poll: 50 rows
    of the 7,184 in the capture would be served that way (#545 finding 1, same class).
    """
    start = play["start"]
    keeps = play["type"]["id"] in _KEEPS_THE_BALL
    gained = int(play.get("statYardage") or 0) if keeps else 0
    to_endzone = start["yardsToEndzone"]
    if to_endzone is not None and gained:
        to_endzone = max(0, min(100, int(to_endzone) - gained))
    side, down, distance = start["team"]["side"], start["down"], start["distance"]
    # distance 0 is goal-to-go, where the sticks are the goal line: a row that kept the ball
    # (so it is not a touchdown type) came up short by definition.
    short = gained < int(distance) if distance else True
    if keeps and int(start.get("down") or 0) == 4 and short:
        side = "home" if side == "away" else "away"
        down, distance = 1, 10
        if to_endzone is not None:
            to_endzone = 100 - to_endzone
    is_home = side == home_side
    return {
        "down": down,
        "distance": distance,
        "yardLine": start["yardLine"] if to_endzone is None else ((100 - to_endzone) if is_home else to_endzone),
        "yardsToEndzone": to_endzone,
        "team": {"id": team_ids[side]},
    }


def _fill_end_state(plays: List[Dict[str, Any]], team_ids: Mapping[str, str], home_side: str) -> None:
    """Fill every play's ``end`` from the **next** snap's start, across drive boundaries.

    Fox states no end state at all, so the next snap is it -- which is how ESPN builds its own,
    and why the search must not stop at a drive boundary: a punt ends in the receiving team's
    frame, on the first row of the next drive. Clock stoppages carry no state of their own and
    are skipped, so the end spot of the play before a timeout is the ball's real spot.

    The search **stops** at the end of a half (:data:`_HALF_BOUNDARY`): the next snap is then a
    kickoff in the other direction, and reading through the boundary gave the clock-killing
    kneel at the end of the half the ensuing kickoff's spot, in the receiving team's frame.

    Two shapes take ESPN's own scoring convention instead: a **touchdown** ends at the goal
    line the scoring team was attacking, credited to the club the play says scored -- the
    defence on a pick six, a punt return or a strip-sack return -- and a **made field goal**
    ends where it was kicked from, credited to the kicking team, not at the ensuing kickoff.
    """
    for index, play in enumerate(plays):
        type_id = play["type"]["id"]
        scoring_side = play.get("_scoring_side")
        if type_id in _TOUCHDOWN_TYPES and scoring_side:
            play["end"] = {
                "down": -1,
                "distance": 0,
                "yardLine": 100 if scoring_side == home_side else 0,
                "yardsToEndzone": 0,
                "team": {"id": team_ids[scoring_side]},
            }
            continue
        if type_id == "59":
            play["end"] = {
                "down": -1,
                "distance": -1,
                "yardLine": play["start"]["yardLine"],
                "yardsToEndzone": play["start"]["yardsToEndzone"],
                "team": {"id": team_ids[scoring_side] if scoring_side else play["start"]["team"]["id"]},
            }
            continue
        nxt = None
        for later in plays[index + 1 :]:
            if later["type"]["text"] in _HALF_BOUNDARY:
                break
            if later["type"]["text"] not in _STOPPAGE:
                nxt = later["start"]
                break
        if nxt is None:
            play["end"] = _trailing_end(play, team_ids, home_side)
            continue
        play["end"] = {
            "down": nxt["down"],
            "distance": nxt["distance"],
            "yardLine": nxt["yardLine"],
            "yardsToEndzone": nxt["yardsToEndzone"],
            "team": {"id": nxt["team"]["id"]},
        }


def _drive(
    event_id: str,
    index: int,
    group: Mapping[str, Any],
    plays: List[Dict[str, Any]],
    team: Mapping[str, Any],
) -> Dict[str, Any]:
    """One ``drives.previous[]`` / ``drives.current`` entry from a Fox drive group."""
    result = str(group.get("title") or "").strip()
    match = _DRIVE_SUBTITLE_RE.match(re.sub(r"[^\x00-\x7f]", " ", str(group.get("subtitle") or "")))
    snaps = [p for p in plays if p["type"]["text"] not in _STOPPAGE] or plays
    return {
        "id": f"{event_id}{index}",
        "description": group.get("subtitle"),
        "team": {
            "id": team.get("id"),
            "abbreviation": team.get("abbreviation"),
            "displayName": team.get("displayName"),
            "name": team.get("name"),
            "shortDisplayName": team.get("shortDisplayName"),
        },
        "start": {
            "period": {"number": snaps[0]["period"]["number"], "type": "quarter"},
            "clock": snaps[0]["clock"],
            "yardLine": snaps[0]["start"]["yardLine"],
            "text": None,
        },
        "end": {
            "period": {"number": snaps[-1]["period"]["number"], "type": "quarter"},
            "clock": snaps[-1]["clock"],
            "yardLine": snaps[-1]["end"].get("yardLine"),
        },
        "timeElapsed": {"displayValue": match.group(3) if match else None},
        "yards": int(match.group(2)) if match else 0,
        "offensivePlays": int(match.group(1)) if match else len(snaps),
        "isScore": result.upper() in ("TOUCHDOWN", "FIELD GOAL", "SAFETY"),
        "result": result or None,
        "shortDisplayResult": result or None,
        "displayResult": result.title() or None,
        "plays": plays,
    }


def _fox_to_espn_summary(
    fox: Mapping[str, Any],
    idmap_row: Mapping[str, Any],
    *,
    league: str,
    play_types: Mapping[str, Tuple[str, Optional[str]]],
    espn_uid_league: str,
    team_meta: Optional[Mapping[str, Mapping[str, Any]]] = None,
    text_aliases: Optional[Mapping[str, str]] = None,
    odds: Optional[Mapping[str, Any]] = None,
) -> Tuple[Dict[str, Any], List[str]]:
    """Project one Fox Bifrost game (final or in progress) onto an ESPN-summary-shaped dict.

    Args:
        fox: A ``{sport}/event/{id}/data`` payload.
        idmap_row: The game's id-map row
            (:data:`sportsdataverse.football.sources.idmap.GAME_SCHEMA`). ``espn_event_id``,
            ``home_espn_team_id`` and ``away_espn_team_id`` are required -- Fox's team ids are
            its own (Cleveland is 7 on Fox and 5 on ESPN), and both processors cast
            ``team.id`` to ``int`` and use it for possession, logos and the box score.
        league: ``"nfl"`` or ``"cfb"``. Nothing in the projection branches on it -- the two
            feeds are one product -- so it is carried only to keep the two call sites symmetric
            with the other adapters.
        play_types: ESPN ``type.id`` -> ``(type.text, type.abbreviation)`` for the league.
        espn_uid_league: ESPN's numeric league slot for the ``uid`` strings (28 NFL, 23 CFB).
        team_meta: Optional per-ESPN-team-id metadata (``abbreviation``, ``color``,
            ``alternateColor``) from the league's own franchise table. Used **only** to prefer
            ESPN's own abbreviation and franchise colours over Fox's; nothing is invented when
            it is absent (college football has no such table and falls back to Fox's colours).
        text_aliases: Extra ``play-text club code -> header club code`` fixes for codes Fox's
            text spells differently from its own header (NFL: ``JAC`` for ``JAX``).
        odds: ``{gameSpread, overUnder, homeFavorite, gameSpreadAvailable}``; becomes the
            summary's one-provider ``pickcenter``.

    Returns:
        ``(summary, notes)``.

        | item | type | description |
        |---|---|---|
        | summary | dict | An ESPN-summary-shaped payload: `header`, `drives.previous` (+ `drives.current` while the game is live), `pickcenter` and empty `boxscore` / passthrough arrays. Feed it to `espn_{nfl,cfb}_pbp(summary=)`. |
        | notes | list[str] | Adapter-side degradations worth surfacing in provenance: a PAT with no touchdown to fold into, rows whose drive states no team, rows filed in the drive group of the possession that just ended (possession flipped), and the open drive moved to `drives.current` for a live game. |

    Raises:
        ValueError: the payload carries no ``header`` with both teams, the id-map row states no
            ESPN team ids, or the payload carries no plays.
        KeyError: the id-map row states no ``espn_event_id``, or the league's ``play_types``
            table has no entry for a type id the classifier emitted. Both reach dispatch as a
            hand-over (it treats any exception from an adapter as one), not as a crash.

    Example:
        Adapt a stored Fox final and process it::

            import json
            from sportsdataverse.nfl.fox_pbp.to_espn_summary import _fox_nfl_to_espn_summary

            with open("fox_11187_data.json") as fh:
                fox = json.load(fh)
            row = {"espn_event_id": "401872922", "home_espn_team_id": "30", "away_espn_team_id": "5"}
            summary, notes = _fox_nfl_to_espn_summary(fox, row)
    """
    header = fox.get("header") if isinstance(fox.get("header"), Mapping) else None
    left = (header or {}).get("leftTeam")
    right = (header or {}).get("rightTeam")
    if not isinstance(left, Mapping) or not isinstance(right, Mapping):
        raise ValueError("fox payload carries no header with both teams")
    event_id = str(idmap_row["espn_event_id"])
    espn_ids = {
        "away": str(idmap_row.get("away_espn_team_id") or ""),
        "home": str(idmap_row.get("home_espn_team_id") or ""),
    }
    if not (espn_ids["away"] and espn_ids["home"]):
        raise ValueError("id-map row states no ESPN team ids: possession cannot be attributed")
    notes: List[str] = []

    fox_team = {"away": left, "home": right}
    # Fox's own team ids are how possession is read: the drive group's entityLink points at
    # ``.../teams/{id}``. Matching on ids rather than names is what survives "WAS" vs "WSH".
    side_by_fox_id = {}
    for side, team in fox_team.items():
        fox_id = _fox_team_id(team)
        if fox_id:
            side_by_fox_id[fox_id] = side

    meta = team_meta or {}
    abbr = {}
    for side in ("away", "home"):
        fox_abbr = str(fox_team[side].get("name") or "").strip()
        espn_abbr = (meta.get(espn_ids[side]) or {}).get("abbreviation")
        sub = (idmap_row.get(f"{side}_team") or {}) if isinstance(idmap_row.get(f"{side}_team"), Mapping) else {}
        abbr[side] = str(sub.get("espn_abbr") or espn_abbr or fox_abbr or "").strip() or fox_abbr
    # Club codes as they appear in Fox's *text*, mapped to the abbreviation actually emitted.
    text_fixes: Dict[str, str] = {}
    for side in ("away", "home"):
        fox_abbr = str(fox_team[side].get("name") or "").strip().upper()
        if fox_abbr and fox_abbr != abbr[side]:
            text_fixes[fox_abbr] = abbr[side]
        for alias, target in (text_aliases or {}).items():
            if target.upper() == fox_abbr and alias.upper() != abbr[side]:
                text_fixes[alias.upper()] = abbr[side]
    # side lookup by every code that can name a club in the text (recovery / penalty sentences)
    side_of: Dict[str, str] = {}
    for side in ("away", "home"):
        for code in (str(fox_team[side].get("name") or ""), abbr[side]):
            if code:
                side_of[code.upper()] = side
    for alias, target in (text_aliases or {}).items():
        if target.upper() in side_of:
            side_of.setdefault(alias.upper(), side_of[target.upper()])

    rows = _flatten(fox)
    if not rows:
        raise ValueError("fox payload carries no plays")

    # running score: a drive group's ``scores[]`` is the score AFTER that drive, so a row gets
    # the pre-drive score until the drive's own scoring play, and the post-drive score from it on
    order = _drive_order(rows)
    score_after: Dict[Any, Dict[str, int]] = {}
    running = {"away": 0, "home": 0}
    for gid in order:
        group = next(r["group"] for r in rows if r["group"].get("id") == gid)
        post = dict(running)
        for entry in group.get("scores") or []:
            side = side_of.get(str((entry or {}).get("title") or "").strip().upper())
            if side is not None:
                try:
                    post[side] = int(entry.get("score"))
                except (TypeError, ValueError):
                    pass
        score_after[gid] = post
        running = post

    emitted: List[Dict[str, Any]] = []
    group_of_play: List[Any] = []
    last_touchdown: Optional[int] = None
    scored_groups: set = set()
    driveless_rows = 0
    regrouped_rows = 0
    turnover_group: Any = None
    for row in rows:
        play, group, period = row["play"], row["group"], row["period"]
        gid = group.get("id")
        offence = side_by_fox_id.get(_fox_team_id(group.get("entityLink")) or "")
        if offence is None:
            offence = emitted[-1]["start"]["team"]["side"] if emitted else "away"
            driveless_rows += 1
        if gid == turnover_group and offence is not None:
            offence = "home" if offence == "away" else "away"
            regrouped_rows += 1
        events = _events(play)
        description = _text(play.get("playDescription"), text_fixes)
        type_id, scoring_side = _classify(play, events, offence, side_of, period)

        if type_id == "PAT":
            # ESPN folds the try into its touchdown: same play, one text, one score step. The
            # try is NOT always the row after the touchdown -- a timeout, a penalty on the try
            # or a replay review sits between them -- so anchor on the newest touchdown.
            if last_touchdown is None:
                notes.append(f"play {play.get('id')}: point-after row with no touchdown to fold into")
                continue
            target = _fold_target(emitted, last_touchdown)
            low = description.lower()
            codes = {str(e.get("text") or "").upper() for e in events}
            # Fox states the outcome in the event code ("PAT - GOOD" / "2-PT CONV - NO GOOD");
            # the text is the fall-back for the rows that carry no code at all.
            if any("NO GOOD" in c or "FAILED" in c or "BLOCKED" in c for c in codes):
                good = False
            elif any(c.endswith("GOOD") for c in codes):
                good = True
            else:
                good = "is good" in low or "is successful" in low or "converted" in low or "succeeds" in low
            two = any("2-PT" in c or c == "2PT" for c in codes) or "two-point" in low or "two point" in low
            try_label = (
                ("Two Point Conversion Good" if good else "Two Point Attempt Failed")
                if two
                else ("Extra Point Good" if good else "Extra Point Missed")
            )
            target["pointAfterAttempt"] = {
                "id": (62 if good else 63) if two else (61 if good else 64),
                "text": try_label,
                "abbreviation": try_label,
                "value": (2 if good else 0) if two else (1 if good else 0),
            }
            if description and description not in target["text"]:
                target["text"] = f"{target['text']} {_try_grammar(description, good, two)}"
            continue

        label, abbreviation = play_types[type_id]
        title = _parse_title(play.get("title"))
        is_kick = type_id in ("53", "32")
        # ESPN credits a kickoff to the KICKING team (the processor flips it back with
        # ``kickoff_vec``); Fox's drive group names the RECEIVING team.
        start_side = ("home" if offence == "away" else "away") if is_kick else offence
        start_pos = _pos(events[0].get("yardStart")) if events else None
        to_endzone = None
        if start_pos is not None:
            to_endzone = (100 - start_pos) if start_side == "away" else start_pos
        down = 0 if is_kick else (title["down"] or 0)
        distance = 0 if is_kick else (title["distance"] if title["distance"] is not None else 0)
        offence_sign = 1 if start_side == "away" else -1
        stat_yardage = _stat_yardage(play, events, type_id, offence_sign, offence_sign)
        if gid in scored_groups or (scoring_side is not None) or type_id == "20":
            scored_groups.add(gid)
            score = score_after[gid]
        else:
            score = score_after[order[order.index(gid) - 1]] if order.index(gid) else {"away": 0, "home": 0}
        espn_play: Dict[str, Any] = {
            "id": f"{event_id}{row['order']:04d}",
            "sequenceNumber": str(row["order"]),
            "type": {"id": type_id, "text": label, "abbreviation": abbreviation},
            "text": description,
            "awayScore": score["away"],
            "homeScore": score["home"],
            "period": {"number": period},
            "clock": {"displayValue": _clock(play.get("timeOfPlay"))},
            "scoringPlay": scoring_side is not None,
            "priority": scoring_side is not None,
            "statYardage": stat_yardage,
            "start": {
                "down": down,
                "distance": distance,
                "yardLine": None if start_pos is None else 100 - start_pos,
                "yardsToEndzone": to_endzone,
                "downDistanceText": _down_distance_text(
                    down, distance, str(play.get("title") or "").split("·")[-1].strip()
                ),
                "team": {"id": espn_ids[start_side], "side": start_side},
            },
            "end": {},
            "_scoring_side": scoring_side,
        }
        penalty = _penalty(description, side_of, espn_ids)
        if penalty:
            espn_play["penalty"] = penalty
        if scoring_side is not None:
            kind = "Field Goal" if type_id == "59" else ("Safety" if type_id == "20" else "Touchdown")
            espn_play["scoringType"] = {"displayName": kind, "name": kind.lower().replace(" ", "-")}
        emitted.append(espn_play)
        group_of_play.append(gid)
        if type_id in _TOUCHDOWN_TYPES:
            last_touchdown = len(emitted) - 1
        if type_id in _CHANGES_POSSESSION and "no play" not in description.lower():
            # a nullified play hands the ball to nobody, so it must not flip the rows Fox filed
            # after it in the same drive group
            turnover_group = gid

    if driveless_rows:
        notes.append(f"{driveless_rows} plays sit in a drive group naming no team: possession carried forward")
    if regrouped_rows:
        notes.append(
            f"{regrouped_rows} plays sit in the drive group of the possession that just ended "
            "(Fox groups the receiving team's first stoppage there): possession flipped"
        )

    # Admin rows carry no state of their own; ESPN gives them the *next* snap's spot. Period-end
    # rows instead get down/distance 0, exactly as ESPN's own End of Half / End of Game rows do:
    # copying the previous snap's state makes ``nfl_pbp``'s duplicate-text filter drop that snap.
    for index, play in enumerate(emitted):
        if play["type"]["text"] not in _STOPPAGE:
            continue
        nxt = next((p for p in emitted[index + 1 :] if p["type"]["text"] not in _STOPPAGE), None)
        prv = next((p for p in reversed(emitted[:index]) if p["type"]["text"] not in _STOPPAGE), None)
        source = nxt or prv
        if source is None:
            continue
        period_end = play["type"]["id"] in ("2", "65", "66", "79")
        play["start"] = {
            "down": 0 if period_end else source["start"]["down"],
            "distance": 0 if period_end else source["start"]["distance"],
            "yardLine": source["start"]["yardLine"],
            "yardsToEndzone": source["start"]["yardsToEndzone"],
            "downDistanceText": None if period_end else source["start"]["downDistanceText"],
            "team": dict(source["start"]["team"]),
        }

    _fill_end_state(emitted, espn_ids, "home")
    for play in emitted:
        play["start"]["team"].pop("side", None)
        play.pop("_scoring_side", None)

    colours = {
        "away": (_hex_colour((header or {}).get("leftColor")), _hex_colour((header or {}).get("alternateLeftColor"))),
        "home": (_hex_colour((header or {}).get("rightColor")), _hex_colour((header or {}).get("alternateRightColor"))),
    }
    competitors = [
        _competitor(
            side,
            order_index,
            espn_ids[side],
            fox_team[side],
            abbr[side],
            meta.get(espn_ids[side]) or {},
            espn_uid_league,
            colours[side],
        )
        for order_index, side in enumerate(("home", "away"))
    ]
    final = (header or {}).get("eventStatus") == 3
    drives: List[Dict[str, Any]] = []
    grouped: Dict[Any, List[Dict[str, Any]]] = {}
    for gid, play in zip(group_of_play, emitted):
        grouped.setdefault(gid, []).append(play)
    for index, gid in enumerate([g for g in order if grouped.get(g)], start=1):
        plays = grouped[gid]
        group = next(r["group"] for r in rows if r["group"].get("id") == gid)
        side = side_by_fox_id.get(_fox_team_id(group.get("entityLink")) or "") or "away"
        team = next(c["team"] for c in competitors if c["homeAway"] == side)
        drives.append(_drive(event_id, index, group, plays, team))
    current: Optional[Dict[str, Any]] = None
    if drives and not final:
        # a live payload's last drive is still open. Fox even says so in its title
        # ("CURRENT - BALL ON DET 22"); it becomes ``drives.current`` so Game on Paper renders
        # it as the current drive, and it states no outcome whatever the title claims.
        current = drives.pop()
        current["result"] = current["shortDisplayResult"] = current["displayResult"] = "In Progress"
        current["isScore"] = False
        notes.append("game is not final: the open drive was moved to drives.current")

    summary = {
        "boxscore": {"teams": [], "players": []},
        "format": {"regulation": {"periods": 4}},
        "gameInfo": {"venue": {}, "attendance": None},
        "drives": {"previous": drives, **({"current": current} if current else {})},
        "leaders": [],
        "broadcasts": [],
        "predictor": {},
        "pickcenter": _pickcenter(odds),
        "againstTheSpread": [],
        "odds": [],
        "winprobability": [],
        "header": {
            "id": event_id,
            "uid": f"s:20~l:{espn_uid_league}~e:{event_id}",
            "season": {
                "year": idmap_row.get("season") or _season_from(header),
                "type": idmap_row.get("season_type") or 2,
            },
            "week": idmap_row.get("week"),
            "timeValid": True,
            "competitions": [
                {
                    "id": event_id,
                    "uid": f"s:20~l:{espn_uid_league}~e:{event_id}~c:{event_id}",
                    "date": (header or {}).get("eventTime"),
                    "neutralSite": bool(idmap_row.get("neutral_site") or False),
                    "conferenceCompetition": False,
                    "boxscoreAvailable": False,
                    "commentaryAvailable": False,
                    "liveAvailable": False,
                    "onWatchESPN": False,
                    "recent": False,
                    "boxscoreSource": "none",
                    "playByPlaySource": "full",
                    "status": _status(header or {}, emitted),
                    "competitors": competitors,
                }
            ],
        },
        "scoringPlays": [],
        "videos": [],
        "standings": {},
    }
    return summary, notes


def _season_from(header: Optional[Mapping[str, Any]]) -> Optional[int]:
    """The season year off ``eventTime`` when the id-map row states none (August rolls forward)."""
    match = re.match(r"(\d{4})-(\d{2})", str((header or {}).get("eventTime") or ""))
    if not match:
        return None
    year, month = int(match.group(1)), int(match.group(2))
    return year if month >= 8 else year - 1


def _hex_colour(value: Any) -> Optional[str]:
    """Fox's ``"1, 164, 31, 53"`` (alpha, r, g, b) -> ``"a41f35"``, or None.

    The only colour any Fox payload states. It matters for college football, where there is no
    ESPN franchise table to read one from and Game on Paper styles the header with it.
    """
    parts = [part.strip() for part in str(value or "").split(",")]
    if len(parts) != 4 or not all(part.isdigit() for part in parts):
        return None
    red, green, blue = (int(part) for part in parts[1:])
    return f"{red:02x}{green:02x}{blue:02x}" if max(red, green, blue) <= 255 else None


def _competitor(
    side: str,
    order: int,
    espn_team_id: str,
    fox_team: Mapping[str, Any],
    abbreviation: str,
    meta: Mapping[str, Any],
    espn_uid_league: str,
    colours: Tuple[Optional[str], Optional[str]] = (None, None),
) -> Dict[str, Any]:
    """One ``header.competitions[0].competitors[]`` entry, home first.

    ``team.name`` is the mascot and it must not be empty: the processor charges a ``Timeout``
    row to whichever club's name parts the text contains, and ``""`` is contained in every
    string, so an empty mascot charges every timeout to both clubs (and moved WP on 167/185
    plays when it last happened).
    """
    location = str(fox_team.get("stackedNameTop") or "").strip()
    mascot = str(fox_team.get("stackedNameBottom") or fox_team.get("longName") or "").strip()
    mascot = mascot or location or abbreviation
    location = location or mascot
    try:
        score = int(fox_team.get("score") or 0)
    except (TypeError, ValueError):
        score = 0
    return {
        "id": str(espn_team_id),
        "uid": f"s:20~l:{espn_uid_league}~t:{espn_team_id}",
        "order": order,
        "homeAway": side,
        "winner": bool(fox_team.get("isLoser") is False and fox_team.get("score") is not None),
        "team": {
            "id": str(espn_team_id),
            "uid": f"s:20~l:{espn_uid_league}~t:{espn_team_id}",
            "location": location,
            "name": mascot,
            "nickname": mascot,
            "abbreviation": abbreviation,
            "displayName": str(fox_team.get("imageAltText") or f"{location} {mascot}").strip(),
            "shortDisplayName": mascot,
            "color": meta.get("color") or colours[0],
            "alternateColor": meta.get("alternateColor") or colours[1],
            "logos": [],
        },
        "score": str(score),
        "linescores": [],
        "record": [],
    }


_STATUS_BY_FOX = {
    3: ("3", "STATUS_FINAL", "post", True),
    2: ("2", "STATUS_IN_PROGRESS", "in", False),
    1: ("2", "STATUS_IN_PROGRESS", "in", False),
}


def _status(header: Mapping[str, Any], plays: Sequence[Mapping[str, Any]]) -> Dict[str, Any]:
    """``header.competitions[0].status`` from Fox's ``eventStatus`` + ``statusLine``.

    Fox's own ``statusLine`` ("FINAL", "4TH 1:48") is the only human detail it gives, and Game
    on Paper renders it verbatim. ``eventStatus`` 1 and 2 are both "in progress" -- a scheduled
    game never reaches here, because it carries no ``pbp``.
    """
    status = header.get("eventStatus")
    type_id, name, state, completed = _STATUS_BY_FOX.get(status, ("2", "STATUS_IN_PROGRESS", "in", False))
    detail = str(header.get("statusLine") or ("Final" if completed else "In Progress")).strip()
    last = plays[-1] if plays else {}
    return {
        "clock": 0.0,
        "displayClock": (last.get("clock") or {}).get("displayValue", "0:00"),
        "period": (last.get("period") or {}).get("number") or 0,
        "type": {
            "id": type_id,
            "name": name,
            "state": state,
            "completed": completed,
            "description": "Final" if completed else "In Progress",
            "detail": detail,
            "shortDetail": detail,
        },
        "foxEventStatus": status,
    }
