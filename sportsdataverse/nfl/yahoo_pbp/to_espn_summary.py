"""Yahoo shangrila playbook rows -> an ESPN-summary-shaped dict ``NFLPlayProcess`` consumes unchanged.

Yahoo states a play's **pre-snap situation** (down, distance, yards to the end zone, clock,
possession, the score, a typed play kind, and -- unlike its college feed -- a structured
``subPlays`` breakdown) and nothing about how the play ended, so this module owns the re-skin:
ESPN's play-type vocabulary, ESPN's absolute yard line, the end state read off the next snap,
the try folded into its touchdown, the drive grouping, GSIS-grammar play text and a renderable
header. The output is validated by
:func:`sportsdataverse.football.sources.contract._validate_summary` and consumed through
``espn_nfl_pbp(summary=)``; the dispatcher registers it as ``source="yahoo"`` for the NFL
(:mod:`sportsdataverse.football.sources.dispatch`).

Everything here is ``_``-prefixed and nothing is re-exported from ``sportsdataverse.nfl``: like
the Shield and Yahoo-CFB adapters this is a private, experimental surface, so **no codegen or
reference-doc regeneration is involved** and the generated docs stay untouched.

Three things the NFL feed does that the college one does not, and that this module therefore
owns (measured, Stage 2 ``s2-yahoo-nfl`` gate):

* **``subPlays`` on every play**, which is where ESPN's ``statYardage`` semantics come from:
  a kick / punt / interception row is booked at its *return* yards and a field goal at its
  *distance*, neither of which is Yahoo's own ``yards``.
* **Two feed eras.** From 2020 Yahoo ships a drive chart, separate ``EXTRA_POINT_ATTEMPT``
  rows and a running scoreboard on every play. Before that it ships **no drives**, folds the
  try into the touchdown's own text, and states the score **only on scoring plays** -- every
  other row reads 0-0. Both are detected by shape (:func:`_synthesize_drives`,
  :func:`_score_is_stated_per_play`), never by season, and both process.
* **Club abbreviations are load-bearing.** ``nfl_pbp._nfl_side_of_abbrev`` matches the play
  text's club code against the header's, so the text this module writes and the header it
  builds both use **ESPN's** spelling (``LAR``, ``WSH``) off the Shield adapter's franchise
  table -- not nflverse's ``LA`` / ``WAS``, which charges no timeout and attributes no penalty.

Documented divergences from a real ESPN summary:

* **No ESPN play ids.** Yahoo's ``playId`` is its own sequence, so the emitted id is
  ``{espn_event_id}{playId:04d}``: stable, unique and increasing, but it joins nothing
  ESPN-sourced. A parity comparison has to join on game state.
* **No air yards, YAC, pass depth, formation, long snapper, fair catch, penalty player or
  ESPN athlete ids** -- Yahoo carries none of them. The list is
  :data:`...contract.KNOWN_LOSSY` for ``("nfl", "yahoo")`` and dispatch stamps it into
  provenance; game state, EP, EPA and WP do not depend on any of it.
* ``boxscore`` is empty: ESPN's box is the only source of ESPN athlete ids, and Yahoo carries
  its own player-id space.
"""

from __future__ import annotations

import re
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

from sportsdataverse.football.yahoo_common import (
    _clock,
    _down_distance_text,
    _lineups,
    _odds_from_game,
    _pickcenter,
    _envelope_games,
    _resolve_game,
    _status,
    _text,
    _venue,
)
from sportsdataverse.nfl.shield_pbp.to_espn_summary import ESPN_PLAY_TYPES, _ESPN_TEAMS, _espn_abbr
from sportsdataverse.nfl.yahoo_pbp.fetch import _fetch_playbook_boxscore, _has_plays, _resolve_row

#: Yahoo ``playTypeId`` values ESPN folds into the touchdown they follow (2020+ feeds only;
#: before that the try lives inside the touchdown's own text).
_PAT_TYPES = frozenset({"EXTRA_POINT_ATTEMPT", "TWO_POINT_PASS", "TWO_POINT_RUSH", "TWO_POINT_ATTEMPT"})
#: ESPN ``type.text`` values that stop the clock rather than describing a snap. The end state of
#: the play *before* one of these is the next real snap's spot, not the stoppage row's.
_STOPPAGE = frozenset(
    {
        "Timeout",
        "Official Timeout",
        "Two-minute warning",
        "End Period",
        "End of Half",
        "End of Game",
        "End of Regulation",
    }
)
#: ESPN type ids on which the offence keeps the ball where it left it, so a play with no next
#: snap still has a derivable end spot (``start - statYardage``). Every other type either
#: changes possession or is a kick, where the yardage says nothing about the next spot.
_KEEPS_THE_BALL = frozenset({"3", "5", "7", "24"})
#: ESPN type ids that open a drive: the kickoff family. Used only when Yahoo ships no drive
#: chart of its own (every season before 2020).
_KICKOFF_TYPES = frozenset({"53", "32"})

_SUFFIX_RE = re.compile(r"\s+(?:Jr|Sr|II|III|IV|V)\.?$")
_FG_YARDS_RE = re.compile(r"(?i)\b(\d{1,2})[- ]yard field goal")
_RECOVERED_RE = re.compile(r"\[(nfl\.p\.\d+)\] recovered fumble")
_PAT_GOOD_RE = re.compile(r"(?i)\bmade PAT\b|\bPAT is good\b")
_TWO_POINT_GOOD_RE = re.compile(r"(?i)2pt attempt converted|two point conversion")
#: The try Yahoo writes **inside** a pre-2020 touchdown's text, e.g. "... for 2 yard touchdown.
#: J.Tucker made PAT".
#: ``". "`` (dot-SPACE) is the feed's sentence break: an abbreviated name never carries one
#: ("S.Gostkowski"), so requiring the space is what keeps a name from being read as one.
_INLINE_PAT_RE = re.compile(r"(?i)\.\s+(?P<kicker>.+?)\s+(?P<result>made|missed)\s+PAT\s*$")

_ORDINAL_QUARTER = {1: "1", 2: "2", 3: "3", 4: "4"}


# --------------------------------------------------------------------------------------
# play typing
# --------------------------------------------------------------------------------------
def _end_of_period_type_id(text: str, period: Optional[int]) -> str:
    """Yahoo's end-of-period texts -> ESPN's four end types."""
    low = text.lower()
    if "game" in low:
        return "66"
    if "regulation" in low:
        return "79"
    if "half" in low or period == 2:
        return "65"
    return "2"


def _fumble_type_id(play: Mapping[str, Any], player_team: Mapping[str, str], touchdown: bool) -> str:
    """Own vs opponent fumble recovery, from the recovering player's team."""
    if touchdown:
        return "39"
    match = _RECOVERED_RE.search(str(play.get("text") or ""))
    recovered_by = player_team.get(match.group(1)) if match else None
    own = recovered_by is None or recovered_by == play.get("teamId")
    return "9" if own else "29"


def _type_id(play: Mapping[str, Any], text: str, player_team: Mapping[str, str]) -> str:
    """The ESPN ``type.id`` for one Yahoo play row.

    Scoring rows are typed from ``scoringPlayInfo`` rather than from the text: a touchdown
    scored by the team that did **not** snap the ball (a pick six, a strip-sack return) is a
    different ESPN type from the same play kind without the score, and typing it off possession
    alone would both mislabel it and, through :func:`_fill_end_state`, flip its end spot 100
    yards. A defensive touchdown on a ``SACK`` row is typed ``39`` (``Fumble Return
    Touchdown``), which is what ESPN's own NFL summaries call it -- the Shield adapter's ``80``
    / ``SFOP`` is Shield's vocabulary, and the choice between them is a parked lead decision;
    either way the ``TD`` abbreviation is what makes the scoring end state fire.
    """
    kind = str(play.get("playTypeId") or "")
    info = play.get("scoringPlayInfo") or {}
    score_type = str(info.get("scoreTypeId") or "")
    touchdown = score_type == "TOUCHDOWN"
    low = text.lower()
    fumbled = "recovered fumble" in low
    if kind == "END_OF_PERIOD":
        return _end_of_period_type_id(text, play.get("period"))
    if kind == "TIMEOUT":
        if "two-minute" in low or "2-minute" in low:
            return "75"
        return "74" if _RE_OFFICIAL_TIMEOUT.match(text.strip()) else "21"
    if score_type == "SAFETY":
        return "20"
    if kind == "PENALTY":
        return "8"
    if kind == "FIELD_GOAL_ATTEMPT":
        if touchdown:
            return "38"
        if score_type == "FIELD_GOAL":
            return "59"
        return "18" if "block" in low else "60"
    if kind == "PUNT":
        if touchdown:
            return "34"
        return "17" if "block" in low else "52"
    if kind == "KICKOFF":
        return "32" if touchdown else "53"
    if kind == "PASS_INTERCEPTED":
        return "36" if touchdown else "26"
    if kind == "SACK":
        if touchdown:
            return "39"
        # A strip sack the DEFENCE recovers is typed ``29`` (``Fumble Recovery (Opponent)``),
        # not ESPN's ``80`` / ``SFOP``. ESPN uses both across the captured summaries, but only
        # ``29`` makes the processor score the row as the turnover it is: on ``80`` it charged
        # the sacked offence **+4.56** EPA where ESPN's own row is -6.18. (The Shield adapter
        # emits ``80`` and is right to -- it also ships ``scoringType``. Choosing between the
        # two is the parked S2-P3b lead decision.)
        return "29" if fumbled and _fumble_type_id(play, player_team, False) == "29" else "7"
    if kind == "FUMBLE" or (fumbled and kind not in ("PASS", "RUSH")):
        return _fumble_type_id(play, player_team, touchdown)
    if kind in ("PASS_INCOMPLETE", "SPIKE"):
        return "3"
    if kind == "PASS":
        if touchdown and _scored_by_defence(play):
            return "39"
        return "67" if touchdown else ("29" if fumbled and _fumble_type_id(play, player_team, False) == "29" else "24")
    if kind == "RUSH":
        if touchdown and _scored_by_defence(play):
            return "39"
        return "68" if touchdown else ("29" if fumbled and _fumble_type_id(play, player_team, False) == "29" else "5")
    # An unmapped Yahoo kind must not silently become a snap: Penalty is the processor's own
    # neutral row (no rush/pass/kick flags fire on it), which is what an unknown row deserves.
    return "8"


def _scored_by_defence(play: Mapping[str, Any]) -> bool:
    """True when the feed credits the score to the club that did **not** snap the ball."""
    info = play.get("scoringPlayInfo") or {}
    scorer = info.get("scoringTeamId")
    return bool(scorer) and str(scorer) != str(play.get("teamId"))


def _point_after(play: Mapping[str, Any], text: str) -> Dict[str, Any]:
    """ESPN's ``pointAfterAttempt`` for a try (``abbreviation`` + ``value`` are what is read)."""
    if str(play.get("playTypeId") or "").startswith("TWO_POINT"):
        good = bool(_TWO_POINT_GOOD_RE.search(text)) or bool((play.get("scoringPlayInfo") or {}).get("scoreTypeId"))
        return {
            "id": 62 if good else 63,
            "text": "Two Point Conversion Good" if good else "Two Point Attempt Failed",
            "abbreviation": "Two Point Conversion Good" if good else "Two Point Attempt Failed",
            "value": 2 if good else 0,
        }
    good = bool(_PAT_GOOD_RE.search(text))
    return {
        "id": 61 if good else 64,
        "text": "Extra Point Good" if good else "Extra Point Missed",
        "abbreviation": "Extra Point Good" if good else "Extra Point Missed",
        "value": 1 if good else 0,
    }


def _sub_yards(play: Mapping[str, Any], sub_type: str) -> Optional[int]:
    """``subPlays[]`` yardage of one kind (``KICK`` / ``RETURN`` / ``PENALTY`` / ``RUSH`` ...)."""
    for sub in play.get("subPlays") or []:
        if isinstance(sub, dict) and sub.get("subPlayType") == sub_type:
            value = sub.get("yards")
            if value is not None:
                return int(value)
    return None


def _stat_yardage(play: Mapping[str, Any], type_id: str, text: str) -> int:
    """ESPN's ``statYardage``, which is not Yahoo's ``yards`` on a kick or a field goal.

    ESPN books a kickoff, a punt and an interception at the **return** yards (Yahoo's ``yards``
    is 0 on all three) and a field goal at its **distance** (Yahoo's is 0). Both come out of
    ``subPlays``, with the kick distance in the text as the fallback for a field goal.
    """
    if type_id in ("59", "60", "18", "38"):
        kick = _sub_yards(play, "KICK")
        if kick:
            return kick
        match = _FG_YARDS_RE.search(text)
        if match:
            return int(match.group(1))
    if type_id in ("53", "32", "52", "34", "17", "26", "36"):
        return _sub_yards(play, "RETURN") or 0
    try:
        return int(play.get("yards") or 0)
    except (TypeError, ValueError):
        return 0


# --------------------------------------------------------------------------------------
# text: Yahoo's own template family -> ESPN/GSIS grammar
# --------------------------------------------------------------------------------------
_RESULT = r"(?P<res>\d+ yard gain|\d+ yard loss|no gain|\d+ yard touchdown|\d+ yards?)"
# Player names arrive already abbreviated ("D.Godchaux"), so a tackler class must not
# exclude "." -- excluding it silently failed EVERY row whose tackler the feed names, which
# is 75 of 88 scrimmage plays, and left Yahoo's own wording (and the processor's regex
# columns) on all of them.
_TACKLE = r"(?:,? tackled by (?P<t1>[^,]+?)(?: and (?P<t2>[^,]+?))?)?"
_RUSH_DIR = {"up the middle": "up the middle", "to the left": "left end", "to the right": "right end"}
_PASS_DIR = {"down the middle": "middle", "to the left": "left", "to the right": "right"}
_DIR = r"(?P<dir>up the middle|down the middle|to the left|to the right)"

_RE_RUSH = re.compile(rf"^(?P<p>.+?) rushed(?: {_DIR})? for {_RESULT}{_TACKLE}\.?$")
_RE_PASS = re.compile(rf"^(?P<p>.+?) passed to (?P<r>.+?)(?: {_DIR})? for {_RESULT}{_TACKLE}\.?$")
_RE_INCOMPLETE = re.compile(rf"^(?P<p>.+?) incomplete pass(?: {_DIR})?(?: intended for (?P<r>.+?))?\.?$")
_RE_INT_MODERN = re.compile(
    rf"^(?P<p>.+?) pass intercepted(?: {_DIR})?\.\s*(?P<i>.+?) intercepted .+? for {_RESULT}{_TACKLE}\.?$"
)
_RE_INT_LEGACY = re.compile(rf"^(?P<i>.+?) intercepted (?P<p>.+?) for {_RESULT}{_TACKLE}\.?$")
#: The sacker is optional -- a coverage sack names nobody ("B.Nix sacked for 7 yard loss").
_RE_SACK = re.compile(rf"^(?P<p>.+?) sacked(?: by (?P<t1>.+?)(?: and (?P<t2>.+?))?)? for {_RESULT}\.?$")
_RE_PUNT = re.compile(
    r"^(?P<k>.+?) punted(?: for (?P<n>\d+) yards?)?"
    r"(?:[,.]\s*(?:no return|(?P<r>.+?) returned punt for (?P<res>\d+ yard gain|\d+ yard loss|no gain|\d+ yards?)))?"
    r"(?:,? tackled by (?P<t1>[^,]+?)(?: and (?P<t2>[^,]+?))?)?\.?$"
)
_RE_KICK_NO_RETURN = re.compile(
    r"^(?P<team>.+?) kicked off(?: for (?P<n>\d+) yards)?,\s*(?P<how>touchback|no return)\.?$"
)
_RE_KICK_RETURN = re.compile(
    rf"^(?P<team>.+?) kicked off(?: for (?P<n>\d+) yards)?,\s*(?P<r>.+?) returned kickoff for {_RESULT}{_TACKLE}\.?$"
)
_RE_FG = re.compile(r"^(?P<k>.+?) (?P<verb>kicked|missed|had blocked) an? (?P<n>\d+)-yard field goal")
_RE_PENALTY = re.compile(r"^(?P<team>.+?) (?:committed (?P<n>\d+) yard penalty|penalty)(?: \((?P<foul>[^)]+)\))?\.?$")
#: A **charged** timeout: the text names the club that called it. The ``TIMEOUT`` sub-play is
#: NOT a substitute -- it carries a ``teamId`` on the TV stoppages too (whoever had the ball),
#: so reading it instead charged 13-14 official timeouts per game to a club that never called
#: one, and with them the whole remainder of each half's ``posTeamTimeouts``.
_RE_TIMEOUT = re.compile(r"(?i)^(?P<team>.+?) (?:charged )?timeout\.?$")
#: A stoppage the feed attributes to nobody -- ESPN's ``Official Timeout`` (type 74), which is
#: what the majority of Yahoo's bare "Timeout" rows are.
_RE_OFFICIAL_TIMEOUT = re.compile(r"(?i)^(?:official )?timeout\.?$")
#: A fumble the feed appends to the play it happened on. Recovery, return yardage and the
#: return's own result are each optional: "X fumbled" alone, "... recovered fumble", "...
#: recovered fumble for no gain" and "... recovered fumble and returned for 27 yard touchdown"
#: all occur. Names are already abbreviated ("B.Nix"), so no class here may exclude ".".
_RE_FUMBLE_TAIL = re.compile(
    r"\.\s+(?P<f>.+?) fumbled\.?(?:\s*(?P<r>.+?) recovered fumble(?: (?:and returned )?for (?P<res>[^.]+?))?)?\.?$"
)
#: A penalty the feed appends to the play it happened on. The club class allows "." ("St.
#: Louis") and the match is anchored on a sentence break, so the leftmost break wins.
_RE_PENALTY_TAIL = re.compile(
    r"\.\s+(?P<team>[A-Z][\w.'\- ]+?) committed (?P<n>\d+) yard penalty(?: \((?P<foul>[^)]+)\))?\.?$"
)
#: A standalone ``FUMBLE`` row, and the blocked-kick / spike rows, which name their club or
#: player in a different order from every other kind.
_RE_FUMBLE_ROW = re.compile(
    r"^(?P<f>.+?) fumbled\.?(?:\s*(?P<r>.+?) recovered fumble(?: (?:and returned )?for (?P<res>[^.]+?))?)?\.?$"
)
_RE_FG_BLOCKED = re.compile(r"^.+? blocked (?P<n>\d+)-yard field goal attempt by (?P<k>\S+?)\.?(?:\s|$)")
_RE_SPIKE = re.compile(r"^(?P<p>.+?) spiked the ball\.?$")
_RE_PUNT_TOUCHBACK = re.compile(r"^(?P<k>.+?) punted(?: for (?P<n>\d+) yards?)?,\s*touchback\.?$")
_RE_END_QUARTER = re.compile(r"(?i)^end of (\d)\w\w quarter$")
_CLOCK_PARTS_RE = re.compile(r"^(\d{1,2}):(\d{2})$")


def _yards_phrase(result: Optional[str]) -> str:
    """Yahoo's ``"4 yard gain"`` / ``"no gain"`` / ``"9 yard touchdown"`` -> ESPN's yardage clause."""
    if not result:
        return ""
    if result == "no gain":
        return "for no gain"
    match = re.match(r"(\d+)", result)
    yards = int(match.group(1)) if match else 0
    if "loss" in result:
        return f"for -{yards} yards"
    return f"for {yards} yards" + (", TOUCHDOWN" if "touchdown" in result else "")


def _tacklers(match: "re.Match[str]") -> str:
    """``" (A; B)"`` -- ESPN's tackler parenthetical, empty when the feed names nobody."""
    try:
        first, second = match.group("t1"), match.group("t2")
    except IndexError:
        return ""
    if not first:
        return ""
    return f" ({first}" + (f"; {second}" if second else "") + ")"


def _spot(own_abbr: str, opp_abbr: str, own_yard_line: int) -> str:
    """``("CLE", "JAX", 20)`` -> ``"CLE 20"``; midfield is ESPN's bare ``"50"``."""
    own_yard_line = max(0, min(100, own_yard_line))
    if own_yard_line == 50:
        return "50"
    return f"{own_abbr} {own_yard_line}" if own_yard_line < 50 else f"{opp_abbr} {100 - own_yard_line}"


class _TextContext:
    """Per-game state the re-skin needs: names, club codes, kickers and the timeout tally."""

    def __init__(self, abbr_by_yahoo: Mapping[str, str], locations: Mapping[str, str]) -> None:
        self.abbr_by_yahoo = dict(abbr_by_yahoo)
        self.locations = dict(locations)  # club location / full name -> Yahoo team id
        self.kicker: Dict[str, str] = {}
        self.timeouts: Dict[Tuple[str, int], int] = {}

    def abbr(self, yahoo_team_id: Any, default: str = "") -> str:
        return self.abbr_by_yahoo.get(str(yahoo_team_id), default)

    def team_from_text(self, text: str, default: str) -> str:
        """The club a "Cincinnati timeout" / "Buffalo committed ..." row names, as an ESPN code."""
        for name, team_id in self.locations.items():
            if name and text.startswith(name):
                return self.abbr(team_id, default)
        return default


def _last(pattern: "re.Pattern[str]", text: str) -> "Optional[re.Match[str]]":
    """The **last** match of ``pattern`` in ``text``; the trailing clauses are read right to left."""
    match = None
    for match in pattern.finditer(text):
        pass
    return match


def _sub_team(play: Mapping[str, Any], sub_type: str) -> Optional[str]:
    for sub in play.get("subPlays") or []:
        if isinstance(sub, dict) and sub.get("subPlayType") == sub_type and sub.get("teamId"):
            return str(sub["teamId"])
    return None


def _reskin(play: Mapping[str, Any], text: str, ctx: _TextContext, player_team: Mapping[str, str]) -> Tuple[str, bool]:
    """Yahoo play text -> ESPN/GSIS grammar; ``(text, matched)``.

    ``matched=False`` leaves Yahoo's own wording in place rather than inventing one: the
    processor's regex columns then degrade for that row, which is a documented loss, while a
    fabricated sentence would be a wrong one. Only facts present in the payload are written --
    no pass depth, no formation, no long snapper, no fair catch, no penalty player, because
    Yahoo carries none of them.
    """
    kind = str(play.get("playTypeId") or "")
    possession = ctx.abbr(play.get("teamId"))
    opponent = next((a for t, a in ctx.abbr_by_yahoo.items() if t != str(play.get("teamId"))), possession)
    # A pre-2020 feed writes the try INSIDE the touchdown's own text ("... for 2 yard touchdown.
    # J.Tucker made PAT"); it is folded onto the play separately, so it must not defeat the match
    # here -- it did, on every pre-2020 touchdown.
    body = _INLINE_PAT_RE.sub("", text).strip()

    tail = ""
    # the LAST sentence break, not the first: "punted. X returned punt for 1 yard loss. X
    # fumbled. X recovered fumble" has two, and cutting at the first throws the return away.
    penalty_tail = _last(_RE_PENALTY_TAIL, body)
    if penalty_tail and kind != "PENALTY":
        on = ctx.abbr(_sub_team(play, "PENALTY"), ctx.team_from_text(penalty_tail.group("team"), possession))
        foul = penalty_tail.group("foul") or "Penalty"
        tail = f" PENALTY on {on}, {foul}, {penalty_tail.group('n')} yards."
        body = body[: penalty_tail.start()]

    fumble = ""
    fumble_tail = _last(_RE_FUMBLE_TAIL, body)
    if fumble_tail:
        recovered_by = _RECOVERED_RE.search(str(play.get("text") or ""))
        recovering_team = player_team.get(recovered_by.group(1)) if recovered_by else None
        own = recovering_team is None or str(recovering_team) == str(play.get("teamId"))
        club = ctx.abbr(recovering_team, possession if own else opponent)
        verb = "recovered by" if own else "RECOVERED by"
        gained = _yards_phrase(fumble_tail.group("res")) if fumble_tail.group("res") else ""
        recovered = fumble_tail.group("r")
        fumble = (
            " FUMBLES."
            if not recovered
            else f" FUMBLES, {verb} {club}-{recovered}." + (f" {recovered} {gained}." if gained else "")
        )
        body = body[: fumble_tail.start()]

    if kind == "RUSH" and (m := _RE_RUSH.match(body)):
        direction = _RUSH_DIR.get(m.group("dir") or "", "")
        return (
            f"{m.group('p')} {direction + ' ' if direction else ''}{_yards_phrase(m.group('res'))}{_tacklers(m)}.{fumble}{tail}",
            True,
        )
    if kind == "PASS" and (m := _RE_PASS.match(body)):
        direction = _PASS_DIR.get(m.group("dir") or "", "")
        where = f" {direction}" if direction else ""
        return (
            f"{m.group('p')} pass{where} to {m.group('r')} {_yards_phrase(m.group('res'))}{_tacklers(m)}.{fumble}{tail}",
            True,
        )
    if kind == "SPIKE" and (m := _RE_SPIKE.match(body)):
        return f"{m.group('p')} spiked the ball to stop the clock.{tail}", True
    if kind == "FUMBLE" and (m := _RE_FUMBLE_ROW.match(body)):
        recovered_by = _RECOVERED_RE.search(str(play.get("text") or ""))
        recovering_team = player_team.get(recovered_by.group(1)) if recovered_by else None
        own = recovering_team is None or str(recovering_team) == str(play.get("teamId"))
        club = ctx.abbr(recovering_team, possession if own else opponent)
        recovered, gained = m.group("r"), (_yards_phrase(m.group("res")) if m.group("res") else "")
        out = f"{m.group('f')} FUMBLES."
        if recovered:
            out += f" {'recovered by' if own else 'RECOVERED by'} {club}-{recovered}." + (
                f" {recovered} {gained}." if gained else ""
            )
        return f"{out}{tail}", True
    if kind in ("PASS_INCOMPLETE", "SPIKE") and (m := _RE_INCOMPLETE.match(body)):
        direction = _PASS_DIR.get(m.group("dir") or "", "")
        where = f" {direction}" if direction else ""
        target = f" to {m.group('r')}" if m.group("r") else ""
        return f"{m.group('p')} pass incomplete{where}{target}.{tail}", True
    if kind == "PASS_INTERCEPTED":
        m = _RE_INT_MODERN.match(body) or _RE_INT_LEGACY.match(body)
        if m:
            direction = _PASS_DIR.get((m.groupdict().get("dir") or ""), "")
            where = f" {direction}" if direction else ""
            return (
                f"{m.group('p')} pass{where} INTERCEPTED by {m.group('i')}. "
                f"{m.group('i')} {_yards_phrase(m.group('res'))}{_tacklers(m)}.{fumble}{tail}",
                True,
            )
    if kind == "SACK" and (m := _RE_SACK.match(body)):
        if m.group("t2"):
            by = f" (sack split by {m.group('t1')} and {m.group('t2')})"
        elif m.group("t1"):
            by = f" ({m.group('t1')})"
        else:
            by = ""
        return f"{m.group('p')} sacked {_yards_phrase(m.group('res'))}{by}.{fumble}{tail}", True
    if kind == "PUNT" and (m := _RE_PUNT_TOUCHBACK.match(body)):
        return f"{m.group('k')} punts {int(m.group('n') or 0)} yards to end zone, Touchback.{tail}", True
    if kind == "PUNT" and (m := _RE_PUNT.match(body)):
        distance = int(m.group("n") or 0)
        to_endzone = play.get("yardsToEndzone")
        landed = (int(to_endzone) - distance) if to_endzone is not None else None
        where = f" to {_spot(opponent, possession, landed)}" if landed is not None else ""
        returned = f" {m.group('r')} {_yards_phrase(m.group('res'))}{_tacklers(m)}." if m.group("r") else ""
        return f"{m.group('k')} punts {distance} yards{where}.{returned}{fumble}{tail}", True
    if kind == "KICKOFF":
        kicker = ctx.kicker.get(str(play.get("teamId")), possession)
        if m := _RE_KICK_NO_RETURN.match(body):
            distance = int(m.group("n") or 65)
            ending = ", Touchback." if m.group("how") == "touchback" else "."
            return f"{kicker} kicks {distance} yards from {possession} 35 to end zone{ending}{tail}", True
        if m := _RE_KICK_RETURN.match(body):
            distance = int(m.group("n") or 0)
            landed = 100 - (35 + distance)
            return (
                f"{kicker} kicks {distance} yards from {possession} 35 to {_spot(opponent, possession, landed)}. "
                f"{m.group('r')} {_yards_phrase(m.group('res'))}{_tacklers(m)}.{fumble}{tail}",
                True,
            )
    if kind == "FIELD_GOAL_ATTEMPT" and (m := _RE_FG_BLOCKED.match(body)):
        return f"{m.group('k')} {m.group('n')} yard field goal is BLOCKED.{tail}", True
    if kind == "FIELD_GOAL_ATTEMPT" and (m := _RE_FG.match(body)):
        outcome = {"kicked": "GOOD", "missed": "No Good", "had blocked": "BLOCKED"}[m.group("verb")]
        return f"{m.group('k')} {m.group('n')} yard field goal is {outcome}.{tail}", True
    if kind == "PENALTY" and (m := _RE_PENALTY.match(body)):
        on = ctx.abbr(_sub_team(play, "PENALTY"), ctx.team_from_text(m.group("team"), possession))
        spot = play.get("yardLine") or "50"
        foul = m.group("foul") or "Penalty"
        return f"PENALTY on {on}, {foul}, {m.group('n') or 0} yards, enforced at {spot} - No Play.", True
    if kind == "TIMEOUT":
        if "two-minute" in body.lower() or "2-minute" in body.lower():
            return "Two-Minute Warning", True
        if _RE_OFFICIAL_TIMEOUT.match(body.strip()):
            return f"Official Timeout at {_clock(play.get('clock'))}.", True
        if m := _RE_TIMEOUT.match(body):
            # the TEXT names the club, and only the text: see ``_RE_TIMEOUT``. A club the header
            # does not carry falls back to the sub-play rather than to possession, and a timeout
            # is never charged to a club the feed did not name.
            club = ctx.team_from_text(m.group("team"), "") or ctx.abbr(_sub_team(play, "TIMEOUT"), possession)
            half = 1 if int(play.get("period") or 1) <= 2 else 2
            key = (club, half)
            ctx.timeouts[key] = ctx.timeouts.get(key, 0) + 1
            return f"Timeout #{ctx.timeouts[key]} by {club} at {_clock(play.get('clock'))}.", True
    if kind == "END_OF_PERIOD":
        low = body.lower()
        if "game" in low:
            return "END GAME", True
        if "half" in low:
            return "END QUARTER 2", True
        if "regulation" in low:
            return "END QUARTER 4", True
        if m := _RE_END_QUARTER.match(body):
            return f"END QUARTER {m.group(1)}", True
        return body, True
    return text, False


# --------------------------------------------------------------------------------------
# end state
# --------------------------------------------------------------------------------------
def _trailing_end(play: Dict[str, Any], home_id: str) -> Dict[str, Any]:
    """The end state of a play the feed states no next snap for, derived from its own yardage.

    Fires on the last row of every final and on the newest row of every live poll -- the row
    Game on Paper renders at the top of the page. Taking the play's own start says the ball
    never moved, so a 19-yard gain is scored as a 0-yard one against a spot no feed reports.

    On a plain scrimmage snap (:data:`_KEEPS_THE_BALL`) the offence keeps the ball, so the end
    spot is ``start - statYardage``. On a turnover or a kick the yardage says nothing about
    where the next team starts, so those keep the start spot rather than a guess.
    """
    start = play["start"]
    gained = int(play.get("statYardage") or 0) if play["type"]["id"] in _KEEPS_THE_BALL else 0
    to_endzone = start["yardsToEndzone"]
    if to_endzone is not None and gained:
        to_endzone = max(0, min(100, int(to_endzone) - gained))
    is_home = str(start["team"]["id"]) == str(home_id)
    return {
        "down": start["down"],
        "distance": start["distance"],
        "yardLine": start["yardLine"] if to_endzone is None else ((100 - to_endzone) if is_home else to_endzone),
        "yardsToEndzone": to_endzone,
        "team": {"id": start["team"]["id"]},
    }


def _fill_end_state(plays: List[Dict[str, Any]], home_id: str, scoring_teams: Mapping[str, str]) -> None:
    """Fill every play's ``end`` from the **next** snap's start, across drive boundaries.

    Yahoo states no end state at all, so the next snap is it -- which is how ESPN builds its
    own, and why the search must not stop at a drive boundary: a punt ends in the receiving
    team's frame, on the first row of the next drive. Clock stoppages carry no state of their
    own and are skipped, so the end spot of the play before a timeout is the ball's real spot.

    Two rows instead take ESPN's own scoring convention, read off the captured ESPN NFL
    summaries: a **touchdown** ends at the goal line the scoring team was attacking
    (``down -1``, ``yardsToEndzone 0``) credited to **the team the feed says scored** -- the
    defence on a pick six or a strip-sack return, where crediting the offence flips the spot
    100 yards -- and a **made field goal** ends where it was kicked from, credited to the
    kicking team, not at the ensuing kickoff.
    """
    for i, play in enumerate(plays):
        nxt = next((p["start"] for p in plays[i + 1 :] if p["type"]["text"] not in _STOPPAGE), None)
        type_id = play["type"]["id"]
        if play["scoringPlay"] and play["type"]["abbreviation"] == "TD":
            team = scoring_teams.get(play["id"]) or play["start"]["team"]["id"]
            play["end"] = {
                "down": -1,
                "distance": 0,
                "yardLine": 100 if str(team) == str(home_id) else 0,
                "yardsToEndzone": 0,
                "team": {"id": team},
            }
            continue
        if type_id == "59":
            team = scoring_teams.get(play["id"]) or play["start"]["team"]["id"]
            play["end"] = {
                "down": -1,
                "distance": -1,
                "yardLine": play["start"]["yardLine"],
                "yardsToEndzone": play["start"]["yardsToEndzone"],
                "team": {"id": team},
            }
            continue
        if nxt is None:
            play["end"] = _trailing_end(play, home_id)
            continue
        play["end"] = {
            "down": nxt["down"],
            "distance": nxt["distance"],
            "yardLine": nxt["yardLine"],
            "yardsToEndzone": nxt["yardsToEndzone"],
            "team": {"id": nxt["team"]["id"]},
        }


# --------------------------------------------------------------------------------------
# drives
# --------------------------------------------------------------------------------------
def _drive(
    event_id: str, index: int, drive: Mapping[str, Any], plays: List[Dict[str, Any]], abbr: Optional[str]
) -> Dict[str, Any]:
    """One ``drives.previous[]`` / ``drives.current`` entry; ESPN's drive id is ``{event}{1-based index}``."""
    result = drive.get("result")
    return {
        "id": f"{event_id}{index}",
        "description": f"{drive.get('numPlays')} plays, {drive.get('yards')} yards, {drive.get('duration')}",
        "team": {
            "shortDisplayName": abbr,
            "displayName": abbr,
            "name": abbr,
            "abbreviation": abbr,
        },
        "start": {
            "period": {"number": (plays[0]["period"]["number"] if plays else None), "type": "quarter"},
            "clock": {"displayValue": plays[0]["clock"]["displayValue"] if plays else "0:00"},
            "yardLine": plays[0]["start"]["yardLine"] if plays else None,
            "text": drive.get("yardLine"),
        },
        "end": {
            "period": {"number": (plays[-1]["period"]["number"] if plays else None), "type": "quarter"},
            "clock": {"displayValue": plays[-1]["clock"]["displayValue"] if plays else "0:00"},
            "yardLine": plays[-1]["end"]["yardLine"] if plays else None,
        },
        "timeElapsed": {"displayValue": drive.get("duration")},
        "yards": drive.get("yards"),
        "isScore": str(result or "").upper() in ("TD", "FG", "TOUCHDOWN", "FIELD GOAL"),
        "offensivePlays": drive.get("numPlays"),
        "result": result,
        "shortDisplayResult": result,
        "displayResult": result,
        "plays": plays,
    }


def _elapsed(first: Mapping[str, Any], last: Mapping[str, Any]) -> str:
    """``"3:41"`` -- wall time between two plays' period+clock, for a synthesized drive."""

    def seconds(play: Mapping[str, Any]) -> Optional[int]:
        period = play.get("period", {}).get("number")
        match = _CLOCK_PARTS_RE.match(str(play.get("clock", {}).get("displayValue") or ""))
        if not period or not match:
            return None
        return (int(period) - 1) * 900 + (900 - int(match.group(1)) * 60 - int(match.group(2)))

    start, end = seconds(first), seconds(last)
    if start is None or end is None or end < start:
        return "0:00"
    return f"{(end - start) // 60}:{(end - start) % 60:02d}"


def _synthesized_meta(segment: Sequence[Dict[str, Any]]) -> Dict[str, Any]:
    """Drive meta for a segment Yahoo shipped no chart entry for.

    ``yards`` and ``duration`` are derived rather than left null: the processor's box score
    **sums** the drive columns, and an all-null column arrives as ``Utf8`` through
    ``json_normalize`` -- which is an ``InvalidOperationError``, not a null row, and it took
    every pre-2020 game down.
    """
    scrimmage = [p for p in segment if p["type"]["id"] not in _KICKOFF_TYPES]
    first = scrimmage[0] if scrimmage else segment[0]
    start, end = first["start"].get("yardsToEndzone"), segment[-1]["end"].get("yardsToEndzone")
    yards = 0 if start is None or end is None else int(start) - int(end)
    return {
        "numPlays": len(segment),
        "yards": max(-100, min(100, yards)),
        "duration": _elapsed(segment[0], segment[-1]),
        "yardLine": first["start"].get("yardLine"),
    }


def _synthesize_drives(emitted: Sequence[Dict[str, Any]]) -> List[List[Dict[str, Any]]]:
    """Segment plays into drives when Yahoo ships no drive chart (every season before 2020).

    A drive opens on a kickoff and on every change of possession among real snaps, which is
    ESPN's own grouping: its NFL drives are the **receiving** team's and start with the kickoff
    row (whose possession is the kicking team's). Clock stoppages carry the preceding snap's
    state and never open a drive.
    """
    groups: List[List[Dict[str, Any]]] = []
    current: List[Dict[str, Any]] = []
    possession: Optional[str] = None
    for play in emitted:
        stoppage = play["type"]["text"] in _STOPPAGE
        kickoff = play["type"]["id"] in _KICKOFF_TYPES
        team = str(play["start"]["team"]["id"]) if play["start"]["team"]["id"] is not None else None
        if current and not stoppage and (kickoff or (not kickoff and possession is not None and team != possession)):
            groups.append(current)
            current = []
            possession = None
        current.append(play)
        if not stoppage and not kickoff and team is not None:
            possession = team
    if current:
        groups.append(current)
    return groups


# --------------------------------------------------------------------------------------
# header
# --------------------------------------------------------------------------------------
def _split_name(team: Mapping[str, Any], abbreviation: Optional[str]) -> Tuple[str, str]:
    """``"Jacksonville Jaguars"`` -> ``("Jacksonville", "Jaguars")``.

    Every NFL club's mascot is a single token ("49ers", "Commanders"), so the split is exact. A
    payload with no ``fullName`` degrades to the abbreviation rather than an empty mascot -- an
    empty ``team.name`` makes every Timeout row match both clubs (contract ``_check_values``).
    """
    full_name = str(team.get("fullName") or "").strip()
    location, _, mascot = full_name.rpartition(" ")
    mascot = mascot or (abbreviation or "")
    return location or mascot, mascot


def _competitor(
    side: str,
    order: int,
    espn_team_id: str,
    team: Mapping[str, Any],
    abbreviation: Optional[str],
    score: Any,
    linescore: Sequence[Mapping[str, Any]],
    winner: bool,
) -> Dict[str, Any]:
    """One ``header.competitions[0].competitors[]`` entry, home first.

    The abbreviation is **ESPN's** (``LAR``, ``WSH``) and is never invented: it comes from the
    id map's era-correct value or the franchise table, both by way of ``_espn_abbr``.
    """
    location, mascot = _split_name(team, abbreviation or espn_team_id)
    _abbr, primary, alternate = _ESPN_TEAMS.get(str(espn_team_id), ("", "000000", "ffffff"))
    return {
        "id": str(espn_team_id),
        "uid": f"s:20~l:28~t:{espn_team_id}",
        "order": order,
        "homeAway": side,
        "winner": bool(winner),
        "team": {
            "id": str(espn_team_id),
            "uid": f"s:20~l:28~t:{espn_team_id}",
            "location": location,
            "name": mascot,
            "nickname": mascot,
            "abbreviation": abbreviation,
            "displayName": str(team.get("fullName") or "").strip() or f"{location} {mascot}".strip(),
            "shortDisplayName": mascot,
            "color": team.get("primaryColor") or primary,
            "alternateColor": team.get("secondaryColor") or alternate,
            "logos": [],
        },
        "score": str(score if score is not None else 0),
        "linescores": [{"displayValue": str((q or {}).get("score") or 0)} for q in linescore or []],
        "record": [],
    }


_SEASON_PHASE = {"PRESEASON": 1, "REGULAR_SEASON": 2, "POSTSEASON": 3, "PRO_BOWL": 4}


def _header(
    game: Mapping[str, Any], event_id: str, home_id: str, away_id: str, abbrs: Mapping[str, Optional[str]]
) -> Dict[str, Any]:
    """The renderable ``header``: season / week / competitors / status, all from the payload."""
    home_score, away_score = game.get("homeScore"), game.get("awayScore")
    final = str(game.get("status") or "").upper().startswith("FINAL")
    return {
        "id": event_id,
        "uid": f"s:20~l:28~e:{event_id}",
        "season": {
            "year": game.get("season"),
            "type": _SEASON_PHASE.get(str(game.get("seasonPhase") or "").upper(), 2),
        },
        "week": game.get("week"),
        "timeValid": True,
        "competitions": [
            {
                "id": event_id,
                "uid": f"s:20~l:28~e:{event_id}~c:{event_id}",
                "date": game.get("startTime"),
                "neutralSite": False,
                "conferenceCompetition": False,
                "boxscoreAvailable": False,
                "commentaryAvailable": False,
                "liveAvailable": False,
                "onWatchESPN": False,
                "recent": False,
                "boxscoreSource": "none",
                "playByPlaySource": "full",
                "status": _status(game),
                "competitors": [
                    _competitor(
                        "home",
                        0,
                        home_id,
                        game.get("homeTeam") or {},
                        abbrs.get(home_id),
                        home_score,
                        game.get("homeLineScore") or [],
                        final and (home_score or 0) > (away_score or 0),
                    ),
                    _competitor(
                        "away",
                        1,
                        away_id,
                        game.get("awayTeam") or {},
                        abbrs.get(away_id),
                        away_score,
                        game.get("awayLineScore") or [],
                        final and (away_score or 0) > (home_score or 0),
                    ),
                ],
            }
        ],
    }


# --------------------------------------------------------------------------------------
# the projection
# --------------------------------------------------------------------------------------
def _score_is_stated_per_play(plays: Sequence[Mapping[str, Any]]) -> bool:
    """True when the feed maintains a running scoreboard on **every** row.

    Yahoo's pre-2020 NFL feed states the score only on scoring plays; every other row reads
    0-0, which -- taken literally -- hands the processor a game that is tied from the opening
    kickoff to the final whistle and wrecks every score-differential EP/WP feature. Detected by
    shape rather than by season: if no non-scoring row states a score while the game has one,
    the scoreboard is reconstructed by carrying each scoring play's post-score forward.
    """
    scored = any(int(p.get("homeScore") or 0) + int(p.get("awayScore") or 0) for p in plays if not p.get("isScoring"))
    return scored or not any(p.get("isScoring") for p in plays)


def _short_names(game: Mapping[str, Any]) -> Dict[str, str]:
    """``player id -> "D.Watson"`` -- ESPN NFL text abbreviates the first name, Yahoo does not."""
    names: Dict[str, str] = {}
    for side in ("homeTeamLineup", "awayTeamLineup"):
        for entry in game.get(side) or []:
            player = (entry or {}).get("player") or {}
            pid, short = player.get("playerId"), player.get("shortDisplayName") or player.get("displayName")
            if pid and short:
                names[pid] = _SUFFIX_RE.sub("", str(short).replace(". ", ".")).strip()
    return names


def _yahoo_to_espn_summary(
    payload: Mapping[str, Any], idmap_row: Mapping[str, Any], *, odds: Optional[Mapping[str, Any]] = None
) -> Tuple[Dict[str, Any], List[str]]:
    """Project one Yahoo NFL game (final or in progress) onto an ESPN-summary-shaped dict.

    Args:
        payload: A shangrila ``playbookBoxscore`` / ``playbookBoxscorePoll`` body (the
            ``{"data": {"games": [...]}}`` envelope) or the game object itself.
        idmap_row: The game's id-map row
            (:data:`sportsdataverse.football.sources.idmap.GAME_SCHEMA`). ``espn_event_id`` is
            required; ``home_espn_team_id`` / ``away_espn_team_id`` are used when present and
            otherwise read off Yahoo's own ``nfl.t.N`` numbers, which **are** the ESPN team ids
            (32/32 measured) -- ``NFLPlayProcess`` uses ``team.id`` for possession, logos and
            the box score, so they must be ESPN's.
        odds: ``{gameSpread, overUnder, homeFavorite, gameSpreadAvailable}`` (a stored closing
            line). Becomes the summary's one-provider ``pickcenter``; when None, Yahoo's own
            pregame line is used if it states one.

    Returns:
        ``(summary, notes)``.

        | item | type | description |
        |---|---|---|
        | summary | dict | An ESPN-summary-shaped payload: `header`, `drives.previous` (+ `drives.current` while the game is live), `gameInfo`, `pickcenter` and empty `boxscore` / passthrough arrays. Feed it to `espn_nfl_pbp(summary=)`. |
        | notes | list[str] | Adapter-side degradations worth surfacing in provenance: an unresolved player-name map, rows whose text could not be re-skinned into ESPN grammar, a try with no touchdown to fold into, a reconstructed (pre-2020) scoreboard, a synthesized drive chart, plays outside the drive chart, and the open drive synthesized for a live game. |

    Raises:
        KeyError: ``idmap_row`` is missing ``espn_event_id``.
        ValueError: the payload carries no game object, or states no home/away team id
            (possession could not be attributed to either club).

    Example:
        Adapt a stored Yahoo final and process it::

            import json
            from sportsdataverse.nfl import NFLPlayProcess
            from sportsdataverse.nfl.yahoo_pbp.to_espn_summary import _yahoo_to_espn_summary

            with open("yahoo_nfl.g.20260913030.json") as fh:
                payload = json.load(fh)
            row = {"espn_event_id": "401872922", "home_espn_team_id": "30", "away_espn_team_id": "5"}
            summary, notes = _yahoo_to_espn_summary(payload, row)
            proc = NFLPlayProcess(gameId=401872922, join_participants=False)
            proc.espn_nfl_pbp(summary=summary)
            result = proc.run_processing_pipeline()
    """
    game = _resolve_game(payload)
    if game is None:
        raise ValueError("payload carries no shangrila game object")
    notes: List[str] = []

    event_id = str(idmap_row["espn_event_id"])
    yahoo_home, yahoo_away = game.get("homeTeamId"), game.get("awayTeamId")
    home_id = str(idmap_row.get("home_espn_team_id") or _espn_team_number(yahoo_home) or "")
    away_id = str(idmap_row.get("away_espn_team_id") or _espn_team_number(yahoo_away) or "")
    espn_by_yahoo = {str(yahoo_home): home_id, str(yahoo_away): away_id}
    if not (home_id and away_id) or len(espn_by_yahoo) < 2 or "None" in espn_by_yahoo:
        # A null Yahoo team id must not become a dict KEY: with both sides null the mapping
        # collapses to one entry, every play's start.team.id is the same club and the whole
        # scoreboard is credited to one side -- silently, since the contract's all-null test
        # only fires at a null rate of 1.0.
        raise ValueError("payload states no home/away team id: possession cannot be attributed")

    abbrs: Dict[str, Optional[str]] = {
        home_id: _espn_abbr(idmap_row, "home", home_id),
        away_id: _espn_abbr(idmap_row, "away", away_id),
    }
    names = _short_names(game)
    _, player_team = _lineups(game)
    if not names:
        notes.append("no lineups in the payload: play text keeps Yahoo's [nfl.p.N] placeholders")
    ctx = _TextContext(
        {str(yahoo_home): abbrs.get(home_id) or home_id, str(yahoo_away): abbrs.get(away_id) or away_id},
        {
            str((game.get("homeTeam") or {}).get("location") or ""): str(yahoo_home),
            str((game.get("awayTeam") or {}).get("location") or ""): str(yahoo_away),
            str((game.get("homeTeam") or {}).get("fullName") or ""): str(yahoo_home),
            str((game.get("awayTeam") or {}).get("fullName") or ""): str(yahoo_away),
        },
    )

    plays_raw = sorted(
        (p for p in (game.get("playByPlay") or []) if isinstance(p, dict) and p.get("playId") is not None),
        key=lambda p: p["playId"],
    )
    # Yahoo repeats the last regulation row as both "End of Regulation" and "End of Game"; ESPN
    # emits one. Drop the first of the pair so the play counts line up.
    texts = [str(p.get("text") or "") for p in plays_raw]
    plays_raw = [
        p
        for i, p in enumerate(plays_raw)
        if not (
            texts[i].strip().lower() == "end of regulation"
            and i + 1 < len(texts)
            and texts[i + 1].strip().lower() == "end of game"
        )
    ]
    # the kicker each club used, so a kickoff row (which names only the club) can be written in
    # ESPN's grammar. Yahoo names the kicker on its field-goal and try rows.
    for play in plays_raw:
        if str(play.get("playTypeId") or "") in ("EXTRA_POINT_ATTEMPT", "FIELD_GOAL_ATTEMPT") and play.get("playerIds"):
            ctx.kicker.setdefault(str(play.get("teamId")), names.get(play["playerIds"][0], ""))
    ctx.kicker = {k: v for k, v in ctx.kicker.items() if v}

    running_score = _score_is_stated_per_play(plays_raw)
    if not running_score:
        notes.append("feed states the score only on scoring plays: the scoreboard was carried forward from them")

    emitted: List[Dict[str, Any]] = []
    by_play_id: Dict[Any, Dict[str, Any]] = {}
    scoring_teams: Dict[str, str] = {}
    last_touchdown: Optional[int] = None
    last_team: Optional[str] = None
    last_home_score, last_away_score = 0, 0
    unmapped_team_rows, unreskinned = 0, 0
    for play in plays_raw:
        text = _text(play.get("text"), names)
        info = play.get("scoringPlayInfo") or {}
        kind = str(play.get("playTypeId") or "")
        if kind in _PAT_TYPES:
            # ESPN folds the try into its touchdown: same play id, one text, one score step.
            # The try is NOT always the row right after the touchdown -- a timeout, a penalty on
            # the try or a replay review sits between them -- so this anchors on the newest
            # touchdown rather than on ``emitted[-1]``, which would hang ``pointAfterAttempt``
            # off a Timeout row and leave the touchdown stepping the scoreboard by 6.
            if last_touchdown is None:
                notes.append(f"play {play.get('playId')}: {kind} with no touchdown to fold into")
                continue
            target = emitted[last_touchdown]
            kicker = names.get((play.get("playerIds") or [None])[0] or "", "")
            good = bool(_PAT_GOOD_RE.search(text)) or bool(_TWO_POINT_GOOD_RE.search(text))
            if kind == "EXTRA_POINT_ATTEMPT":
                target["text"] = f"{target['text']} {kicker} extra point is {'GOOD' if good else 'No Good'}.".strip()
            elif text and text not in target["text"]:
                target["text"] = f"{target['text']} ({text})"
            target["pointAfterAttempt"] = _point_after(play, text)
            # the try's own score is the post-try one; carry it onto the touchdown and any row
            # between them, so the scoreboard never steps backwards
            if running_score:
                for later in emitted[last_touchdown:]:
                    later["homeScore"], later["awayScore"] = play.get("homeScore"), play.get("awayScore")
                last_home_score = int(play.get("homeScore") or last_home_score)
                last_away_score = int(play.get("awayScore") or last_away_score)
            continue

        type_id = _type_id(play, text, player_team)
        label, abbreviation = ESPN_PLAY_TYPES[type_id]
        team_id = espn_by_yahoo.get(str(play.get("teamId")))
        if team_id is None:
            # Yahoo ships an occasional row credited to a placeholder club. Carry the previous
            # row's possession rather than emitting a null: a null start.team.id is what the
            # processor forward-fills anyway, but on the way it nulls start.yardLine and floats
            # the whole column.
            team_id = last_team
            unmapped_team_rows += 1
        last_team = team_id or last_team
        to_endzone = play.get("yardsToEndzone")
        yard_line = None
        if to_endzone is not None and team_id is not None:
            yard_line = (100 - int(to_endzone)) if team_id == home_id else int(to_endzone)
        # ESPN states a kickoff as down 0 / distance 0; Yahoo repeats the ensuing 1st & 10.
        kickoff = type_id in _KICKOFF_TYPES
        down = 0 if kickoff else int(play.get("down") or 0)
        distance = 0 if kickoff else int(play.get("distance") or 0)
        scoring = bool(play.get("isScoring"))
        # The row's own score is the score BEFORE the try; scoringPlayInfo states the score
        # after the whole scoring sequence, which is what ESPN puts on the folded row. A feed
        # that states no per-play score carries the last scoring play's total forward.
        home_score = info.get("homeScore") if scoring and info else (play.get("homeScore") if running_score else None)
        away_score = info.get("awayScore") if scoring and info else (play.get("awayScore") if running_score else None)
        home_score = last_home_score if home_score is None else int(home_score)
        away_score = last_away_score if away_score is None else int(away_score)
        last_home_score, last_away_score = home_score, away_score
        reskinned, matched = _reskin(play, text, ctx, player_team)
        if not matched:
            unreskinned += 1
        espn_play: Dict[str, Any] = {
            "id": f"{event_id}{int(play['playId']):04d}",
            "sequenceNumber": str(play["playId"]),
            "type": {"id": type_id, "text": label, "abbreviation": abbreviation},
            "text": reskinned,
            "awayScore": away_score,
            "homeScore": home_score,
            "period": {"number": play.get("period")},
            "clock": {"displayValue": _clock(play.get("clock"))},
            "scoringPlay": scoring,
            "priority": scoring,
            "statYardage": _stat_yardage(play, type_id, text),
            "start": {
                "down": down,
                "distance": distance,
                "yardLine": yard_line,
                "yardsToEndzone": to_endzone,
                "downDistanceText": _down_distance_text(down, distance, to_endzone, play.get("yardLine")),
                "team": {"id": team_id},
            },
            "end": {},
        }
        scoring_team = espn_by_yahoo.get(str(info.get("scoringTeamId") or ""))
        if scoring_team:
            scoring_teams[espn_play["id"]] = scoring_team
        emitted.append(espn_play)
        by_play_id[play["playId"]] = espn_play
        if str(info.get("scoreTypeId") or "") == "TOUCHDOWN":
            last_touchdown = len(emitted) - 1
            # a pre-2020 feed writes the try inside the touchdown's own text; fold it here so
            # ``pointAfterAttempt`` exists in both eras
            inline = _INLINE_PAT_RE.search(text)
            if inline:
                espn_play["pointAfterAttempt"] = {
                    "id": 61 if inline.group("result").lower() == "made" else 64,
                    "text": "Extra Point Good" if inline.group("result").lower() == "made" else "Extra Point Missed",
                    "abbreviation": (
                        "Extra Point Good" if inline.group("result").lower() == "made" else "Extra Point Missed"
                    ),
                    "value": 1 if inline.group("result").lower() == "made" else 0,
                }
                espn_play["text"] = (
                    f"{espn_play['text']} {inline.group('kicker').strip()} extra point is "
                    f"{'GOOD' if inline.group('result').lower() == 'made' else 'No Good'}."
                )

    if unmapped_team_rows:
        notes.append(
            f"{unmapped_team_rows} plays name a team id the header does not carry: "
            "possession carried from the preceding play"
        )
    if unreskinned:
        notes.append(f"{unreskinned} of {len(emitted)} play texts kept Yahoo's own wording (no ESPN grammar match)")
    _fill_end_state(emitted, home_id, scoring_teams)

    drives_raw = [d for d in (game.get("drives") or []) if isinstance(d, dict)]
    grouped: List[Tuple[Dict[str, Any], List[Dict[str, Any]]]] = []
    if drives_raw:
        owner: Dict[Any, Any] = {}
        for drive in drives_raw:
            for pid in drive.get("playList") or []:
                owner[pid] = drive.get("driveId")
        last_owner = drives_raw[0].get("driveId")
        orphans = 0
        for play in plays_raw:
            pid = play["playId"]
            if pid not in owner:
                owner[pid] = last_owner
                orphans += 1
            last_owner = owner[pid]
        if orphans:
            notes.append(f"{orphans} plays fell outside the drive chart and were attached to the preceding drive")
        by_drive: Dict[Any, List[Dict[str, Any]]] = {}
        for play in plays_raw:
            emitted_play = by_play_id.get(play["playId"])
            if emitted_play is not None:
                by_drive.setdefault(owner.get(play["playId"]), []).append(emitted_play)
        grouped = [(d, by_drive.get(d.get("driveId")) or []) for d in drives_raw]
    else:
        # every season before 2020: Yahoo ships plays but no drive chart
        segments = _synthesize_drives(emitted)
        if segments:
            notes.append(f"payload carries no drive chart: {len(segments)} drives segmented from possession changes")
        grouped = [(_synthesized_meta(seg), seg) for seg in segments]

    final = str(game.get("status") or "").upper().startswith("FINAL")
    drives: List[Dict[str, Any]] = []
    for index, (meta, plays) in enumerate(grouped, start=1):
        if not plays:
            continue
        team_id = next(
            (p["start"]["team"]["id"] for p in plays if p["type"]["id"] not in _KICKOFF_TYPES),
            plays[0]["start"]["team"]["id"],
        )
        drives.append(_drive(event_id, index, meta, plays, abbrs.get(str(team_id))))
    current: Optional[Dict[str, Any]] = None
    if drives and not final:
        # a live payload's last drive is still open: it becomes ``drives.current`` so Game on
        # Paper renders it as the current drive and the processor still sees its plays. Its last
        # play keeps the end its own yardage puts it at (there is no next snap yet).
        current = drives.pop()
        # the open drive has not ended, so it states no outcome -- whatever a stored payload
        # says. Nulled here rather than at build time, because the drive that becomes
        # ``current`` is the last one that HAS plays, not always the last entry in the chart.
        current["result"] = current["shortDisplayResult"] = current["displayResult"] = "In Progress"
        current["isScore"] = False
        notes.append(f"game is {game.get('status')}: the open drive was moved to drives.current")

    return {
        "boxscore": {"teams": [], "players": []},
        "format": {"regulation": {"periods": int(len(game.get("regulationPeriods") or []) or 4)}},
        "gameInfo": {"venue": _venue(game), "attendance": None},
        "drives": {"previous": drives, **({"current": current} if current else {})},
        "leaders": [],
        "broadcasts": [],
        "predictor": {},
        "pickcenter": _pickcenter(odds or _odds_from_game(game, espn_by_yahoo, home_id)),
        "againstTheSpread": [],
        "odds": [],
        "winprobability": [],
        "header": _header(game, event_id, home_id, away_id, abbrs),
        "scoringPlays": [],
        "videos": [],
        "standings": {},
    }, notes


def _espn_team_number(yahoo_team_id: Any) -> Optional[str]:
    """``"nfl.t.30"`` -> ``"30"``: Yahoo's NFL club number **is** the ESPN team id (32/32).

    Only used when the id-map row states no ESPN team ids, which is what makes an injected
    payload processable with nothing but an ``espn_event_id``. It is a documented identity, not
    a guess -- and it is deliberately NOT extended to the college feed, where Kentucky is Yahoo
    69 and ESPN 96.
    """
    text = str(yahoo_team_id or "")
    number = text.rsplit(".", 1)[-1]
    return number if number.isdigit() and number != "0" else None


def _yahoo_adapter(league: str, espn_id: int, ctx: Any) -> Any:
    """Dispatch adapter for ``source="yahoo"`` (NFL). Registered in :mod:`...sources.dispatch`.

    Hands over to the next source (:class:`...dispatch.SourceUnavailable`) when the Yahoo game
    id cannot be resolved without inventing one, when the fetch fails or comes back without the
    shangrila envelope (a rate limit answers with a 23-byte ``text/html`` body), when the body
    Yahoo serves echoes a **different** ``gameId`` than the one asked for, and -- the HTTP-200
    case -- when Yahoo returns a real game object carrying **no plays**: the Pro Bowl, and every
    game before Yahoo's play floor.
    """
    from sportsdataverse.football.sources.dispatch import AdaptedGame, SourceUnavailable
    from sportsdataverse.football.sources.idmap import _odds_override_from_row

    payload = ctx.payload
    if payload is None:
        # Resolution runs only on the fetch path: an injected payload already IS the game, and
        # resolving anyway would make an offline replay read a release asset over the network.
        row, game_id, resolved = _resolve_row(espn_id, ctx.idmap_row)
        if not game_id:
            raise SourceUnavailable(f"nfl {espn_id}: no yahoo_game_id in the id map and none computable")
        live = str(row.get("status") or "").upper() not in ("FINAL", "FINAL_OVERTIME")
        try:
            payload = _fetch_playbook_boxscore(game_id, live=live)
        except Exception as exc:  # noqa: BLE001 -- network / JSON -> the next source
            raise SourceUnavailable(f"yahoo playbookBoxscore fetch failed: {type(exc).__name__}: {exc}") from exc
    else:
        row = dict(ctx.idmap_row or {})
        row.setdefault("espn_event_id", str(espn_id))
        game_id = row.get("yahoo_game_id")
        resolved = {"idmap_resolved_by": "idmap" if ctx.idmap_row else "none", "yahoo_id_resolved_by": "payload"}

    game = _resolve_game(payload)
    if game is None:
        # An EMPTY ``data.games`` list is Yahoo's NFL "no coverage" answer -- HTTP 200, a
        # well-formed envelope, no game -- and is never worth another try; the college feed
        # says the same thing with a real game object and no plays. Anything that is not an
        # envelope at all (a 429's 23-byte text/html body, an error page, a truncated
        # response) IS worth another try, and the two must not read alike in the log.
        if _envelope_games(payload) == []:
            raise SourceUnavailable(
                f"nfl {espn_id}: yahoo carries no game for this event (Pro Bowl, or before its play floor)"
            )
        raise SourceUnavailable(f"nfl {espn_id}: yahoo returned no shangrila game object (rate limit or error body)")
    if not _has_plays(game):
        # checked BEFORE the id-map fields: "Yahoo does not cover this game" is a fact about the
        # source and is true whatever the row holds, and it is the message the fall-through log
        # should carry for every uncovered game.
        raise SourceUnavailable(
            f"nfl {espn_id}: yahoo game {game_id or game.get('gameId')} carries no play-by-play "
            "(Pro Bowl, or before Yahoo's play floor)"
        )
    expected_id = game_id or row.get("yahoo_game_id")
    served_id = game.get("gameId")
    if expected_id and served_id and str(served_id) != str(expected_id):
        # On the fetch path the Yahoo id is a FORMULA, so a moved kickoff resolves to a real
        # game that is not this one; on the injected path the caller picked the payload. Yahoo
        # echoes the id it served, so either way the mismatch is checkable -- and serving it
        # would file another game's plays under this ESPN event.
        raise SourceUnavailable(f"nfl {espn_id}: yahoo served game {served_id}, not {expected_id}")

    odds = ctx.odds_override or _odds_override_from_row(ctx.idmap_row) or _odds_override_from_row(row)
    summary, notes = _yahoo_to_espn_summary(game, row, odds=odds)
    served = summary["drives"]["previous"] + [d for d in (summary["drives"].get("current"),) if d]
    if not any(d.get("plays") for d in served):
        # every play fell outside the drive chart, or the chart is empty: the contract would
        # fail on ``drives[].plays[]``; say why instead. Checked over BOTH groupings, since a
        # live game's only drive is the open one, which lives under ``current``.
        raise SourceUnavailable(f"nfl {espn_id}: yahoo drive chart carries no plays yet")
    return AdaptedGame(
        summary=summary,
        participants=ctx.participants,
        odds_override=ctx.odds_override,
        native_ids={
            "espn_event_id": str(espn_id),
            "yahoo_game_id": game_id or game.get("gameId"),
            **resolved,
        },
        notes=notes,
    )


def _register_yahoo() -> None:
    """Register :func:`_yahoo_adapter` with the dispatcher (called on import)."""
    from sportsdataverse.football.sources.dispatch import _register

    _register("nfl", "yahoo")(_yahoo_adapter)


_register_yahoo()
