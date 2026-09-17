"""Parse stats.ncaa.org college-football (NCAA sport code ``MFB``) play-by-play
HTML into a tidy polars frame.

Structured, cfbfastR-style: one row per play with drive context + down/distance/
yard_line + play_type + players (rusher/passer/receiver/kicker/punter/returner/
tacklers) + directions + yards + kick/FG details + scoring/turnover/penalty/first-
down/out-of-bounds flags, keeping the raw ``play_text`` for anything not yet lifted.

**Provenance.** Original sdv-py code (no upstream port). The HTML source is the
Akamai-``bm-verify``-gated ``/contests/{id}/play_by_play`` game page, fetched via
the shared browser transport in :mod:`sportsdataverse.mbb.mbb_ncaa_fetch`
(patchright + ``--headless=new`` + residential IP). This module is the *parser*
half of that pipeline; capture/discovery is a producer concern.

Markup (fixture-verified, contest 5362535): ``div.drives`` holds, per drive, an
``h5.(non_)scoring_play`` title, a header-body ``div`` (team + score), then a
``div`` whose bordered child ``div``s are the plays -- each ``<span>`` bold
down/dist/yardline + ``<span>`` play text. ``scoring_play`` class = drive scored.
"""

from __future__ import annotations

import re
from typing import TYPE_CHECKING, Union

import polars as pl
from bs4 import BeautifulSoup

if TYPE_CHECKING:
    import pandas as pd

__all__ = ["DRIVE_TITLES_SCHEMA", "PBP_SCHEMA", "parse_cfb_ncaa_drive_titles", "parse_cfb_ncaa_pbp"]

# A player name, in any of the page generations' forms (case-SENSITIVE inside the
# case-insensitive play regexes: names are capitalised, the narrative is not):
#   * official "Last[ Suffix],First" -- "Wilborn Jr.,James", "Smith, Danny", "McKENZIE, D"
#   * 2019-era "First Last" -- "Joe Burrow", "C. Ed.-Helaire", "K. Duncan Jr.", or a lone
#     surname ("ALEXANDER-STEVE"); anchored by the play verb that follows it
#   * 2025 jersey style -- "#95 K.Kimble"
_SUFFIX = r"(?:\s(?:Jr|Sr|II|III|IV)\.?)?"
_TOKEN = r"[A-Z][A-Za-z.'\-]*"
_NAME = rf"(?-i:[A-Z][A-Za-z.'\-]+{_SUFFIX},\s?{_TOKEN}|#\d{{1,2}}\s{_TOKEN}|{_TOKEN}(?:\s{_TOKEN}){{0,2}}{_SUFFIX})"
_LAST_FIRST_RE = re.compile(rf"^[A-Z][A-Za-z.'\-]+{_SUFFIX},\s?{_TOKEN}$")


def _clean_name(name: "str | None") -> "str | None":
    """Drop the sentence period the regex swallows ("DRAYTON, Matt." / "Hodge, C..") -- an
    initial keeps its own ("Covington, J.")."""
    if not name:
        return name
    while name.endswith(".."):
        name = name[:-1]
    last = re.split(r"[,\s]+", name)[-1]
    return name[:-1] if name.endswith(".") and len(last.rstrip(".")) >= 3 else name


# Yard-line token = side code + yard number (0-50). A code is NOT a fixed character
# class: "Ric25", "W&M25", nicknames ("SPARTANS25"), digits ("SFA2" + 25 is printed
# "SFA225"), hyphens ("SU-ETSU25"), "ST. FRAN20", "MONT_ST25", "HOW(3)30"; and play
# text abbreviates differently from the drive headers ("SDSU00" / "Hawaii15" / "FAU 47"
# against "SDS25" / "HAW25" / "FAU25"). Tokens are therefore captured whole and split
# against the game's own codes (:func:`_side_codes`, :func:`_split_yard_line`) --
# the one side-code rule shared by the parser and ``cfb_ncaa_cfbfastr``.
_SIDE_CHARS = r"[\w&.'~()\-]"
_YL_TOKEN = rf"{_SIDE_CHARS}+(?: [A-Za-z]{_SIDE_CHARS}*)? ?\d{{1,2}}"

# h5 drive title: "{team} {RESULT} {clock},{yardline}, {n} plays, {yards} yards, {top} {a} - {h}".
# RESULT is an OPTIONAL all-caps token (TD/FG/FGA/PUNT/INT/FUMB/DOWNS/HALF/...);
# anchoring on that keeps multi-word team names intact when it is missing (a
# lazy `.+?` team + mandatory result donates "Carolina" -> "East Carolina" = "East").
_DRIVE_RE = re.compile(
    r"^(?P<team>.+?)(?:\s+(?P<result>[A-Z/]{2,10}))?\s+"
    r"(?P<start_clock>\d+:\d+),(?P<start_yard_line>[^,]*\d),\s+"
    r"(?P<n_plays>\d+)\s+plays?,\s+(?P<yards>-?\d+)\s+yards?,\s+(?P<top>\d+:\d+)\s+"
    r"(?P<score_away>\d+)\s*-\s*(?P<score_home>\d+)\s*$"
)
_DD_RE = re.compile(
    r"^(?P<down>1st|2nd|3rd|4th)\s+&\s+(?P<distance>\d+|Goal)\s+at\s+(?P<yard_line>\S.*\d)\s*$",
    re.I,
)
_DOWN = {"1st": 1, "2nd": 2, "3rd": 3, "4th": 4}


def _yl_candidates(token: str) -> "list[tuple[str, int]]":
    """(code, yard) splits of a token: the yard is its last 2 or 1 digits (<= 50)."""
    out = []
    for k in (2, 1):
        num = token[-k:]
        code = token[:-k].rstrip()
        if len(token) > k and num.isdigit() and int(num) <= 50 and re.search("[A-Za-z]", code):
            out.append((code, int(num)))
    return out


def _norm_code(code: str) -> str:
    return re.sub(r"[^a-z0-9]", "", code.lower())


def _side_codes(tokens: "list[str]") -> "list[str]":
    """The game's (up to two) side codes, from its drive-header/down-distance tokens.

    Greedy cover: the code that explains the most tokens wins, then the best code
    among the tokens it leaves (ties -> the shorter code). "SFA225"/"SFA232"/"SFA20"
    all share "SFA2" but not "SFA" (225 > 50) nor "SFA22"/"SFA23".
    """
    remaining = [t for t in tokens if _yl_candidates(t)]
    codes: "list[str]" = []
    while remaining and len(codes) < 2:
        counts: "dict[str, int]" = {}
        for t in remaining:
            for c, _ in _yl_candidates(t):
                counts[c] = counts.get(c, 0) + 1
        best = max(counts.items(), key=lambda kv: (kv[1], -len(kv[0])))[0]
        codes.append(best)
        remaining = [t for t in remaining if best not in (c for c, _ in _yl_candidates(t))]
    return codes


def _split_yard_line(token: "str | None", codes: "list[str]") -> "tuple[str | None, int] | None":
    """Split a yard-line token into (game side code, yard) -> ``None`` when unresolvable.

    Matches a candidate code against ``codes`` case/punctuation-insensitively, then by a
    unique prefix ("SDSU" -> "SDS", "Hawaii" -> "HAW"). With no ``codes`` the 2-digit
    split wins. Midfield ("50") has no side: ``(None, 50)``.
    """
    if not token:
        return None
    if token.strip() == "50":
        return None, 50
    cands = _yl_candidates(token)
    for c, n in cands:
        for code in codes:
            if _norm_code(c) == _norm_code(code):
                return code, n
    for c, n in cands:
        hits = [
            code
            for code in codes
            if _norm_code(c).startswith(_norm_code(code)) or _norm_code(code).startswith(_norm_code(c))
        ]
        if len(hits) == 1:
            return hits[0], n
    if codes or not cands:
        return None
    return cands[0]


# --- play_text field regexes ----------------------------------------------
# some games prefix each play with "(MM:SS)" / "Clock MM:SS,"
_CLOCK_RE = re.compile(r"^(?:\((\d{1,2}:\d{2})\)|Clock (\d{1,2}:\d{2}),)\s*")
_REVIEW_RE = re.compile(r"\s*(?:The previous play is under|\(Original Play:)")
# 2025 words, or the 2019-era codes "SH,"/"SHOT,"/"SG,"/"SGUN,"/"NHSG,"/"NH,"/"PSTL,"
_FORMATION_RE = re.compile(r"^(No Huddle(?:-Shotgun)?|Shotgun|Wildcat|Pistol|(?:SHOT|SGUN|NHSG|PSTL|SG|SH|NH),)\s+")
# The play's yardage is its FIRST "for ..." clause: "for 7 yards gain" / "for 5 yards
# loss" (2025), "for loss of 4 yards" / "for 13 yards" (2019-era), "for no gain".
# Leftmost wins, so a later fumble-advance clause ("..., recovered by VU Smith at
# VU36, Smith for 1 yard to the VU37") is not read as the play's gain.
_YARDS_RE = re.compile(
    r"for (?:(?P<n>\d+) yards? (?P<dir>gain|loss)|(?:a )?loss of (?P<loss>\d+) yards?|(?P<plain>\d+) yards?\b|no gain)",
    re.I,
)
# "to the VU37" -- or "to the 50 yardline" (midfield has no side code; emitted as "50")
_END_YL_RE = re.compile(rf"to the (?:(50) yard ?line|({_YL_TOKEN})(?!\w))")
_RUSH_RE = re.compile(
    rf"(?P<rusher>{_NAME}) rush(?:es)?(?:\s+(?P<dir>left|right|middle|up the middle))?",
    re.I,
)
_PASS_RE = re.compile(
    rf"(?P<passer>{_NAME})(?-i:(?:\s[a-z]+){{0,2}}) pass (?P<result>complete|incomplete|intercepted)"
    rf"(?:\s+(?P<depth>short|deep))?(?:\s+(?P<dir>left|right|middle))?"
    rf"(?:.*?\bto\s+(?P<receiver>{_NAME}))?",
    re.I,
)
_KICKOFF_RE = re.compile(rf"(?P<kicker>{_NAME}) kickoff \d+ yards(?:.*?(?P<returner>{_NAME}) return)?", re.I)
_PUNT_RE = re.compile(
    rf"(?P<punter>{_NAME}) punt \d+ yards(?:.*?(?:fair catch by (?P<fc>{_NAME})|(?P<returner>{_NAME}) return))?",
    re.I,
)
_SACK_RE = re.compile(rf"(?P<passer>{_NAME}) sacked", re.I)
_FG_RE = re.compile(rf"(?P<kicker>{_NAME}) field goal", re.I)
_XP_RE = re.compile(rf"(?P<kicker>{_NAME}) kick attempt", re.I)
_POSSESSION_RE = re.compile(r"^[^,]{1,16}? ball on [^,]*\d")  # "AKR ball on AKR20." / "Ore ball on Ore25." drive marker
_TWOPT_RE = re.compile(
    rf"(?P<player>{_NAME}) (?P<kind>pass|run|rush) attempt (?P<result>Successful|failed)",
    re.I,
)
_KICK_YDS_RE = re.compile(r"kickoff (\d+) yards", re.I)
_PUNT_YDS_RE = re.compile(r"punt (\d+) yards", re.I)
_RET_YDS_RE = re.compile(r"return (\d+) yards", re.I)
_FG_DETAIL_RE = re.compile(r"field goal attempt from (\d+) yards\s+(GOOD|NO GOOD)", re.I)
_PENALTY_RE = re.compile(
    rf"PENALTY (?P<team>{_SIDE_CHARS}{{2,10}}) (?P<type>[A-Za-z][A-Za-z /'\-]*?)"
    rf"(?:\s+\((?P<player>{_NAME})\))?\s+(?P<yards>\d+) yards",
    re.I,
)

_DECOMP_KEYS = (
    "play_type",
    "clock",
    "yards_gained",
    "formation",
    "passer",
    "rusher",
    "receiver",
    "kicker",
    "punter",
    "returner",
    "run_direction",
    "pass_complete",
    "pass_depth",
    "pass_direction",
    "tackler_1",
    "tackler_2",
    "kick_yards",
    "return_yards",
    "punt_yards",
    "fg_distance",
    "fg_made",
    "is_first_down",
    "is_touchdown",
    "is_safety",
    "is_fumble",
    "is_turnover",
    "turnover_type",
    "out_of_bounds",
    "no_play",
    "fair_catch",
    "penalty_flag",
    "penalty_team",
    "penalty_type",
    "penalty_player",
    "penalty_yards",
    "end_yard_line",
)


def _spaces(text: str) -> str:
    return " ".join(text.split())


def _yards_gained(text: str) -> "int | None":
    m = _YARDS_RE.search(text)
    if not m:
        return None
    if m.group("n"):
        return int(m.group("n")) * (1 if m.group("dir").lower() == "gain" else -1)
    if m.group("loss"):
        return -int(m.group("loss"))
    return int(m.group("plain")) if m.group("plain") else 0


def _tacklers(text: str) -> "tuple[str | None, str | None]":
    """The (up to two) tacklers in the play's last parenthesised group.

    2025 pages separate them with ";". 2019-era pages arrive damaged: the separator is
    a mangled ":" ("MORGAN, D.J.3aPAUL, Keyshawn") or gone entirely, one name running
    into the next ("Smith, JohnDOE, Jane", "Kristian FultonJaCoby Stevens"). "Last, First"
    names can still be pulled apart at the "Last, " that starts the next one; "First Last"
    names cannot (a CamelCase first name looks the same), so a comma-less group is trusted
    only as a single name and is otherwise left null rather than emitted as garbage.
    """
    pre = text.split("PENALTY")[0]  # tacklers belong to the play, before any penalty note
    cand = [
        g.strip()
        for g in re.findall(r"\(([^()]+)\)", pre)
        if not g.startswith(("H:", "LS:")) and not g.strip().isdigit()
    ]
    if not cand:
        return None, None
    group = cand[-1]
    if "," not in group:
        one = re.fullmatch(rf"{_TOKEN}(?:\s{_TOKEN})?{_SUFFIX}", group) is not None  # one "First Last[ Jr.]"
        return (group if one else None), None
    pieces = [
        x.strip()
        for x in re.split(r";\s*|(?<=[\w.])3a(?=[A-Z])|(?<=[a-z.])(?=[A-Z][A-Za-z.'\-]*,\s?[A-Z])", group)
        if x.strip()
    ]
    # a piece without a comma is a "Last, First" cut inside its surname ("Mc|Clellan, Matt")
    names: "list[str]" = []
    for piece in pieces:
        if names and "," not in names[-1]:
            names[-1] += piece
        else:
            names.append(piece)
    if not all(_LAST_FIRST_RE.match(n) for n in names):
        return None, None
    return names[0], (names[1] if len(names) > 1 else None)


def _decompose_play_text(text: str) -> "dict":
    out: "dict" = dict.fromkeys(_DECOMP_KEYS)
    cm = _CLOCK_RE.match(text)
    if cm:
        out["clock"] = cm.group(1)
        text = text[cm.end() :]
    fm = _FORMATION_RE.match(text)
    if fm:
        out["formation"] = fm.group(1)
        text = text[fm.end() :]
    # a replay review appends its note and, when overturned, reprints the ORIGINAL call
    # ("... PLAY OVERTURNED. (Original Play: ... TOUCHDOWN ...)"); only the ruling before
    # the note is the play (play_text keeps the whole string)
    text = _REVIEW_RE.split(text, 1)[0]
    tl = text.lower()

    # non-play markers -- classify + return early (no per-play fields apply)
    if "drive start at" in tl or _POSSESSION_RE.match(text):  # "AKR ball on AKR20."
        out["play_type"] = "drive_start"
        return out
    if re.search(r"(start|end) of (1st|2nd|3rd|4th) quarter|end of game|end of (?:the )?half", tl):
        out["play_type"] = "period_marker"
        return out
    if "timeout" in tl:
        out["play_type"] = "timeout"
        return out
    if "will receive" in tl or "will defend" in tl or "won the toss" in tl:
        out["play_type"] = "coin_toss"
        return out

    # universal flags (case-sensitive caps markers)
    out["is_first_down"] = "1ST DOWN" in text
    # "TOUCHDOWN nullified by penalty" scored nothing
    out["is_touchdown"] = "TOUCHDOWN" in text and "TOUCHDOWN nullified" not in text
    out["is_safety"] = "SAFETY" in text
    out["is_fumble"] = "FUMBLE" in text.upper()
    out["out_of_bounds"] = "out of bounds" in tl
    out["no_play"] = "NO PLAY" in text
    out["fair_catch"] = "fair catch" in tl
    if "TURNOVER ON DOWNS" in text:
        out["is_turnover"], out["turnover_type"] = True, "downs"
    elif "INTERCEPT" in text.upper():
        out["is_turnover"], out["turnover_type"] = True, "interception"
    elif out["is_fumble"] and "recovered by" in tl:
        out["turnover_type"] = "fumble"
    out["tackler_1"], out["tackler_2"] = _tacklers(text)
    out["end_yard_line"] = next((a or b for a, b in reversed(_END_YL_RE.findall(text))), None)
    pm = _PENALTY_RE.search(text)
    if pm:
        out.update(
            penalty_flag=True,
            penalty_team=pm.group("team"),
            penalty_type=_spaces(pm.group("type")),
            penalty_player=pm.groupdict().get("player"),
            penalty_yards=int(pm.group("yards")),
        )
    else:
        out["penalty_flag"] = "PENALTY" in text

    # play type + type-specific fields
    if "kickoff" in tl:
        out["play_type"] = "kickoff"
        m = _KICKOFF_RE.search(text)
        if m:
            out["kicker"], out["returner"] = (
                m.group("kicker"),
                m.groupdict().get("returner"),
            )
        ky = _KICK_YDS_RE.search(text)
        ry = _RET_YDS_RE.search(text)
        out["kick_yards"] = int(ky.group(1)) if ky else None
        out["return_yards"] = int(ry.group(1)) if ry else None
    elif "punt" in tl:
        out["play_type"] = "punt"
        m = _PUNT_RE.search(text)
        if m:
            out["punter"] = m.group("punter")
            out["returner"] = m.groupdict().get("returner") or m.groupdict().get("fc")
        py = _PUNT_YDS_RE.search(text)
        ry = _RET_YDS_RE.search(text)
        out["punt_yards"] = int(py.group(1)) if py else None
        out["return_yards"] = int(ry.group(1)) if ry else None
    elif "field goal" in tl:
        out["play_type"] = "field_goal"
        m = _FG_RE.search(text)
        if m:
            out["kicker"] = m.group("kicker")
        fg = _FG_DETAIL_RE.search(text)
        if fg:
            out["fg_distance"], out["fg_made"] = (
                int(fg.group(1)),
                fg.group(2).upper() == "GOOD",
            )
    elif "kick attempt" in tl or "extra point" in tl:
        out["play_type"] = "extra_point"
        m = _XP_RE.search(text)
        if m:
            out["kicker"] = m.group("kicker")
    elif "pass attempt" in tl or "run attempt" in tl or "rush attempt" in tl:
        out["play_type"] = "two_point"  # 2-pt conversion ("... attempt Successful/failed")
        tm = _TWOPT_RE.search(text)
        if tm:
            out["passer" if tm.group("kind").lower() == "pass" else "rusher"] = tm.group("player")
    elif "sacked" in tl:
        out["play_type"] = "sack"
        out["yards_gained"] = _yards_gained(text)
        m = _SACK_RE.search(text)
        if m:
            out["passer"] = m.group("passer")
    elif "pass complete" in tl or "pass incomplete" in tl or "pass intercepted" in tl:
        out["play_type"] = "pass"
        # the result is in the text whether or not the passer's name matches _NAME
        # (2019 pages print "First Last", which the "Last,First" pattern cannot)
        complete = "pass complete" in tl
        out["pass_complete"] = complete
        out["yards_gained"] = _yards_gained(text) if complete else 0
        m = _PASS_RE.search(text)
        if m:
            out["passer"] = m.group("passer")
            out["receiver"] = m.groupdict().get("receiver")
            out["pass_depth"] = (m.groupdict().get("depth") or "").lower() or None
            out["pass_direction"] = (m.groupdict().get("dir") or "").lower() or None
    elif "kneel" in tl:
        out["play_type"] = "kneel"
        out["yards_gained"] = _yards_gained(text)
    elif "rush" in tl:
        out["play_type"] = "rush"
        out["yards_gained"] = _yards_gained(text)
        m = _RUSH_RE.search(text)
        if m:
            out["rusher"] = m.group("rusher")
            out["run_direction"] = (m.groupdict().get("dir") or "").lower() or None
    elif out["penalty_flag"]:
        out["play_type"] = "penalty"
    else:
        out["play_type"] = "unknown"
    for k in ("passer", "rusher", "receiver", "kicker", "punter", "returner", "penalty_player"):
        out[k] = _clean_name(out[k])
    return out


# base structural columns, then decomposed fields, then raw play_text last.
PBP_SCHEMA: "dict[str, pl.DataType]" = {
    "contest_id": pl.Utf8,
    "drive_number": pl.Int64,
    "play_number": pl.Int64,
    "offense": pl.Utf8,
    "drive_result": pl.Utf8,
    "drive_scored": pl.Boolean,
    "down": pl.Int64,
    "distance": pl.Int64,
    "yard_line": pl.Utf8,
    "yard_line_side": pl.Utf8,
    "yard_line_number": pl.Int64,
    "play_type": pl.Utf8,
    "clock": pl.Utf8,
    "yards_gained": pl.Int64,
    "formation": pl.Utf8,
    "passer": pl.Utf8,
    "rusher": pl.Utf8,
    "receiver": pl.Utf8,
    "kicker": pl.Utf8,
    "punter": pl.Utf8,
    "returner": pl.Utf8,
    "run_direction": pl.Utf8,
    # Derived post-parse (NCAA text does NOT label scrambles): a rush by a player
    # who also throws passes in the game = a QB run (conflates designed keepers +
    # true scrambles). Null on non-rush plays.
    "qb_scramble": pl.Boolean,
    "pass_complete": pl.Boolean,
    "pass_depth": pl.Utf8,
    "pass_direction": pl.Utf8,
    "tackler_1": pl.Utf8,
    "tackler_2": pl.Utf8,
    "kick_yards": pl.Int64,
    "return_yards": pl.Int64,
    "punt_yards": pl.Int64,
    "fg_distance": pl.Int64,
    "fg_made": pl.Boolean,
    "is_first_down": pl.Boolean,
    "is_touchdown": pl.Boolean,
    "is_safety": pl.Boolean,
    "is_fumble": pl.Boolean,
    "is_turnover": pl.Boolean,
    "turnover_type": pl.Utf8,
    "out_of_bounds": pl.Boolean,
    "no_play": pl.Boolean,
    "fair_catch": pl.Boolean,
    "penalty_flag": pl.Boolean,
    "penalty_team": pl.Utf8,
    "penalty_type": pl.Utf8,
    "penalty_player": pl.Utf8,
    "penalty_yards": pl.Int64,
    "end_yard_line": pl.Utf8,
    "play_text": pl.Utf8,
}


def parse_cfb_ncaa_pbp(
    html: str,
    contest_id: "str | int | None" = None,
    *,
    return_as_pandas: bool = False,
) -> "Union[pl.DataFrame, pd.DataFrame]":
    """Parse a stats.ncaa.org college-football ``play_by_play`` page into a tidy frame.

    One row per play, cfbfastR-style: drive context (``drive_number``/``offense``/
    ``drive_result``/``drive_scored``), down/distance/yard-line, a classified
    ``play_type``, extracted players/directions/yards/kick-FG details, and a set of
    boolean flags (first-down, touchdown, safety, fumble, turnover, penalty, ...),
    with the raw ``play_text`` retained for anything not yet lifted into a column.

    ``qb_scramble`` is derived frame-wide: NCAA official text does not label
    scrambles, so a rush by a player who also passes in the same game is flagged as
    a QB run.

    Args:
        html: Raw HTML of the ``/contests/{id}/play_by_play`` page (as returned by
            :meth:`sportsdataverse.mbb.mbb_ncaa_fetch.NcaaFetcher.fetch_game_pbp`).
        contest_id: Optional stats.ncaa.org contest id, written to every row's
            ``contest_id`` column. Coerced to ``str``.
        return_as_pandas: Return a ``pandas.DataFrame`` instead of ``polars``.

    Returns:
        A ``polars.DataFrame`` (or ``pandas.DataFrame`` when ``return_as_pandas``)
        with one row per play. Empty/unparseable input returns a **zero-row frame
        carrying the documented schema**, so callers can chain without null-checks.

    Example:
        Quick start::

            from sportsdataverse.cfb import parse_cfb_ncaa_pbp
            df = parse_cfb_ncaa_pbp(open("contest_5362535.html").read(), contest_id=5362535)
            print(df.shape)

        Inspect scoring plays::

            df.filter(pl.col("is_touchdown") == True).select("offense", "play_text")

        See Also:
            * `cfbfastR`_ -- ESPN-sourced college-football pbp (R)

        .. _cfbfastR: https://cfbfastR.sportsdataverse.org
    """
    soup = BeautifulSoup(html or "", "html.parser")
    rows: "list[dict]" = []
    drive_number = 0
    cid = str(contest_id) if contest_id is not None else None
    start_tokens: "list[str]" = []

    for container in soup.select("div.drives"):
        drive: "dict" = {}
        for child in container.find_all(["h5", "div"], recursive=False):
            classes = child.get("class") or []
            if not ("scoring_play" in classes or "non_scoring_play" in classes):
                continue
            if child.name == "h5":
                drive_number += 1
                m = _DRIVE_RE.match(_spaces(child.get_text(" ", strip=True)))
                if m:
                    start_tokens.append(m.group("start_yard_line"))
                drive = {
                    "drive_number": drive_number,
                    "offense": m.group("team") if m else None,
                    "drive_result": m.group("result") if m else None,
                    "drive_scored": "scoring_play" in classes,
                }
            elif child.select_one(".headerRight") is None:
                # play-list div (the header-body div has .headerRight; skip it)
                play_number = 0
                for play in child.find_all("div", recursive=False):
                    spans = play.find_all("span")
                    if len(spans) < 2:
                        continue
                    ddm = _DD_RE.match(_spaces(spans[0].get_text(" ", strip=True)))
                    play_number += 1
                    dist = ddm.group("distance") if ddm else None
                    yl = ddm.group("yard_line") if ddm else None
                    play_text = _spaces(spans[1].get_text(" ", strip=True))
                    row = {
                        "contest_id": cid,
                        "drive_number": drive.get("drive_number"),
                        "play_number": play_number,
                        "offense": drive.get("offense"),
                        "drive_result": drive.get("drive_result"),
                        "drive_scored": drive.get("drive_scored"),
                        "down": _DOWN.get(ddm.group("down").lower()) if ddm else None,
                        "distance": int(dist) if dist and dist.isdigit() else None,
                        "yard_line": yl,
                        "play_text": play_text,
                    }
                    row.update(_decompose_play_text(play_text))
                    rows.append(row)

    # split every token against the game's own side codes (drive headers + down/distance)
    codes = _side_codes(start_tokens + [r["yard_line"] for r in rows if r["yard_line"]])
    for r in rows:
        side, num = _split_yard_line(r["yard_line"], codes) or (None, None)
        r["yard_line_side"], r["yard_line_number"] = side, num
    df = pl.DataFrame(rows, schema=PBP_SCHEMA) if rows else pl.DataFrame(schema=PBP_SCHEMA)
    if df.height:
        # qb_scramble = a rush by a player who also passes in this game (QB run).
        # Frame-level derivation because NCAA text does not label scrambles.
        qbs = df.filter(pl.col("passer").is_not_null()).get_column("passer").unique().to_list()
        df = df.with_columns(
            pl.when(pl.col("play_type") == "rush")
            .then(pl.col("rusher").is_in(qbs))
            .otherwise(None)
            .alias("qb_scramble")
        )
    return df.to_pandas() if return_as_pandas else df


# --- drive titles (running-score checkpoints) -----------------------------

DRIVE_TITLES_SCHEMA: "dict[str, pl.DataType]" = {
    "contest_id": pl.Utf8,
    "drive_number": pl.Int64,
    "team": pl.Utf8,
    "result": pl.Utf8,
    "start_clock": pl.Utf8,
    "start_yard_line": pl.Utf8,
    "n_plays": pl.Int64,
    "yards": pl.Int64,
    "top": pl.Utf8,
    "score_away": pl.Int64,
    "score_home": pl.Int64,
}


def parse_cfb_ncaa_drive_titles(
    html: str,
    contest_id: "str | int | None" = None,
    *,
    return_as_pandas: bool = False,
) -> "Union[pl.DataFrame, pd.DataFrame]":
    """Parse the drive ``h5`` titles of a ``play_by_play`` page -> one row per drive.

    Each drive header on the stats.ncaa.org pbp page reads
    ``"{team} {RESULT} {clock},{yardline}, {n} plays, {yards} yards, {top} {away} - {home}"``.
    This lifts it into a frame of per-drive team/result/start/length plus the
    game score **after** the drive (``score_away`` / ``score_home``) -- an
    authoritative running-score checkpoint a play-level score can snap to.

    The ``RESULT`` token is optional on some pages and side codes can be mixed
    case (``Ric25``, ``W&M25``); both variants parse. A title that still does not
    match yields a row with only ``drive_number`` populated, so drive numbering
    stays aligned with :func:`parse_cfb_ncaa_pbp`.

    Args:
        html: Raw HTML of the ``/contests/{id}/play_by_play`` page.
        contest_id: Optional stats.ncaa.org contest id, written to every row's
            ``contest_id`` column. Coerced to ``str``.
        return_as_pandas: Return a ``pandas.DataFrame`` instead of ``polars``.

    Returns:
        A ``polars.DataFrame`` (or ``pandas.DataFrame`` when ``return_as_pandas``)
        with one row per drive. Empty/unparseable input returns a **zero-row frame
        carrying the documented schema**.

    Example:
        Quick start::

            from sportsdataverse.cfb import parse_cfb_ncaa_drive_titles
            df = parse_cfb_ncaa_drive_titles(open("contest_6386335.html").read(), contest_id=6386335)
            print(df.shape)

        Running-score checkpoints::

            df.select("drive_number", "team", "result", "score_away", "score_home")

        See Also:
            * `cfbfastR`_ -- ESPN-sourced college-football drives (R)

        .. _cfbfastR: https://cfbfastR.sportsdataverse.org
    """
    soup = BeautifulSoup(html or "", "html.parser")
    cid = str(contest_id) if contest_id is not None else None
    rows: "list[dict]" = []
    n = 0
    for container in soup.select("div.drives"):
        for h5 in container.find_all("h5", recursive=False):
            classes = h5.get("class") or []
            if not ("scoring_play" in classes or "non_scoring_play" in classes):
                continue
            n += 1
            m = _DRIVE_RE.match(_spaces(h5.get_text(" ", strip=True)))
            g = m.groupdict() if m else {}
            rows.append(
                {
                    "contest_id": cid,
                    "drive_number": n,
                    "team": g.get("team"),
                    "result": g.get("result"),
                    "start_clock": g.get("start_clock"),
                    "start_yard_line": g.get("start_yard_line"),
                    "n_plays": int(g["n_plays"]) if m else None,
                    "yards": int(g["yards"]) if m else None,
                    "top": g.get("top"),
                    "score_away": int(g["score_away"]) if m else None,
                    "score_home": int(g["score_home"]) if m else None,
                }
            )
    df = pl.DataFrame(rows, schema=DRIVE_TITLES_SCHEMA) if rows else pl.DataFrame(schema=DRIVE_TITLES_SCHEMA)
    return df.to_pandas() if return_as_pandas else df
