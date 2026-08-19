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

# NCAA official "Last,First", incl. suffixes ("Wilborn Jr.,James", "Jordan III,Tre").
_NAME = r"[A-Z][\w.'\-]+(?:\s(?:Jr|Sr|II|III|IV)\.?)?,\s?[A-Z][\w.'\-]+"

# Yard-line side code. NOT upper-case only: "Ric25" (Rice), "W&M25" (William &
# Mary) -- an [A-Z]-only class silently drops every such team's drives/plays.
_SIDE = r"[A-Za-z&]{1,4}"

# h5 drive title: "{team} {RESULT} {clock},{yardline}, {n} plays, {yards} yards, {top} {a} - {h}".
# RESULT is an OPTIONAL all-caps token (TD/FG/FGA/PUNT/INT/FUMB/DOWNS/HALF/...);
# anchoring on that keeps multi-word team names intact when it is missing (a
# lazy `.+?` team + mandatory result donates "Carolina" -> "East Carolina" = "East").
_DRIVE_RE = re.compile(
    rf"^(?P<team>.+?)(?:\s+(?P<result>[A-Z/]{{2,10}}))?\s+"
    rf"(?P<start_clock>\d+:\d+),(?P<start_yard_line>{_SIDE}\d+),\s+"
    r"(?P<n_plays>\d+)\s+plays?,\s+(?P<yards>-?\d+)\s+yards?,\s+(?P<top>\d+:\d+)\s+"
    r"(?P<score_away>\d+)\s*-\s*(?P<score_home>\d+)\s*$"
)
_DD_RE = re.compile(
    rf"^(?P<down>1st|2nd|3rd|4th)\s+&\s+(?P<distance>\d+|Goal)\s+at\s+(?P<yard_line>{_SIDE}\d+)",
    re.I,
)
_DOWN = {"1st": 1, "2nd": 2, "3rd": 3, "4th": 4}
_YL_SPLIT_RE = re.compile(rf"^({_SIDE})(\d+)$")

# --- play_text field regexes ----------------------------------------------
_CLOCK_RE = re.compile(r"^\((\d{1,2}:\d{2})\)\s*")  # some games prefix each play with "(MM:SS)"
_FORMATION_RE = re.compile(r"^(No Huddle(?:-Shotgun)?|Shotgun|Wildcat|Pistol)\s+")
_YARDS_RE = re.compile(r"for (\d+) yards? (gain|loss)", re.I)
_YARDS_PLAIN_RE = re.compile(r"for (\d+) yards? to the", re.I)  # completed pass / 0-yard run: positive
_END_YL_RE = re.compile(rf"to the ({_SIDE}\d+)")
_RUSH_RE = re.compile(
    rf"(?P<rusher>{_NAME}) rush(?:es)?(?:\s+(?P<dir>left|right|middle|up the middle))?",
    re.I,
)
_PASS_RE = re.compile(
    rf"(?P<passer>{_NAME}) pass (?P<result>complete|incomplete|intercepted)"
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
_POSSESSION_RE = re.compile(rf"^{_SIDE} ball on {_SIDE}\d+")  # "AKR ball on AKR20." / "Ore ball on Ore25." drive marker
_TWOPT_RE = re.compile(
    rf"(?P<player>{_NAME}) (?P<kind>pass|run|rush) attempt (?P<result>Successful|failed)",
    re.I,
)
_KICK_YDS_RE = re.compile(r"kickoff (\d+) yards", re.I)
_PUNT_YDS_RE = re.compile(r"punt (\d+) yards", re.I)
_RET_YDS_RE = re.compile(r"return (\d+) yards", re.I)
_FG_DETAIL_RE = re.compile(r"field goal attempt from (\d+) yards\s+(GOOD|NO GOOD)", re.I)
_PENALTY_RE = re.compile(
    rf"PENALTY (?P<team>[A-Z]{{2,4}}) (?P<type>[A-Za-z][A-Za-z /'\-]*?)"
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
    if m:
        return int(m.group(1)) * (1 if m.group(2).lower() == "gain" else -1)
    m = _YARDS_PLAIN_RE.search(text)
    if m:
        return int(m.group(1))
    if re.search(r"for no gain", text, re.I):
        return 0
    return None


def _tacklers(text: str) -> "tuple[str | None, str | None]":
    pre = text.split("PENALTY")[0]  # tacklers belong to the play, before any penalty note
    cand = [g for g in re.findall(r"\(([^)]+)\)", pre) if "," in g and not g.startswith(("H:", "LS:"))]
    if not cand:
        return None, None
    names = [n.strip() for n in re.split(r";\s*", cand[-1]) if n.strip()]
    return (names[0] if names else None), (names[1] if len(names) > 1 else None)


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
    out["is_touchdown"] = "TOUCHDOWN" in text
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
    out["end_yard_line"] = (_END_YL_RE.findall(text) or [None])[-1]
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
        m = _PASS_RE.search(text)
        if m:
            out["passer"] = m.group("passer")
            out["receiver"] = m.groupdict().get("receiver")
            out["pass_depth"] = (m.groupdict().get("depth") or "").lower() or None
            out["pass_direction"] = (m.groupdict().get("dir") or "").lower() or None
            complete = m.group("result").lower() == "complete"
            out["pass_complete"] = complete
            out["yards_gained"] = _yards_gained(text) if complete else 0
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

    for container in soup.select("div.drives"):
        drive: "dict" = {}
        for child in container.find_all(["h5", "div"], recursive=False):
            classes = child.get("class") or []
            if not ("scoring_play" in classes or "non_scoring_play" in classes):
                continue
            if child.name == "h5":
                drive_number += 1
                m = _DRIVE_RE.match(_spaces(child.get_text(" ", strip=True)))
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
                    yl_m = _YL_SPLIT_RE.match(yl) if yl else None
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
                        "yard_line_side": yl_m.group(1) if yl_m else None,
                        "yard_line_number": int(yl_m.group(2)) if yl_m else None,
                        "play_text": play_text,
                    }
                    row.update(_decompose_play_text(play_text))
                    rows.append(row)

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
