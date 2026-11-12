"""stats.ncaa.org parsed contest -> the ESPN-summary dict ``helper_<league>_pbp`` parses (BB-D16).

``source="ncaa"`` for MBB and WBB in :mod:`sportsdataverse.basketball.sources.dispatch`. The input is one
contest as the daily NCAA pipeline publishes it (``sportsdataverse.scrape.ncaa``, the Data API's
``{lg}.ncaa_pbp`` / ``ncaa_shots`` / ``ncaa_player_box`` and ``ncaa_player_crosswalk``), so the
stats.ncaa.org parse rules are the pipeline's own, ``fix_technicals=True`` included (the documented, correct
default of :func:`~sportsdataverse.mbb.mbb_ncaa_game_pbp.parse_ncaa_bb_game_pbp`). Nothing here fetches
stats.ncaa.org.

ESPN's college play-by-play and stats.ncaa.org's are the same NCAA LiveStats feed: on the 16-game BB-D16
corpus every rebound, steal, block, free throw, substitution and challenge row has its ESPN twin (turnovers:
one extra in 16 games). What ESPN does to that feed, and this projection repeats:

* **Rows.** stats.ncaa.org's "Draw Foul" row (the fouled player) is dropped, as is a held-ball jump ball (the
  arrow, not a tip); ESPN adds a "Start game" row and an "End Period" / "End Game" row, so this does too.
  ESPN's media timeouts ("Official TV Timeout") are not in the NCAA feed; its overtime tip is (ESPN has none).
* **Types.** Every jumper and hook is ``JumpShot``; layups and dunks keep their type except tip-ins
  (``TipShot``); a free throw is ``MadeFreeThrow`` made or missed; every turnover is ``Lost Ball Turnover``.
* **Ids.** Team ids are the ESPN ids the pipeline stamps on every row (``*_espn_team_id``); players go through
  the D37 ``ncaa_player_crosswalk``. An unmapped player is a null participant and an adapter note; a game
  whose two teams are not both mapped fails (the win-probability model needs one id space).
* **Play ids.** stats.ncaa.org has no event number, so play ids are this adapter's own
  (``{espn_id}{row:05d}``), never ESPN's; :mod:`~sportsdataverse.basketball.sources.parity` pairs the two
  feeds' plays by alignment before comparing them.
* **Scores.** stats.ncaa.org's score columns follow its page's team order, which is sometimes away first; a
  game whose plays add up the other way round has them swapped back (WBB 401913054 in the corpus).
* **Order.** ``ncaa_pbp`` carries no event index, the Data API returns a contest in storage blocks, and rows
  within one game second arrive in no reliable order, so rows are sorted on ``(period, game_seconds, running
  total)`` and then by kind in ESPN's usual order (shot, block, turnover, steal, foul, timeout, substitutions,
  free throws by number, rebound). ESPN's real order within a second (a substitution between two free throws,
  say) is not recoverable from the table; the parity harness measures what that costs.
* **Shot coordinates.** ``ncaa_shots`` (the box-score page's shot chart) gives feet from the rim, the same
  frame as ESPN's: ``x = 25 + shot_x``, ``y = shot_y``. A shot is matched to its play on period, seconds
  left, team, shooter and result; an unmatched field goal carries ESPN's missing-coordinate sentinel.
* **Box.** Counts are summed from the plays (team rebounds count in a side's totals; a player the season's
  roster lacks is boxed under their name). Minutes are the pipeline's stint minutes (``ncaa_player_box``)
  rounded half up; they are not the official box's and disagree with ESPN's on most players, the main reason
  ``"ncaa"`` is not yet in ``SOURCE_ORDER`` (:data:`~sportsdataverse.basketball.sources.parity.HARD_GATES`).

What the feed cannot say is in :data:`~sportsdataverse.basketball.sources.contract.KNOWN_LOSSY` for
``("mbb" | "wbb", "ncaa")``, measured against ESPN on the corpus: no neutral-site flag, no tip-off time, no
line, no colors or logos, ESPN's own play wording, and the season type (stats.ncaa.org does not say whether a
game is postseason, so ``season.type`` is 2).
"""

from __future__ import annotations

import math
import os
from collections import defaultdict
from collections.abc import Mapping, Sequence
from typing import Any, Optional

#: ESPN college ``type.text`` -> ``type.id``, read off the BB-D16 corpus' ESPN captures (MBB and WBB share them).
ESPN_TYPE_IDS: dict[str, str] = {
    "End Game": "402",
    "End Period": "412",
    "TipShot": "437",
    "Dead Ball Rebound": "449",
    "PersonalFoul": "519",
    "Technical Foul": "521",
    "MadeFreeThrow": "540",
    "JumpShot": "558",
    "LayUpShot": "572",
    "DunkShot": "574",
    "RegularTimeOut": "578",
    "ShortTimeOut": "579",
    "Substitution": "584",
    "Offensive Rebound": "586",
    "Defensive Rebound": "587",
    "Lost Ball Turnover": "598",
    "Steal": "607",
    "Jumpball": "615",
    "Block Shot": "618",
    "Coach's Challenge (Overturned)": "215",
    "Coach's Challenge (Stands)": "216",
}

#: stats.ncaa.org field-goal ``event_type`` -> ESPN ``type.text`` (a tip-in is ``TipShot``, see :func:`_shot_type`).
_FIELD_GOALS = {
    "Three Point Jumper": "JumpShot",
    "Two Point Jumper": "JumpShot",
    "Hook": "JumpShot",
    "Layup": "LayUpShot",
    "Dunk": "DunkShot",
}

#: Rows ESPN does not carry: the fouled player's own row, and the held-ball (possession arrow) jump balls.
_DROPPED = {"Draw Foul", "Jumpball (held ball)", "Jumpball (lodged ball)", "Jumpball (out of bounds)"}

#: ESPN's missing-coordinate pair, carried by a field goal ``ncaa_shots`` does not locate.
_NO_COORDINATE = {"x": -214748340, "y": -214748365}
#: ESPN's coordinate on every play that is not a field goal.
_NOT_A_SHOT = {"x": 25, "y": 0}

#: ``(league) -> (regulation periods, period minutes, period word)``; WBB plays quarters from 2015-16 on.
_REGULATION = {"mbb": (2, 20, "half"), "wbb": (4, 10, "Quarter")}

_ORDINAL = {1: "1st", 2: "2nd", 3: "3rd"}


def _ordinal(n: int) -> str:
    return _ORDINAL.get(n, f"{n}th")


def _period_label(period: int, regulation: int, word: str) -> str:
    """ESPN's ``period.displayValue``: ``1st Half`` / ``2nd Quarter`` / ``OT`` / ``2OT``."""
    if period <= regulation:
        return f"{_ordinal(period)} {word.capitalize()}"
    extra = period - regulation
    return "OT" if extra == 1 else f"{extra}OT"


def _end_text(period: int, regulation: int, word: str) -> str:
    """ESPN's end-of-period text: ``End of 1st half``, ``End of 2nd Quarter``, ``End of 1st OT``."""
    if period <= regulation:
        return f"End of {_ordinal(period)} {word}"
    return f"End of {_ordinal(period - regulation)} OT"


def _clock(value: Any) -> Optional[str]:
    """stats.ncaa.org ``MM:SS`` -> ESPN's ``M:SS`` (``09:31`` -> ``9:31``, ``00:45`` -> ``0:45``)."""
    text = str(value or "")
    if ":" not in text:
        return None
    minutes, seconds = text.split(":", 1)
    try:
        return f"{int(minutes)}:{int(float(seconds)):02d}"
    except ValueError:
        return None


def _seconds_left(clock: Any) -> Optional[int]:
    text = _clock(clock)
    if text is None:
        return None
    m, s = text.split(":")
    return int(m) * 60 + int(s)


def _round_half_up(value: Any) -> int:
    return math.floor(float(value or 0) + 0.5)


def _pct(made: int, att: int) -> str:
    """ESPN's whole-number percentage, half up (the liveData adapter's rule): ``(200 m + a) // 2a``."""
    return str((200 * made + att) // (2 * att)) if att else "0"


def _subtype(row: Mapping[str, Any]) -> str:
    """The LiveStats shot subtype in ``event_description`` (``"X, 2pt drivinglayup ...; made"`` -> ``drivinglayup``)."""
    desc = str(row.get("event_description") or "")
    for tag in ("3pt ", "2pt "):
        if tag in desc:
            rest = desc.split(tag, 1)[1].split()
            return rest[0].lower() if rest else ""
    return ""


def _shot_type(row: Mapping[str, Any]) -> str:
    sub = _subtype(row)
    return "TipShot" if sub.startswith("tipin") else _FIELD_GOALS[row["event_type"]]


#: LiveStats subtype -> the words ESPN uses for it (text is presentation; KNOWN_LOSSY carries ``text``).
_SHOT_WORDS = {
    "layup": "layup",
    "drivinglayup": "driving layup",
    "tipinlayup": "tip in layup",
    "dunk": "dunk",
    "alleyoop": "alley oop dunk",
    "tipindunk": "tip dunk",
    "hookshot": "hook shot",
    "jumpshot": "jump shot",
    "pullupjumpshot": "pullup jump shot",
    "stepbackjumpshot": "step back jumpshot",
    "turnaroundjumpshot": "turnaround jump shot",
    "floatingjumpshot": "floating jump shot",
}


class _Ids:
    """Player id rewriting into ESPN's space, collecting what did not map."""

    def __init__(self, players: Mapping[str, Any]):
        self.players = players
        self.unmapped: set[str] = set()

    def player(self, ncaa_id: Any) -> Optional[str]:
        if ncaa_id in (None, ""):
            return None
        key = str(ncaa_id)
        espn = self.players.get(key)
        if espn is None:
            self.unmapped.add(key)
            return None
        return str(espn)


#: Order of the row kinds stats.ncaa.org logs at one game second, ESPN's order (module docstring, **Order**).
_SECOND_RANK = {
    "won Jumpball": 0,
    "lost Jumpball": 0,
    "Three Point Jumper": 1,
    "Two Point Jumper": 1,
    "Hook": 1,
    "Layup": 1,
    "Dunk": 1,
    "Blocked Shot": 2,
    "Turnover": 3,
    "Steal": 4,
    "Commits Foul": 5,
    "Draw Foul": 5,
    "Timeout": 6,
    "Leaves Game": 7,
    "Enters Game": 8,
    "Free Throw": 9,
    "Offensive Rebound": 10,
    "Defensive Rebound": 10,
    "Deadball Rebound": 10,
}


def _made_points(row: Mapping[str, Any]) -> int:
    if row.get("event_result") != "made":
        return 0
    return 1 if row.get("event_type") == "Free Throw" else int(row.get("shot_value") or 0)


def _normalise(pbp: Sequence[Mapping[str, Any]]) -> tuple[list[dict], bool]:
    """Rows in game order with the score columns on the right sides; ``(rows, scores were swapped)``.

    stats.ncaa.org's score columns follow the page's team order, which is not always home first: a game whose
    plays add up to the score columns the other way round has them swapped back. Game order is a stable sort on
    ``(period, game_seconds, running total)`` (a running total never falls), then :data:`_SECOND_RANK`, then the
    free-throw number: within one second the published rows arrive in no reliable order.
    """
    rows = [dict(r) for r in pbp]
    pts = {"home": 0, "away": 0}
    for r in rows:
        side = _side(r)
        if side:
            pts[side] += _made_points(r)
    last = max(rows, key=lambda r: (int(r.get("period") or 0), int(r.get("game_seconds") or 0), _total(r)))
    fh, fa = int(last.get("home_score") or 0), int(last.get("away_score") or 0)
    swapped = abs(pts["home"] - fa) + abs(pts["away"] - fh) < abs(pts["home"] - fh) + abs(pts["away"] - fa)
    if swapped:
        for r in rows:
            r["home_score"], r["away_score"] = r.get("away_score"), r.get("home_score")
    rows.sort(
        key=lambda r: (
            int(r.get("period") or 0),
            int(r.get("game_seconds") or 0),
            _total(r),
            _SECOND_RANK.get(r.get("event_type") or "", 6),
            int(r.get("ft_number") or 0),
        )
    )
    return rows, swapped


def _total(row: Mapping[str, Any]) -> int:
    return int(row.get("home_score") or 0) + int(row.get("away_score") or 0)


def _side(row: Mapping[str, Any]) -> Optional[str]:
    team = row.get("event_team_espn_team_id")
    if team is None:
        return None
    if str(team) == str(row.get("home_espn_team_id")):
        return "home"
    if str(team) == str(row.get("away_espn_team_id")):
        return "away"
    return None


def _pkey(pid: Any, name: Any) -> Optional[str]:
    """A player's key: the stats.ncaa.org id, else ``name:<NAME>`` for a player the season's roster lacks (a
    walk-on the pipeline could not identify still has a box line); None for the team itself."""
    if pid not in (None, ""):
        return str(pid)
    text = str(name or "").strip()
    return None if text.upper() in ("", "TEAM") else f"name:{text}"


def _is_team_actor(row: Mapping[str, Any]) -> bool:
    return _pkey(row.get("player_1_id"), row.get("player_1")) is None


def _text(row: Mapping[str, Any], kind: str, team_name: str) -> str:
    """ESPN-style wording for one row (presentation only)."""
    name = row.get("player_1_clean_name") or team_name
    etype = row["event_type"]
    if etype in _FIELD_GOALS:
        verb = "makes" if row.get("event_result") == "made" else "misses"
        words = "three point jumper" if row.get("shot_value") == 3 else _SHOT_WORDS.get(_subtype(row), "shot")
        assist = (
            f" ({row['player_2_clean_name']} assists)" if verb == "makes" and row.get("player_2_clean_name") else ""
        )
        return f"{name} {verb} {words}{assist}"
    if etype == "Free Throw":
        verb = "makes" if row.get("event_result") == "made" else "misses"
        n, of = row.get("ft_number"), row.get("ft_attempts")
        return f"{name} {verb} free throw" + (f" {n} of {of}" if n and of else "")
    if kind in ("Offensive Rebound", "Defensive Rebound"):
        return f"{team_name} {kind.split()[0]} Team Rebound." if _is_team_actor(row) else f"{name} {kind}."
    if kind == "Dead Ball Rebound":
        return f"{team_name} Deadball Team Rebound."
    if kind == "Technical Foul":
        return f"Technical Foul on {name}."
    if kind == "PersonalFoul":
        return f"Foul on {name}."
    if kind == "Lost Ball Turnover":
        return f"{name} turnover"
    if kind in ("Steal", "Block Shot"):
        return f"{name} {'Steal' if kind == 'Steal' else 'Block'}."
    if kind == "Substitution":
        return f"{name} subbing {'in' if etype == 'Enters Game' else 'out'} for {team_name}"
    if kind in ("ShortTimeOut", "RegularTimeOut"):
        return f"{team_name} Timeout"
    if kind.startswith("Coach's Challenge"):
        return f"{team_name} {kind}"
    if kind == "Jumpball":
        return f"Jump Ball {'won' if etype.startswith('won') else 'lost'} by {team_name}"
    return str(row.get("event_description") or "")


def _espn_kind(row: Mapping[str, Any]) -> Optional[str]:
    """ESPN ``type.text`` for one stats.ncaa.org row, or None for a row ESPN does not carry."""
    etype = row.get("event_type") or ""
    if etype in _DROPPED:
        return None
    if etype in _FIELD_GOALS:
        return _shot_type(row)
    simple = {
        "Free Throw": "MadeFreeThrow",
        "Offensive Rebound": "Offensive Rebound",
        "Defensive Rebound": "Defensive Rebound",
        "Deadball Rebound": "Dead Ball Rebound",
        "Turnover": "Lost Ball Turnover",
        "Steal": "Steal",
        "Blocked Shot": "Block Shot",
        "Leaves Game": "Substitution",
        "Enters Game": "Substitution",
        "won Jumpball": "Jumpball",
        "lost Jumpball": "Jumpball",
    }
    if etype in simple:
        return simple[etype]
    if etype == "Commits Foul":
        return "Technical Foul" if row.get("foul_tech_class") else "PersonalFoul"
    if etype == "Timeout":
        return "RegularTimeOut" if row.get("timeout_type") == "full" else "ShortTimeOut"
    if etype.startswith("Challenge"):
        # the challenger keeps its timeout ("accepted") is ESPN's "Stands" type (16-game corpus: 3 and 3)
        return (
            "Coach's Challenge (Stands)"
            if row.get("challenge_outcome") == "accepted"
            else "Coach's Challenge (Overturned)"
        )
    return None


def _shot_index(shots: Sequence[Mapping[str, Any]]) -> dict[tuple, list[Mapping[str, Any]]]:
    """``(period, seconds left, ESPN team id, made) -> shots``, consumed in order as plays claim them."""
    index: dict[tuple, list[Mapping[str, Any]]] = defaultdict(list)
    for s in shots:
        try:
            key = (int(s["period"]), int(float(s["sec_left"])), str(s.get("espn_team_id")), bool(s.get("made")))
        except (KeyError, TypeError, ValueError):
            continue
        index[key].append(s)
    return index


def _claim_shot(index: dict[tuple, list], row: Mapping[str, Any], seconds_left: Optional[int]) -> Optional[Mapping]:
    """The ``ncaa_shots`` row of one field-goal play: same key, the same shooter first, else the first left."""
    key = (
        int(row.get("period") or 0),
        seconds_left,
        str(row.get("event_team_espn_team_id")),
        row.get("event_result") == "made",
    )
    cands = index.get(key) or []
    for i, s in enumerate(cands):
        if s.get("shooter_player_id") and str(s["shooter_player_id"]) == str(row.get("player_1_id")):
            return cands.pop(i)
    return cands.pop(0) if cands else None


_EMPTY_LISTS = (
    "leaders",
    "broadcasts",
    "pickcenter",
    "againstTheSpread",
    "odds",
    "winprobability",
    "videos",
    "teamInfo",
    "espnWP",
    "timeouts",
)

_PLAYER_KEYS = [
    "minutes",
    "points",
    "fieldGoalsMade-fieldGoalsAttempted",
    "threePointFieldGoalsMade-threePointFieldGoalsAttempted",
    "freeThrowsMade-freeThrowsAttempted",
    "rebounds",
    "assists",
    "turnovers",
    "steals",
    "blocks",
    "offensiveRebounds",
    "defensiveRebounds",
    "fouls",
]

_COUNTS = ("pts", "fgm", "fga", "tpm", "tpa", "ftm", "fta", "orb", "drb", "ast", "tov", "stl", "blk", "pf")


def _tally(rows: Sequence[Mapping[str, Any]]) -> tuple[dict[str, dict[str, int]], dict[str, dict[str, int]]]:
    """Box counts from the plays: ``(per NCAA player id, per side)``; the side totals include team rows."""
    players: dict[str, dict[str, int]] = defaultdict(lambda: dict.fromkeys(_COUNTS, 0))
    sides: dict[str, dict[str, int]] = {
        s: dict.fromkeys((*_COUNTS, "team_reb", "team_tov", "tech", "to_pts"), 0) for s in ("home", "away")
    }
    for r in rows:
        side, etype = _side(r), r.get("event_type") or ""
        if side is None:
            continue
        pid = _pkey(r.get("player_1_id"), r.get("player_1"))
        bump: dict[str, int] = {}
        made = r.get("event_result") == "made"
        if etype in _FIELD_GOALS:
            three = r.get("shot_value") == 3
            bump = {
                "fga": 1,
                "fgm": int(made),
                "tpa": int(three),
                "tpm": int(three and made),
                "pts": (3 if three else 2) * made,
            }
            assister = _pkey(r.get("player_2_id"), r.get("player_2"))
            if made and assister:
                players[assister]["ast"] += 1
                sides[side]["ast"] += 1
        elif etype == "Free Throw":
            bump = {"fta": 1, "ftm": int(made), "pts": int(made)}
        elif etype in ("Offensive Rebound", "Defensive Rebound"):
            key = "orb" if etype.startswith("Off") else "drb"
            if pid:
                bump = {key: 1}
            else:
                # a team rebound counts in the side's offensive / defensive totals (ESPN's team box)
                sides[side]["team_reb"] += 1
                sides[side][key] += 1
        elif etype == "Turnover":
            bump = {"tov": 1}
            if not pid:
                sides[side]["team_tov"] += 1
        elif etype == "Steal":
            bump = {"stl": 1}
        elif etype == "Blocked Shot":
            bump = {"blk": 1}
        elif etype == "Commits Foul":
            # a bench / administrative technical charged to the team is no player's foul (ESPN's team fouls)
            bump = {"pf": 1} if pid else {}
            if r.get("foul_tech_class"):
                sides[side]["tech"] += 1
        if made and r.get("is_from_turnover") and etype in (*_FIELD_GOALS, "Free Throw"):
            # points scored off the other side's turnovers (ESPN's turnoverPoints)
            sides[side]["to_pts"] += bump.get("pts", 0)
        for k, v in bump.items():
            sides[side][k] += v
            if pid:
                players[pid][k] += v
    return dict(players), sides


def _team_statistics(t: Mapping[str, int]) -> list[dict[str, str]]:
    """Side totals as ESPN's ``boxscore.teams[].statistics`` (the names the team box reads)."""
    pairs = {
        "fieldGoalsMade-fieldGoalsAttempted": f"{t['fgm']}-{t['fga']}",
        "fieldGoalPct": _pct(t["fgm"], t["fga"]),
        "threePointFieldGoalsMade-threePointFieldGoalsAttempted": f"{t['tpm']}-{t['tpa']}",
        "threePointFieldGoalPct": _pct(t["tpm"], t["tpa"]),
        "freeThrowsMade-freeThrowsAttempted": f"{t['ftm']}-{t['fta']}",
        "freeThrowPct": _pct(t["ftm"], t["fta"]),
        "totalRebounds": t["orb"] + t["drb"],
        "offensiveRebounds": t["orb"],
        "defensiveRebounds": t["drb"],
        "assists": t["ast"],
        "steals": t["stl"],
        "blocks": t["blk"],
        "turnovers": t["tov"],
        "teamTurnovers": t["team_tov"],
        "totalTurnovers": t["tov"],
        "technicalFouls": t["tech"],
        "totalTechnicalFouls": t["tech"],
        "turnoverPoints": t["to_pts"],
        "fouls": t["pf"],
    }
    return [{"name": k, "displayValue": str(v)} for k, v in pairs.items()]


def _player_stats(c: Mapping[str, int], minutes: Any) -> list[str]:
    return [
        str(_round_half_up(minutes)),
        str(c["pts"]),
        f"{c['fgm']}-{c['fga']}",
        f"{c['tpm']}-{c['tpa']}",
        f"{c['ftm']}-{c['fta']}",
        str(c["orb"] + c["drb"]),
        str(c["ast"]),
        str(c["tov"]),
        str(c["stl"]),
        str(c["blk"]),
        str(c["orb"]),
        str(c["drb"]),
        str(c["pf"]),
    ]


def _roster(rows: Sequence[Mapping[str, Any]], player_box: Sequence[Mapping[str, Any]]) -> dict[str, dict[str, Any]]:
    """Every NCAA player who appears (on the floor, in a row or in the box): ``id -> {side, name, minutes, starter}``."""
    out: dict[str, dict[str, Any]] = {}
    first = next((r for r in rows if int(r.get("period") or 0) == 1), None)
    starters = (
        {_pkey(first.get(f"{s}_{i}_player_id"), first.get(f"{s}_{i}")) for s in ("home", "away") for i in range(1, 6)}
        if first
        else set()
    )
    for r in rows:
        for s in ("home", "away"):
            for i in range(1, 6):
                pid = _pkey(r.get(f"{s}_{i}_player_id"), r.get(f"{s}_{i}"))
                if pid and pid not in out:
                    out[pid] = {"side": s, "name": r.get(f"{s}_{i}_clean_name") or r.get(f"{s}_{i}"), "minutes": 0.0}
        side = _side(r)
        for k in ("player_1", "player_2"):
            pid = _pkey(r.get(f"{k}_id"), r.get(k))
            if pid and pid not in out and side is not None:
                # player_2 of a made shot is the assister, same side; any other player_2 is not boxed here
                out[pid] = {"side": side, "name": r.get(f"{k}_clean_name") or r.get(k), "minutes": 0.0}
    for b in player_box:
        pid = _pkey(b.get("player_id"), b.get("player"))
        if pid in out:
            out[pid]["minutes"] = b.get("mins") or 0.0
    for pid, p in out.items():
        p["starter"] = pid in starters
    return out


def _ncaa_to_espn_summary(
    bundle: Mapping[str, Any],
    *,
    espn_id: int | str,
    league: str,
    teams: Mapping[str, Mapping[str, Any]],
) -> tuple[dict, list[str]]:
    """Project one parsed stats.ncaa.org contest onto ``espn_<lg>_pbp(raw=True)``'s dict.

    Args:
        bundle: The contest's published rows: ``{"pbp": [...], "shots": [...], "player_box": [...],
            "player_crosswalk": [...]}`` (``{lg}.ncaa_pbp`` / ``ncaa_shots`` / ``ncaa_player_box`` /
            ``ncaa_player_crosswalk`` of the Data API, or the parsed contest JSON the pipeline writes).
        espn_id: The ESPN event id the summary is served under (``header.id``).
        league: ``"mbb"`` or ``"wbb"``.
        teams: ESPN team id -> its ``ncaa_espn_team_crosswalk`` row (``espn_location``, ``espn_mascot``,
            ``espn_abbreviation``, ``espn_display_name``).

    Returns:
        ``(summary, notes)``: the ESPN-shaped dict ``helper_<league>_pbp`` and ``game_page_frames`` read,
        and the adapter's degradations (unmapped players, unlocated shots).

    Raises:
        ValueError: No plays, a team without an ESPN id, a team missing from ``teams``, or a final score in
            the plays that the plays' own box does not reproduce.
    """
    pbp = [r for r in bundle.get("pbp") or [] if isinstance(r, Mapping)]
    if not pbp:
        raise ValueError("the contest has no play-by-play rows")
    regulation, _, word = _REGULATION[league]
    home_id, away_id = pbp[0].get("home_espn_team_id"), pbp[0].get("away_espn_team_id")
    if not home_id or not away_id:
        raise ValueError(f"a team has no ESPN id (home {home_id!r}, away {away_id!r})")
    team_obj = {}
    for side, tid in (("home", str(home_id)), ("away", str(away_id))):
        t = teams.get(tid)
        if not t:
            raise ValueError(f"ESPN team {tid} is not in the team crosswalk")
        team_obj[side] = {
            "id": tid,
            "location": t.get("espn_location"),
            "name": t.get("espn_mascot"),
            "abbreviation": t.get("espn_abbreviation"),
            "displayName": t.get("espn_display_name"),
            "shortDisplayName": t.get("espn_location"),
        }
    xwalk = {
        str(x["ncaa_player_id"]): x["espn_athlete_id"]
        for x in bundle.get("player_crosswalk") or []
        if x.get("espn_athlete_id")
    }
    ids, notes, eid = _Ids(xwalk), [], str(espn_id)
    rows, swapped = _normalise(pbp)
    if swapped:
        notes.append("score columns were away-first on stats.ncaa.org; swapped back to home / away")
    plays, unlocated = _plays(rows, eid, league, team_obj, ids, _shot_index(list(bundle.get("shots") or [])))
    counts, sides = _tally(rows)
    final = (int(rows[-1].get("home_score") or 0), int(rows[-1].get("away_score") or 0))
    if (sides["home"]["pts"], sides["away"]["pts"]) != final:
        notes.append(
            f"the plays end at {final} but their box adds up to {(sides['home']['pts'], sides['away']['pts'])}"
        )

    roster = _roster(rows, list(bundle.get("player_box") or []))
    box_players = []
    for side in ("away", "home"):
        athletes = []
        for pid, p in roster.items():
            if p["side"] != side:
                continue
            athletes.append(
                {
                    "active": False,
                    "athlete": {"id": ids.player(pid), "displayName": p["name"], "shortName": p["name"]},
                    "starter": p["starter"],
                    "didNotPlay": False,
                    "ejected": False,
                    "stats": _player_stats(counts.get(pid, dict.fromkeys(_COUNTS, 0)), p["minutes"]),
                }
            )
        box_players.append({"team": team_obj[side], "statistics": [{"keys": list(_PLAYER_KEYS), "athletes": athletes}]})

    if ids.unmapped:
        notes.append(
            f"{len(ids.unmapped)} stats.ncaa.org players not in ncaa_player_crosswalk (null athlete ids): {sorted(ids.unmapped)}"
        )
    if unlocated:
        notes.append(f"{unlocated} field goals ncaa_shots does not locate (ESPN's missing-coordinate sentinel)")

    summary: dict[str, Any] = {
        "boxscore": {
            "teams": [
                {"team": team_obj[s], "statistics": _team_statistics(sides[s]), "displayOrder": i, "homeAway": s}
                for i, s in enumerate(("away", "home"), start=1)
            ],
            "players": box_players,
        },
        "format": {"regulation": {"periods": regulation}},
        "header": _header(rows, eid, league, team_obj, final, regulation),
        "plays": plays,
    }
    for key in ("gameInfo", "predictor", "article", "standings", "season"):
        summary[key] = {}
    for key in _EMPTY_LISTS:
        summary[key] = []
    return summary, notes


def _header(
    rows: Sequence[Mapping[str, Any]], eid: str, league: str, team_obj: dict, final: tuple, regulation: int
) -> dict:
    """ESPN's ``header``: season, date, status and both competitors (home first, as ESPN orders them)."""
    by_period: dict[int, tuple[int, int]] = {}
    for r in rows:
        by_period[int(r.get("period") or 0)] = (int(r.get("home_score") or 0), int(r.get("away_score") or 0))
    lines: dict[str, list[dict[str, str]]] = {"home": [], "away": []}
    prev = (0, 0)
    for p in sorted(by_period):
        h, a = by_period[p]
        lines["home"].append({"displayValue": str(h - prev[0])})
        lines["away"].append({"displayValue": str(a - prev[1])})
        prev = (h, a)
    score = {"home": final[0], "away": final[1]}
    competitors = [
        {
            "id": team_obj[s]["id"],
            "homeAway": s,
            "score": str(score[s]),
            "winner": score[s] > score["away" if s == "home" else "home"],
            "team": team_obj[s],
            "linescores": lines[s],
        }
        for s in ("home", "away")
    ]
    month, day, year = (str(rows[0].get("game_date") or "//").split("/") + ["", "", ""])[:3]
    return {
        "id": eid,
        # stats.ncaa.org does not say whether a game is postseason (KNOWN_LOSSY: season_type)
        "season": {"year": int(rows[0].get("season") or 0) or None, "type": 2},
        "competitions": [
            {
                "id": eid,
                # the game's local date, no tip-off time (KNOWN_LOSSY: game_date*)
                "date": f"{year}-{month}-{day}T00:00Z" if year else None,
                # stats.ncaa.org has no neutral-site flag (KNOWN_LOSSY: neutral_site)
                "neutralSite": False,
                "playByPlaySource": "full",
                "boxscoreSource": "full",
                # the pipeline publishes a contest the night after it is played: always a final
                "status": {"type": {"completed": True, "state": "post", "description": "Final"}},
                "competitors": competitors,
            }
        ],
    }


def _plays(
    rows: Sequence[Mapping[str, Any]], eid: str, league: str, team_obj: dict, ids: _Ids, shots: dict[tuple, list]
) -> tuple[list[dict], int]:
    """ESPN ``plays[]`` in game order, with ESPN's start / end rows; ``(plays, unlocated field goals)``."""
    regulation, minutes, word = _REGULATION[league]
    plays: list[dict] = []
    unlocated = 0

    def emit(kind: str, text: str, period: int, clock: str, home: int, away: int, **extra: Any) -> dict:
        row = {
            "id": f"{eid}{len(plays) + 1:05d}",
            "sequenceNumber": str(len(plays) + 1),
            "type": {"id": ESPN_TYPE_IDS[kind], "text": kind},
            "text": text,
            "awayScore": away,
            "homeScore": home,
            "period": {"number": period, "displayValue": _period_label(period, regulation, word)},
            "clock": {"displayValue": clock},
            "scoringPlay": False,
            "scoreValue": 0,
            "shootingPlay": False,
            "pointsAttempted": 0,
            "coordinate": dict(_NOT_A_SHOT),
        }
        row.update(extra)
        plays.append(row)
        return row

    emit("Jumpball", "Start game", 1, f"{minutes}:00", 0, 0)
    for i, r in enumerate(rows):
        period, home, away = int(r.get("period") or 0), int(r.get("home_score") or 0), int(r.get("away_score") or 0)
        kind = _espn_kind(r)
        if kind is not None:
            side = _side(r)
            team_name = team_obj[side]["location"] if side else ""
            clock = _clock(r.get("clock")) or "0:00"
            made = r.get("event_result") == "made"
            fg = r.get("event_type") in _FIELD_GOALS
            ft = r.get("event_type") == "Free Throw"
            row = emit(kind, _text(r, kind, team_name), period, clock, home, away)
            if side:
                row["team"] = {"id": team_obj[side]["id"]}
            if fg or ft:
                value = int(r.get("shot_value") or (1 if ft else 2))
                # ESPN's college scoreValue is the attempt's value, made or missed
                row.update(scoringPlay=made, scoreValue=value, shootingPlay=True, pointsAttempted=value)
            actor, assister = (
                _pkey(r.get("player_1_id"), r.get("player_1")),
                _pkey(r.get("player_2_id"), r.get("player_2")),
            )
            parts = [ids.player(actor)] if actor else []
            if fg and made and assister:
                parts.append(ids.player(assister))
            if parts:
                row["participants"] = [{"athlete": {"id": p}} for p in parts]
            if fg:
                shot = _claim_shot(shots, r, _seconds_left(r.get("clock")))
                if shot is not None and shot.get("shot_x") is not None and shot.get("shot_y") is not None:
                    # feet from the rim, ESPN's frame (module docstring)
                    row["coordinate"] = {"x": 25 + float(shot["shot_x"]), "y": float(shot["shot_y"])}
                else:
                    row["coordinate"] = dict(_NO_COORDINATE)
                    unlocated += 1
        last_of_period = i + 1 == len(rows) or int(rows[i + 1].get("period") or 0) != period
        if last_of_period:
            emit("End Period", _end_text(period, regulation, word), period, "0:00", home, away)
    last = rows[-1]
    emit(
        "End Game",
        "End of Game",
        int(last.get("period") or 0),
        "0:00",
        int(last.get("home_score") or 0),
        int(last.get("away_score") or 0),
    )
    return plays, unlocated


# --------------------------------------------------------------------------- dispatch


#: Data API tables one contest is read from when the adapter fetches (no ``ctx.payload``).
_TABLES = ("pbp", "shots", "player_box")


def _api_rows(league: str, table: str, base_url: str, **params: Any) -> list[dict]:
    """Every row of ``GET {base_url}/v1/{league}/{table}?…`` (paged, ``limit`` 5000)."""
    from sportsdataverse._codegen_runtime import _json_body, _transport_errors
    from sportsdataverse.football.sources.idmap import _api_headers

    from sportsdataverse.dl_utils import download  # read at call time, so a patched download is the one used

    url = base_url.rstrip("/") + f"/v1/{league}/{table}"
    out: list[dict] = []
    while True:
        with _transport_errors(url):
            resp = download(url=url, params={**params, "limit": 5000, "offset": len(out)}, headers=_api_headers())
        body = _json_body(resp, url)
        page = body.get("data") if isinstance(body, dict) else None
        if not isinstance(page, list):
            raise ValueError(f"{url} answered no data array")
        out += page
        if len(page) < 5000:
            return out


def _fetch_contest(league: str, espn_id: int, base_url: str) -> dict[str, Any]:
    """One contest's published rows from the Data API, keyed by the ESPN game id the pipeline stamps."""
    bundle: dict[str, Any] = {t: _api_rows(league, f"ncaa_{t}", base_url, espn_game_id=str(espn_id)) for t in _TABLES}
    pbp = bundle["pbp"]
    if not pbp:
        raise ValueError(f"no ncaa_pbp rows for ESPN game {espn_id}")
    season = pbp[0].get("season")
    teams = sorted({str(r[k]) for r in pbp for k in ("home_ncaa_team_id", "away_ncaa_team_id") if r.get(k)})
    bundle["player_crosswalk"] = [
        row
        for t in teams
        for row in _api_rows(league, "ncaa_player_crosswalk", base_url, season=season, ncaa_team_id=t)
    ]
    return bundle


def _team_rows(league: str, season: int) -> dict[str, dict[str, Any]]:
    """ESPN team id -> its ``ncaa_espn_team_crosswalk`` row for the season (``2027`` = ``"2026-27"``)."""
    import polars as pl

    from sportsdataverse.mbb import ncaa_espn_team_crosswalk

    label = f"{season - 1}-{str(season)[2:]}"
    frame = ncaa_espn_team_crosswalk(league).filter((pl.col("season") == label) & pl.col("espn_team_id").is_not_null())
    return {str(r["espn_team_id"]): r for r in frame.iter_rows(named=True)}


def _ncaa_adapter(league: str, espn_id: int, ctx: Any) -> Any:
    """Dispatch adapter for ``source="ncaa"`` (MBB, WBB). Registered in :mod:`...sources.dispatch`.

    ``ctx.payload`` (all keys optional) is ``{"contest": <bundle>, "teams": {espn team id: crosswalk row}}``;
    a missing contest is read from the Data API (``SDV_DATA_API_URL``, the id-map resolver's root, with the
    ``SDV_DATA_API_KEY`` bearer), a missing team table from ``ncaa_espn_team_crosswalk``.

    Raises:
        SourceUnavailable: no payload and no Data API root, the contest is not published (the pipeline runs
            nightly, so a game in progress never is), a read failed, or the rows do not project.
    """
    from sportsdataverse.basketball.sources.dispatch import AdaptedGame
    from sportsdataverse.football.sources.dispatch import IDMAP_BASE_URL_ENV, SourceUnavailable

    payload = dict(ctx.payload or {})
    try:
        bundle = payload.get("contest")
        if bundle is None:
            base_url = os.environ.get(IDMAP_BASE_URL_ENV)
            if not base_url:
                raise ValueError(f"no contest payload and no {IDMAP_BASE_URL_ENV}")
            bundle = _fetch_contest(league, espn_id, base_url)
        pbp = bundle.get("pbp") or []
        got = {str(r.get("espn_game_id")) for r in pbp}
        if got != {str(espn_id)}:
            # an injected contest of another game would be served under this ESPN id
            raise ValueError(f"contest rows are ESPN game(s) {sorted(got)}, not {espn_id}")
        teams = payload.get("teams") or _team_rows(league, int(pbp[0]["season"]))
        summary, notes = _ncaa_to_espn_summary(bundle, espn_id=espn_id, league=league, teams=teams)
    except Exception as exc:  # a read, id or projection failure hands over to the next source
        raise SourceUnavailable(f"{league} {espn_id}: stats.ncaa.org: {type(exc).__name__}: {exc}") from exc
    contest = str((bundle.get("pbp") or [{}])[0].get("contest_id"))
    return AdaptedGame(summary=summary, native_ids={"espn_event_id": str(espn_id), "contest_id": contest}, notes=notes)


def _register_ncaa() -> None:
    """Register :func:`_ncaa_adapter` for MBB and WBB (called on import)."""
    from sportsdataverse.basketball.sources.dispatch import _register

    for league in ("mbb", "wbb"):
        _register(league, "ncaa")(_ncaa_adapter)


_register_ncaa()
