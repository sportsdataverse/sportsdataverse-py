"""FoxSports-sourced backup for the ESPN college-football play processor.

``CFBPlayProcess`` (see :mod:`sportsdataverse.cfb.cfb_pbp`) normally consumes
ESPN's ``college-football/summary`` feed. This module lets the **same**
processor run on **FoxSports Bifrost** data instead, as a backup/alternative
source when ESPN is unavailable.

The strategy is to adapt at the JSON boundary rather than reimplement the
pipeline: :func:`fox_to_espn_summary` reshapes a Fox ``cfb/event/{id}/data``
payload into the ESPN-``summary`` shape the processor requires (it only needs
``header`` + ``drives`` + optional ``pickcenter``), and
:func:`fox_cfb_play_process` fetches, adapts, and runs the unmodified
``CFBPlayProcess`` end-to-end -- producing the same EPA / WPA /
advanced-box-score output. (Contrast
:func:`~sportsdataverse.cfb.cfb_fox_ext.fox_cfb_pbp`, which returns the *raw*
Fox play rows without EPA/WPA.)

Field map (load-bearing):

================================================  ===========================
Fox source                                        ESPN field synthesized
================================================  ===========================
``pbp.sections[]`` (quarters) ``groups[]`` drives  ``drives.previous[].plays[]``
play ``title`` ``"1st & 10 . FSU 35"``             ``start.down`` / ``distance`` / field side
``modalPlay.play.events[0].yardStart``             ``start.yardsToEndzone`` (exact)
``events[].text`` (RUSH/PASS/INC/PUNT/FG-GOOD...)  ``type.text`` (mapped to ESPN vocab)
``timeOfPlay`` ``"15:00"``                          ``clock.displayValue`` (already MM:SS)
``periodOfPlay`` ``"1ST"``                          ``period.number``
``playDescription``                                ``text``
``play.image.altText`` (team logo)                 possession ``start.team.id``
``header.leftTeam`` / ``rightTeam``                ``competitors[]`` (left=away, right=home)
================================================  ===========================

Fidelity: the structured/numeric path (down, distance, yards-to-goal, clock,
possession, play-type, EPA, WPA) is high fidelity. Text-grammar-derived features
(detailed player attribution, penalty yardage) degrade because Fox's play
description grammar differs from ESPN's. Archive-format Fox games (with
``eventHeadline``/``keyPlays`` and no ``modalPlay`` geometry) are unsupported and
raise a clear error. Reverse-engineering notes + the validation harness live in
the sdv-internal-refs repo under ``_notes/foxsportsapi/cfb_pbp_backup/``.
"""

from __future__ import annotations

import re
from typing import Any, Dict, List, Optional

from sportsdataverse.cfb.cfb_fox_ext import _fox_get
from sportsdataverse.cfb.cfb_pbp import CFBPlayProcess

__all__ = ["fox_cfb_play_process", "fox_to_espn_summary"]

# Neutral spread used when the caller supplies no odds. EPA is unaffected; the
# WP model just sees a pick'em line (gameSpreadAvailable=False).
_NEUTRAL_ODDS = {
    "gameSpread": 0.0,
    "overUnder": 55.5,
    "homeFavorite": True,
    "gameSpreadAvailable": False,
}


# --------------------------------------------------------------------------- #
# play-level parsing helpers
# --------------------------------------------------------------------------- #
def _parse_title(title: Optional[str]) -> Dict[str, Any]:
    """Parse a Fox play ``title`` into down/distance/field-side/yardline.

    Handles ``"1st & 10 . FSU 35"`` (modern) and admin titles
    (Timeout / Two Minute Warning / Period End / Game End).
    """
    out = {"down": None, "distance": None, "side": None, "yl": None, "admin": None}
    if not title:
        return out
    # Fox titles separate down/distance from field position with a middle dot
    # (U+00B7) that some captures mangle to the replacement char. A CFB title is
    # otherwise pure ASCII (down, "&", distance, team abbr, yard line), so
    # collapsing any non-ASCII byte to a dash normalizes every separator variant
    # without embedding a literal replacement char (the lint forbids that).
    t = re.sub(r"[^\x00-\x7f]", "-", title)
    low = t.lower()
    for adm in ("timeout", "two minute warning", "period end", "game end", "end of"):
        if adm in low:
            out["admin"] = t.strip()
            return out
    m = re.search(r"(\d+)(st|nd|rd|th)\s*(?:&|and)\s*(goal|\d+)", low)
    if m:
        out["down"] = int(m.group(1))
        out["distance"] = 0 if m.group(3) == "goal" else int(m.group(3))
    m2 = re.search(r"([A-Za-z]{1,5})?\s+(\d{1,2})\s*$", t)
    if m2:
        out["side"] = (m2.group(1) or "").strip() or None
        out["yl"] = int(m2.group(2))
    return out


def _scrimmage_ytg(play: Dict[str, Any]) -> Optional[int]:
    """Yards-to-goal at the snap, lifted from modalPlay drive-chart geometry."""
    mp = (play.get("modalPlay") or {}).get("play") or {}
    for e in mp.get("events") or []:
        if e.get("yardStart") is not None:
            return int(e["yardStart"])
    for ln in mp.get("lines") or []:
        if ln.get("type") == "scrimmage" and ln.get("position") is not None:
            return int(ln["position"])
    return None


def _yards_gained(play: Dict[str, Any]) -> int:
    mp = (play.get("modalPlay") or {}).get("play") or {}
    evs = mp.get("events") or []
    if evs and evs[0].get("yardStart") is not None and evs[-1].get("yardEnd") is not None:
        return int(evs[0]["yardStart"]) - int(evs[-1]["yardEnd"])
    return 0


def _event_texts(play: Dict[str, Any]) -> List[str]:
    mp = (play.get("modalPlay") or {}).get("play") or {}
    return [e.get("text", "").upper() for e in (mp.get("events") or [])]


def _espn_type_text(play: Dict[str, Any], parsed: Dict[str, Any]) -> str:
    """Resolve ESPN ``type.text`` from the Fox event set + title + description."""
    evs = set(_event_texts(play))
    desc = (play.get("playDescription") or "").lower()
    if parsed["admin"]:
        a = parsed["admin"].lower()
        if "timeout" in a:
            return "Timeout"
        if "two minute" in a:
            return "Two-minute warning"
        if "period" in a or "quarter" in a:
            return "End Period"
        if "game" in a:
            return "End of Game"
    td = "TD" in evs
    if "INT" in evs:
        return "Interception Return Touchdown" if td else "Pass Interception Return"
    if "SACK" in evs:
        return "Sack"
    if "FG - GOOD" in evs or "FG-GOOD" in evs:
        return "Field Goal Good"
    if ("fg" in desc and "missed" in desc) or "FG - NO" in evs:
        return "Field Goal Missed"
    if "PAT - GOOD" in evs or "PAT-GOOD" in evs:
        return "Extra Point Good"
    if "PUNT" in evs:
        return "Punt Return Touchdown" if td else "Punt"
    if "KICK" in evs:
        return "Kickoff Return Touchdown" if td else "Kickoff"
    if "INC" in evs:
        return "Pass Incompletion"
    if "PASS" in evs:
        return "Passing Touchdown" if td else "Pass Reception"
    if "RUSH" in evs:
        return "Rushing Touchdown" if td else "Rush"
    if "PEN" in evs or desc.startswith("penalty"):
        return "Penalty"
    if "pass" in desc and ("incomplete" in desc or "no gain" in desc):
        return "Pass Incompletion"
    if "pass" in desc:
        return "Pass Reception"
    if "rush" in desc or "run" in desc:
        return "Rush"
    return "No Play"


def _period_num(s: Optional[str]) -> Optional[int]:
    return {"1ST": 1, "2ND": 2, "3RD": 3, "4TH": 4, "OT": 5}.get((s or "").upper().strip())


def _abbr(team: Dict[str, Any]) -> str:
    return (team.get("name") or "").strip()


def _team_id(team: Dict[str, Any]) -> Optional[str]:
    uri = team.get("uri") or (team.get("entityLink") or {}).get("contentUri") or ""
    m = re.search(r"/teams/(\d+)", uri)
    return m.group(1) if m else None


# --------------------------------------------------------------------------- #
# adapter
# --------------------------------------------------------------------------- #
def fox_to_espn_summary(fox_data: Dict[str, Any]) -> Dict[str, Any]:
    """Adapt a Fox ``cfb/event/{id}/data`` payload into the ESPN-summary shape.

    Args:
        fox_data: Parsed JSON from
            ``api.foxsports.com/bifrost/v1/cfb/event/{id}/data``.

    Returns:
        A dict shaped like ESPN's ``college-football/summary`` response
        (``header`` + ``drives`` + stub ``pickcenter``/``boxscore``/...),
        ready to assign onto ``CFBPlayProcess(...).json``.

    Raises:
        ValueError: if ``fox_data`` is an archive-format game (no ``modalPlay``
            geometry on any play), where yards-to-goal is unavailable and the
            EPA pipeline cannot run.
    """
    h = fox_data.get("header") or {}
    left, right = h.get("leftTeam") or {}, h.get("rightTeam") or {}
    # Fox convention: left = away, right = home ("KENT at FSU").
    away_id, home_id = _team_id(left), _team_id(right)
    away_abbr, home_abbr = _abbr(left), _abbr(right)

    def team_meta(t: Dict[str, Any], side: str) -> Dict[str, Any]:
        return {
            "homeAway": side,
            "team": {
                "id": _team_id(t),
                "name": (t.get("stackedNameBottom") or "").strip(),  # mascot
                "location": (t.get("longName") or t.get("name") or "").strip(),
                "abbreviation": _abbr(t),
            },
        }

    yr = None
    m = re.search(r"(\d{4})-\d{2}-\d{2}", h.get("eventTime") or "")
    if m:
        yr = int(m.group(1))

    header = {
        "id": h.get("id"),
        "season": {"year": yr, "type": 2},
        "week": None,
        "competitions": [
            {
                "status": {"type": {"completed": (h.get("eventStatus") == 3)}},
                "playByPlaySource": "full",
                "boxscoreSource": "full",
                "competitors": [team_meta(right, "home"), team_meta(left, "away")],
            }
        ],
    }

    # Possession resolver: map every team display string Fox might attach to a
    # play/drive (logo altText, longName, stacked names, abbr) -> team id. The
    # play's team logo (``play.image.altText``) names the team running the play
    # -- the correct possession signal, NOT the field-position abbr in the title
    # (which is whose *territory* the ball sits in).
    name2id: Dict[str, Optional[str]] = {}
    for t, tid in ((left, away_id), (right, home_id)):
        for key in (
            t.get("imageAltText"),
            t.get("longName"),
            t.get("stackedNameTop"),
            t.get("name"),
            (t.get("entityLink") or {}).get("imageAltText"),
        ):
            if key:
                name2id[key.strip()] = tid

    secs = (fox_data.get("pbp") or {}).get("sections") or []

    any_modal = any(p.get("modalPlay") for s in secs for g in (s.get("groups") or []) for p in (g.get("plays") or []))
    if secs and not any_modal:
        raise ValueError(
            "Fox archive-format game (no modalPlay geometry / 'eventHeadline' "
            "present): yards-to-goal is unavailable, so the EPA pipeline cannot "
            "run. Only the modern full-data Fox format is supported."
        )

    home_running, away_running = 0, 0
    seq = 0
    drives_out: List[Dict[str, Any]] = []

    for s in secs:
        for g in s.get("groups") or []:
            off_name = (g.get("entityLink") or {}).get("imageAltText") or ""
            off_id = name2id.get(off_name.strip())
            post: Dict[str, int] = {}
            for sc in g.get("scores") or []:
                try:
                    post[(sc.get("title") or "").strip()] = int(sc.get("score"))
                except (TypeError, ValueError):
                    pass
            post_home = post.get(home_abbr, home_running)
            post_away = post.get(away_abbr, away_running)

            plays_out: List[Dict[str, Any]] = []
            group_plays = g.get("plays") or []
            for idx, p in enumerate(group_plays):
                seq += 1
                parsed = _parse_title(p.get("title"))
                ytg = _scrimmage_ytg(p)
                gained = _yards_gained(p)
                ttext = _espn_type_text(p, parsed)
                play_team = (p.get("image") or {}).get("altText") or ""
                pos_id = name2id.get(play_team.strip()) or off_id
                is_score = ttext in (
                    "Rushing Touchdown",
                    "Passing Touchdown",
                    "Field Goal Good",
                    "Extra Point Good",
                    "Interception Return Touchdown",
                    "Punt Return Touchdown",
                    "Kickoff Return Touchdown",
                )
                hs, as_ = home_running, away_running
                if is_score and idx == len(group_plays) - 1:
                    hs, as_ = post_home, post_away

                is_home_poss = pos_id == home_id

                def _yl(y: Optional[int], _home: bool = is_home_poss) -> Optional[int]:
                    if y is None:
                        return None
                    return (100 - y) if _home else y

                end_ytg = None if ytg is None else max(0, ytg - gained)
                plays_out.append(
                    {
                        "id": f"{header['id']}{seq:03d}",
                        "sequenceNumber": str(seq),
                        "text": p.get("playDescription"),
                        "type": {"text": ttext},
                        "scoringPlay": bool(is_score),
                        "statYardage": gained,
                        "homeScore": hs,
                        "awayScore": as_,
                        "clock": {"displayValue": p.get("timeOfPlay") or "0:00"},
                        "period": {"number": _period_num(p.get("periodOfPlay"))},
                        "start": {
                            "down": parsed["down"],
                            "distance": parsed["distance"],
                            "yardLine": _yl(ytg),
                            "yardsToEndzone": ytg,
                            "downDistanceText": re.split(r"[^\x00-\x7f]", p.get("title") or "")[0].strip(),
                            "team": {"id": pos_id},
                        },
                        "end": {
                            "down": None,
                            "distance": None,
                            "yardLine": _yl(end_ytg),
                            "yardsToEndzone": end_ytg,
                            "team": {"id": pos_id},
                        },
                    }
                )

            # A play's END state = the NEXT play's START state within the drive
            # (correct EPA "end" situation + keeps end.* columns numerically typed
            # -- an all-null column is inferred as String and breaks comparisons).
            for i in range(len(plays_out) - 1):
                nxt = plays_out[i + 1]["start"]
                end = plays_out[i]["end"]
                if nxt["down"] is not None:
                    end["down"] = nxt["down"]
                    end["distance"] = nxt["distance"]
                if nxt["yardsToEndzone"] is not None:
                    end["yardsToEndzone"] = nxt["yardsToEndzone"]
                    end["yardLine"] = nxt["yardLine"]
                    end["team"]["id"] = nxt["team"]["id"]

            home_running, away_running = post_home, post_away

            first, last = plays_out[0], plays_out[-1]
            drive_yards = sum(pp["statYardage"] for pp in plays_out if isinstance(pp["statYardage"], (int, float)))
            drives_out.append(
                {
                    "id": g.get("id"),
                    "displayResult": g.get("title"),
                    "result": g.get("title"),
                    "shortDisplayResult": g.get("title"),
                    "description": g.get("subtitle"),
                    "isScore": (g.get("title") in ("TOUCHDOWN", "FIELD GOAL")),
                    "team": {
                        "shortDisplayName": off_name,
                        "displayName": off_name,
                        "name": off_name,
                        "abbreviation": (home_abbr if off_id == home_id else away_abbr),
                    },
                    "yards": drive_yards,
                    "offensivePlays": len(plays_out),
                    "timeElapsed": {"displayValue": None},
                    "start": {
                        "period": {"number": first["period"]["number"], "type": None},
                        "yardLine": first["start"]["yardLine"],
                        "clock": {"displayValue": first["clock"]["displayValue"]},
                        "text": None,
                    },
                    "end": {
                        "period": {"number": last["period"]["number"], "type": None},
                        "yardLine": last["end"]["yardLine"],
                        "clock": {"displayValue": last["clock"]["displayValue"]},
                    },
                    "plays": plays_out,
                }
            )

    return {
        "header": header,
        "drives": {"previous": drives_out},
        "pickcenter": [],
        "boxscore": fox_data.get("boxscore") or {},
        "scoringPlays": [],
        "standings": [],
        "leaders": [],
        "videos": [],
        "broadcasts": [],
        "againstTheSpread": [],
        "odds": [],
        "winprobability": [],
        "predictor": {},
        "gameInfo": {},
        "format": {"regulation": {"periods": 4}},
    }


# --------------------------------------------------------------------------- #
# public loader
# --------------------------------------------------------------------------- #
def fox_cfb_play_process(
    event_id,
    odds_override: Optional[Dict[str, Any]] = None,
    process: bool = True,
    raw: bool = False,
    **kwargs,
) -> Dict[str, Any]:
    """Build a *processed* CFB play-by-play game from FoxSports as a backup to ESPN.

    Where :func:`~sportsdataverse.cfb.cfb_fox_ext.fox_cfb_pbp` returns the raw Fox
    play-by-play rows, this runs Fox data through the full ESPN play processor:
    it fetches FoxSports Bifrost ``cfb/event/{event_id}/data``, adapts it into the
    ESPN-``summary`` shape via :func:`fox_to_espn_summary`, and runs the same
    :class:`~sportsdataverse.cfb.cfb_pbp.CFBPlayProcess` pipeline ESPN games use
    -- producing EPA / WPA / advanced box score. The result carries
    ``source="fox"`` so downstream consumers know the provenance (and that
    text-derived columns are lower fidelity than the ESPN path).

    Note:
        ``event_id`` is a **FoxSports** event id, which differs from the ESPN
        game id. To back up a specific ESPN game, resolve the matching Fox
        event by teams + date first.

    Args:
        event_id: FoxSports CFB event id (e.g. ``41616``).
        odds_override: Optional ``{gameSpread, overUnder, homeFavorite,
            gameSpreadAvailable}`` dict. Fox does not expose a clean pre-game
            spread, so when omitted a neutral pick'em line is used (EPA is
            unaffected; only the WP model's spread term is neutralized).
        process: If ``True`` (default) run the full
            :meth:`~sportsdataverse.cfb.cfb_pbp.CFBPlayProcess.run_processing_pipeline`
            (EPA/WPA/box). If ``False`` run the lighter
            :meth:`~sportsdataverse.cfb.cfb_pbp.CFBPlayProcess.run_cleaning_pipeline`.
        raw: If ``True`` skip the processor entirely and return the adapted
            ESPN-summary dict (the input the processor would consume).
        **kwargs: Forwarded to the Fox HTTP fetch.

    Returns:
        dict: The processed game payload (same keys as
        :meth:`CFBPlayProcess.run_processing_pipeline`) with an added
        ``source="fox"`` key. When ``raw=True``, the adapted summary dict.

    Example:
        Quick start::

            from sportsdataverse.cfb import fox_cfb_play_process
            game = fox_cfb_play_process(41616)
            print(len(game["plays"]), game["source"])
    """
    fox_data = _fox_get(f"cfb/event/{event_id}/data", **kwargs)

    summary = fox_to_espn_summary(fox_data)
    if raw:
        summary["source"] = "fox"
        return summary

    proc = CFBPlayProcess(
        gameId=int(event_id),
        odds_override=odds_override or _NEUTRAL_ODDS,
    )
    proc.json = summary
    result = proc.run_processing_pipeline() if process else proc.run_cleaning_pipeline()
    if isinstance(result, dict):
        result["source"] = "fox"
    return result
