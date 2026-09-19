"""Resolve CBS's own college-football game id for an ESPN event (private, experimental).

CBS is the one measured alternate CFB source with **no** id map of any kind: its NAPI
payloads carry an empty ``vendorMappings``, ``cfb_crosswalk`` has no CBS provider, and the
pre-kickoff id map's ``cbs_game_id`` column
(:data:`sportsdataverse.football.sources.idmap.GAME_SCHEMA`) is null because no builder can
fill it without the page this module reads. The id is published on the **week scoreboard
page**, days before kickoff, as ``<div id="game-{cbs id}" data-enhanced="true"
data-abbrev="NCAAF_{YYYYMMDD in US-Eastern}_{AWAY}@{HOME}">`` -- so the adapter reads one
page per (season, division, CBS week), caches it for the life of the process, and joins on
the Eastern date plus both clubs.

**The join is on CBS's numeric team id, not on an abbreviation.** Each card embeds both
clubs' logo URLs (``team-logos/alt/{cbs team id}.svg``, away row first), and CBS spells one
school three different ways across its own surfaces -- Texas is ``TEX`` in the team
directory, ``TEXAS`` in ``data-abbrev`` and ``UT`` in ``shortName`` -- so a number is the
only stable key. :mod:`sportsdataverse.cfb.cbs_pbp.teams` holds the ESPN -> CBS team id
snapshot; an ESPN team it does not carry resolves to nothing, never to a guess.

**CBS's CFB week is not ESPN's.** It is its own count, level with ESPN's in 2022-2025 and one
behind it in 2026, so a small window of weeks is tried and the *match* decides, rather than
an offset rule that is wrong in some season. Nothing here reads a stated week number: the
page that holds the card is the one that names it.
Only the ``FBS`` division page is read: CBS serves no play-by-play for an FCS-hosted game in
any era, so not finding the card there is itself the right answer (:func:`_resolve_cbs_game_id`
returns None and the adapter hands over).

The page cache, the card parser and the week walk are
:mod:`sportsdataverse.football.cbs_common`'s; only the league's own URL, division and week
rules live here.
"""

from __future__ import annotations

import re
from typing import Any, Callable, Dict, Optional, Tuple

from sportsdataverse.cfb.cbs_pbp.teams import _cbs_team
from sportsdataverse.dl_utils import download
from sportsdataverse.football.cbs_common import _et_date, _resolve_from_scoreboard

#: One scoreboard card's header. ``data-enhanced`` is set days before kickoff and is CBS's
#: own pre-kickoff signal that it will carry play-level data for the game at all.
_CARD_RE = re.compile(
    r'id="game-(?P<id>\d+)"\s+data-enhanced="(?P<enhanced>true|false)"\s+'
    r'data-abbrev="NCAAF_(?P<date>\d{8})_(?P<away>[A-Z0-9&.\-]+)@(?P<home>[A-Z0-9&.\-]+)"'
)


def _scoreboard_url(season: int, cbs_week: int, division: str = "FBS") -> str:
    """The week scoreboard page every card is read from."""
    return f"https://www.cbssports.com/college-football/scoreboard/{division}/{int(season)}/regular/{int(cbs_week)}/"


def _resolve_cbs_game_id(
    season: int,
    week: int,
    home_espn_team_id: Any,
    away_espn_team_id: Any,
    *,
    kickoff_utc: Optional[str] = None,
    division: str = "FBS",
    transport: Callable[..., Any] = download,
    **kwargs: Any,
) -> Tuple[Optional[str], Dict[str, Any]]:
    """The CBS game id for one ESPN college-football event, from the week scoreboard page.

    Args:
        season: ESPN season year.
        week: ESPN week within the regular season.
        home_espn_team_id: ESPN team id of the home club.
        away_espn_team_id: ESPN team id of the away club.
        kickoff_utc: ``"YYYY-MM-DDTHH:MMZ"``; narrows the match to the Eastern date.
        division: CBS scoreboard division segment. ``"FBS"`` is the only one with
            play-by-play; ``"FCS"`` exists but every game on it answers NAPI with a 404 body.
        transport: injection point for tests -- anything with ``download``'s signature.
        **kwargs: forwarded to ``transport`` (headers, timeout, ...).

    Returns:
        ``(cbs_game_id or None, provenance)``.

        | key | description |
        |---|---|
        | how | the scoreboard URL the id came from, or `"unresolved"` |
        | enhanced | CBS's own pre-kickoff signal that it will carry play-by-play |
        | weeks_tried | the CBS week numbers read, in order |

        The id is **never** synthesized: an unmatched game returns ``None`` and the adapter
        hands the game to the next source.
    """
    home, away = _cbs_team(home_espn_team_id), _cbs_team(away_espn_team_id)
    if not home or not away:
        return None, {"how": "unresolved", "enhanced": None, "weeks_tried": []}
    # CBS's CFB week is its own count -- level with ESPN's in 2022-2025, one behind it in 2026
    # -- so a small window is tried and the match decides, rather than a per-season offset rule.
    weeks = [w for w in (int(week), int(week) - 1, int(week) + 1) if w >= 1]
    return _resolve_from_scoreboard(
        [(w, _scoreboard_url(season, w, division)) for w in weeks],
        _CARD_RE,
        (str(home[0]),),
        (str(away[0]),),
        _et_date(kickoff_utc),
        fields=("home_cbs_team_id", "away_cbs_team_id"),
        transport=transport,
        **kwargs,
    )
