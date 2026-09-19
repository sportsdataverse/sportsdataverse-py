"""Resolve CBS's own NFL game id for an ESPN event (private, experimental).

CBS is the one measured alternate source with **no** offline id map: its NAPI payloads
carry an empty ``vendorMappings``, and the pre-kickoff id map's ``cbs_game_id`` column
(:data:`sportsdataverse.football.sources.idmap.GAME_SCHEMA`) is null because no builder
can fill it without this page. The id is published on the **week scoreboard page**, days
before kickoff, as ``<div id="game-{cbs id}" data-enhanced="true"
data-abbrev="NFL_{YYYYMMDD in US-Eastern}_{AWAY}@{HOME}">`` -- so the adapter scrapes one
page per (season, season type, week), caches it for the life of the process, and joins on
the Eastern date plus both club codes. That join is unique inside an NFL week.

The join is by **label**, never by arithmetic on the id: CBS ids are 7-digit in 2019
(``3115572``) and 8-digit in 2026 (``50029216``) with no relation between eras, and
nothing here ever synthesizes one -- an unresolved game raises out of the adapter and
dispatch falls through to the next source.

Two club-code divergences from ESPN, both era-stable: CBS writes ``JAC`` where ESPN writes
``JAX`` and ``WAS`` where ESPN writes ``WSH``. Relocated franchises carry their era's code
on the page they appeared in (``OAK`` in 2019, ``LV`` in 2026), so every acceptable spelling
per ESPN franchise id is listed in :data:`CBS_SCOREBOARD_ABBRS`.
"""

from __future__ import annotations

import re
from typing import Any, Callable, Dict, Optional, Tuple

from sportsdataverse.dl_utils import download
from sportsdataverse.football.cbs_common import _et_date, _resolve_from_scoreboard

#: One scoreboard card: the CBS game id, whether CBS says it will carry enhanced (play-level)
#: data, and the ``NFL_{date}_{away}@{home}`` abbreviation. ``data-enhanced`` is set days
#: before kickoff and is the only pre-kickoff signal that CBS will serve play-by-play at all.
_CARD_RE = re.compile(
    r'id="game-(?P<id>\d+)"\s+data-enhanced="(?P<enhanced>true|false)"\s+'
    r'data-abbrev="NFL_(?P<date>\d{8})_(?P<away>[A-Z0-9]+)@(?P<home>[A-Z0-9]+)"'
)

#: ESPN season type -> the CBS scoreboard path segment.
_SEGMENT = {1: "preseason", 2: "regular", 3: "postseason"}

#: ESPN franchise id -> every club code that franchise has appeared under on a CBS
#: scoreboard page. First entry is the current one. ``JAC``/``WAS`` are CBS's spellings of
#: ESPN's ``JAX``/``WSH``; the rest are relocations, which keep the ESPN franchise id.
CBS_SCOREBOARD_ABBRS: Dict[str, Tuple[str, ...]] = {
    "1": ("ATL",),
    "2": ("BUF",),
    "3": ("CHI",),
    "4": ("CIN",),
    "5": ("CLE",),
    "6": ("DAL",),
    "7": ("DEN",),
    "8": ("DET",),
    "9": ("GB",),
    "10": ("TEN",),
    "11": ("IND",),
    "12": ("KC",),
    "13": ("LV", "OAK"),
    "14": ("LAR", "LA", "STL"),
    "15": ("MIA",),
    "16": ("MIN",),
    "17": ("NE",),
    "18": ("NO",),
    "19": ("NYG",),
    "20": ("NYJ",),
    "21": ("PHI",),
    "22": ("ARI",),
    "23": ("PIT",),
    "24": ("LAC", "SD"),
    "25": ("SF",),
    "26": ("SEA",),
    "27": ("TB",),
    "28": ("WAS", "WSH"),
    "29": ("CAR",),
    "30": ("JAC", "JAX"),
    "33": ("BAL",),
    "34": ("HOU",),
}


def _regular_season_weeks(season: int) -> int:
    """18 regular-season weeks from 2021, 17 before (the 17-game schedule moved the playoffs)."""
    return 18 if int(season) >= 2021 else 17


def _cbs_week(season: int, season_type: int, week: int) -> int:
    """ESPN ``(season_type, week)`` -> the week number in the CBS scoreboard URL.

    CBS numbers postseason weeks as a continuation of the regular season, and it counts the
    Pro Bowl week that ESPN also counts (ESPN postseason week 5 = Super Bowl), so the Super
    Bowl of an 18-week season is week 23 -- verified on 2025 postseason week 23 (SEA @ NE).
    Preseason and regular-season weeks are ESPN's own.
    """
    if int(season_type) == 3:
        return _regular_season_weeks(season) + int(week)
    return int(week)


def _scoreboard_url(season: int, season_type: int, cbs_week: int) -> str:
    segment = _SEGMENT.get(int(season_type))
    if segment is None:
        raise ValueError(f"season_type must be one of {sorted(_SEGMENT)}, got {season_type!r}")
    return f"https://www.cbssports.com/nfl/scoreboard/{int(season)}/{segment}/{int(cbs_week)}/"


def _resolve_cbs_game_id(
    season: int,
    season_type: int,
    week: int,
    home_espn_team_id: str,
    away_espn_team_id: str,
    *,
    kickoff_utc: Optional[str] = None,
    transport: Callable[..., Any] = download,
    **kwargs: Any,
) -> Tuple[Optional[str], Dict[str, Any]]:
    """The CBS game id for one ESPN event, from the week scoreboard page.

    Args:
        season: ESPN season year.
        season_type: ESPN season type (1 preseason, 2 regular, 3 postseason).
        week: ESPN week within that season type.
        home_espn_team_id: ESPN franchise id of the home club.
        away_espn_team_id: ESPN franchise id of the away club.
        kickoff_utc: ``"YYYY-MM-DDTHH:MMZ"``; narrows the match to the Eastern date.
        transport: injection point for tests -- anything with ``download``'s signature.
        **kwargs: forwarded to ``transport`` (headers, timeout, ...).

    Returns:
        ``(cbs_game_id or None, provenance)``. ``provenance`` carries ``how`` (the URL the id
        came from, or ``"unresolved"``), ``enhanced`` (CBS's own pre-kickoff signal that it
        will carry play-by-play) and ``weeks_tried``.

        The id is **never** synthesized: an unmatched game returns ``None`` and the adapter
        hands over to the next source.
    """
    primary = _cbs_week(season, season_type, week)
    # CBS's postseason numbering has moved with the schedule (a 17th regular-season week was
    # added in 2021, the Pro Bowl week has come and gone), so a neighbouring week is tried
    # before giving up. Regular-season weeks match on the first try and never reach this.
    candidates = [w for w in ([primary] if int(season_type) != 3 else [primary, primary + 1, primary - 1]) if w >= 1]
    if int(season_type) not in _SEGMENT:
        # no such scoreboard page exists, so every candidate week is a miss
        return None, {"how": "unresolved", "enhanced": None, "weeks_tried": candidates}
    return _resolve_from_scoreboard(
        [(w, _scoreboard_url(season, season_type, w)) for w in candidates],
        _CARD_RE,
        CBS_SCOREBOARD_ABBRS.get(str(home_espn_team_id), ()),
        CBS_SCOREBOARD_ABBRS.get(str(away_espn_team_id), ()),
        _et_date(kickoff_utc),
        transport=transport,
        **kwargs,
    )
