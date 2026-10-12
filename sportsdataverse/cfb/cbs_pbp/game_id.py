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
The play-by-play adapter reads only the ``FBS`` division page: CBS serves no play-by-play for
an FCS-hosted game in any era, so not finding the card there is itself the right answer
(:func:`_resolve_cbs_game_id` returns None and the adapter hands over). The id map passes
``divisions=("FBS", "FCS")``, because an FCS game still has a CBS id worth storing.

History is matched offline by :func:`_match_cbs_card` against a season's stored cards (the
``cfbfastR-cfb-alt-raw`` harvest), which needs no week rule at all: a pair of teams plus the
Eastern date names one game in a season.

The page cache, the card parser and the week walk are
:mod:`sportsdataverse.football.cbs_common`'s; only the league's own URL, division and week
rules live here.
"""

from __future__ import annotations

import re
from datetime import datetime
from typing import Any, Callable, Dict, List, Mapping, Optional, Sequence, Tuple

from sportsdataverse.cfb.cbs_pbp.teams import _cbs_team
from sportsdataverse.dl_utils import download
from sportsdataverse.football.cbs_common import _et_date, _resolve_from_scoreboard

#: One scoreboard card's header. ``data-enhanced`` is set days before kickoff and is CBS's
#: own pre-kickoff signal that it will carry play-level data for the game at all.
_CARD_RE = re.compile(
    r'id="game-(?P<id>\d+)"\s+data-enhanced="(?P<enhanced>true|false)"\s+'
    r'data-abbrev="NCAAF_(?P<date>\d{8})_(?P<away>[A-Z0-9&.\-]+)@(?P<home>[A-Z0-9&.\-]+)"'
)


#: CBS postseason week numbers continue its own regular-season count (FBS 2015: 17-20, 2017:
#: 16-19), and ESPN numbers every bowl week 1, so a postseason game tries this whole window.
# ponytail: a fixed window, not the season's own week list; the upgrade path is reading week 1's
# navigation links like the history harvest does, if CBS ever runs a week past 21.
_POSTSEASON_WEEKS = tuple(range(14, 22))


def _scoreboard_url(season: int, cbs_week: int, division: str = "FBS", segment: str = "regular") -> str:
    """The week scoreboard page every card is read from."""
    return f"https://www.cbssports.com/college-football/scoreboard/{division}/{int(season)}/{segment}/{int(cbs_week)}/"


def _resolve_cbs_game_id(
    season: int,
    week: int,
    home_espn_team_id: Any,
    away_espn_team_id: Any,
    *,
    kickoff_utc: Optional[str] = None,
    season_type: int = 2,
    divisions: Sequence[str] = ("FBS",),
    transport: Callable[..., Any] = download,
    **kwargs: Any,
) -> Tuple[Optional[str], Dict[str, Any]]:
    """The CBS game id for one ESPN college-football event, from the week scoreboard page.

    Args:
        season: ESPN season year.
        week: ESPN week within the season type (ignored for the postseason, where ESPN
            numbers every bowl week 1 and :data:`_POSTSEASON_WEEKS` is read instead).
        home_espn_team_id: ESPN team id of the home club.
        away_espn_team_id: ESPN team id of the away club.
        kickoff_utc: ``"YYYY-MM-DDTHH:MMZ"``; narrows the match to the Eastern date.
        season_type: ESPN season type: ``3`` reads CBS's ``postseason`` pages, anything else
            the ``regular`` ones.
        divisions: CBS scoreboard divisions to read, in order. ``("FBS",)`` (the default) is
            the only one with play-by-play; the id map adds ``"FCS"``, whose games answer NAPI
            with a 404 body but still carry a CBS game id.
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
    if int(season_type) == 3:
        segment, weeks = "postseason", list(_POSTSEASON_WEEKS)
    else:
        # CBS's CFB week is its own count -- level with ESPN's in 2022-2025, one behind it in
        # 2026 -- so a small window is tried and the match decides, not a per-season offset rule.
        segment, weeks = "regular", [w for w in (int(week), int(week) - 1, int(week) + 1) if w >= 1]
    return _resolve_from_scoreboard(
        [(w, _scoreboard_url(season, w, d, segment)) for w in weeks for d in divisions],
        _CARD_RE,
        (str(home[0]),),
        (str(away[0]),),
        _et_date(kickoff_utc),
        fields=("home_cbs_team_id", "away_cbs_team_id"),
        transport=transport,
        **kwargs,
    )


def _match_cbs_card(
    cards: List[Mapping[str, Any]],
    home_espn_team_id: Any,
    away_espn_team_id: Any,
    kickoff_utc: Optional[str] = None,
) -> Optional[str]:
    """The CBS game id for one ESPN game from an already-read list of scoreboard cards, or None.

    The offline twin of :func:`_resolve_cbs_game_id`, for a whole season's stored cards (the
    ``cfbfastR-cfb-alt-raw`` harvest keeps them as the parsed dicts this module's pages yield).
    It joins on both clubs' CBS team ids **in either order** -- CBS and ESPN can name opposite
    home teams for a neutral-site game -- and then on the Eastern date, give or take three
    days. A pair with no card that close, or two of them, is None, never a guess.

    Args:
        cards: scoreboard cards (``cbs_game_id``, ``date``, ``home_cbs_team_id``,
            ``away_cbs_team_id``, ...), any number of pages and divisions mixed.
        home_espn_team_id: ESPN team id of the home club.
        away_espn_team_id: ESPN team id of the away club.
        kickoff_utc: ``"YYYY-MM-DDTHH:MMZ"``; narrows the match to the Eastern date.

    Returns:
        The CBS game id, or None when either club has no CBS id or no single game matches.
    """
    home, away = _cbs_team(home_espn_team_id), _cbs_team(away_espn_team_id)
    if not home or not away:
        return None
    pair = {str(home[0]), str(away[0])}
    hits = [c for c in cards if {str(c.get("home_cbs_team_id")), str(c.get("away_cbs_team_id"))} == pair]
    et_date = _et_date(kickoff_utc)
    if et_date:
        # A card a day or three off is still this game (no pair meets twice in a week). Further
        # off is a different meeting -- or ESPN's ghost row for a game it re-listed under a new
        # id when it was postponed (UCF-Tulane 2016, 28 days), which must not claim the id.
        target = datetime.strptime(et_date, "%Y%m%d")
        near = [c for c in hits if abs((datetime.strptime(str(c.get("date")), "%Y%m%d") - target).days) <= 3]
        hits = [c for c in near if c.get("date") == et_date] or near
    # One game can sit on two pages (FBS and FCS for a cross-division game): count ids, not cards.
    ids = {str(c["cbs_game_id"]) for c in hits}
    return ids.pop() if len(ids) == 1 else None
