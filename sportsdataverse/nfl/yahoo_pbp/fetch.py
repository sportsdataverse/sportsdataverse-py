"""Resolve one NFL game's Yahoo id from an ESPN event id (private, experimental).

The NFL is the one league whose Yahoo game id needs **no stored map and no team table**:
``nfl.g.{US-Eastern kickoff date YYYYMMDD}{ESPN home team id, zero-padded to 3}``, because
Yahoo's ``nfl.t.N`` number *is* the ESPN team id (32/32 measured, ``B_yahoo_nfl.md`` §4).
Verified 17/17 on 2013 week 1, 17/17 on 2017 week 1, 16/16 on 2026 week 1 (including the
Wednesday opener whose UTC kickoff rolls past midnight) and on the neutral-site Super Bowl LX.

Cascade, strongest first -- an id is **never invented**:

1. the id map's stored ``yahoo_game_id`` (a fact, not a formula);
2. the formula over the row's ``kickoff_utc`` + ``home_espn_team_id``;
3. the same formula over a row rebuilt from the nflverse schedule, which is how Game on Paper's
   call shape (the ESPN event id and nothing else) resolves offline;
4. :class:`...dispatch.SourceUnavailable` -- hand the game to the next source.

Fetching, the 429's 23-byte ``text/html`` body and the HTTP-200 "no coverage" shape are
league-neutral and live in :mod:`sportsdataverse.football.yahoo_common`, shared with the CFB
adapter.
"""

from __future__ import annotations

from typing import Any, Dict, Mapping, Optional, Tuple

import polars as pl

from sportsdataverse.football.yahoo_common import (
    _fetch_playbook_boxscore,
    _game_block,
    _has_plays,
    _kickoff_et_date,
    _resolve_game,
)

__all__ = [
    "_fetch_playbook_boxscore",
    "_game_block",
    "_has_plays",
    "_kickoff_et_date",
    "_resolve_game",
    "_resolve_row",
    "_row_from_nflverse_schedule",
    "_yahoo_nfl_game_id",
]


def _yahoo_nfl_game_id(
    kickoff_utc: Optional[str], home_espn_team_id: Any, *, et_date: Optional[str] = None
) -> Optional[str]:
    """``nfl.g.{ET date}{ESPN home id:03d}``, or None when either half is unknown.

    ``et_date`` (``"YYYYMMDD"`` or ``"YYYY-MM-DD"``) is the nflverse schedule's ``gameday``,
    which is already the kickoff's Eastern calendar date; it is used when the row states no
    ``kickoff_utc``. Never guesses: a missing date or an unmappable club returns None, and the
    adapter then hands the game to the next source rather than fetching a wrong one.
    """
    date = _kickoff_et_date(kickoff_utc) or (str(et_date).replace("-", "") if et_date else None)
    if not date or len(date) != 8 or not date.isdigit() or home_espn_team_id is None:
        return None
    try:
        number = int(str(home_espn_team_id).rsplit(".", 1)[-1])
    except (TypeError, ValueError):
        return None
    return f"nfl.g.{date}{number:03d}"


def _row_from_nflverse_schedule(espn_id: Any) -> Optional[Dict[str, Any]]:
    """Partial id-map row for one ESPN event id from the nflverse schedule (offline, cached).

    Game on Paper calls dispatch with the ESPN event id alone, so without this the Yahoo id is
    unresolvable on its call shape. The schedule states the kickoff's Eastern date (``gameday``)
    and both clubs, which is the whole formula. It reuses the Shield adapter's per-process
    schedule cache and its club-code -> ESPN-franchise table rather than re-reading the release
    asset or re-deriving the mapping, and -- per that adapter's own finding -- emits **no**
    ``home_team`` / ``away_team`` sub-row: the schedule's club code is nflverse's (``LA``,
    ``WAS``) and writing it where an ESPN abbreviation is expected charges no timeout and
    attributes no penalty to either club.

    Returns None when the schedule is unreachable or the event id is not in it.
    """
    try:
        from sportsdataverse.nfl.shield_pbp.to_espn_summary import (
            _ESPN_TEAM_ID_BY_ABBR,
            _nflverse_schedule,
        )
        from sportsdataverse.nfl.utils_date import get_current_nfl_season

        season = int(get_current_nfl_season())
        schedule = _nflverse_schedule((season - 1, season))
        if schedule is None:
            return None
        hit = schedule.filter(pl.col("espn").cast(pl.Utf8) == str(espn_id))
        if hit.is_empty():
            return None
        game = hit.row(0, named=True)
    except Exception:  # noqa: BLE001 -- an unreachable release asset is a miss, never a raise
        return None
    return {
        "league": "nfl",
        "espn_event_id": str(espn_id),
        "nflverse_game_id": game.get("game_id"),
        "gameday": game.get("gameday"),
        "home_espn_team_id": _ESPN_TEAM_ID_BY_ABBR.get(str(game.get("home_team"))),
        "away_espn_team_id": _ESPN_TEAM_ID_BY_ABBR.get(str(game.get("away_team"))),
        "spread_line": game.get("spread_line"),
        "total_line": game.get("total_line"),
        "odds_source": "nflverse_schedule",
    }


def _resolve_row(
    espn_id: Any, idmap_row: Optional[Mapping[str, Any]]
) -> Tuple[Dict[str, Any], Optional[str], Dict[str, str]]:
    """``(row, yahoo_game_id, provenance)`` -- the id-map row filled in far enough to fetch.

    ``provenance`` is ``{"idmap_resolved_by": ..., "yahoo_id_resolved_by": ...}``. The schedule
    leg runs only when the row cannot already answer both questions the adapter asks of it (the
    Yahoo id, and the two ESPN team ids), so a complete id-map row costs no release-asset read
    on Game on Paper's request path. ``yahoo_game_id`` is None when nothing resolved it -- the
    adapter then hands over rather than inventing one.
    """
    row: Dict[str, Any] = dict(idmap_row or {})
    row.setdefault("espn_event_id", str(espn_id))

    def _game_id() -> Optional[str]:
        return row.get("yahoo_game_id") or _yahoo_nfl_game_id(
            row.get("kickoff_utc"), row.get("home_espn_team_id"), et_date=row.get("gameday")
        )

    idmap_resolved_by = "idmap" if idmap_row else "none"
    if not (_game_id() and row.get("home_espn_team_id") and row.get("away_espn_team_id")):
        from_schedule = _row_from_nflverse_schedule(espn_id)
        if from_schedule:
            idmap_resolved_by = "nflverse_schedule" if not idmap_row else "idmap+nflverse_schedule"
            for key, value in from_schedule.items():
                if row.get(key) is None:
                    row[key] = value
    game_id = _game_id()
    return (
        row,
        str(game_id) if game_id else None,
        {
            "idmap_resolved_by": idmap_resolved_by,
            "yahoo_id_resolved_by": ("idmap" if row.get("yahoo_game_id") else "computed") if game_id else "unresolved",
        },
    )
