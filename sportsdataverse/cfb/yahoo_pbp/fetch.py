"""Fetch one Yahoo CFB game and resolve its game id from an ESPN event id (private, experimental).

Two jobs, both of which have to work while ESPN is unreachable -- that is the outage this
adapter exists for:

* **Id resolution.** Yahoo is the only CFB source whose game id needs no stored map:
  ``ncaaf.g.{US-Eastern kickoff date YYYYMMDD}{home team's Yahoo number, zero-padded to 4}``
  (157/157 verified 2014-2026, 934/934 against the stored 2025 crosswalk). The stored id map
  is still preferred when it has one -- it is a fact, not a formula -- and the published
  ``cfb_crosswalk`` release asset is the last resort for a game the id map never saw.
* **Fetching.** Through :func:`sportsdataverse.yahoo.yahoo_shangrila.yahoo_playbook_boxscore`
  (``playbookBoxscorePoll`` while a game is live), so retry/backoff/caching stay in
  ``dl_utils.download``. Yahoo's edge answers a rate limit with a **23-byte ``text/html``
  body**, not JSON: ``download`` already retries 429, and anything that still comes back
  without the shangrila envelope is reported as a transient failure rather than parsed as
  an empty game (:func:`_game_block`).
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, Mapping, Optional, Tuple
from zoneinfo import ZoneInfo

import polars as pl

from sportsdataverse.cfb.yahoo_pbp.teams import _yahoo_team_number

_ET = ZoneInfo("America/New_York")

#: ``kickoff_utc`` formats the id map has shipped. The minutes-only form is what
#: ``idmap.GAME_SCHEMA`` documents; the others are what a schedule row carries.
_KICKOFF_FORMATS = ("%Y-%m-%dT%H:%MZ", "%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%dT%H:%M%z")


def _kickoff_et_date(kickoff: Optional[str]) -> Optional[str]:
    """``"2026-09-13T00:00Z"`` -> ``"20260912"``: the kickoff's **US-Eastern** calendar date.

    Eastern, not the venue's local zone: it is what reproduces Yahoo's own ids on a full
    season (934/934 in 2025, including the late-window West-coast games that roll past
    midnight UTC).
    """
    if not kickoff:
        return None
    text = str(kickoff).strip()
    for fmt in _KICKOFF_FORMATS:
        try:
            parsed = datetime.strptime(text, fmt)
        except ValueError:
            continue
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return f"{parsed.astimezone(_ET):%Y%m%d}"
    return None


def _yahoo_cfb_game_id(
    kickoff_utc: Optional[str], home_espn_team_id: Any, yahoo_team_number: Any = None
) -> Optional[str]:
    """``ncaaf.g.{ET date}{home Yahoo number:04d}``, or None when either half is unknown.

    ``yahoo_team_number`` (the id map's ``home_team.yahoo_team_id``) wins over the committed
    ESPN -> Yahoo table when it is given: a stored fact outranks a snapshot.
    """
    date = _kickoff_et_date(kickoff_utc)
    if date is None:
        return None
    number = yahoo_team_number if yahoo_team_number is not None else _yahoo_team_number(home_espn_team_id)
    if number is None:
        return None
    try:
        number = int(str(number).rsplit(".", 1)[-1])
    except (TypeError, ValueError):
        return None
    return f"ncaaf.g.{date}{number:04d}"


#: ``cfb_crosswalk`` frames already read, keyed by the season tuple. Successful reads only:
#: caching a miss would make one transient release-asset failure permanent for the life of a
#: Game on Paper worker, and this runs on its request path.
_CROSSWALK_CACHE: Dict[Tuple[int, ...], pl.DataFrame] = {}


def _crosswalk_yahoo_id(espn_id: Any, seasons: Tuple[int, ...]) -> Optional[str]:
    """``yahoo_game_id`` for an ESPN event id from the published ``cfb_crosswalk`` asset, or None.

    The last leg of the cascade, and the only one that works with **no id-map row at all**
    (the formula needs a kickoff date, which only a row or a schedule states). Never raises:
    an unreachable release asset is a miss.
    """
    if seasons not in _CROSSWALK_CACHE:
        try:
            from sportsdataverse.cfb import load_cfb_schedule_crosswalk

            frame = load_cfb_schedule_crosswalk(list(seasons))
        except Exception:  # noqa: BLE001 -- an unreachable asset is a miss, never a raise
            return None
        if not isinstance(frame, pl.DataFrame) or frame.is_empty() or "yahoo_game_id" not in frame.columns:
            return None
        _CROSSWALK_CACHE[seasons] = frame
    frame = _CROSSWALK_CACHE[seasons]
    hit = frame.filter(pl.col("espn_game_id").cast(pl.Utf8) == str(espn_id))
    if hit.is_empty():
        return None
    return hit.row(0, named=True).get("yahoo_game_id") or None


def _resolve_game_id(
    espn_id: Any, idmap_row: Optional[Mapping[str, Any]], seasons: Tuple[int, ...]
) -> Tuple[Optional[str], str]:
    """``(yahoo_game_id, provenance)`` for one ESPN event id; ``(None, ...)`` means hand over.

    Cascade, strongest first: the id map's stored id, the formula over the row's kickoff date
    (with the row's own Yahoo team number when it carries one), then the published crosswalk.
    An id is **never** invented -- a game with no resolvable id falls through to the next
    source rather than fetching a wrong one.
    """
    row = idmap_row or {}
    stored = row.get("yahoo_game_id")
    if stored:
        return str(stored), "idmap"
    home_team = row.get("home_team") or {}
    computed = _yahoo_cfb_game_id(
        row.get("kickoff_utc"),
        row.get("home_espn_team_id"),
        (home_team or {}).get("yahoo_team_id"),
    )
    if computed:
        return computed, "computed" if not (home_team or {}).get("yahoo_team_id") else "computed_idmap_team"
    from_crosswalk = _crosswalk_yahoo_id(espn_id, seasons)
    if from_crosswalk:
        return str(from_crosswalk), "cfb_schedule_crosswalk"
    return None, "unresolved"


def _game_block(payload: Any) -> Optional[Dict[str, Any]]:
    """``data.games[0]`` of a shangrila playbook payload, or None when the envelope is absent.

    None means "this is not a game payload" -- a 429's 23-byte ``text/html`` body, an error
    envelope, a truncated response. It is deliberately **not** the same as a game with no
    plays (:func:`_has_plays`), which is Yahoo saying it does not cover the game: the first
    is worth another try, the second never is.
    """
    if not isinstance(payload, dict):
        return None
    games = (payload.get("data") or {}).get("games") if isinstance(payload.get("data"), dict) else None
    if not isinstance(games, list) or not games or not isinstance(games[0], dict):
        return None
    return games[0]


def _has_plays(game: Mapping[str, Any]) -> bool:
    """True when the payload carries play-by-play.

    Yahoo answers **HTTP 200** for a game it does not cover -- every FCS-hosted game, and
    every game before its play floor -- with a real game object that has scores, odds and a
    win-probability stub but an empty ``playByPlay``. Detecting that by shape is the only way
    to tell "no coverage" from "fetch failed".
    """
    return bool(game.get("playByPlay"))


def _fetch_playbook_boxscore(yahoo_game_id: str, *, live: bool = False, **kwargs: Any) -> Dict[str, Any]:
    """The shangrila playbook boxscore for one Yahoo game id (``...Poll`` variant when ``live``)."""
    from sportsdataverse.yahoo.yahoo_shangrila import yahoo_playbook_boxscore, yahoo_playbook_boxscore_poll

    fetch = yahoo_playbook_boxscore_poll if live else yahoo_playbook_boxscore
    return fetch(game_id=yahoo_game_id, is_football="true", return_parsed=False, **kwargs)
