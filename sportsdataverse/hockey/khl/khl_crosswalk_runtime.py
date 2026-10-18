"""Deterministic KHL club id <-> team-season id crosswalk (no fuzzy matching).

khl.ru keys a club twice: the STABLE club id (``teama`` / ``teamb`` / ``clubid``; 7 = Spartak) and a
per-tournament team-season id (``homeId`` / ``visitorId`` / ``cid`` / ``clubidt`` / ``teid`` / ``tid``;
Spartak in tournament 1369 = 14934). The schedule and standings bodies carry only the club id; the
game pages (header, text, protocol) and the player career table carry both, so the mapping is read
from those and nothing is guessed: a team-season id that maps to two clubs raises ``ValueError``.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Set, Union

import pandas as pd
import polars as pl

from sportsdataverse.errors import NoDataError

__all__ = ["khl_club_crosswalk", "khl_club_crosswalk_from_frames"]

Frame = Union[pl.DataFrame, pd.DataFrame]
_SCHEMA = {"tournament_id": pl.Int64, "club_id": pl.Int64, "team_season_id": pl.Int64}
_PAIRS = (
    ("home_club_id", "home_team_season_id"),
    ("away_club_id", "away_team_season_id"),
    ("club_id", "team_season_id"),
)


def khl_club_crosswalk_from_frames(*frames: Any, return_as_pandas: bool = False) -> Frame:
    """Club id <-> team-season id from frames that carry both (game header / info, player career).

    Args:
        *frames: polars (or pandas) frames from ``khl_game_header``, ``khl_game_info`` or
            ``khl_player_career``; any ``home_`` / ``away_`` pair or ``club_id`` / ``team_season_id``
            pair with a ``tournament_id`` is read, anything else is ignored.
        return_as_pandas: return a pandas DataFrame instead of polars.

    Returns:
        polars.DataFrame | pandas.DataFrame: one row per (``tournament_id``, ``club_id``,
        ``team_season_id``), all ``Int64``, sorted; zero rows when no frame carries a pair.

    Raises:
        ValueError: a team-season id maps to two clubs, or a club has two team-season ids in one
            tournament (the sources disagree; nothing is picked).

    Example:
        Quick start::

            from sportsdataverse.hockey.khl import khl_club_crosswalk_from_frames, khl_game_header
            khl_club_crosswalk_from_frames(khl_game_header(898236, season=2026))

    See Also:
        * :func:`sportsdataverse.hockey.khl.khl_game_header` - a frame that carries both ids.
        * :func:`sportsdataverse.hockey.khl.khl_player_career` - also carries both ids.
        * :func:`sportsdataverse.hockey.khl.khl_club_crosswalk` - fetches the frames for a whole tournament.
        * `fastRhockey`_ - SportsDataverse hockey package (R).

    .. _fastRhockey: https://fastRhockey.sportsdataverse.org
    """
    rows: Set[tuple] = set()
    for frame in frames:
        df = pl.from_pandas(frame) if isinstance(frame, pd.DataFrame) else frame
        if not isinstance(df, pl.DataFrame) or "tournament_id" not in df.columns:
            continue
        for club, season_id in _PAIRS:
            if club in df.columns and season_id in df.columns:
                sub = df.select("tournament_id", club, season_id).drop_nulls()
                rows.update((int(t), int(c), int(s)) for t, c, s in sub.iter_rows())
    by_season: Dict[tuple, int] = {}
    by_club: Dict[tuple, int] = {}
    for t, c, s in rows:
        if by_season.setdefault((t, s), c) != c or by_club.setdefault((t, c), s) != s:
            raise ValueError(f"khl crosswalk: tournament {t} club {c} / team-season {s} conflicts with another source")
    out = pl.DataFrame(sorted(rows), schema=list(_SCHEMA), orient="row").cast(_SCHEMA)
    return out.to_pandas() if return_as_pandas else out


def _cover(schedule: pl.DataFrame) -> List[int]:
    """Game ids that together touch every club (played games first, then the rest)."""
    seen: Set[int] = set()
    picks: List[int] = []
    for df in (schedule.filter(pl.col("home_score").is_not_null()), schedule.filter(pl.col("home_score").is_null())):
        for gid, home, away in df.select("game_id", "home_club_id", "away_club_id").iter_rows():
            if ({home, away} - {None}) - seen:
                seen.update((home, away))
                picks.append(gid)
    return picks


def khl_club_crosswalk(
    season: Optional[int] = None,
    stage: Optional[str] = "regular",
    *,
    tournament_id: Optional[int] = None,
    return_as_pandas: bool = False,
    **kwargs: Any,
) -> Frame:
    """Club id <-> team-season id for one tournament, read from a covering set of game headers.

    Fetches the schedule, picks the fewest games (greedy) that touch every club, fetches those game
    headers (about 15 requests, each waiting its polite second) and maps the ids they carry.

    Args:
        season: season END year (2026 = 2025-26); None = the site's current season.
        stage: ``regular`` or ``playoff`` (team-season ids differ per tournament).
        tournament_id: khl.ru tournament id (tnId), instead of season / stage.
        return_as_pandas: return a pandas DataFrame instead of polars.
        **kwargs: forwarded to the underlying wrappers.

    Returns:
        polars.DataFrame | pandas.DataFrame: ``tournament_id``, ``club_id`` (stable),
        ``team_season_id`` (this tournament's), one row per club.

    Raises:
        sportsdataverse.errors.AssetFetchError: khl.ru answered an error, or a wrong page.
        sportsdataverse.errors.NoDataError: no schedule rows, or a club the headers give no team-season id.
        ValueError: two sources disagree on an id pair.

    Example:
        Quick start::

            from sportsdataverse.hockey.khl import khl_club_crosswalk
            xw = khl_club_crosswalk(season=2026)
            xw.filter(xw["club_id"] == 7)

    See Also:
        * :func:`sportsdataverse.hockey.khl.khl_schedule` - picks the covering games.
        * :func:`sportsdataverse.hockey.khl.khl_game_header` - where the id pairs are read.
        * :func:`sportsdataverse.hockey.khl.khl_club_crosswalk_from_frames` - the same map from frames you hold.
        * `fastRhockey`_ - SportsDataverse hockey package (R).

    .. _fastRhockey: https://fastRhockey.sportsdataverse.org
    """
    from sportsdataverse.hockey.khl.khl_rest import khl_game_header, khl_schedule

    schedule = khl_schedule(season, stage, tournament_id=tournament_id, **kwargs)
    if schedule.height == 0:
        raise NoDataError(f"khl crosswalk: no schedule rows (season={season}, stage={stage}, tnId={tournament_id})")
    tid = tournament_id if tournament_id is not None else schedule["tournament_id"].mode().sort().item(0)
    schedule = schedule.filter(pl.col("tournament_id") == tid)
    headers = [khl_game_header(g, tournament_id=tid, **kwargs) for g in _cover(schedule)]
    out = khl_club_crosswalk_from_frames(*headers)
    clubs = set(schedule["home_club_id"].drop_nulls()) | set(schedule["away_club_id"].drop_nulls())
    missing = sorted(clubs - set(out["club_id"]))
    if missing:
        raise NoDataError(
            f"khl crosswalk: tournament {tid}: no team-season id for club(s) {missing} in the game headers"
        )
    return out.to_pandas() if return_as_pandas else out
