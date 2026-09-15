"""Polars parsers for the modern ``api.nfl.com`` ``/football/v2`` + ``/experience`` surface.

Each ``parse_nfl_*`` here flattens one :mod:`sportsdataverse.nfl.nfl_api` wrapper's
raw payload into a tidy frame. The records of interest live under a different key
per endpoint (``weeks[].standings``, ``rosters``, ``teams``, ``picks``, ``data``,
or a bare list / single object), so each parser does its own record extraction and
funnels through :func:`_to_frame`, which follows the shared parser contract used
across every ``*_parsers.py`` module: ``pandas.json_normalize(..., sep="_")`` for
one-pass flattening, list-valued cells stringified so polars can ingest the frame,
and columns snake-cased via :func:`sportsdataverse.dl_utils.underscore`.

Every parser returns a ``polars.DataFrame`` by default; pass
``return_as_pandas=True`` for a ``pandas.DataFrame`` instead.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Dict, List, Union

if TYPE_CHECKING:  # pragma: no cover -- annotation-only imports (PEP 563 defers eval)
    import pandas as pd
    import polars as pl

# Return type shared by every parser: polars by default, pandas when toggled.
DataFrameT = Union["pl.DataFrame", "pd.DataFrame"]

__all__ = [
    "parse_nfl_standings",
    "parse_nfl_rosters",
    "parse_nfl_teams_history",
    "parse_nfl_team",
    "parse_nfl_weeks",
    "parse_nfl_weeks_by_date",
    "parse_nfl_combine_profiles",
    "parse_nfl_draft_picks",
    "parse_nfl_injuries",
    "parse_nfl_game_summaries",
    "parse_nfl_weekly_game_details",
    "parse_nfl_live_team_statistics",
    "parse_nfl_live_player_statistics",
    "parse_nfl_game_details_v2",
    "parse_nfl_game_details_by_slug",
]


def _to_frame(records: List, return_as_pandas: bool) -> DataFrameT:
    """Flatten a list of nested dicts into a polars (or pandas) DataFrame.

    Follows the shared ``*_parsers.py`` contract: ``pandas.json_normalize`` for
    one-pass nested flattening, list-valued cells stringified so polars accepts
    the frame, and column names snake-cased via
    :func:`sportsdataverse.dl_utils.underscore`.

    Args:
        records: A list of (possibly nested) JSON record dicts. ``None`` / empty /
            malformed yields a zero-row frame rather than raising.
        return_as_pandas: When ``True``, return a ``pandas.DataFrame``; otherwise a
            ``polars.DataFrame``.

    Returns:
        A ``polars.DataFrame`` (default) or ``pandas.DataFrame`` with nested keys
        flattened (``sep="_"``) and snake-cased columns.
    """
    import pandas as pd
    import polars as pl

    from sportsdataverse.dl_utils import underscore

    if not records:
        return pd.DataFrame() if return_as_pandas else pl.DataFrame()
    try:
        df = pd.json_normalize(records, sep="_")
    except Exception:  # noqa: BLE001 -- malformed payload -> zero-row frame, never raise
        return pd.DataFrame() if return_as_pandas else pl.DataFrame()
    # Stringify list-valued cells so polars can ingest the frame.
    for col in df.columns:
        if df[col].apply(lambda v: isinstance(v, list)).any():
            df[col] = df[col].apply(lambda v: str(v) if isinstance(v, list) else v)
    # Snake-case columns (underscore + flatten any residual dotted keys).
    df.columns = [underscore(c).replace(".", "_") for c in df.columns]
    if return_as_pandas:
        return df
    try:
        return pl.from_pandas(df)
    except Exception:  # noqa: BLE001 -- fall back to all-string object cols
        df2 = df.copy()
        for col in [c for c in df2.columns if df2[c].dtype == "object"]:
            df2[col] = df2[col].astype(str)
        return pl.from_pandas(df2)


def parse_nfl_standings(raw: Dict, return_as_pandas: bool = False) -> DataFrameT:
    """Flatten ``/football/v2/standings`` into one row per team standing.

    Args:
        raw: Raw JSON dict from :func:`sportsdataverse.nfl.nfl_standings` (the
            records live under ``weeks[].standings[]``).
        return_as_pandas: Return a ``pandas.DataFrame`` instead of polars.

    Returns:
        A ``polars`` (or ``pandas``) ``DataFrame``, one row per team standing
        across the returned week(s).

    Example:
        Quick start::

            from sportsdataverse.nfl import nfl_standings, parse_nfl_standings
            raw = nfl_standings(season=2024, season_type="REG", week=18, return_parsed=False)
            parse_nfl_standings(raw).head()
    """
    records: List = []
    for wk in raw.get("weeks", []) or []:
        records.extend(wk.get("standings", []) or [])
    return _to_frame(records, return_as_pandas)


def parse_nfl_rosters(raw: Dict, return_as_pandas: bool = False) -> DataFrameT:
    """Flatten ``/football/v2/rosters`` into one row per team roster.

    Args:
        raw: Raw JSON dict from :func:`sportsdataverse.nfl.nfl_rosters` (records
            under the ``rosters`` key).
        return_as_pandas: Return a ``pandas.DataFrame`` instead of polars.

    Returns:
        A ``polars`` (or ``pandas``) ``DataFrame``, one row per team roster.

    Example:
        Quick start::

            from sportsdataverse.nfl import nfl_rosters, parse_nfl_rosters
            raw = nfl_rosters(season=2024, return_parsed=False)
            parse_nfl_rosters(raw).head()
    """
    return _to_frame(raw.get("rosters", []), return_as_pandas)


def parse_nfl_teams_history(raw: Dict, return_as_pandas: bool = False) -> DataFrameT:
    """Flatten ``/football/v2/teams/history`` into one row per team.

    Args:
        raw: Raw JSON dict from :func:`sportsdataverse.nfl.nfl_teams_history`
            (records under the ``teams`` key).
        return_as_pandas: Return a ``pandas.DataFrame`` instead of polars.

    Returns:
        A ``polars`` (or ``pandas``) ``DataFrame``, one row per team for a season.

    Example:
        Quick start::

            from sportsdataverse.nfl import nfl_teams_history, parse_nfl_teams_history
            raw = nfl_teams_history(season=2024, return_parsed=False)
            parse_nfl_teams_history(raw).head()
    """
    return _to_frame(raw.get("teams", []), return_as_pandas)


def parse_nfl_team(raw: Union[Dict, List], return_as_pandas: bool = False) -> DataFrameT:
    """Flatten ``/football/v2/teams/{team_id}`` into a one-row team-detail frame.

    Args:
        raw: Raw JSON from :func:`sportsdataverse.nfl.nfl_team` -- a single team
            object (wrapped into a one-element list) or an already-list payload.
        return_as_pandas: Return a ``pandas.DataFrame`` instead of polars.

    Returns:
        A ``polars`` (or ``pandas``) ``DataFrame`` with a single team-detail row.

    Example:
        Quick start::

            from sportsdataverse.nfl import nfl_team, parse_nfl_team
            raw = nfl_team(team_id="10403800-517c-7b8c-65a3-c61b95d86123", return_parsed=False)
            parse_nfl_team(raw)
    """
    return _to_frame([raw] if isinstance(raw, dict) else raw, return_as_pandas)


def parse_nfl_weeks(raw: Dict, return_as_pandas: bool = False) -> DataFrameT:
    """Flatten ``/football/v2/weeks/season/...`` into one row per week.

    Args:
        raw: Raw JSON dict from :func:`sportsdataverse.nfl.nfl_weeks` (records
            under the ``weeks`` key).
        return_as_pandas: Return a ``pandas.DataFrame`` instead of polars.

    Returns:
        A ``polars`` (or ``pandas``) ``DataFrame``, one row per week in the
        season's week calendar.

    Example:
        Quick start::

            from sportsdataverse.nfl import nfl_weeks, parse_nfl_weeks
            raw = nfl_weeks(season=2024, season_type="REG", return_parsed=False)
            parse_nfl_weeks(raw).head()
    """
    return _to_frame(raw.get("weeks", []), return_as_pandas)


def parse_nfl_weeks_by_date(raw: Union[Dict, List], return_as_pandas: bool = False) -> DataFrameT:
    """Flatten ``/football/v2/weeks/date/{date}`` into a one-row week frame.

    Args:
        raw: Raw JSON from :func:`sportsdataverse.nfl.nfl_weeks_by_date` -- a
            single week object (wrapped into a one-element list) or a list payload.
        return_as_pandas: Return a ``pandas.DataFrame`` instead of polars.

    Returns:
        A ``polars`` (or ``pandas``) ``DataFrame`` with the single week that
        contains the requested date.

    Example:
        Quick start::

            from sportsdataverse.nfl import nfl_weeks_by_date, parse_nfl_weeks_by_date
            raw = nfl_weeks_by_date(date="2024-09-08", return_parsed=False)
            parse_nfl_weeks_by_date(raw)
    """
    return _to_frame([raw] if isinstance(raw, dict) else raw, return_as_pandas)


def parse_nfl_combine_profiles(raw: Dict, return_as_pandas: bool = False) -> DataFrameT:
    """Flatten ``/football/v2/combine/profiles`` into one row per prospect.

    Args:
        raw: Raw JSON dict from :func:`sportsdataverse.nfl.nfl_combine_profiles`
            (records under the ``combineProfiles`` key).
        return_as_pandas: Return a ``pandas.DataFrame`` instead of polars.

    Returns:
        A ``polars`` (or ``pandas``) ``DataFrame``, one row per combine prospect.

    Example:
        Quick start::

            from sportsdataverse.nfl import nfl_combine_profiles, parse_nfl_combine_profiles
            raw = nfl_combine_profiles(year=2024, return_parsed=False)
            parse_nfl_combine_profiles(raw).head()
    """
    return _to_frame(raw.get("combineProfiles", []), return_as_pandas)


def parse_nfl_draft_picks(raw: Dict, return_as_pandas: bool = False) -> DataFrameT:
    """Flatten ``/football/v2/draft/picks/report`` into one row per pick.

    Args:
        raw: Raw JSON dict from :func:`sportsdataverse.nfl.nfl_draft_picks`
            (records under the ``picks`` key).
        return_as_pandas: Return a ``pandas.DataFrame`` instead of polars.

    Returns:
        A ``polars`` (or ``pandas``) ``DataFrame``, one row per draft pick.

    Example:
        Quick start::

            from sportsdataverse.nfl import nfl_draft_picks, parse_nfl_draft_picks
            raw = nfl_draft_picks(year=2024, return_parsed=False)
            parse_nfl_draft_picks(raw).head()
    """
    return _to_frame(raw.get("picks", []), return_as_pandas)


def parse_nfl_injuries(raw: Dict, return_as_pandas: bool = False) -> DataFrameT:
    """Flatten ``/football/v2/injuries`` into one row per injured player.

    Args:
        raw: Raw JSON dict from :func:`sportsdataverse.nfl.nfl_injuries` (records
            under the ``injuries`` key).
        return_as_pandas: Return a ``pandas.DataFrame`` instead of polars.

    Returns:
        A ``polars`` (or ``pandas``) ``DataFrame``, one row per injured player.

    Example:
        Quick start::

            from sportsdataverse.nfl import nfl_injuries, parse_nfl_injuries
            raw = nfl_injuries(season=2024, season_type="REG", week=1, return_parsed=False)
            parse_nfl_injuries(raw).head()
    """
    return _to_frame(raw.get("injuries", []), return_as_pandas)


def parse_nfl_game_summaries(raw: Dict, return_as_pandas: bool = False) -> DataFrameT:
    """Flatten ``/football/v2/stats/live/game-summaries`` into one row per game.

    Args:
        raw: Raw JSON dict from :func:`sportsdataverse.nfl.nfl_game_summaries`
            (records under the ``data`` key).
        return_as_pandas: Return a ``pandas.DataFrame`` instead of polars.

    Returns:
        A ``polars`` (or ``pandas``) ``DataFrame``, one row per game (live state).

    Example:
        Quick start::

            from sportsdataverse.nfl import nfl_game_summaries, parse_nfl_game_summaries
            raw = nfl_game_summaries(season=2024, season_type="REG", week=1, return_parsed=False)
            parse_nfl_game_summaries(raw).head()
    """
    return _to_frame(raw.get("data", []), return_as_pandas)


def parse_nfl_weekly_game_details(raw: Union[Dict, List], return_as_pandas: bool = False) -> DataFrameT:
    """Flatten ``/football/v2/experience/weekly-game-details`` into one row per game.

    Args:
        raw: Raw JSON from :func:`sportsdataverse.nfl.nfl_weekly_game_details` --
            typically a bare list, with a ``games`` / ``data`` dict fallback.
        return_as_pandas: Return a ``pandas.DataFrame`` instead of polars.

    Returns:
        A ``polars`` (or ``pandas``) ``DataFrame``, one row per game.

    Example:
        Quick start::

            from sportsdataverse.nfl import nfl_weekly_game_details, parse_nfl_weekly_game_details
            raw = nfl_weekly_game_details(season=2024, season_type="REG", week=1, return_parsed=False)
            parse_nfl_weekly_game_details(raw).head()
    """
    records = raw if isinstance(raw, list) else raw.get("games", []) or raw.get("data", [])
    return _to_frame(records, return_as_pandas)


def parse_nfl_live_team_statistics(raw: Dict, return_as_pandas: bool = False) -> DataFrameT:
    """Flatten ``/football/v2/stats/live/team-statistics/{game_id}`` into one row per side.

    The payload holds one flat stat object per side under ``awayTeam`` /
    ``homeTeam``; each becomes a row tagged with ``side`` and carrying the
    payload's ``game_id`` and ``offset`` (the live-feed position the stats
    reflect -- it advances as the game is played).

    Args:
        raw: Raw JSON dict from :func:`sportsdataverse.nfl.nfl_live_team_statistics`.
        return_as_pandas: Return a ``pandas.DataFrame`` instead of polars.

    Returns:
        A ``polars`` (or ``pandas``) ``DataFrame`` with two rows (``side`` =
        ``away`` / ``home``); zero rows for a game with no stats yet.

    Example:
        Quick start::

            from sportsdataverse.nfl import nfl_live_team_statistics, parse_nfl_live_team_statistics
            raw = nfl_live_team_statistics(game_id="a9a890ed-4feb-11f1-abca-2c54536568a9", return_parsed=False)
            parse_nfl_live_team_statistics(raw).select(["side", "team_id"])
    """
    if not isinstance(raw, dict):
        return _to_frame([], return_as_pandas)
    rows = []
    for side in ("away", "home"):
        team = raw.get(f"{side}Team")
        if isinstance(team, dict):
            rows.append({"gameId": raw.get("gameId"), "offset": raw.get("offset"), "side": side, **team})
    return _to_frame(rows, return_as_pandas)


def parse_nfl_live_player_statistics(raw: Dict, return_as_pandas: bool = False) -> DataFrameT:
    """Flatten ``/football/v2/stats/live/player-statistics/{game_id}`` into one row per player.

    Each side is ``{teamId, players[]}``; every player row is tagged with
    ``side`` and ``team_id`` and carries the payload's ``game_id`` / ``offset``.
    Player ids are GSIS (``gsis_player_id``) plus NFL.com ``person_id``.

    Args:
        raw: Raw JSON dict from :func:`sportsdataverse.nfl.nfl_live_player_statistics`.
        return_as_pandas: Return a ``pandas.DataFrame`` instead of polars.

    Returns:
        A ``polars`` (or ``pandas``) ``DataFrame``, one row per player per side.

    Example:
        Quick start::

            from sportsdataverse.nfl import nfl_live_player_statistics, parse_nfl_live_player_statistics
            raw = nfl_live_player_statistics(game_id="a9a890ed-4feb-11f1-abca-2c54536568a9", return_parsed=False)
            parse_nfl_live_player_statistics(raw).select(["side", "gsis_player_name"]).head()
    """
    if not isinstance(raw, dict):
        return _to_frame([], return_as_pandas)
    rows = []
    for side in ("away", "home"):
        team = raw.get(f"{side}Team")
        if not isinstance(team, dict):
            continue
        for player in team.get("players") or []:
            if isinstance(player, dict):
                rows.append(
                    {
                        "gameId": raw.get("gameId"),
                        "offset": raw.get("offset"),
                        "side": side,
                        "teamId": team.get("teamId"),
                        **player,
                    }
                )
    return _to_frame(rows, return_as_pandas)


def parse_nfl_game_details_v2(raw: Dict, return_as_pandas: bool = False) -> DataFrameT:
    """Flatten ``/experience/v2/gamedetails/{game_id}`` into one row.

    The v2 payload is flat (only ``/experience/v1/gamedetails/{game_id}`` wraps
    the game under ``data``; the v1 by-slug route is flat too): game fields
    at the top level plus ``summary`` and, when requested, ``driveChart``,
    ``replays`` and the two standings blocks. Nested objects flatten into
    prefixed columns; list-valued sections (``externalIds``, ``replays``, drive
    lists) are stringified, as in :func:`parse_nfl_weekly_game_details`.

    Args:
        raw: Raw JSON dict from :func:`sportsdataverse.nfl.nfl_game_details_v2`.
        return_as_pandas: Return a ``pandas.DataFrame`` instead of polars.

    Returns:
        A one-row ``polars`` (or ``pandas``) ``DataFrame``.

    Example:
        Quick start::

            from sportsdataverse.nfl import nfl_game_details_v2, parse_nfl_game_details_v2
            raw = nfl_game_details_v2(game_id="a9a890ed-4feb-11f1-abca-2c54536568a9", return_parsed=False)
            parse_nfl_game_details_v2(raw).select(["id", "status"])
    """
    return _to_frame([raw] if isinstance(raw, dict) and raw else [], return_as_pandas)


def parse_nfl_game_details_by_slug(raw: Dict, return_as_pandas: bool = False) -> DataFrameT:
    """Flatten ``/experience/v1/gamedetailsbyslug/{slug}`` into one row.

    Same flat shape as :func:`parse_nfl_game_details_v2`, looked up by the
    nfl.com slug instead of the Shield uuid. Unlike
    ``/experience/v1/gamedetails/{game_id}``, this v1 route does NOT wrap the
    game under ``data`` -- ``id`` / ``status`` sit at the top level
    (``tests/fixtures/nfl_api/game_details_by_slug.json``).

    Args:
        raw: Raw JSON dict from :func:`sportsdataverse.nfl.nfl_game_details_by_slug`.
        return_as_pandas: Return a ``pandas.DataFrame`` instead of polars.

    Returns:
        A one-row ``polars`` (or ``pandas``) ``DataFrame``.

    Example:
        Quick start::

            from sportsdataverse.nfl import nfl_game_details_by_slug, parse_nfl_game_details_by_slug
            raw = nfl_game_details_by_slug(slug="broncos-at-chiefs-2026-reg-1", return_parsed=False)
            parse_nfl_game_details_by_slug(raw).select(["id", "status"])
    """
    return _to_frame([raw] if isinstance(raw, dict) and raw else [], return_as_pandas)
