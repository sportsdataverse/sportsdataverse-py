"""ESPN WNBA season-level producers -- rosters, season stats, standings.

Polars ports of the script-local ``parse_one_*`` parsers in the
``wehoop-wnba-data`` creation scripts. The WNBA scripts 04-07 are
byte-identical to their WBB siblings after league-name normalization (only
release tags and lambda styling differ), so every producer here is a thin
delegation to the shared implementation in
``sportsdataverse.wbb.wbb_season_producers``:

* ``helper_wnba_rosters`` -- one team's ``wnba/team_rosters/json/{season}/{team_id}.json``.
* ``helper_wnba_player_season_stats`` -- one athlete's
  ``wnba/player_season_stats/json/{season}/{athlete_id}.json`` (parallel-array
  categories pivot long; identity backfilled from the team-rosters lookup).
* ``helper_wnba_team_season_stats`` -- one team's
  ``wnba/team_stats/json/{season}/{team_id}.json`` (two payload shapes).
* ``helper_wnba_standings`` -- the single ``wnba/standings/json/{season}.json``
  (recursive group walk).

The R-released ``espn_wnba_*`` parquets are the parity oracles.
"""

from __future__ import annotations

from typing import Any

import polars as pl

from sportsdataverse.wbb.wbb_season_producers import (
    build_athlete_identity_lookup,
    helper_wbb_player_season_stats,
    helper_wbb_rosters,
    helper_wbb_standings,
    helper_wbb_team_season_stats,
)

__all__ = [
    # League-neutral identity-lookup builder, re-exported so WNBA season
    # builders can import it from sportsdataverse.wnba.
    "build_athlete_identity_lookup",
    "helper_wnba_player_season_stats",
    "helper_wnba_rosters",
    "helper_wnba_standings",
    "helper_wnba_team_season_stats",
]


def helper_wnba_rosters(payload: dict, *, season: int, team_id: int | str) -> pl.DataFrame:
    """Parse one team's season roster JSON into the released rosters frame.

    Faithful polars port of ``parse_one_team`` in
    ``wehoop-wnba-data/R/espn_wnba_04_rosters_creation.R`` -- byte-identical
    to the WBB parser after league normalization, so this delegates to the
    shared implementation. The R-released ``espn_wnba_rosters`` parquet is
    the parity oracle.

    Args:
        payload: One team's ``wnba/team_rosters/json/{season}/{team_id}.json``.
        season: Season year the roster file belongs to.
        team_id: ESPN team id the roster file belongs to.

    Returns:
        pl.DataFrame: One row per athlete; empty (zero-column) frame for
        degenerate payloads -- season builders skip empty frames.

    Example:
        Quick start::

            import json
            from sportsdataverse.wnba import helper_wnba_rosters
            payload = json.load(open("14.json", encoding="utf-8"))
            df = helper_wnba_rosters(payload, season=2026, team_id=14)
            print(df.shape)

    See Also:
        * `wehoop`_ -- the R producer this ports; retained as the parity oracle.

    .. _wehoop: https://wehoop.sportsdataverse.org
    """
    return helper_wbb_rosters(payload, season=season, team_id=team_id)


def helper_wnba_player_season_stats(
    payload: dict,
    *,
    season: int,
    athlete_id: int | str,
    identity_lookup: dict[str, dict[str, Any]] | None = None,
) -> pl.DataFrame:
    """Parse one athlete's season-stats JSON into the released long frame.

    Faithful polars port of the parsers in
    ``wehoop-wnba-data/R/espn_wnba_05_player_season_stats_creation.R`` --
    byte-identical to the WBB parser after league normalization, so this
    delegates to the shared implementation. The R-released
    ``espn_wnba_player_season_stats`` parquet is the parity oracle.

    Args:
        payload: One athlete's ``wnba/player_season_stats/json/{season}/{athlete_id}.json``.
        season: Season year the stats file belongs to.
        athlete_id: ESPN athlete id the stats file belongs to.
        identity_lookup: Optional ``{athlete_id: identity-fields}`` mapping
            built from the season's team rosters; payload blocks are the
            fallback when absent.

    Returns:
        pl.DataFrame: One row per (category, stat); empty (zero-column) frame
        for degenerate payloads -- season builders skip empty frames.

    Example:
        Quick start::

            import json
            from sportsdataverse.wnba import helper_wnba_player_season_stats
            payload = json.load(open("2529140.json", encoding="utf-8"))
            df = helper_wnba_player_season_stats(payload, season=2026, athlete_id=2529140)
            print(df.shape)

    See Also:
        * `wehoop`_ -- the R producer this ports; retained as the parity oracle.

    .. _wehoop: https://wehoop.sportsdataverse.org
    """
    return helper_wbb_player_season_stats(
        payload, season=season, athlete_id=athlete_id, identity_lookup=identity_lookup
    )


def helper_wnba_team_season_stats(payload: dict, *, season: int, team_id: int | str) -> pl.DataFrame:
    """Parse one team's season-stats JSON into the released long frame.

    Faithful polars port of ``parse_one_team`` in
    ``wehoop-wnba-data/R/espn_wnba_06_team_season_stats_creation.R`` --
    byte-identical to the WBB parser after league normalization (both payload
    shapes handled), so this delegates to the shared implementation. The
    R-released ``espn_wnba_team_season_stats`` parquet is the parity oracle.

    Args:
        payload: One team's ``wnba/team_stats/json/{season}/{team_id}.json``.
        season: Season year the stats file belongs to.
        team_id: ESPN team id the stats file belongs to.

    Returns:
        pl.DataFrame: One row per (category, stat); empty (zero-column) frame
        for degenerate payloads -- season builders skip empty frames.

    Example:
        Quick start::

            import json
            from sportsdataverse.wnba import helper_wnba_team_season_stats
            payload = json.load(open("14.json", encoding="utf-8"))
            df = helper_wnba_team_season_stats(payload, season=2026, team_id=14)
            print(df.shape)

    See Also:
        * `wehoop`_ -- the R producer this ports; retained as the parity oracle.

    .. _wehoop: https://wehoop.sportsdataverse.org
    """
    return helper_wbb_team_season_stats(payload, season=season, team_id=team_id)


def helper_wnba_standings(payload: dict, *, season: int) -> pl.DataFrame:
    """Parse one season's standings JSON into the released standings frame.

    Faithful polars port of the recursive group walk in
    ``wehoop-wnba-data/R/espn_wnba_07_standings_creation.R`` -- byte-identical
    to the WBB parser after league normalization, so this delegates to the
    shared implementation. The R-released ``espn_wnba_standings`` parquet is
    the parity oracle.

    Args:
        payload: The season's ``wnba/standings/json/{season}.json`` as a dict.
        season: Season year the standings file belongs to.

    Returns:
        pl.DataFrame: One row per (team, stat); empty (zero-column) frame for
        degenerate payloads -- season builders skip empty frames.

    Example:
        Quick start::

            import json
            from sportsdataverse.wnba import helper_wnba_standings
            payload = json.load(open("2026.json", encoding="utf-8"))
            df = helper_wnba_standings(payload, season=2026)
            print(df.shape)

    See Also:
        * `wehoop`_ -- the R producer this ports; retained as the parity oracle.

    .. _wehoop: https://wehoop.sportsdataverse.org
    """
    return helper_wbb_standings(payload, season=season)
