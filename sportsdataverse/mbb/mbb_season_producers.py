"""ESPN MBB season-level producers -- rosters, season stats, standings.

Three of the four datasets here are byte-identical to their WBB siblings
after league-name normalization (``hoopR-mbb-data/R/espn_mbb_0{4,6,7}_*_
creation.R`` vs ``wehoop-wbb-data``'s scripts -- pure refactor / CONFIG-only
diffs per the discovery pass), so they are thin delegations to
``sportsdataverse.wbb.wbb_season_producers``, exactly like the NBA cutover:

* ``helper_mbb_rosters`` -- one team's ``mbb/team_rosters/json/{season}/{team_id}.json``.
* ``helper_mbb_team_season_stats`` -- one team's
  ``mbb/team_stats/json/{season}/{team_id}.json`` (two payload shapes).
* ``helper_mbb_standings`` -- the single ``mbb/standings/json/{season}.json``
  (recursive group walk).

``helper_mbb_player_season_stats`` is **shared with NBA, NOT a WBB
delegation** -- ``espn_mbb_05_player_season_stats_creation.R`` matches
``espn_nba_05_player_season_stats_creation.R``'s shape exactly (the biggest
real rewrite vs the WBB/WNBA sibling): the raw payload is a *flat, full-career*
``mbb/player_season_stats/json/{athlete_id}.json`` (no ``{season}/`` partition
-- ESPN ignores the season query param and ships every season the athlete
played), athlete identity is backfilled from the *released*
``espn_mens_college_basketball_player_boxscores`` (``hoopR::load_mbb_player_
box()``, NOT the team-rosters lookup -- ESPN's team-roster endpoint is
current-only and cannot answer "who played in season Y"), each category's
``statistics[]`` array is filtered down to the requested season year, the
whole-season "Totals" stint is preferred over a mid-season-trade partial
stint, and the released frame carries ``team_slug`` but no
``athlete_first_name`` / ``athlete_last_name``. This module therefore
re-exports the NBA implementation rather than forking a second copy.

The R-released ``espn_mbb_*`` parquets are the parity oracles.
"""

from __future__ import annotations

from typing import Any

import polars as pl

from sportsdataverse.nba.nba_season_producers import (
    build_nba_player_identity_lookup as build_mbb_player_identity_lookup,
)
from sportsdataverse.nba.nba_season_producers import (
    helper_nba_player_season_stats,
)
from sportsdataverse.wbb.wbb_season_producers import (
    build_athlete_identity_lookup,
    helper_wbb_rosters,
    helper_wbb_standings,
    helper_wbb_team_season_stats,
)

__all__ = [
    # League-neutral identity-lookup builder (team-rosters based), re-exported
    # so MBB season builders can import it from sportsdataverse.mbb -- only
    # player_season_stats uses the player_box-based identity builder below.
    "build_athlete_identity_lookup",
    "build_mbb_player_identity_lookup",
    "helper_mbb_player_season_stats",
    "helper_mbb_rosters",
    "helper_mbb_standings",
    "helper_mbb_team_season_stats",
]


def helper_mbb_rosters(payload: dict, *, season: int, team_id: int | str) -> pl.DataFrame:
    """Parse one team's season roster JSON into the released rosters frame.

    Faithful polars port of ``parse_one_team`` in
    ``hoopR-mbb-data/R/espn_mbb_04_rosters_creation.R`` -- byte-identical to
    the WBB parser after league normalization, so this delegates to the
    shared implementation. The R-released ``espn_mens_college_basketball_
    rosters`` parquet is the parity oracle.

    Args:
        payload: One team's ``mbb/team_rosters/json/{season}/{team_id}.json``.
        season: Season year the roster file belongs to.
        team_id: ESPN team id the roster file belongs to.

    Returns:
        pl.DataFrame: One row per athlete; empty (zero-column) frame for
        degenerate payloads -- season builders skip empty frames.

    Example:
        Quick start::

            import json
            from sportsdataverse.mbb import helper_mbb_rosters
            payload = json.load(open("52.json", encoding="utf-8"))
            df = helper_mbb_rosters(payload, season=2026, team_id=52)
            print(df.shape)

    See Also:
        * `hoopR`_ -- the R producer this ports; retained as the parity oracle.

    .. _hoopR: https://hoopR.sportsdataverse.org
    """
    return helper_wbb_rosters(payload, season=season, team_id=team_id)


def helper_mbb_player_season_stats(
    payload: dict,
    *,
    season: int,
    athlete_id: int | str,
    identity_lookup: dict[str, dict[str, Any]] | None = None,
) -> pl.DataFrame:
    """Parse one athlete's flat career-stats JSON into the released long frame.

    Faithful polars port of ``parse_one_category`` / ``parse_one_athlete`` in
    ``hoopR-mbb-data/R/espn_mbb_05_player_season_stats_creation.R``. This
    delegates to the shared NBA implementation -- see the module docstring
    for the full list of divergences from the WBB/WNBA shape. In short: the
    raw payload is the athlete's *entire career*, so each category's
    ``statistics[]`` entries are filtered down to the ones tagged with the
    requested ``season`` year; when a player was traded mid-season ESPN ships
    one stint per team plus a whole-season "Totals" row (``teamSlug``
    containing ``"Totals"``, case-insensitive) -- the Totals row is
    preferred, falling back to the last stint.

    Args:
        payload: One athlete's ``mbb/player_season_stats/json/{athlete_id}.json``
            (flat, full-career -- NOT season-partitioned).
        season: Season year to filter each category's ``statistics[]`` to.
        athlete_id: The ESPN athlete id this stats file belongs to.
        identity_lookup: Output of :func:`build_mbb_player_identity_lookup`
            (built from the season's *player_box*, not team rosters). The
            MBB player_season_stats payload carries no reliable identity of
            its own, so a miss here means blank identity columns -- matching
            R's ``NA`` fallback.

    Returns:
        pl.DataFrame: One row per (category, stat) for the requested season;
        empty (zero-column) frame when no category has a matching-season
        entry -- season builders skip empty frames.

    Example:
        Quick start::

            import json
            from sportsdataverse.mbb import helper_mbb_player_season_stats
            payload = json.load(open("4433137.json", encoding="utf-8"))
            df = helper_mbb_player_season_stats(payload, season=2025, athlete_id=4433137)
            print(df.shape)

    See Also:
        * `hoopR`_ -- the R producer this ports; retained as the parity oracle.

    .. _hoopR: https://hoopR.sportsdataverse.org
    """
    return helper_nba_player_season_stats(
        payload, season=season, athlete_id=athlete_id, identity_lookup=identity_lookup
    )


def helper_mbb_team_season_stats(payload: dict, *, season: int, team_id: int | str) -> pl.DataFrame:
    """Parse one team's season-stats JSON into the released long frame.

    Faithful polars port of ``parse_one_team`` in
    ``hoopR-mbb-data/R/espn_mbb_06_team_season_stats_creation.R`` --
    byte-identical to the WBB parser after league normalization (both payload
    shapes handled), so this delegates to the shared implementation. The
    R-released ``espn_mens_college_basketball_team_season_stats`` parquet is
    the parity oracle.

    Args:
        payload: One team's ``mbb/team_stats/json/{season}/{team_id}.json``.
        season: Season year the stats file belongs to.
        team_id: ESPN team id the stats file belongs to.

    Returns:
        pl.DataFrame: One row per (category, stat); empty (zero-column) frame
        for degenerate payloads -- season builders skip empty frames.

    Example:
        Quick start::

            import json
            from sportsdataverse.mbb import helper_mbb_team_season_stats
            payload = json.load(open("52.json", encoding="utf-8"))
            df = helper_mbb_team_season_stats(payload, season=2026, team_id=52)
            print(df.shape)

    See Also:
        * `hoopR`_ -- the R producer this ports; retained as the parity oracle.

    .. _hoopR: https://hoopR.sportsdataverse.org
    """
    return helper_wbb_team_season_stats(payload, season=season, team_id=team_id)


def helper_mbb_standings(payload: dict, *, season: int) -> pl.DataFrame:
    """Parse one season's standings JSON into the released standings frame.

    Faithful polars port of the recursive group walk in
    ``hoopR-mbb-data/R/espn_mbb_07_standings_creation.R`` -- byte-identical
    to the WBB parser after league normalization, so this delegates to the
    shared implementation. The R-released ``espn_mens_college_basketball_
    standings`` parquet is the parity oracle.

    Args:
        payload: The season's ``mbb/standings/json/{season}.json`` as a dict.
        season: Season year the standings file belongs to.

    Returns:
        pl.DataFrame: One row per (team, stat); empty (zero-column) frame for
        degenerate payloads -- season builders skip empty frames.

    Example:
        Quick start::

            import json
            from sportsdataverse.mbb import helper_mbb_standings
            payload = json.load(open("2026.json", encoding="utf-8"))
            df = helper_mbb_standings(payload, season=2026)
            print(df.shape)

    See Also:
        * `hoopR`_ -- the R producer this ports; retained as the parity oracle.

    .. _hoopR: https://hoopR.sportsdataverse.org
    """
    return helper_wbb_standings(payload, season=season)
