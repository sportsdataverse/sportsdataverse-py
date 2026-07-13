"""ESPN NBA season-level producers -- rosters, season stats, standings.

Three of the four datasets here are byte-identical to their WBB siblings
after league-name normalization (``hoopR-nba-data/R/espn_nba_0{4,6,7}_*_creation.R``
vs ``wehoop-wbb-data``'s scripts -- 0 changed lines per the discovery diff), so
they are thin delegations to ``sportsdataverse.wbb.wbb_season_producers``,
exactly like the WNBA cutover:

* ``helper_nba_rosters`` -- one team's ``nba/team_rosters/json/{season}/{team_id}.json``.
* ``helper_nba_team_season_stats`` -- one team's
  ``nba/team_stats/json/{season}/{team_id}.json`` (two payload shapes).
* ``helper_nba_standings`` -- the single ``nba/standings/json/{season}.json``
  (recursive group walk).

``helper_nba_player_season_stats`` is a **novel port**, NOT a delegation --
``espn_nba_05_player_season_stats_creation.R`` is the biggest real rewrite
vs the WBB/WNBA sibling (235 normalized-diff lines): the raw payload is a
*flat, full-career* ``nba/player_season_stats/json/{athlete_id}.json`` (no
``{season}/`` partition -- ESPN ignores the season query param and ships
every season the athlete played), athlete identity is backfilled from the
*released* ``espn_nba_player_boxscores`` (``hoopR::load_nba_player_box()``,
NOT the team-rosters lookup -- ESPN's team-roster endpoint is current-only
and cannot answer "who played in season Y"), each category's
``statistics[]`` array is filtered down to the requested season year, the
whole-season "Totals" stint is preferred over a mid-season-trade partial
stint, and the released frame carries ``team_slug`` but no
``athlete_first_name`` / ``athlete_last_name``.

The R-released ``espn_nba_*`` parquets are the parity oracles.
"""

from __future__ import annotations

from typing import Any

import polars as pl

from sportsdataverse.wbb.wbb_game_rosters import _rel_chr, _rel_int
from sportsdataverse.wbb.wbb_season_producers import (
    _coalesce_chr,
    _frame,
    _pad,
    _to_num,
    build_athlete_identity_lookup,
    helper_wbb_rosters,
    helper_wbb_standings,
    helper_wbb_team_season_stats,
)

__all__ = [
    # League-neutral identity-lookup builder, re-exported so NBA season
    # builders can import it from sportsdataverse.nba (rosters helper still
    # goes through this -- only player_season_stats uses the player_box
    # identity builder below).
    "build_athlete_identity_lookup",
    "build_nba_player_identity_lookup",
    "helper_nba_player_season_stats",
    "helper_nba_rosters",
    "helper_nba_standings",
    "helper_nba_team_season_stats",
]


def helper_nba_rosters(payload: dict, *, season: int, team_id: int | str) -> pl.DataFrame:
    """Parse one team's season roster JSON into the released rosters frame.

    Faithful polars port of ``parse_one_team`` in
    ``hoopR-nba-data/R/espn_nba_04_rosters_creation.R`` -- byte-identical to
    the WBB parser after league normalization, so this delegates to the
    shared implementation. The R-released ``espn_nba_rosters`` parquet is
    the parity oracle.

    Args:
        payload: One team's ``nba/team_rosters/json/{season}/{team_id}.json``.
        season: Season year the roster file belongs to.
        team_id: ESPN team id the roster file belongs to.

    Returns:
        pl.DataFrame: One row per athlete; empty (zero-column) frame for
        degenerate payloads -- season builders skip empty frames.

    Example:
        Quick start::

            import json
            from sportsdataverse.nba import helper_nba_rosters
            payload = json.load(open("13.json", encoding="utf-8"))
            df = helper_nba_rosters(payload, season=2026, team_id=13)
            print(df.shape)

    See Also:
        * `hoopR`_ -- the R producer this ports; retained as the parity oracle.

    .. _hoopR: https://hoopR.sportsdataverse.org
    """
    return helper_wbb_rosters(payload, season=season, team_id=team_id)


def build_nba_player_identity_lookup(player_box: pl.DataFrame) -> dict[str, dict[str, Any]]:
    """R ``build_identity_lookup(season)``: athlete_id -> identity from the
    season's already-compiled ``player_box`` -- the authoritative "who played
    in season Y" source (ESPN's team-roster endpoint is current-only and
    cannot answer that for historical seasons).

    Args:
        player_box: The season's compiled player_box frame (e.g.
            ``nba/player_box/parquet/player_box_{season}.parquet``, or
            whatever the season builder just wrote for this pass). Must carry
            ``athlete_id``; other identity columns are best-effort.

    Returns:
        dict: athlete_id (str) -> identity fields for
        :func:`helper_nba_player_season_stats`. When an athlete appears in
        multiple rows (multiple games), the LAST row (by frame order) wins --
        mirroring R's ``!duplicated(athlete_id, fromLast = TRUE)``, which
        keeps an athlete's most recent team within the season.
    """
    lookup: dict[str, dict[str, Any]] = {}
    if player_box.is_empty() or "athlete_id" not in player_box.columns:
        return lookup
    cols = player_box.columns
    n = player_box.height

    def _column(name: str) -> list[Any]:
        return player_box.get_column(name).to_list() if name in cols else [None] * n

    ids = _column("athlete_id")
    disp = _column("athlete_display_name")
    pos = _column("athlete_position_abbreviation")
    jersey = _column("athlete_jersey")
    team_id = _column("team_id")
    team_display_name = _column("team_display_name")

    for i in range(n):
        aid = _rel_int(ids[i])
        if aid is None:
            continue
        # Overwrite-on-repeat: iterating in frame order and always keeping
        # the latest write reproduces R's "last appearance wins" dedup
        # without a separate reverse pass.
        lookup[str(aid)] = {
            "display_name": _rel_chr(disp[i]),
            "position_abbreviation": _rel_chr(pos[i]),
            "jersey": _rel_chr(jersey[i]),
            "team_id": _rel_int(team_id[i]),
            "team_display_name": _rel_chr(team_display_name[i]),
        }
    return lookup


_PLAYER_STATS_COLS: tuple[str, ...] = (
    "season",
    "athlete_id",
    "athlete_display_name",
    "athlete_position_abbreviation",
    "athlete_jersey",
    "team_id",
    "team_slug",
    "team_display_name",
    "category",
    "stat_label",
    "stat_name",
    "stat_display_name",
    "stat_description",
    "display_value",
    "value",
)


def helper_nba_player_season_stats(
    payload: dict,
    *,
    season: int,
    athlete_id: int | str,
    identity_lookup: dict[str, dict[str, Any]] | None = None,
) -> pl.DataFrame:
    """Parse one athlete's flat career-stats JSON into the released long frame.

    Faithful polars port of ``parse_one_category`` / ``parse_one_athlete`` in
    ``hoopR-nba-data/R/espn_nba_05_player_season_stats_creation.R``. This is
    a NOVEL algorithm, not a WBB delegation -- see the module docstring for
    the full list of divergences. In short: the raw payload is the athlete's
    *entire career*, so each category's ``statistics[]`` entries are filtered
    down to the ones tagged with the requested ``season`` year; when a player
    was traded mid-season ESPN ships one stint per team plus a whole-season
    "Totals" row (``teamSlug`` containing ``"Totals"``, case-insensitive) --
    the Totals row is preferred, falling back to the last stint.

    Args:
        payload: One athlete's ``nba/player_season_stats/json/{athlete_id}.json``
            (flat, full-career -- NOT season-partitioned).
        season: Season year to filter each category's ``statistics[]`` to.
        athlete_id: The ESPN athlete id this stats file belongs to.
        identity_lookup: Output of :func:`build_nba_player_identity_lookup`
            (built from the season's *player_box*, not team rosters). The
            NBA player_season_stats payload carries no reliable identity of
            its own, so a miss here means blank identity columns -- matching
            R's ``NA`` fallback.

    Returns:
        pl.DataFrame: One row per (category, stat) for the requested season;
        empty (zero-column) frame when no category has a matching-season
        entry -- season builders skip empty frames.

    Example:
        Quick start::

            import json
            from sportsdataverse.nba import helper_nba_player_season_stats
            payload = json.load(open("3945274.json", encoding="utf-8"))
            df = helper_nba_player_season_stats(payload, season=2025, athlete_id=3945274)
            print(df.shape)

    See Also:
        * `hoopR`_ -- the R producer this ports; retained as the parity oracle.

    .. _hoopR: https://hoopR.sportsdataverse.org
    """
    # R uses %||% (NULL-coalesce), which only falls through on absent keys, not on
    # an empty-but-present list. Gate on ``is None`` (not truthiness) so a payload
    # shipping ``categories: []`` alongside a populated ``statCategories`` resolves
    # to the empty ``categories`` exactly as R does.
    categories = payload.get("categories")
    if categories is None:
        categories = payload.get("statCategories")
    if categories is None:
        categories = (payload.get("splits") or {}).get("categories")
    if categories is None:
        categories = []
    if not categories:
        return pl.DataFrame()

    ident = (identity_lookup or {}).get(str(_rel_int(athlete_id))) or {}
    season_int = int(season)

    rows: list[dict[str, Any]] = []
    for cat in categories:
        stats_entries = cat.get("statistics") or []
        if not stats_entries:
            continue
        matches = [s for s in stats_entries if _rel_int((s.get("season") or {}).get("year")) == season_int]
        if not matches:
            continue
        # Prefer the whole-season Totals stint; else the last (most recent) stint.
        chosen = None
        for s in matches:
            slug = _rel_chr(s.get("teamSlug"))
            if slug and "totals" in slug.lower():
                chosen = s
                break
        if chosen is None:
            chosen = matches[-1]

        vals = [_rel_chr(v) for v in (chosen.get("stats") or [])]
        n = len(vals)
        if n == 0:
            continue

        cat_name = _coalesce_chr(cat.get("name"), cat.get("displayName"))
        labels = _pad(cat.get("labels"), n)
        names = _pad(cat.get("names"), n)
        display_names = _pad(cat.get("displayNames"), n)
        descriptions = _pad(cat.get("descriptions"), n)

        # team for this season: the chosen stint's team, falling back to the
        # player_box identity (the Totals row carries no teamId).
        stint_team_id = _rel_int(chosen.get("teamId"))
        team_id = stint_team_id if stint_team_id is not None else ident.get("team_id")
        team_slug = _rel_chr(chosen.get("teamSlug"))

        for i in range(n):
            rows.append(
                {
                    "season": season_int,
                    "athlete_id": _rel_int(athlete_id),
                    "athlete_display_name": ident.get("display_name"),
                    "athlete_position_abbreviation": ident.get("position_abbreviation"),
                    "athlete_jersey": ident.get("jersey"),
                    "team_id": team_id,
                    "team_slug": team_slug,
                    "team_display_name": ident.get("team_display_name"),
                    "category": cat_name,
                    "stat_label": labels[i],
                    "stat_name": names[i],
                    "stat_display_name": display_names[i],
                    "stat_description": descriptions[i],
                    "display_value": vals[i],
                    "value": _to_num(vals[i]),
                }
            )
    return _frame(
        rows,
        _PLAYER_STATS_COLS,
        int32=("season", "athlete_id", "team_id"),
        float64=("value",),
    )


def helper_nba_team_season_stats(payload: dict, *, season: int, team_id: int | str) -> pl.DataFrame:
    """Parse one team's season-stats JSON into the released long frame.

    Faithful polars port of ``parse_one_team`` in
    ``hoopR-nba-data/R/espn_nba_06_team_season_stats_creation.R`` --
    byte-identical to the WBB parser after league normalization (both payload
    shapes handled), so this delegates to the shared implementation. The
    R-released ``espn_nba_team_season_stats`` parquet is the parity oracle.

    Args:
        payload: One team's ``nba/team_stats/json/{season}/{team_id}.json``.
        season: Season year the stats file belongs to.
        team_id: ESPN team id the stats file belongs to.

    Returns:
        pl.DataFrame: One row per (category, stat); empty (zero-column) frame
        for degenerate payloads -- season builders skip empty frames.

    Example:
        Quick start::

            import json
            from sportsdataverse.nba import helper_nba_team_season_stats
            payload = json.load(open("13.json", encoding="utf-8"))
            df = helper_nba_team_season_stats(payload, season=2026, team_id=13)
            print(df.shape)

    See Also:
        * `hoopR`_ -- the R producer this ports; retained as the parity oracle.

    .. _hoopR: https://hoopR.sportsdataverse.org
    """
    return helper_wbb_team_season_stats(payload, season=season, team_id=team_id)


def helper_nba_standings(payload: dict, *, season: int) -> pl.DataFrame:
    """Parse one season's standings JSON into the released standings frame.

    Faithful polars port of the recursive group walk in
    ``hoopR-nba-data/R/espn_nba_07_standings_creation.R`` -- byte-identical
    to the WBB parser after league normalization, so this delegates to the
    shared implementation. The R-released ``espn_nba_standings`` parquet is
    the parity oracle.

    Args:
        payload: The season's ``nba/standings/json/{season}.json`` as a dict.
        season: Season year the standings file belongs to.

    Returns:
        pl.DataFrame: One row per (team, stat); empty (zero-column) frame for
        degenerate payloads -- season builders skip empty frames.

    Example:
        Quick start::

            import json
            from sportsdataverse.nba import helper_nba_standings
            payload = json.load(open("2026.json", encoding="utf-8"))
            df = helper_nba_standings(payload, season=2026)
            print(df.shape)

    See Also:
        * `hoopR`_ -- the R producer this ports; retained as the parity oracle.

    .. _hoopR: https://hoopR.sportsdataverse.org
    """
    return helper_wbb_standings(payload, season=season)
