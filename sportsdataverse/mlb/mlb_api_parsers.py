"""sportsdataverse.mlb.mlb_api_parsers — polars parsers for the official
MLB Stats API at ``statsapi.mlb.com/api/v1/``.

**Documentation**:

* MLB Stats API parser deep-dive: https://py.sportsdataverse.org/docs/mlb/parsers
* MLB module overview: https://py.sportsdataverse.org/docs/mlb/
* Parsers overview: https://py.sportsdataverse.org/docs/parsers/

The wrappers in :mod:`sportsdataverse.mlb.mlb_api` all return ``Dict``;
this module turns those payloads into tidy polars (or pandas)
DataFrames. Mirrors the design of
:mod:`sportsdataverse._common_espn_parsers`:

* Every parser returns ``polars.DataFrame`` by default; pass
  ``return_as_pandas=True`` for pandas.
* Empty / malformed payloads return a zero-row frame instead of
  raising.
* Output columns are snake-cased via
  :func:`sportsdataverse.dl_utils.underscore`.
* Most parsers use :func:`pandas.json_normalize` for one-pass
  flattening of nested dicts.

Parser → endpoint mapping
-------------------------

* :func:`parse_mlb_api_schedule`     → :func:`mlb_api_schedule`
                                       (unrolls ``dates[].games[]``)
* :func:`parse_mlb_api_teams`        → :func:`mlb_api_teams`
                                       (one row per team from ``teams[]``)
* :func:`parse_mlb_api_team_roster`  → :func:`mlb_api_team_roster`
                                       (one row per player from ``roster[]``)
* :func:`parse_mlb_api_standings`    → :func:`mlb_api_standings`
                                       (unrolls ``records[].teamRecords[]``)
* :func:`parse_mlb_api_person_stats` → :func:`mlb_api_person_stats` and
                                       :func:`mlb_api_team_stats`
                                       (unrolls ``stats[].splits[]``)
* :func:`parse_mlb_api_list`         → generic fallback for any
                                       ``{copyright, <key>: [...]}`` payload
                                       (venues, sports, leagues, divisions,
                                       seasons, awards, umpires, etc.)
"""

from __future__ import annotations

from typing import Dict

import pandas as pd
import polars as pl

from sportsdataverse.dl_utils import underscore

# ---------------------------------------------------------------------------
# Internal helpers (mirror _common_espn_parsers conventions)
# ---------------------------------------------------------------------------


def _snake_columns(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = [underscore(c).replace(".", "_") for c in df.columns]
    return df


def _to_output(df: pd.DataFrame, return_as_pandas: bool):
    if return_as_pandas:
        return df
    try:
        return pl.from_pandas(df)
    except Exception:
        df2 = df.copy()
        for col in [c for c in df2.columns if df2[c].dtype == "object"]:
            df2[col] = df2[col].astype(str)
        return pl.from_pandas(df2)


def _empty_frame(return_as_pandas: bool = False):
    df = pd.DataFrame()
    return df if return_as_pandas else pl.DataFrame()


def _flatten_rows(items, return_as_pandas: bool):
    """Generic: ``pd.json_normalize`` a list of dicts → tidy frame.

    Stringifies any list-valued cells so polars can ingest the frame.
    Returns a zero-row frame when ``items`` is missing or empty.
    """
    if not isinstance(items, list) or not items:
        return _empty_frame(return_as_pandas)
    try:
        df = pd.json_normalize(items, sep="_")
    except Exception:
        return _empty_frame(return_as_pandas)
    for col in df.columns:
        if df[col].apply(lambda v: isinstance(v, list)).any():
            df[col] = df[col].apply(lambda v: str(v) if isinstance(v, list) else v)
    df = _snake_columns(df)
    return _to_output(df, return_as_pandas)


# ---------------------------------------------------------------------------
# Generic list flattener (covers venues / sports / leagues / divisions /
# seasons / awards / umpires / draft_prospects / etc.)
# ---------------------------------------------------------------------------


# Common top-level array keys in Stats API responses. Tried in order.
_LIST_KEYS = (
    "teams",
    "venues",
    "sports",
    "leagues",
    "divisions",
    "seasons",
    "awards",
    "awardRecipients",
    "umpires",
    "people",
    "players",
    "items",
    "records",
)


def parse_mlb_api_list(payload: Dict, return_as_pandas: bool = False) -> pl.DataFrame:
    """Generic parser for any MLB Stats API response that wraps a list
    of records under a known top-level key.

    Walks the payload looking for the first key in
    ``{teams, venues, sports, leagues, divisions, seasons, awards,
    awardRecipients, umpires, people, players, items, records}`` that
    resolves to a non-empty list of dicts, then flattens it.

    Use a dedicated parser (:func:`parse_mlb_api_schedule`,
    :func:`parse_mlb_api_standings`, :func:`parse_mlb_api_person_stats`)
    for endpoints that need extra unrolling logic.

    Args:
        payload: Raw JSON dict from any ``mlb_api_*`` wrapper.
        return_as_pandas: Return ``pandas.DataFrame`` instead of polars.

    Returns:
        ``pl.DataFrame`` (or pandas) with one row per item. Zero rows
        when no recognized list key resolves.
    """
    if not isinstance(payload, dict):
        return _empty_frame(return_as_pandas)
    for key in _LIST_KEYS:
        candidate = payload.get(key)
        if isinstance(candidate, list) and candidate and isinstance(candidate[0], dict):
            return _flatten_rows(candidate, return_as_pandas)
    return _empty_frame(return_as_pandas)


# ---------------------------------------------------------------------------
# Schedule
# ---------------------------------------------------------------------------


def parse_mlb_api_schedule(payload: Dict, return_as_pandas: bool = False) -> pl.DataFrame:
    """Parse a Stats API schedule response into a tidy frame of games.

    Stats API ships the schedule as
    ``{dates: [{date, games: [...]}, ...], totalGames, ...}``. This
    parser walks every ``dates[].games[]``, prefixes the schedule date
    onto each game row, and produces one row per game with the rich
    nested ``teams.home.*`` / ``teams.away.*`` / ``venue.*`` / ``status.*``
    fields flattened.

    Args:
        payload: Raw JSON dict from :func:`mlb_api_schedule` or
            :func:`mlb_api_schedule_postseason`.
        return_as_pandas: Return ``pandas.DataFrame`` instead of polars.

    Returns:
        ``pl.DataFrame`` (or pandas) with one row per game.
    """
    if not isinstance(payload, dict):
        return _empty_frame(return_as_pandas)
    dates = payload.get("dates") or []
    if not isinstance(dates, list) or not dates:
        return _empty_frame(return_as_pandas)
    rows = []
    for date_entry in dates:
        date_str = (date_entry or {}).get("date")
        for game in date_entry.get("games") or []:
            row = {"schedule_date": date_str}
            for k, v in (game or {}).items():
                row[k] = v
            rows.append(row)
    return _flatten_rows(rows, return_as_pandas)


# ---------------------------------------------------------------------------
# Teams + team roster
# ---------------------------------------------------------------------------


def parse_mlb_api_teams(payload: Dict, return_as_pandas: bool = False) -> pl.DataFrame:
    """Parse ``mlb_api_teams()`` into one row per team."""
    return _flatten_rows((payload or {}).get("teams"), return_as_pandas)


def parse_mlb_api_team_roster(payload: Dict, return_as_pandas: bool = False) -> pl.DataFrame:
    """Parse ``mlb_api_team_roster()`` into one row per player.

    Input: ``{roster: [{person, jerseyNumber, position, status}, ...],
    teamId, rosterType, ...}``. The ``person`` / ``position`` / ``status``
    sub-dicts are flattened (``person_id``, ``position_abbreviation``,
    ``status_code``).
    """
    return _flatten_rows((payload or {}).get("roster"), return_as_pandas)


# ---------------------------------------------------------------------------
# Standings
# ---------------------------------------------------------------------------


def parse_mlb_api_standings(payload: Dict, return_as_pandas: bool = False) -> pl.DataFrame:
    """Parse ``mlb_api_standings()`` into one row per (division × team).

    Input: ``{records: [{standingsType, league, division, sport,
    teamRecords: [...]}, ...]}``. This parser walks each division's
    ``teamRecords[]`` and prefixes the division identifiers onto each
    team row, so a single output row carries both the division context
    (``division_id``, ``league_id``) and the team's full standing stats
    (wins, losses, pct, gamesBack, streak, division/league/sport rank,
    home/away/extraInning records, etc.).
    """
    if not isinstance(payload, dict):
        return _empty_frame(return_as_pandas)
    records = payload.get("records") or []
    if not isinstance(records, list) or not records:
        return _empty_frame(return_as_pandas)
    rows = []
    for div_entry in records:
        if not isinstance(div_entry, dict):
            continue
        # Namespace the division-level fields with a ``standings_`` prefix
        # so they don't collide with same-named team-record fields when
        # ``pd.json_normalize`` flattens the inner dict (e.g. teamRecords
        # has its own ``lastUpdated``).
        div_base = {
            "standings_type": div_entry.get("standingsType"),
            "standings_league_id": (div_entry.get("league") or {}).get("id"),
            "standings_league_name": (div_entry.get("league") or {}).get("name"),
            "standings_division_id": (div_entry.get("division") or {}).get("id"),
            "standings_division_name": (div_entry.get("division") or {}).get("name"),
            "standings_last_updated": div_entry.get("lastUpdated"),
        }
        for team_row in div_entry.get("teamRecords") or []:
            row = dict(div_base)
            for k, v in (team_row or {}).items():
                row[k] = v
            rows.append(row)
    return _flatten_rows(rows, return_as_pandas)


# ---------------------------------------------------------------------------
# Person stats (splits) — also matches mlb_api_team_stats
# ---------------------------------------------------------------------------


def parse_mlb_api_person_stats(payload: Dict, return_as_pandas: bool = False) -> pl.DataFrame:
    """Parse ``mlb_api_person_stats()`` / ``mlb_api_team_stats()`` into
    one row per stats split.

    Input: ``{stats: [{type, group, exemptions, splits: [{season, stat,
    team, player, league, sport, gameType}, ...]}, ...]}``. Each
    ``stats[]`` entry corresponds to a (statsType × statsGroup) bucket
    (e.g. ``season`` × ``hitting``); each ``splits[]`` row is one
    sliced view (e.g. one season, one team, one game-type). This parser
    flattens each ``splits[]`` row and prefixes ``stats_type`` /
    ``stats_group`` from the parent so a single output row carries both
    context columns and the wide ``stat_*`` block (34+ columns of
    batting/pitching/fielding metrics depending on the group).

    Args:
        payload: Raw JSON dict from :func:`mlb_api_person_stats` or
            :func:`mlb_api_team_stats`.
        return_as_pandas: Return ``pandas.DataFrame`` instead of polars.

    Returns:
        ``pl.DataFrame`` (or pandas) with one row per split.
    """
    if not isinstance(payload, dict):
        return _empty_frame(return_as_pandas)
    stats = payload.get("stats") or []
    if not isinstance(stats, list) or not stats:
        return _empty_frame(return_as_pandas)
    rows = []
    for block in stats:
        if not isinstance(block, dict):
            continue
        block_base = {
            "stats_type": (block.get("type") or {}).get("displayName"),
            "stats_group": (block.get("group") or {}).get("displayName"),
        }
        for split in block.get("splits") or []:
            row = dict(block_base)
            for k, v in (split or {}).items():
                row[k] = v
            rows.append(row)
    return _flatten_rows(rows, return_as_pandas)


# ---------------------------------------------------------------------------
# Endpoint → parser registry
# ---------------------------------------------------------------------------


# Maps an mlb_api_* wrapper name to its parser. Endpoints not in the
# registry can be parsed via :func:`parse_mlb_api_list` (generic
# list flattener) or via :func:`pd.json_normalize` directly.
MLB_API_ENDPOINT_PARSERS = {
    # Dedicated parsers (extra unrolling logic):
    "mlb_api_schedule": parse_mlb_api_schedule,
    "mlb_api_schedule_postseason": parse_mlb_api_schedule,
    "mlb_api_teams": parse_mlb_api_teams,
    "mlb_api_team_roster": parse_mlb_api_team_roster,
    "mlb_api_standings": parse_mlb_api_standings,
    "mlb_api_person_stats": parse_mlb_api_person_stats,
    "mlb_api_team_stats": parse_mlb_api_person_stats,
    # Generic list-shape endpoints:
    "mlb_api_people": parse_mlb_api_list,
    "mlb_api_sport_players": parse_mlb_api_list,
    "mlb_api_sports": parse_mlb_api_list,
    "mlb_api_leagues": parse_mlb_api_list,
    "mlb_api_divisions": parse_mlb_api_list,
    "mlb_api_seasons": parse_mlb_api_list,
    "mlb_api_venues": parse_mlb_api_list,
    "mlb_api_awards": parse_mlb_api_list,
    "mlb_api_award_recipients": parse_mlb_api_list,
    "mlb_api_umpires": parse_mlb_api_list,
    "mlb_api_team_leaders": parse_mlb_api_list,
    "mlb_api_team_alumni": parse_mlb_api_list,
    "mlb_api_team_affiliates": parse_mlb_api_list,
    "mlb_api_stats": parse_mlb_api_list,
    "mlb_api_stats_leaders": parse_mlb_api_list,
    "mlb_api_stats_streaks": parse_mlb_api_list,
    "mlb_api_draft": parse_mlb_api_list,
    "mlb_api_draft_prospects": parse_mlb_api_list,
    "mlb_api_attendance": parse_mlb_api_list,
}


def parser_for_mlb_api(fn_name: str):
    """Return the registered parser for an ``mlb_api_*`` wrapper name.

    Falls back to :func:`parse_mlb_api_list` (the generic list flattener)
    when no specific parser is registered, so the caller always gets a
    DataFrame-returning function rather than ``None``.

    Args:
        fn_name: The ``__name__`` of any ``mlb_api_*`` wrapper.

    Returns:
        Parser callable. Never ``None``.
    """
    return MLB_API_ENDPOINT_PARSERS.get(fn_name, parse_mlb_api_list)
