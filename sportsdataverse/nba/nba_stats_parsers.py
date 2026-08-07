"""Parsers for the stats.nba.com / stats.wnba.com resultSets envelope.

One generic parser handles every endpoint. Most responses use the uniform
``{resultSets: [{name, headers, rowSet}]}`` envelope (a few use singular ``resultSet``);
the parser also normalizes the family's two non-uniform shapes — the shot-location
endpoints' single-dict ``resultSets`` with 2-level grouped headers (flattened to
composite columns like ``less_than_5_ft_fgm``), ``scoreboardv3``'s ``scoreboard.games``
feed (one row per game), and ``scheduleleaguev2``'s ``leagueSchedule.gameDates[].games[]``
feed (also one row per game). Honors the universal parser contract: polars by default, pandas
via flag, empty/malformed returns a zero-row frame, columns snake_cased via dl_utils.underscore.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Optional, Union

import polars as pl

from sportsdataverse.dl_utils import underscore

if TYPE_CHECKING:
    import pandas as pd

__all__ = [
    "parse_nba_stats_result_sets",
    "parse_nba_stats_leaguedashplayerstats",
    "parse_nba_stats_playercareerstats",
    "parse_nba_stats_commonallplayers",
]


def _result_sets(raw: dict) -> list:
    """Extract the list of result-set dicts from a stats.nba.com response envelope.

    Handles both the plural ``resultSets`` and the singular ``resultSet`` variants.

    Args:
        raw: The raw JSON response dictionary from stats.nba.com.

    Returns:
        A list of result-set dicts, each containing ``name``, ``headers``, and ``rowSet``.
    """
    if isinstance(raw.get("resultSets"), list):
        return raw["resultSets"]
    if isinstance(raw.get("resultSets"), dict):
        # shot-location family ships a single result set as a dict (with 2-level headers)
        return [raw["resultSets"]]
    rs = raw.get("resultSet")
    if isinstance(rs, dict):
        return [rs]
    if isinstance(rs, list):
        return rs
    sb = raw.get("scoreboard")
    if isinstance(sb, dict) and isinstance(sb.get("games"), list):
        # scoreboardv3 has no resultSets envelope — synthesize one from scoreboard.games
        return [_scoreboard_result_set(sb)]
    ls = raw.get("leagueSchedule")
    if isinstance(ls, dict) and isinstance(ls.get("gameDates"), list):
        # scheduleleaguev2 has no resultSets envelope either — see below
        return [_league_schedule_result_set(ls)]
    return []


def _flatten_headers(headers: list) -> list:
    """Flatten stats.nba.com 2-level shot-location headers into composite column names.

    The shot-location endpoints (``leaguedash{player,team}shotlocations``) return
    ``headers`` as ``[group_header, flat_header]`` rather than a flat list of strings:
    ``group_header`` carries the distance-range group names plus ``columnsToSkip``
    (leading identity columns) and ``columnSpan`` (columns per group). Identity columns
    keep their name; each grouped column becomes ``"<group> <stat>"``. A plain flat
    header list is returned unchanged.

    Args:
        headers: Either a flat ``list[str]`` or the 2-element nested-header form.

    Returns:
        A flat ``list[str]`` of column names.
    """
    if not headers or not isinstance(headers[0], dict):
        return list(headers)
    group, flat_hdr = headers[0], headers[1]
    flat = list(flat_hdr.get("columnNames") or [])
    skip = group.get("columnsToSkip", 0)
    span = group.get("columnSpan", 1)
    out = list(flat[:skip])
    idx = skip
    for grp in group.get("columnNames") or []:
        # join with underscores so the later ``underscore()`` pass (which keeps spaces)
        # yields the same snake name the catalog schema uses (e.g. less_than_5_ft_fgm)
        prefix = str(grp).replace(".", "").strip().replace(" ", "_")
        for _ in range(span):
            if idx < len(flat):
                out.append(f"{prefix}_{flat[idx]}")
                idx += 1
    out.extend(flat[idx:])
    return out


def _flatten_game(record: dict, prefix: str = "") -> dict:
    """Flatten a nested scoreboardv3 game object (home/away team dicts inlined with a
    prefix; list-valued keys like gameLeaders/broadcasters dropped). Keys are lowered so
    the subsequent ``underscore`` pass is idempotent and matches the catalog schema."""
    out: dict = {}
    for key, value in record.items():
        name = f"{prefix}{key}" if prefix else key
        if isinstance(value, dict):
            out.update(_flatten_game(value, f"{name}_"))
        elif isinstance(value, list):
            continue
        else:
            out[name.lower()] = value
    return out


def _scoreboard_result_set(sb: dict) -> dict:
    """Build a ``{name, headers, rowSet}`` result-set (one row per game) from the v3
    ``scoreboard.games`` feed so the generic parser can render it as a tidy frame."""
    base = {k: v for k, v in sb.items() if k != "games" and not isinstance(v, (dict, list))}
    rows = [
        _flatten_game({**base, **{k: v for k, v in g.items() if not isinstance(v, list)}})
        for g in sb.get("games", [])
        if isinstance(g, dict)
    ]
    headers = sorted({k for r in rows for k in r})
    return {"name": "GameHeader", "headers": headers, "rowSet": [[r.get(h) for h in headers] for r in rows]}


_SEASON_TYPE_BY_ID = {
    "1": "Pre-Season",
    "2": "Regular Season",
    "3": "All-Star",
    "4": "Playoffs",
    "5": "Play-In Game",
}


def _nested_name(outer: str, inner: str) -> str:
    """Snake-case a nested key pair, collapsing the ``team_team`` stutter.

    ``homeTeam.teamId`` -> ``home_team_id`` (not ``home_team_team_id``), matching
    ``hoopR::nba_schedule()`` / ``wehoop::wnba_schedule()``, which do the same via
    ``janitor::clean_names()`` + ``gsub("team_team", "team", ...)``.
    """
    return f"{underscore(outer)}_{underscore(inner)}".replace("team_team", "team")


def _league_schedule_result_set(ls: dict) -> dict:
    """Build a ``{name, headers, rowSet}`` result-set from the ``leagueSchedule`` feed.

    ``scheduleleaguev2`` (and the CDN mirror the R packages now read,
    ``cdn.{nba,wnba}.com/static/json/staticData/scheduleLeagueV2.json``) ships
    ``{leagueSchedule: {seasonYear, leagueId, gameDates: [{gameDate, games: [...]}]}}``
    — no ``resultSets`` envelope at all. This unrolls it to one row per game so the
    generic parser can render it, reproducing the R accessors' column contract:
    ``homeTeam``/``awayTeam`` inlined as ``home_team_*``/``away_team_*``, the
    list-valued ``broadcasters``/``pointsLeaders`` dropped, and ``season_type_id`` /
    ``season_type_description`` derived from the 3rd character of ``game_id``.
    """
    season, league_id = ls.get("seasonYear"), ls.get("leagueId")
    rows: list = []
    for game_date in ls.get("gameDates") or []:
        if not isinstance(game_date, dict):
            continue
        day = {underscore(k): v for k, v in game_date.items() if k != "games" and not isinstance(v, (dict, list))}
        for game in game_date.get("games") or []:
            if not isinstance(game, dict):
                continue
            row = dict(day)
            for key, value in game.items():
                if isinstance(value, list):
                    continue  # broadcasters / pointsLeaders — dropped, as the R readers do
                if isinstance(value, dict):
                    row.update({_nested_name(key, k): v for k, v in value.items() if not isinstance(v, (dict, list))})
                else:
                    row[underscore(key)] = value
            gid = row.get("game_id")
            season_type_id = str(gid)[2:3] if gid else None
            row["season_type_id"] = season_type_id
            row["season_type_description"] = _SEASON_TYPE_BY_ID.get(season_type_id or "")
            row["season"] = season
            row["league_id"] = league_id
            rows.append(row)
    headers = sorted({k for r in rows for k in r})
    # "SeasonGames" is the name the canonical catalog / returns-table schema uses.
    return {"name": "SeasonGames", "headers": headers, "rowSet": [[r.get(h) for h in headers] for r in rows]}


def _to_frame(rs: dict) -> pl.DataFrame:
    """Convert a single result-set dict to a polars DataFrame.

    Args:
        rs: A dict with ``headers`` (list of str) and ``rowSet`` (list of lists).

    Returns:
        A polars DataFrame with snake_cased column names. Returns an empty DataFrame
        when ``headers`` is absent or ``rowSet`` is empty.
    """
    headers = [underscore(h) for h in _flatten_headers(rs.get("headers", []))]
    rows = rs.get("rowSet", []) or []
    if not headers:
        return pl.DataFrame()
    # Stringify any list-valued cells so polars accepts a uniform schema
    norm = [[("|".join(map(str, c)) if isinstance(c, list) else c) for c in row] for row in rows]
    # Drop ragged rows (width mismatch → ShapeError otherwise)
    norm = [r for r in norm if len(r) == len(headers)]
    if norm:
        try:
            # infer_schema_length=None scans EVERY row before choosing dtypes.
            # The default (100) infers Null for a column whose first rows are
            # all null, then errors when a real value appears deeper in the
            # rowSet — and the except below would silently turn a 3k-row
            # payload into an empty frame (observed: WNBA 1998 leaguegamelog,
            # PLUS_MINUS null until late in the season).
            return pl.DataFrame(norm, schema=headers, orient="row", infer_schema_length=None)
        except Exception:
            pass
    return pl.DataFrame(schema={h: pl.Utf8 for h in headers})


def parse_nba_stats_result_sets(
    raw: dict,
    result_set: Optional[str] = None,
    *,
    return_as_pandas: bool = False,
) -> Union[pl.DataFrame, "pd.DataFrame", dict]:
    """Parse a stats.nba.com / stats.wnba.com ``resultSets`` response.

    The stats.nba.com / stats.wnba.com API wraps every endpoint response in the
    same envelope: ``{resultSets: [{name, headers, rowSet}]}``. This function
    converts that envelope into tidy polars DataFrames (or pandas if requested).

    Args:
        raw: Raw JSON response dict from stats.nba.com or stats.wnba.com.
            Malformed or empty payloads return a zero-row frame rather than raising.
        result_set: If given, return only the named result-set as a DataFrame.
            If the name is not found, returns a zero-row polars DataFrame.
            If ``None`` and the response has exactly one result-set, returns that
            frame directly. If ``None`` and there are multiple result-sets, returns
            a ``dict`` mapping result-set names to DataFrames.
        return_as_pandas: When ``True``, convert the output to a pandas DataFrame
            (or dict of pandas DataFrames for multi-set responses). Defaults to
            ``False`` (polars output).

    Returns:
        * ``pl.DataFrame`` (or ``pd.DataFrame``) when ``result_set`` is given or
          the payload has exactly one result-set.
        * ``dict[str, pl.DataFrame]`` (or ``dict[str, pd.DataFrame]``) when
          ``result_set`` is ``None`` and the payload has multiple result-sets.
        * Zero-row ``pl.DataFrame`` (or ``pd.DataFrame``) on empty/malformed input.

    Example:
        Quick start — named result-set::

            import json
            from sportsdataverse.nba.nba_stats_parsers import parse_nba_stats_result_sets

            with open("leaguedashplayerstats.json", encoding="utf-8") as f:
                raw = json.load(f)
            df = parse_nba_stats_result_sets(raw, result_set="LeagueDashPlayerStats")
            print(df.shape)

        All result-sets as a dict::

            out = parse_nba_stats_result_sets(raw)  # dict[str, pl.DataFrame]
            print(list(out.keys()))

        Pandas output::

            df_pd = parse_nba_stats_result_sets(raw, result_set="LeagueDashPlayerStats",
                                                 return_as_pandas=True)

        See Also:
            * `nba_api`_ — comprehensive NBA/WNBA stats Python client
            * `hoopR`_ — men's basketball (R)
            * `wehoop`_ — women's basketball / WNBA (R)

        .. _nba_api: https://github.com/swar/nba_api
        .. _hoopR: https://hoopR.sportsdataverse.org
        .. _wehoop: https://wehoop.sportsdataverse.org
    """
    sets = _result_sets(raw)
    frames: dict[str, pl.DataFrame] = {rs.get("name", f"set_{i}"): _to_frame(rs) for i, rs in enumerate(sets)}

    def _maybe_pandas(df: pl.DataFrame) -> Union[pl.DataFrame, "pd.DataFrame"]:
        return df.to_pandas() if return_as_pandas else df

    if result_set is not None:
        return _maybe_pandas(frames.get(result_set, pl.DataFrame()))
    if not frames:
        return _maybe_pandas(pl.DataFrame())
    if len(frames) == 1:
        return _maybe_pandas(next(iter(frames.values())))
    return {name: _maybe_pandas(df) for name, df in frames.items()}


def parse_nba_stats_leaguedashplayerstats(
    raw: dict,
    *,
    return_as_pandas: bool = False,
) -> Union[pl.DataFrame, "pd.DataFrame"]:
    """Parse the ``LeagueDashPlayerStats`` result-set from a stats.nba.com response.

    Convenience wrapper around :func:`parse_nba_stats_result_sets` that selects
    the ``LeagueDashPlayerStats`` result-set, which is present on the
    ``leaguedashplayerstats`` endpoint for NBA, WNBA, G-League, and Summer League.

    Args:
        raw: Raw JSON response dict from stats.nba.com or stats.wnba.com.
        return_as_pandas: When ``True``, return a pandas DataFrame. Defaults to
            ``False`` (polars output).

    Returns:
        A polars DataFrame (or pandas if ``return_as_pandas=True``) with one row per
        player. Zero-row frame when the result-set is absent or payload is malformed.

    Example:
        Quick start::

            from sportsdataverse.nba.nba_stats_parsers import parse_nba_stats_leaguedashplayerstats

            df = parse_nba_stats_leaguedashplayerstats(raw)
            print(df.select(["player_name", "pts"]).head())

        See Also:
            * `nba_api`_ — comprehensive NBA/WNBA stats Python client
            * `hoopR`_ — men's basketball (R)

        .. _nba_api: https://github.com/swar/nba_api
        .. _hoopR: https://hoopR.sportsdataverse.org
    """
    out = parse_nba_stats_result_sets(raw, "LeagueDashPlayerStats", return_as_pandas=return_as_pandas)
    assert not isinstance(out, dict)  # a fixed result_set always yields a single frame
    return out


def parse_nba_stats_playercareerstats(
    raw: dict,
    *,
    return_as_pandas: bool = False,
) -> Union[dict, "pd.DataFrame", pl.DataFrame]:
    """Parse all result-sets from a stats.nba.com ``playercareerstats`` response.

    The ``playercareerstats`` endpoint returns up to 14 named result-sets
    (regular season, post-season, all-star, college, etc.). This wrapper returns
    the full dict so callers can access any result-set by name.

    Args:
        raw: Raw JSON response dict from stats.nba.com.
        return_as_pandas: When ``True``, each dict value is a pandas DataFrame.
            Defaults to ``False`` (polars output).

    Returns:
        A ``dict`` mapping result-set names to DataFrames (polars or pandas).
        Falls back to a single DataFrame when the response has exactly one result-set.

    Example:
        Quick start::

            from sportsdataverse.nba.nba_stats_parsers import parse_nba_stats_playercareerstats

            out = parse_nba_stats_playercareerstats(raw)
            df_reg = out["SeasonTotalsRegularSeason"]
            print(df_reg.shape)

        See Also:
            * `nba_api`_ — comprehensive NBA/WNBA stats Python client
            * `hoopR`_ — men's basketball (R)

        .. _nba_api: https://github.com/swar/nba_api
        .. _hoopR: https://hoopR.sportsdataverse.org
    """
    return parse_nba_stats_result_sets(raw, return_as_pandas=return_as_pandas)


def parse_nba_stats_commonallplayers(
    raw: dict,
    *,
    return_as_pandas: bool = False,
) -> Union[pl.DataFrame, "pd.DataFrame"]:
    """Parse the ``CommonAllPlayers`` result-set from a stats.nba.com response.

    Convenience wrapper around :func:`parse_nba_stats_result_sets` targeting the
    ``CommonAllPlayers`` result-set returned by the ``commonallplayers`` endpoint.

    Args:
        raw: Raw JSON response dict from stats.nba.com.
        return_as_pandas: When ``True``, return a pandas DataFrame. Defaults to
            ``False`` (polars output).

    Returns:
        A polars DataFrame (or pandas if ``return_as_pandas=True``) with one row
        per player. Zero-row frame when the result-set is absent or payload is malformed.

    Example:
        Quick start::

            from sportsdataverse.nba.nba_stats_parsers import parse_nba_stats_commonallplayers

            df = parse_nba_stats_commonallplayers(raw)
            print(df.columns)

        See Also:
            * `nba_api`_ — comprehensive NBA/WNBA stats Python client
            * `hoopR`_ — men's basketball (R)

        .. _nba_api: https://github.com/swar/nba_api
        .. _hoopR: https://hoopR.sportsdataverse.org
    """
    out = parse_nba_stats_result_sets(raw, "CommonAllPlayers", return_as_pandas=return_as_pandas)
    assert not isinstance(out, dict)  # a fixed result_set always yields a single frame
    return out
