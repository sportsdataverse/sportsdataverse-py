"""ESPN women's-college-basketball team-level season roster scraper.

Single ESPN endpoint:
    site.api.espn.com/apis/site/v2/sports/basketball/womens-college-basketball/teams/{team_id}/roster

Returns one row per athlete on the team's CURRENT roster. ESPN's roster
endpoint ignores ``?season=YYYY``; the ``season`` argument is recorded on the
output frame as a column for downstream join purposes but does NOT alter the
request URL.
"""

from __future__ import annotations

from typing import Any, Literal, overload

import pandas as pd
import polars as pl

from sportsdataverse.dl_utils import download, underscore

_LEAGUE_SLUG: str = "womens-college-basketball"

# Canonical column set emitted by the parser (mirrors the wehoop R wrapper at
# R/espn_basketball_team_helpers.R:274-291 plus the richer keys ESPN ships in
# this endpoint vs. game-rosters).
_OUTPUT_COLUMNS: list[str] = [
    "athlete_id",
    "athlete_uid",
    "first_name",
    "last_name",
    "full_name",
    "display_name",
    "short_name",
    "jersey",
    "position_id",
    "position_name",
    "position_abbreviation",
    "height",
    "display_height",
    "weight",
    "display_weight",
    "age",
    "date_of_birth",
    "birth_city",
    "birth_state",
    "headshot_href",
    "link_web",
    "status_name",
]


@overload
def espn_wbb_team_roster(
    team_id: int,
    season: int | None = ...,
    *,
    raw: Literal[True],
    return_as_pandas: bool = ...,
    **kwargs: Any,
) -> dict[str, Any]: ...
@overload
def espn_wbb_team_roster(
    team_id: int,
    season: int | None = ...,
    *,
    raw: Literal[False] = ...,
    return_as_pandas: Literal[True],
    **kwargs: Any,
) -> pd.DataFrame: ...
@overload
def espn_wbb_team_roster(
    team_id: int,
    season: int | None = ...,
    *,
    raw: Literal[False] = ...,
    return_as_pandas: Literal[False] = ...,
    **kwargs: Any,
) -> pl.DataFrame: ...
def espn_wbb_team_roster(
    team_id: int,
    season: int | None = None,
    *,
    raw: bool = False,
    return_as_pandas: bool = False,
    **kwargs: Any,
) -> pl.DataFrame | pd.DataFrame | dict[str, Any]:
    """Pull the current ESPN team roster for a women's-college-basketball team.

    Args:
        team_id: ESPN team identifier (e.g. ``2509`` for UConn).
        season: Season year. Recorded as the ``season`` column on the output;
            does NOT alter the request URL because ESPN's
            ``/teams/{id}/roster`` endpoint ignores ``?season=YYYY``.
        raw: If True, returns the parsed JSON dict before any flattening.
        return_as_pandas: If True, returns a pandas DataFrame; otherwise polars.
        **kwargs: Forwarded to ``sportsdataverse.dl_utils.download``.

    Returns:
        Polars (or pandas) DataFrame with one row per athlete:
        ``athlete_id``, ``athlete_uid``, ``first_name``, ``last_name``,
        ``full_name``, ``display_name``, ``short_name``, ``jersey``,
        ``position_id``, ``position_name``, ``position_abbreviation``,
        ``height``, ``display_height``, ``weight``, ``display_weight``,
        ``age``, ``date_of_birth``, ``birth_city``, ``birth_state``,
        ``headshot_href``, ``link_web``, ``status_name``, ``team_id``,
        ``season``.

        If ``raw=True``, returns the raw response dict.

    Raises:
        sportsdataverse.errors.NoDataError: ESPN returned 404.
        requests.exceptions.RequestException: Other network failures after retries.

    Example:
        Quick start (UConn ``team_id=2509``)::

            from sportsdataverse.wbb import espn_wbb_team_roster
            roster = espn_wbb_team_roster(team_id=2509, season=2025)
            print(roster.shape)
            roster.select(["full_name", "jersey", "position_abbreviation"]).head()

        Pandas round-trip::

            roster_pd = espn_wbb_team_roster(team_id=2509, season=2025, return_as_pandas=True)
            roster_pd.head()

        Pipeline next step - join with team metadata::

            from sportsdataverse.wbb import espn_wbb_teams
            teams = espn_wbb_teams()
            roster.join(
                teams.select(["team_id", "team_display_name"]),
                on="team_id",
                how="left",
            )

        Raw payload (skip the cleaning pipeline)::

            raw = espn_wbb_team_roster(team_id=2509, season=2025, raw=True)
            sorted(raw.keys())

        See Also:
            * `wehoop`_ - R sister package
            * `cfbfastR`_ - companion R package for college football
            * `ESPN`_ - data origin

        .. _wehoop: https://wehoop.sportsdataverse.org
        .. _cfbfastR: https://cfbfastR.sportsdataverse.org
        .. _ESPN: https://www.espn.com
    """
    return _espn_basketball_team_roster(
        league=_LEAGUE_SLUG,
        team_id=team_id,
        season=season,
        raw=raw,
        return_as_pandas=return_as_pandas,
        **kwargs,
    )


def _espn_basketball_team_roster(
    league: str,
    team_id: int,
    season: int | None = None,
    *,
    raw: bool = False,
    return_as_pandas: bool = False,
    **kwargs: Any,
) -> pl.DataFrame | pd.DataFrame | dict[str, Any]:
    """Shared implementation for ``espn_wbb_team_roster`` / ``espn_wnba_team_roster``.

    Builds the ESPN site-v2 roster URL for the supplied ``league`` slug,
    downloads, flattens the ``athletes`` array, normalizes the column names to
    snake_case, attaches ``team_id`` / ``season`` as constant columns, and
    returns polars (or pandas).
    """
    url = f"https://site.api.espn.com/apis/site/v2/sports/basketball/{league}/teams/{team_id}/roster"
    resp = download(url, **kwargs)
    summary: dict[str, Any] = resp.json()

    if raw:
        return summary

    athletes = _extract_athletes(summary)

    if not athletes:
        # Empty roster -> empty frame with the documented column set.
        empty_schema: dict[str, type[pl.DataType] | pl.DataType] = dict.fromkeys(_OUTPUT_COLUMNS, pl.Utf8)
        empty_schema["team_id"] = pl.Int64
        empty_schema["season"] = pl.Int32
        empty = pl.DataFrame(schema=empty_schema)
        return empty.to_pandas() if return_as_pandas else empty

    flat = pd.json_normalize(athletes, sep="_")
    df = pl.from_pandas(flat)
    df = df.rename({c: underscore(c) for c in df.columns})

    # ESPN ships some keys nested under ``links``; pull the first href as the
    # canonical web link. ``pd.json_normalize`` leaves ``links`` as a
    # list-of-dicts column, so unpack defensively.
    df = _attach_link_web(df)

    # Map ESPN's normalized keys onto the documented output names. Any column
    # the API stops emitting is filled with nulls so the downstream schema is
    # stable.
    rename_map: dict[str, str] = {
        "id": "athlete_id",
        "uid": "athlete_uid",
        "position_abbreviation": "position_abbreviation",
        "position_display_name": "position_name",
        "position_id": "position_id",
        "birth_place_city": "birth_city",
        "birth_place_state": "birth_state",
        "headshot_href": "headshot_href",
        "status_type": "status_name",
    }
    present_renames = {old: new for old, new in rename_map.items() if old in df.columns and new not in df.columns}
    if present_renames:
        df = df.rename(present_renames)

    # Ensure every documented column exists (lit-null fallback for absent keys).
    missing_cols = [c for c in _OUTPUT_COLUMNS if c not in df.columns]
    if missing_cols:
        df = df.with_columns([pl.lit(None).cast(pl.Utf8).alias(c) for c in missing_cols])

    # Project to documented columns, then attach team_id + season at the tail.
    df = df.select(_OUTPUT_COLUMNS)
    df = df.with_columns(
        pl.lit(team_id).cast(pl.Int64).alias("team_id"),
        pl.lit(season).cast(pl.Int32).alias("season"),
    )

    return df.to_pandas() if return_as_pandas else df


def _extract_athletes(summary: dict[str, Any]) -> list[dict[str, Any]]:
    """Pull the flat athletes list out of ESPN's roster payload.

    ESPN sometimes returns ``athletes`` as a flat list and sometimes as a list
    of position-group buckets each carrying their own ``items`` array. Handle
    both shapes.
    """
    raw_athletes = summary.get("athletes")
    if not raw_athletes:
        return []

    if (
        isinstance(raw_athletes, list)
        and raw_athletes
        and isinstance(raw_athletes[0], dict)
        and "items" in raw_athletes[0]
    ):
        flattened: list[dict[str, Any]] = []
        for group in raw_athletes:
            items = group.get("items") or []
            if isinstance(items, list):
                flattened.extend(item for item in items if isinstance(item, dict))
        return flattened

    if isinstance(raw_athletes, list):
        return [a for a in raw_athletes if isinstance(a, dict)]

    return []


def _attach_link_web(df: pl.DataFrame) -> pl.DataFrame:
    """Surface the first ``links[].href`` value as a top-level ``link_web`` column."""
    if "link_web" in df.columns:
        return df
    if "links" not in df.columns:
        return df.with_columns(pl.lit(None).cast(pl.Utf8).alias("link_web"))

    def _first_href(value: Any) -> str | None:
        if not isinstance(value, list) or not value:
            return None
        first = value[0]
        if isinstance(first, dict):
            href = first.get("href")
            return str(href) if href is not None else None
        return None

    hrefs = [_first_href(v) for v in df["links"].to_list()]
    return df.with_columns(pl.Series(name="link_web", values=hrefs, dtype=pl.Utf8))
