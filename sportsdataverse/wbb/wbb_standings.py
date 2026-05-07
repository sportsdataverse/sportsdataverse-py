"""ESPN women's-college-basketball standings scraper.

Single ESPN endpoint:
    site.api.espn.com/apis/v2/sports/basketball/womens-college-basketball/standings?season={year}&group={group}

ESPN ships standings as a tree: the top-level payload has ``children[]``
(one entry per conference under the requested group; ``group=50`` is NCAA
Division I women), each carrying a ``standings.entries[]`` array. Each
entry pairs a ``team`` block with a ``stats[]`` array of stat objects
(``avgPointsAgainst``, ``wins``, ``losses``, ``streak``, etc.). The
wrapper flattens that tree to a single polars DataFrame, one row per
team, with the stat values surfaced as named columns.
"""

from __future__ import annotations

from typing import Any, Literal, overload

import pandas as pd
import polars as pl

from sportsdataverse.dl_utils import download

_LEAGUE_SLUG: str = "womens-college-basketball"

# Documented column set, in emit order. ``_OUTPUT_COLUMNS`` doubles as the
# empty-frame schema spine: every key here lands in the output frame even
# if ESPN omits the corresponding stat for a particular team / season.
_OUTPUT_COLUMNS: list[str] = [
    "team_id",
    "team_uid",
    "team_slug",
    "team_location",
    "team_name",
    "team_abbreviation",
    "team_display_name",
    "team_short_display_name",
    "team_color",
    "conference_id",
    "conference_name",
    "conference_abbreviation",
    "wins",
    "losses",
    "win_percent",
    "games_back",
    "streak",
    "points_for",
    "points_against",
    "point_differential",
    "avg_points_for",
    "avg_points_against",
    "home_wins",
    "home_losses",
    "road_wins",
    "road_losses",
    "division_wins",
    "division_losses",
    "season",
]

# ESPN ``stats[].name`` -> output column. Stats not in this map are
# ignored (the documented column set is fixed; new ESPN stats land in the
# raw payload via ``raw=True``).
_STAT_VALUE_MAP: dict[str, str] = {
    "wins": "wins",
    "losses": "losses",
    "winPercent": "win_percent",
    "gamesBehind": "games_back",
    "pointsFor": "points_for",
    "pointsAgainst": "points_against",
    "pointDifferential": "point_differential",
    "avgPointsFor": "avg_points_for",
    "avgPointsAgainst": "avg_points_against",
}

# Stats whose useful payload is the ``summary`` / ``displayValue`` string
# rather than the numeric ``value`` (typically W-L splits).
_STAT_SUMMARY_MAP: dict[str, str] = {
    "Home": "home_summary",
    "home": "home_summary",
    "Road": "road_summary",
    "road": "road_summary",
    "vs. Div.": "division_summary",
    "vsdiv": "division_summary",
    "vs. Conf.": "conference_summary",
    "vsconf": "conference_summary",
    "streak": "streak",
}


@overload
def espn_wbb_standings(
    season: int,
    group: int = ...,
    *,
    raw: Literal[True],
    return_as_pandas: bool = ...,
    **kwargs: Any,
) -> dict[str, Any]: ...
@overload
def espn_wbb_standings(
    season: int,
    group: int = ...,
    *,
    raw: Literal[False] = ...,
    return_as_pandas: Literal[True],
    **kwargs: Any,
) -> pd.DataFrame: ...
@overload
def espn_wbb_standings(
    season: int,
    group: int = ...,
    *,
    raw: Literal[False] = ...,
    return_as_pandas: Literal[False] = ...,
    **kwargs: Any,
) -> pl.DataFrame: ...
def espn_wbb_standings(
    season: int,
    group: int = 50,
    *,
    raw: bool = False,
    return_as_pandas: bool = False,
    **kwargs: Any,
) -> pl.DataFrame | pd.DataFrame | dict[str, Any]:
    """Pull ESPN women's-college-basketball standings for a season.

    Args:
        season: Season year, forwarded to ESPN as ``?season=YYYY``.
        group: ESPN ``group`` filter. ``50`` is NCAA Division I women's
            basketball (the default); ``51`` is Division II/III.
        raw: If True, returns the parsed JSON dict before any flattening.
        return_as_pandas: If True, returns a pandas DataFrame; otherwise
            polars.
        **kwargs: Forwarded to ``sportsdataverse.dl_utils.download``.

    Returns:
        Polars (or pandas) DataFrame with one row per team. Documented
        columns include ``team_id``, ``team_uid``, ``team_slug``,
        ``team_location``, ``team_name``, ``team_abbreviation``,
        ``team_display_name``, ``team_short_display_name``,
        ``team_color``, ``conference_id``, ``wins``, ``losses``,
        ``win_percent``, ``games_back``, ``streak``, ``points_for``,
        ``points_against``, ``point_differential``, ``home_wins``,
        ``home_losses``, ``road_wins``, ``road_losses``,
        ``division_wins``, ``division_losses``, ``season``.

        If ``raw=True``, returns the raw response dict.

    Raises:
        sportsdataverse.errors.NoESPNDataError: ESPN returned 404.
        requests.exceptions.RequestException: Other network failures after
            retries.

    Example:
        Quick start (Division I women's standings, 2024 season)::

            from sportsdataverse.wbb import espn_wbb_standings
            standings = espn_wbb_standings(season=2024, group=50)
            print(standings.shape)
            standings.select(
                ["team_display_name", "wins", "losses", "win_percent"]
            ).head(10)

        Top teams by win percentage::

            import polars as pl
            top10 = standings.sort("win_percent", descending=True).head(10)

        Pandas round-trip + Division II/III::

            d2_d3 = espn_wbb_standings(
                season=2024, group=51, return_as_pandas=True
            )
            d2_d3.head()

        Raw payload (skip the cleaning pipeline)::

            raw = espn_wbb_standings(season=2024, group=50, raw=True)
            sorted(raw.keys())

        See Also:
            * `wehoop`_ - R sister package
            * `cfbfastR`_ - companion R package for college football
            * `ESPN`_ - data origin

        .. _wehoop: https://wehoop.sportsdataverse.org
        .. _cfbfastR: https://cfbfastR.sportsdataverse.org
        .. _ESPN: https://www.espn.com
    """
    return _espn_basketball_standings(
        league=_LEAGUE_SLUG,
        season=season,
        group=group,
        raw=raw,
        return_as_pandas=return_as_pandas,
        **kwargs,
    )


def _espn_basketball_standings(
    league: str,
    season: int,
    group: int | None = None,
    *,
    raw: bool = False,
    return_as_pandas: bool = False,
    **kwargs: Any,
) -> pl.DataFrame | pd.DataFrame | dict[str, Any]:
    """Shared implementation for ``espn_wbb_standings`` / ``espn_wnba_standings``.

    Builds the ESPN site-v2 standings URL for the supplied ``league``
    slug, downloads, walks the ``children[].standings.entries[]`` tree,
    and returns one row per team. ``group`` is appended to the URL only
    when supplied (the WNBA endpoint doesn't need it).
    """
    base = f"https://site.api.espn.com/apis/v2/sports/basketball/{league}/standings?season={season}"
    url = f"{base}&group={group}" if group is not None else base
    resp = download(url, **kwargs)
    payload: dict[str, Any] = resp.json()

    if raw:
        return payload

    rows = list(_iter_rows(payload, season))

    if not rows:
        empty_schema = _build_empty_schema()
        empty = pl.DataFrame(schema=empty_schema)
        return empty.to_pandas() if return_as_pandas else empty

    df = pl.DataFrame(rows, schema=_build_empty_schema())

    return df.to_pandas() if return_as_pandas else df


def _build_empty_schema() -> dict[str, type[pl.DataType] | pl.DataType]:
    """Return the documented output column set as a polars schema dict."""
    int_cols = {
        "team_id",
        "conference_id",
        "wins",
        "losses",
        "home_wins",
        "home_losses",
        "road_wins",
        "road_losses",
        "division_wins",
        "division_losses",
        "season",
    }
    float_cols = {
        "win_percent",
        "games_back",
        "points_for",
        "points_against",
        "point_differential",
        "avg_points_for",
        "avg_points_against",
    }
    schema: dict[str, type[pl.DataType] | pl.DataType] = {}
    for col in _OUTPUT_COLUMNS:
        if col in int_cols:
            schema[col] = pl.Int64
        elif col in float_cols:
            schema[col] = pl.Float64
        else:
            schema[col] = pl.Utf8
    return schema


def _iter_rows(payload: dict[str, Any], season: int) -> Any:
    """Yield one flat row dict per team across all ``children[]`` conferences."""
    children = payload.get("children")
    # Some payloads put the standings at the top level (no children wrapper).
    if not isinstance(children, list) or not children:
        entries = (payload.get("standings") or {}).get("entries")
        if isinstance(entries, list):
            for entry in entries:
                row = _entry_to_row(entry, conf_meta={}, season=season)
                if row is not None:
                    yield row
        return

    for child in children:
        if not isinstance(child, dict):
            continue
        conf_meta = {
            "conference_id": _coerce_int(child.get("id")),
            "conference_name": _stringify(child.get("name")),
            "conference_abbreviation": _stringify(child.get("abbreviation")),
        }
        entries = (child.get("standings") or {}).get("entries")
        if not isinstance(entries, list):
            continue
        for entry in entries:
            row = _entry_to_row(entry, conf_meta=conf_meta, season=season)
            if row is not None:
                yield row


def _entry_to_row(entry: Any, conf_meta: dict[str, Any], season: int) -> dict[str, Any] | None:
    """Flatten one ``children[].standings.entries[i]`` dict to an output row."""
    if not isinstance(entry, dict):
        return None

    team = entry.get("team") or {}
    stats = entry.get("stats") or []

    row: dict[str, Any] = {col: None for col in _OUTPUT_COLUMNS}
    row["team_id"] = _coerce_int(team.get("id"))
    row["team_uid"] = _stringify(team.get("uid"))
    row["team_slug"] = _stringify(team.get("slug"))
    row["team_location"] = _stringify(team.get("location"))
    row["team_name"] = _stringify(team.get("name"))
    row["team_abbreviation"] = _stringify(team.get("abbreviation"))
    row["team_display_name"] = _stringify(team.get("displayName"))
    row["team_short_display_name"] = _stringify(team.get("shortDisplayName"))
    row["team_color"] = _stringify(team.get("color"))
    row["conference_id"] = conf_meta.get("conference_id")
    row["conference_name"] = conf_meta.get("conference_name")
    row["conference_abbreviation"] = conf_meta.get("conference_abbreviation")
    row["season"] = int(season)

    # Walk the stats[] array twice: once for numeric stats (mapped via
    # ``_STAT_VALUE_MAP``), once for string-summary stats (mapped via
    # ``_STAT_SUMMARY_MAP``). Both maps tolerate either ``name`` or
    # ``type`` as the key — ESPN ships both keys on every stat.
    summaries: dict[str, str | None] = {}
    for stat in stats:
        if not isinstance(stat, dict):
            continue
        keys = (stat.get("name"), stat.get("type"))
        for k in keys:
            if not k:
                continue
            if k in _STAT_VALUE_MAP:
                row[_STAT_VALUE_MAP[k]] = _coerce_float(stat.get("value"))
                break
            if k in _STAT_SUMMARY_MAP:
                summaries[_STAT_SUMMARY_MAP[k]] = _stringify(stat.get("summary") or stat.get("displayValue"))
                break

    home_w, home_l = _split_record(summaries.get("home_summary"))
    road_w, road_l = _split_record(summaries.get("road_summary"))
    div_w, div_l = _split_record(summaries.get("division_summary"))

    row["home_wins"] = home_w
    row["home_losses"] = home_l
    row["road_wins"] = road_w
    row["road_losses"] = road_l
    row["division_wins"] = div_w
    row["division_losses"] = div_l
    row["streak"] = summaries.get("streak") or row.get("streak")

    # Coerce ints in case the schema dict is satisfied later.
    for int_col in ("wins", "losses"):
        v = row.get(int_col)
        if isinstance(v, float):
            row[int_col] = int(v)

    return row


def _split_record(summary: str | None) -> tuple[int | None, int | None]:
    """Parse an ESPN W-L summary string like ``"16-4"`` -> ``(16, 4)``."""
    if not summary or not isinstance(summary, str):
        return None, None
    parts = summary.split("-")
    if len(parts) != 2:
        return None, None
    try:
        return int(parts[0]), int(parts[1])
    except ValueError:
        return None, None


def _coerce_int(v: Any) -> int | None:
    if v is None:
        return None
    if isinstance(v, bool):
        return None
    if isinstance(v, int):
        return v
    if isinstance(v, float):
        return int(v)
    if isinstance(v, str):
        s = v.strip()
        if not s:
            return None
        try:
            return int(s)
        except ValueError:
            try:
                return int(float(s))
            except ValueError:
                return None
    return None


def _coerce_float(v: Any) -> float | None:
    if v is None:
        return None
    if isinstance(v, bool):
        return None
    if isinstance(v, (int, float)):
        return float(v)
    if isinstance(v, str):
        s = v.strip()
        if not s:
            return None
        try:
            return float(s)
        except ValueError:
            return None
    return None


def _stringify(v: Any) -> str | None:
    if v is None:
        return None
    if isinstance(v, str):
        return v
    return str(v)
