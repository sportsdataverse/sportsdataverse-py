"""ESPN women's-college-basketball game officials scraper.

Single ESPN endpoint:
    sports.core.api.espn.com/v2/sports/basketball/leagues/womens-college-basketball/events/{event_id}/competitions/{event_id}/officials

Returns one row per official assigned to a game (referee, umpires, etc.). The
``items[]`` array carries each official's identity (``id``, ``fullName``,
``firstName``, ``lastName``, ``displayName``) and a nested ``position``
sub-object with the assignment role. ESPN's site-v2 ``summary?event={id}``
endpoint surfaces the same officials list under ``gameInfo.officials[]`` but
without the official's ``id``, so this wrapper prefers the core-api path that
the wehoop R helper uses too.

The ``wbb`` and ``wnba`` public wrappers share a single internal helper
(``_espn_basketball_game_officials``) parameterized by league slug, mirroring
the ``team_roster`` / ``player_stats`` shim pattern.

This hand-written parsed wrapper is the canonical ``espn_*_game_officials``
for wbb/wnba (the generated raw same-endpoint wrapper is dropped for these two
leagues via ``espn_rename_map.yaml``): it uniquely surfaces each official's
``id`` from the core-api path, which the site-v2 officials list omits.
"""

from __future__ import annotations

from typing import Any, Literal, overload

import pandas as pd
import polars as pl

from sportsdataverse.dl_utils import download, underscore

_LEAGUE_SLUG: str = "womens-college-basketball"

# Canonical column set emitted by the parser. Mirrors the wehoop R wrapper
# (R/espn_basketball_event_helpers.R, .espn_basketball_event_officials) plus
# the firstName/lastName keys ESPN ships on the core-api endpoint.
_OUTPUT_COLUMNS: list[str] = [
    "game_id",
    "season",
    "official_id",
    "first_name",
    "last_name",
    "full_name",
    "display_name",
    "position_id",
    "position_name",
    "position_display_name",
    "order",
]


@overload
def espn_wbb_game_officials(
    game_id: int,
    season: int | None = ...,
    *,
    raw: Literal[True],
    return_as_pandas: bool = ...,
    **kwargs: Any,
) -> dict[str, Any]: ...
@overload
def espn_wbb_game_officials(
    game_id: int,
    season: int | None = ...,
    *,
    raw: Literal[False] = ...,
    return_as_pandas: Literal[True],
    **kwargs: Any,
) -> pd.DataFrame: ...
@overload
def espn_wbb_game_officials(
    game_id: int,
    season: int | None = ...,
    *,
    raw: Literal[False] = ...,
    return_as_pandas: Literal[False] = ...,
    **kwargs: Any,
) -> pl.DataFrame: ...
def espn_wbb_game_officials(
    game_id: int,
    season: int | None = None,
    *,
    raw: bool = False,
    return_as_pandas: bool = False,
    **kwargs: Any,
) -> pl.DataFrame | pd.DataFrame | dict[str, Any]:
    """Pull the officials assigned to a women's-college-basketball game.

    Args:
        game_id: ESPN event identifier (e.g. ``401637613`` for the 2024
            NCAA Division I women's championship game).
        season: Season year. Recorded as the ``season`` column on the output;
            does NOT alter the request URL because ESPN's officials endpoint
            keys on event ID alone.
        raw: If True, returns the parsed JSON dict before any flattening.
        return_as_pandas: If True, returns a pandas DataFrame; otherwise polars.
        **kwargs: Forwarded to ``sportsdataverse.dl_utils.download``.

    Returns:
        Polars (or pandas) DataFrame with one row per official:
        ``game_id``, ``season``, ``official_id``, ``first_name``,
        ``last_name``, ``full_name``, ``display_name``, ``position_id``,
        ``position_name``, ``position_display_name``, ``order``.

        When ESPN ships no officials for the game (often for unscheduled or
        future events), an empty frame with the documented schema is
        returned so callers see a stable column set.

        If ``raw=True``, returns the raw response dict.

    Raises:
        sportsdataverse.errors.NoESPNDataError: ESPN returned 404.
        requests.exceptions.RequestException: Other network failures after retries.

    Example:
        Quick start (2024 NCAA W championship game)::

            from sportsdataverse.wbb import espn_wbb_game_officials
            officials = espn_wbb_game_officials(game_id=401587902, season=2024)
            print(officials.shape)
            officials.select(["full_name", "position_display_name", "order"]).head()

        Pandas round-trip::

            officials_pd = espn_wbb_game_officials(
                game_id=401587902, season=2024, return_as_pandas=True
            )
            officials_pd.head()

        Raw payload (skip the cleaning pipeline)::

            raw = espn_wbb_game_officials(
                game_id=401587902, season=2024, raw=True
            )
            sorted(raw.keys())

        See Also:
            * `wehoop`_ - R sister package
            * `cfbfastR`_ - companion R package for college football
            * `ESPN`_ - data origin

        .. _wehoop: https://wehoop.sportsdataverse.org
        .. _cfbfastR: https://cfbfastR.sportsdataverse.org
        .. _ESPN: https://www.espn.com
    """
    return _espn_basketball_game_officials(
        league=_LEAGUE_SLUG,
        game_id=game_id,
        season=season,
        raw=raw,
        return_as_pandas=return_as_pandas,
        **kwargs,
    )


def _espn_basketball_game_officials(
    league: str,
    game_id: int,
    season: int | None = None,
    *,
    raw: bool = False,
    return_as_pandas: bool = False,
    **kwargs: Any,
) -> pl.DataFrame | pd.DataFrame | dict[str, Any]:
    """Shared implementation for ``espn_wbb_game_officials`` / ``espn_wnba_game_officials``.

    Builds the ESPN core-api officials URL for the supplied ``league`` slug,
    downloads, walks the ``items`` array, flattens the nested ``position``
    sub-object, normalizes column names to snake_case, attaches ``game_id``
    and ``season`` as constant columns, and returns polars (or pandas).
    """
    url = (
        "https://sports.core.api.espn.com/v2/sports/basketball/leagues/"
        f"{league}/events/{game_id}/competitions/{game_id}/officials"
    )
    resp = download(url, **kwargs)
    payload: dict[str, Any] = resp.json()

    if raw:
        return payload

    items_raw = payload.get("items")
    items = items_raw if isinstance(items_raw, list) else []
    rows: list[dict[str, Any]] = [item for item in items if isinstance(item, dict)]

    if not rows:
        return _empty_frame(game_id=game_id, season=season, return_as_pandas=return_as_pandas)

    parsed: list[dict[str, Any]] = [_parse_official(row) for row in rows]

    df = pl.from_dicts(parsed, schema=_polars_schema())
    df = df.rename({c: underscore(c) for c in df.columns})

    # Ensure every documented column exists (lit-null fallback for absent keys).
    missing_cols = [c for c in _OUTPUT_COLUMNS if c not in df.columns]
    if missing_cols:
        df = df.with_columns([pl.lit(None).cast(_polars_schema()[c]).alias(c) for c in missing_cols])

    df = df.with_columns(
        pl.lit(game_id).cast(pl.Int64).alias("game_id"),
        pl.lit(season).cast(pl.Int32).alias("season"),
    )
    df = df.select(_OUTPUT_COLUMNS)

    return df.to_pandas() if return_as_pandas else df


def _parse_official(item: dict[str, Any]) -> dict[str, Any]:
    """Flatten one ESPN officials ``items[i]`` dict into a single-row record."""
    position = item.get("position") if isinstance(item.get("position"), dict) else {}
    return {
        "game_id": None,  # backfilled with the call argument after frame construction
        "season": None,
        "official_id": _opt_str(item.get("id")),
        "first_name": _opt_str(item.get("firstName")),
        "last_name": _opt_str(item.get("lastName")),
        "full_name": _opt_str(item.get("fullName")),
        "display_name": _opt_str(item.get("displayName")),
        "position_id": _opt_str(position.get("id")),
        "position_name": _opt_str(position.get("name")),
        "position_display_name": _opt_str(position.get("displayName")),
        "order": _opt_int(item.get("order")),
    }


def _polars_schema() -> dict[str, type[pl.DataType] | pl.DataType]:
    """Documented schema for the officials frame."""
    return {
        "game_id": pl.Int64,
        "season": pl.Int32,
        "official_id": pl.Utf8,
        "first_name": pl.Utf8,
        "last_name": pl.Utf8,
        "full_name": pl.Utf8,
        "display_name": pl.Utf8,
        "position_id": pl.Utf8,
        "position_name": pl.Utf8,
        "position_display_name": pl.Utf8,
        "order": pl.Int32,
    }


def _empty_frame(
    *,
    game_id: int,
    season: int | None,
    return_as_pandas: bool,
) -> pl.DataFrame | pd.DataFrame:
    """Return an empty frame carrying the documented schema.

    ``game_id`` / ``season`` columns are still present (and empty) so the
    downstream column set is stable regardless of whether ESPN ships
    officials for the requested game.
    """
    _ = game_id, season  # arguments retained for API symmetry; no rows -> nothing to attach
    empty = pl.DataFrame(schema=_polars_schema())
    return empty.to_pandas() if return_as_pandas else empty


def _opt_str(value: Any) -> str | None:
    """Coerce ``value`` to ``str`` or ``None``."""
    if value is None:
        return None
    return str(value)


def _opt_int(value: Any) -> int | None:
    """Coerce ``value`` to ``int`` or ``None``."""
    if value is None:
        return None
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    if isinstance(value, str):
        s = value.strip()
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
