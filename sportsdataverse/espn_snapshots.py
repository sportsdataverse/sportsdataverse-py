"""Daily snapshots of ESPN endpoints that report CURRENT STATE ONLY.

ESPN's league-level ``injuries`` feed answers "who is hurt right now". It carries
no history: yesterday's report is gone the moment ESPN updates it. History
therefore exists only if it is **snapshotted**, so every frame this module emits
is stamped with an ``as_of_date`` and is meant to be **appended** to the
previously-collected rows — never to overwrite them.

Why the league endpoint and not the game summary
------------------------------------------------
ESPN's per-game ``summary`` payload has an ``injuries`` key, and a producer stage
built on it can never emit a row: the key is present but always ``[]``. That is
why the ``espn_cfb_injuries`` release tag sat at zero assets while a correctly
wired per-game stage ran daily. The **league** endpoint
(``site.api.espn.com/apis/site/v2/sports/{sport}/{league}/injuries``) is the only
route to real injury records — do not re-wire the per-game path.

Cost: the endpoint takes **no arguments** and returns every team in one response,
so a full multi-league snapshot is one request per league per day.

Example:
    Snapshot one league::

        from sportsdataverse.espn_snapshots import espn_injuries_snapshot
        df = espn_injuries_snapshot("nfl")
        print(df.select("as_of_date", "team_id", "athlete_id", "status").head())

    Snapshot every league that reports injuries::

        df = espn_injuries_snapshot(["nfl", "nba", "nhl", "mlb", "wnba", "cfb"])

    Parse a payload you already captured (offline, no network)::

        from sportsdataverse.espn_snapshots import parse_injuries_snapshot
        df = parse_injuries_snapshot(payload, league="nfl", as_of_date=date(2026, 9, 2))

    See Also:
        * `cfbfastR`_ -- the R college-football sister package
        * `hoopR`_ -- the R men's-basketball sister package

    .. _cfbfastR: https://cfbfastR.sportsdataverse.org
    .. _hoopR: https://hoopR.sportsdataverse.org
"""

from __future__ import annotations

import importlib
import re
import time
from datetime import date, datetime, timezone
from typing import Any, Dict, List, Mapping, Optional, Sequence, Union

import polars as pl

__all__ = [
    "ESPN_INJURY_LEAGUES",
    "INJURY_SNAPSHOT_SCHEMA",
    "espn_injuries_snapshot",
    "parse_injuries_snapshot",
]

#: Leagues whose ESPN league-level ``injuries`` endpoint exists. Measured live on
#: 2026-09-02: ``nfl`` 32 teams / 800 athlete rows, ``mlb`` 30 / 280, ``nba`` 27 / 76,
#: ``nhl`` 26 / 95, ``wnba`` 13 / 40, ``cfb`` 3 / 3, ``mbb`` 0, ``wbb`` 0. The two
#: college-basketball leagues answer 200 with an empty list out of season; they are
#: kept in the tuple because an empty answer is a legitimate observation, and the
#: caller — not this module — decides not to persist a zero-row day.
ESPN_INJURY_LEAGUES: tuple[str, ...] = (
    "nfl",
    "nba",
    "wnba",
    "nhl",
    "mlb",
    "cfb",
    "mbb",
    "wbb",
)

#: The frame contract. An empty payload yields these columns with zero rows, so a
#: caller can concat/append without null-checking the shape first.
#:
#: Every id is ``Utf8``. ESPN ships them as numeric strings here, but the same ids
#: arrive as ints from other ESPN families, and a float-origin id stringifies as
#: ``"123.0"`` — so ids are pinned to Utf8 at this boundary via :func:`_as_id`,
#: which goes through ``int`` and never through ``float``.
INJURY_SNAPSHOT_SCHEMA: Dict[str, Any] = {
    "as_of_date": pl.Date,
    "league": pl.Utf8,
    "team_id": pl.Utf8,
    "team_display_name": pl.Utf8,
    "injury_id": pl.Utf8,
    "athlete_id": pl.Utf8,
    "athlete_display_name": pl.Utf8,
    "athlete_short_name": pl.Utf8,
    "athlete_position": pl.Utf8,
    "athlete_position_name": pl.Utf8,
    "status": pl.Utf8,
    "injury_date": pl.Utf8,
    "type_id": pl.Utf8,
    "type_name": pl.Utf8,
    "type_abbreviation": pl.Utf8,
    "type_description": pl.Utf8,
    "detail_type": pl.Utf8,
    "detail_location": pl.Utf8,
    "detail_detail": pl.Utf8,
    "detail_side": pl.Utf8,
    "detail_return_date": pl.Utf8,
    "detail_fantasy_status": pl.Utf8,
    "short_comment": pl.Utf8,
    "long_comment": pl.Utf8,
    "source_id": pl.Utf8,
    "source_description": pl.Utf8,
}

#: ESPN omits ``athlete.id`` from this payload entirely (verified: 0 of 800 NFL
#: records carry it on 2026-09-02). The athlete id survives only inside the
#: player-card link, so it is recovered from there rather than left null — an
#: injury row with no athlete id cannot be joined to a roster.
_ATHLETE_ID_RE = re.compile(r"/id/(\d+)")


def _as_id(value: Any) -> Optional[str]:
    """Coerce an ESPN id to ``Utf8``, never via ``float``.

    ``str(123.0)`` is ``"123.0"``, which silently fails every downstream join, so
    a float id is routed through ``int`` and a non-integral one is dropped rather
    than stringified wrong. ``bool`` is rejected because it is an ``int`` subclass.
    """
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, str):
        return value.strip() or None
    if isinstance(value, int):
        return str(value)
    if isinstance(value, float):
        return str(int(value)) if value.is_integer() else None
    return None


def _mapping(value: Any) -> Mapping[str, Any]:
    """A nested object as a Mapping, or an empty one.

    ``value or {}`` is not enough: a truthy non-dict (ESPN collapsing an object to
    a scalar) survives it and then raises ``AttributeError`` on ``.get``.
    """
    return value if isinstance(value, Mapping) else {}


def _list(value: Any) -> List[Any]:
    """A nested array as a list, or an empty one.

    Anything that is not a list is dropped rather than iterated. A truthy scalar
    raises ``TypeError`` when iterated, and a truthy *iterable* non-list (a str,
    a dict) iterates over characters or keys and silently yields nothing useful —
    both are malformed input, and this parser's contract is to degrade to zero
    rows on malformed input rather than raise.
    """
    return value if isinstance(value, list) else []


def _athlete_id(athlete: Mapping[str, Any]) -> Optional[str]:
    """Athlete id from ``athlete.id`` when present, else from the player-card link."""
    direct = _as_id(athlete.get("id"))
    if direct:
        return direct
    for link in _list(athlete.get("links")):
        match = _ATHLETE_ID_RE.search(str(_mapping(link).get("href") or ""))
        if match:
            return match.group(1)
    return None


def _text(container: Any, key: str) -> Optional[str]:
    """A string field from a possibly-absent nested dict."""
    if not isinstance(container, Mapping):
        return None
    value = container.get(key)
    return None if value is None else str(value)


def parse_injuries_snapshot(
    payload: Optional[Mapping[str, Any]],
    *,
    league: str,
    as_of_date: Optional[date] = None,
    return_as_pandas: bool = False,
) -> Any:
    """Flatten one league-level ``injuries`` payload to one row per athlete injury.

    The raw payload is one row per TEAM with a nested per-athlete ``injuries``
    list; this explodes it to the athlete grain and stamps every row with
    ``as_of_date`` so appended snapshots stay distinguishable.

    Args:
        payload: Raw JSON dict from ``espn_{league}_injuries(return_parsed=False)``.
            ``None`` or a malformed payload yields a zero-row frame.
        league: League slug written into the ``league`` column.
        as_of_date: Observation date; defaults to today (UTC).
        return_as_pandas: Return a pandas DataFrame instead of polars.

    Returns:
        A frame with :data:`INJURY_SNAPSHOT_SCHEMA`'s columns — zero rows when the
        payload reports no injuries, never a raise.

    Example:
        Parse a captured payload::

            df = parse_injuries_snapshot(payload, league="nfl")
    """
    stamp = as_of_date or datetime.now(timezone.utc).date()
    teams = _list(_mapping(payload).get("injuries"))
    rows: List[Dict[str, Any]] = []
    for team in teams:
        if not isinstance(team, Mapping):
            continue
        team_id = _as_id(team.get("id"))
        team_name = _text(team, "displayName")
        for record in _list(team.get("injuries")):
            if not isinstance(record, Mapping):
                continue
            athlete = _mapping(record.get("athlete"))
            position = _mapping(athlete.get("position"))
            type_ = _mapping(record.get("type"))
            details = _mapping(record.get("details"))
            fantasy = _mapping(details.get("fantasyStatus"))
            source = _mapping(record.get("source"))
            rows.append(
                {
                    "as_of_date": stamp,
                    "league": league,
                    "team_id": team_id,
                    "team_display_name": team_name,
                    "injury_id": _as_id(record.get("id")),
                    "athlete_id": _athlete_id(athlete),
                    "athlete_display_name": _text(athlete, "displayName"),
                    "athlete_short_name": _text(athlete, "shortName"),
                    "athlete_position": _text(position, "abbreviation"),
                    "athlete_position_name": _text(position, "displayName"),
                    "status": _text(record, "status"),
                    "injury_date": _text(record, "date"),
                    "type_id": _as_id(type_.get("id")),
                    "type_name": _text(type_, "name"),
                    "type_abbreviation": _text(type_, "abbreviation"),
                    "type_description": _text(type_, "description"),
                    "detail_type": _text(details, "type"),
                    "detail_location": _text(details, "location"),
                    "detail_detail": _text(details, "detail"),
                    "detail_side": _text(details, "side"),
                    "detail_return_date": _text(details, "returnDate"),
                    "detail_fantasy_status": _text(fantasy, "description"),
                    "short_comment": _text(record, "shortComment"),
                    "long_comment": _text(record, "longComment"),
                    "source_id": _as_id(source.get("id")),
                    "source_description": _text(source, "description"),
                }
            )
    df = pl.DataFrame(rows, schema=INJURY_SNAPSHOT_SCHEMA)
    return df.to_pandas() if return_as_pandas else df


def espn_injuries_snapshot(
    leagues: Union[str, Sequence[str]] = ESPN_INJURY_LEAGUES,
    *,
    as_of_date: Optional[date] = None,
    request_delay: float = 1.5,
    return_as_pandas: bool = False,
) -> Any:
    """Fetch today's injury report for one or more leagues, stamped with the date.

    One request per league, issued **sequentially** with ``request_delay`` seconds
    between them: ESPN answers aggressive polling with 403, and this endpoint is
    cheap enough (one call per league per day) that there is nothing to gain from
    concurrency.

    A league whose payload reports nothing contributes zero rows rather than a
    null-filled placeholder row, so an empty observation can never be counted or
    persisted as data.

    Args:
        leagues: One league slug or a sequence of them; defaults to
            :data:`ESPN_INJURY_LEAGUES`.
        as_of_date: Observation date stamped on every row; defaults to today (UTC).
        request_delay: Seconds to sleep between league requests.
        return_as_pandas: Return a pandas DataFrame instead of polars.

    Returns:
        One frame with :data:`INJURY_SNAPSHOT_SCHEMA`'s columns, long over league.

    Raises:
        ValueError: If a league slug has no ``espn_{league}_injuries`` wrapper.

    Example:
        Today's NFL report::

            df = espn_injuries_snapshot("nfl")

        Every league that reports injuries::

            df = espn_injuries_snapshot(["nfl", "nba", "nhl", "mlb", "wnba", "cfb"])
    """
    slugs = [leagues] if isinstance(leagues, str) else list(leagues)
    stamp = as_of_date or datetime.now(timezone.utc).date()
    frames: List[pl.DataFrame] = []
    for index, slug in enumerate(slugs):
        if index and request_delay > 0:
            time.sleep(request_delay)
        try:
            module = importlib.import_module(f"sportsdataverse.{slug}")
            fetch = getattr(module, f"espn_{slug}_injuries")
        except (ImportError, AttributeError) as exc:
            raise ValueError(
                f"no espn_{slug}_injuries wrapper -- known leagues: {', '.join(ESPN_INJURY_LEAGUES)}"
            ) from exc
        frames.append(parse_injuries_snapshot(fetch(return_parsed=False), league=slug, as_of_date=stamp))
    df = pl.concat(frames, how="vertical") if frames else pl.DataFrame(schema=INJURY_SNAPSHOT_SCHEMA)
    return df.to_pandas() if return_as_pandas else df
