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

The team ``depthcharts`` endpoint is the module's second surface and has the same
character -- current state, no history -- but is priced per team rather than per
league (NFL 32, NBA 30, MLB 30). ESPN publishes no depth chart at all for NHL,
WNBA or college football: those answer 200 with the ``depthchart`` key absent.

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
    "DEPTHCHART_SNAPSHOT_SCHEMA",
    "ESPN_DEPTHCHART_LEAGUES",
    "ESPN_INJURY_LEAGUES",
    "INJURY_SNAPSHOT_SCHEMA",
    "espn_depthcharts_snapshot",
    "espn_injuries_snapshot",
    "parse_depthchart_snapshot",
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

#: Second recovery route, for the day the player-card link shape changes. It has
#: never been needed (0 of 1,291 injury records across all 8 leagues lacked a
#: link id on 2026-09-02) but 98.7% of those records also carry a headshot, so it
#: is a real backstop rather than a hypothetical one — and a null athlete id is
#: an unjoinable row, not a cosmetic gap.
_HEADSHOT_ID_RE = re.compile(r"/(\d+)\.png")


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
    """Athlete id from ``athlete.id``, else the player-card link, else the headshot.

    ESPN omits ``athlete.id`` from the injuries payload on every record, so the
    id is *recovered* rather than read — which makes it fragile in one specific
    way: if the link shape changes, every id silently becomes null and the rows
    stay joinable-looking. The headshot URL carries the same id and is the
    fallback for that day.
    """
    direct = _as_id(athlete.get("id"))
    if direct:
        return direct
    for link in _list(athlete.get("links")):
        match = _ATHLETE_ID_RE.search(str(_mapping(link).get("href") or ""))
        if match:
            return match.group(1)
    headshot = _HEADSHOT_ID_RE.search(str(_mapping(athlete.get("headshot")).get("href") or ""))
    return headshot.group(1) if headshot else None


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


#: Leagues whose ESPN ``teams/{team_id}/depthcharts`` payload carries a
#: ``depthchart`` block. Measured live on 2026-09-02: ``nfl`` 3 groups / ~68
#: athletes per team, ``mlb`` 1 group / ~76, ``nba`` 1 / ~39. ``nhl``, ``wnba``
#: and ``college-football`` answer 200 with the key absent entirely -- ESPN
#: publishes no depth chart for them, so polling those leagues is wasted
#: requests, not an empty dataset. Re-probe before adding one.
ESPN_DEPTHCHART_LEAGUES: tuple[str, ...] = ("nfl", "nba", "mlb")

#: One row per athlete SLOT. A depth chart is a group (NFL ships three -- an
#: offensive package, a defensive front and special teams) of positions, each an
#: ordered list of athletes, and that order IS the depth. ``depth_rank`` makes it
#: explicit rather than leaving it to the row order of a parquet file.
#:
#: ``position_slot`` is ESPN's own key for the slot and is what makes the grain
#: unique: NFL's "3WR 1TE" group has THREE slots -- ``wr1``/``wr2``/``wr3`` --
#: that all carry position id ``1`` and abbreviation ``WR``, so a row keyed only
#: on the position cannot say which receiver spot it describes.
#:
#: Ids are ``Utf8`` for the same reason the injuries schema pins them: ESPN's own
#: wire form, coerced through ``int`` and never through ``float``.
DEPTHCHART_SNAPSHOT_SCHEMA: Dict[str, Any] = {
    "as_of_date": pl.Date,
    "league": pl.Utf8,
    "season": pl.Int64,
    "season_type": pl.Int64,
    "team_id": pl.Utf8,
    "team_display_name": pl.Utf8,
    "team_abbreviation": pl.Utf8,
    "group_id": pl.Utf8,
    "group_name": pl.Utf8,
    "position_slot": pl.Utf8,
    "position_id": pl.Utf8,
    "position_abbreviation": pl.Utf8,
    "position_name": pl.Utf8,
    "depth_rank": pl.Int64,
    "athlete_id": pl.Utf8,
    "athlete_display_name": pl.Utf8,
    "athlete_short_name": pl.Utf8,
    "espn_timestamp": pl.Utf8,
}


def _as_int(value: Any) -> Optional[int]:
    """Coerce to ``int``, or ``None`` -- never a partially-parsed number."""
    try:
        return int(str(value).strip())
    except (TypeError, ValueError):
        return None


def parse_depthchart_snapshot(
    payload: Optional[Mapping[str, Any]],
    *,
    league: str,
    as_of_date: Optional[date] = None,
    return_as_pandas: bool = False,
) -> Any:
    """Flatten one team's ``depthcharts`` payload to one row per athlete slot.

    Args:
        payload: Raw JSON dict from
            ``espn_{league}_team_depthcharts(team_id=..., return_parsed=False)``.
            ``None``, a malformed payload, or a league that publishes no depth
            chart yields a zero-row frame.
        league: League slug written into the ``league`` column.
        as_of_date: Observation date; defaults to today (UTC).
        return_as_pandas: Return a pandas DataFrame instead of polars.

    Returns:
        A frame with :data:`DEPTHCHART_SNAPSHOT_SCHEMA`'s columns -- zero rows
        when the team publishes no depth chart, never a raise.

    Example:
        Parse a captured payload::

            from sportsdataverse.espn_snapshots import parse_depthchart_snapshot
            df = parse_depthchart_snapshot(payload, league="nfl")

        Starters only::

            df.filter(pl.col("depth_rank") == 1)

    See Also:
        * `nflreadpy`_ -- the NFL sister package
        * `hoopR`_ -- the R men's-basketball sister package

    .. _nflreadpy: https://github.com/nflverse/nflreadpy
    .. _hoopR: https://hoopR.sportsdataverse.org
    """
    stamp = as_of_date or datetime.now(timezone.utc).date()
    root = _mapping(payload)
    season = _mapping(root.get("season"))
    team = _mapping(root.get("team"))
    timestamp = root.get("timestamp")
    rows: List[Dict[str, Any]] = []
    for group in _list(root.get("depthchart")):
        group_map = _mapping(group)
        for slot_key, slot in _mapping(group_map.get("positions")).items():
            slot_map = _mapping(slot)
            position = _mapping(slot_map.get("position"))
            for rank, athlete in enumerate(_list(slot_map.get("athletes")), start=1):
                if not isinstance(athlete, Mapping):
                    # A malformed entry is not a depth slot. `_mapping` would turn it
                    # into an all-null row that still counts as one -- the same rule
                    # the injuries parser applies to its records. `rank` keeps
                    # enumerating the ORIGINAL positions, so skipping an entry leaves
                    # a gap rather than silently promoting everyone behind it.
                    continue
                athlete_map = athlete
                rows.append(
                    {
                        "as_of_date": stamp,
                        "league": league,
                        "season": _as_int(season.get("year")),
                        "season_type": _as_int(season.get("type")),
                        "team_id": _as_id(team.get("id")),
                        "team_display_name": _text(team, "displayName"),
                        "team_abbreviation": _text(team, "abbreviation"),
                        "group_id": _as_id(group_map.get("id")),
                        "group_name": _text(group_map, "name"),
                        "position_slot": str(slot_key),
                        "position_id": _as_id(position.get("id")),
                        "position_abbreviation": _text(position, "abbreviation"),
                        "position_name": _text(position, "displayName"),
                        "depth_rank": rank,
                        "athlete_id": _athlete_id(athlete_map),
                        "athlete_display_name": _text(athlete_map, "displayName"),
                        "athlete_short_name": _text(athlete_map, "shortName"),
                        "espn_timestamp": None if timestamp is None else str(timestamp),
                    }
                )
    df = pl.DataFrame(rows, schema=DEPTHCHART_SNAPSHOT_SCHEMA)
    return df.to_pandas() if return_as_pandas else df


def espn_depthcharts_snapshot(
    league: str,
    *,
    team_ids: Optional[Sequence[Union[str, int]]] = None,
    as_of_date: Optional[date] = None,
    request_delay: float = 1.5,
    return_as_pandas: bool = False,
) -> Any:
    """Fetch today's depth chart for every team in a league, stamped with the date.

    Unlike ``injuries``, this endpoint is **per team**, so a league costs one
    request per team -- NFL 32, NBA 30, MLB 30. Requests are issued
    **sequentially** with ``request_delay`` seconds between them because ESPN
    answers aggressive polling with 403.

    A team that publishes no depth chart contributes zero rows rather than a
    null-filled placeholder, so an empty observation can never be counted or
    persisted as data.

    Args:
        league: League slug; see :data:`ESPN_DEPTHCHART_LEAGUES`.
        team_ids: Teams to fetch. Defaults to every team from
            ``espn_{league}_teams()`` (one extra request).
        as_of_date: Observation date stamped on every row; defaults to today (UTC).
        request_delay: Seconds to sleep between team requests.
        return_as_pandas: Return a pandas DataFrame instead of polars.

    Returns:
        One frame with :data:`DEPTHCHART_SNAPSHOT_SCHEMA`'s columns, long over team.

    Raises:
        ValueError: If the league has no ``espn_{league}_team_depthcharts`` wrapper,
            or if ``team_ids`` was not supplied and ``espn_{league}_teams()`` returned
            no teams -- an empty directory is an upstream outage, and reporting it as
            zero rows would be indistinguishable from a league ESPN publishes no depth
            chart for. Passing ``team_ids=[]`` explicitly is a no-op, not an error.

    Example:
        Today's NFL depth charts::

            from sportsdataverse.espn_snapshots import espn_depthcharts_snapshot
            df = espn_depthcharts_snapshot("nfl")

        Two teams only::

            df = espn_depthcharts_snapshot("nba", team_ids=[1, 2])

    See Also:
        * `nflreadpy`_ -- the NFL sister package
        * `hoopR`_ -- the R men's-basketball sister package

    .. _nflreadpy: https://github.com/nflverse/nflreadpy
    .. _hoopR: https://hoopR.sportsdataverse.org
    """
    stamp = as_of_date or datetime.now(timezone.utc).date()
    try:
        module = importlib.import_module(f"sportsdataverse.{league}")
        fetch = getattr(module, f"espn_{league}_team_depthcharts")
        # resolved in the same guard: a league missing its teams wrapper must
        # fail with the same clear error, not an AttributeError three lines later
        list_teams = None if team_ids is not None else getattr(module, f"espn_{league}_teams")
    except (ImportError, AttributeError) as exc:
        raise ValueError(
            f"no espn_{league}_team_depthcharts wrapper -- leagues with depth charts: "
            f"{', '.join(ESPN_DEPTHCHART_LEAGUES)}"
        ) from exc
    if list_teams is not None:
        team_ids = list_teams()["team_id"].to_list()
        if not team_ids:
            # An empty directory is an ESPN outage, not an observation. Left alone
            # it yields zero rows -- indistinguishable from the leagues ESPN
            # publishes no depth chart for, which is the one distinction this
            # dataset is built on. Callers passing team_ids=[] still get a no-op.
            raise ValueError(
                f"espn_{league}_teams() returned no teams -- refusing to report an empty"
                " snapshot that would read as 'this league has no depth charts'"
            )
    frames: List[pl.DataFrame] = []
    for index, team_id in enumerate(team_ids or []):
        if index and request_delay > 0:
            time.sleep(request_delay)
        frames.append(
            parse_depthchart_snapshot(
                fetch(team_id=str(team_id), return_parsed=False),
                league=league,
                as_of_date=stamp,
            )
        )
    df = pl.concat(frames, how="vertical") if frames else pl.DataFrame(schema=DEPTHCHART_SNAPSHOT_SCHEMA)
    return df.to_pandas() if return_as_pandas else df
